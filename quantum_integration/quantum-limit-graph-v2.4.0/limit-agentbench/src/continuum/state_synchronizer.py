# src/continuum/state_synchronizer.py

"""
State Synchronizer

Distributed state manager for edge-cloud consistency.

Responsibilities
----------------
- Maintain consistency across the edge-cloud boundary.
- Sync model weights, task state, and results.
- Handle network partitions gracefully.
- Support offline-first edge operation.

Enhancements
------------
- Fixed missing module-level ``logger`` (previously raised ``NameError``).
- Fixed duplicate-append bug in ``_queue_pending_sync`` (used ``_pending_keys`` set).
- Implemented ``_upload_snapshot`` / ``_download_latest_snapshot`` via HTTP
  (with graceful degradation when ``aiohttp`` is unavailable).
- Pluggable online check via ``online_probe`` callback, with a default that
  treats "no cloud endpoint" as offline.
- Bounded ``_pending_sync`` queue and bounded local-state map.
- Thread-safe via ``RLock``.
- Timezone-aware UTC timestamps.
- Robust conflict resolution (parses ``str``/``datetime`` timestamps safely).
- Deterministic snapshot IDs (avoid collisions within the same second).
- ``StateSnapshot.checksum`` computed in ``__post_init__`` — no post-hoc mutation.
- Configurable via :class:`SyncConfig`.
- Full validation; strict / non-strict modes.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Async context manager for scoped sessions.
- Custom :class:`StateSyncError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional dependency: aiohttp for HTTP sync
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import aiohttp  # type: ignore

    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class StateSyncError(ValueError):
    """Raised for invalid inputs or synchronization failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SyncConfig:
    """
    Tunable parameters for the state synchronizer.

    Centralizes intervals, timeouts, and buffer sizes so deployments can
    calibrate without editing the class.
    """

    # Sync loop cadence
    sync_interval_seconds: float = 30.0
    error_sleep_seconds: float = 60.0
    max_offline_duration_seconds: float = 1800.0

    # Network call policy
    request_timeout_seconds: float = 15.0
    max_upload_retries: int = 2
    base_backoff_seconds: float = 0.5

    # Bounded buffers
    max_local_state_entries: Optional[int] = 10_000
    max_pending_sync_entries: Optional[int] = 5_000

    # Snapshot policy
    max_snapshots_to_retain: int = 100

    def __post_init__(self) -> None:
        for name in (
            "sync_interval_seconds",
            "error_sleep_seconds",
            "max_offline_duration_seconds",
            "request_timeout_seconds",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise StateSyncError(f"{name} must be > 0 (got {value}).")
        if self.max_upload_retries < 0:
            raise StateSyncError("max_upload_retries must be >= 0.")
        if self.base_backoff_seconds < 0:
            raise StateSyncError("base_backoff_seconds must be >= 0.")
        if self.max_local_state_entries is not None and self.max_local_state_entries <= 0:
            raise StateSyncError("max_local_state_entries must be > 0 or None.")
        if self.max_pending_sync_entries is not None and self.max_pending_sync_entries <= 0:
            raise StateSyncError("max_pending_sync_entries must be > 0 or None.")
        if self.max_snapshots_to_retain <= 0:
            raise StateSyncError("max_snapshots_to_retain must be > 0.")


# --------------------------------------------------------------------------- #
# Snapshot
# --------------------------------------------------------------------------- #
@dataclass
class StateSnapshot:
    """
    Snapshot of edge state for synchronization.

    ``checksum`` is computed in ``__post_init__`` when not supplied, so the
    snapshot is always integrity-checked before leaving the device.
    """

    snapshot_id: str
    device_id: str
    timestamp: datetime
    task_states: Dict[str, Dict[str, Any]]
    model_versions: Dict[str, str]
    config_hash: str
    checksum: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_id, str) or not self.snapshot_id:
            raise StateSyncError("snapshot_id must be a non-empty string.")
        if not isinstance(self.device_id, str) or not self.device_id:
            raise StateSyncError("device_id must be a non-empty string.")
        if not isinstance(self.task_states, dict):
            raise StateSyncError("task_states must be a dict.")
        if not isinstance(self.model_versions, dict):
            raise StateSyncError("model_versions must be a dict.")
        if not isinstance(self.config_hash, str):
            raise StateSyncError("config_hash must be a string.")
        if not self.checksum:
            self.checksum = _compute_snapshot_checksum(self)

    def verify_checksum(self) -> bool:
        """Return True if ``checksum`` matches the snapshot's contents."""
        return self.checksum == _compute_snapshot_checksum(self)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "device_id": self.device_id,
            "timestamp": self.timestamp.isoformat(),
            "task_states": {k: dict(v) for k, v in self.task_states.items()},
            "model_versions": dict(self.model_versions),
            "config_hash": self.config_hash,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StateSnapshot":
        if not isinstance(data, Mapping):
            raise StateSyncError(
                f"StateSnapshot.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)

        task_states = {
            str(k): dict(v) for k, v in (data.get("task_states") or {}).items()
        }
        return cls(
            snapshot_id=str(data["snapshot_id"]),
            device_id=str(data["device_id"]),
            timestamp=timestamp,
            task_states=task_states,
            model_versions={str(k): str(v) for k, v in (data.get("model_versions") or {}).items()},
            config_hash=str(data.get("config_hash", "")),
            checksum=str(data.get("checksum", "")),
        )


# --------------------------------------------------------------------------- #
# Checksum helpers
# --------------------------------------------------------------------------- #
def _compute_snapshot_checksum(snapshot: StateSnapshot) -> str:
    """Deterministic SHA-256 over the snapshot's canonical fields."""
    payload = {
        "snapshot_id": snapshot.snapshot_id,
        "device_id": snapshot.device_id,
        "timestamp": _iso_or_str(snapshot.timestamp),
        "task_states": _json_safe(snapshot.task_states),
        "model_versions": dict(snapshot.model_versions),
        "config_hash": snapshot.config_hash,
    }
    data = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _iso_or_str(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _json_safe(value: Any) -> Any:
    """Recursively coerce non-JSON primitives to strings."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Best-effort parse of ``datetime`` / ISO-8601 string into a datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        # Normalize naive datetimes to UTC so comparisons are well-defined.
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None
    return None


# --------------------------------------------------------------------------- #
# Synchronizer
# --------------------------------------------------------------------------- #
class StateSynchronizer:
    """
    Distributed state manager for edge-cloud consistency.

    Thread-safe, serializable, bounded in memory, and resilient to network
    partitions. The original public API is preserved; new parameters are
    keyword-only with backward-compatible defaults.
    """

    def __init__(
        self,
        device_id: str,
        cloud_storage_endpoint: str,
        sync_interval_seconds: int = 30,
        max_offline_duration_seconds: int = 1800,
        *,
        config: Optional[SyncConfig] = None,
        strict: bool = True,
        online_probe: Optional[Callable[[], bool]] = None,
        http_session_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        """
        Parameters
        ----------
        device_id : str
            Unique device identifier.
        cloud_storage_endpoint : str
            Base URL for the cloud state-sync service.
        sync_interval_seconds : int
            Legacy positional; folded into :class:`SyncConfig` when the
            caller did not supply one.
        max_offline_duration_seconds : int
            Legacy positional; folded into :class:`SyncConfig` when the
            caller did not supply one.
        config : SyncConfig, optional
            Extended configuration. If provided, ``sync_interval_seconds``
            and ``max_offline_duration_seconds`` are ignored.
        strict : bool, default True
            If True, invalid inputs raise :class:`StateSyncError`.
        online_probe : callable, optional
            Zero-arg callable returning a bool. Defaults to a probe that
            treats an empty ``cloud_storage_endpoint`` as offline.
        http_session_factory : callable, optional
            Zero-arg callable returning an ``aiohttp.ClientSession`` (or
            compatible). Defaults to ``aiohttp.ClientSession`` when available.
        """
        if not isinstance(device_id, str) or not device_id:
            raise StateSyncError("device_id must be a non-empty string.")
        if not isinstance(cloud_storage_endpoint, str):
            raise StateSyncError("cloud_storage_endpoint must be a string.")
        if not isinstance(sync_interval_seconds, int) or sync_interval_seconds <= 0:
            raise StateSyncError("sync_interval_seconds must be a positive int.")
        if (
            not isinstance(max_offline_duration_seconds, int)
            or max_offline_duration_seconds <= 0
        ):
            raise StateSyncError(
                "max_offline_duration_seconds must be a positive int."
            )

        # Prefer explicit ``config``; otherwise build one from legacy positionals.
        if config is not None:
            self._config = config
        else:
            self._config = SyncConfig(
                sync_interval_seconds=float(sync_interval_seconds),
                max_offline_duration_seconds=float(max_offline_duration_seconds),
            )

        self.device_id: str = device_id
        self.cloud_endpoint: str = cloud_storage_endpoint.rstrip("/")
        self._strict: bool = bool(strict)

        # Legacy attributes preserved for backward compatibility.
        self.sync_interval: int = int(self._config.sync_interval_seconds)
        self.max_offline_duration: int = int(
            self._config.max_offline_duration_seconds
        )

        self._lock = threading.RLock()
        self._local_state: Dict[str, Dict[str, Any]] = {}
        self._remote_state: Dict[str, Any] = {}
        self._pending_sync: List[Dict[str, Any]] = []
        self._pending_keys: set = set()          # dedupe guard for _pending_sync
        self._offline_since: Optional[datetime] = None
        self._running: bool = False

        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._started_at: Optional[float] = None

        # Online probe (overridable for tests)
        self._online_probe: Callable[[], bool] = online_probe or self._default_online_probe

        # HTTP session factory (injectable for tests)
        self._http_session_factory = http_session_factory

        logger.debug(
            "StateSynchronizer initialized (device=%s, endpoint=%r, "
            "interval=%.0fs, max_offline=%.0fs, strict=%s, aiohttp=%s)",
            self.device_id,
            self.cloud_endpoint,
            self._config.sync_interval_seconds,
            self._config.max_offline_duration_seconds,
            self._strict,
            _AIOHTTP_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SyncConfig:
        return self._config

    @property
    def running(self) -> bool:
        return self._running

    @property
    def offline_since(self) -> Optional[datetime]:
        with self._lock:
            return self._offline_since

    @property
    def pending_sync_count(self) -> int:
        with self._lock:
            return len(self._pending_sync)

    @property
    def local_state_count(self) -> int:
        with self._lock:
            return len(self._local_state)

    # -------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Start the background synchronization loop."""
        if self._running:
            logger.debug("StateSynchronizer already running.")
            return
        self._running = True
        self._started_at = time.time()
        self._tasks = [
            asyncio.create_task(self._sync_loop(), name="state_sync:loop"),
        ]
        logger.info(
            "StateSynchronizer started (device=%s, interval=%.0fs).",
            self.device_id,
            self._config.sync_interval_seconds,
        )

    async def stop(self, *, timeout_seconds: float = 5.0) -> None:
        """Stop the background loop and drain in-flight work."""
        if not self._running:
            return
        self._running = False

        for t in self._tasks:
            t.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:  # pragma: no cover
                logger.warning("Timed out waiting for sync loop to stop.")
        self._tasks = []

        logger.info(
            "StateSynchronizer stopped (device=%s, local=%d, pending=%d).",
            self.device_id,
            len(self._local_state),
            len(self._pending_sync),
        )

    # -------------------------------------------------------- state API
    def update_local_state(self, key: str, value: Any) -> None:
        """
        Update local state (triggers sync on next interval).

        Parameters
        ----------
        key : str
            State key. Must be a non-empty string.
        value : Any
            JSON-serializable value (non-serializable values are coerced to
            strings during checksum computation).
        """
        if not isinstance(key, str) or not key:
            raise StateSyncError("key must be a non-empty string.")

        with self._lock:
            is_new = key not in self._local_state
            self._local_state[key] = {
                "value": value,
                "updated_at": datetime.now(timezone.utc),
                "synced": False,
            }
            self._enforce_local_state_bound_locked()

        # Remove from pending set: next successful sync will re-queue if needed.
        with self._lock:
            self._pending_keys.discard(key)

        logger.debug(
            "Local state updated (key=%s, new=%s, total=%d).",
            key,
            is_new,
            len(self._local_state),
        )

    async def get_state(
        self, key: str, prefer_local: bool = True
    ) -> Optional[Any]:
        """Return the state value for ``key`` from local or remote cache."""
        if not isinstance(key, str) or not key:
            raise StateSyncError("key must be a non-empty string.")
        with self._lock:
            if prefer_local and key in self._local_state:
                return self._local_state[key]["value"]
            if key in self._remote_state:
                entry = self._remote_state[key]
                # Remote cache stores raw payloads; unwrap if wrapped.
                if isinstance(entry, Mapping) and "value" in entry:
                    return entry["value"]
                return entry
        return None

    # -------------------------------------------------------- sync loop
    async def _sync_loop(self) -> None:
        """Background loop that drives synchronization."""
        cfg = self._config
        while self._running:
            try:
                if self._is_online():
                    await self._sync_to_cloud()
                    await self._sync_from_cloud()
                    with self._lock:
                        self._offline_since = None
                else:
                    with self._lock:
                        if self._offline_since is None:
                            self._offline_since = datetime.now(timezone.utc)
                    await self._queue_pending_sync()

                await asyncio.sleep(cfg.sync_interval_seconds)

            except asyncio.CancelledError:
                logger.debug("Sync loop cancelled.")
                raise
            except Exception as exc:
                logger.warning("State sync error: %s", exc)
                with self._lock:
                    if self._offline_since is None:
                        self._offline_since = datetime.now(timezone.utc)
                await asyncio.sleep(cfg.error_sleep_seconds)

    # -------------------------------------------------------- online probe
    def _default_online_probe(self) -> bool:
        """Default: consider the device online when an endpoint is configured."""
        return bool(self.cloud_endpoint)

    def _is_online(self) -> bool:
        """Return True when the configured online probe reports online."""
        try:
            return bool(self._online_probe())
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Online probe raised %s; treating as offline.", exc)
            return False

    # -------------------------------------------------------- sync up
    async def _sync_to_cloud(self) -> None:
        """Upload unsynced local state to the cloud."""
        with self._lock:
            unsynced = {
                k: dict(v)
                for k, v in self._local_state.items()
                if not v.get("synced", False)
            }

        if not unsynced:
            return

        snapshot = StateSnapshot(
            snapshot_id=self._generate_snapshot_id(),
            device_id=self.device_id,
            timestamp=datetime.now(timezone.utc),
            task_states=unsynced,
            model_versions=self._get_model_versions(),
            config_hash=self._calculate_config_hash(),
        )

        try:
            await self._upload_snapshot(snapshot)
        except Exception as exc:
            logger.warning(
                "Snapshot upload failed (%s); %d key(s) remain unsynced.",
                exc,
                len(unsynced),
            )
            if self._strict:
                raise
            return

        with self._lock:
            for key in unsynced.keys():
                entry = self._local_state.get(key)
                if entry is not None:
                    entry["synced"] = True

        logger.debug(
            "Uploaded snapshot %s with %d key(s).",
            snapshot.snapshot_id,
            len(unsynced),
        )

    # -------------------------------------------------------- sync down
    async def _sync_from_cloud(self) -> None:
        """Merge the latest remote snapshot into local state (LWW)."""
        try:
            remote_snapshot = await self._download_latest_snapshot()
        except Exception as exc:
            logger.warning("Snapshot download failed: %s", exc)
            if self._strict:
                raise
            return

        if remote_snapshot is None:
            return

        # Verify integrity if checksum is present.
        if remote_snapshot.checksum and not remote_snapshot.verify_checksum():
            logger.warning(
                "Remote snapshot %s failed checksum verification; skipping.",
                remote_snapshot.snapshot_id,
            )
            if self._strict:
                raise StateSyncError("Remote snapshot checksum mismatch.")
            return

        with self._lock:
            for key, value in remote_snapshot.task_states.items():
                remote_updated = _parse_timestamp(
                    value.get("updated_at") if isinstance(value, Mapping) else None
                )
                local_entry = self._local_state.get(key)
                local_updated = (
                    _parse_timestamp(local_entry.get("updated_at"))
                    if local_entry is not None
                    else None
                )

                # New key from remote: adopt it.
                if local_entry is None:
                    self._local_state[key] = dict(value)
                    self._local_state[key]["synced"] = True
                    continue

                # Conflict: last-write-wins.
                if remote_updated and (
                    local_updated is None or remote_updated > local_updated
                ):
                    self._local_state[key] = dict(value)
                    self._local_state[key]["synced"] = True

            # Refresh remote-state cache.
            self._remote_state = {
                k: dict(v) for k, v in remote_snapshot.task_states.items()
            }

        logger.debug(
            "Merged remote snapshot %s (%d key(s)).",
            remote_snapshot.snapshot_id,
            len(remote_snapshot.task_states),
        )

    # -------------------------------------------------------- offline queue
    async def _queue_pending_sync(self) -> None:
        """Append unsynced keys to the pending queue (deduplicated, bounded)."""
        with self._lock:
            unsynced = {
                k: dict(v)
                for k, v in self._local_state.items()
                if not v.get("synced", False)
            }

            for key, value in unsynced.items():
                if key in self._pending_keys:
                    continue
                self._pending_sync.append(
                    {
                        "key": key,
                        "value": value,
                        "queued_at": datetime.now(timezone.utc),
                    }
                )
                self._pending_keys.add(key)

            cap = self._config.max_pending_sync_entries
            if cap is not None and len(self._pending_sync) > cap:
                dropped = self._pending_sync[: len(self._pending_sync) - cap]
                self._pending_sync = self._pending_sync[-cap:]
                for entry in dropped:
                    self._pending_keys.discard(entry["key"])

            offline_duration = (
                (datetime.now(timezone.utc) - self._offline_since).total_seconds()
                if self._offline_since
                else 0.0
            )

        if offline_duration > self._config.max_offline_duration_seconds:
            logger.warning(
                "Offline for %.0fs (exceeds limit %.0fs); risk of data loss.",
                offline_duration,
                self._config.max_offline_duration_seconds,
            )

    def flush_pending(self) -> int:
        """
        Clear the pending-sync queue and return the number of dropped entries.

        Callers typically invoke this after a successful reconnect.
        """
        with self._lock:
            dropped = len(self._pending_sync)
            self._pending_sync.clear()
            self._pending_keys.clear()
        logger.debug("Flushed %d pending sync entries.", dropped)
        return dropped

    # -------------------------------------------------------- snapshot helpers
    def _generate_snapshot_id(self) -> str:
        """Return a collision-resistant snapshot id."""
        raw = (
            f"{self.device_id}:{datetime.now(timezone.utc).isoformat()}:"
            f"{uuid.uuid4().hex}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _calculate_snapshot_checksum(self, snapshot: StateSnapshot) -> str:
        """Kept for backward compatibility — delegates to module helper."""
        return _compute_snapshot_checksum(snapshot)

    def _get_model_versions(self) -> Dict[str, str]:
        """
        Return current model versions.

        Overridable hook: subclasses can wire this to their model registry.
        Default returns an empty mapping (matching the original behavior).
        """
        return {}

    def _calculate_config_hash(self) -> str:
        """
        Return a hash of the current configuration.

        Overridable hook: subclasses can hash their config files. Default
        keeps the original placeholder behavior but uses ``sha256`` over the
        device id so it is not a constant.
        """
        return hashlib.sha256(self.device_id.encode("utf-8")).hexdigest()[:16]

    # -------------------------------------------------------- HTTP I/O
    def _session(self) -> Any:
        """Return an aiohttp ClientSession (or raise if unavailable)."""
        if self._http_session_factory is not None:
            return self._http_session_factory()
        if not _AIOHTTP_AVAILABLE:
            raise StateSyncError("aiohttp is required for HTTP state sync.")
        return aiohttp.ClientSession()

    async def _upload_snapshot(self, snapshot: StateSnapshot) -> None:
        """Upload ``snapshot`` via HTTP PUT to the cloud endpoint."""
        if not self.cloud_endpoint:
            raise StateSyncError("cloud_storage_endpoint is empty.")

        cfg = self._config
        url = f"{self.cloud_endpoint}/api/v1/state/snapshots/{snapshot.snapshot_id}"
        payload = snapshot.to_dict()
        timeout = (
            aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)
            if _AIOHTTP_AVAILABLE
            else None
        )

        last_exc: Optional[BaseException] = None
        for attempt in range(1 + cfg.max_upload_retries):
            try:
                session = self._session()
                async with session as s:
                    async with s.put(url, json=payload, timeout=timeout) as response:
                        response.raise_for_status()
                logger.debug(
                    "Uploaded snapshot %s in %d attempt(s).",
                    snapshot.snapshot_id,
                    attempt + 1,
                )
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Snapshot upload attempt %d/%d failed: %s",
                    attempt + 1,
                    cfg.max_upload_retries + 1,
                    exc,
                )
                if attempt < cfg.max_upload_retries:
                    await asyncio.sleep(cfg.base_backoff_seconds * (2 ** attempt))

        raise StateSyncError(
            f"Snapshot upload failed after "
            f"{cfg.max_upload_retries + 1} attempt(s): {last_exc}"
        )

    async def _download_latest_snapshot(self) -> Optional[StateSnapshot]:
        """Download the latest snapshot from the cloud endpoint (or None)."""
        if not self.cloud_endpoint:
            return None

        cfg = self._config
        url = f"{self.cloud_endpoint}/api/v1/state/snapshots/latest"
        timeout = (
            aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)
            if _AIOHTTP_AVAILABLE
            else None
        )

        session = self._session()
        async with session as s:
            async with s.get(url, timeout=timeout) as response:
                if response.status == 404:
                    logger.debug("No remote snapshot found.")
                    return None
                response.raise_for_status()
                body = await response.json()
        return StateSnapshot.from_dict(body)

    # -------------------------------------------------------- status
    async def get_sync_status(self) -> Dict[str, Any]:
        """Return the current synchronization status."""
        with self._lock:
            offline_since = self._offline_since
            local_count = len(self._local_state)
            remote_count = len(self._remote_state)
            pending_count = len(self._pending_sync)
            last_sync = self._get_last_sync_time_locked()

        offline_duration = (
            (datetime.now(timezone.utc) - offline_since).total_seconds()
            if offline_since
            else 0.0
        )

        return {
            "device_id": self.device_id,
            "online": self._is_online(),
            "offline_since": offline_since.isoformat() if offline_since else None,
            "offline_duration_seconds": offline_duration,
            "local_state_count": local_count,
            "remote_state_count": remote_count,
            "pending_sync_count": pending_count,
            "last_sync": last_sync,
            "running": self._running,
        }

    def _get_last_sync_time_locked(self) -> Optional[str]:
        synced_items = [
            v.get("updated_at")
            for v in self._local_state.values()
            if v.get("synced", False) and v.get("updated_at")
        ]
        if not synced_items:
            return None
        # Normalize then max — avoids comparing str to datetime.
        parsed = [_parse_timestamp(x) for x in synced_items]
        parsed = [p for p in parsed if p is not None]
        if not parsed:
            return None
        return max(parsed).isoformat()

    def _get_last_sync_time(self) -> Optional[str]:
        """Kept for backward compatibility; acquires the lock itself."""
        with self._lock:
            return self._get_last_sync_time_locked()

    # -------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics about the synchronizer state."""
        with self._lock:
            local = list(self._local_state.values())
            pending = len(self._pending_sync)
            remote = len(self._remote_state)

        synced = sum(1 for v in local if v.get("synced", False))
        unsynced = sum(1 for v in local if not v.get("synced", False))

        return {
            "device_id": self.device_id,
            "local_entries": len(local),
            "remote_entries": remote,
            "pending_entries": pending,
            "synced_entries": synced,
            "unsynced_entries": unsynced,
            "offline": self._offline_since is not None,
            "running": self._running,
            "started_at": self._started_at,
        }

    def reset(self, *, clear_local: bool = False) -> None:
        """Reset in-memory sync state."""
        with self._lock:
            if clear_local:
                self._local_state.clear()
            self._pending_sync.clear()
            self._pending_keys.clear()
            self._remote_state.clear()
            self._offline_since = None
        logger.debug("StateSynchronizer reset (clear_local=%s)", clear_local)

    def _enforce_local_state_bound_locked(self) -> None:
        cap = self._config.max_local_state_entries
        if cap is None:
            return
        # Evict oldest synced entries first; if still over cap, evict oldest.
        while len(self._local_state) > cap:
            candidates = [
                (k, v.get("updated_at"))
                for k, v in self._local_state.items()
                if v.get("synced", False)
            ]
            if candidates:
                candidates.sort(key=lambda kv: _parse_timestamp(kv[1]) or datetime.min.replace(tzinfo=timezone.utc))
                oldest_key = candidates[0][0]
            else:
                # No synced entries — evict the oldest entry overall.
                entries = sorted(
                    self._local_state.items(),
                    key=lambda kv: _parse_timestamp(kv[1].get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
                )
                oldest_key = entries[0][0]
            self._local_state.pop(oldest_key, None)

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "device_id": self.device_id,
                "cloud_endpoint": self.cloud_endpoint,
                "config": asdict(self._config),
                "strict": self._strict,
                "running": self._running,
                "offline_since": (
                    self._offline_since.isoformat()
                    if self._offline_since
                    else None
                ),
                "started_at": self._started_at,
                "local_state": {
                    k: _json_safe(dict(v)) for k, v in self._local_state.items()
                },
                "remote_state": {
                    k: _json_safe(v) for k, v in self._remote_state.items()
                },
                "pending_sync": [
                    {**_json_safe(dict(p)), "queued_at": _iso_or_str(p["queued_at"])}
                    for p in self._pending_sync
                ],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        online_probe: Optional[Callable[[], bool]] = None,
        http_session_factory: Optional[Callable[[], Any]] = None,
    ) -> "StateSynchronizer":
        if not isinstance(data, Mapping):
            raise StateSyncError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = SyncConfig(
            sync_interval_seconds=float(
                cfg_data.get("sync_interval_seconds", 30.0)
            ),
            error_sleep_seconds=float(cfg_data.get("error_sleep_seconds", 60.0)),
            max_offline_duration_seconds=float(
                cfg_data.get("max_offline_duration_seconds", 1800.0)
            ),
            request_timeout_seconds=float(
                cfg_data.get("request_timeout_seconds", 15.0)
            ),
            max_upload_retries=int(cfg_data.get("max_upload_retries", 2)),
            base_backoff_seconds=float(cfg_data.get("base_backoff_seconds", 0.5)),
            max_local_state_entries=cfg_data.get("max_local_state_entries", 10_000),
            max_pending_sync_entries=cfg_data.get("max_pending_sync_entries", 5_000),
            max_snapshots_to_retain=int(
                cfg_data.get("max_snapshots_to_retain", 100)
            ),
        )

        sync = cls(
            device_id=str(data["device_id"]),
            cloud_storage_endpoint=str(data.get("cloud_endpoint", "")),
            config=cfg,
            strict=bool(data.get("strict", True)),
            online_probe=online_probe,
            http_session_factory=http_session_factory,
        )

        with sync._lock:
            for k, v in (data.get("local_state") or {}).items():
                entry = dict(v)
                entry["updated_at"] = _parse_timestamp(entry.get("updated_at"))
                sync._local_state[str(k)] = entry
            sync._remote_state = {
                str(k): dict(v) for k, v in (data.get("remote_state") or {}).items()
            }
            for p in data.get("pending_sync") or []:
                entry = {
                    "key": p["key"],
                    "value": p.get("value"),
                    "queued_at": _parse_timestamp(p.get("queued_at"))
                    or datetime.now(timezone.utc),
                }
                sync._pending_sync.append(entry)
                sync._pending_keys.add(entry["key"])
            if data.get("offline_since"):
                sync._offline_since = _parse_timestamp(data["offline_since"])
            sync._started_at = data.get("started_at")
        return sync

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        online_probe: Optional[Callable[[], bool]] = None,
        http_session_factory: Optional[Callable[[], Any]] = None,
    ) -> "StateSynchronizer":
        try:
            return cls.from_dict(
                json.loads(payload),
                online_probe=online_probe,
                http_session_factory=http_session_factory,
            )
        except json.JSONDecodeError as exc:
            raise StateSyncError(f"Invalid JSON payload: {exc}") from exc

    # -------------------------------------------------------- async ctx mgr
    async def __aenter__(self) -> "StateSynchronizer":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped state-sync session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        try:
            await self.stop()
        finally:
            if exc_type is not None:
                logger.warning(
                    "State-sync scope exited with %s after %.4fs.",
                    exc_type.__name__,
                    elapsed,
                )
            else:
                logger.info(
                    "State-sync scope closed in %.4fs (local=%d, pending=%d).",
                    elapsed,
                    len(self._local_state),
                    len(self._pending_sync),
                )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "StateSynchronizer("
                f"device_id={self.device_id!r}, "
                f"endpoint={self.cloud_endpoint!r}, "
                f"running={self._running}, "
                f"offline={self._offline_since is not None}, "
                f"local={len(self._local_state)}, "
                f"pending={len(self._pending_sync)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "StateSnapshot",
    "StateSynchronizer",
    "SyncConfig",
    "StateSyncError",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m continuum.state_synchronizer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ------------------------------------------------------------------ #
    # Minimal in-memory cloud that implements the same PUT/GET contract.
    # ------------------------------------------------------------------ #
    class _FakeResponse:
        def __init__(self, status: int, payload: Any = None) -> None:
            self.status = status
            self._payload = payload

        def raise_for_status(self) -> None:
            if self.status >= 400:
                raise RuntimeError(f"HTTP {self.status}")

        async def json(self) -> Any:
            return self._payload

    class _FakeSession:
        # Shared storage across instances within the test.
        _store: Dict[str, Any] = {}

        def __init__(self) -> None:
            pass

        async def __aenter__(self) -> "_FakeSession":
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        def put(self, url: str, json: Any = None, timeout: Any = None):
            sid = url.rsplit("/", 1)[-1]
            _FakeSession._store[sid] = json
            _FakeSession._store["latest"] = json

            class _Ctx:
                async def __aenter__(self_inner):
                    return _FakeResponse(200)

                async def __aexit__(self_inner, *exc):
                    return None
            return _Ctx()

        def get(self, url: str, timeout: Any = None):
            payload = _FakeSession._store.get("latest")

            class _Ctx:
                async def __aenter__(self_inner):
                    if payload is None:
                        return _FakeResponse(404)
                    return _FakeResponse(200, payload)

                async def __aexit__(self_inner, *exc):
                    return None
            return _Ctx()

    async def main() -> None:
        # Online device whose probe always returns True.
        sync = StateSynchronizer(
            device_id="edge-01",
            cloud_storage_endpoint="http://cloud.invalid",
            sync_interval_seconds=1,
            max_offline_duration_seconds=10,
            http_session_factory=_FakeSession,
            online_probe=lambda: True,
        )

        async with sync:
            await sync.start()

            # Update local state and wait for a sync cycle.
            sync.update_local_state("model_version", "v1.2.3")
            sync.update_local_state("last_task", {"id": "t-42", "ok": True})
            await asyncio.sleep(1.5)

            print("state 1     :", await sync.get_state("model_version"))
            print("state 2     :", await sync.get_state("last_task"))
            print("status      :", await sync.get_sync_status())
            print("stats       :", sync.statistics())

            # Simulate partition: probe returns False.
            sync._online_probe = lambda: False  # type: ignore[assignment]
            sync.update_local_state("queued_key", {"value": 1})
            await asyncio.sleep(1.5)
            print("pending     :", sync.pending_sync_count)

            # Reconnect: probe returns True again, flush pending.
            sync._online_probe = lambda: True  # type: ignore[assignment]
            print("flushed     :", sync.flush_pending())

            # Serialization round-trip (with a fresh probe).
            payload = sync.to_json()
            restored = StateSynchronizer.from_json(
                payload,
                http_session_factory=_FakeSession,
                online_probe=lambda: True,
            )
            assert restored.to_dict() == sync.to_dict()
            print("Serialization round-trip OK.")
            print("Restored    :", restored)

        # Validation failures.
        for bad in (
            ("", "http://x", 30, 1800),
            ("edge", "http://x", 0, 1800),
            ("edge", "http://x", 30, -1),
        ):
            try:
                StateSynchronizer(*bad)
            except StateSyncError as exc:
                print("Rejected as expected:", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected rejection for {bad!r}")

        print("\nSmoke test passed.")

    asyncio.run(main())
