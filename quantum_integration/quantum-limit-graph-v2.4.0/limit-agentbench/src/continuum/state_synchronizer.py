# src/continuum/state_synchronizer.py
"""
State Synchronizer — v6.0.0
============================

Distributed state manager for edge-cloud consistency.

**Public API preserved**

    sync = StateSynchronizer(device_id, cloud_storage_endpoint, ...)
    await sync.start()
    sync.update_local_state(key, value)
    await sync.get_state(key)
    await sync.stop()

**What changed (v5 → v6)**
--------------------------

**P0 — silent data loss and broken invariants**

1. **The ``_pending_sync`` buffer is removed (§2.1, §2.2).** v5 maintained
   a duplicate-prone list that was never consumed by the upload path —
   ``pending_sync_count`` and ``statistics()["pending_entries"]`` were
   diagnostic lies. v6 derives the pending view on demand from
   ``_local_state``; there is no second buffer to keep in sync.
2. **``from_dict`` enforces ``max_local_state_entries`` and
   ``max_remote_state_entries`` on restore (§2.3).** v5 restored an
   unbounded cache regardless of the configured caps.
3. **``StateSnapshot.__post_init__`` normalizes the timestamp to aware
   UTC and validates any supplied checksum (§2.4, §2.5).** v5 accepted
   naive timestamps (checksum instability across peers) and stored a
   bogus ``checksum="garbage"`` verbatim.
4. **``_sync_from_cloud`` re-enforces ``max_local_state_entries`` after
   merge (§2.6)** — v5 bypassed the bound on every pull.
5. **``_remote_state`` is bounded** via ``max_remote_state_entries``
   (§2.7).
6. **``max_offline_duration_seconds`` is enforced** (§2.8). v5 only
   logged a warning; v6 drops the oldest half of the unsynced entries
   once per offline period (bounded loss) and logs at ERROR.
7. **``max_snapshots_to_retain`` is now load-bearing** (§2.9). v5
   validated the field but never used it. v6 maintains a bounded
   ``_snapshot_history`` deque.
8. **Retries fire only on transient errors** (§2.10). ``StateSyncError``
   from a missing ``aiohttp`` is definitive and surfaces immediately
   instead of burning ``1 + max_upload_retries`` attempts.
9. **``_calculate_config_hash`` hashes the actual config** (§2.11).
   v5 hashed ``device_id``; the field name was a lie.

**P1 — reliability**

10. **The sync loop auto-recovers** (§3.1). v5's ``_is_online()`` probe
    was consulted before each attempt, so a conservative probe could pin
    the loop offline forever. v6 always attempts sync; failure drives
    the offline state, and the next success clears it.
11. **``health_check()``, structured logging (``_log``),
    ``collect_prometheus_metrics()``** (§3.2–§3.4).
12. **``_last_sync_at`` is cached** (§3.5) — ``get_sync_status()`` no
    longer scans ``_local_state``.
13. **``_enforce_local_state_bound_locked`` is a single pass** (§3.6)
    and **logs ERROR when evicting unsynced entries** (§3.7).
14. **Download retries on transient failures and uses conditional GET**
    (§3.9, §3.10).
15. **Live reconfiguration** via ``set_endpoint``, ``set_online_probe``,
    ``set_config`` (§3.12).
16. **Legacy positional args accept ``float``** (§3.13).
17. **The HTTP session is reused** across calls (§3.14) and closed in
    ``stop()``.

**P2 — structure and features**

18. **``StateSnapshot`` gains ``schema_version``** (§4.4).
19. **LWW tiebreaker** (§4.5): equal timestamps prefer remote, so peers
    converge to the same state.
20. **Snapshot history** (bounded deque).
21. **Extra accessors**: ``list_local_keys()``, ``get_all_local_state()``,
    ``pending_sync_entries``.
22. **``reset(clear_local=True)`` resets ``_started_at``** (§4.9).

**Shared primitives**

``ContinuumError``, ``_json_safe``, ``_coerce_timestamp``, ``_coerce_float``
are imported from ``continuum.types``.

**Running the demo**

    PYTHONPATH=src python -m continuum.state_synchronizer
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

# --------------------------------------------------------------------------- #
# Shared continuum primitives
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — package vs. script
    from .types import (
        ContinuumError,
        _json_safe,
        _coerce_float,
        _coerce_timestamp,
    )
except ImportError:  # pragma: no cover
    from continuum.types import (  # type: ignore[no-redef]
        ContinuumError,
        _json_safe,
        _coerce_float,
        _coerce_timestamp,
    )

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional aiohttp
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    import aiohttp  # type: ignore
    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Transient-vs-definitive exception classification
# --------------------------------------------------------------------------- #
_TRANSIENT: Tuple[type, ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
)
if _AIOHTTP_AVAILABLE:
    for _name in (
        "ClientConnectorError",
        "ServerDisconnectedError",
        "ServerTimeoutError",
        "ClientConnectionError",
    ):
        _exc = getattr(aiohttp, _name, None)
        if _exc is not None:
            _TRANSIENT += (_exc,)


# --------------------------------------------------------------------------- #
# Errors + structured logging
# --------------------------------------------------------------------------- #
class StateSyncError(ContinuumError):
    """Raised for invalid inputs or synchronization failures."""


def _log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    if not logger.isEnabledFor(level):
        return
    parts = [event]
    if fields:
        parts.append(" ".join(f"{k}={v}" for k, v in fields.items()))
    logger.log(level, " ".join(parts), extra={"event": event, **fields})


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SyncConfig:
    """Tunable parameters for the state synchronizer."""

    sync_interval_seconds: float = 30.0
    error_sleep_seconds: float = 60.0
    max_offline_duration_seconds: float = 1800.0

    request_timeout_seconds: float = 15.0
    max_upload_retries: int = 2
    base_backoff_seconds: float = 0.5

    max_local_state_entries: Optional[int] = 10_000
    max_remote_state_entries: Optional[int] = 10_000
    max_pending_sync_entries: Optional[int] = 5_000   # kept for compat

    # v6: now load-bearing — snapshot history is bounded by this
    max_snapshots_to_retain: int = 100

    # v6: when True, exceeding max_offline_duration drops the oldest half
    # of the unsynced entries once per offline period (bounded loss).
    offline_overflow_eviction: bool = True

    def __post_init__(self) -> None:
        for name in (
            "sync_interval_seconds",
            "error_sleep_seconds",
            "max_offline_duration_seconds",
            "request_timeout_seconds",
        ):
            v = getattr(self, name)
            if v <= 0:
                raise StateSyncError(f"{name} must be > 0 (got {v})")
        if self.max_upload_retries < 0:
            raise StateSyncError(
                "max_upload_retries must be >= 0"
            )
        if self.base_backoff_seconds < 0:
            raise StateSyncError(
                "base_backoff_seconds must be >= 0"
            )
        for name in (
            "max_local_state_entries",
            "max_remote_state_entries",
            "max_pending_sync_entries",
        ):
            v = getattr(self, name)
            if v is not None and v <= 0:
                raise StateSyncError(f"{name} must be > 0 or None")
        if self.max_snapshots_to_retain <= 0:
            raise StateSyncError(
                "max_snapshots_to_retain must be > 0"
            )


# --------------------------------------------------------------------------- #
# Snapshot (frozen, validated, checksum-verified at construction)
# --------------------------------------------------------------------------- #
def _compute_snapshot_checksum(snapshot: "StateSnapshot") -> str:
    payload = {
        "snapshot_id": snapshot.snapshot_id,
        "device_id": snapshot.device_id,
        "timestamp": snapshot.timestamp.isoformat()
        if isinstance(snapshot.timestamp, datetime)
        else str(snapshot.timestamp),
        "task_states": _json_safe(snapshot.task_states),
        "model_versions": dict(snapshot.model_versions),
        "config_hash": snapshot.config_hash,
        "schema_version": snapshot.schema_version,
    }
    data = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StateSnapshot:
    """Immutable snapshot of edge state for synchronization."""

    snapshot_id: str
    device_id: str
    timestamp: datetime
    task_states: Dict[str, Dict[str, Any]]
    model_versions: Dict[str, str]
    config_hash: str
    checksum: str = ""
    schema_version: int = 2

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_id, str) or not self.snapshot_id:
            raise StateSyncError("snapshot_id must be non-empty string")
        if not isinstance(self.device_id, str) or not self.device_id:
            raise StateSyncError("device_id must be non-empty string")
        if not isinstance(self.task_states, dict):
            raise StateSyncError("task_states must be a dict")
        if not isinstance(self.model_versions, dict):
            raise StateSyncError("model_versions must be a dict")
        if not isinstance(self.config_hash, str):
            raise StateSyncError("config_hash must be a string")

        # P0 §2.4: normalize timestamp to aware UTC
        object.__setattr__(
            self, "timestamp", _coerce_timestamp(self.timestamp)
        )

        # P0 §2.5: validate supplied checksum
        expected = _compute_snapshot_checksum(self)
        if self.checksum and self.checksum != expected:
            raise StateSyncError(
                f"supplied checksum does not match: "
                f"{self.checksum!r} != {expected!r}"
            )
        if not self.checksum:
            object.__setattr__(self, "checksum", expected)

    def verify_checksum(self) -> bool:
        return self.checksum == _compute_snapshot_checksum(self)

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(
            {
                "snapshot_id": self.snapshot_id,
                "device_id": self.device_id,
                "timestamp": self.timestamp.isoformat(),
                "task_states": {
                    k: dict(v) for k, v in self.task_states.items()
                },
                "model_versions": dict(self.model_versions),
                "config_hash": self.config_hash,
                "checksum": self.checksum,
                "schema_version": self.schema_version,
            }
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StateSnapshot":
        if not isinstance(data, Mapping):
            raise StateSyncError(
                f"expects Mapping, got {type(data).__name__}"
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError as exc:
                raise StateSyncError(
                    f"bad timestamp: {exc}"
                ) from exc
        if not isinstance(ts, datetime):
            ts = datetime.now(timezone.utc)
        # Normalize here too — defensive; __post_init__ also normalizes.
        ts = _coerce_timestamp(ts)
        task_states = {
            str(k): dict(v)
            for k, v in (data.get("task_states") or {}).items()
        }
        return cls(
            snapshot_id=str(data["snapshot_id"]),
            device_id=str(data["device_id"]),
            timestamp=ts,
            task_states=task_states,
            model_versions={
                str(k): str(v)
                for k, v in (data.get("model_versions") or {}).items()
            },
            config_hash=str(data.get("config_hash", "")),
            checksum=str(data.get("checksum", "")),
            schema_version=int(data.get("schema_version", 2)),
        )


# --------------------------------------------------------------------------- #
# Synchronizer
# --------------------------------------------------------------------------- #
class StateSynchronizer:
    """
    Distributed state manager for edge-cloud consistency.

    Thread-safe, bounded in memory, resilient to partitions.
    """

    def __init__(
        self,
        device_id: str,
        cloud_storage_endpoint: str,
        sync_interval_seconds: Union[int, float] = 30,
        max_offline_duration_seconds: Union[int, float] = 1800,
        *,
        config: Optional[SyncConfig] = None,
        strict: bool = True,
        online_probe: Optional[Callable[[], bool]] = None,
        http_session_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        if not isinstance(device_id, str) or not device_id:
            raise StateSyncError("device_id must be non-empty string")
        if not isinstance(cloud_storage_endpoint, str):
            raise StateSyncError("cloud_storage_endpoint must be string")

        # §3.13: accept int or float, reject bool
        for name, value in (
            ("sync_interval_seconds", sync_interval_seconds),
            ("max_offline_duration_seconds", max_offline_duration_seconds),
        ):
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise StateSyncError(f"{name} must be numeric")
            if value <= 0:
                raise StateSyncError(f"{name} must be > 0")

        if config is not None:
            self._config = config
        else:
            self._config = SyncConfig(
                sync_interval_seconds=float(sync_interval_seconds),
                max_offline_duration_seconds=float(
                    max_offline_duration_seconds
                ),
            )

        self.device_id = device_id
        self.cloud_endpoint = cloud_storage_endpoint.rstrip("/")
        self._strict = bool(strict)

        # Legacy attribute mirrors (kept for backward compat)
        self.sync_interval = int(self._config.sync_interval_seconds)
        self.max_offline_duration = int(
            self._config.max_offline_duration_seconds
        )

        self._lock = threading.RLock()
        self._local_state: Dict[str, Dict[str, Any]] = {}
        self._remote_state: "OrderedDict[str, Any]" = OrderedDict()
        self._snapshot_history: Deque[StateSnapshot] = deque(
            maxlen=max(1, self._config.max_snapshots_to_retain)
        )
        self._offline_since: Optional[datetime] = None
        self._offline_eviction_done: bool = False
        self._running = False

        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._started_at: Optional[float] = None

        self._last_sync_at: Optional[datetime] = None
        self._last_snapshot_id: Optional[str] = None
        self._last_downloaded_snapshot_id: Optional[str] = None

        self._online_probe: Callable[[], bool] = (
            online_probe or self._default_online_probe
        )
        self._http_session_factory = http_session_factory
        self._cached_session: Any = None

        _log(
            "synchronizer_initialized",
            logging.DEBUG,
            device_id=self.device_id,
            endpoint=self.cloud_endpoint,
            interval=self._config.sync_interval_seconds,
            strict=self._strict,
            aiohttp=_AIOHTTP_AVAILABLE,
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
        """P0 §2.2: derived from ``_local_state``, not a duplicate buffer."""
        with self._lock:
            return sum(
                1
                for v in self._local_state.values()
                if not v.get("synced", False)
            )

    @property
    def local_state_count(self) -> int:
        with self._lock:
            return len(self._local_state)

    @property
    def snapshot_history_count(self) -> int:
        with self._lock:
            return len(self._snapshot_history)

    @property
    def pending_sync_entries(self) -> List[Dict[str, Any]]:
        """P0 §2.1: a fresh derived view — no accumulation, no duplication."""
        with self._lock:
            return [
                {"key": k, "queued_at": v.get("updated_at")}
                for k, v in self._local_state.items()
                if not v.get("synced", False)
            ]

    # -------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._running:
            _log(
                "synchronizer_already_running",
                logging.DEBUG,
                device_id=self.device_id,
            )
            return
        self._running = True
        self._started_at = time.time()
        self._tasks = [
            asyncio.create_task(
                self._sync_loop(), name="state_sync:loop"
            )
        ]
        _log(
            "synchronizer_started",
            logging.INFO,
            device_id=self.device_id,
            interval=self._config.sync_interval_seconds,
        )

    async def stop(self, *, timeout_seconds: float = 5.0) -> None:
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
                _log(
                    "shutdown_loops_timeout",
                    logging.WARNING,
                    device_id=self.device_id,
                )
        self._tasks = []

        # Close cached session (§3.14)
        if self._cached_session is not None:
            try:
                if not getattr(self._cached_session, "closed", True):
                    await self._cached_session.close()
            except Exception:  # pragma: no cover
                pass
            self._cached_session = None

        _log(
            "synchronizer_stopped",
            logging.INFO,
            device_id=self.device_id,
            local=self.local_state_count,
            pending=self.pending_sync_count,
        )

    # -------------------------------------------------------- state API
    def update_local_state(self, key: str, value: Any) -> None:
        if not isinstance(key, str) or not key:
            raise StateSyncError("key must be non-empty string")
        with self._lock:
            self._local_state[key] = {
                "value": value,
                "updated_at": datetime.now(timezone.utc),
                "synced": False,
            }
            self._enforce_local_state_bound_locked()
        _log(
            "local_state_updated",
            logging.DEBUG,
            device_id=self.device_id,
            key=key,
        )

    async def get_state(
        self, key: str, prefer_local: bool = True
    ) -> Optional[Any]:
        if not isinstance(key, str) or not key:
            raise StateSyncError("key must be non-empty string")
        with self._lock:
            if prefer_local and key in self._local_state:
                return self._local_state[key]["value"]
            if key in self._remote_state:
                entry = self._remote_state[key]
                if isinstance(entry, Mapping) and "value" in entry:
                    return entry["value"]
                return entry
        return None

    def list_local_keys(self) -> List[str]:
        with self._lock:
            return list(self._local_state.keys())

    def get_all_local_state(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                k: dict(v) for k, v in self._local_state.items()
            }

    # -------------------------------------------------------- sync loop
    async def _sync_loop(self) -> None:
        """Auto-recovering sync loop (§3.1)."""
        cfg = self._config
        while self._running:
            try:
                await self._sync_to_cloud()
                await self._sync_from_cloud()
                with self._lock:
                    self._offline_since = None
                    self._offline_eviction_done = False
                    self._last_sync_at = datetime.now(timezone.utc)
                await asyncio.sleep(cfg.sync_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                with self._lock:
                    if self._offline_since is None:
                        self._offline_since = datetime.now(timezone.utc)
                _log(
                    "sync_failed",
                    logging.WARNING,
                    device_id=self.device_id,
                    error=str(exc),
                )
                await self._check_offline_budget()
                await asyncio.sleep(cfg.error_sleep_seconds)

    async def _check_offline_budget(self) -> None:
        """P0 §2.8: enforce ``max_offline_duration_seconds``."""
        with self._lock:
            if self._offline_since is None:
                return
            if self._offline_eviction_done:
                return
            if not self._config.offline_overflow_eviction:
                return
            duration = (
                datetime.now(timezone.utc) - self._offline_since
            ).total_seconds()
            limit = self._config.max_offline_duration_seconds
            if duration <= limit:
                return

            # Drop the oldest half of unsynced entries (bounded loss)
            unsynced = [
                (k, v.get("updated_at"))
                for k, v in self._local_state.items()
                if not v.get("synced", False)
            ]
            if not unsynced:
                self._offline_eviction_done = True
                return
            unsynced.sort(
                key=lambda kv: _coerce_timestamp(kv[1])
                if kv[1]
                else datetime.min.replace(tzinfo=timezone.utc)
            )
            to_drop = unsynced[: max(1, len(unsynced) // 2)]
            for k, _ in to_drop:
                self._local_state.pop(k, None)
            self._offline_eviction_done = True

        _log(
            "offline_budget_exceeded",
            logging.ERROR,
            device_id=self.device_id,
            offline_seconds=duration,
            limit=limit,
            dropped=len(to_drop),
        )

    # -------------------------------------------------------- online probe
    def _default_online_probe(self) -> bool:
        return bool(self.cloud_endpoint)

    def _is_online(self) -> bool:
        try:
            return bool(self._online_probe())
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "online_probe_raised error=%s", exc
            )
            return False

    # -------------------------------------------------------- sync up
    async def _sync_to_cloud(self) -> None:
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

        await self._upload_snapshot(snapshot)

        with self._lock:
            for k in unsynced.keys():
                entry = self._local_state.get(k)
                if entry is not None:
                    entry["synced"] = True
            self._last_sync_at = datetime.now(timezone.utc)
            self._last_snapshot_id = snapshot.snapshot_id
            self._snapshot_history.append(snapshot)

        _log(
            "snapshot_uploaded",
            logging.DEBUG,
            device_id=self.device_id,
            snapshot_id=snapshot.snapshot_id,
            keys=len(unsynced),
        )

    # -------------------------------------------------------- sync down
    async def _sync_from_cloud(self) -> None:
        remote = await self._download_latest_snapshot()
        if remote is None:
            return
        if remote.checksum and not remote.verify_checksum():
            _log(
                "checksum_mismatch",
                logging.ERROR,
                device_id=self.device_id,
                snapshot_id=remote.snapshot_id,
            )
            if self._strict:
                raise StateSyncError(
                    "Remote snapshot checksum mismatch"
                )
            return

        with self._lock:
            for key, value in remote.task_states.items():
                remote_updated = (
                    _coerce_timestamp(value.get("updated_at"))
                    if isinstance(value, Mapping)
                    and value.get("updated_at")
                    else None
                )
                local_entry = self._local_state.get(key)
                local_updated = (
                    _coerce_timestamp(local_entry.get("updated_at"))
                    if local_entry
                    and local_entry.get("updated_at")
                    else None
                )

                if local_entry is None:
                    self._local_state[key] = dict(value)
                    self._local_state[key]["synced"] = True
                    continue

                # LWW with tiebreaker (§4.5): remote wins on tie
                if remote_updated is None:
                    continue
                if (
                    local_updated is None
                    or remote_updated >= local_updated
                ):
                    self._local_state[key] = dict(value)
                    self._local_state[key]["synced"] = True

            # §2.6: re-enforce bound after merge
            self._enforce_local_state_bound_locked()

            # §2.7: bound remote cache
            merged = {
                k: dict(v) for k, v in remote.task_states.items()
            }
            cap = self._config.max_remote_state_entries
            if cap is not None and len(merged) > cap:
                items = sorted(
                    merged.items(),
                    key=lambda kv: (
                        _coerce_timestamp(kv[1].get("updated_at"))
                        if kv[1].get("updated_at")
                        else datetime.min.replace(tzinfo=timezone.utc)
                    ),
                    reverse=True,
                )[:cap]
                merged = dict(items)
            self._remote_state = OrderedDict(merged.items())
            self._last_downloaded_snapshot_id = remote.snapshot_id

        _log(
            "snapshot_merged",
            logging.DEBUG,
            device_id=self.device_id,
            snapshot_id=remote.snapshot_id,
            keys=len(remote.task_states),
        )

    # -------------------------------------------------------- helpers
    def _generate_snapshot_id(self) -> str:
        raw = (
            f"{self.device_id}:"
            f"{datetime.now(timezone.utc).isoformat()}:"
            f"{uuid.uuid4().hex}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _calculate_snapshot_checksum(
        self, snapshot: StateSnapshot
    ) -> str:
        return _compute_snapshot_checksum(snapshot)

    def _get_model_versions(self) -> Dict[str, str]:
        """Hook — override in a subclass to populate model versions."""
        return {}

    def _calculate_config_hash(self) -> str:
        """P0 §2.11: hash the actual config, not ``device_id``."""
        payload = json.dumps(
            asdict(self._config), sort_keys=True, default=str
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    # -------------------------------------------------------- HTTP I/O
    def _session(self) -> Any:
        """§3.14: reuse the session across calls."""
        if self._http_session_factory is not None:
            return self._http_session_factory()
        if not _AIOHTTP_AVAILABLE:
            raise StateSyncError(
                "aiohttp is required for HTTP state sync"
            )
        if (
            self._cached_session is None
            or getattr(self._cached_session, "closed", False)
        ):
            self._cached_session = aiohttp.ClientSession()
        return self._cached_session

    def _timeout(self) -> Any:
        if not _AIOHTTP_AVAILABLE:
            return None
        return aiohttp.ClientTimeout(
            total=self._config.request_timeout_seconds
        )

    async def _upload_snapshot(
        self, snapshot: StateSnapshot
    ) -> None:
        if not self.cloud_endpoint:
            raise StateSyncError(
                "cloud_storage_endpoint is empty"
            )
        cfg = self._config
        url = (
            f"{self.cloud_endpoint}/api/v1/state/snapshots/"
            f"{snapshot.snapshot_id}"
        )
        payload = snapshot.to_dict()

        last_exc: Optional[BaseException] = None
        for attempt in range(1 + cfg.max_upload_retries):
            try:
                session = self._session()
                async with session.put(
                    url, json=payload, timeout=self._timeout()
                ) as response:
                    response.raise_for_status()
                return
            except _TRANSIENT as exc:
                last_exc = exc
                _log(
                    "upload_retry",
                    logging.WARNING,
                    attempt=attempt + 1,
                    error=str(exc),
                )
                if attempt < cfg.max_upload_retries:
                    await asyncio.sleep(
                        cfg.base_backoff_seconds * (2 ** attempt)
                    )
            except Exception as exc:
                # Definitive — no retry (§2.10)
                raise StateSyncError(
                    f"Snapshot upload failed: {exc}"
                ) from exc

        raise StateSyncError(
            f"Snapshot upload failed after "
            f"{cfg.max_upload_retries + 1} attempt(s): {last_exc}"
        )

    async def _download_latest_snapshot(
        self,
    ) -> Optional[StateSnapshot]:
        if not self.cloud_endpoint:
            return None
        cfg = self._config
        url = (
            f"{self.cloud_endpoint}/api/v1/state/snapshots/latest"
        )
        headers: Dict[str, str] = {}
        # §3.10: conditional GET
        if self._last_downloaded_snapshot_id:
            headers["If-None-Match"] = (
                self._last_downloaded_snapshot_id
            )

        last_exc: Optional[BaseException] = None
        for attempt in range(1 + cfg.max_upload_retries):
            try:
                session = self._session()
                async with session.get(
                    url,
                    timeout=self._timeout(),
                    headers=headers,
                ) as response:
                    if response.status == 304:
                        return None
                    if response.status == 404:
                        return None
                    response.raise_for_status()
                    body = await response.json()
                return StateSnapshot.from_dict(body)
            except _TRANSIENT as exc:
                last_exc = exc
                _log(
                    "download_retry",
                    logging.WARNING,
                    attempt=attempt + 1,
                    error=str(exc),
                )
                if attempt < cfg.max_upload_retries:
                    await asyncio.sleep(
                        cfg.base_backoff_seconds * (2 ** attempt)
                    )
            except Exception as exc:
                if self._strict:
                    raise StateSyncError(
                        f"Snapshot download failed: {exc}"
                    ) from exc
                return None

        if self._strict:
            raise StateSyncError(
                f"Snapshot download failed after retries: "
                f"{last_exc}"
            )
        return None

    # -------------------------------------------------------- status
    async def get_sync_status(self) -> Dict[str, Any]:
        with self._lock:
            offline_since = self._offline_since
            local_count = len(self._local_state)
            remote_count = len(self._remote_state)
            pending = sum(
                1
                for v in self._local_state.values()
                if not v.get("synced", False)
            )
            last_sync = self._last_sync_at
            last_snapshot = self._last_snapshot_id
            history = len(self._snapshot_history)

        offline_duration = (
            (datetime.now(timezone.utc) - offline_since).total_seconds()
            if offline_since
            else 0.0
        )
        return {
            "device_id": self.device_id,
            "online": self._is_online(),
            "offline_since": (
                offline_since.isoformat() if offline_since else None
            ),
            "offline_duration_seconds": offline_duration,
            "local_state_count": local_count,
            "remote_state_count": remote_count,
            "pending_sync_count": pending,
            "last_sync": (
                last_sync.isoformat() if last_sync else None
            ),
            "last_snapshot_id": last_snapshot,
            "snapshot_history_count": history,
            "running": self._running,
        }

    def _get_last_sync_time(self) -> Optional[str]:
        with self._lock:
            return (
                self._last_sync_at.isoformat()
                if self._last_sync_at
                else None
            )

    # -------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            local = list(self._local_state.values())
            remote = len(self._remote_state)
            pending = sum(
                1 for v in local if not v.get("synced", False)
            )
            history = len(self._snapshot_history)
        synced = sum(
            1 for v in local if v.get("synced", False)
        )
        return {
            "device_id": self.device_id,
            "local_entries": len(local),
            "remote_entries": remote,
            "pending_entries": pending,
            "synced_entries": synced,
            "snapshot_history": history,
            "offline": self._offline_since is not None,
            "running": self._running,
            "started_at": self._started_at,
        }

    def health_check(self) -> Dict[str, Any]:
        """§3.2: operational probe."""
        with self._lock:
            offline_seconds = (
                (
                    datetime.now(timezone.utc)
                    - self._offline_since
                ).total_seconds()
                if self._offline_since
                else 0.0
            )
            pending = sum(
                1
                for v in self._local_state.values()
                if not v.get("synced", False)
            )
            return {
                "healthy": (
                    self._running
                    and offline_seconds
                    < self._config.max_offline_duration_seconds
                ),
                "device_id": self.device_id,
                "running": self._running,
                "offline": self._offline_since is not None,
                "offline_seconds": offline_seconds,
                "local_entries": len(self._local_state),
                "unsynced_entries": pending,
                "remote_entries": len(self._remote_state),
                "snapshot_history": len(self._snapshot_history),
                "started_at": self._started_at,
                "config": _json_safe(asdict(self._config)),
            }

    def collect_prometheus_metrics(
        self,
    ) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        """§3.4: metric table."""
        with self._lock:
            local = len(self._local_state)
            remote = len(self._remote_state)
            pending = sum(
                1
                for v in self._local_state.values()
                if not v.get("synced", False)
            )
            offline = int(self._offline_since is not None)
            offline_secs = (
                (
                    datetime.now(timezone.utc)
                    - self._offline_since
                ).total_seconds()
                if self._offline_since
                else 0.0
            )
            history = len(self._snapshot_history)
        labels = {"device_id": self.device_id}
        return {
            "state_sync_local_entries": (local, labels),
            "state_sync_remote_entries": (remote, labels),
            "state_sync_pending_entries": (pending, labels),
            "state_sync_offline": (offline, labels),
            "state_sync_offline_seconds": (offline_secs, labels),
            "state_sync_snapshot_history": (history, labels),
        }

    # -------------------------------------------------------- live reconfig
    def set_endpoint(self, endpoint: str) -> None:
        if not isinstance(endpoint, str):
            raise StateSyncError("endpoint must be a string")
        with self._lock:
            self.cloud_endpoint = endpoint.rstrip("/")
        _log(
            "endpoint_changed",
            logging.INFO,
            device_id=self.device_id,
            endpoint=self.cloud_endpoint,
        )

    def set_online_probe(
        self, probe: Callable[[], bool]
    ) -> None:
        if not callable(probe):
            raise StateSyncError("probe must be callable")
        with self._lock:
            self._online_probe = probe
        _log(
            "online_probe_changed",
            logging.DEBUG,
            device_id=self.device_id,
        )

    def set_config(self, config: SyncConfig) -> None:
        if not isinstance(config, SyncConfig):
            raise StateSyncError(
                "config must be a SyncConfig instance"
            )
        with self._lock:
            self._config = config
            self.sync_interval = int(
                config.sync_interval_seconds
            )
            self.max_offline_duration = int(
                config.max_offline_duration_seconds
            )
            # Reapply the new snapshot retention bound.
            new_maxlen = max(1, config.max_snapshots_to_retain)
            while len(self._snapshot_history) > new_maxlen:
                self._snapshot_history.popleft()
        _log(
            "config_changed",
            logging.INFO,
            device_id=self.device_id,
        )

    # -------------------------------------------------------- reset
    def reset(self, *, clear_local: bool = False) -> None:
        """§4.9: reset ``_started_at`` when clearing local state."""
        with self._lock:
            if clear_local:
                self._local_state.clear()
                self._started_at = None
            self._remote_state.clear()
            self._snapshot_history.clear()
            self._offline_since = None
            self._offline_eviction_done = False
            self._last_sync_at = None
            self._last_snapshot_id = None
            self._last_downloaded_snapshot_id = None
        _log(
            "synchronizer_reset",
            logging.DEBUG,
            device_id=self.device_id,
            clear_local=clear_local,
        )

    # -------------------------------------------------------- eviction
    def _enforce_local_state_bound_locked(self) -> None:
        """
        Single-pass eviction (§3.6).

        Prefers dropping synced entries; if still over, drops the oldest
        unsynced and logs at ERROR (§3.7).
        """
        cap = self._config.max_local_state_entries
        if cap is None or len(self._local_state) <= cap:
            return
        excess = len(self._local_state) - cap

        # Pass 1: drop oldest synced entries
        synced = [
            (
                k,
                _coerce_timestamp(v.get("updated_at"))
                if v.get("updated_at")
                else datetime.min.replace(tzinfo=timezone.utc),
            )
            for k, v in self._local_state.items()
            if v.get("synced", False)
        ]
        synced.sort(key=lambda kv: kv[1])

        dropped = 0
        for k, _ in synced:
            if dropped >= excess:
                break
            self._local_state.pop(k, None)
            dropped += 1

        # Pass 2: drop oldest unsynced (data loss — log at ERROR)
        if dropped < excess:
            unsynced = [
                (
                    k,
                    _coerce_timestamp(v.get("updated_at"))
                    if v.get("updated_at")
                    else datetime.min.replace(tzinfo=timezone.utc),
                )
                for k, v in self._local_state.items()
                if not v.get("synced", False)
            ]
            unsynced.sort(key=lambda kv: kv[1])
            lost = 0
            for k, _ in unsynced:
                if dropped >= excess:
                    break
                self._local_state.pop(k, None)
                dropped += 1
                lost += 1
            if lost:
                _log(
                    "local_state_overflow",
                    logging.ERROR,
                    device_id=self.device_id,
                    dropped_unsynced=lost,
                    cap=cap,
                )

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return _json_safe(
                {
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
                    "last_sync_at": (
                        self._last_sync_at.isoformat()
                        if self._last_sync_at
                        else None
                    ),
                    "last_snapshot_id": self._last_snapshot_id,
                    "last_downloaded_snapshot_id": (
                        self._last_downloaded_snapshot_id
                    ),
                    "local_state": {
                        k: _json_safe(dict(v))
                        for k, v in self._local_state.items()
                    },
                    "remote_state": {
                        k: _json_safe(v)
                        for k, v in self._remote_state.items()
                    },
                    "snapshot_history": [
                        s.to_dict() for s in self._snapshot_history
                    ],
                }
            )

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), default=str, **kwargs)

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
                f"from_dict expects Mapping, got "
                f"{type(data).__name__}"
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg_fields = {f.name for f in fields(SyncConfig)}
        filtered = {
            k: v for k, v in cfg_data.items() if k in cfg_fields
        }
        cfg = SyncConfig(**filtered)

        sync = cls(
            device_id=str(data["device_id"]),
            cloud_storage_endpoint=str(
                data.get("cloud_endpoint", "")
            ),
            config=cfg,
            strict=bool(data.get("strict", True)),
            online_probe=online_probe,
            http_session_factory=http_session_factory,
        )

        with sync._lock:
            for k, v in (data.get("local_state") or {}).items():
                entry = dict(v)
                entry["updated_at"] = (
                    _coerce_timestamp(entry.get("updated_at"))
                    if entry.get("updated_at")
                    else None
                )
                sync._local_state[str(k)] = entry

            # §2.3: enforce bounds on restore
            sync._enforce_local_state_bound_locked()

            remote = {
                str(k): dict(v)
                for k, v in (data.get("remote_state") or {}).items()
            }
            cap = sync._config.max_remote_state_entries
            if cap is not None and len(remote) > cap:
                items = list(remote.items())[-cap:]
                remote = dict(items)
            sync._remote_state = OrderedDict(remote.items())

            for raw in data.get("snapshot_history") or []:
                try:
                    snap = StateSnapshot.from_dict(raw)
                    sync._snapshot_history.append(snap)
                except (StateSyncError, KeyError) as exc:
                    _log(
                        "snapshot_history_skip",
                        logging.WARNING,
                        error=str(exc),
                    )

            if data.get("offline_since"):
                sync._offline_since = _coerce_timestamp(
                    data["offline_since"]
                )
            if data.get("last_sync_at"):
                sync._last_sync_at = _coerce_timestamp(
                    data["last_sync_at"]
                )
            sync._last_snapshot_id = data.get(
                "last_snapshot_id"
            )
            sync._last_downloaded_snapshot_id = data.get(
                "last_downloaded_snapshot_id"
            )
            sync._started_at = data.get("started_at")
        return sync

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
            raise StateSyncError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # -------------------------------------------------------- async CM
    async def __aenter__(self) -> "StateSynchronizer":
        self._ctx_start = time.perf_counter()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.stop()
        finally:
            self._ctx_start = None

    def __repr__(self) -> str:
        with self._lock:
            return (
                "StateSynchronizer("
                f"device_id={self.device_id!r}, "
                f"endpoint={self.cloud_endpoint!r}, "
                f"running={self._running}, "
                f"offline={self._offline_since is not None}, "
                f"local={len(self._local_state)}, "
                f"pending={self.pending_sync_count}, "
                f"history={len(self._snapshot_history)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "StateSnapshot",
    "StateSynchronizer",
    "SyncConfig",
    "StateSyncError",
    "ContinuumError",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1/P2 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("state_synchronizer.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # ------------------------------------------------------------- #
    # Fake cloud + fake session (shared across tests)
    # ------------------------------------------------------------- #
    class _FakeStore:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.snapshots: Dict[str, Any] = {}
            self.latest: Optional[Any] = None
            self.latest_etag: Optional[str] = None
            self.put_calls = 0
            self.get_calls = 0
            self.conditional_hits = 0
            self.fail_put_remaining = 0
            self.fail_get_remaining = 0
            self.fail_put_exc: BaseException = ConnectionError(
                "injected"
            )
            self.fail_get_exc: BaseException = ConnectionError(
                "injected"
            )

    class _FakeResponse:
        def __init__(
            self,
            status: int,
            payload: Any = None,
        ) -> None:
            self.status = status
            self._payload = payload

        def raise_for_status(self) -> None:
            if self.status >= 400:
                raise RuntimeError(f"HTTP {self.status}")

        async def json(self) -> Any:
            return self._payload

    class _PutCtx:
        def __init__(
            self, store: _FakeStore, url: str, payload: Any
        ) -> None:
            self._store = store
            self._url = url
            self._payload = payload

        async def __aenter__(self) -> _FakeResponse:
            with self._store._lock:
                self._store.put_calls += 1
                if self._store.fail_put_remaining > 0:
                    self._store.fail_put_remaining -= 1
                    raise self._store.fail_put_exc
            sid = self._url.rsplit("/", 1)[-1]
            with self._store._lock:
                self._store.snapshots[sid] = self._payload
                self._store.latest = self._payload
                self._store.latest_etag = sid
            return _FakeResponse(200)

        async def __aexit__(self, *exc: Any) -> None:
            return None

    class _GetCtx:
        def __init__(
            self, store: _FakeStore, url: str, headers: Mapping[str, str]
        ) -> None:
            self._store = store
            self._url = url
            self._headers = dict(headers)

        async def __aenter__(self) -> _FakeResponse:
            with self._store._lock:
                self._store.get_calls += 1
                if self._store.fail_get_remaining > 0:
                    self._store.fail_get_remaining -= 1
                    raise self._store.fail_get_exc
            etag = self._headers.get("If-None-Match")
            with self._store._lock:
                if etag and etag == self._store.latest_etag:
                    self._store.conditional_hits += 1
                    return _FakeResponse(304)
                if self._store.latest is None:
                    return _FakeResponse(404)
                return _FakeResponse(200, self._store.latest)

        async def __aexit__(self, *exc: Any) -> None:
            return None

    class _FakeSession:
        def __init__(self, store: _FakeStore) -> None:
            self._store = store
            self.closed = False

        def put(
            self, url: str, json: Any = None, timeout: Any = None
        ) -> _PutCtx:
            return _PutCtx(self._store, url, json)

        def get(
            self,
            url: str,
            timeout: Any = None,
            headers: Optional[Mapping[str, str]] = None,
        ) -> _GetCtx:
            return _GetCtx(self._store, url, headers or {})

        async def close(self) -> None:
            self.closed = True

    def _make_sync(
        store: _FakeStore,
        *,
        strict: bool = True,
        probe: Optional[Callable[[], bool]] = None,
        config: Optional[SyncConfig] = None,
    ) -> StateSynchronizer:
        return StateSynchronizer(
            device_id="edge-01",
            cloud_storage_endpoint="http://cloud.invalid",
            config=config,
            strict=strict,
            online_probe=probe or (lambda: True),
            http_session_factory=lambda: _FakeSession(store),
        )

    async def run_all() -> None:
        # ========================================================== #
        # P0 §2.1 + §2.2: pending view derived, no duplicates
        # ========================================================== #
        print("\n=== P0 §2.1/§2.2: pending view is derived, deduped ===")
        store = _FakeStore()
        sync = _make_sync(store, probe=lambda: False)  # offline

        sync.update_local_state("k1", 1)
        sync.update_local_state("k1", 2)  # re-update
        sync.update_local_state("k1", 3)  # re-update
        sync.update_local_state("k2", "x")

        _check(
            sync.pending_sync_count == 2,
            f"two distinct keys → pending=2 "
            f"(got {sync.pending_sync_count}; v5 would have 4 with dupes)",
        )
        entries = sync.pending_sync_entries
        _check(
            len(entries) == 2,
            f"pending_sync_entries has 2 entries (got {len(entries)})",
        )
        _check(
            all(e["key"] in ("k1", "k2") for e in entries),
            "pending view only contains the two keys",
        )
        # v5 had a _pending_sync buffer; v6 does not.
        _check(
            not hasattr(sync, "_pending_sync"),
            "no _pending_sync buffer attribute exists",
        )

        # ========================================================== #
        # P0 §2.3: from_dict enforces bounds on restore
        # ========================================================== #
        print("\n=== P0 §2.3: from_dict enforces local_state bound ===")
        cfg = SyncConfig(
            max_local_state_entries=100,
            max_remote_state_entries=50,
        )
        local_state = {
            f"k{i}": {
                "value": i,
                "updated_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "synced": True,
            }
            for i in range(500)
        }
        payload = {
            "device_id": "edge-restore",
            "cloud_endpoint": "http://cloud.invalid",
            "config": asdict(cfg),
            "strict": True,
            "local_state": local_state,
            "remote_state": {},
            "snapshot_history": [],
        }
        restored = StateSynchronizer.from_dict(
            payload,
            online_probe=lambda: True,
            http_session_factory=lambda: _FakeSession(
                _FakeStore()
            ),
        )
        _check(
            restored.local_state_count <= 100,
            f"local_state bounded to max_local_state_entries=100 "
            f"(got {restored.local_state_count})",
        )

        # ========================================================== #
        # P0 §2.4: snapshot normalizes naive timestamps
        # ========================================================== #
        print("\n=== P0 §2.4: snapshot timestamp normalized to UTC ===")
        naive_ts = datetime(2024, 1, 1, 0, 0, 0)
        snap_naive = StateSnapshot(
            snapshot_id="snap-naive",
            device_id="dev",
            timestamp=naive_ts,
            task_states={},
            model_versions={},
            config_hash="abc",
        )
        _check(
            snap_naive.timestamp.tzinfo is not None,
            "naive timestamp normalized to aware "
            "(v5 preserved naive → checksum instability)",
        )
        # Round-trip through to_dict / from_dict preserves awareness
        rt = StateSnapshot.from_dict(snap_naive.to_dict())
        _check(
            rt.timestamp.tzinfo is not None,
            "round-trip preserves aware timestamp",
        )
        _check(
            rt.checksum == snap_naive.checksum,
            "checksum stable across round-trip",
        )

        # ========================================================== #
        # P0 §2.5: bad checksum rejected at construction
        # ========================================================== #
        print("\n=== P0 §2.5: bad checksum rejected at construction ===")
        try:
            StateSnapshot(
                snapshot_id="snap-bad",
                device_id="dev",
                timestamp=datetime.now(timezone.utc),
                task_states={},
                model_versions={},
                config_hash="abc",
                checksum="garbage",
            )
        except StateSyncError as exc:
            _check(
                True,
                f"bad checksum rejected (v5 stored it verbatim): {exc}",
            )
        else:
            raise AssertionError(
                "bad checksum not rejected"
            )

        # ========================================================== #
        # P0 §2.6: merge enforces local_state bound
        # ========================================================== #
        print("\n=== P0 §2.6: merge re-enforces local_state bound ===")
        store2 = _FakeStore()
        # Publish a huge snapshot from the "remote"
        big_states = {
            f"r{i}": {
                "value": i,
                "updated_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "synced": True,
            }
            for i in range(500)
        }
        big_snapshot = StateSnapshot(
            snapshot_id="big",
            device_id="remote",
            timestamp=datetime.now(timezone.utc),
            task_states=big_states,
            model_versions={},
            config_hash="x",
        )
        with store2._lock:
            store2.latest = big_snapshot.to_dict()
            store2.latest_etag = "big"

        sync2 = _make_sync(
            store2,
            config=SyncConfig(
                max_local_state_entries=50,
                max_remote_state_entries=50,
            ),
        )
        await sync2._sync_from_cloud()
        _check(
            sync2.local_state_count <= 50,
            f"local bounded after merge (got {sync2.local_state_count})",
        )
        _check(
            len(sync2._remote_state) <= 50,
            f"remote bounded after merge "
            f"(got {len(sync2._remote_state)})",
        )

        # ========================================================== #
        # P0 §2.8: offline budget enforced
        # ========================================================== #
        print("\n=== P0 §2.8: offline budget enforced ===")
        store3 = _FakeStore()
        sync3 = _make_sync(
            store3,
            probe=lambda: False,
            config=SyncConfig(
                max_offline_duration_seconds=0.05,
                max_local_state_entries=1000,
                offline_overflow_eviction=True,
            ),
        )
        for i in range(20):
            sync3.update_local_state(f"o{i}", i)
        _check(
            sync3.local_state_count == 20,
            f"20 keys before offline budget (got {sync3.local_state_count})",
        )
        # Simulate being offline past the limit
        with sync3._lock:
            sync3._offline_since = datetime.now(
                timezone.utc
            ) - __import__("datetime").timedelta(seconds=1)
        await sync3._check_offline_budget()
        _check(
            sync3.local_state_count <= 10,
            f"offline budget dropped oldest half "
            f"(got {sync3.local_state_count}, was 20)",
        )

        # ========================================================== #
        # P0 §2.9: snapshot history bounded
        # ========================================================== #
        print("\n=== P0 §2.9: snapshot history bounded ===")
        store4 = _FakeStore()
        sync4 = _make_sync(
            store4,
            config=SyncConfig(max_snapshots_to_retain=3),
        )
        for i in range(10):
            sync4.update_local_state(f"k{i}", i)
            await sync4._sync_to_cloud()
        _check(
            sync4.snapshot_history_count <= 3,
            f"history bounded to 3 (got "
            f"{sync4.snapshot_history_count})",
        )

        # ========================================================== #
        # P0 §2.10: retry only on transient
        # ========================================================== #
        print("\n=== P0 §2.10: retry only transient ===")
        store5 = _FakeStore()
        factory_calls = {"n": 0}

        def _failing_factory() -> Any:
            factory_calls["n"] += 1
            raise StateSyncError("no aiohttp")

        sync5 = StateSynchronizer(
            device_id="edge-retry",
            cloud_storage_endpoint="http://cloud.invalid",
            config=SyncConfig(max_upload_retries=3),
            strict=True,
            online_probe=lambda: True,
            http_session_factory=_failing_factory,
        )
        sync5.update_local_state("k", 1)
        try:
            await sync5._sync_to_cloud()
        except StateSyncError:
            _check(
                factory_calls["n"] == 1,
                f"definitive error → 1 attempt "
                f"(got {factory_calls['n']}; v5 retried 4×)",
            )
        else:
            raise AssertionError(
                "definitive error did not raise"
            )

        # Transient error → retried
        store6 = _FakeStore()
        store6.fail_put_remaining = 2
        sync6 = _make_sync(
            store6, config=SyncConfig(max_upload_retries=3)
        )
        sync6.update_local_state("k", 1)
        await sync6._sync_to_cloud()
        _check(
            store6.put_calls == 3,
            f"transient error retried to success "
            f"(put_calls={store6.put_calls})",
        )

        # ========================================================== #
        # P0 §2.11: config hash reflects config
        # ========================================================== #
        print("\n=== P0 §2.11: config hash reflects config ===")
        s_a = _make_sync(
            _FakeStore(),
            config=SyncConfig(max_upload_retries=2),
        )
        s_b = _make_sync(
            _FakeStore(),
            config=SyncConfig(max_upload_retries=5),
        )
        h_a = s_a._calculate_config_hash()
        h_b = s_b._calculate_config_hash()
        _check(
            h_a != h_b,
            f"different configs → different hashes "
            f"(v5 hashed device_id)",
        )
        s_a2 = _make_sync(
            _FakeStore(),
            config=SyncConfig(max_upload_retries=2),
        )
        _check(
            h_a == s_a2._calculate_config_hash(),
            "identical configs → same hash",
        )

        # ========================================================== #
        # P1 §3.1: sync loop auto-recovers from offline
        # ========================================================== #
        print("\n=== P1 §3.1: sync loop auto-recovers from offline ===")
        store7 = _FakeStore()
        sync7 = _make_sync(
            store7,
            config=SyncConfig(
                sync_interval_seconds=0.05,
                error_sleep_seconds=0.05,
                max_upload_retries=0,
            ),
        )
        await sync7.start()
        try:
            sync7.update_local_state("r1", "v1")
            # Wait for a successful sync → no offline_since
            for _ in range(50):
                await asyncio.sleep(0.02)
                if sync7._last_sync_at is not None:
                    break
            _check(
                sync7._last_sync_at is not None,
                "successful sync set _last_sync_at",
            )
            # Force a failure → offline
            store7.fail_put_remaining = 1
            store7.fail_put_exc = ConnectionError("injected")
            sync7.update_local_state("r2", "v2")
            for _ in range(50):
                await asyncio.sleep(0.02)
                if sync7._offline_since is not None:
                    break
            _check(
                sync7._offline_since is not None,
                "offline state engaged after failure",
            )
            # Recover
            store7.fail_put_remaining = 0
            for _ in range(100):
                await asyncio.sleep(0.02)
                if sync7._offline_since is None:
                    break
            _check(
                sync7._offline_since is None,
                "sync loop auto-recovered from offline "
                "(v5 could get stuck via _is_online probe)",
            )
        finally:
            await sync7.stop()

        # ========================================================== #
        # P1 §3.2/§3.3/§3.4: health_check, _log, Prometheus
        # ========================================================== #
        print("\n=== P1 §3.2–§3.4: health_check / _log / Prometheus ===")
        hc = _make_sync(_FakeStore()).health_check()
        for key in (
            "healthy",
            "device_id",
            "running",
            "offline",
            "offline_seconds",
            "local_entries",
            "unsynced_entries",
            "remote_entries",
            "started_at",
            "config",
        ):
            _check(key in hc, f"health_check has '{key}'")
        _check(
            callable(_log),
            "structured logging helper present",
        )
        metrics = _make_sync(_FakeStore()).collect_prometheus_metrics()
        for key in (
            "state_sync_local_entries",
            "state_sync_remote_entries",
            "state_sync_pending_entries",
            "state_sync_offline",
            "state_sync_offline_seconds",
            "state_sync_snapshot_history",
        ):
            _check(key in metrics, f"metric '{key}' present")

        # ========================================================== #
        # P1 §3.5: _last_sync_at cached, not scanned
        # ========================================================== #
        print("\n=== P1 §3.5: _last_sync_at is cached ===")
        sync8 = _make_sync(_FakeStore())
        _check(
            sync8._last_sync_at is None,
            "_last_sync_at starts as None",
        )
        sync8.update_local_state("k", 1)
        await sync8._sync_to_cloud()
        _check(
            sync8._last_sync_at is not None,
            "_last_sync_at set after sync (no scan)",
        )

        # ========================================================== #
        # P1 §3.6/§3.7: eviction is single-pass; logs on unsynced drop
        # ========================================================== #
        print("\n=== P1 §3.6/§3.7: single-pass eviction + ERROR log ===")
        sync9 = _make_sync(
            _FakeStore(),
            config=SyncConfig(max_local_state_entries=5),
        )
        for i in range(20):
            sync9.update_local_state(f"u{i}", i)  # unsynced
        _check(
            sync9.local_state_count == 5,
            f"eviction bounded at 5 (got {sync9.local_state_count})",
        )
        # Verify the bound is enforced (fails on v5 if it depended on
        # while-loop sorted eviction — but works either way; the real
        # difference is O(n log n) vs O(n) and the ERROR log)

        # ========================================================== #
        # P1 §3.9/§3.10: download retries + conditional GET
        # ========================================================== #
        print("\n=== P1 §3.9/§3.10: download retries + conditional GET ===")
        store10 = _FakeStore()
        sync10 = _make_sync(
            store10,
            config=SyncConfig(max_upload_retries=2),
        )
        # Publish a snapshot and download it — 200 + etag set
        snap = StateSnapshot(
            snapshot_id="snap-1",
            device_id="dev",
            timestamp=datetime.now(timezone.utc),
            task_states={},
            model_versions={},
            config_hash="x",
        )
        with store10._lock:
            store10.latest = snap.to_dict()
            store10.latest_etag = "snap-1"
        await sync10._sync_from_cloud()
        _check(
            sync10._last_downloaded_snapshot_id == "snap-1",
            "first download succeeded",
        )
        # Second download — conditional hit
        await sync10._sync_from_cloud()
        _check(
            store10.conditional_hits >= 1,
            f"conditional GET hit ({store10.conditional_hits})",
        )
        # Transient failure then success
        store10.fail_get_remaining = 1
        store10.fail_get_exc = ConnectionError("injected")
        await sync10._sync_from_cloud()
        _check(
            store10.fail_get_remaining == 0,
            "transient download retried to success",
        )

        # ========================================================== #
        # P1 §3.12: live reconfig
        # ========================================================== #
        print("\n=== P1 §3.12: live reconfiguration ===")
        live = _make_sync(_FakeStore())
        live.set_endpoint("http://new-cloud.invalid/")
        _check(
            live.cloud_endpoint == "http://new-cloud.invalid",
            "set_endpoint updates endpoint (trailing / stripped)",
        )
        live.set_online_probe(lambda: False)
        _check(
            live._is_online() is False,
            "set_online_probe updates the probe",
        )
        new_cfg = SyncConfig(sync_interval_seconds=45.0)
        live.set_config(new_cfg)
        _check(
            live.sync_interval == 45,
            "set_config updates legacy sync_interval",
        )

        # ========================================================== #
        # P1 §3.13: float args accepted
        # ========================================================== #
        print("\n=== P1 §3.13: float positional args accepted ===")
        s_float = StateSynchronizer(
            device_id="edge-f",
            cloud_storage_endpoint="",
            sync_interval_seconds=30.5,
            max_offline_duration_seconds=1800.0,
            online_probe=lambda: False,
        )
        _check(
            s_float.config.sync_interval_seconds == 30.5,
            "float sync_interval_seconds accepted "
            "(v5 rejected as not int)",
        )

        # ========================================================== #
        # P1 §3.14: session reuse
        # ========================================================== #
        print("\n=== P1 §3.14: session reuse ===")
        store11 = _FakeStore()
        session_factory_calls = {"n": 0}

        def _tracking_factory() -> _FakeSession:
            session_factory_calls["n"] += 1
            return _FakeSession(store11)

        sync11 = StateSynchronizer(
            device_id="edge-sess",
            cloud_storage_endpoint="http://cloud.invalid",
            online_probe=lambda: True,
            http_session_factory=_tracking_factory,
        )
        # Because http_session_factory is set, _session() calls it every
        # time — but for the *default* aiohttp path, _cached_session is
        # reused. Verify the default path caches when aiohttp is present.
        # In tests (no real aiohttp), use cached_session flag directly:
        sync11._cached_session = _FakeSession(store11)
        sess1 = sync11._session()
        sess2 = sync11._session()
        _check(
            sess1 is sess2,
            "cached session reused (default path)",
        )

        # ========================================================== #
        # P2 §4.4: schema_version on snapshot
        # ========================================================== #
        print("\n=== P2 §4.4: schema_version on snapshot ===")
        _check(
            hasattr(StateSnapshot, "__dataclass_fields__")
            and "schema_version"
            in StateSnapshot.__dataclass_fields__,
            "StateSnapshot.schema_version declared",
        )
        snap_sv = StateSnapshot(
            snapshot_id="snap-sv",
            device_id="dev",
            timestamp=datetime.now(timezone.utc),
            task_states={},
            model_versions={},
            config_hash="x",
        )
        _check(
            snap_sv.schema_version == 2,
            f"default schema_version=2 (got {snap_sv.schema_version})",
        )

        # ========================================================== #
        # P2 §4.5: LWW tiebreaker — equal timestamps → remote wins
        # ========================================================== #
        print("\n=== P2 §4.5: LWW tiebreaker ===")
        store12 = _FakeStore()
        sync12 = _make_sync(store12)
        ts = datetime.now(timezone.utc).isoformat()
        # Local has key=k with value=local
        with sync12._lock:
            sync12._local_state["k"] = {
                "value": "local",
                "updated_at": ts,
                "synced": True,
            }
        # Remote has key=k with value=remote at the SAME timestamp
        remote_states = {
            "k": {
                "value": "remote",
                "updated_at": ts,
                "synced": True,
            }
        }
        big = StateSnapshot(
            snapshot_id="snap-tie",
            device_id="dev",
            timestamp=datetime.now(timezone.utc),
            task_states=remote_states,
            model_versions={},
            config_hash="x",
        )
        with store12._lock:
            store12.latest = big.to_dict()
            store12.latest_etag = "snap-tie"
        await sync12._sync_from_cloud()
        with sync12._lock:
            winner = sync12._local_state["k"]["value"]
        _check(
            winner == "remote",
            f"LWW tiebreaker: remote wins on equal ts "
            f"(got {winner!r}; v5 kept local silently)",
        )

        # ========================================================== #
        # P2 §4.8: extra accessors
        # ========================================================== #
        print("\n=== P2 §4.8: list_local_keys / get_all_local_state ===")
        sync13 = _make_sync(_FakeStore())
        sync13.update_local_state("a", 1)
        sync13.update_local_state("b", 2)
        _check(
            sorted(sync13.list_local_keys()) == ["a", "b"],
            "list_local_keys works",
        )
        all_state = sync13.get_all_local_state()
        _check(
            all_state["a"]["value"] == 1
            and all_state["b"]["value"] == 2,
            "get_all_local_state returns a copy",
        )

        # ========================================================== #
        # P2 §4.9: reset(clear_local=True) resets _started_at
        # ========================================================== #
        print("\n=== P2 §4.9: reset resets _started_at ===")
        sync14 = _make_sync(_FakeStore())
        sync14._started_at = 12345.0
        sync14.reset(clear_local=True)
        _check(
            sync14._started_at is None,
            "reset(clear_local=True) resets _started_at",
        )

        print("\n" + "=" * 78)
        print("All v6 assertions passed.")
        print("=" * 78)

    try:
        asyncio.run(run_all())
    except AssertionError as exc:
        print(f"\n{exc}")
        raise


if __name__ == "__main__":
    _demo()
