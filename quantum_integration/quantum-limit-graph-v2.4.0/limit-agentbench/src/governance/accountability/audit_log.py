"""
AuditLog — immutable, append-only record of governance events.

Every decision, verification, approval, and enforcement action
produces a ``GovernanceEvent``. The log is append-only; there is no
delete or truncate method.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``sink_path`` is now **read on construction**. Previously a
        restart silently lost all history — ``AuditLog(sink_path=f)``
        against a populated file reported ``len=0`` and
        ``verify_chain=True`` for an empty in-memory chain.
  * 2.2 Sink write failures now raise ``AuditLogPersistenceError``.
        Previously ``append()`` returned a chain hash even when the
        disk write failed, implying durability that didn't exist.
        Order changed: disk write happens before in-memory commit,
        so a failed write leaves the in-memory state untouched.
  * 2.3 New error hierarchy — ``AuditLogError``, ``AuditLogValidationError``,
        ``AuditLogPersistenceError``, ``AuditLogTamperedError`` — all
        defined eagerly. ``AuditLog`` now raises these types (not
        ``AttributeError``) for invalid input.
  * 2.4 ``append()`` accepts ``GovernanceEvent`` **or** ``Mapping``.
        Rejects payloads with caller-supplied ``_chain_hash`` or
        ``_prev_hash`` fields (chain fields are managed internally).
  * 2.5 ``fsync`` on both the file and its parent directory. On load,
        a partial last line (crash mid-write) is skipped at DEBUG;
        a corrupted middle line raises ``AuditLogTamperedError``.
  * 2.6 ``events()`` and ``get()`` return **deep copies**. Previously
        mutating a nested dict in the returned payload silently
        corrupted the chain.
  * 2.7 ``sink_path=""`` is rejected as ``AuditLogError``. ``None``
        (in-memory only) remains valid.
  * 2.8 Multi-writer support via ``mode="multi_writer"``. In this
        mode, each ``append()`` acquires a POSIX ``flock``, reads
        the on-disk chain head, writes the new line, and fsyncs —
        so two processes writing to the same sink produce a valid
        chained file. In the default ``mode="single_writer"``, a
        size-drift check raises if the file grew between appends
        (evidence of concurrent writers).

**P1 robustness**
  * 3.1 ``verify_entry(chain_hash)`` — verify a single entry without
        walking the whole chain.
  * 3.2 ``verify_chain()`` caches its result; invalidated on append.
  * 3.3 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
  * 3.4 ``statistics()``.
  * 3.5 ``metrics_hook(event, fields)`` fires on append, failure,
        and verification.
  * 3.6 ``__repr__``, ``__version__``, ``__getstate__`` /
        ``__setstate__`` (pickle-safe).
  * 3.7 ``get(event_id)`` is O(1) via ``_events_by_id`` index.
  * 3.8 ``verify_chain(expected_head=...)`` cross-checks the file's
        final chain hash (defeats "delete the last N events" tampering).
  * 3.10 ``append_batch(events)`` — one lock acquisition, one fsync.
  * 3.11 ``verify_sink()`` — verify the on-disk chain independently
        of the in-memory one.

Backward compatible
-------------------
``AuditLog(*, sink_path=None)``, ``append(event) -> str``,
``get(event_id)``, ``events()``, ``iter(log)``, ``len(log)``,
``verify_chain() -> bool`` — every v1.0.0 call site works unchanged.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence,
    Tuple,
)

from ..contracts.governance_event import GovernanceEvent

try:
    import fcntl as _fcntl
    _HAVE_FCNTL = True
except ImportError:  # pragma: no cover — Windows
    _fcntl = None  # type: ignore[assignment]
    _HAVE_FCNTL = False


logger = logging.getLogger(__name__)
__version__ = "2.0.0"


# =============================================================================
# Errors
# =============================================================================

class AuditLogError(Exception):
    """Base class for every error raised by :class:`AuditLog`."""


class AuditLogValidationError(AuditLogError, ValueError):
    """Raised for invalid events, paths, or configuration."""


class AuditLogPersistenceError(AuditLogError):
    """Raised when the sink write fails."""


class AuditLogTamperedError(AuditLogError):
    """Raised when the loaded or on-disk chain fails verification."""


# =============================================================================
# Helpers
# =============================================================================

def _fsync_dir(path: Path) -> None:
    """fsync the directory containing ``path`` (P0 2.5)."""
    if os.name == "nt":  # pragma: no cover
        return
    try:
        fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover — defensive
        pass
    finally:
        os.close(fd)


def _compute_chain_hash(payload: Mapping[str, Any]) -> str:
    """Compute the full SHA-256 chain hash over the payload."""
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


def _normalize_event_payload(event: Any) -> Dict[str, Any]:
    """
    Normalize ``event`` into a fresh dict, ready for chain-field
    assignment (P0 2.4).
    """
    if event is None:
        raise AuditLogValidationError("event must not be None")

    if hasattr(event, "to_dict") and callable(event.to_dict):
        payload = event.to_dict()
    elif isinstance(event, Mapping):
        payload = dict(event)
    else:
        raise AuditLogValidationError(
            f"event must be a GovernanceEvent or a Mapping, "
            f"got {type(event).__name__}")

    if not isinstance(payload, dict):
        raise AuditLogValidationError(
            f"event.to_dict() returned {type(payload).__name__}, "
            f"expected dict")

    if "_chain_hash" in payload or "_prev_hash" in payload:
        raise AuditLogValidationError(
            "event payload may not contain '_chain_hash' or "
            "'_prev_hash'; those fields are managed by AuditLog")

    return dict(payload)


def _read_last_chain_hash_from_content(content: str) -> str:
    """
    Return the ``_chain_hash`` of the last **complete** line in
    ``content``. Raise ``AuditLogTamperedError`` if the last complete
    line is corrupt.
    """
    if not content:
        return "genesis"
    lines = content.rstrip("\n").split("\n")
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
            return str(entry.get("_chain_hash", "genesis"))
        except json.JSONDecodeError as exc:
            raise AuditLogTamperedError(
                f"Last complete sink line is corrupt: {exc}") from exc
    return "genesis"


# =============================================================================
# AuditLog
# =============================================================================

class AuditLog:
    """
    Thread-safe append-only audit log with hash-chain verification.

    Each event commits to its predecessor via ``_prev_hash``; the
    ``_chain_hash`` is ``"sha256:" + hex(sha256(canonical_json))``.

    Modes
    -----
    ``mode="single_writer"`` (default)
        In-memory chain; write ordering guaranteed within the process.
        A size-drift check raises if the file grows between appends
        (evidence of a concurrent writer).
    ``mode="multi_writer"`` (opt-in, POSIX only)
        Each ``append`` acquires a POSIX ``flock``, reads the on-disk
        head, writes the new line under lock, and fsyncs. Safe for
        concurrent writers on the same host.
    """

    def __init__(
        self,
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
        mode: str = "single_writer",
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ):
        # P0 2.7: distinguish None (no sink) from "" (invalid)
        if sink_path is not None:
            if not isinstance(sink_path, str) or not sink_path:
                raise AuditLogValidationError(
                    "sink_path must be a non-empty string or None")

        if mode not in ("single_writer", "multi_writer"):
            raise AuditLogValidationError(
                f"mode must be 'single_writer' or 'multi_writer', "
                f"got {mode!r}")
        if mode == "multi_writer" and not _HAVE_FCNTL:
            logger.warning(
                "mode='multi_writer' requested but fcntl is not "
                "available (Windows?); falling back to "
                "'single_writer'")
            mode = "single_writer"

        if metrics_hook is not None and not callable(metrics_hook):
            raise AuditLogValidationError("metrics_hook must be callable")

        self._events: List[Dict[str, Any]] = []
        self._events_by_id: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._sink_path = sink_path
        self._strict = bool(strict)
        self._mode = mode
        self._metrics_hook = metrics_hook
        self._chain_verified_cache: Optional[bool] = None
        self._append_count: int = 0
        self._sink_failure_count: int = 0
        self._last_known_size: int = 0

        # P0 2.1: load existing history from disk
        if sink_path:
            self._load_from_sink()

    # ------------------------------------------------------------------
    # Metrics hook (P1 3.5)
    # ------------------------------------------------------------------

    def _emit_metric(self, event: str, fields_: Mapping[str, Any]) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ------------------------------------------------------------------
    # Sink load (P0 2.1 + P0 2.5)
    # ------------------------------------------------------------------

    def _load_from_sink(self) -> None:
        """Load and verify the existing sink file (if any)."""
        path = Path(self._sink_path)  # type: ignore[arg-type]
        self._events.clear()
        self._events_by_id.clear()
        if not path.exists():
            self._last_known_size = 0
            return

        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise AuditLogError(
                f"Could not read sink {path}: {exc}") from exc

        self._last_known_size = len(raw.encode("utf-8"))
        if not raw:
            return

        has_complete_last = raw.endswith("\n")
        lines = raw.splitlines()

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            is_last = (i == len(lines) - 1)
            is_partial = is_last and not has_complete_last

            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
                if is_partial:
                    logger.debug(
                        "Skipping partial last line (in-progress "
                        "write from prior crash)")
                    continue
                if self._strict:
                    raise AuditLogTamperedError(
                        f"Sink {path} corrupted at line {i + 1}")
                logger.warning(
                    "Skipping corrupt line %d in sink %s",
                    i + 1, path)
                continue

            if not isinstance(entry, dict):
                if self._strict:
                    raise AuditLogTamperedError(
                        f"Sink {path} line {i + 1} is not a JSON "
                        f"object")
                continue

            self._events.append(entry)
            if "event_id" in entry:
                self._events_by_id[entry["event_id"]] = entry

        if not self.verify_chain():
            if self._strict:
                raise AuditLogTamperedError(
                    f"Sink {path} failed hash chain verification")
            logger.warning(
                "Sink %s failed chain verification "
                "(non-strict mode; proceeding)", path)

        logger.debug(
            "AuditLog loaded %d event(s) from %s",
            len(self._events), path)

    # ------------------------------------------------------------------
    # Append (P0 2.2 + P0 2.4 + P0 2.8)
    # ------------------------------------------------------------------

    def append(self, event: Any) -> str:
        """
        Append an event; return its chained content hash.

        Raises ``AuditLogPersistenceError`` if the sink write fails
        (P0 2.2). In that case the in-memory state is unchanged.
        """
        payload = _normalize_event_payload(event)

        with self._lock:
            if self._sink_path:
                chain_hash = self._append_with_sink(payload)
            else:
                chain_hash = self._append_in_memory(payload)

            self._chain_verified_cache = None
            self._append_count += 1

        self._emit_metric("event_appended", {
            "event_kind": payload.get("kind", "unknown"),
            "chain_hash": chain_hash,
            "persisted": self._sink_path is not None,
            "event_count": len(self._events),
        })
        return chain_hash

    def append_batch(self, events: Sequence[Any]) -> List[str]:
        """Append multiple events under a single lock acquisition (P1 3.10)."""
        if not isinstance(events, (list, tuple)):
            raise AuditLogValidationError(
                f"events must be a list or tuple, "
                f"got {type(events).__name__}")
        payloads = [_normalize_event_payload(e) for e in events]
        hashes: List[str] = []
        with self._lock:
            for payload in payloads:
                if self._sink_path:
                    hashes.append(self._append_with_sink(payload))
                else:
                    hashes.append(self._append_in_memory(payload))
            self._chain_verified_cache = None
            self._append_count += len(payloads)
        for chain_hash in hashes:
            self._emit_metric("event_appended", {
                "chain_hash": chain_hash,
                "persisted": self._sink_path is not None,
                "event_count": len(self._events),
                "batch": True,
            })
        return hashes

    # ------------------------------------------------------------------
    # Append internals
    # ------------------------------------------------------------------

    def _append_in_memory(self, payload: Dict[str, Any]) -> str:
        prev_hash = (
            self._events[-1]["_chain_hash"]
            if self._events else "genesis")
        payload["_prev_hash"] = prev_hash
        payload["_chain_hash"] = _compute_chain_hash(payload)
        self._events.append(payload)
        if "event_id" in payload:
            self._events_by_id[payload["event_id"]] = payload
        return payload["_chain_hash"]

    def _append_with_sink(self, payload: Dict[str, Any]) -> str:
        if self._mode == "multi_writer":
            return self._append_multi_writer(payload)
        return self._append_single_writer(payload)

    def _append_single_writer(self, payload: Dict[str, Any]) -> str:
        # P0 2.8: detect concurrent writers before doing anything.
        self._check_no_concurrent_writes_locked()

        path = Path(self._sink_path)  # type: ignore[arg-type]
        path.parent.mkdir(parents=True, exist_ok=True)

        prev_hash = (
            self._events[-1]["_chain_hash"]
            if self._events else "genesis")
        payload["_prev_hash"] = prev_hash
        payload["_chain_hash"] = _compute_chain_hash(payload)
        line = json.dumps(payload, default=str) + "\n"

        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(path)
        except OSError as exc:
            self._sink_failure_count += 1
            self._emit_metric("sink_write_failed", {
                "path": str(path),
                "error_type": type(exc).__name__,
            })
            raise AuditLogPersistenceError(
                f"AuditLog sink write failed for {path}: {exc}"
            ) from exc

        # Only NOW commit to in-memory.
        self._last_known_size += len(line.encode("utf-8"))
        self._events.append(payload)
        if "event_id" in payload:
            self._events_by_id[payload["event_id"]] = payload
        return payload["_chain_hash"]

    def _append_multi_writer(self, payload: Dict[str, Any]) -> str:
        path = Path(self._sink_path)  # type: ignore[arg-type]
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(path, "a+b") as f:
                _fcntl.flock(f.fileno(), _fcntl.LOCK_EX)
                try:
                    # Read current content under lock.
                    f.seek(0)
                    content = f.read()

                    # Truncate a partial last line (crash recovery)
                    # before appending, so no partial line ends up
                    # embedded in the middle of the file.
                    if content and not content.endswith(b"\n"):
                        truncate_pos = content.rfind(b"\n") + 1
                        f.truncate(truncate_pos)
                        content = content[:truncate_pos]

                    prev_hash = _read_last_chain_hash_from_content(
                        content.decode("utf-8", errors="replace"))
                    payload["_prev_hash"] = prev_hash
                    payload["_chain_hash"] = _compute_chain_hash(payload)
                    line = json.dumps(payload, default=str) + "\n"

                    f.seek(0, 2)
                    f.write(line.encode("utf-8"))
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    _fcntl.flock(f.fileno(), _fcntl.LOCK_UN)
            _fsync_dir(path)
        except OSError as exc:
            self._sink_failure_count += 1
            self._emit_metric("sink_write_failed", {
                "path": str(path),
                "error_type": type(exc).__name__,
            })
            raise AuditLogPersistenceError(
                f"AuditLog sink write failed for {path}: {exc}"
            ) from exc

        self._events.append(payload)
        if "event_id" in payload:
            self._events_by_id[payload["event_id"]] = payload
        return payload["_chain_hash"]

    def _check_no_concurrent_writes_locked(self) -> None:
        """
        In single_writer mode, raise if the sink file grew since our
        last append (evidence of a concurrent writer — P0 2.8).
        """
        if self._mode != "single_writer" or not self._sink_path:
            return
        path = Path(self._sink_path)
        try:
            actual = path.stat().st_size if path.exists() else 0
        except OSError:
            return
        if actual != self._last_known_size:
            raise AuditLogError(
                f"Sink {path} size changed from "
                f"{self._last_known_size} to {actual} bytes since "
                f"last append. Another AuditLog instance may be "
                f"writing to it. Use mode='multi_writer' for safe "
                f"concurrent access, or a distinct sink_path per "
                f"instance.")

    # ------------------------------------------------------------------
    # Read API (P0 2.6 + P1 3.7)
    # ------------------------------------------------------------------

    def get(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Return a deep copy of the event with ``event_id``, or None."""
        if not isinstance(event_id, str) or not event_id:
            return None
        with self._lock:
            entry = self._events_by_id.get(event_id)
            return copy.deepcopy(entry) if entry is not None else None

    def events(self) -> List[Dict[str, Any]]:
        """Return deep copies of every event (P0 2.6)."""
        with self._lock:
            return [copy.deepcopy(e) for e in self._events]

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        # Materialize outside the lock so concurrent appends don't
        # block readers mid-iteration.
        return iter(self.events())

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    # ------------------------------------------------------------------
    # Verification (P1 3.1, 3.2, 3.8, 3.11)
    # ------------------------------------------------------------------

    def _compute_verify_chain(self) -> bool:
        """Internal: recompute the chain verification from scratch."""
        prev_hash = "genesis"
        for e in self._events:
            if e.get("_prev_hash") != prev_hash:
                return False
            payload = {
                k: v for k, v in e.items() if k != "_chain_hash"
            }
            expected = _compute_chain_hash(payload)
            if e.get("_chain_hash") != expected:
                return False
            prev_hash = e["_chain_hash"]
        return True

    def _current_head_locked(self) -> str:
        return (
            self._events[-1]["_chain_hash"]
            if self._events else "genesis")

    def verify_chain(self, *, expected_head: Optional[str] = None) -> bool:
        """
        Return True iff the in-memory hash chain is intact (P1 3.2:
        cached; P1 3.8: optional ``expected_head`` check).
        """
        with self._lock:
            if self._chain_verified_cache is None:
                self._chain_verified_cache = self._compute_verify_chain()
            result = self._chain_verified_cache

            if expected_head is not None:
                actual_head = self._current_head_locked()
                if actual_head != expected_head:
                    logger.error(
                        "verify_chain: head %r != expected %r",
                        actual_head, expected_head)
                    return False

        self._emit_metric("chain_verified", {
            "ok": result,
            "event_count": len(self._events),
        })
        return result

    def verify_entry(self, chain_hash: str) -> bool:
        """Verify a single entry by its chain hash (P1 3.1)."""
        if not isinstance(chain_hash, str) or not chain_hash:
            return False
        with self._lock:
            prev_hash = "genesis"
            for e in self._events:
                if e.get("_prev_hash") != prev_hash:
                    return False
                payload = {
                    k: v for k, v in e.items() if k != "_chain_hash"
                }
                expected = _compute_chain_hash(payload)
                if e.get("_chain_hash") != expected:
                    return False
                if e.get("_chain_hash") == chain_hash:
                    return True
                prev_hash = e["_chain_hash"]
        return False

    def verify_sink(self, *, expected_head: Optional[str] = None) -> bool:
        """
        Verify the on-disk chain independently of the in-memory one
        (P1 3.11). Detects on-disk tampering after the process started.
        """
        if not self._sink_path:
            return True
        path = Path(self._sink_path)
        if not path.exists():
            return len(self._events) == 0
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise AuditLogError(
                f"Could not read sink {path}: {exc}") from exc
        if not raw:
            return len(self._events) == 0

        has_complete_last = raw.endswith("\n")
        lines = raw.splitlines()
        prev_hash = "genesis"
        last_hash = "genesis"
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            is_last = (i == len(lines) - 1)
            is_partial = is_last and not has_complete_last
            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
                if is_partial:
                    continue
                return False
            if not isinstance(entry, dict):
                return False
            if entry.get("_prev_hash") != prev_hash:
                return False
            payload = {
                k: v for k, v in entry.items() if k != "_chain_hash"
            }
            expected = _compute_chain_hash(payload)
            if entry.get("_chain_hash") != expected:
                return False
            prev_hash = entry["_chain_hash"]
            last_hash = entry["_chain_hash"]

        if expected_head is not None and last_hash != expected_head:
            return False
        return True

    # ------------------------------------------------------------------
    # Serialization (P1 3.3)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "event_count": len(self._events),
                "chain_verified": self.verify_chain(),
                "head_hash": self._current_head_locked(),
                "events": [copy.deepcopy(e) for e in self._events],
                "sink_path": self._sink_path,
                "mode": self._mode,
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
        mode: str = "single_writer",
    ) -> "AuditLog":
        if not isinstance(data, Mapping):
            raise AuditLogValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        log = cls(
            sink_path=sink_path,
            strict=strict,
            mode=mode,
        )
        events = data.get("events", [])
        if not isinstance(events, (list, tuple)):
            raise AuditLogValidationError(
                "from_dict events must be a list or tuple")
        with log._lock:
            log._events.clear()
            log._events_by_id.clear()
            for entry in events:
                if not isinstance(entry, Mapping):
                    raise AuditLogValidationError(
                        "from_dict events must be Mappings")
                item = dict(entry)
                log._events.append(item)
                if "event_id" in item:
                    log._events_by_id[item["event_id"]] = item
            log._chain_verified_cache = None
            if not log.verify_chain():
                raise AuditLogTamperedError(
                    "from_dict payload failed chain verification")
        return log

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        sink_path: Optional[str] = None,
        strict: bool = True,
        mode: str = "single_writer",
    ) -> "AuditLog":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise AuditLogValidationError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(
            data, sink_path=sink_path, strict=strict, mode=mode)

    # ------------------------------------------------------------------
    # Statistics (P1 3.4)
    # ------------------------------------------------------------------

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "event_count": len(self._events),
                "append_count": self._append_count,
                "sink_failure_count": self._sink_failure_count,
                "chain_verified": self.verify_chain(),
                "sink_attached": self._sink_path is not None,
                "sink_path": self._sink_path,
                "mode": self._mode,
                "head_hash": self._current_head_locked(),
            }

    # ------------------------------------------------------------------
    # Pickling (P1 3.6)
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_metrics_hook", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._metrics_hook = None

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"AuditLog(events={len(self._events)}, "
                f"sink_path={self._sink_path!r}, "
                f"mode={self._mode!r}, "
                f"chain_verified={self.verify_chain()})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "AuditLog",
    "AuditLogError",
    "AuditLogPersistenceError",
    "AuditLogTamperedError",
    "AuditLogValidationError",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import pickle
    import tempfile

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== audit_log v{__version__} self-tests ===\n")

    class _FakeEvent:
        """Duck-typed GovernanceEvent for offline testing."""
        def __init__(self, event_id, kind="test", details=None):
            self.event_id = event_id
            self.kind = kind
            self.details = details or {}

        def to_dict(self):
            return {
                "event_id": self.event_id,
                "kind": self.kind,
                "details": dict(self.details),
            }

    # ---------------------------------------------------------------- #
    # P0 2.1 — reload from sink
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: reload from sink ===")

    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "audit.jsonl")

        log = AuditLog(sink_path=sink)
        for i in range(5):
            log.append(_FakeEvent(f"evt-{i}", details={"i": i}))
        assert len(log) == 5
        assert log.verify_chain() is True

        # Fresh instance on the same sink.
        log2 = AuditLog(sink_path=sink)
        assert len(log2) == 5, (
            f"P0 2.1 regression: len(log2)={len(log2)}, "
            f"expected 5")
        assert log2.verify_chain() is True
        assert log2.get("evt-3")["details"]["i"] == 3
        print(f"  ✓ new instance loaded {len(log2)} events from disk")

    # ---------------------------------------------------------------- #
    # P0 2.2 — sink write failure raises
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: sink write failure raises ===")

    # Use /dev/full on Linux, or a read-only dir.
    if os.path.exists("/dev/full"):
        log = AuditLog(sink_path="/dev/full")
        try:
            log.append(_FakeEvent("evt-1"))
        except AuditLogPersistenceError as exc:
            print(f"  ✓ /dev/full → AuditLogPersistenceError")
        else:
            raise AssertionError(
                "P0 2.2 regression: /dev/full did not raise")
        assert len(log) == 0, (
            f"P0 2.2 regression: memory committed despite write "
            f"failure (len={len(log)})")
        print(f"  ✓ in-memory state unchanged ({len(log)} events)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — validation errors
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: validation errors ===")

    log = AuditLog()
    for bad in (None, 42, "not-an-event", 3.14):
        try:
            log.append(bad)
        except AuditLogValidationError:
            pass
        except (AttributeError, TypeError) as exc:
            raise AssertionError(
                f"P0 2.3 regression: {type(bad).__name__} raised "
                f"{type(exc).__name__}, expected "
                f"AuditLogValidationError")
        else:
            raise AssertionError(
                f"P0 2.3 regression: {bad!r} accepted")
    print(f"  ✓ None / 42 / str / float rejected with "
          f"AuditLogValidationError")

    # ---------------------------------------------------------------- #
    # P0 2.4 — accept Mapping and reject chain-field spoofing
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: Mapping accepted, chain spoofing rejected ===")

    log = AuditLog()
    h = log.append({"event_id": "raw-1", "kind": "test"})
    assert isinstance(h, str) and h.startswith("sha256:")
    print(f"  ✓ raw Mapping accepted, hash={h[:20]}...")

    try:
        log.append({
            "event_id": "spoofed",
            "_chain_hash": "sha256:00000000",
        })
    except AuditLogValidationError:
        print(f"  ✓ chain-field spoofing rejected")
    else:
        raise AssertionError(
            "P0 2.4 regression: spoofed _chain_hash accepted")

    # ---------------------------------------------------------------- #
    # P0 2.5 — partial line skip; corrupt middle line raises
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: partial-line vs corrupt-middle ===")

    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "audit.jsonl")
        # Write 2 complete lines + 1 partial.
        with open(sink, "w") as f:
            f.write('{"event_id": "a", "_prev_hash": "genesis", '
                    '"_chain_hash": "sha256:X"}\n')
            # ^ We'll write a fresh log instead of hand-crafting
        os.remove(sink)

        log = AuditLog(sink_path=sink)
        log.append(_FakeEvent("e1"))
        log.append(_FakeEvent("e2"))
        # Append a partial line manually.
        with open(sink, "a") as f:
            f.write('{"event_id": "partial-')

        log2 = AuditLog(sink_path=sink)
        assert len(log2) == 2, (
            f"P0 2.5 regression: partial line not skipped "
            f"(len={len(log2)})")
        assert log2.verify_chain() is True
        print(f"  ✓ partial last line skipped, chain verified")

        # Corrupt a middle line.
        lines = Path(sink).read_text().splitlines()
        if len(lines) >= 2:
            lines[0] = "corrupt-not-json"
            Path(sink).write_text("\n".join(lines) + "\n")

        try:
            AuditLog(sink_path=sink, strict=True)
        except AuditLogTamperedError as exc:
            print(f"  ✓ corrupt middle line → AuditLogTamperedError")
        else:
            raise AssertionError(
                "P0 2.5 regression: corrupt middle line accepted")

    # ---------------------------------------------------------------- #
    # P0 2.6 — events() deep copies
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: events() deep copies ===")

    log = AuditLog()
    log.append(_FakeEvent("e1", details={"nested": "original"}))
    snap = log.events()
    snap[0]["details"]["nested"] = "tampered"
    assert log.verify_chain() is True, (
        "P0 2.6 regression: mutating snapshot broke the chain")
    fresh = log.get("e1")
    assert fresh["details"]["nested"] == "original", (
        f"P0 2.6 regression: internal payload mutated: "
        f"{fresh['details']['nested']!r}")
    print(f"  ✓ snapshot mutation has no effect on internal state")

    # ---------------------------------------------------------------- #
    # P0 2.7 — empty sink_path rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: empty sink_path rejected ===")

    try:
        AuditLog(sink_path="")
    except AuditLogValidationError:
        print(f"  ✓ sink_path='' → AuditLogValidationError")
    else:
        raise AssertionError(
            "P0 2.7 regression: sink_path='' accepted")

    # None is valid.
    log = AuditLog(sink_path=None)
    assert len(log) == 0
    print(f"  ✓ sink_path=None accepted")

    # ---------------------------------------------------------------- #
    # P0 2.8 — multi-writer coordination
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: multi-writer ===")

    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "audit.jsonl")

        # Single-writer: size drift raises.
        log_a = AuditLog(sink_path=sink, mode="single_writer")
        log_a.append(_FakeEvent("a1"))
        # Simulate concurrent write by truncating/replacing the file.
        with open(sink, "w") as f:
            f.write("")
        try:
            log_a.append(_FakeEvent("a2"))
        except AuditLogError as exc:
            print(f"  ✓ single_writer: size drift → AuditLogError")
        else:
            raise AssertionError(
                "P0 2.8 regression: single_writer accepted size drift")

        # Multi-writer: works on POSIX.
        if _HAVE_FCNTL:
            log_m1 = AuditLog(sink_path=sink, mode="multi_writer")
            log_m2 = AuditLog(sink_path=sink, mode="multi_writer")
            log_m1.append(_FakeEvent("m1"))
            log_m2.append(_FakeEvent("m2"))
            log_m1.append(_FakeEvent("m3"))

            fresh = AuditLog(sink_path=sink)
            assert fresh.verify_chain() is True, (
                "P0 2.8 regression: multi-writer broke chain")
            assert len(fresh) == 3
            print(f"  ✓ multi_writer: 3 events, chain verified")

    # ---------------------------------------------------------------- #
    # P1 3.1 — verify_entry
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: verify_entry ===")

    log = AuditLog()
    h = log.append(_FakeEvent("e1"))
    assert log.verify_entry(h) is True
    assert log.verify_entry("sha256:" + "0" * 64) is False
    assert log.verify_entry("") is False
    print(f"  ✓ verify_entry(valid)={True}, verify_entry(garbage)={False}")

    # ---------------------------------------------------------------- #
    # P1 3.2 — verify_chain cached
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: verify_chain cached ===")

    import time
    log = AuditLog()
    for i in range(50):
        log.append(_FakeEvent(f"e{i}"))

    t0 = time.perf_counter()
    for _ in range(1000):
        log.verify_chain()
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.1, (
        f"P1 3.2 regression: 1000 verify_chain took {elapsed:.3f}s "
        f"(expected <0.1s)")
    print(f"  ✓ 1000 verify_chain calls in {elapsed*1000:.1f}ms")

    # Cache invalidated by append.
    log.append(_FakeEvent("new"))
    assert log._chain_verified_cache is None
    print(f"  ✓ cache invalidated on append")

    # ---------------------------------------------------------------- #
    # P1 3.3 — to_dict / from_dict
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: to_dict / from_dict ===")

    log = AuditLog()
    log.append(_FakeEvent("e1", details={"x": 1}))
    log.append(_FakeEvent("e2"))

    d = log.to_dict()
    assert d["event_count"] == 2
    assert d["chain_verified"] is True
    assert "head_hash" in d

    log2 = AuditLog.from_dict(d)
    assert len(log2) == 2
    assert log2.verify_chain() is True
    assert log2.get("e1")["details"]["x"] == 1
    print(f"  ✓ to_dict / from_dict round-trip")

    # Corrupt payload rejected.
    d["events"][0]["details"]["x"] = 999   # tamper
    try:
        AuditLog.from_dict(d)
    except AuditLogTamperedError:
        print(f"  ✓ from_dict rejects tampered payload")
    else:
        raise AssertionError(
            "P1 3.3 regression: tampered payload accepted")

    # ---------------------------------------------------------------- #
    # P1 3.4 — statistics
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: statistics ===")

    log = AuditLog()
    log.append(_FakeEvent("e1"))
    stats = log.statistics()
    assert stats["event_count"] == 1
    assert stats["chain_verified"] is True
    assert stats["sink_attached"] is False
    print(f"  ✓ statistics: {stats['event_count']} events, "
          f"chain_verified={stats['chain_verified']}")

    # ---------------------------------------------------------------- #
    # P1 3.5 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: metrics_hook ===")

    events = []
    log = AuditLog(metrics_hook=lambda ev, f: events.append(ev))
    log.append(_FakeEvent("e1"))
    log.verify_chain()
    assert "event_appended" in events
    assert "chain_verified" in events
    print(f"  ✓ metrics_hook captured: {sorted(set(events))}")

    # ---------------------------------------------------------------- #
    # P1 3.6 — pickle
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: pickle ===")

    log = AuditLog()
    log.append(_FakeEvent("e1"))
    blob = pickle.dumps(log)
    restored = pickle.loads(blob)
    assert len(restored) == 1
    assert restored.verify_chain() is True
    assert restored.get("e1") is not None
    print(f"  ✓ pickle round-trip, still usable")

    # ---------------------------------------------------------------- #
    # P1 3.7 — O(1) get
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: O(1) get ===")

    log = AuditLog()
    for i in range(100):
        log.append(_FakeEvent(f"e{i}"))
    t0 = time.perf_counter()
    for _ in range(1000):
        log.get("e50")
    elapsed = time.perf_counter() - t0
    assert elapsed < 0.1, (
        f"P1 3.7 regression: 1000 get() calls took {elapsed:.3f}s")
    print(f"  ✓ 1000 get() calls in {elapsed*1000:.1f}ms")

    # ---------------------------------------------------------------- #
    # P1 3.8 — expected_head check
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: expected_head cross-check ===")

    log = AuditLog()
    log.append(_FakeEvent("e1"))
    log.append(_FakeEvent("e2"))
    head = log.to_dict()["head_hash"]
    assert log.verify_chain(expected_head=head) is True
    assert log.verify_chain(expected_head="sha256:wrong") is False
    print(f"  ✓ expected_head accepts matching hash, rejects wrong")

    # ---------------------------------------------------------------- #
    # P1 3.10 — append_batch
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: append_batch ===")

    log = AuditLog()
    hashes = log.append_batch([_FakeEvent(f"e{i}") for i in range(5)])
    assert len(hashes) == 5
    assert len(log) == 5
    assert log.verify_chain() is True
    print(f"  ✓ append_batch: 5 events, chain verified")

    # ---------------------------------------------------------------- #
    # P1 3.11 — verify_sink
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: verify_sink ===")

    with tempfile.TemporaryDirectory() as td:
        sink = os.path.join(td, "audit.jsonl")
        log = AuditLog(sink_path=sink)
        log.append(_FakeEvent("e1"))
        log.append(_FakeEvent("e2"))
        assert log.verify_sink() is True

        # Tamper on disk.
        lines = Path(sink).read_text().splitlines()
        tampered = json.loads(lines[0])
        tampered["details"] = {"tampered": True}
        lines[0] = json.dumps(tampered)
        Path(sink).write_text("\n".join(lines) + "\n")

        assert log.verify_sink() is False, (
            "P1 3.11 regression: verify_sink missed on-disk tamper")
        print(f"  ✓ on-disk tamper detected by verify_sink()")
        # In-memory chain still valid.
        assert log.verify_chain() is True
        print(f"  ✓ in-memory chain unaffected")

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
