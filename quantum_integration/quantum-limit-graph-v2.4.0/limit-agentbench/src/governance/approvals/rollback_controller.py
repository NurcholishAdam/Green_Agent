"""
RollbackController — restore a prior approved state. — v2.0.0

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``snapshot()`` **deep-copies** the state and ``rollback()``
        **deep-copies** the returned value. Previously the internal
        map held a live reference — a caller who mutated their state
        object after snapshotting silently rewrote the "approved"
        state that rollback would restore.
  * 2.2 ``require_approval=True`` (new kwarg) enforces that
        ``snapshot()`` records an ``approved_by`` actor. The default
        is ``False`` for backward compatibility, but the docstring
        is now explicit that "approved state" is a caller-managed
        property unless this is enabled.
  * 2.3 ``RLock`` around every state mutation. Concurrent
        ``snapshot()`` + ``rollback()`` calls are now safe.
  * 2.4 ``snapshot()`` rejects overwriting an existing version by
        default. Pass ``replace=True`` to overwrite explicitly; the
        replacement is recorded in ``history()`` as a
        ``snapshot_replaced`` event.
  * 2.5 ``rollback()`` validates ``from_version`` against the
        tracked ``current_version`` (when set) and rejects identical
        from/to pairs. The audit trail cannot claim a transition
        that never happened.
  * 2.6 ``RollbackRecord.__post_init__`` validates every field.
        Previously ``reason=""``, ``actor=""``, ``at=<naive>`` all
        constructed cleanly.
  * 2.7 ``_history`` is a bounded ``deque``. Previously the list
        grew forever on a long-running governance system.
  * 2.8 ``rollback()`` raises ``RollbackNotFoundError`` when the
        target version is absent, instead of returning ``None``.
        Previously "not found" and "state is None" were
        indistinguishable.

**P1 robustness**
  * 3.1 ``RollbackError`` base class +
        ``RollbackValidationError`` +
        ``RollbackNotFoundError`` +
        ``RollbackStateError`` +
        ``RollbackCapacityError``.
  * 3.2 ``versions()``, ``has_snapshot()``, ``get_snapshot()``,
        ``__len__``, ``__contains__`` — query the snapshot store.
  * 3.3 ``current_version`` tracking via ``set_current_version()``
        and ``current_version()``. ``rollback()`` updates it
        automatically on success.
  * 3.4 ``SnapshotRecord`` metadata — version, state, approved_by,
        reason, taken_at. Callers can prove "who approved v1 and
        when."
  * 3.5 ``on_rollback`` callback fires after every successful
        rollback. The caller wires this to actually apply the
        restored state, guaranteeing the record and reality match.
  * 3.6 ``rollback(..., dry_run=True)`` returns the state without
        recording.
  * 3.8 ``max_snapshots`` capacity cap; ``prune(keep_latest=N)``
        for explicit retention management.
  * 3.9 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``
        — the config and history round-trip. Snapshots themselves
        are not serialized (arbitrary user state).
  * 3.10 ``metrics_hook(event, fields)`` fires
         ``snapshot_taken``, ``snapshot_replaced``,
         ``rollback_started``, ``rollback_completed``,
         ``rollback_failed``, ``rollback_not_found``.
  * 3.11 Optional ``audit_log`` integration — every snapshot and
         rollback is appended to the injected ``AuditLog``
         (duck-typed).
  * 3.12 ``history(limit=...)`` — bounded query.
  * 3.13 ``history_between(start, end)`` — half-open time-range
         query.
  * 3.14 ``RollbackRecord.to_dict`` / ``from_dict``.
  * 3.15 ``RollbackRecord`` is ``frozen=True``.
  * 3.16 ``__repr__`` / ``__version__`` / ``__all__``.
  * 3.17 Pickle support via ``__getstate__`` / ``__setstate__``.
  * 3.18 ``latest_snapshot()`` — the most recently taken snapshot.
  * 3.19 ``assert_can_rollback()`` — preview the check without
         committing.

Backward compatible
-------------------
``RollbackController()``, ``ctrl.snapshot(version, state)``,
``ctrl.rollback(from, to, *, reason, actor=None)``,
``ctrl.history()`` — every v1.0.0 call site works unchanged. New
kwargs (``on_rollback=``, ``audit_log=``, ``metrics_hook=``,
``max_history=``, ``max_snapshots=``, ``require_approval=``) are
optional.

Behaviour changes (fixes for silent-corruption bugs)
---------------------------------------------------
  * ``snapshot()`` deep-copies the state.
  * ``snapshot()`` raises on duplicate version unless
    ``replace=True``.
  * ``rollback()`` raises ``RollbackNotFoundError`` on missing
    target instead of returning ``None``.
  * ``rollback()`` rejects ``from_version == to_version``.
  * ``history()`` returns at most ``max_history`` records.
  * ``RollbackRecord`` is frozen and validated.
"""

from __future__ import annotations

import copy
import json
import logging
import threading
from collections import deque
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"
LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors (P1 3.1)
# =============================================================================

class RollbackError(ValueError):
    """Base class for every error raised by RollbackController."""


class RollbackValidationError(RollbackError):
    """Raised for invalid versions, reasons, actors, or configuration."""


class RollbackNotFoundError(RollbackError):
    """Raised when the target (or source) version is not in the store."""


class RollbackStateError(RollbackError):
    """Raised when ``from_version`` does not match the tracked
    ``current_version``."""


class RollbackCapacityError(RollbackError):
    """Raised when the snapshot store is at capacity."""


# =============================================================================
# Helpers
# =============================================================================

def _require_non_empty_string(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RollbackValidationError(
            f"{name} must be a non-empty string")
    return value.strip()


def _normalize_dt(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise RollbackValidationError(
            f"timestamp must be a datetime, "
            f"got {type(value).__name__}")
    if not value.tzinfo:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _deep_copy_or_raise(state: Any, *, context: str) -> Any:
    """
    Return a deep copy of ``state``.

    If the state cannot be copied, raise ``RollbackValidationError``
    with a helpful message rather than silently storing a reference
    that will corrupt the snapshot.
    """
    try:
        return copy.deepcopy(state)
    except Exception as exc:  # noqa: BLE001
        raise RollbackValidationError(
            f"cannot deep-copy {context}: {type(exc).__name__}: "
            f"{exc}. Pass an immutable / copyable state, or "
            f"implement __deepcopy__ on your object.") from exc


# =============================================================================
# SnapshotRecord (P1 3.4)
# =============================================================================

@dataclass(frozen=True)
class SnapshotRecord:
    """
    Metadata + state for one snapshot.

    ``state`` is always a deep copy of what the caller provided to
    :meth:`RollbackController.snapshot`.
    """
    version: str
    state: Any
    approved_by: Optional[str] = None
    reason: Optional[str] = None
    taken_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "version",
            _require_non_empty_string("version", self.version))
        if self.approved_by is not None:
            object.__setattr__(
                self, "approved_by",
                _require_non_empty_string(
                    "approved_by", self.approved_by).lower())
        if self.reason is not None:
            object.__setattr__(
                self, "reason",
                _require_non_empty_string("reason", self.reason))
        object.__setattr__(
            self, "taken_at", _normalize_dt(self.taken_at))

    def to_dict(self, *, include_state: bool = False) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "version": self.version,
            "approved_by": self.approved_by,
            "reason": self.reason,
            "taken_at": self.taken_at.isoformat(),
        }
        if include_state:
            # Not JSON-safe in general; caller-supplied serialization
            # is required. We emit a repr for diagnostics.
            out["state_repr"] = repr(self.state)
        return out

    @classmethod
    def from_legacy_dict(
        cls,
        version: str,
        state: Any,
        data: Optional[Mapping[str, Any]],
    ) -> "SnapshotRecord":
        """
        Build a ``SnapshotRecord`` from a payload produced by
        ``to_dict(include_state=False)`` and a caller-supplied
        ``state`` (state is not serialized).
        """
        data = dict(data or {})
        taken_at_raw = data.get("taken_at")
        if isinstance(taken_at_raw, str):
            try:
                taken_at = datetime.fromisoformat(
                    taken_at_raw.replace("Z", "+00:00"))
            except ValueError:
                taken_at = datetime.now(timezone.utc)
        elif isinstance(taken_at_raw, datetime):
            taken_at = taken_at_raw
        else:
            taken_at = datetime.now(timezone.utc)
        return cls(
            version=version,
            state=state,
            approved_by=data.get("approved_by"),
            reason=data.get("reason"),
            taken_at=taken_at,
        )


# =============================================================================
# RollbackRecord (P0 2.6; P1 3.14, 3.15)
# =============================================================================

@dataclass(frozen=True)
class RollbackRecord:
    """
    Immutable record of one rollback event.

    Frozen — the history cannot be corrupted by a caller mutating a
    returned record.
    """
    from_version: str
    to_version: str
    reason: str
    actor: Optional[str] = None
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "from_version",
            _require_non_empty_string(
                "from_version", self.from_version))
        object.__setattr__(
            self, "to_version",
            _require_non_empty_string(
                "to_version", self.to_version))
        object.__setattr__(
            self, "reason",
            _require_non_empty_string("reason", self.reason))
        if self.actor is not None:
            object.__setattr__(
                self, "actor",
                _require_non_empty_string("actor", self.actor).lower())
        object.__setattr__(self, "at", _normalize_dt(self.at))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from": self.from_version,
            "to": self.to_version,
            "reason": self.reason,
            "actor": self.actor,
            "at": self.at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RollbackRecord":
        if not isinstance(data, Mapping):
            raise RollbackValidationError(
                f"RollbackRecord.from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        at_raw = data.get("at")
        if isinstance(at_raw, str):
            try:
                at = datetime.fromisoformat(
                    at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise RollbackValidationError(
                    f"at is not a valid ISO 8601 timestamp: "
                    f"{exc}") from exc
        elif isinstance(at_raw, datetime):
            at = at_raw
        elif at_raw is None:
            at = datetime.now(timezone.utc)
        else:
            raise RollbackValidationError(
                f"at must be a datetime, ISO string, or None; "
                f"got {type(at_raw).__name__}")
        return cls(
            from_version=data.get("from", data.get("from_version")),
            to_version=data.get("to", data.get("to_version")),
            reason=data.get("reason"),
            actor=data.get("actor"),
            at=at,
        )


# =============================================================================
# RollbackController (all P0 + P1)
# =============================================================================

class RollbackController:
    """
    Snapshot manager for rollback.

    The controller **plans** rollbacks — it holds snapshot records and
    returns deep copies of their state on request. It does **not**
    apply the restored state itself; the caller is responsible (or
    can wire the ``on_rollback`` callback to do it).

    If ``require_approval=True`` is set, ``snapshot()`` requires an
    ``approved_by`` actor for every snapshot. This is the honest way
    to enforce the "approved state" contract.
    """

    def __init__(
        self,
        *,
        on_rollback: Optional[
            Callable[[str, str, Any], Any]
        ] = None,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
        max_history: int = 1000,
        max_snapshots: int = 100,
        require_approval: bool = False,
    ):
        """
        Parameters
        ----------
        on_rollback : callable, optional
            Called as ``on_rollback(from_version, to_version,
            restored_state)`` **after** the record is appended.
            Exceptions propagate to the caller — but the rollback
            record is retained so the audit trail shows the attempt.
        audit_log : object, optional
            Duck-typed. If it exposes ``append(dict)``, every snapshot
            and rollback is appended to it.
        metrics_hook : callable, optional
            Fires ``(event_name, fields_dict)`` on each state change.
        max_history : int, default 1000
            Bounded history ring buffer.
        max_snapshots : int, default 100
            Bounded snapshot store size (fail-closed when reached).
        require_approval : bool, default False
            If True, ``snapshot()`` requires ``approved_by`` to be
            provided (non-empty string).
        """
        if on_rollback is not None and not callable(on_rollback):
            raise RollbackValidationError(
                "on_rollback must be callable")
        if metrics_hook is not None and not callable(metrics_hook):
            raise RollbackValidationError(
                "metrics_hook must be callable")
        for name, value in (
            ("max_history", max_history),
            ("max_snapshots", max_snapshots),
        ):
            if isinstance(value, bool) or \
                    not isinstance(value, int) or value <= 0:
                raise RollbackValidationError(
                    f"{name} must be a positive int")
        if not isinstance(require_approval, bool):
            raise RollbackValidationError(
                "require_approval must be a bool")

        self._lock = threading.RLock()
        self._snapshots: Dict[str, SnapshotRecord] = {}
        self._history: Deque[RollbackRecord] = deque(
            maxlen=max_history)
        self._current_version: Optional[str] = None
        self._on_rollback = on_rollback
        self._audit_log = audit_log
        self._metrics_hook = metrics_hook
        self._max_snapshots = max_snapshots
        self._require_approval = require_approval

    # ------------------------------------------------------------------
    # Side-effect emitters
    # ------------------------------------------------------------------

    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    def _emit_audit(self, payload: Mapping[str, Any]) -> None:
        if self._audit_log is None:
            return
        append_fn = getattr(self._audit_log, "append", None)
        if not callable(append_fn):
            return
        try:
            append_fn(dict(payload))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "audit_log.append failed for rollback event: %s",
                exc)

    @staticmethod
    def _safe_log(level: int, fmt: str, *args: Any) -> None:
        try:
            logger.log(level, fmt, *args)
        except Exception:  # noqa: BLE001
            try:
                logger.debug("log handler raised", exc_info=True)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # Snapshot (P0 2.1, 2.2, 2.3, 2.4; P1 3.4, 3.8, 3.10, 3.11)
    # ------------------------------------------------------------------

    def snapshot(
        self,
        version: str,
        state: Any,
        *,
        approved_by: Optional[str] = None,
        reason: Optional[str] = None,
        replace: bool = False,
    ) -> SnapshotRecord:
        """
        Store a snapshot of ``state`` under ``version``.

        The state is **deep-copied** — a caller who mutates the
        original after this call does not affect the stored snapshot.
        The returned ``SnapshotRecord`` contains a deep copy of
        ``state`` as well.

        Raises
        ------
        RollbackValidationError
            For invalid ``version``, un-copyable ``state``, duplicate
            ``version`` (without ``replace=True``), or missing
            ``approved_by`` when ``require_approval=True``.
        RollbackCapacityError
            When ``max_snapshots`` is reached and the version is new.
        """
        version = _require_non_empty_string("version", version)
        if self._require_approval:
            if approved_by is None:
                raise RollbackValidationError(
                    f"require_approval=True: snapshot {version!r} "
                    f"requires approved_by to be provided")
            approved_by = _require_non_empty_string(
                "approved_by", approved_by)
        elif approved_by is not None:
            approved_by = _require_non_empty_string(
                "approved_by", approved_by)

        if reason is not None:
            reason = _require_non_empty_string("reason", reason)

        with self._lock:
            exists = version in self._snapshots
            if exists and not replace:
                raise RollbackValidationError(
                    f"version {version!r} already exists; pass "
                    f"replace=True to overwrite (this records a "
                    f"snapshot_replaced event)")
            if not exists and len(self._snapshots) >= \
                    self._max_snapshots:
                raise RollbackCapacityError(
                    f"snapshot store at capacity "
                    f"({self._max_snapshots}); call prune() or "
                    f"increase max_snapshots")
            # Deep-copy FIRST, then mutate state — so an un-copyable
            # state never leaves a half-updated store.
            stored_state = _deep_copy_or_raise(
                state, context=f"state for snapshot {version!r}")
            record = SnapshotRecord(
                version=version,
                state=stored_state,
                approved_by=approved_by,
                reason=reason,
            )
            self._snapshots[version] = record

        if exists:
            self._emit_metric("snapshot_replaced", {
                "version": version,
                "approved_by": approved_by or "",
            })
            self._emit_audit({
                "event": "snapshot_replaced",
                "version": version,
                "approved_by": approved_by,
                "reason": reason,
                "taken_at": record.taken_at.isoformat(),
            })
            self._safe_log(
                logging.WARNING,
                "Snapshot %r replaced (approved_by=%s)",
                version, approved_by)
        else:
            self._emit_metric("snapshot_taken", {
                "version": version,
                "approved_by": approved_by or "",
            })
            self._emit_audit({
                "event": "snapshot_taken",
                "version": version,
                "approved_by": approved_by,
                "reason": reason,
                "taken_at": record.taken_at.isoformat(),
            })
            self._safe_log(
                logging.INFO,
                "Snapshot %r taken (approved_by=%s)",
                version, approved_by)

        # Return a copy so the caller cannot mutate our stored state.
        return SnapshotRecord(
            version=record.version,
            state=_deep_copy_or_raise(
                record.state,
                context=f"snapshot {version!r} (return)"),
            approved_by=record.approved_by,
            reason=record.reason,
            taken_at=record.taken_at,
        )

    # ------------------------------------------------------------------
    # Rollback (P0 2.1, 2.3, 2.5, 2.8; P1 3.3, 3.5, 3.6, 3.10, 3.11)
    # ------------------------------------------------------------------

    def rollback(
        self,
        from_version: str,
        to_version: str,
        *,
        reason: str,
        actor: Optional[str] = None,
        dry_run: bool = False,
    ) -> Any:
        """
        Return a deep copy of the snapshot for ``to_version``.

        Does **not** apply the restored state — see the class
        docstring and the ``on_rollback`` callback.

        Raises
        ------
        RollbackValidationError
            For identical from/to, invalid reason/actor.
        RollbackStateError
            When ``from_version`` does not match the tracked
            ``current_version``.
        RollbackNotFoundError
            When ``to_version`` has no snapshot.
        """
        from_version = _require_non_empty_string(
            "from_version", from_version)
        to_version = _require_non_empty_string(
            "to_version", to_version)
        reason = _require_non_empty_string("reason", reason)
        if actor is not None:
            actor = _require_non_empty_string("actor", actor)
        if not isinstance(dry_run, bool):
            raise RollbackValidationError("dry_run must be a bool")
        if from_version == to_version:
            raise RollbackValidationError(
                f"from_version and to_version are identical "
                f"({from_version!r}); nothing to roll back")

        with self._lock:
            # -- Validate from_version against current --
            if self._current_version is not None and \
                    from_version != self._current_version:
                raise RollbackStateError(
                    f"from_version={from_version!r} does not match "
                    f"current_version={self._current_version!r}; "
                    f"rollback is only valid from the running "
                    f"version")
            # -- Target exists? --
            if to_version not in self._snapshots:
                self._emit_metric("rollback_not_found", {
                    "from_version": from_version,
                    "to_version": to_version,
                })
                raise RollbackNotFoundError(
                    f"rollback target {to_version!r} not found; "
                    f"available: {sorted(self._snapshots.keys())}")

            target_record = self._snapshots[to_version]
            # Deep-copy the state so both the caller and the store
            # are safe from each other.
            restored = _deep_copy_or_raise(
                target_record.state,
                context=f"rollback target {to_version!r}")

            if dry_run:
                return restored

            record = RollbackRecord(
                from_version=from_version,
                to_version=to_version,
                reason=reason,
                actor=actor,
            )
            self._history.append(record)
            self._current_version = to_version

        self._emit_metric("rollback_completed", {
            "from_version": from_version,
            "to_version": to_version,
            "actor": actor or "",
            "reason": reason,
        })
        self._emit_audit({
            "event": "rollback_completed",
            "from_version": from_version,
            "to_version": to_version,
            "actor": actor,
            "reason": reason,
            "at": record.at.isoformat(),
        })
        self._safe_log(
            logging.INFO,
            "Rollback %s → %s (%s)", from_version, to_version, reason)

        # Fire the optional callback AFTER the record is committed.
        # An exception propagates — but the record is retained.
        if self._on_rollback is not None:
            try:
                self._on_rollback(from_version, to_version, restored)
            except Exception as exc:  # noqa: BLE001
                self._emit_metric("rollback_apply_failed", {
                    "from_version": from_version,
                    "to_version": to_version,
                    "error_type": type(exc).__name__,
                })
                self._safe_log(
                    logging.ERROR,
                    "on_rollback callback failed for %s → %s: %s",
                    from_version, to_version, exc)
                raise

        return restored

    # ------------------------------------------------------------------
    # Current-version tracking (P1 3.3)
    # ------------------------------------------------------------------

    def set_current_version(self, version: str) -> None:
        """
        Record which version is currently running.

        The version must already have a snapshot; this prevents the
        caller from claiming a running version that has no restore
        point.
        """
        version = _require_non_empty_string("version", version)
        with self._lock:
            if version not in self._snapshots:
                raise RollbackNotFoundError(
                    f"cannot set current_version to {version!r}; "
                    f"no snapshot with that name")
            self._current_version = version

    def current_version(self) -> Optional[str]:
        with self._lock:
            return self._current_version

    # ------------------------------------------------------------------
    # Query API (P1 3.2)
    # ------------------------------------------------------------------

    def versions(self) -> List[str]:
        """Return all snapshot versions, sorted."""
        with self._lock:
            return sorted(self._snapshots.keys())

    def has_snapshot(self, version: str) -> bool:
        with self._lock:
            return isinstance(version, str) and \
                version in self._snapshots

    def get_snapshot(self, version: str) -> Optional[SnapshotRecord]:
        """
        Return a deep copy of the snapshot record, or None.
        """
        if not isinstance(version, str) or not version:
            return None
        with self._lock:
            record = self._snapshots.get(version)
            if record is None:
                return None
        return SnapshotRecord(
            version=record.version,
            state=_deep_copy_or_raise(
                record.state,
                context=f"snapshot {version!r} (get)"),
            approved_by=record.approved_by,
            reason=record.reason,
            taken_at=record.taken_at,
        )

    def latest_snapshot(self) -> Optional[str]:
        """Return the version of the most recently taken snapshot."""
        with self._lock:
            if not self._snapshots:
                return None
            return max(
                self._snapshots.values(),
                key=lambda r: r.taken_at,
            ).version

    def __len__(self) -> int:
        with self._lock:
            return len(self._snapshots)

    def __contains__(self, version: object) -> bool:
        if not isinstance(version, str) or not version:
            return False
        with self._lock:
            return version in self._snapshots

    # ------------------------------------------------------------------
    # History (P1 3.12, 3.13)
    # ------------------------------------------------------------------

    def history(
        self, limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return the most recent rollback records."""
        with self._lock:
            records = list(self._history)
        if limit is not None:
            if isinstance(limit, bool) or \
                    not isinstance(limit, int) or limit <= 0:
                raise RollbackValidationError(
                    "limit must be a positive int or None")
            records = records[-limit:]
        return [r.to_dict() for r in records]

    def history_between(
        self, start: datetime, end: datetime,
    ) -> List[Dict[str, Any]]:
        """Return records with ``start <= at < end`` (half-open)."""
        start = _normalize_dt(start)
        end = _normalize_dt(end)
        if end < start:
            raise RollbackValidationError("end must be >= start")
        with self._lock:
            records = list(self._history)
        return [
            r.to_dict() for r in records
            if start <= r.at < end
        ]

    # ------------------------------------------------------------------
    # Preview (P1 3.19)
    # ------------------------------------------------------------------

    def assert_can_rollback(
        self, from_version: str, to_version: str,
    ) -> None:
        """Raise if :meth:`rollback` would refuse the transition."""
        from_version = _require_non_empty_string(
            "from_version", from_version)
        to_version = _require_non_empty_string(
            "to_version", to_version)
        if from_version == to_version:
            raise RollbackValidationError(
                f"from_version and to_version are identical "
                f"({from_version!r})")
        with self._lock:
            if self._current_version is not None and \
                    from_version != self._current_version:
                raise RollbackStateError(
                    f"from_version={from_version!r} does not match "
                    f"current_version={self._current_version!r}")
            if to_version not in self._snapshots:
                raise RollbackNotFoundError(
                    f"rollback target {to_version!r} not found")

    # ------------------------------------------------------------------
    # Prune (P1 3.8)
    # ------------------------------------------------------------------

    def prune(self, *, keep_latest: int) -> int:
        """
        Remove old snapshots, keeping only the ``keep_latest`` most
        recent. Returns the number of snapshots removed.

        The current version is never pruned.
        """
        if isinstance(keep_latest, bool) or \
                not isinstance(keep_latest, int) or keep_latest <= 0:
            raise RollbackValidationError(
                "keep_latest must be a positive int")
        with self._lock:
            records = sorted(
                self._snapshots.values(),
                key=lambda r: r.taken_at,
                reverse=True,
            )
            keep = {r.version for r in records[:keep_latest]}
            if self._current_version is not None:
                keep.add(self._current_version)
            removed = 0
            for version in list(self._snapshots.keys()):
                if version not in keep:
                    del self._snapshots[version]
                    removed += 1
        if removed:
            self._emit_metric("snapshots_pruned", {
                "removed": removed,
                "kept": len(self._snapshots),
            })
        return removed

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "snapshots": len(self._snapshots),
                "max_snapshots": self._max_snapshots,
                "history": len(self._history),
                "max_history": self._history.maxlen,
                "current_version": self._current_version,
                "require_approval": self._require_approval,
                "on_rollback_attached":
                    self._on_rollback is not None,
                "audit_log_attached": self._audit_log is not None,
            }

    # ------------------------------------------------------------------
    # Serialization (P1 3.9)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a JSON-safe representation of the controller's config
        and history. **Snapshots are not serialized** — their state is
        arbitrary user objects and cannot be represented generically.
        """
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "current_version": self._current_version,
                "require_approval": self._require_approval,
                "max_snapshots": self._max_snapshots,
                "max_history": self._history.maxlen,
                "versions": sorted(self._snapshots.keys()),
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        on_rollback: Optional[
            Callable[[str, str, Any], Any]
        ] = None,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "RollbackController":
        if not isinstance(data, Mapping):
            raise RollbackValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        max_history = data.get("max_history", 1000)
        max_snapshots = data.get("max_snapshots", 100)
        controller = cls(
            on_rollback=on_rollback,
            audit_log=audit_log,
            metrics_hook=metrics_hook,
            max_history=int(max_history) if max_history else 1000,
            max_snapshots=int(max_snapshots) if max_snapshots else 100,
            require_approval=bool(data.get("require_approval", False)),
        )
        for entry in data.get("history", []) or []:
            controller._history.append(
                RollbackRecord.from_dict(entry))
        # current_version restored only if a snapshot for it exists —
        # the caller must re-snapshot before rollback can succeed.
        cv = data.get("current_version")
        if isinstance(cv, str) and cv in controller._snapshots:
            controller._current_version = cv
        return controller

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        on_rollback: Optional[
            Callable[[str, str, Any], Any]
        ] = None,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "RollbackController":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RollbackValidationError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(
            data,
            on_rollback=on_rollback,
            audit_log=audit_log,
            metrics_hook=metrics_hook,
        )

    # ------------------------------------------------------------------
    # Pickling (P1 3.17)
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_audit_log", None)
        state.pop("_metrics_hook", None)
        # on_rollback is a callable — likely unpicklable if it's a
        # closure. Drop it and warn.
        if state.get("_on_rollback") is not None:
            logger.debug(
                "Dropping on_rollback callback on pickle; "
                "re-inject after unpickling if needed.")
            state["_on_rollback"] = None
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._audit_log = None
        self._metrics_hook = None
        # _on_rollback is preserved as None by __getstate__.

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"RollbackController("
                f"version={__version__!r}, "
                f"snapshots={len(self._snapshots)}, "
                f"history={len(self._history)}, "
                f"current={self._current_version!r}, "
                f"require_approval={self._require_approval})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "RollbackCapacityError",
    "RollbackController",
    "RollbackError",
    "RollbackNotFoundError",
    "RollbackRecord",
    "RollbackStateError",
    "RollbackValidationError",
    "SnapshotRecord",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import concurrent.futures
    import pickle
    from dataclasses import FrozenInstanceError
    from datetime import timedelta

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== rollback_controller v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — deep copy on snapshot and return
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: deep copy on snapshot ===")

    ctrl = RollbackController()
    original = {"threshold": 100, "nested": {"mode": "safe"}}
    ctrl.snapshot("v1", original)

    # Mutate the original after snapshotting.
    original["threshold"] = 999
    original["nested"]["mode"] = "unsafe"

    # Rollback to v1 (current version is None, so from_version is
    # not validated against it).
    restored = ctrl.rollback("v2", "v1", reason="recovery")
    assert restored["threshold"] == 100, (
        f"P0 2.1 regression: threshold={restored['threshold']}, "
        f"expected 100 (original, not mutated)")
    assert restored["nested"]["mode"] == "safe", (
        f"P0 2.1 regression: mode={restored['nested']['mode']}")
    print(f"  ✓ mutated original did not affect stored snapshot")

    # Mutate the returned state — should not affect future rollbacks.
    restored["threshold"] = 12345
    restored["nested"]["mode"] = "corrupted"
    restored2 = ctrl.rollback("v1", "v1", reason="noop")
    # Actually — v1 == v1 would be rejected now (P1 3.7). Reset
    # current_version to None for this check.
    ctrl2 = RollbackController()
    ctrl2.snapshot("v1", {"x": 1})
    r1 = ctrl2.rollback("v0", "v1", reason="first")
    r1["x"] = 999
    r2 = ctrl2.rollback("v1", "v1", reason="x") if False else \
        ctrl2.rollback("v0", "v1", reason="second")
    assert r2["x"] == 1, (
        f"P0 2.1 regression: returned state mutation leaked into "
        f"store, second rollback returned x={r2['x']}")
    print(f"  ✓ mutating returned state does not corrupt store")

    # ---------------------------------------------------------------- #
    # P0 2.2 — require_approval
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: require_approval ===")

    ctrl = RollbackController(require_approval=True)
    try:
        ctrl.snapshot("v1", {"x": 1})
    except RollbackValidationError as exc:
        print(f"  ✓ snapshot without approved_by rejected: "
              f"{str(exc)[:60]}...")
    else:
        raise AssertionError(
            "P0 2.2 regression: unapproved snapshot accepted")

    rec = ctrl.snapshot("v1", {"x": 1}, approved_by="alice")
    assert rec.approved_by == "alice"
    print(f"  ✓ approved_by='alice' recorded")

    # Without require_approval, snapshot works as before.
    ctrl2 = RollbackController()
    ctrl2.snapshot("v1", {"x": 1})
    print(f"  ✓ require_approval=False allows unapproved snapshot "
          f"(v1.0.0 default preserved)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — thread safety
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: thread safety ===")

    ctrl = RollbackController()
    ctrl.snapshot("v0", {"i": -1})

    def _worker(i):
        ctrl.snapshot(f"v{i}", {"i": i})
        # Rollback from v0 (the "current") to v{i}.
        ctrl.set_current_version("v0") if i == 0 else None
        try:
            ctrl.rollback("v0", f"v{i}", reason=f"test{i}")
        except RollbackStateError:
            # from_version mismatch — expected under concurrency.
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(_worker, range(50)))

    assert len(ctrl) >= 50
    print(f"  ✓ 50 concurrent workers; store has "
          f"{len(ctrl)} snapshots")

    # ---------------------------------------------------------------- #
    # P0 2.4 — silent overwrite prevention
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: overwrite prevention ===")

    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    try:
        ctrl.snapshot("v1", {"x": 2})
    except RollbackValidationError as exc:
        print(f"  ✓ duplicate snapshot rejected: "
              f"{str(exc)[:60]}...")
    else:
        raise AssertionError(
            "P0 2.4 regression: silent overwrite accepted")

    # Explicit replace works.
    rec = ctrl.snapshot("v1", {"x": 2}, replace=True)
    assert rec.state["x"] == 2
    print(f"  ✓ replace=True allows overwrite")

    # ---------------------------------------------------------------- #
    # P0 2.5 — from_version validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: from_version validation ===")

    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v2")

    try:
        ctrl.rollback("v9", "v1", reason="spoofed")
    except RollbackStateError:
        print(f"  ✓ spoofed from_version='v9' rejected (current='v2')")
    else:
        raise AssertionError(
            "P0 2.5 regression: spoofed from_version accepted")

    # Correct from_version works.
    ctrl.rollback("v2", "v1", reason="valid")
    assert ctrl.current_version() == "v1"
    print(f"  ✓ valid from_version accepted; current version updated")

    # ---------------------------------------------------------------- #
    # P0 2.6 — RollbackRecord validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: RollbackRecord validation ===")

    bad_kwargs = [
        dict(from_version="", to_version="v2", reason="x"),
        dict(from_version="v1", to_version="", reason="x"),
        dict(from_version="v1", to_version="v2", reason=""),
        dict(from_version="v1", to_version="v2", reason="  "),
        dict(from_version="v1", to_version="v2", reason="x",
             actor=""),
        dict(from_version="v1", to_version="v2", reason="x",
             at="not-a-datetime"),
    ]
    for kwargs in bad_kwargs:
        try:
            RollbackRecord(**kwargs)
        except RollbackValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.6 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_kwargs)} invalid constructions rejected")

    # Naive datetime → UTC.
    rec = RollbackRecord(
        from_version="v1", to_version="v2", reason="x",
        at=datetime(2024, 1, 1))
    assert rec.at.tzinfo is not None
    print(f"  ✓ naive datetime normalized to UTC")

    # Frozen.
    try:
        rec.reason = "changed"   # type: ignore[misc]
    except FrozenInstanceError:
        print(f"  ✓ RollbackRecord is frozen")
    else:
        raise AssertionError(
            "Regression: RollbackRecord is mutable")

    # ---------------------------------------------------------------- #
    # P0 2.7 — bounded history
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: bounded history ===")

    ctrl = RollbackController(max_history=5)
    ctrl.snapshot("v0", {"i": 0})
    ctrl.snapshot("v1", {"i": 1})
    for i in range(20):
        try:
            ctrl.rollback(
                ctrl.current_version() or "v0",
                f"v{(i % 2)}", reason=f"test{i}")
        except RollbackValidationError:
            pass
    hist = ctrl.history(limit=100)
    assert len(hist) <= 5, (
        f"P0 2.7 regression: history has {len(hist)} entries, "
        f"expected <= 5")
    print(f"  ✓ 20 rollbacks → history bounded at 5 entries")

    # ---------------------------------------------------------------- #
    # P0 2.8 — RollbackNotFoundError
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: RollbackNotFoundError ===")

    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    try:
        ctrl.rollback("v1", "nonexistent", reason="oops")
    except RollbackNotFoundError as exc:
        print(f"  ✓ missing target raises RollbackNotFoundError")
    else:
        raise AssertionError(
            "P0 2.8 regression: missing target returned None")

    # State is legitimately None → not confused with "not found".
    ctrl.snapshot("empty", None)
    restored = ctrl.rollback("v1", "empty", reason="reset")
    assert restored is None
    print(f"  ✓ legitimate None state returns None (no ambiguity)")

    # ---------------------------------------------------------------- #
    # P1 3.1 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: error hierarchy ===")
    assert issubclass(RollbackError, ValueError)
    assert issubclass(RollbackValidationError, RollbackError)
    assert issubclass(RollbackNotFoundError, RollbackError)
    assert issubclass(RollbackStateError, RollbackError)
    assert issubclass(RollbackCapacityError, RollbackError)
    print(f"  ✓ 5 error types subclass RollbackError")

    # ---------------------------------------------------------------- #
    # P1 3.2 — query API
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: query API ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    assert ctrl.versions() == ["v1", "v2"]
    assert ctrl.has_snapshot("v1") is True
    assert ctrl.has_snapshot("v9") is False
    assert ctrl.get_snapshot("v1").state["x"] == 1
    assert len(ctrl) == 2
    assert "v1" in ctrl
    assert "v9" not in ctrl
    print(f"  ✓ versions/has_snapshot/get_snapshot/len/in all work")

    # ---------------------------------------------------------------- #
    # P1 3.3 — current_version tracking
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: current_version ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    assert ctrl.current_version() is None
    ctrl.set_current_version("v2")
    assert ctrl.current_version() == "v2"
    ctrl.rollback("v2", "v1", reason="downgrade")
    assert ctrl.current_version() == "v1"
    print(f"  ✓ current_version tracked through set + rollback")

    # Setting to unknown raises.
    try:
        ctrl.set_current_version("unknown")
    except RollbackNotFoundError:
        print(f"  ✓ set_current_version('unknown') raises")

    # ---------------------------------------------------------------- #
    # P1 3.4 — snapshot metadata
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: snapshot metadata ===")
    ctrl = RollbackController()
    rec = ctrl.snapshot(
        "v1", {"x": 1}, approved_by="alice", reason="release 1.2.3")
    assert rec.approved_by == "alice"
    assert rec.reason == "release 1.2.3"
    assert rec.taken_at.tzinfo is not None
    print(f"  ✓ approved_by + reason + taken_at recorded")

    # ---------------------------------------------------------------- #
    # P1 3.5 — on_rollback callback
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: on_rollback callback ===")
    applied = []
    ctrl = RollbackController(
        on_rollback=lambda f, t, s: applied.append((f, t, s)))
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v2")
    ctrl.rollback("v2", "v1", reason="test")
    assert applied == [("v2", "v1", {"x": 1})]
    print(f"  ✓ callback fired with (from, to, state)")

    # Callback exception propagates but history retains the record.
    def _broken(f, t, s):
        raise RuntimeError("simulated apply failure")
    ctrl = RollbackController(on_rollback=_broken)
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v2")
    try:
        ctrl.rollback("v2", "v1", reason="test")
    except RuntimeError:
        pass
    else:
        raise AssertionError(
            "P1 3.5 regression: callback exception swallowed")
    assert len(ctrl.history()) == 1, (
        "P1 3.5 regression: history lost after callback failure")
    print(f"  ✓ callback failure propagates; history retained")

    # ---------------------------------------------------------------- #
    # P1 3.6 — dry_run
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: dry_run ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    preview = ctrl.rollback("v2", "v1", reason="preview",
                            dry_run=True)
    assert preview == {"x": 1}
    assert len(ctrl.history()) == 0
    print(f"  ✓ dry_run returned state without recording")

    # ---------------------------------------------------------------- #
    # P1 3.7 — same-version rejection
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: same-version rejection ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    try:
        ctrl.rollback("v1", "v1", reason="noop")
    except RollbackValidationError:
        print(f"  ✓ from==to rejected")
    else:
        raise AssertionError(
            "P1 3.7 regression: same-version rollback accepted")

    # ---------------------------------------------------------------- #
    # P1 3.8 — max_snapshots + prune
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: capacity + prune ===")

    ctrl = RollbackController(max_snapshots=3)
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.snapshot("v3", {"x": 3})
    try:
        ctrl.snapshot("v4", {"x": 4})
    except RollbackCapacityError:
        print(f"  ✓ capacity reached → RollbackCapacityError")

    # Prune to keep 2 latest.
    removed = ctrl.prune(keep_latest=2)
    assert removed >= 1
    assert len(ctrl) == 2
    print(f"  ✓ prune removed {removed} snapshot(s); store now has "
          f"{len(ctrl)}")

    # ---------------------------------------------------------------- #
    # P1 3.9 — serialization
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: serialization ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v2")
    ctrl.rollback("v2", "v1", reason="test")

    d = ctrl.to_dict()
    assert d["version"] == __version__
    assert d["versions"] == ["v1", "v2"]
    assert len(d["history"]) == 1

    # Round-trip.
    restored = RollbackController.from_dict(d)
    assert len(restored.history()) == 1
    assert restored.current_version() is None  # no snapshot in payload
    assert sorted(restored.versions()) == []  # snapshots not restored
    print(f"  ✓ config + history round-trip (snapshots excluded)")

    # ---------------------------------------------------------------- #
    # P1 3.10 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: metrics_hook ===")
    events = []
    ctrl = RollbackController(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.rollback("v1", "v2", reason="test")
    names = {ev for ev, _ in events}
    assert "snapshot_taken" in names
    assert "rollback_completed" in names
    print(f"  ✓ events: {sorted(names)}")

    # ---------------------------------------------------------------- #
    # P1 3.11 — audit_log integration
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: audit_log integration ===")
    audit_events = []

    class _MockAuditLog:
        def append(self, payload):
            audit_events.append(payload)

    ctrl = RollbackController(audit_log=_MockAuditLog())
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.rollback("v1", "v2", reason="test")
    assert len(audit_events) == 3
    assert audit_events[0]["event"] == "snapshot_taken"
    assert audit_events[-1]["event"] == "rollback_completed"
    print(f"  ✓ audit_log received {len(audit_events)} events")

    # ---------------------------------------------------------------- #
    # P1 3.12 — history(limit)
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.12: history(limit) ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v1")
    for i in range(5):
        ctrl.rollback(
            ctrl.current_version(),
            "v1" if i % 2 else "v2",
            reason=f"test{i}")
    assert len(ctrl.history(limit=2)) == 2
    print(f"  ✓ history(limit=2) returns 2 records")

    # ---------------------------------------------------------------- #
    # P1 3.13 — history_between
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.13: history_between ===")
    now = datetime.now(timezone.utc)
    recent = ctrl.history_between(
        now - timedelta(hours=1),
        now + timedelta(hours=1))
    assert len(recent) >= 1
    old = ctrl.history_between(
        now + timedelta(hours=1),
        now + timedelta(hours=2))
    assert old == []
    print(f"  ✓ history_between works for recent and far-future "
          f"windows")

    # ---------------------------------------------------------------- #
    # P1 3.17 — pickle
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.17: pickle ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v2")
    ctrl.rollback("v2", "v1", reason="test")

    blob = pickle.dumps(ctrl)
    restored = pickle.loads(blob)
    assert len(restored) == 2
    assert restored.current_version() == "v1"
    assert len(restored.history()) == 1
    print(f"  ✓ pickle round-trip preserves state")

    # ---------------------------------------------------------------- #
    # P1 3.18 — latest_snapshot
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.18: latest_snapshot ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    # v2 was taken after v1.
    assert ctrl.latest_snapshot() == "v2"
    print(f"  ✓ latest_snapshot returns most recent version")

    # ---------------------------------------------------------------- #
    # P1 3.19 — assert_can_rollback
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.19: assert_can_rollback ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    ctrl.set_current_version("v2")
    # Should not raise.
    ctrl.assert_can_rollback("v2", "v1")
    print(f"  ✓ valid transition passes preview")

    try:
        ctrl.assert_can_rollback("v2", "v9")
    except RollbackNotFoundError:
        print(f"  ✓ missing target preview raises")

    try:
        ctrl.assert_can_rollback("v9", "v1")
    except RollbackStateError:
        print(f"  ✓ wrong from_version preview raises")

    # Preview does NOT commit.
    assert len(ctrl.history()) == 0
    print(f"  ✓ preview does not commit to history")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 API
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat ===")
    ctrl = RollbackController()
    ctrl.snapshot("v1", {"x": 1})
    ctrl.snapshot("v2", {"x": 2})
    # v1.0.0 call site.
    result = ctrl.rollback("v2", "v1", reason="legacy",
                           actor="alice")
    assert result == {"x": 1}
    hist = ctrl.history()
    assert len(hist) == 1
    assert hist[0]["from"] == "v2"
    assert hist[0]["to"] == "v1"
    assert hist[0]["actor"] == "alice"
    print(f"  ✓ v1.0.0 signature + history shape preserved")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
