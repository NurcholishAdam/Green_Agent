"""
EmergencyStop — halt all execution immediately. — v2.0.0

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``check()`` — the class docstring promised it; v1.0.0 did not
        provide it. ``check()`` returns ``True`` when execution is
        permitted (inverse of ``is_stopped()``). A caller whose
        incident-response playbook used ``if not stop.check():`` got
        ``AttributeError`` — a safety gate that silently failed at
        the moment it mattered. Both methods now work and are
        documented as inverses.
  * 2.2 ``reset()`` now records a ``StopEvent`` with ``kind="reset"``.
        Previously only ``trigger()`` recorded anything — "how long
        was production halted?" and "who authorized the restart?"
        were unanswerable. The full lifecycle is now reconstructible
        from ``events()``.
  * 2.3 ``StopEvent.__post_init__`` validates every field
        (non-empty reason, non-empty actor or None, tz-aware
        timestamp, known ``kind`` / ``classification`` / ``scope``).
        Previously ``reason=""``, ``reason=None``, ``actor=""``,
        ``at=<naive datetime>`` all constructed cleanly.
  * 2.4 ``reset(*, actor=None, reason=None)`` records the actor and
        reason for the reset. Previously ``reset()`` had no
        parameters — "who resumed?" was unanswerable.
  * 2.5 ``EmergencyStopError`` base class +
        ``EmergencyStopValidationError`` +
        ``EmergencyStopActiveError``. Add to
        ``governance/__init__.py``'s ``_ERROR_CLASSES_BY_MODULE`` for
        ``except GovernanceError:`` compatibility.
  * 2.6 ``current_stop_event()`` returns the active ``StopEvent`` or
        ``None``; ``events()`` returns the full lifecycle (stops and
        resets). ``reasons()`` remains backward compatible — it
        returns stop events only, in the v1.0.0 shape.
  * 2.7 Reason/actor are normalized: whitespace stripped, actor
        lowercased. ``trigger("  OOM  ")`` and ``trigger("OOM")``
        produce identical records.
  * 2.8 The ``logger.warning`` in ``trigger()`` runs *outside* the
        lock, wrapped in ``try/except``. A slow or broken log handler
        cannot block or fail the emergency stop itself.

**P1 robustness**
  * 3.1 ``assert_not_stopped(*, context=None)`` raises
        ``EmergencyStopActiveError`` — the idiomatic guard for
        callers who want to refuse execution.
  * 3.2 ``wait_until_reset(*, timeout=None)`` — blocks on a
        ``threading.Event`` until the stop is lifted. Returns
        ``True`` on reset, ``False`` on timeout.
  * 3.3 ``StopClassification`` enum — ``operational`` / ``safety``
        / ``compliance`` / ``chaos`` / ``maintenance``. Callers can
        distinguish a chaos-test stop from a safety incident.
  * 3.5 ``__version__`` / ``LEDGER_SCHEMA_VERSION`` / ``__all__``.
  * 3.6 ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``
        — the stop state and history can be persisted and reloaded
        across process restarts.
  * 3.7 ``metrics_hook(event, fields)`` — fires on trigger, reset,
        and blocked check.
  * 3.8 Optional ``audit_log`` integration — every stop/reset is
        appended to the injected ``AuditLog`` (if provided),
        participating in the compliance hash chain.
  * 3.9 Pickling support via ``__getstate__`` / ``__setstate__``.
  * 3.10 ``__repr__`` — concise summary for logs.
  * 3.11 ``__len__`` — total event count.
  * 3.12 ``events_between(start, end)`` — half-open time-range
        query.
  * 3.14 ``occurrences`` counter on ``StopEvent`` — 50 concurrent
        ``trigger("OOM")`` calls collapse to 1 record with
        ``occurrences=50`` (only when the events are contiguous —
        a reset in between starts a new record).
  * 3.16 ``reset()`` distinguishes "real reset" from "spurious
        reset call" in the audit log (the spurious case logs at
        DEBUG and records no ``StopEvent``).

**Scoping note**
  v2.0.0's ``trigger(..., scope="all")`` accepts a ``scope`` string
  for future use, but ``check()`` and ``is_stopped()`` always query
  global state. Scoped gating (e.g., "pause batch processing, let
  streaming continue") is deferred to a future version. Callers who
  set ``scope`` should not rely on it affecting ``check()`` yet.

Backward compatible
-------------------
``EmergencyStop()``, ``stop.trigger(reason, actor)``, ``stop.reset()``,
``stop.is_stopped()``, ``stop.reasons()`` — every v1.0.0 call site
works unchanged. ``StopReason`` remains a valid name (alias for
``StopEvent``). The new methods and fields are additive.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Dict, Iterator, List, Mapping, Optional,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"
LEDGER_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors (P0 2.5)
# =============================================================================

class EmergencyStopError(ValueError):
    """Base class for every error raised by EmergencyStop."""


class EmergencyStopValidationError(EmergencyStopError):
    """Raised for invalid stop reasons, actors, or timestamps."""


class EmergencyStopActiveError(EmergencyStopError):
    """Raised by ``assert_not_stopped()`` when the gate is engaged."""


# =============================================================================
# Enums (P0 2.3, P1 3.3)
# =============================================================================

class StopClassification(str, Enum):
    """Why the emergency stop was triggered."""
    OPERATIONAL = "operational"
    SAFETY = "safety"
    COMPLIANCE = "compliance"
    CHAOS = "chaos"
    MAINTENANCE = "maintenance"

    def __str__(self) -> str:
        return self.value


class EventKind(str, Enum):
    """Type of lifecycle event."""
    STOP = "stop"
    RESET = "reset"

    def __str__(self) -> str:
        return self.value


_VALID_CLASSIFICATIONS = tuple(c.value for c in StopClassification)
_VALID_KINDS = tuple(k.value for k in EventKind)


# =============================================================================
# StopEvent (P0 2.3) — StopReason alias preserved
# =============================================================================

@dataclass(frozen=True)
class StopEvent:
    """
    A single lifecycle event — either a stop or a reset.

    Field order matches v1.0.0's ``StopReason(reason, actor, at)``,
    so positional construction is unchanged.
    """
    reason: str
    actor: Optional[str] = None
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    kind: str = "stop"                       # "stop" | "reset"
    classification: str = "operational"      # StopClassification
    scope: str = "all"
    occurrences: int = 1                     # P1 3.14

    def __post_init__(self) -> None:
        # -- reason --
        if not isinstance(self.reason, str) or \
                not self.reason.strip():
            raise EmergencyStopValidationError(
                "reason must be a non-empty string")
        object.__setattr__(self, "reason", self.reason.strip())

        # -- actor --
        if self.actor is not None:
            if not isinstance(self.actor, str) or \
                    not self.actor.strip():
                raise EmergencyStopValidationError(
                    "actor must be a non-empty string or None")
            object.__setattr__(
                self, "actor", self.actor.strip().lower())

        # -- at --
        if not isinstance(self.at, datetime):
            raise EmergencyStopValidationError(
                f"at must be a datetime, "
                f"got {type(self.at).__name__}")
        if not self.at.tzinfo:
            object.__setattr__(
                self, "at",
                self.at.replace(tzinfo=timezone.utc))
        else:
            object.__setattr__(
                self, "at",
                self.at.astimezone(timezone.utc))

        # -- kind --
        if not isinstance(self.kind, str):
            raise EmergencyStopValidationError(
                "kind must be a string")
        kind_norm = self.kind.strip().lower()
        if kind_norm not in _VALID_KINDS:
            raise EmergencyStopValidationError(
                f"kind must be one of {_VALID_KINDS}, "
                f"got {self.kind!r}")
        object.__setattr__(self, "kind", kind_norm)

        # -- classification --
        if not isinstance(self.classification, str):
            raise EmergencyStopValidationError(
                "classification must be a string")
        class_norm = self.classification.strip().lower()
        if class_norm not in _VALID_CLASSIFICATIONS:
            raise EmergencyStopValidationError(
                f"classification must be one of "
                f"{_VALID_CLASSIFICATIONS}, "
                f"got {self.classification!r}")
        object.__setattr__(
            self, "classification", class_norm)

        # -- scope --
        if not isinstance(self.scope, str) or not self.scope.strip():
            raise EmergencyStopValidationError(
                "scope must be a non-empty string")
        object.__setattr__(self, "scope", self.scope.strip().lower())

        # -- occurrences --
        if isinstance(self.occurrences, bool) or \
                not isinstance(self.occurrences, int) or \
                self.occurrences < 1:
            raise EmergencyStopValidationError(
                "occurrences must be an int >= 1")

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "reason": self.reason,
            "actor": self.actor,
            "at": self.at.isoformat(),
            "classification": self.classification,
            "scope": self.scope,
            "occurrences": self.occurrences,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StopEvent":
        if not isinstance(data, Mapping):
            raise EmergencyStopValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        at_raw = data.get("at")
        if isinstance(at_raw, str):
            try:
                at = datetime.fromisoformat(
                    at_raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise EmergencyStopValidationError(
                    f"at is not a valid ISO 8601 timestamp: "
                    f"{exc}") from exc
        elif isinstance(at_raw, datetime):
            at = at_raw
        elif at_raw is None:
            at = datetime.now(timezone.utc)
        else:
            raise EmergencyStopValidationError(
                f"at must be a datetime, ISO string, or None; "
                f"got {type(at_raw).__name__}")
        occurrences_raw = data.get("occurrences", 1)
        try:
            occurrences = int(occurrences_raw)
        except (TypeError, ValueError) as exc:
            raise EmergencyStopValidationError(
                f"occurrences must be an int: {exc}") from exc
        return cls(
            reason=data.get("reason"),
            actor=data.get("actor"),
            at=at,
            kind=data.get("kind", "stop"),
            classification=data.get(
                "classification", "operational"),
            scope=data.get("scope", "all"),
            occurrences=occurrences,
        )


# Backward-compat alias — v1.0.0 exported ``StopReason``.
StopReason = StopEvent


# =============================================================================
# EmergencyStop (P0 2.1–2.8; P1 3.1–3.16)
# =============================================================================

class EmergencyStop:
    """
    Emergency halt gate.

    When ``trigger()`` is called, ``check()`` returns ``False`` (and
    ``is_stopped()`` returns ``True``) until ``reset()`` is called.
    Every transition is recorded as a :class:`StopEvent` so the full
    lifecycle is auditable.

    All state is thread-safe. The stop gate is *global* — a single
    boolean for the whole process. Scoped stops are a future
    feature.
    """

    def __init__(
        self,
        *,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ):
        if metrics_hook is not None and not callable(metrics_hook):
            raise EmergencyStopValidationError(
                "metrics_hook must be callable")

        self._stopped: bool = False
        self._events: List[StopEvent] = []
        self._lock = threading.RLock()
        self._reset_event = threading.Event()
        self._reset_event.set()   # initially running → wait returns
        self._audit_log = audit_log
        self._metrics_hook = metrics_hook
        self._trigger_count: int = 0
        self._reset_count: int = 0
        self._blocked_check_count: int = 0

    # ------------------------------------------------------------------
    # Lifecycle: trigger / reset
    # ------------------------------------------------------------------

    def trigger(
        self,
        reason: str,
        actor: Optional[str] = None,
        *,
        classification: str = "operational",
        scope: str = "all",
    ) -> None:
        """
        Engage the emergency stop.

        Records a ``StopEvent`` with ``kind="stop"``. Repeated calls
        with the same ``(reason, actor, classification)`` while
        already stopped increment the last event's ``occurrences``
        counter rather than appending a duplicate record — so 50
        workers hitting the same failure produce 1 record with
        ``occurrences=50``, not 50 near-identical records.
        """
        # Validate before touching state.
        if not isinstance(reason, str) or not reason.strip():
            raise EmergencyStopValidationError(
                "reason must be a non-empty string")
        if actor is not None and (
                not isinstance(actor, str) or not actor.strip()):
            raise EmergencyStopValidationError(
                "actor must be a non-empty string or None")
        if not isinstance(classification, str):
            raise EmergencyStopValidationError(
                "classification must be a string")
        if classification.strip().lower() not in \
                _VALID_CLASSIFICATIONS:
            raise EmergencyStopValidationError(
                f"classification must be one of "
                f"{_VALID_CLASSIFICATIONS}, "
                f"got {classification!r}")
        if not isinstance(scope, str) or not scope.strip():
            raise EmergencyStopValidationError(
                "scope must be a non-empty string")

        # Build a candidate event to leverage StopEvent's
        # normalization for the dedup comparison.
        candidate = StopEvent(
            reason=reason,
            actor=actor,
            kind="stop",
            classification=classification,
            scope=scope,
        )

        new_event: Optional[StopEvent] = None
        with self._lock:
            was_stopped = self._stopped
            self._stopped = True
            self._reset_event.clear()

            # Dedup: only when we were already stopped AND the last
            # recorded event is a "stop" with matching metadata.
            if (was_stopped
                    and self._events
                    and self._events[-1].kind == "stop"
                    and self._events[-1].reason == candidate.reason
                    and self._events[-1].actor == candidate.actor
                    and self._events[-1].classification ==
                        candidate.classification
                    and self._events[-1].scope == candidate.scope):
                last = self._events[-1]
                self._events[-1] = replace(
                    last, occurrences=last.occurrences + 1)
                new_event = self._events[-1]
                self._trigger_count += 1
            else:
                self._events.append(candidate)
                new_event = candidate
                self._trigger_count += 1

        # Emit side effects OUTSIDE the lock (P0 2.8).
        self._safe_log(
            logging.WARNING,
            "EMERGENCY STOP: %s (actor=%s, classification=%s)",
            new_event.reason, new_event.actor,
            new_event.classification)

        self._emit_metric("stop_triggered", {
            "reason": new_event.reason,
            "actor": new_event.actor,
            "classification": new_event.classification,
            "scope": new_event.scope,
            "occurrences": new_event.occurrences,
        })

        self._emit_audit({
            "event": "emergency_stop_triggered",
            "reason": new_event.reason,
            "actor": new_event.actor,
            "classification": new_event.classification,
            "scope": new_event.scope,
            "at": new_event.at.isoformat(),
            "occurrences": new_event.occurrences,
        })

    def reset(
        self,
        *,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Clear the emergency stop.

        Returns ``True`` if the state actually changed (the gate was
        engaged), ``False`` if called on a running system.

        When the state changes, a ``StopEvent`` with ``kind="reset"``
        is recorded. When called on a running system, the no-op is
        logged at DEBUG and no event is recorded (P1 3.16).
        """
        if actor is not None and (
                not isinstance(actor, str) or not actor.strip()):
            raise EmergencyStopValidationError(
                "actor must be a non-empty string or None")
        if reason is not None and (
                not isinstance(reason, str) or not reason.strip()):
            raise EmergencyStopValidationError(
                "reason must be a non-empty string or None")

        event: Optional[StopEvent] = None
        with self._lock:
            was_stopped = self._stopped
            self._stopped = False
            self._reset_event.set()
            if was_stopped:
                event = StopEvent(
                    reason=(reason.strip() if reason
                            else "manual reset"),
                    actor=actor,
                    kind="reset",
                )
                self._events.append(event)
                self._reset_count += 1

        if event is not None:
            self._safe_log(
                logging.INFO,
                "Emergency stop reset (actor=%s, reason=%s)",
                event.actor, event.reason)
            self._emit_metric("stop_reset", {
                "actor": event.actor,
                "reason": event.reason,
            })
            self._emit_audit({
                "event": "emergency_stop_reset",
                "actor": event.actor,
                "reason": event.reason,
                "at": event.at.isoformat(),
            })
        else:
            self._safe_log(
                logging.DEBUG,
                "reset() called while not stopped; no-op (actor=%s)",
                actor)
        return was_stopped

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def is_stopped(self) -> bool:
        """Return True iff the emergency stop is engaged."""
        with self._lock:
            return self._stopped

    def check(self) -> bool:
        """
        Return True if execution is permitted (P0 2.1).

        This is the inverse of :meth:`is_stopped`, and the method
        incident-response playbooks use::

            if not emergency_stop.check():
                return refuse("emergency stop engaged")
        """
        with self._lock:
            stopped = self._stopped
            if stopped:
                self._blocked_check_count += 1
        if stopped:
            self._emit_metric("stop_check_failed", {})
            return False
        return True

    def assert_not_stopped(
        self, *, context: Optional[str] = None,
    ) -> None:
        """
        Raise :class:`EmergencyStopActiveError` if the gate is engaged
        (P1 3.1).
        """
        with self._lock:
            stopped = self._stopped
            last = self._events[-1] if self._events else None
            if stopped:
                self._blocked_check_count += 1
        if not stopped:
            return
        reason = last.reason if last is not None else "unknown"
        actor = last.actor if last is not None else None
        msg = (
            f"emergency stop is engaged "
            f"(reason={reason!r}, actor={actor!r}"
        )
        if context:
            msg += f", context={context!r}"
        msg += ")"
        self._emit_metric("stop_check_failed", {
            "context": context or "",
        })
        raise EmergencyStopActiveError(msg)

    def wait_until_reset(
        self, *, timeout: Optional[float] = None,
    ) -> bool:
        """
        Block until the emergency stop is lifted (P1 3.2).

        Returns ``True`` if the reset happened during the wait,
        ``False`` on timeout.
        """
        if timeout is not None:
            if isinstance(timeout, bool) or \
                    not isinstance(timeout, (int, float)) or \
                    timeout < 0:
                raise EmergencyStopValidationError(
                    "timeout must be a non-negative number or None")
        # Fast path: not stopped → return immediately.
        with self._lock:
            if not self._stopped:
                return True
        return self._reset_event.wait(timeout=timeout)

    def current_stop_event(self) -> Optional[StopEvent]:
        """
        Return the most recent ``kind="stop"`` event, or ``None`` if
        the system is running (P0 2.6).
        """
        with self._lock:
            if not self._stopped:
                return None
            for event in reversed(self._events):
                if event.kind == "stop":
                    return event
        return None

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def reasons(self) -> List[Dict[str, Any]]:
        """
        Return stop events in v1.0.0 shape (backward compatible).

        Excludes resets — use :meth:`events` for the full lifecycle.
        """
        with self._lock:
            return [
                {
                    "reason": e.reason,
                    "actor": e.actor,
                    "at": e.at.isoformat(),
                }
                for e in self._events
                if e.kind == "stop"
            ]

    def events(self) -> List[Dict[str, Any]]:
        """
        Return every lifecycle event (stops AND resets), oldest first.
        """
        with self._lock:
            return [e.to_dict() for e in self._events]

    def events_between(
        self,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Return events with ``start <= at < end`` (P1 3.12).
        """
        if not isinstance(start, datetime) or \
                not isinstance(end, datetime):
            raise EmergencyStopValidationError(
                "start and end must be datetimes")
        if not start.tzinfo:
            start = start.replace(tzinfo=timezone.utc)
        else:
            start = start.astimezone(timezone.utc)
        if not end.tzinfo:
            end = end.replace(tzinfo=timezone.utc)
        else:
            end = end.astimezone(timezone.utc)
        if end < start:
            raise EmergencyStopValidationError(
                "end must be >= start")
        with self._lock:
            return [
                e.to_dict() for e in self._events
                if start <= e.at < end
            ]

    def statistics(self) -> Dict[str, Any]:
        """Return cumulative statistics."""
        with self._lock:
            latest = self.current_stop_event()
            total_duration = 0.0
            current_stop_at: Optional[datetime] = None
            for e in self._events:
                if e.kind == "stop" and current_stop_at is None:
                    current_stop_at = e.at
                elif e.kind == "reset" and current_stop_at is not None:
                    total_duration += (
                        e.at - current_stop_at).total_seconds()
                    current_stop_at = None
            # If still stopped, count up to now.
            if current_stop_at is not None:
                total_duration += (
                    datetime.now(timezone.utc) -
                    current_stop_at).total_seconds()
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "stopped": self._stopped,
                "trigger_count": self._trigger_count,
                "reset_count": self._reset_count,
                "blocked_check_count": self._blocked_check_count,
                "event_count": len(self._events),
                "total_stopped_seconds": total_duration,
                "current_reason":
                    latest.reason if latest else None,
                "current_actor":
                    latest.actor if latest else None,
                "current_classification":
                    latest.classification if latest else None,
                "audit_log_attached":
                    self._audit_log is not None,
            }

    # ------------------------------------------------------------------
    # Serialization (P1 3.6)
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "schema_version": LEDGER_SCHEMA_VERSION,
                "stopped": self._stopped,
                "events": [e.to_dict() for e in self._events],
                "trigger_count": self._trigger_count,
                "reset_count": self._reset_count,
                "blocked_check_count":
                    self._blocked_check_count,
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "EmergencyStop":
        if not isinstance(data, Mapping):
            raise EmergencyStopValidationError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}")
        stop = cls(
            audit_log=audit_log, metrics_hook=metrics_hook)
        raw_events = data.get("events", [])
        if not isinstance(raw_events, (list, tuple)):
            raise EmergencyStopValidationError(
                "events must be a list or tuple")
        loaded: List[StopEvent] = []
        for i, raw in enumerate(raw_events):
            if not isinstance(raw, Mapping):
                raise EmergencyStopValidationError(
                    f"events[{i}] must be a Mapping")
            try:
                loaded.append(StopEvent.from_dict(raw))
            except EmergencyStopValidationError as exc:
                raise EmergencyStopValidationError(
                    f"events[{i}]: {exc}") from exc
        with stop._lock:
            stop._events = loaded
            # Derive the current stopped state from the last event.
            stop._stopped = bool(
                loaded and loaded[-1].kind == "stop")
            if stop._stopped:
                stop._reset_event.clear()
            else:
                stop._reset_event.set()
            # Counters: honour the payload if present, else recompute.
            stop._trigger_count = int(
                data.get("trigger_count",
                         sum(1 for e in loaded if e.kind == "stop")))
            stop._reset_count = int(
                data.get("reset_count",
                         sum(1 for e in loaded if e.kind == "reset")))
            stop._blocked_check_count = int(
                data.get("blocked_check_count", 0))
        return stop

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        audit_log: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "EmergencyStop":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EmergencyStopValidationError(
                f"Invalid JSON payload: {exc}") from exc
        return cls.from_dict(
            data, audit_log=audit_log, metrics_hook=metrics_hook)

    # ------------------------------------------------------------------
    # Internal: safe logging + metric + audit emitters
    # ------------------------------------------------------------------

    def _safe_log(
        self, level: int, fmt: str, *args: Any,
    ) -> None:
        """
        Emit a log record without letting the handler's failure
        propagate (P0 2.8). If a caller has attached a broken
        handler, the emergency stop still records its state.
        """
        try:
            logger.log(level, fmt, *args)
        except Exception:  # noqa: BLE001
            try:
                logger.debug(
                    "log handler raised; emergency stop unaffected",
                    exc_info=True)
            except Exception:  # noqa: BLE001
                pass

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
        """
        Best-effort audit-log emission (P1 3.8).

        If the injected ``audit_log`` has an ``append`` method, we
        try calling it with the payload. Failures are logged and
        swallowed — a broken audit log must not prevent the stop
        from being recorded.
        """
        if self._audit_log is None:
            return
        append_fn = getattr(self._audit_log, "append", None)
        if not callable(append_fn):
            return
        try:
            append_fn(dict(payload))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "audit_log.append failed for emergency-stop event: %s",
                exc)

    # ------------------------------------------------------------------
    # Pickling (P1 3.9)
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_reset_event", None)
        state.pop("_audit_log", None)
        state.pop("_metrics_hook", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._reset_event = threading.Event()
        if not self._stopped:
            self._reset_event.set()
        self._audit_log = None
        self._metrics_hook = None

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.events())

    def __repr__(self) -> str:
        with self._lock:
            latest = self.current_stop_event()
            extra = ""
            if self._stopped and latest is not None:
                extra = (
                    f", reason={latest.reason!r}, "
                    f"actor={latest.actor!r}")
            return (
                f"EmergencyStop("
                f"version={__version__!r}, "
                f"stopped={self._stopped}, "
                f"triggers={self._trigger_count}, "
                f"resets={self._reset_count}"
                f"{extra})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "LEDGER_SCHEMA_VERSION",
    "EmergencyStop",
    "EmergencyStopActiveError",
    "EmergencyStopError",
    "EmergencyStopValidationError",
    "EventKind",
    "StopClassification",
    "StopEvent",
    "StopReason",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import pickle
    import tempfile
    import threading
    import time
    from dataclasses import FrozenInstanceError
    from datetime import timedelta

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== emergency_stop v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — check() exists and works
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: check() / is_stopped() symmetry ===")
    stop = EmergencyStop()
    assert stop.check() is True
    assert stop.is_stopped() is False

    stop.trigger("oom")
    assert stop.check() is False
    assert stop.is_stopped() is True

    stop.reset()
    assert stop.check() is True
    assert stop.is_stopped() is False
    print(f"  ✓ check() and is_stopped() are symmetric across the "
          f"lifecycle")

    # ---------------------------------------------------------------- #
    # P0 2.2 — reset() records an event
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: reset() records lifecycle ===")
    stop = EmergencyStop()
    stop.trigger("oom", actor="alice")
    stop.reset(actor="bob", reason="resolved")
    events = stop.events()
    assert len(events) == 2
    assert events[0]["kind"] == "stop"
    assert events[0]["actor"] == "alice"
    assert events[1]["kind"] == "reset"
    assert events[1]["actor"] == "bob"
    assert events[1]["reason"] == "resolved"
    print(f"  ✓ reset() recorded: kind={events[1]['kind']!r}, "
          f"actor={events[1]['actor']!r}")

    # reasons() still returns only stops (backward compat).
    reasons = stop.reasons()
    assert len(reasons) == 1
    assert reasons[0]["reason"] == "oom"
    print(f"  ✓ reasons() returns only stops (v1.0.0 shape)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — StopEvent validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: StopEvent validation ===")

    bad_kwargs = [
        dict(reason=""),
        dict(reason="  "),
        dict(reason=None),
        dict(reason="ok", actor=""),
        dict(reason="ok", actor="  "),
        dict(reason="ok", at="not-a-datetime"),
        dict(reason="ok", kind="garbage"),
        dict(reason="ok", classification="garbage"),
        dict(reason="ok", scope=""),
        dict(reason="ok", occurrences=0),
    ]
    for kwargs in bad_kwargs:
        try:
            StopEvent(**kwargs)
        except EmergencyStopValidationError:
            pass
        else:
            raise AssertionError(
                f"P0 2.3 regression: {kwargs!r} accepted")
    print(f"  ✓ {len(bad_kwargs)} invalid constructions rejected")

    # Naive datetime normalized to UTC.
    naive = StopEvent(reason="r", at=datetime(2024, 1, 1))
    assert naive.at.tzinfo is not None
    assert naive.at.tzinfo == timezone.utc
    print(f"  ✓ naive datetime → UTC aware")

    # Whitespace stripped from reason and actor.
    ev = StopEvent(reason="  OOM  ", actor="  Alice  ")
    assert ev.reason == "OOM"
    assert ev.actor == "alice"
    print(f"  ✓ whitespace stripped, actor lowercased")

    # ---------------------------------------------------------------- #
    # P0 2.4 — reset() takes actor
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: reset(actor=) ===")
    stop = EmergencyStop()
    stop.trigger("r", actor="alice")
    stop.reset(actor="bob", reason="recovered")
    latest = stop.events()[-1]
    assert latest["kind"] == "reset"
    assert latest["actor"] == "bob"
    assert latest["reason"] == "recovered"
    print(f"  ✓ reset recorded: actor=bob, reason=recovered")

    # ---------------------------------------------------------------- #
    # P0 2.5 — error hierarchy
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: error hierarchy ===")
    assert issubclass(EmergencyStopError, ValueError)
    assert issubclass(
        EmergencyStopValidationError, EmergencyStopError)
    assert issubclass(
        EmergencyStopActiveError, EmergencyStopError)
    print(f"  ✓ 3 error types subclass EmergencyStopError "
          f"(and ValueError)")

    # ---------------------------------------------------------------- #
    # P0 2.6 — current_stop_event()
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: current_stop_event() ===")
    stop = EmergencyStop()
    assert stop.current_stop_event() is None
    stop.trigger("A")
    assert stop.current_stop_event().reason == "A"
    stop.reset()
    assert stop.current_stop_event() is None
    stop.trigger("B")
    assert stop.current_stop_event().reason == "B"
    print(f"  ✓ returns active stop; None after reset")

    # ---------------------------------------------------------------- #
    # P0 2.7 — reason/actor normalization
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: reason/actor normalization ===")
    stop = EmergencyStop()
    stop.trigger("  OOM  ", actor="  ALICE  ")
    record = stop.reasons()[0]
    assert record["reason"] == "OOM"
    assert record["actor"] == "alice"
    print(f"  ✓ '  OOM  ' → 'OOM'; '  ALICE  ' → 'alice'")

    # ---------------------------------------------------------------- #
    # P0 2.8 — logger failure doesn't block trigger
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: log failure doesn't block trigger ===")

    class _BrokenHandler(logging.Handler):
        def emit(self, record):
            raise RuntimeError("simulated log handler failure")

    broken = _BrokenHandler()
    logger.addHandler(broken)
    try:
        stop = EmergencyStop()
        # Should NOT raise — the log emission is best-effort.
        stop.trigger("r", actor="alice")
        assert stop.is_stopped() is True
        print(f"  ✓ trigger() succeeded despite broken handler")
    finally:
        logger.removeHandler(broken)

    # ---------------------------------------------------------------- #
    # P1 3.1 — assert_not_stopped()
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: assert_not_stopped() ===")
    stop = EmergencyStop()
    stop.assert_not_stopped()   # no-op when running
    stop.trigger("r", actor="alice")
    try:
        stop.assert_not_stopped(context="batch pipeline")
    except EmergencyStopActiveError as exc:
        assert "alice" in str(exc)
        assert "batch pipeline" in str(exc)
        print(f"  ✓ raised with reason/actor/context")
    else:
        raise AssertionError(
            "P1 3.1 regression: no exception raised")

    # ---------------------------------------------------------------- #
    # P1 3.2 — wait_until_reset()
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: wait_until_reset() ===")
    stop = EmergencyStop()
    # Fast path: running → returns immediately.
    assert stop.wait_until_reset(timeout=0.1) is True
    # Stop then wait — a background thread will reset.
    stop.trigger("r")
    def _reset_soon():
        time.sleep(0.05)
        stop.reset(actor="bg")
    threading.Thread(target=_reset_soon, daemon=True).start()
    t0 = time.perf_counter()
    result = stop.wait_until_reset(timeout=1.0)
    elapsed = time.perf_counter() - t0
    assert result is True
    assert 0.03 <= elapsed <= 0.5, elapsed
    print(f"  ✓ returned True after {elapsed*1000:.0f}ms reset")

    # Timeout path.
    stop2 = EmergencyStop()
    stop2.trigger("r")
    assert stop2.wait_until_reset(timeout=0.05) is False
    print(f"  ✓ timeout returns False")

    # ---------------------------------------------------------------- #
    # P1 3.3 — StopClassification
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: classification ===")
    stop = EmergencyStop()
    stop.trigger("chaos test", classification="chaos")
    rec = stop.reasons()[0]
    # Classification isn't part of reasons() shape (v1.0.0 compat);
    # but it IS part of events().
    ev = stop.events()[-1]
    assert ev["classification"] == "chaos"
    print(f"  ✓ classification='chaos' recorded")

    stop.reset()
    stop.trigger("human at risk", classification="safety")
    assert stop.events()[-1]["classification"] == "safety"
    print(f"  ✓ classification='safety' recorded")

    # ---------------------------------------------------------------- #
    # P1 3.5 — version + __all__
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: version constants ===")
    assert __version__ == "2.0.0"
    assert LEDGER_SCHEMA_VERSION == "5.0.0"
    import sys
    mod = sys.modules[__name__]
    assert hasattr(mod, "__all__")
    assert "__version__" in mod.__all__
    print(f"  ✓ __version__={__version__!r}, "
          f"schema={LEDGER_SCHEMA_VERSION!r}")

    # ---------------------------------------------------------------- #
    # P1 3.6 — to_dict / from_dict
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: serialization ===")
    stop = EmergencyStop()
    stop.trigger("A", actor="alice")
    stop.reset(actor="bob")
    stop.trigger("B")

    d = stop.to_dict()
    assert d["stopped"] is True
    assert len(d["events"]) == 3

    restored = EmergencyStop.from_dict(d)
    assert restored.is_stopped() is True
    assert len(restored) == 3
    assert restored.current_stop_event().reason == "B"
    print(f"  ✓ round-trip: stopped={restored.is_stopped()}, "
          f"events={len(restored)}")

    # JSON round-trip.
    restored2 = EmergencyStop.from_json(stop.to_json())
    assert restored2.to_dict() == d
    print(f"  ✓ JSON round-trip works")

    # ---------------------------------------------------------------- #
    # P1 3.7 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: metrics_hook ===")
    events_seen = []
    stop = EmergencyStop(
        metrics_hook=lambda ev, f: events_seen.append(ev))
    stop.trigger("r")
    stop.check()
    stop.reset()
    names = set(events_seen)
    assert "stop_triggered" in names
    assert "stop_check_failed" in names
    assert "stop_reset" in names
    print(f"  ✓ events: {sorted(names)}")

    # ---------------------------------------------------------------- #
    # P1 3.8 — audit_log integration
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: audit_log integration ===")
    audit_events = []

    class _MockAuditLog:
        def append(self, payload):
            audit_events.append(payload)

    stop = EmergencyStop(audit_log=_MockAuditLog())
    stop.trigger("A", actor="alice")
    stop.reset(actor="bob")
    assert len(audit_events) == 2
    assert audit_events[0]["event"] == "emergency_stop_triggered"
    assert audit_events[1]["event"] == "emergency_stop_reset"
    print(f"  ✓ audit_log received {len(audit_events)} events")

    # Broken audit_log should not break trigger.
    class _BrokenAuditLog:
        def append(self, payload):
            raise RuntimeError("simulated audit failure")

    stop = EmergencyStop(audit_log=_BrokenAuditLog())
    stop.trigger("r")     # should not raise
    assert stop.is_stopped() is True
    print(f"  ✓ broken audit_log swallowed; trigger succeeded")

    # ---------------------------------------------------------------- #
    # P1 3.9 — pickle
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: pickle ===")
    stop = EmergencyStop()
    stop.trigger("A", actor="alice")
    stop.reset(actor="bob")
    blob = pickle.dumps(stop)
    restored = pickle.loads(blob)
    assert len(restored) == 2
    assert restored.check() is True
    # After unpickle, trigger works.
    restored.trigger("B")
    assert restored.is_stopped() is True
    print(f"  ✓ pickle round-trip, still functional")

    # ---------------------------------------------------------------- #
    # P1 3.10 — __repr__
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: __repr__ ===")
    stop = EmergencyStop()
    stop.trigger("oom", actor="alice")
    r = repr(stop)
    assert "stopped=True" in r
    assert "reason='oom'" in r
    assert "actor='alice'" in r
    print(f"  ✓ {r[:80]}")

    # ---------------------------------------------------------------- #
    # P1 3.11 — __len__
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.11: __len__ ===")
    stop = EmergencyStop()
    assert len(stop) == 0
    stop.trigger("A")
    assert len(stop) == 1
    stop.reset()
    assert len(stop) == 2
    print(f"  ✓ len reflects total events")

    # ---------------------------------------------------------------- #
    # P1 3.12 — events_between()
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.12: events_between() ===")
    stop = EmergencyStop()
    stop.trigger("A")
    stop.reset()
    stop.trigger("B")

    now = datetime.now(timezone.utc)
    one_second_ago = now - timedelta(seconds=1)
    one_second_later = now + timedelta(seconds=1)
    events_in_range = stop.events_between(
        one_second_ago, one_second_later)
    assert len(events_in_range) == 3
    print(f"  ✓ 3 events in [now-1s, now+1s)")

    # Half-open: [start, end) — an event exactly at start is included,
    # one exactly at end is excluded. Hard to test with sub-ms precision,
    # so just check the empty range:
    empty_range = stop.events_between(
        now + timedelta(seconds=10),
        now + timedelta(seconds=20))
    assert empty_range == []
    print(f"  ✓ empty range returns []")

    # ---------------------------------------------------------------- #
    # P1 3.14 — occurrence counting
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.14: occurrence counting ===")
    stop = EmergencyStop()
    stop.trigger("OOM")
    stop.trigger("OOM")
    stop.trigger("OOM")
    events = stop.events()
    assert len(events) == 1
    assert events[0]["occurrences"] == 3
    print(f"  ✓ 3 identical triggers → 1 event with "
          f"occurrences=3")

    # Different actor → separate event.
    stop.trigger("OOM", actor="alice")
    events = stop.events()
    assert len(events) == 2
    print(f"  ✓ different actor → separate event")

    # Reset breaks dedup chain.
    stop.reset()
    stop.trigger("OOM")
    events = stop.events()
    assert len(events) == 4   # stop, stop-alice, reset, stop-OOM
    assert events[-1]["occurrences"] == 1
    print(f"  ✓ reset breaks dedup chain; new stop starts fresh")

    # ---------------------------------------------------------------- #
    # P1 3.16 — spurious reset distinguished
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.16: spurious reset ===")
    stop = EmergencyStop()
    # reset() on running system returns False, records no event.
    changed = stop.reset(actor="whoever")
    assert changed is False
    assert len(stop) == 0
    print(f"  ✓ reset() on running system is a no-op "
          f"(no event recorded)")

    # Real reset returns True.
    stop.trigger("r")
    changed = stop.reset(actor="bob")
    assert changed is True
    assert len(stop) == 2
    print(f"  ✓ real reset returns True and records event")

    # ---------------------------------------------------------------- #
    # Statistics
    # ---------------------------------------------------------------- #
    print("\n=== statistics() ===")
    stop = EmergencyStop()
    stop.trigger("A")
    stop.check()          # blocked
    stop.check()          # blocked
    stop.reset()
    stop.trigger("B")
    stats = stop.statistics()
    assert stats["trigger_count"] == 2
    assert stats["reset_count"] == 1
    assert stats["blocked_check_count"] == 2
    assert stats["stopped"] is True
    assert stats["current_reason"] == "B"
    print(f"  ✓ statistics: triggers={stats['trigger_count']}, "
          f"resets={stats['reset_count']}, "
          f"blocked_checks={stats['blocked_check_count']}")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 API
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat ===")
    stop = EmergencyStop()
    stop.trigger("r", "alice")   # positional args
    assert stop.is_stopped() is True
    reasons = stop.reasons()
    assert reasons[0]["reason"] == "r"
    assert reasons[0]["actor"] == "alice"
    assert "at" in reasons[0]
    stop.reset()
    assert stop.is_stopped() is False
    print(f"  ✓ v1.0.0 positional trigger + reasons shape work")

    # StopReason alias.
    from sys import modules
    mod = modules[__name__]
    assert mod.StopReason is mod.StopEvent
    print(f"  ✓ StopReason alias → StopEvent")

    # Frozen dataclass.
    ev = StopEvent(reason="r")
    try:
        ev.reason = "changed"   # type: ignore[misc]
    except FrozenInstanceError:
        print(f"  ✓ StopEvent is frozen")
    else:
        raise AssertionError(
            "Regression: StopEvent is mutable")

    print(f"\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
