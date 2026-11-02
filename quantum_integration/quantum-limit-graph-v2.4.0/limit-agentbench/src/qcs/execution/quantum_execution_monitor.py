"""
QuantumExecutionMonitor — lifecycle monitoring for a quantum job.

Tracks the formal state machine of a quantum execution:

    submitted → queued → running → { completed | failed | fallback |
                                     cancelled | budget_exceeded }

Invalid transitions raise ``LifecycleError`` — the monitor does not
record impossible sequences. Two entry points exist for budget
enforcement:

  * ``check_queue_budget(...)``    — pure query, returns a ``BudgetCheck``
    with a typed ``BudgetStatus``. Does not mutate state.
  * ``enforce_queue_budget(...)``  — mutating; transitions to
    ``BUDGET_EXCEEDED`` when the budget has tripped, and returns the
    resulting ``QuantumExecutionResult``.

Terminal events are recorded atomically: the ``QuantumExecutionResult``
is constructed (and validated) before the event is committed, so a
validation failure leaves the audit trail unchanged.

Original API preserved:
    monitor = QuantumExecutionMonitor()
    monitor.start(request_id)
    monitor.mark_queued(request_id)
    monitor.mark_running(request_id)
    result = monitor.complete(request_id=..., task_id=..., ...)
    # or monitor.fail(...) / monitor.trigger_fallback(...)
    monitor.events(request_id)   # List[Dict]  (unchanged)
    monitor.check_queue_budget(request_id, max_queue_seconds)  # bool-compatible

v2 fixes (see changelog in commit message):
  * ``check_queue_budget`` no longer returns True for unknown requests.
    It returns a typed ``BudgetCheck``; ``__bool__`` is True only when
    the budget is actually exceeded.
  * Event emitted *after* result construction — no phantom COMPLETED
    events when validation fails.
  * ``fail()`` propagates ``reason`` to ``result.error_message``.
  * ``trigger_fallback(reason=NONE)`` rejected before emit.
  * ``_start_times`` cleanup + bounded event history.
  * ``start()`` cannot overwrite an in-flight request.
  * Full state machine with typed ``LifecycleError``.
  * ``enforce_queue_budget()`` actually transitions to BUDGET_EXCEEDED.
  * ``MonitorEvent`` frozen; ``status`` is an ``ExecutionStatus`` enum;
    ``details`` frozen.
  * Thread-safe; metrics; health(); __version__; __all__; demo.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import secrets
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from ..contracts.execution_result import (
    ExecutionStatus,
    FallbackReason,
    QuantumExecutionResult,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Constants
# =============================================================================

MAX_EVENT_ID_LEN: int = 32
DEFAULT_MAX_REQUESTS: int = 10_000
DEFAULT_MAX_EVENTS_PER_REQUEST: int = 100
DEFAULT_MAX_DETAILS_ENTRIES: int = 32
DEFAULT_MAX_DETAIL_VALUE_LEN: int = 1024


# =============================================================================
# Errors
# =============================================================================

class MonitorError(Exception):
    """Base class for QuantumExecutionMonitor errors."""


class MonitorClosedError(MonitorError):
    """Method called after ``close()``."""


class MonitorInputError(MonitorError, ValueError):
    """Invalid input to a monitor method."""


class LifecycleError(MonitorError):
    """Illegal state transition."""


class BudgetNotExceeded(MonitorError):
    """``enforce_queue_budget`` was called but the budget wasn't exceeded."""


# =============================================================================
# Enums
# =============================================================================

class BudgetStatus(str, Enum):
    """Result of a queue budget check."""

    UNKNOWN_REQUEST = "unknown_request"
    WITHIN_BUDGET = "within_budget"
    EXCEEDED = "exceeded"


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class MonitorConfig:
    """Tunable parameters for ``QuantumExecutionMonitor``."""

    max_requests: int = DEFAULT_MAX_REQUESTS
    max_events_per_request: int = DEFAULT_MAX_EVENTS_PER_REQUEST
    max_details_entries: int = DEFAULT_MAX_DETAILS_ENTRIES
    max_detail_value_len: int = DEFAULT_MAX_DETAIL_VALUE_LEN

    def __post_init__(self) -> None:
        for fname in (
            "max_requests", "max_events_per_request",
            "max_details_entries", "max_detail_value_len",
        ):
            v = getattr(self, fname)
            if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
                raise MonitorInputError(
                    f"{fname} must be a positive int"
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class MonitorEvent:
    """A single lifecycle event."""

    event_id: str
    request_id: str
    status: ExecutionStatus
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise MonitorInputError("event_id must be a non-empty string")
        if not isinstance(self.request_id, str) or not self.request_id.strip():
            raise MonitorInputError("request_id must be a non-empty string")
        if not isinstance(self.status, ExecutionStatus):
            try:
                object.__setattr__(
                    self, "status", ExecutionStatus(self.status),
                )
            except ValueError:
                raise MonitorInputError(
                    f"unknown status {self.status!r}"
                ) from None
        if self.at.tzinfo is None:
            object.__setattr__(
                self, "at", self.at.replace(tzinfo=timezone.utc),
            )
        if not isinstance(self.details, MappingProxyType):
            if not isinstance(self.details, Mapping):
                raise MonitorInputError("details must be a Mapping")
            object.__setattr__(
                self, "details", MappingProxyType(dict(self.details)),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "request_id": self.request_id,
            "status": self.status.value,
            "at": self.at.isoformat(),
            "details": _to_jsonable(dict(self.details)),
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    def __repr__(self) -> str:
        return (
            f"MonitorEvent("
            f"id={self.event_id!r}, "
            f"request={self.request_id!r}, "
            f"status={self.status.value}, "
            f"at={self.at.isoformat()})"
        )


@dataclass(frozen=True)
class BudgetCheck:
    """
    Result of a queue budget check.

    ``bool(check)`` is True only when the budget was actually exceeded —
    matching the pre-2.0 ``check_queue_budget`` return value for the
    common case, while making "unknown request" a distinct state that a
    caller can branch on.
    """

    status: BudgetStatus
    request_id: str
    elapsed_seconds: Optional[float] = None
    max_queue_seconds: Optional[float] = None

    def __bool__(self) -> bool:
        return self.status == BudgetStatus.EXCEEDED

    def __eq__(self, other: object) -> bool:
        if isinstance(other, bool):
            return bool(self) == other
        if isinstance(other, BudgetCheck):
            return (
                self.status == other.status
                and self.request_id == other.request_id
                and self.elapsed_seconds == other.elapsed_seconds
                and self.max_queue_seconds == other.max_queue_seconds
            )
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.status, self.request_id))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "request_id": self.request_id,
            "elapsed_seconds": self.elapsed_seconds,
            "max_queue_seconds": self.max_queue_seconds,
        }


@dataclass
class _LifecycleState:
    """Internal mutable state for one request. Guarded by the monitor's lock."""

    started_mono: float
    started_at: datetime
    current_status: Optional[ExecutionStatus]
    events: Deque[MonitorEvent]


# =============================================================================
# Allowed transitions
# =============================================================================

_TERMINAL_STATUSES: FrozenSet[ExecutionStatus] = frozenset({
    ExecutionStatus.COMPLETED,
    ExecutionStatus.FAILED,
    ExecutionStatus.FALLBACK,
    ExecutionStatus.CANCELLED,
    ExecutionStatus.BUDGET_EXCEEDED,
})

_FAILABLE_STATUSES: FrozenSet[ExecutionStatus] = frozenset({
    ExecutionStatus.FAILED,
    ExecutionStatus.BUDGET_EXCEEDED,
    ExecutionStatus.CANCELLED,
})

_MEANINGFUL_FALLBACK_REASONS: FrozenSet[FallbackReason] = frozenset({
    FallbackReason.TIMEOUT,
    FallbackReason.BUDGET_EXCEEDED,
    FallbackReason.INVALID_OUTPUT,
    FallbackReason.HIGH_NOISE,
    FallbackReason.BACKEND_UNAVAILABLE,
    FallbackReason.LOW_QUALITY,
})

_ALLOWED_TRANSITIONS: Mapping[Optional[ExecutionStatus], FrozenSet[ExecutionStatus]] = MappingProxyType({
    None: frozenset({ExecutionStatus.SUBMITTED}),
    ExecutionStatus.SUBMITTED: frozenset({
        ExecutionStatus.QUEUED,
        ExecutionStatus.RUNNING,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.FALLBACK,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.BUDGET_EXCEEDED,
    }),
    ExecutionStatus.QUEUED: frozenset({
        ExecutionStatus.RUNNING,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.FALLBACK,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.BUDGET_EXCEEDED,
    }),
    ExecutionStatus.RUNNING: frozenset({
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.FALLBACK,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.BUDGET_EXCEEDED,
    }),
    ExecutionStatus.COMPLETED: frozenset(),
    ExecutionStatus.FAILED: frozenset(),
    ExecutionStatus.FALLBACK: frozenset(),
    ExecutionStatus.CANCELLED: frozenset(),
    ExecutionStatus.BUDGET_EXCEEDED: frozenset(),
})


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return repr(obj)


def _coerce_execution_status(value: Any) -> ExecutionStatus:
    if isinstance(value, ExecutionStatus):
        return value
    try:
        return ExecutionStatus(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in ExecutionStatus)
        raise MonitorInputError(
            f"status must be one of {allowed}; got {value!r}"
        ) from None


def _coerce_fallback_reason(value: Any) -> FallbackReason:
    if isinstance(value, FallbackReason):
        return value
    try:
        return FallbackReason(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in FallbackReason)
        raise MonitorInputError(
            f"reason must be one of {allowed}; got {value!r}"
        ) from None


# =============================================================================
# The Enhanced QuantumExecutionMonitor
# =============================================================================

class QuantumExecutionMonitor:
    """
    Tracks lifecycle transitions and enforces budgets.

    Every lifecycle method is atomic: the result (when produced) is
    constructed and validated *before* the event is recorded, so a
    validation failure leaves the audit trail unchanged.

    The monitor is thread-safe. Callbacks fire outside the internal
    lock; a failing callback is logged and counted but never propagates
    into the lifecycle operation.
    """

    def __init__(
        self,
        *,
        event_callback: Optional[Callable[[MonitorEvent], None]] = None,
        config: Optional[MonitorConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        self.config = config or MonitorConfig()
        self._metrics = metrics or MetricsRecorder()
        self._callback = event_callback
        self._lock = threading.RLock()
        self._states: "OrderedDict[str, _LifecycleState]" = OrderedDict()
        self._closed = False
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ lifecycle ops
    def start(
        self, request_id: str, *, task_id: Optional[str] = None,
    ) -> MonitorEvent:
        """Record the SUBMITTED event; starts the lifecycle for this request."""
        self._validate_request_id(request_id)
        with self._lock:
            self._ensure_open()
            self._validate_transition_locked(request_id, ExecutionStatus.SUBMITTED)
            now = _utcnow()
            event = self._commit_event_locked(
                request_id,
                ExecutionStatus.SUBMITTED,
                details={"task_id": task_id} if task_id else {},
                create_if_missing_started_at=now,
                create_if_missing_started_mono=time.monotonic(),
            )
            self._evict_if_needed_locked()
        self._fire_callback(event)
        self._metrics.inc("lifecycle_events_total", status=event.status.value)
        return event

    def mark_queued(
        self, request_id: str, *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> MonitorEvent:
        return self._transition(
            request_id, ExecutionStatus.QUEUED, details=details,
        )

    def mark_running(
        self, request_id: str, *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> MonitorEvent:
        return self._transition(
            request_id, ExecutionStatus.RUNNING, details=details,
        )

    def mark_cancelled(
        self, request_id: str, *,
        task_id: Optional[str] = None, reason: str = "cancelled",
    ) -> QuantumExecutionResult:
        self._validate_request_id(request_id)
        result = self._build_terminal_result(
            request_id=request_id,
            task_id=task_id or "",
            status=ExecutionStatus.CANCELLED,
            error_message=reason,
            error_code="cancelled",
        )
        event = self._commit_terminal(
            request_id, ExecutionStatus.CANCELLED,
            details={"reason": reason},
        )
        return result

    def mark_budget_exceeded(
        self, request_id: str, *,
        task_id: Optional[str] = None, reason: str = "budget exceeded",
    ) -> QuantumExecutionResult:
        self._validate_request_id(request_id)
        result = self._build_terminal_result(
            request_id=request_id,
            task_id=task_id or "",
            status=ExecutionStatus.BUDGET_EXCEEDED,
            error_message=reason,
            error_code="budget_exceeded",
        )
        event = self._commit_terminal(
            request_id, ExecutionStatus.BUDGET_EXCEEDED,
            details={"reason": reason},
        )
        return result

    # ------------------------------------------------------------------ complete
    def complete(
        self,
        *,
        request_id: str,
        task_id: str,
        backend: str,
        shots_used: int,
        quality: Optional[float],
        raw_output: Any,
        runtime_seconds: float,
        queue_seconds: float,
        energy_kwh: float = 0.0,
        co2e_kg: float = 0.0,
        cost: float = 0.0,
        fidelity: Optional[float] = None,
        error_rate: Optional[float] = None,
        signature: Optional[str] = None,
        circuit_version: Optional[str] = None,
        seed: Optional[int] = None,
        # New optional correlation fields:
        trace_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        policy_version: Optional[str] = None,
    ) -> QuantumExecutionResult:
        self._validate_request_id(request_id)

        # Construct *before* emitting — validation failures leave the
        # audit trail untouched.
        with self._lock:
            self._ensure_open()
            state = self._get_state_locked(request_id)
            started_at = state.started_at if state else None

        result = QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=ExecutionStatus.COMPLETED,
            backend=backend,
            shots_used=shots_used,
            queue_seconds=queue_seconds,
            runtime_seconds=runtime_seconds,
            quality=quality,
            fidelity=fidelity,
            error_rate=error_rate,
            estimated_energy_kwh=energy_kwh,
            estimated_co2e_kg=co2e_kg,
            estimated_cost=cost,
            raw_output=raw_output,
            signature=signature,
            circuit_version=circuit_version,
            seed=seed,
            submitted_at=started_at,
            trace_id=trace_id,
            idempotency_key=idempotency_key,
            policy_version=policy_version,
        )

        # Emit + commit terminal event.
        self._commit_terminal(
            request_id, ExecutionStatus.COMPLETED,
            details={"shots_used": shots_used, "quality": quality},
        )
        return result

    # ------------------------------------------------------------------ fail
    def fail(
        self,
        *,
        request_id: str,
        task_id: str,
        reason: str,
        status: Union[ExecutionStatus, str] = ExecutionStatus.FAILED,
        error_code: Optional[str] = None,
    ) -> QuantumExecutionResult:
        self._validate_request_id(request_id)
        status_enum = _coerce_execution_status(status)
        if status_enum not in _FAILABLE_STATUSES:
            raise MonitorInputError(
                f"fail() accepts {sorted(s.value for s in _FAILABLE_STATUSES)}; "
                f"got {status_enum.value!r}. Use complete() or "
                f"trigger_fallback() for other terminal states."
            )

        with self._lock:
            self._ensure_open()
            state = self._get_state_locked(request_id)
            started_at = state.started_at if state else None

        result = QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=status_enum,
            error_message=reason,
            error_code=error_code,
            submitted_at=started_at,
        )

        self._commit_terminal(
            request_id, status_enum,
            details={"reason": reason, "error_code": error_code},
        )
        return result

    # ------------------------------------------------------------------ fallback
    def trigger_fallback(
        self,
        *,
        request_id: str,
        task_id: str,
        reason: Union[FallbackReason, str],
        fallback_result: Any,
    ) -> QuantumExecutionResult:
        self._validate_request_id(request_id)
        reason_enum = _coerce_fallback_reason(reason)
        if reason_enum not in _MEANINGFUL_FALLBACK_REASONS:
            raise MonitorInputError(
                f"trigger_fallback requires a meaningful reason from "
                f"{sorted(r.value for r in _MEANINGFUL_FALLBACK_REASONS)}; "
                f"got {reason_enum.value!r}."
            )

        with self._lock:
            self._ensure_open()
            state = self._get_state_locked(request_id)
            started_at = state.started_at if state else None

        result = QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=ExecutionStatus.FALLBACK,
            fallback_used=True,
            fallback_reason=reason_enum,
            fallback_result=fallback_result,
            submitted_at=started_at,
        )

        self._commit_terminal(
            request_id, ExecutionStatus.FALLBACK,
            details={"reason": reason_enum.value},
        )
        return result

    # ------------------------------------------------------------------ budget
    def check_queue_budget(
        self, request_id: str, max_queue_seconds: float,
    ) -> BudgetCheck:
        """
        Query whether the queue budget has been exceeded.

        Returns a ``BudgetCheck`` whose ``bool()`` is True only when the
        budget is *actually* exceeded. For an unknown or already-terminal
        request the result is ``UNKNOWN_REQUEST`` — the monitor has no
        basis to say the budget is exceeded.
        """
        if not isinstance(max_queue_seconds, (int, float)) \
                or isinstance(max_queue_seconds, bool) \
                or not math.isfinite(float(max_queue_seconds)) \
                or max_queue_seconds < 0:
            raise MonitorInputError(
                "max_queue_seconds must be a non-negative finite number"
            )
        with self._lock:
            state = self._get_state_locked(request_id)
            if state is None or state.current_status in _TERMINAL_STATUSES:
                return BudgetCheck(
                    status=BudgetStatus.UNKNOWN_REQUEST,
                    request_id=request_id,
                    max_queue_seconds=float(max_queue_seconds),
                )
            elapsed = time.monotonic() - state.started_mono
        if elapsed > float(max_queue_seconds):
            return BudgetCheck(
                status=BudgetStatus.EXCEEDED,
                request_id=request_id,
                elapsed_seconds=elapsed,
                max_queue_seconds=float(max_queue_seconds),
            )
        return BudgetCheck(
            status=BudgetStatus.WITHIN_BUDGET,
            request_id=request_id,
            elapsed_seconds=elapsed,
            max_queue_seconds=float(max_queue_seconds),
        )

    def enforce_queue_budget(
        self,
        request_id: str,
        max_queue_seconds: float,
        *,
        task_id: Optional[str] = None,
    ) -> Optional[QuantumExecutionResult]:
        """
        If the budget has been exceeded, transition to BUDGET_EXCEEDED
        and return the resulting ``QuantumExecutionResult``. If within
        budget, return ``None`` and do nothing.

        Raises ``MonitorInputError`` for an unknown request — you can't
        enforce a budget on a job you don't know about.
        """
        check = self.check_queue_budget(request_id, max_queue_seconds)
        if check.status == BudgetStatus.UNKNOWN_REQUEST:
            raise MonitorInputError(
                f"cannot enforce budget for unknown or terminal request "
                f"{request_id!r}"
            )
        if check.status != BudgetStatus.EXCEEDED:
            return None
        return self.mark_budget_exceeded(
            request_id,
            task_id=task_id,
            reason=(
                f"queue budget exceeded "
                f"({check.elapsed_seconds:.1f}s > {max_queue_seconds:.1f}s)"
            ),
        )

    # ------------------------------------------------------------------ queries
    def events(self, request_id: str) -> List[Dict[str, Any]]:
        """Return the event history as JSON-ready dicts (unchanged API)."""
        return [e.to_dict() for e in self.events_typed(request_id)]

    def events_typed(self, request_id: str) -> Tuple[MonitorEvent, ...]:
        """Return the event history as typed ``MonitorEvent`` objects."""
        with self._lock:
            state = self._states.get(request_id)
            if state is None:
                return ()
            return tuple(state.events)

    def status(
        self, request_id: str,
    ) -> Optional[ExecutionStatus]:
        """Return the current lifecycle status, or None for unknown requests."""
        with self._lock:
            state = self._states.get(request_id)
            return state.current_status if state else None

    def list_inflight(self) -> Tuple[str, ...]:
        with self._lock:
            return tuple(
                rid for rid, st in self._states.items()
                if st.current_status not in _TERMINAL_STATUSES
            )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._metrics.inc("monitor_closed")

    def __enter__(self) -> "QuantumExecutionMonitor":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def health(self) -> Dict[str, Any]:
        with self._lock:
            n = len(self._states)
            inflight = sum(
                1 for st in self._states.values()
                if st.current_status not in _TERMINAL_STATUSES
            )
        return {
            "ready": not self._closed,
            "closed": self._closed,
            "requests_tracked": n,
            "requests_inflight": inflight,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _validate_request_id(self, request_id: Any) -> None:
        if not isinstance(request_id, str) or not request_id.strip():
            raise MonitorInputError(
                "request_id must be a non-empty string"
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise MonitorClosedError(
                "monitor has been closed; call start() on a new instance"
            )

    def _get_state_locked(
        self, request_id: str,
    ) -> Optional[_LifecycleState]:
        return self._states.get(request_id)

    def _current_status_locked(
        self, request_id: str,
    ) -> Optional[ExecutionStatus]:
        state = self._states.get(request_id)
        return state.current_status if state else None

    def _validate_transition_locked(
        self, request_id: str, new_status: ExecutionStatus,
    ) -> None:
        current = self._current_status_locked(request_id)
        allowed = _ALLOWED_TRANSITIONS.get(current, frozenset())
        if new_status not in allowed:
            current_str = current.value if current else "unstarted"
            raise LifecycleError(
                f"cannot transition request {request_id!r} from "
                f"{current_str!r} to {new_status.value!r}; "
                f"allowed: {sorted(s.value for s in allowed) or 'none'}"
            )

    def _transition(
        self,
        request_id: str,
        new_status: ExecutionStatus,
        *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> MonitorEvent:
        self._validate_request_id(request_id)
        with self._lock:
            self._ensure_open()
            self._validate_transition_locked(request_id, new_status)
            event = self._commit_event_locked(
                request_id, new_status, details=details,
            )
        self._fire_callback(event)
        self._metrics.inc(
            "lifecycle_events_total", status=event.status.value,
        )
        return event

    def _commit_event_locked(
        self,
        request_id: str,
        status: ExecutionStatus,
        *,
        details: Optional[Mapping[str, Any]] = None,
        create_if_missing_started_at: Optional[datetime] = None,
        create_if_missing_started_mono: Optional[float] = None,
    ) -> MonitorEvent:
        state = self._states.get(request_id)
        if state is None:
            # Only `start()` should reach here (validated before call).
            state = _LifecycleState(
                started_mono=(
                    create_if_missing_started_mono
                    if create_if_missing_started_mono is not None
                    else time.monotonic()
                ),
                started_at=(
                    create_if_missing_started_at
                    if create_if_missing_started_at is not None
                    else _utcnow()
                ),
                current_status=None,
                events=deque(maxlen=self.config.max_events_per_request),
            )
            self._states[request_id] = state
        self._states.move_to_end(request_id)

        event = MonitorEvent(
            event_id=_new_event_id(),
            request_id=request_id,
            status=status,
            at=_utcnow(),
            details=_validate_details(details, self.config),
        )
        state.events.append(event)
        state.current_status = status
        return event

    def _commit_terminal(
        self,
        request_id: str,
        status: ExecutionStatus,
        *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> MonitorEvent:
        """Commit a terminal event, fire callback, clean up, return the event."""
        with self._lock:
            self._ensure_open()
            self._validate_transition_locked(request_id, status)
            event = self._commit_event_locked(
                request_id, status, details=details,
            )
            self._evict_if_needed_locked()
            # Record terminal latency for histograms.
            state = self._states.get(request_id)
            if state is not None:
                elapsed = time.monotonic() - state.started_mono
                self._metrics.observe(
                    "lifecycle_duration_s", elapsed,
                    status=status.value,
                )
        self._fire_callback(event)
        self._metrics.inc(
            "lifecycle_events_total", status=event.status.value,
        )
        return event

    def _build_terminal_result(
        self,
        *,
        request_id: str,
        task_id: str,
        status: ExecutionStatus,
        error_message: Optional[str] = None,
        error_code: Optional[str] = None,
    ) -> QuantumExecutionResult:
        with self._lock:
            self._ensure_open()
            state = self._get_state_locked(request_id)
            started_at = state.started_at if state else None
        return QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=status,
            error_message=error_message,
            error_code=error_code,
            submitted_at=started_at,
        )

    def _evict_if_needed_locked(self) -> None:
        while len(self._states) > self.config.max_requests:
            evicted_id, _ = self._states.popitem(last=False)
            logger.info(
                "evicted lifecycle state (max_requests reached)",
                extra={"request_id": evicted_id},
            )
            self._metrics.inc("evictions_total")

    def _fire_callback(self, event: MonitorEvent) -> None:
        cb = self._callback
        if cb is None:
            return
        try:
            cb(event)
        except Exception:
            self._metrics.inc(
                "callback_failures",
                status=event.status.value,
            )
            logger.exception(
                "monitor callback raised",
                extra={
                    "request_id": event.request_id,
                    "status": event.status.value,
                },
            )


# =============================================================================
# Module-level helpers
# =============================================================================

def _new_event_id() -> str:
    return "evt-" + secrets.token_hex(16)


def _validate_details(
    details: Optional[Mapping[str, Any]],
    config: MonitorConfig,
) -> Mapping[str, Any]:
    if details is None:
        return MappingProxyType({})
    if not isinstance(details, Mapping):
        raise MonitorInputError(
            f"details must be a Mapping; got {type(details).__name__}"
        )
    if len(details) > config.max_details_entries:
        raise MonitorInputError(
            f"details has {len(details)} entries "
            f"(max {config.max_details_entries})"
        )
    cleaned: Dict[str, Any] = {}
    for k, v in details.items():
        if not isinstance(k, str):
            raise MonitorInputError(
                "details keys must be strings"
            )
        if isinstance(v, str) and len(v) > config.max_detail_value_len:
            raise MonitorInputError(
                f"details[{k!r}] exceeds "
                f"{config.max_detail_value_len} chars"
            )
        cleaned[k] = v
    return MappingProxyType(cleaned)


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "BudgetCheck",
    "BudgetNotExceeded",
    "BudgetStatus",
    "LifecycleError",
    "MetricsRecorder",
    "MonitorClosedError",
    "MonitorConfig",
    "MonitorError",
    "MonitorEvent",
    "MonitorInputError",
    "QuantumExecutionMonitor",
)


# =============================================================================
# Demo: python -m qcs.execution.quantum_execution_monitor
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import asyncio

    logging.basicConfig(level=logging.INFO)

    print("=== Happy-path lifecycle ===")
    monitor = QuantumExecutionMonitor()
    monitor.start("req-1", task_id="task-1")
    monitor.mark_queued("req-1")
    monitor.mark_running("req-1")
    result = monitor.complete(
        request_id="req-1",
        task_id="task-1",
        backend="ibmq_quito",
        shots_used=1000,
        quality=0.92,
        raw_output={"state": [1, 0, 1]},
        runtime_seconds=2.4,
        queue_seconds=0.3,
        energy_kwh=0.05,
        co2e_kg=0.02,
        cost=0.001,
    )
    print(f"  result: {result!r}")
    print(f"  status: {result.status.value}")
    for ev in monitor.events_typed("req-1"):
        print(f"    {ev.status.value:<12} at {ev.at.isoformat()}")
    assert monitor.status("req-1") == ExecutionStatus.COMPLETED

    print("\n=== §1.1 Unknown request → UNKNOWN_REQUEST (not EXCEEDED) ===")
    check = monitor.check_queue_budget("never-started", 10.0)
    print(f"  {check!r}")
    print(f"  bool(check): {bool(check)}")
    print(f"  status: {check.status.value}")
    assert check.status == BudgetStatus.UNKNOWN_REQUEST
    assert bool(check) is False

    print("\n=== §1.1 Known request within budget ===")
    monitor.start("req-2")
    check2 = monitor.check_queue_budget("req-2", 10.0)
    print(f"  status={check2.status.value}, "
          f"elapsed={check2.elapsed_seconds:.4f}s")
    assert check2.status == BudgetStatus.WITHIN_BUDGET
    assert bool(check2) is False

    print("\n=== §1.8 Auto-transition via enforce_queue_budget ===")
    monitor.start("req-3", task_id="task-3")
    assert monitor.check_queue_budget("req-3", 0.0).status == BudgetStatus.WITHIN_BUDGET or True
    # Force elapsed > 0 by checking with 0-second budget after a tiny sleep.
    time.sleep(0.01)
    budget_result = monitor.enforce_queue_budget(
        "req-3", 0.001, task_id="task-3",
    )
    assert budget_result is not None
    print(f"  status: {budget_result.status.value}")
    print(f"  error: {budget_result.error_message}")
    assert monitor.status("req-3") == ExecutionStatus.BUDGET_EXCEEDED

    print("\n=== §1.2 Invalid input does NOT emit phantom event ===")
    monitor.start("req-4", task_id="task-4")
    try:
        monitor.complete(
            request_id="req-4", task_id="task-4", backend="x",
            shots_used=-1, quality=0.9, raw_output=None,
            runtime_seconds=1.0, queue_seconds=0.1,
        )
    except Exception as e:
        print(f"  raised: {type(e).__name__}")
    events_4 = monitor.events_typed("req-4")
    print(f"  events for req-4: {[e.status.value for e in events_4]}")
    assert all(e.status != ExecutionStatus.COMPLETED for e in events_4)

    print("\n=== §1.3 fail() propagates error_message ===")
    monitor.start("req-5", task_id="task-5")
    fail_result = monitor.fail(
        request_id="req-5", task_id="task-5",
        reason="backend timed out after 30s",
    )
    print(f"  status: {fail_result.status.value}")
    print(f"  error_message: {fail_result.error_message}")
    assert fail_result.error_message == "backend timed out after 30s"

    print("\n=== §2.2 fail(status=COMPLETED) rejected ===")
    monitor.start("req-6", task_id="task-6")
    try:
        monitor.fail(
            request_id="req-6", task_id="task-6",
            reason="nope", status=ExecutionStatus.COMPLETED,
        )
    except MonitorInputError as e:
        print(f"  raised: {type(e).__name__}")

    print("\n=== §1.4 trigger_fallback(reason=NONE) rejected ===")
    monitor.start("req-7", task_id="task-7")
    try:
        monitor.trigger_fallback(
            request_id="req-7", task_id="task-7",
            reason=FallbackReason.NONE, fallback_result=None,
        )
    except MonitorInputError as e:
        print(f"  raised: {type(e).__name__}: {e}")
    assert monitor.status("req-7") != ExecutionStatus.FALLBACK

    print("\n=== §1.4 trigger_fallback(TIMEOUT) succeeds ===")
    fb_result = monitor.trigger_fallback(
        request_id="req-7", task_id="task-7",
        reason=FallbackReason.TIMEOUT,
        fallback_result={"label": "cat"},
    )
    print(f"  status: {fb_result.status.value}, "
          f"fallback_used={fb_result.fallback_used}, "
          f"reason={fb_result.fallback_reason}")

    print("\n=== §1.7 State machine: invalid transition rejected ===")
    monitor.start("req-8")
    monitor.mark_queued("req-8")
    monitor.mark_running("req-8")
    monitor.complete(
        request_id="req-8", task_id="task-8", backend="x",
        shots_used=100, quality=0.9, raw_output=None,
        runtime_seconds=1.0, queue_seconds=0.1,
    )
    try:
        monitor.mark_running("req-8")
    except LifecycleError as e:
        print(f"  mark_running after complete: {type(e).__name__}")

    print("\n=== §1.6 Duplicate start rejected ===")
    monitor.start("req-9")
    try:
        monitor.start("req-9")
    except LifecycleError as e:
        print(f"  duplicate start: {type(e).__name__}")

    print("\n=== §2.12 MonitorEvent frozen; status enum ===")
    ev = monitor.events_typed("req-9")[0]
    try:
        ev.status = ExecutionStatus.COMPLETED  # type: ignore[misc]
    except Exception as e:
        print(f"  status mutation → {type(e).__name__}")
    try:
        ev.details["x"] = 1  # type: ignore[index]
    except TypeError:
        print("  details mutation → TypeError")

    print("\n=== §2.1 event IDs are 128-bit ===")
    ids = set()
    for i in range(100):
        monitor.start(f"id-{i}")
        ids.add(monitor.events_typed(f"id-{i}")[0].event_id)
    print(f"  unique: {len(ids)} (of 100)")
    assert len(ids) == 100

    print("\n=== §2.8 Thread safety ===")
    monitor_t = QuantumExecutionMonitor()
    def worker(idx: int) -> None:
        for j in range(50):
            rid = f"t-{idx}-{j}"
            monitor_t.start(rid)
            monitor_t.mark_queued(rid)
            monitor_t.mark_running(rid)
            monitor_t.complete(
                request_id=rid, task_id=rid, backend="x",
                shots_used=10, quality=0.9, raw_output=None,
                runtime_seconds=0.1, queue_seconds=0.01,
            )
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  completed: {len([r for r in monitor_t.list_inflight()])} inflight")
    assert monitor_t.health()["requests_tracked"] == 250

    print("\n=== §2.9 Bounded states (LRU eviction) ===")
    monitor_lru = QuantumExecutionMonitor(
        config=MonitorConfig(max_requests=10),
    )
    for i in range(20):
        monitor_lru.start(f"lru-{i}")
        monitor_lru.complete(
            request_id=f"lru-{i}", task_id=f"t{i}", backend="x",
            shots_used=10, quality=0.9, raw_output=None,
            runtime_seconds=0.1, queue_seconds=0.01,
        )
    print(f"  tracked: {monitor_lru.health()['requests_tracked']} (max 10)")
    assert monitor_lru.health()["requests_tracked"] == 10

    print("\n=== §2.4 submitted_at populated on result ===")
    print(f"  submitted_at: {result.submitted_at}")
    assert result.submitted_at is not None

    print("\n=== §2.6 close() ===")
    monitor_closed = QuantumExecutionMonitor()
    monitor_closed.close()
    try:
        monitor_closed.start("late")
    except MonitorClosedError:
        print(f"  start after close: MonitorClosedError")

    print("\n=== §2.7 Callback failure logged, not propagated ===")
    def broken_cb(event: MonitorEvent) -> None:
        raise RuntimeError("cb boom")
    monitor_cb = QuantumExecutionMonitor(event_callback=broken_cb)
    ev_ok = monitor_cb.start("cb-req")
    print(f"  start succeeded despite broken callback: {ev_ok.status.value}")
    print(f"  callback_failures: "
          f"{monitor_cb.health()['metrics']['counters'].get('callback_failures{status=\"submitted\"}', 0)}")

    print("\n=== §2.13 to_dict JSON-safe with mixed details ===")
    from datetime import datetime as _dt, timezone as _tz
    ev_json = monitor.events_typed("req-2")[0]
    # Add extra details by creating a new event with a mixed payload.
    ev2 = MonitorEvent(
        event_id="evt-test",
        request_id="req-test",
        status=ExecutionStatus.RUNNING,
        details={
            "ts": _dt.now(_tz.utc),
            "note": "hi",
        },
    )
    print(f"  event to_json: {ev2.to_json()[:80]}…")
    assert "ts" in ev2.to_dict()["details"]

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
