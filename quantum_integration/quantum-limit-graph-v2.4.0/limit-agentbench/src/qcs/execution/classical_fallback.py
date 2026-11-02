"""
ClassicalFallback — safe, typed fallback to classical execution.

When a quantum job times out, exceeds budget, or produces invalid
output, the caller invokes the fallback with a ``FallbackReason``. The
fallback looks up the plan registered for the task type, verifies the
reason is covered, and runs the classical callable with a bounded
timeout.

Two guarantees this module actually enforces:

  * **Bounded runtime** — every execution is wrapped in a timeout. A
    hung classical solver cannot stall the caller.
  * **Typed outcome** — ``FallbackResult`` is a frozen dataclass with a
    ``status`` enum. Callers branch on ``status``; they never have to
    guess which keys exist in a dict.

Two guarantees this module does **not** enforce (and does not claim to):

  * **Callable determinism** — the caller's classical callable may use
    RNG, wall-clock time, or network I/O. If a deterministic fallback is
    required, that's the caller's responsibility.
  * **Forced cancellation** — Python threads cannot be interrupted. When
    a sync callable times out, the caller receives ``FallbackStatus.TIMEOUT``
    but the underlying thread continues running until the callable
    returns. Use async callables for cooperative cancellation, or
    bounded-time classical solvers.

Original API preserved:
    fallback = ClassicalFallback()
    fallback.register("classify", classical_callable=fn)
    result = fallback.execute("classify", task=t, reason=FallbackReason.TIMEOUT)
    result["fallback_available"] / result["fallback_failed"] / result["result"]

v2 fixes (see changelog in commit message):
  * Typed ``FallbackResult`` + ``FallbackStatus`` (was an unstructured dict
    with three branches).
  * No more ``except Exception`` swallowing ``CancelledError``; failures
    preserve traceback + exception class.
  * Bounded execution — every call runs under a timeout.
  * ``register()`` validates inputs; duplicate registration is
    controllable.
  * ``FallbackPlan`` is frozen; ``reason: str = "registered"`` (a dead
    lie) replaced with ``reasons: FrozenSet[FallbackReason]`` (real
    per-reason dispatch) and a deprecated ``reason`` property.
  * ``estimated_cost`` is ``Optional[float]``, surfaced on the result,
    and enforceable via ``max_cost``.
  * ``execute_async()`` for async callers; sync path uses a thread pool.
  * Registry management: ``get_plan`` / ``unregister`` / ``clear`` /
    ``list_plans``.
  * Thread-safe; metrics; structured logging; health(); __version__;
    __all__; demo.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from ..contracts.execution_result import FallbackReason

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__version__ = "2.0.0"


# =============================================================================
# Errors
# =============================================================================

class FallbackError(Exception):
    """Base class for ClassicalFallback errors."""


class FallbackConfigError(FallbackError, ValueError):
    """Invalid inputs to ``register`` / ``execute`` / the constructor."""


# =============================================================================
# Enums
# =============================================================================

class FallbackStatus(str, Enum):
    """Outcome of a fallback execution."""

    NOT_CONFIGURED = "not_configured"       # no plan for this task_type
    NO_PLAN_FOR_REASON = "no_plan_for_reason"  # plan doesn't cover this reason
    COST_OVER_BUDGET = "cost_over_budget"   # estimated_cost > max_cost
    TIMEOUT = "timeout"                     # callable exceeded timeout_s
    FAILED = "failed"                       # callable raised
    SUCCEEDED = "succeeded"                 # callable returned successfully


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
# Config
# =============================================================================

@dataclass(frozen=True)
class ClassicalFallbackConfig:
    """Tunable parameters for ``ClassicalFallback``."""

    default_timeout_s: float = 30.0
    max_timeout_s: float = 3600.0
    allow_overwrite: bool = False
    thread_pool_size: int = 4

    def __post_init__(self) -> None:
        for fname in ("default_timeout_s", "max_timeout_s"):
            v = getattr(self, fname)
            if not isinstance(v, (int, float)) \
                    or isinstance(v, bool) \
                    or not math.isfinite(float(v)) \
                    or v <= 0:
                raise FallbackConfigError(
                    f"{fname} must be a positive finite number"
                )
        if self.default_timeout_s > self.max_timeout_s:
            raise FallbackConfigError(
                "default_timeout_s must be <= max_timeout_s"
            )
        if self.thread_pool_size <= 0:
            raise FallbackConfigError("thread_pool_size must be > 0")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Data classes
# =============================================================================

@dataclass(frozen=True)
class FallbackPlan:
    """
    A registered fallback plan for one task type.

    ``reasons`` names the failure modes this plan is valid for. Default
    is "all reasons"; register per-reason plans to differentiate between,
    say, a fast heuristic for ``TIMEOUT`` and a heavy solver for
    ``LOW_QUALITY``.
    """

    task_type: str
    classical_callable: Callable[[Any], Any]
    description: str = ""
    estimated_cost: Optional[float] = None
    reasons: FrozenSet[FallbackReason] = field(
        default_factory=lambda: frozenset(FallbackReason),
    )
    timeout_s: Optional[float] = None
    registered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.task_type, str) or not self.task_type.strip():
            raise FallbackConfigError(
                "task_type must be a non-empty string"
            )
        if not callable(self.classical_callable):
            raise FallbackConfigError(
                f"classical_callable for {self.task_type!r} is not callable"
            )
        if not isinstance(self.description, str):
            raise FallbackConfigError(
                "description must be a string"
            )
        if self.estimated_cost is not None:
            if not isinstance(self.estimated_cost, (int, float)) \
                    or isinstance(self.estimated_cost, bool) \
                    or not math.isfinite(float(self.estimated_cost)) \
                    or self.estimated_cost < 0:
                raise FallbackConfigError(
                    "estimated_cost must be >= 0 or None"
                )
        if not isinstance(self.reasons, frozenset) or not self.reasons:
            raise FallbackConfigError(
                "reasons must be a non-empty frozenset of FallbackReason"
            )
        for r in self.reasons:
            if not isinstance(r, FallbackReason):
                raise FallbackConfigError(
                    f"reasons must contain FallbackReason values; "
                    f"got {type(r).__name__}"
                )
        if self.timeout_s is not None:
            if not isinstance(self.timeout_s, (int, float)) \
                    or isinstance(self.timeout_s, bool) \
                    or not math.isfinite(float(self.timeout_s)) \
                    or self.timeout_s <= 0:
                raise FallbackConfigError(
                    "timeout_s must be a positive finite number or None"
                )
        if self.registered_at.tzinfo is None:
            object.__setattr__(
                self, "registered_at",
                self.registered_at.replace(tzinfo=timezone.utc),
            )

    # --- Backward-compat ---
    @property
    def reason(self) -> str:
        """
        Deprecated: always ``"registered"``.

        The old dataclass had a ``reason: str = "registered"`` field that
        was set at construction and never read. Use ``reasons`` to check
        which failure modes this plan applies to.
        """
        return "registered"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_type": self.task_type,
            "description": self.description,
            "estimated_cost": self.estimated_cost,
            "reasons": [r.value for r in self.reasons],
            "timeout_s": self.timeout_s,
            "registered_at": self.registered_at.isoformat(),
        }


@dataclass(frozen=True)
class FallbackResult:
    """
    Immutable outcome of a fallback execution.

    Backward-compatible ``__getitem__`` access:
        result["fallback_available"]  → bool
        result["fallback_failed"]     → bool (only on FAILED branch)
        result["reason"]              → str (the reason passed in)
        result["result"]              → Any (only on SUCCEEDED branch)
        result["description"]         → str (only on SUCCEEDED branch)
        result["error"]               → str (only on FAILED branch)

    Prefer ``result.status`` — it's the single source of truth.
    """

    status: FallbackStatus
    reason: FallbackReason
    task_type: str
    result: Any = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    description: str = ""
    estimated_cost: Optional[float] = None
    actual_duration_s: Optional[float] = None
    trace_id: Optional[str] = None
    request_id: Optional[str] = None
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.status, FallbackStatus):
            try:
                object.__setattr__(
                    self, "status", FallbackStatus(self.status),
                )
            except ValueError:
                raise FallbackConfigError(
                    f"unknown status {self.status!r}"
                ) from None
        if not isinstance(self.reason, FallbackReason):
            try:
                object.__setattr__(
                    self, "reason", FallbackReason(self.reason),
                )
            except ValueError:
                raise FallbackConfigError(
                    f"unknown reason {self.reason!r}"
                ) from None
        if not isinstance(self.task_type, str) or not self.task_type.strip():
            raise FallbackConfigError(
                "task_type must be a non-empty string"
            )
        if self.at.tzinfo is None:
            object.__setattr__(
                self, "at", self.at.replace(tzinfo=timezone.utc),
            )
        if self.actual_duration_s is not None \
                and self.actual_duration_s < 0:
            raise FallbackConfigError(
                "actual_duration_s must be >= 0 or None"
            )

    # --- Derived ---
    @property
    def succeeded(self) -> bool:
        return self.status == FallbackStatus.SUCCEEDED

    @property
    def failed(self) -> bool:
        return self.status in (
            FallbackStatus.FAILED,
            FallbackStatus.TIMEOUT,
        )

    @property
    def available(self) -> bool:
        return self.status not in (
            FallbackStatus.NOT_CONFIGURED,
            FallbackStatus.NO_PLAN_FOR_REASON,
        )

    # --- Backward-compat dict-like access ---
    def _dict_view(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "fallback_available": self.available,
            "reason": self.reason.value,
            "status": self.status.value,
            "task_type": self.task_type,
        }
        if self.status == FallbackStatus.SUCCEEDED:
            d["fallback_failed"] = False
            d["result"] = self.result
            d["description"] = self.description
            if self.estimated_cost is not None:
                d["estimated_cost"] = self.estimated_cost
        elif self.status in (
            FallbackStatus.FAILED, FallbackStatus.TIMEOUT,
            FallbackStatus.COST_OVER_BUDGET,
        ):
            d["fallback_failed"] = True
            if self.error_message is not None:
                d["error"] = self.error_message
            if self.error_type is not None:
                d["error_type"] = self.error_type
        return d

    def __getitem__(self, key: str) -> Any:
        view = self._dict_view()
        if key not in view:
            raise KeyError(key)
        return view[key]

    def __contains__(self, key: str) -> bool:
        return key in self._dict_view()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "reason": self.reason.value,
            "task_type": self.task_type,
            "result": _to_jsonable(self.result),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "description": self.description,
            "estimated_cost": self.estimated_cost,
            "actual_duration_s": self.actual_duration_s,
            "trace_id": self.trace_id,
            "request_id": self.request_id,
            "at": self.at.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            f"FallbackResult(status={self.status.value}, "
            f"task_type={self.task_type!r}, "
            f"reason={self.reason.value}, "
            f"duration_s={self.actual_duration_s!r})"
        )


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


def _coerce_reason(value: Any) -> FallbackReason:
    if isinstance(value, FallbackReason):
        return value
    try:
        return FallbackReason(value)
    except (ValueError, KeyError):
        allowed = sorted(e.value for e in FallbackReason)
        raise FallbackConfigError(
            f"reason must be one of {allowed}; got {value!r}"
        ) from None


# =============================================================================
# The Enhanced ClassicalFallback
# =============================================================================

class ClassicalFallback:
    """
    Manages classical fallback for quantum jobs.

    Every execution is bounded by a timeout (from the plan, the config,
    or the per-call override). The returned ``FallbackResult`` is a
    frozen dataclass with a typed ``status`` — callers branch on it and
    never have to guess which keys exist in a dict.
    """

    def __init__(
        self,
        *,
        config: Optional[ClassicalFallbackConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        self.config = config or ClassicalFallbackConfig()
        self._metrics = metrics or MetricsRecorder()
        self._lock = threading.RLock()
        self._plans: Dict[str, FallbackPlan] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.thread_pool_size,
            thread_name_prefix="classical-fallback",
        )
        self._closed = False
        self._started_mono = time.monotonic()

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "ClassicalFallback":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ registry
    def register(
        self,
        task_type: str,
        *,
        classical_callable: Callable[[Any], Any],
        description: str = "",
        estimated_cost: Optional[float] = None,
        reasons: Optional[Sequence[FallbackReason]] = None,
        timeout_s: Optional[float] = None,
        overwrite: Optional[bool] = None,
    ) -> FallbackPlan:
        """
        Register a fallback plan for ``task_type``.

        ``reasons`` defaults to "all reasons". ``timeout_s`` defaults to
        ``config.default_timeout_s``. ``overwrite`` defaults to
        ``config.allow_overwrite``.
        """
        if not isinstance(task_type, str) or not task_type.strip():
            raise FallbackConfigError(
                "task_type must be a non-empty string"
            )
        if not callable(classical_callable):
            raise FallbackConfigError(
                f"classical_callable for {task_type!r} is not callable"
            )
        reason_set = (
            frozenset(reasons) if reasons is not None
            else frozenset(FallbackReason)
        )
        overwrite_effective = (
            overwrite if overwrite is not None
            else self.config.allow_overwrite
        )
        if timeout_s is not None:
            if timeout_s > self.config.max_timeout_s:
                raise FallbackConfigError(
                    f"timeout_s={timeout_s} exceeds "
                    f"max_timeout_s={self.config.max_timeout_s}"
                )
        plan = FallbackPlan(
            task_type=task_type,
            classical_callable=classical_callable,
            description=description,
            estimated_cost=estimated_cost,
            reasons=reason_set,
            timeout_s=timeout_s,
        )
        with self._lock:
            existed = task_type in self._plans
            if existed and not overwrite_effective:
                raise FallbackConfigError(
                    f"a plan for task_type={task_type!r} is already "
                    f"registered; pass overwrite=True to replace it"
                )
            if existed:
                logger.warning(
                    "overwriting existing fallback plan",
                    extra={"task_type": task_type},
                )
                self._metrics.inc(
                    "plan_overwrites", task_type=task_type,
                )
            self._plans[task_type] = plan
        logger.info(
            "registered fallback plan",
            extra={
                "task_type": task_type,
                "estimated_cost": estimated_cost,
                "reasons": sorted(r.value for r in reason_set),
                "timeout_s": timeout_s,
            },
        )
        return plan

    def unregister(self, task_type: str) -> bool:
        with self._lock:
            existed = self._plans.pop(task_type, None) is not None
        if existed:
            logger.info(
                "unregistered fallback plan",
                extra={"task_type": task_type},
            )
        return existed

    def clear(self) -> None:
        with self._lock:
            self._plans.clear()
        logger.info("cleared all fallback plans")

    def get_plan(self, task_type: str) -> Optional[FallbackPlan]:
        with self._lock:
            return self._plans.get(task_type)

    def list_plans(self) -> Tuple[FallbackPlan, ...]:
        with self._lock:
            return tuple(self._plans.values())

    def has_fallback(
        self,
        task_type: str,
        *,
        reason: Optional[FallbackReason] = None,
    ) -> bool:
        """
        Return True if a plan exists for ``task_type`` and — when
        ``reason`` is given — the plan covers that reason.

        ``register()`` rejects ``classical_callable=None``, so a plan
        with a callable and a plan without one are indistinguishable
        here: the "no callable" state cannot exist.
        """
        with self._lock:
            plan = self._plans.get(task_type)
        if plan is None:
            return False
        if reason is None:
            return True
        try:
            reason_coerced = _coerce_reason(reason)
        except FallbackConfigError:
            return False
        return reason_coerced in plan.reasons

    # ------------------------------------------------------------------ execute
    def execute(
        self,
        task_type: str,
        *,
        task: Any,
        reason: FallbackReason,
        max_cost: Optional[float] = None,
        timeout_s: Optional[float] = None,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> FallbackResult:
        """
        Run the registered fallback synchronously (in a worker thread).

        Backward-compatible return value: the ``FallbackResult`` behaves
        like the old dict via ``__getitem__`` and ``to_dict()``. New
        callers should branch on ``result.status``.

        If ``classical_callable`` is a coroutine function, raises
        ``FallbackConfigError`` — use ``execute_async`` instead.
        """
        reason_c = _coerce_reason(reason)
        plan, early_result = self._prepare(task_type, reason_c, max_cost,
                                           timeout_s, trace_id, request_id)
        if early_result is not None:
            return early_result

        assert plan is not None  # _prepare guarantees plan or early_result
        if inspect.iscoroutinefunction(plan.classical_callable):
            raise FallbackConfigError(
                f"classical_callable for {task_type!r} is a coroutine "
                f"function; use execute_async() instead"
            )

        effective_timeout = self._resolve_timeout(plan, timeout_s)
        t0 = time.monotonic()
        try:
            future = self._executor.submit(
                plan.classical_callable, task,
            )
            try:
                value = future.result(timeout=effective_timeout)
            except concurrent.futures.TimeoutError:
                # Best-effort: cancel if not yet running. The thread may
                # keep running if the callable is not cooperative.
                future.cancel()
                duration = time.monotonic() - t0
                self._metrics.inc(
                    "fallback_failed", task_type=task_type,
                    reason=reason_c.value, error_type="timeout",
                )
                logger.warning(
                    "fallback timed out",
                    extra={
                        "task_type": task_type,
                        "reason": reason_c.value,
                        "timeout_s": effective_timeout,
                    },
                )
                return self._fail_result(
                    status=FallbackStatus.TIMEOUT,
                    task_type=task_type, reason=reason_c,
                    plan=plan,
                    error_type="TimeoutError",
                    error_message=(
                        f"callable exceeded timeout_s="
                        f"{effective_timeout:.1f}"
                    ),
                    duration_s=duration,
                    trace_id=trace_id, request_id=request_id,
                )
            duration = time.monotonic() - t0
            return self._success_result(
                value=value, task_type=task_type, reason=reason_c,
                plan=plan, duration_s=duration,
                trace_id=trace_id, request_id=request_id,
            )
        except asyncio.CancelledError:
            # Propogate cancellation — the caller is shutting down.
            raise
        except BaseException as exc:
            duration = time.monotonic() - t0
            self._metrics.inc(
                "fallback_failed", task_type=task_type,
                reason=reason_c.value, error_type=type(exc).__name__,
            )
            logger.exception(
                "classical fallback raised",
                extra={
                    "task_type": task_type,
                    "reason": reason_c.value,
                    "error_type": type(exc).__name__,
                },
            )
            return self._fail_result(
                status=FallbackStatus.FAILED,
                task_type=task_type, reason=reason_c,
                plan=plan,
                error_type=type(exc).__name__,
                error_message=str(exc) or repr(exc),
                duration_s=duration,
                trace_id=trace_id, request_id=request_id,
            )

    async def execute_async(
        self,
        task_type: str,
        *,
        task: Any,
        reason: FallbackReason,
        max_cost: Optional[float] = None,
        timeout_s: Optional[float] = None,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> FallbackResult:
        """
        Run the registered fallback asynchronously.

        Supports both sync and async callables. Sync callables are
        dispatched to the internal thread pool via
        ``asyncio.to_thread`` so the event loop is never blocked.
        """
        reason_c = _coerce_reason(reason)
        plan, early_result = self._prepare(task_type, reason_c, max_cost,
                                           timeout_s, trace_id, request_id)
        if early_result is not None:
            return early_result

        assert plan is not None
        effective_timeout = self._resolve_timeout(plan, timeout_s)
        t0 = time.monotonic()

        async def _run() -> Any:
            if inspect.iscoroutinefunction(plan.classical_callable):
                return await plan.classical_callable(task)
            return await asyncio.to_thread(plan.classical_callable, task)

        try:
            value = await asyncio.wait_for(_run(), timeout=effective_timeout)
            duration = time.monotonic() - t0
            return self._success_result(
                value=value, task_type=task_type, reason=reason_c,
                plan=plan, duration_s=duration,
                trace_id=trace_id, request_id=request_id,
            )
        except asyncio.TimeoutError:
            duration = time.monotonic() - t0
            self._metrics.inc(
                "fallback_failed", task_type=task_type,
                reason=reason_c.value, error_type="timeout",
            )
            logger.warning(
                "fallback timed out (async)",
                extra={
                    "task_type": task_type,
                    "reason": reason_c.value,
                    "timeout_s": effective_timeout,
                },
            )
            return self._fail_result(
                status=FallbackStatus.TIMEOUT,
                task_type=task_type, reason=reason_c,
                plan=plan,
                error_type="TimeoutError",
                error_message=(
                    f"callable exceeded timeout_s="
                    f"{effective_timeout:.1f}"
                ),
                duration_s=duration,
                trace_id=trace_id, request_id=request_id,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            duration = time.monotonic() - t0
            self._metrics.inc(
                "fallback_failed", task_type=task_type,
                reason=reason_c.value, error_type=type(exc).__name__,
            )
            logger.exception(
                "classical fallback raised (async)",
                extra={
                    "task_type": task_type,
                    "reason": reason_c.value,
                    "error_type": type(exc).__name__,
                },
            )
            return self._fail_result(
                status=FallbackStatus.FAILED,
                task_type=task_type, reason=reason_c,
                plan=plan,
                error_type=type(exc).__name__,
                error_message=str(exc) or repr(exc),
                duration_s=duration,
                trace_id=trace_id, request_id=request_id,
            )

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        with self._lock:
            n = len(self._plans)
            task_types = sorted(self._plans.keys())
        return {
            "ready": not self._closed,
            "closed": self._closed,
            "plans_registered": n,
            "task_types": task_types,
            "config": self.config.to_dict(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "metrics": self._metrics.snapshot(),
        }

    # ------------------------------------------------------------------ internals
    def _prepare(
        self,
        task_type: str,
        reason: FallbackReason,
        max_cost: Optional[float],
        timeout_s: Optional[float],
        trace_id: Optional[str],
        request_id: Optional[str],
    ) -> Tuple[Optional[FallbackPlan], Optional[FallbackResult]]:
        if not isinstance(task_type, str) or not task_type.strip():
            raise FallbackConfigError(
                "task_type must be a non-empty string"
            )
        if timeout_s is not None:
            if timeout_s <= 0 or not math.isfinite(timeout_s):
                raise FallbackConfigError(
                    "timeout_s must be a positive finite number or None"
                )
        if max_cost is not None:
            if max_cost < 0 or not math.isfinite(max_cost):
                raise FallbackConfigError(
                    "max_cost must be >= 0 or None"
                )

        with self._lock:
            if self._closed:
                raise FallbackError("fallback is closed")
            plan = self._plans.get(task_type)

        if plan is None:
            self._metrics.inc(
                "fallback_not_configured", task_type=task_type,
            )
            logger.warning(
                "no classical fallback registered",
                extra={"task_type": task_type, "reason": reason.value},
            )
            return None, FallbackResult(
                status=FallbackStatus.NOT_CONFIGURED,
                reason=reason,
                task_type=task_type,
                trace_id=trace_id,
                request_id=request_id,
            )

        if reason not in plan.reasons:
            self._metrics.inc(
                "fallback_no_plan_for_reason",
                task_type=task_type, reason=reason.value,
            )
            logger.warning(
                "no plan covers this reason",
                extra={
                    "task_type": task_type,
                    "reason": reason.value,
                    "covered_reasons": sorted(r.value for r in plan.reasons),
                },
            )
            return None, FallbackResult(
                status=FallbackStatus.NO_PLAN_FOR_REASON,
                reason=reason,
                task_type=task_type,
                description=plan.description,
                estimated_cost=plan.estimated_cost,
                trace_id=trace_id,
                request_id=request_id,
            )

        if max_cost is not None and plan.estimated_cost is not None:
            if plan.estimated_cost > max_cost:
                self._metrics.inc(
                    "fallback_cost_over_budget",
                    task_type=task_type,
                )
                logger.info(
                    "fallback skipped: over budget",
                    extra={
                        "task_type": task_type,
                        "estimated_cost": plan.estimated_cost,
                        "max_cost": max_cost,
                    },
                )
                return None, FallbackResult(
                    status=FallbackStatus.COST_OVER_BUDGET,
                    reason=reason,
                    task_type=task_type,
                    description=plan.description,
                    estimated_cost=plan.estimated_cost,
                    trace_id=trace_id,
                    request_id=request_id,
                )

        return plan, None

    def _resolve_timeout(
        self, plan: FallbackPlan, override: Optional[float],
    ) -> float:
        if override is not None:
            return min(float(override), self.config.max_timeout_s)
        if plan.timeout_s is not None:
            return float(plan.timeout_s)
        return float(self.config.default_timeout_s)

    def _success_result(
        self,
        *,
        value: Any,
        task_type: str,
        reason: FallbackReason,
        plan: FallbackPlan,
        duration_s: float,
        trace_id: Optional[str],
        request_id: Optional[str],
    ) -> FallbackResult:
        self._metrics.inc(
            "fallback_succeeded",
            task_type=task_type, reason=reason.value,
        )
        self._metrics.observe(
            "fallback_duration_s", duration_s,
            task_type=task_type,
        )
        logger.info(
            "classical fallback succeeded",
            extra={
                "task_type": task_type,
                "reason": reason.value,
                "duration_s": round(duration_s, 3),
            },
        )
        return FallbackResult(
            status=FallbackStatus.SUCCEEDED,
            reason=reason,
            task_type=task_type,
            result=value,
            description=plan.description,
            estimated_cost=plan.estimated_cost,
            actual_duration_s=duration_s,
            trace_id=trace_id,
            request_id=request_id,
        )

    def _fail_result(
        self,
        *,
        status: FallbackStatus,
        task_type: str,
        reason: FallbackReason,
        plan: FallbackPlan,
        error_type: str,
        error_message: str,
        duration_s: float,
        trace_id: Optional[str],
        request_id: Optional[str],
    ) -> FallbackResult:
        self._metrics.observe(
            "fallback_duration_s", duration_s,
            task_type=task_type,
        )
        return FallbackResult(
            status=status,
            reason=reason,
            task_type=task_type,
            error_type=error_type,
            error_message=error_message,
            description=plan.description,
            estimated_cost=plan.estimated_cost,
            actual_duration_s=duration_s,
            trace_id=trace_id,
            request_id=request_id,
        )


# =============================================================================
# Public API
# =============================================================================

__all__: Tuple[str, ...] = (
    "ClassicalFallback",
    "ClassicalFallbackConfig",
    "FallbackConfigError",
    "FallbackError",
    "FallbackPlan",
    "FallbackResult",
    "FallbackStatus",
    "MetricsRecorder",
)


# =============================================================================
# Demo: python -m qcs.execution.classical_fallback
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    def classify_classically(task: Any) -> Dict[str, Any]:
        return {"label": "cat", "score": 0.83, "task": task}

    def always_fails(task: Any) -> Any:
        raise RuntimeError("classical solver diverged")

    def slow_solver(task: Any) -> Any:
        time.sleep(10)
        return {"label": "never"}

    async def async_classify(task: Any) -> Dict[str, Any]:
        await asyncio.sleep(0.01)
        return {"label": "cat", "score": 0.83, "task": task}

    print("=== Happy path (sync) ===")
    fb = ClassicalFallback()
    fb.register(
        "classify",
        classical_callable=classify_classically,
        description="k-NN over cached features",
        estimated_cost=0.01,
    )
    r = fb.execute(
        "classify", task={"image": "x"},
        reason=FallbackReason.TIMEOUT,
    )
    print(f"  {r!r}")
    print(f"  status: {r.status.value}")
    print(f"  succeeded: {r.succeeded}")
    print(f"  result: {r.result}")
    print(f"  dict-compat: available={r['fallback_available']}, "
          f"failed={r['fallback_failed']}")

    print("\n=== §1.1 No plan → typed NOT_CONFIGURED ===")
    r_nc = fb.execute("missing", task={}, reason=FallbackReason.TIMEOUT)
    print(f"  status: {r_nc.status.value}")
    print(f"  available: {r_nc.available}")
    print(f"  dict-compat: available={r_nc['fallback_available']}")
    # No KeyError when reading fallback_failed via __contains__
    assert "fallback_failed" not in r_nc

    print("\n=== §1.5 Failure preserves type + message ===")
    fb.register("fails", classical_callable=always_fails)
    r_fail = fb.execute("fails", task={}, reason=FallbackReason.TIMEOUT)
    print(f"  status: {r_fail.status.value}")
    print(f"  error_type: {r_fail.error_type}")
    print(f"  error_message: {r_fail.error_message}")

    print("\n=== §2.1 Timeout ===")
    fb.register(
        "slow", classical_callable=slow_solver,
        timeout_s=0.1,
    )
    r_to = fb.execute(
        "slow", task={}, reason=FallbackReason.TIMEOUT,
    )
    print(f"  status: {r_to.status.value}")
    print(f"  error_type: {r_to.error_type}")

    print("\n=== §1.4 register validation ===")
    for label, kwargs in [
        ("empty task_type", dict(task_type="", classical_callable=classify_classically)),
        ("non-callable", dict(task_type="x", classical_callable=None)),
        ("negative cost", dict(
            task_type="x", classical_callable=classify_classically,
            estimated_cost=-1.0)),
    ]:
        try:
            fb.register(**kwargs)
            print(f"  {label}: NOT rejected")
        except FallbackConfigError as e:
            print(f"  {label}: {type(e).__name__}")

    print("\n=== §1.3 Duplicate registration ===")
    try:
        fb.register("classify", classical_callable=classify_classically)
    except FallbackConfigError as e:
        print(f"  strict default: {type(e).__name__}")
    fb.register(
        "classify", classical_callable=classify_classically,
        overwrite=True,
    )
    print(f"  overwrite=True: accepted")

    print("\n=== §1.8 estimated_cost surfaced + max_cost ===")
    fb2 = ClassicalFallback()
    fb2.register(
        "paid", classical_callable=classify_classically,
        estimated_cost=5.0,
    )
    r_paid = fb2.execute(
        "paid", task={}, reason=FallbackReason.TIMEOUT,
    )
    print(f"  success: estimated_cost={r_paid.estimated_cost}")
    r_budget = fb2.execute(
        "paid", task={}, reason=FallbackReason.TIMEOUT, max_cost=1.0,
    )
    print(f"  over budget: status={r_budget.status.value}")

    print("\n=== §3.2 Per-reason dispatch ===")
    fb3 = ClassicalFallback()
    fb3.register(
        "task", classical_callable=classify_classically,
        reasons=(FallbackReason.TIMEOUT, FallbackReason.LOW_QUALITY),
    )
    print(f"  has_fallback(TIMEOUT): "
          f"{fb3.has_fallback('task', reason=FallbackReason.TIMEOUT)}")
    print(f"  has_fallback(HIGH_NOISE): "
          f"{fb3.has_fallback('task', reason=FallbackReason.HIGH_NOISE)}")
    r_ok = fb3.execute("task", task={}, reason=FallbackReason.TIMEOUT)
    r_no = fb3.execute("task", task={}, reason=FallbackReason.HIGH_NOISE)
    print(f"  TIMEOUT: {r_ok.status.value}")
    print(f"  HIGH_NOISE: {r_no.status.value}")

    print("\n=== §2.4 Async execution ===")
    async def _async_demo() -> None:
        fb4 = ClassicalFallback()
        fb4.register("async_task", classical_callable=async_classify)
        r = await fb4.execute_async(
            "async_task", task={},
            reason=FallbackReason.TIMEOUT,
        )
        print(f"  async success: {r.status.value}, result={r.result}")
    asyncio.run(_async_demo())

    print("\n=== §2.4 Sync execute with async callable ===")
    fb5 = ClassicalFallback()
    fb5.register("async_only", classical_callable=async_classify)
    try:
        fb5.execute("async_only", task={}, reason=FallbackReason.TIMEOUT)
    except FallbackConfigError as e:
        print(f"  correctly rejected: {e}")

    print("\n=== §2.2 Registry management ===")
    print(f"  plans: {[p.task_type for p in fb.list_plans()]}")
    assert fb.get_plan("classify") is not None
    assert fb.unregister("classify")
    assert fb.get_plan("classify") is None

    print("\n=== §3.5 Trace correlation ===")
    fb6 = ClassicalFallback()
    fb6.register("traced", classical_callable=classify_classically)
    r_traced = fb6.execute(
        "traced", task={}, reason=FallbackReason.TIMEOUT,
        trace_id="trace-abc", request_id="req-123",
    )
    print(f"  trace_id: {r_traced.trace_id}")
    print(f"  request_id: {r_traced.request_id}")

    print("\n=== §3.6 has_fallback with reason ===")
    print(f"  has_fallback('traced') = {fb6.has_fallback('traced')}")
    print(f"  has_fallback('traced', TIMEOUT) = "
          f"{fb6.has_fallback('traced', reason=FallbackReason.TIMEOUT)}")

    print("\n=== §2.7 Thread safety ===")
    fb7 = ClassicalFallback()
    fb7.register("t", classical_callable=lambda t: 1)
    def _hit() -> None:
        for _ in range(100):
            fb7.execute("t", task={}, reason=FallbackReason.TIMEOUT)
    threads = [threading.Thread(target=_hit) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  metrics: {fb7.health()['metrics']['counters'].get('fallback_succeeded{reason=\"timeout\",task_type=\"t\"}', 0)}")

    print("\n=== §2.12 Health ===")
    print(f"  ready: {fb.health()['ready']}, "
          f"plans: {fb.health()['plans_registered']}")
    fb.close()
    print(f"  after close: ready={fb.health()['ready']}")

    print(f"\n=== __version__ === {__version__}")
    print(f"=== __all__ === {__all__}")
    print("\nSmoke test passed.")
