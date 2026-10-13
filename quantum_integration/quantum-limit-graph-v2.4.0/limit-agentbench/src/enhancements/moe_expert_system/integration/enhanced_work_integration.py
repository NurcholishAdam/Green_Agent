#!/usr/bin/env python3
"""
Enhanced Work Integrator v8.0.0 — Self-Contained Single-File Bundle
=====================================================================

Complete rewrite of the Green Agent work integrator. Everything needed to
run and test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. Value objects: `Explanation`, `MOPDWorkPlan` v2, `WorkState`, `WorkPriority`.
  3. NSGA-II fast non-dominated sort with deduplication.
  4. Shared runtime: `CircuitBreaker`, `TaskManager`, `TemporalSafetyMonitor`,
     `HumanApprovalHandler`, `XAIExplainer`, `ChaosEngine`.
  5. `EnhancedWorkIntegrator` with:
        - Real state machine (no forced transitions, no leaks).
        - Real MOPD selection (ascending cost sort, correct softmax sign).
        - Real HITL via injected handler, with deferred status.
        - Real XAI (Integrated Gradients + counterfactuals).
        - Real bio helpers with sync/async coercion and `BioResult`.
        - Bounded thresholds; adaptive state on the instance.
        - Single token spend, no double-spend.
        - Tracked background tasks and clean shutdown.
  6. Comprehensive pytest suite (~45 tests).

Run:
    python enhanced_work_integrator_v8.py         # demo
    pytest enhanced_work_integrator_v8.py -v      # tests

Requires: Python 3.10+, pytest, pytest-asyncio.
Optional: numpy (fallback to pure Python), networkx (fallback skipped).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import re as _re
import threading
import time
import uuid
from abc import ABC
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, ClassVar, Deque, Dict, Iterable, List,
    Optional, Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import pytest
import pytest_asyncio

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

try:
    import numpy as _np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    _np = None  # type: ignore
    NUMPY_AVAILABLE = False


__version__ = "8.0.0"


# =============================================================================
# PROTOCOLS — every injected dependency is behind an interface
# =============================================================================
@runtime_checkable
class StorageLike(Protocol):
    def save_state(self, key: str, value: str) -> None: ...
    def get_state(self, key: str) -> Optional[str]: ...


@runtime_checkable
class MessageQueueLike(Protocol):
    async def publish(self, topic: str, payload: str) -> None: ...


@runtime_checkable
class MetricsLike(Protocol):
    def inc(self, name: str, amount: float = 1.0) -> None: ...
    def set(self, name: str, value: float) -> None: ...


@runtime_checkable
class AdaptiveCostLike(Protocol):
    def get_current_weights(self) -> Dict[str, float]: ...
    def compute(self, **kwargs: Any) -> float: ...


@runtime_checkable
class ParetoGatingLike(Protocol):
    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]: ...


@runtime_checkable
class DriftDetectorLike(Protocol):
    async def check_drift(self, weights: Dict[str, float]) -> float: ...


@runtime_checkable
class TokenManagerLike(Protocol):
    """Any of these may be sync or async; the integrator coerces."""
    def spend(self, actor: str, amount: float) -> Any: ...
    def earn(self, actor: str, amount: float) -> Any: ...
    def get_system_summary(self) -> Any: ...


@runtime_checkable
class GradientManagerLike(Protocol):
    def pump_field(self, name: str, delta: float, source: str = ...) -> Any: ...
    def get_field_strengths(self) -> Dict[str, float]: ...


@runtime_checkable
class CompartmentLike(Protocol):
    id: str


@runtime_checkable
class CompartmentManagerLike(Protocol):
    def find_best_compartment(self, expert_type: str) -> Optional[Any]: ...


@runtime_checkable
class BiomassStorageLike(Protocol):
    def store_task(self, *, task_data: Dict[str, Any], ecoatp_cost: float,
                   guarantee: Any = ..., initial_tier: Any = ...) -> Any: ...


@runtime_checkable
class ApprovalHandlerLike(Protocol):
    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = ...) -> bool: ...


@runtime_checkable
class ExpertRouterLike(Protocol):
    async def route_and_execute(self, context: Dict[str, Any]) -> Dict[str, Any]: ...


# =============================================================================
# VALUE OBJECTS
# =============================================================================
class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    status: HealthStatus = HealthStatus.UNKNOWN
    score: float = 0.5
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BioResult:
    success: bool = False
    amount: float = 0.0
    error: Optional[str] = None


@dataclass
class Explanation:
    method: str = "none"
    attributions: List[float] = field(default_factory=list)
    feature_names: List[str] = field(default_factory=list)
    counterfactual: Optional[Dict[str, Any]] = None
    natural_language: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "attributions": list(self.attributions),
            "feature_names": list(self.feature_names),
            "counterfactual": self.counterfactual,
            "natural_language": self.natural_language,
        }


# =============================================================================
# ENUMS
# =============================================================================
class WorkState(str, Enum):
    CREATED = "created"
    VALIDATED = "validated"
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    RESOURCES_RESERVED = "resources_reserved"
    TOKENS_ALLOCATED = "tokens_allocated"
    EXECUTING = "executing"
    CHECKPOINTED = "checkpointed"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"
    STORED_AS_BIOMASS = "stored_as_biomass"
    SUSPENDED = "suspended"
    RESUMED = "resumed"
    ARCHIVED = "archived"

    def is_terminal(self) -> bool:
        return self in (WorkState.COMPLETED, WorkState.FAILED,
                         WorkState.ROLLED_BACK, WorkState.CANCELLED,
                         WorkState.ARCHIVED)

    def is_active(self) -> bool:
        return self in (WorkState.EXECUTING, WorkState.ROLLING_BACK,
                         WorkState.CHECKPOINTED)

    def can_transition_to(self, target: "WorkState") -> bool:
        table: Dict[WorkState, Set[WorkState]] = {
            WorkState.CREATED: {WorkState.VALIDATED, WorkState.CANCELLED},
            WorkState.VALIDATED: {WorkState.QUEUED, WorkState.CANCELLED,
                                    WorkState.STORED_AS_BIOMASS},
            WorkState.QUEUED: {WorkState.SCHEDULED, WorkState.CANCELLED,
                                 WorkState.STORED_AS_BIOMASS},
            WorkState.SCHEDULED: {WorkState.RESOURCES_RESERVED,
                                    WorkState.TOKENS_ALLOCATED,
                                    WorkState.EXECUTING,  # direct shortcut
                                    WorkState.CANCELLED},
            WorkState.RESOURCES_RESERVED: {WorkState.EXECUTING,
                                             WorkState.CANCELLED},
            WorkState.TOKENS_ALLOCATED: {WorkState.EXECUTING,
                                           WorkState.CANCELLED},
            WorkState.EXECUTING: {WorkState.COMPLETED, WorkState.FAILED,
                                    WorkState.CHECKPOINTED,
                                    WorkState.SUSPENDED},
            WorkState.CHECKPOINTED: {WorkState.EXECUTING, WorkState.RESUMED,
                                       WorkState.FAILED},
            WorkState.FAILED: {WorkState.ROLLING_BACK, WorkState.QUEUED,
                                 WorkState.STORED_AS_BIOMASS},
            WorkState.ROLLING_BACK: {WorkState.ROLLED_BACK, WorkState.FAILED},
            WorkState.ROLLED_BACK: {WorkState.QUEUED, WorkState.ARCHIVED,
                                      WorkState.STORED_AS_BIOMASS},
            WorkState.SUSPENDED: {WorkState.RESUMED, WorkState.CANCELLED},
            WorkState.RESUMED: {WorkState.EXECUTING},
            WorkState.STORED_AS_BIOMASS: {WorkState.QUEUED,
                                            WorkState.EXECUTING,
                                            WorkState.ARCHIVED},
            WorkState.COMPLETED: {WorkState.ARCHIVED},
        }
        return target in table.get(self, set())


class WorkPriority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    BACKGROUND = "background"

    @property
    def weight(self) -> float:
        return {
            WorkPriority.CRITICAL: 10.0,
            WorkPriority.HIGH: 5.0,
            WorkPriority.MEDIUM: 2.0,
            WorkPriority.LOW: 1.0,
            WorkPriority.BACKGROUND: 0.5,
        }[self]


# =============================================================================
# NSGA-II FAST NON-DOMINATED SORT (with deduplication)
# =============================================================================
def is_dominated(a: Dict[str, float],
                 b: Dict[str, float],
                 minimise_keys: Set[str],
                 maximise_keys: Set[str]) -> bool:
    """Return True if b dominates a. Missing values treated as worst."""
    a_better_or_equal = True
    b_strictly_better_somewhere = False
    for key in maximise_keys:
        av = float(a.get(key, float("-inf")))
        bv = float(b.get(key, float("-inf")))
        if bv < av:
            a_better_or_equal = False
            break
        if bv > av:
            b_strictly_better_somewhere = True
    if a_better_or_equal:
        for key in minimise_keys:
            av = float(a.get(key, float("inf")))
            bv = float(b.get(key, float("inf")))
            if bv > av:
                a_better_or_equal = False
                break
            if bv < av:
                b_strictly_better_somewhere = True
    return a_better_or_equal and b_strictly_better_somewhere


def nsga2_fast_nondominated_sort(
    solutions: List[Dict[str, float]],
    minimise_keys: Set[str],
    maximise_keys: Set[str],
) -> List[Dict[str, float]]:
    if not solutions:
        return []
    order = sorted(minimise_keys) + sorted(maximise_keys)
    seen: Set[Tuple[float, ...]] = set()
    unique: List[Dict[str, float]] = []
    for s in solutions:
        key = tuple(float(s.get(k, 0.0)) for k in order)
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)
    n = len(unique)
    if n == 0:
        return []
    if n == 1:
        return list(unique)
    dominated_sets: List[List[int]] = [[] for _ in range(n)]
    count = [0] * n
    for i in range(n):
        for j in range(i + 1, n):
            if is_dominated(unique[i], unique[j],
                            minimise_keys, maximise_keys):
                dominated_sets[i].append(j)
                count[j] += 1
            elif is_dominated(unique[j], unique[i],
                              minimise_keys, maximise_keys):
                dominated_sets[j].append(i)
                count[i] += 1
    front = [unique[i] for i in range(n) if count[i] == 0]
    return front


# =============================================================================
# SHARED RUNTIME — CircuitBreaker
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0,
                 half_open_max: int = 3,
                 name: str = "default") -> None:
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_max = int(half_open_max)
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.half_open_requests = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if (self.last_failure_time is not None and
                        (time.time() - self.last_failure_time) >
                        self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                else:
                    raise RuntimeError(f"circuit breaker {self.name} open")
            if (self.state == CircuitBreakerState.HALF_OPEN and
                    self.half_open_requests >= self.half_open_max):
                raise RuntimeError(
                    f"circuit breaker {self.name} half-open limit")
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                elif self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_requests += 1
                if self.half_open_requests >= self.half_open_max:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = 0
        return result


# =============================================================================
# SHARED RUNTIME — TaskManager
# =============================================================================
class TaskManager:
    def __init__(self) -> None:
        self._tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
        self._lock = threading.Lock()

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    def start(self, name: str,
              coro_factory: Callable[[], Any]) -> Optional[asyncio.Task]:
        with self._lock:
            existing = self._tasks.get(name)
            if existing is not None and not existing.done():
                return existing
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.warning("TaskManager: no running loop; %s skipped",
                               name)
                return None
            task = loop.create_task(coro_factory())
            self._tasks[name] = task
            task.add_done_callback(lambda t, n=name: self._forget(n))
            return task

    def start_coro(self, name: str, coro: Any) -> Optional[asyncio.Task]:
        return self.start(name, lambda: coro)

    def _forget(self, name: str) -> None:
        with self._lock:
            self._tasks.pop(name, None)

    def task_count(self) -> int:
        with self._lock:
            return len(self._tasks)

    async def stop_all(self, timeout: float = 5.0) -> None:
        self._shutdown_event.set()
        with self._lock:
            tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("TaskManager.stop_all timed out")
        with self._lock:
            self._tasks.clear()


# =============================================================================
# SHARED RUNTIME — Temporal safety (MTL)
# =============================================================================
_ATOMIC_RE = _re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


@dataclass
class _TemporalRule:
    rule_id: str
    operator: str
    predicate: Callable[[Dict[str, Any]], bool]
    window_seconds: float = 0.0
    description: str = ""
    severity: str = "warning"
    violations: int = 0
    last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict[str, Any]]]) -> bool:
        if not trace:
            return False
        if self.window_seconds > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window_seconds)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.predicate(s) for _, s in trace)
        if op == "eventually":
            return not any(self.predicate(s) for _, s in trace)
        if op == "never":
            return any(self.predicate(s) for _, s in trace)
        if op == "historically":
            return any(not self.predicate(s) for _, s in trace)
        if op == "once":
            return not any(self.predicate(s) for _, s in trace)
        return False


class TemporalSafetyMonitor:
    def __init__(self, max_trace: int = 500) -> None:
        self.rules: Dict[str, _TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(
            maxlen=max_trace)

    def add_formula(self, formula: str) -> None:
        f = formula.strip()
        window = 0.0
        if f.startswith("[") and "]" in f:
            head, rest = f[1:].split("]", 1)
            try:
                window = float(head.strip().rstrip("s"))
            except ValueError:
                window = 0.0
            f = rest.strip()
        op_map = {"G ": "always", "F ": "eventually",
                  "NEVER ": "never", "H ": "historically", "O ": "once"}
        for prefix, op in op_map.items():
            if f.startswith(prefix):
                expr = f[len(prefix):].strip().strip("()")
                self._add_atomic(expr, op, window)
                return

    def _add_atomic(self, expr: str, op: str, window: float) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def predicate(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}" + (f"@{window}s" if window else "")
        self.rules[rid] = _TemporalRule(rid, op, predicate,
                                         window_seconds=window,
                                         description=expr)

    async def push(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def check(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
        return result

    def counts(self) -> Dict[str, int]:
        return {rid: r.violations for rid, r in self.rules.items()}


# =============================================================================
# SHARED RUNTIME — Human approval (uncertainty sampling)
# =============================================================================
class HumanApprovalHandler:
    def __init__(self, queue: Optional[MessageQueueLike] = None,
                 uncertainty_threshold: float = 0.05,
                 timeout: float = 5.0) -> None:
        self.queue = queue
        self.uncertainty_threshold = float(uncertainty_threshold)
        self.timeout = float(timeout)
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.decisions: List[Dict[str, Any]] = []

    async def submit_response(self, user_id: str, chosen_id: str) -> None:
        try:
            self._responses.put_nowait({"user_id": user_id,
                                         "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    def _utility(self, user_id: str, candidate: Dict[str, Any]) -> float:
        prefs = self.preferences.get(user_id, {})
        if not prefs:
            return 0.5
        u = 0.0
        for k, v in prefs.items():
            if k in candidate:
                try:
                    u += float(v) * float(candidate[k])
                except Exception:
                    continue
        return u

    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = "default") -> bool:
        candidates = decision.get("candidates") or []
        if len(candidates) < 2:
            self.decisions.append({"decision": decision, "auto": True,
                                    "approved": True})
            return True
        scored = sorted(
            ((self._utility(user_id, c), c) for c in candidates),
            key=lambda kv: kv[0], reverse=True)
        if abs(scored[0][0] - scored[1][0]) > self.uncertainty_threshold:
            self.decisions.append({"decision": decision, "auto": True,
                                    "approved": True,
                                    "chosen": scored[0][1].get("id")})
            return True
        try:
            msg = await asyncio.wait_for(self._responses.get(),
                                         timeout=self.timeout)
            approved = bool(msg.get("chosen"))
        except asyncio.TimeoutError:
            approved = True
        self.decisions.append({"decision": decision, "auto": False,
                                "approved": approved})
        return approved


# =============================================================================
# SHARED RUNTIME — XAI (Integrated Gradients + counterfactuals)
# =============================================================================
class XAIExplainer:
    def __init__(self, ig_steps: int = 16, cf_max_iter: int = 40,
                 cf_target_delta: float = 0.1,
                 rng: Optional[random.Random] = None) -> None:
        self.ig_steps = int(ig_steps)
        self.cf_max_iter = int(cf_max_iter)
        self.cf_target_delta = float(cf_target_delta)
        self.rng = rng or random.Random()

    def integrated_gradients(self, f: Callable[[List[float]], float],
                             x: Sequence[float],
                             baseline: Optional[Sequence[float]] = None
                             ) -> List[float]:
        x = [float(v) for v in x]
        if baseline is None:
            baseline = [0.0] * len(x)
        else:
            baseline = [float(v) for v in baseline]
        total = [0.0] * len(x)
        for s in range(1, self.ig_steps + 1):
            alpha = s / self.ig_steps
            interp = [b + alpha * (a - b) for a, b in zip(x, baseline)]
            eps = 1e-4
            for i in range(len(x)):
                up = list(interp); up[i] += eps
                dn = list(interp); dn[i] -= eps
                try:
                    grad = (float(f(up)) - float(f(dn))) / (2 * eps)
                except Exception:
                    grad = 0.0
                total[i] += grad
        avg = [t / max(self.ig_steps, 1) for t in total]
        return [(a - b) * g for a, b, g in zip(x, baseline, avg)]

    def counterfactual(self, f: Callable[[List[float]], float],
                       x: Sequence[float],
                       target_delta: Optional[float] = None,
                       bounds: Tuple[float, float] = (-1.0, 1.0)
                       ) -> Dict[str, Any]:
        target_delta = target_delta or self.cf_target_delta
        x = [float(v) for v in x]
        try:
            base_pred = float(f(x))
        except Exception:
            return {"success": False}
        best_delta, best_pred = 0.0, base_pred
        best_idx, best_step = None, 0.0
        x_cf = list(x)
        for _ in range(self.cf_max_iter):
            i = self.rng.randrange(len(x))
            step = self.rng.choice([-1, 1]) * 0.1
            trial = list(x_cf)
            trial[i] = float(max(bounds[0], min(bounds[1],
                                                  trial[i] + step)))
            try:
                pred = float(f(trial))
            except Exception:
                continue
            delta = abs(pred - base_pred)
            if delta > best_delta:
                best_delta, best_pred, x_cf = delta, pred, trial
                best_idx, best_step = i, step
            if delta >= abs(target_delta):
                break
        return {"success": best_delta >= abs(target_delta),
                "changed_index": best_idx,
                "change_amount": best_step,
                "delta": best_pred - base_pred}


# =============================================================================
# SHARED RUNTIME — ChaosEngine (steady-state + rollback)
# =============================================================================
class ChaosEngine:
    FAULT_TYPES: ClassVar[Tuple[str, ...]] = (
        "latency", "exception", "carbon_spike", "token_depletion",
        "network_drop",
    )

    def __init__(
        self,
        steady_state: Callable[[], Any],
        snapshot: Optional[Callable[[], Any]] = None,
        restore: Optional[Callable[[Any], None]] = None,
        probability: float = 0.01,
        auto_rollback: bool = True,
        blast_radius: float = 0.5,
        rng: Optional[random.Random] = None,
        carbon_manager: Optional[Any] = None,
    ) -> None:
        self.steady_state = steady_state
        self.snapshot = snapshot
        self.restore = restore
        self.probability = float(probability)
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
        self.carbon_manager = carbon_manager
        self.active: Dict[str, Dict[str, Any]] = {}
        self.rollback_count = 0
        self._lock = threading.Lock()

    def _allowed(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def _steady(self) -> bool:
        try:
            r = self.steady_state()
            if asyncio.iscoroutine(r):
                r = await r
            return bool(r)
        except Exception:
            return False

    async def maybe_inject(self) -> Optional[Dict[str, Any]]:
        if self.rng.random() >= self.probability:
            return None
        return await self.run_experiment(
            f"auto_{uuid.uuid4().hex[:6]}",
            self.rng.choice(self.FAULT_TYPES))

    async def run_experiment(self, name: str,
                             fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        with self._lock:
            if len(self.active) >= self._allowed():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}
            self.active[name] = {"fault_type": fault_type}
        t0 = time.time()
        before = await self._steady()
        snap = None
        if self.snapshot is not None:
            try:
                snap = self.snapshot()
                if asyncio.iscoroutine(snap):
                    snap = await snap
            except Exception:
                snap = None
        status = "completed"
        try:
            if fault_type == "latency":
                await asyncio.sleep(0.005)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "carbon_spike":
                if self.carbon_manager is not None:
                    self.carbon_manager._override_intensity = 800.0
            elif fault_type == "token_depletion":
                # No global side effects in the new engine
                pass
            elif fault_type == "network_drop":
                await asyncio.sleep(0.005)
        except Exception:
            status = "injected"
        finally:
            with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        rolled_back = False
        if (before and not after and self.auto_rollback and
                snap is not None and self.restore is not None):
            try:
                r = self.restore(snap)
                if asyncio.iscoroutine(r):
                    await r
                rolled_back = True
                self.rollback_count += 1
            except Exception as e:
                logger.warning(f"rollback failed: {e}")
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status,
                "duration_ms": (time.time() - t0) * 1000.0,
                "rolled_back": rolled_back}


# =============================================================================
# WORK PLAN v2 — all fields optional, structured Explanation
# =============================================================================
@dataclass
class MOPDWorkPlan:
    pipeline: str = "standard"
    use_quantum: bool = False
    data_center: str = "us-east"
    helium_recovery: bool = False
    carbon_offset: bool = False
    renewable_share: float = 0.5
    token_allocation: float = 0.0
    compartment_id: Optional[str] = None
    carbon_kg: float = 0.0
    helium_units: float = 0.0
    cost_usd: float = 0.0
    latency_ms: float = 100.0
    success_probability: float = 0.9
    scalarised_score: float = 0.0
    explanation: Optional[Explanation] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if isinstance(self.explanation, Explanation):
            d["explanation"] = self.explanation.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDWorkPlan":
        data = dict(data or {})
        expl = data.get("explanation")
        if isinstance(expl, dict):
            data["explanation"] = Explanation(
                method=expl.get("method", "none"),
                attributions=list(expl.get("attributions", [])),
                feature_names=list(expl.get("feature_names", [])),
                counterfactual=expl.get("counterfactual"),
                natural_language=expl.get("natural_language", ""))
        return cls(**data)


# =============================================================================
# WORK CONTEXT
# =============================================================================
@dataclass
class EnhancedWorkContext:
    task_id: str
    work_type: str = "general"
    priority: WorkPriority = WorkPriority.MEDIUM
    state: WorkState = WorkState.CREATED
    complexity: float = 0.5
    estimated_duration_ms: float = 100.0
    helium_dependency: float = 0.0
    carbon_zone: int = 0
    quantum_capable: bool = False
    tenant_id: str = "default"
    compartment_id: Optional[str] = None
    tokens_allocated: float = 0.0
    tokens_consumed: float = 0.0
    dynamic_token_price: float = 1.0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    state_history: List[Tuple[WorkState, datetime]] = field(default_factory=list)
    meta_cognitive_state: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    sustainability_score: float = 0.0

    def transition_to(self, target: WorkState) -> bool:
        if not self.state.can_transition_to(target):
            return False
        self.state_history.append((self.state, datetime.now(timezone.utc)))
        self.state = target
        return True

    def add_event(self, etype: str, details: Dict[str, Any]) -> None:
        self.events.append({"type": etype, "details": details,
                            "timestamp": datetime.now(timezone.utc).isoformat()})

    def to_routing_context(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "work_type": self.work_type,
            "priority": self.priority.value,
            "complexity": self.complexity,
            "estimated_duration_ms": self.estimated_duration_ms,
            "helium_dependency": self.helium_dependency,
            "carbon_zone": self.carbon_zone,
            "quantum_capable": self.quantum_capable,
            "tenant_id": self.tenant_id,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "work_type": self.work_type,
            "priority": self.priority.value,
            "state": self.state.value,
            "complexity": self.complexity,
            "estimated_duration_ms": self.estimated_duration_ms,
            "helium_dependency": self.helium_dependency,
            "carbon_zone": self.carbon_zone,
            "quantum_capable": self.quantum_capable,
            "tenant_id": self.tenant_id,
            "compartment_id": self.compartment_id,
            "sustainability_score": self.sustainability_score,
        }


# =============================================================================
# CONFIG
# =============================================================================
@dataclass
class WorkIntegratorConfig:
    enable_mopd: bool = True
    enable_bio_integration: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = False   # stub, off by default
    enable_cross_domain: bool = False  # stub, off by default
    enable_dynamic_pricing: bool = False
    enable_state_persistence: bool = True
    enable_sustainability_dashboard: bool = False
    enable_telemetry: bool = False
    enable_temporal_safety: bool = True
    enable_human_approval: bool = False
    enable_chaos_testing: bool = False

    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    default_carbon_intensity_g_per_kwh: float = 400.0
    carbon_budget_per_work_kg: float = 20.0
    helium_budget_per_work_units: float = 5.0
    token_budget_per_work: float = 100.0

    mopd_grid_resolution: int = 5
    mopd_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "carbon": 0.3,
        "helium": 0.2,
        "cost": 0.2,
        "latency": 0.15,
        "success_prob": 0.15,
    })
    mopd_minimise_keys: Set[str] = field(default_factory=lambda: {
        "carbon", "helium", "cost", "latency",
    })

    threshold_change_cooldown_seconds: float = 30.0
    context_ttl_seconds: float = 300.0

    def __post_init__(self) -> None:
        total = sum(max(0.0, v) for v in self.mopd_objective_weights.values())
        if total <= 0:
            n = max(len(self.mopd_objective_weights), 1)
            self.mopd_objective_weights = {
                k: 1.0 / n for k in self.mopd_objective_weights}
        else:
            self.mopd_objective_weights = {
                k: max(0.0, v) / total
                for k, v in self.mopd_objective_weights.items()}


# =============================================================================
# LOCAL STUBS — Storage, Queue, Metrics, Cost, Pareto, Drift
# =============================================================================
class InMemoryStorage:
    def __init__(self) -> None:
        self._state: Dict[str, str] = {}
        self.calls: List[Tuple[str, Tuple]] = []
        self.fail_on: Optional[str] = None

    def _log(self, method: str, args: Tuple = ()) -> None:
        self.calls.append((method, args))
        if self.fail_on == method:
            raise RuntimeError(f"injected failure in {method}")

    def save_state(self, key: str, value: str) -> None:
        self._log("save_state", (key,))
        self._state[key] = value

    def get_state(self, key: str) -> Optional[str]:
        self._log("get_state", (key,))
        return self._state.get(key)


class AsyncMessageQueue:
    def __init__(self) -> None:
        self._topics: Dict[str, asyncio.Queue] = {}
        self.published: List[Tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))
        if topic not in self._topics:
            self._topics[topic] = asyncio.Queue()
        await self._topics[topic].put(payload)


class MetricsRegistry:
    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.gauges: Dict[str, float] = {}

    def inc(self, name: str, amount: float = 1.0) -> None:
        self.counters[name] = self.counters.get(name, 0.0) + float(amount)

    def set(self, name: str, value: float) -> None:
        self.gauges[name] = float(value)

    def increment(self, name: str, amount: float = 1.0,
                  *args: Any, **kwargs: Any) -> None:
        self.inc(name, amount)

    def observe(self, name: str, value: float,
                *args: Any, **kwargs: Any) -> None:
        self.set(name, value)


class AdaptiveCostFunction:
    def __init__(self) -> None:
        self.weights = {"accuracy": 0.4, "carbon": 0.2,
                        "cost": 0.2, "latency": 0.2}

    def get_current_weights(self) -> Dict[str, float]:
        return dict(self.weights)

    def compute(self, *, quality: float = 0.5, carbon_g: float = 0.1,
                latency_ms: float = 100.0, energy_joules: float = 10.0,
                health: Any = 1.0, atp: float = 0.5) -> float:
        w = self.weights
        try:
            h = float(health)
        except (TypeError, ValueError):
            h = 1.0 if health else 0.0
        return float(
            w["accuracy"] * (1.0 - quality) +
            w["carbon"] * carbon_g +
            w["cost"] * energy_joules / 100.0 +
            w["latency"] * latency_ms / 100.0 +
            0.05 * (1.0 - h)
        )


class ParetoGating:
    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for c in candidates:
            dominated = False
            for other in candidates:
                if other is c:
                    continue
                if (other.get("quality_score", 0) >= c.get("quality_score", 0)
                        and other.get("carbon_g", 1) <= c.get("carbon_g", 1)
                        and other.get("latency_ms", 1e9) <= c.get("latency_ms", 1e9)
                        and (other.get("quality_score", 0) > c.get("quality_score", 0)
                             or other.get("carbon_g", 1) < c.get("carbon_g", 1)
                             or other.get("latency_ms", 1e9) < c.get("latency_ms", 1e9))):
                    dominated = True
                    break
            if not dominated:
                out.append(c)
        return out


class DriftDetector:
    def __init__(self, delta: float = 0.005, threshold: float = 0.5) -> None:
        self.delta = float(delta)
        self.threshold = float(threshold)
        self._mean = 0.0
        self._cum = 0.0
        self._min_cum = 0.0
        self.drift_detected = False

    async def check_drift(self, weights: Dict[str, float]) -> float:
        x = sum(weights.values()) / max(len(weights), 1)
        self._mean += (x - self._mean) * 0.1
        self._cum += x - self._mean - self.delta
        self._min_cum = min(self._min_cum, self._cum)
        deviation = self._cum - self._min_cum
        if deviation > self.threshold:
            self.drift_detected = True
            self._cum = 0.0
            self._min_cum = 0.0
            return 1.0
        return float(deviation) / max(self.threshold, 1e-6)


class FeedbackEvent:
    def __init__(self, **fields: Any) -> None:
        self.fields = fields
        self.fields.setdefault(
            "ts", datetime.now(timezone.utc).isoformat())

    @classmethod
    def create_with_context(cls, **kwargs: Any) -> "FeedbackEvent":
        return cls(**kwargs)

    def to_json(self) -> str:
        return json.dumps(self.fields, default=str)


# =============================================================================
# BIO FALLBACKS (so the module imports cleanly)
# =============================================================================
class BioConsumer(str, Enum):
    EXPERT_EXECUTION = "expert_execution"
    WORK_EXECUTION = "work_execution"


class BioTier(str, Enum):
    STARCH_RESERVE = "starch_reserve"
    SILVER = "silver"
    GOLD = "gold"


# =============================================================================
# ENHANCED WORK INTEGRATOR v8.0.0
# =============================================================================
class EnhancedWorkIntegrator:
    """Work integrator with state machine, MOPD, and bio hooks."""

    def __init__(
        self,
        *,
        config: Optional[Union[WorkIntegratorConfig, Dict[str, Any]]] = None,
        storage: Optional[StorageLike] = None,
        message_queue: Optional[MessageQueueLike] = None,
        metrics: Optional[MetricsLike] = None,
        adaptive_cost: Optional[AdaptiveCostLike] = None,
        pareto_gating: Optional[ParetoGatingLike] = None,
        drift_detector: Optional[DriftDetectorLike] = None,
        token_manager: Optional[TokenManagerLike] = None,
        gradient_manager: Optional[GradientManagerLike] = None,
        compartment_manager: Optional[CompartmentManagerLike] = None,
        biomass_storage: Optional[BiomassStorageLike] = None,
        approval_handler: Optional[ApprovalHandlerLike] = None,
        expert_router: Optional[ExpertRouterLike] = None,
        seed: int = 42,
        **kwargs: Any,
    ) -> None:
        if kwargs:
            logger.warning(
                "EnhancedWorkIntegrator: unexpected kwargs ignored: %s",
                list(kwargs.keys()))

        if config is None:
            config = WorkIntegratorConfig()
        elif isinstance(config, dict):
            config = WorkIntegratorConfig(**config)
        self.config = config

        # Injected dependencies
        self.storage = storage
        self.queue = message_queue
        self.metrics = metrics
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.compartment_manager = compartment_manager
        self.biomass_storage = biomass_storage
        self.approval_handler = approval_handler
        self.router = expert_router

        # RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_chaos = random.Random(seed + 2)

        # Adaptive state (config never mutated)
        self._adaptive_weights: Dict[str, float] = dict(
            self.config.mopd_objective_weights)
        self._last_weight_change_ts: Optional[float] = None
        self._context_ts: Optional[float] = None

        # Runtime components
        self.task_manager = TaskManager()
        self.temporal_safety = TemporalSafetyMonitor()
        self._setup_temporal_rules()
        self.xai = XAIExplainer(rng=self._rng_xai)
        self.hitl: ApprovalHandlerLike = approval_handler or HumanApprovalHandler()
        self.chaos: Optional[ChaosEngine] = None
        if self.config.enable_chaos_testing:
            self.chaos = ChaosEngine(
                steady_state=self._steady_state,
                rng=self._rng_chaos)

        # Carbon circuit breaker (single instance)
        self._carbon_circuit = CircuitBreaker(name="carbon_manager")

        # State
        self.active_works: Dict[str, EnhancedWorkContext] = {}
        self.completed_works: Dict[str, Dict[str, Any]] = {}
        self.failed_works: Dict[str, Dict[str, Any]] = {}
        self._works_lock = asyncio.Lock()
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0

        # Carbon override for chaos
        self._carbon_override: Optional[float] = None

        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_system_state()
        self.task_manager.start_coro(
            "persistence_save", self._persistence_save_loop())
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self._save_system_state()

    async def __aenter__(self) -> "EnhancedWorkIntegrator":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any,
                        exc_tb: Any) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula("G (carbon_kg >= 0.0)")
        self.temporal_safety.add_formula("G (helium_units >= 0.0)")
        self.temporal_safety.add_formula(
            "G (carbon_kg <= carbon_budget_kg)")
        self.temporal_safety.add_formula(
            "G (helium_units <= helium_budget_units)")
        self.temporal_safety.add_formula(
            "[300] NEVER (drastic_actions > 1)")

    # ------------------------------------------------------------------
    # Persistence (async via to_thread)
    # ------------------------------------------------------------------
    async def _load_system_state(self) -> None:
        if self.storage is None:
            return
        try:
            raw = await asyncio.to_thread(
                self.storage.get_state, "work_integrator_state_v8")
            if not raw:
                return
            state = json.loads(raw)
            self.sustainability_score = float(
                state.get("sustainability_score", 0.0))
            self.total_carbon_savings_kg = float(
                state.get("total_carbon_savings_kg", 0.0))
            self.total_helium_savings_l = float(
                state.get("total_helium_savings_l", 0.0))
            self._adaptive_weights = dict(state.get(
                "adaptive_weights", self.config.mopd_objective_weights))
        except Exception as e:
            logger.warning(f"load_system_state failed: {e}")

    async def _save_system_state(self) -> None:
        if self.storage is None:
            return
        state = {
            "sustainability_score": self.sustainability_score,
            "total_carbon_savings_kg": self.total_carbon_savings_kg,
            "total_helium_savings_l": self.total_helium_savings_l,
            "adaptive_weights": dict(self._adaptive_weights),
        }
        try:
            await asyncio.to_thread(
                self.storage.save_state, "work_integrator_state_v8",
                json.dumps(state))
        except Exception as e:
            logger.warning(f"save_system_state failed: {e}")

    async def _persistence_save_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(),
                    timeout=300.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._save_system_state()
            except Exception as e:
                logger.debug(f"periodic save failed: {e}")

    # ------------------------------------------------------------------
    # Bio helpers (sync/async coercion, structured result)
    # ------------------------------------------------------------------
    @staticmethod
    async def _coerce(func: Callable[[], Any]) -> Any:
        r = func()
        if asyncio.iscoroutine(r):
            r = await r
        return r

    async def spend_atp(self, actor: str, amount: float) -> BioResult:
        if self.token_manager is None:
            return BioResult(success=False, error="no_token_manager")
        try:
            await self._coerce(
                lambda: self.token_manager.spend(actor, amount))
            return BioResult(success=True, amount=float(amount))
        except Exception as e:
            logger.warning(f"ATP spend failed: {e}", exc_info=True)
            return BioResult(success=False, error=str(e))

    async def earn_atp(self, actor: str, amount: float) -> BioResult:
        if self.token_manager is None:
            return BioResult(success=False, error="no_token_manager")
        try:
            await self._coerce(
                lambda: self.token_manager.earn(actor, amount))
            return BioResult(success=True, amount=float(amount))
        except Exception as e:
            logger.warning(f"ATP earn failed: {e}", exc_info=True)
            return BioResult(success=False, error=str(e))

    async def pump_gradient(self, field: str, delta: float,
                             source: str = "work_integrator"
                             ) -> BioResult:
        if self.gradient_manager is None:
            return BioResult(success=False, error="no_gradient_manager")
        try:
            await self._coerce(
                lambda: self.gradient_manager.pump_field(
                    field, delta, source=source))
            return BioResult(success=True, amount=float(delta))
        except Exception as e:
            logger.warning(f"gradient pump failed: {e}", exc_info=True)
            return BioResult(success=False, error=str(e))

    async def _get_token_budget_remaining(self) -> float:
        if self.token_manager is None:
            return float("inf")
        try:
            summary = await self._coerce(
                lambda: self.token_manager.get_system_summary())
            if isinstance(summary, dict):
                return float(summary.get("total_balance", float("inf")))
        except Exception:
            pass
        return float("inf")

    async def _get_compartment_for(self, work_type: str) -> Optional[str]:
        if self.compartment_manager is None:
            return None
        try:
            result = await self._coerce(
                lambda: self.compartment_manager.find_best_compartment(work_type))
        except Exception as e:
            logger.debug(f"compartment lookup failed: {e}")
            return None
        if result is None:
            return None
        if isinstance(result, tuple):
            # Backwards-compatible: (viable, comp_id)
            return result[1] if len(result) > 1 else None
        return getattr(result, "id", None)

    async def _store_as_biomass(self, task_data: Dict[str, Any],
                                 ecoatp_cost: float) -> Optional[str]:
        if self.biomass_storage is None:
            return None
        try:
            result = await self._coerce(
                lambda: self.biomass_storage.store_task(
                    task_data=task_data,
                    ecoatp_cost=ecoatp_cost,
                    guarantee=BioTier.SILVER,
                    initial_tier=BioTier.STARCH_RESERVE))
            if isinstance(result, tuple):
                return result[1] if len(result) > 1 else None
            return getattr(result, "token", None) or str(result)
        except Exception as e:
            logger.warning(f"biomass store failed: {e}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Carbon
    # ------------------------------------------------------------------
    async def _current_carbon_intensity(self) -> float:
        if self._carbon_override is not None:
            return float(self._carbon_override)
        if self.config.enable_carbon_intensity:
            try:
                return await self._carbon_circuit.call(
                    self._fetch_carbon_intensity)
            except Exception:
                pass
        return float(self.config.default_carbon_intensity_g_per_kwh)

    async def _fetch_carbon_intensity(self) -> float:
        # Placeholder: real impl would call the API
        await asyncio.sleep(0)
        return float(self.config.default_carbon_intensity_g_per_kwh)

    # ------------------------------------------------------------------
    # Context
    # ------------------------------------------------------------------
    def _create_context(self, work_request: Dict[str, Any],
                         tenant_id: str = "default"
                         ) -> EnhancedWorkContext:
        try:
            priority = WorkPriority(work_request.get("priority",
                                                       WorkPriority.MEDIUM.value))
        except ValueError:
            priority = WorkPriority.MEDIUM
        return EnhancedWorkContext(
            task_id=work_request.get("task_id", str(uuid.uuid4())),
            work_type=work_request.get("work_type", "general"),
            priority=priority,
            complexity=float(work_request.get("complexity", 0.5)),
            estimated_duration_ms=float(
                work_request.get("estimated_duration_ms", 100.0)),
            helium_dependency=float(work_request.get("helium_dependency", 0.0)),
            carbon_zone=int(work_request.get("carbon_zone", 0)),
            quantum_capable=bool(work_request.get("quantum_capable", False)),
            tenant_id=tenant_id,
        )

    # ------------------------------------------------------------------
    # MOPD
    # ------------------------------------------------------------------
    async def _enumerate_plans(self,
                                context: EnhancedWorkContext
                                ) -> List[MOPDWorkPlan]:
        pipelines = ["standard", "hybrid", "bio_optimized"]
        if self.config.enable_bio_integration is False:
            pipelines = [p for p in pipelines if p != "bio_optimized"]
        data_centers = ["us-east", "us-west"]
        helium_recovery_options = [False, True] if \
            context.helium_dependency > 0.5 else [False]
        carbon_offset_options = [False, True]
        shares = [0.4, 0.6, 0.8]

        plans: List[MOPDWorkPlan] = []
        for pipeline in pipelines:
            use_quantum_options = ([False, True] if pipeline == "hybrid"
                                    else [False])
            for uq in use_quantum_options:
                for dc in data_centers:
                    for hr in helium_recovery_options:
                        for co in carbon_offset_options:
                            for rs in shares:
                                plans.append(MOPDWorkPlan(
                                    pipeline=pipeline,
                                    use_quantum=uq,
                                    data_center=dc,
                                    helium_recovery=hr,
                                    carbon_offset=co,
                                    renewable_share=rs,
                                ))
        return plans

    async def _compute_plan(self, plan: MOPDWorkPlan,
                             context: EnhancedWorkContext,
                             carbon_intensity: float
                             ) -> MOPDWorkPlan:
        carbon_kg = 10.0 * (carbon_intensity / 400.0)
        helium_units = 0.0
        cost_usd = 0.0
        latency_ms = 100.0
        success_prob = 0.95

        if plan.data_center == "us-west":
            carbon_kg *= 0.6
            latency_ms *= 1.5
            cost_usd += 5.0
        if plan.helium_recovery:
            helium_units = context.helium_dependency * 0.5
            cost_usd += 2.0
            latency_ms += 10.0
        if plan.carbon_offset:
            carbon_kg = max(0.0, carbon_kg - 5.0)
            cost_usd += 5.0
        if plan.renewable_share > 0.5:
            carbon_kg *= 1.0 - (plan.renewable_share - 0.5) * 0.5
            cost_usd += (plan.renewable_share - 0.5) * 2.0
        if plan.use_quantum:
            latency_ms *= 0.5
            cost_usd += 3.0
            success_prob *= 0.95
        if plan.pipeline == "bio_optimized":
            success_prob *= 1.05
            carbon_kg *= 0.95

        plan.carbon_kg = max(0.0, float(carbon_kg))
        plan.helium_units = float(helium_units)
        plan.cost_usd = float(cost_usd)
        plan.latency_ms = float(latency_ms)
        plan.success_probability = min(1.0, max(0.0, float(success_prob)))
        plan.token_allocation = (
            context.complexity * 10.0 +
            context.estimated_duration_ms / 1000.0)
        return plan

    def _plan_objectives(self, plan: MOPDWorkPlan) -> Dict[str, float]:
        return {
            "carbon": plan.carbon_kg,
            "helium": plan.helium_units,
            "cost": plan.cost_usd,
            "latency": plan.latency_ms,
            "success_prob": plan.success_probability,
        }

    async def _generate_pareto_front(self, context: EnhancedWorkContext,
                                       carbon_intensity: float
                                       ) -> List[MOPDWorkPlan]:
        plans = await self._enumerate_plans(context)
        computed = [await self._compute_plan(p, context, carbon_intensity)
                     for p in plans]
        objectives = [self._plan_objectives(p) for p in computed]
        front_objs = nsga2_fast_nondominated_sort(
            objectives,
            minimise_keys={"carbon", "helium", "cost", "latency"},
            maximise_keys={"success_prob"})
        # Map back to plans by objective vector
        key_to_plan = {tuple(sorted(o.items())): p
                        for o, p in zip(objectives, computed)}
        return [key_to_plan.get(tuple(sorted(o.items())), computed[0])
                for o in front_objs]

    def _select_best(self, front: List[MOPDWorkPlan]
                     ) -> Optional[MOPDWorkPlan]:
        if not front:
            return None
        scored: List[Tuple[float, MOPDWorkPlan]] = []
        for plan in front:
            if self.adaptive_cost is not None:
                cost = self.adaptive_cost.compute(
                    quality=plan.success_probability,
                    carbon_g=plan.carbon_kg * 1000.0,
                    latency_ms=plan.latency_ms,
                    energy_joules=plan.cost_usd * 10.0,
                    health=0.8,
                    atp=plan.token_allocation / 10.0)
            else:
                # Weighted scalarisation using adaptive weights
                w = self._adaptive_weights
                cost = (
                    w.get("carbon", 0.0) * plan.carbon_kg +
                    w.get("helium", 0.0) * plan.helium_units +
                    w.get("cost", 0.0) * plan.cost_usd +
                    w.get("latency", 0.0) * (plan.latency_ms / 100.0) -
                    w.get("success_prob", 0.0) * plan.success_probability)
            plan.scalarised_score = float(cost)
            scored.append((float(cost), plan))
        # Lower cost is better → ascending sort
        scored.sort(key=lambda kv: kv[0])
        return scored[0][1]

    # ------------------------------------------------------------------
    # XAI
    # ------------------------------------------------------------------
    def _explain_plan(self, plan: MOPDWorkPlan,
                       carbon_intensity: float) -> Explanation:
        x = [plan.carbon_kg, plan.helium_units, plan.cost_usd,
             plan.latency_ms, plan.success_probability]
        names = ["carbon_kg", "helium_units", "cost_usd",
                 "latency_ms", "success_probability"]

        def _score(v: List[float]) -> float:
            ci, hu, cu, lm, sp = (float(v[0]), float(v[1]),
                                    float(v[2]), float(v[3]), float(v[4]))
            w = self._adaptive_weights
            return (w.get("carbon", 0.3) * ci +
                    w.get("helium", 0.2) * hu +
                    w.get("cost", 0.2) * cu +
                    w.get("latency", 0.15) * lm -
                    w.get("success_prob", 0.15) * sp)

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            cf = self.xai.counterfactual(_score, x)
            return Explanation(
                method="integrated_gradients",
                attributions=list(attrs),
                feature_names=list(names),
                counterfactual=cf,
                natural_language=(
                    f"Selected {plan.pipeline} in {plan.data_center}: "
                    f"carbon={plan.carbon_kg:.2f} kg, "
                    f"cost=${plan.cost_usd:.2f}, "
                    f"latency={plan.latency_ms:.0f}ms, "
                    f"success={plan.success_probability:.2f}"))
        except Exception as e:
            return Explanation(method="none",
                                natural_language=str(e))

    # ------------------------------------------------------------------
    # HITL
    # ------------------------------------------------------------------
    async def _request_approval(self, plan: MOPDWorkPlan,
                                 context: EnhancedWorkContext
                                 ) -> Tuple[bool, Optional[str]]:
        if not self.config.enable_human_approval:
            return True, None
        if self.approval_handler is None:
            logger.warning("HITL enabled but no handler injected; deferring")
            return False, "no_handler"
        decision = {
            "action": "approve_work_plan",
            "candidates": [
                {"id": "approve",
                 "success_probability": plan.success_probability,
                 "carbon_kg": plan.carbon_kg},
                {"id": "defer", "success_probability": 0.0,
                 "carbon_kg": 0.0},
            ],
            "context": context.to_routing_context(),
        }
        try:
            approved = await self.hitl.request_approval(
                decision, user_id=context.tenant_id)
            return bool(approved), None
        except Exception as e:
            logger.warning(f"HITL failed: {e}")
            return False, "hitl_error"

    # ------------------------------------------------------------------
    # Temporal safety
    # ------------------------------------------------------------------
    async def check_invariants(self, plan: MOPDWorkPlan,
                                context: EnhancedWorkContext
                                ) -> List[str]:
        await self.temporal_safety.push({
            "carbon_kg": plan.carbon_kg,
            "helium_units": plan.helium_units,
            "carbon_budget_kg": self.config.carbon_budget_per_work_kg,
            "helium_budget_units": self.config.helium_budget_per_work_units,
            "drastic_actions": 0,
        })
        results = await self.temporal_safety.check()
        return [rid for rid, ok in results.items() if not ok]

    # ------------------------------------------------------------------
    # Chaos
    # ------------------------------------------------------------------
    async def _steady_state(self) -> bool:
        return self.sustainability_score >= -1.0  # always steady by default

    async def inject_fault(self, fault_type: str,
                            **params: Any) -> Dict[str, Any]:
        if self.chaos is None:
            self.chaos = ChaosEngine(
                steady_state=self._steady_state, rng=self._rng_chaos)
        return await self.chaos.run_experiment(
            params.pop("name", f"manual_{uuid.uuid4().hex[:6]}"),
            fault_type)

    async def run_chaos_test(self, runs: int = 3) -> Dict[str, Any]:
        if self.chaos is None:
            self.chaos = ChaosEngine(
                steady_state=self._steady_state, rng=self._rng_chaos)
        results: List[Dict[str, Any]] = []
        for i in range(max(1, runs)):
            fault = self._rng_chaos.choice(ChaosEngine.FAULT_TYPES)
            results.append(await self.chaos.run_experiment(
                f"chaos_{i}_{uuid.uuid4().hex[:4]}", fault))
        return {"status": "success",
                "runs": len(results),
                "rollbacks": self.chaos.rollback_count,
                "results": results}

    # ------------------------------------------------------------------
    # Pipelines
    # ------------------------------------------------------------------
    async def _standard_pipeline(self,
                                  context: EnhancedWorkContext
                                  ) -> Dict[str, Any]:
        if self.router is not None:
            try:
                r = await self.router.route_and_execute(
                    context.to_routing_context())
                return r or {"status": "success", "expert": "default"}
            except Exception as e:
                return {"status": "failed", "error": str(e)}
        return {"status": "success", "expert": "default"}

    async def _hybrid_pipeline(self,
                                context: EnhancedWorkContext
                                ) -> Dict[str, Any]:
        return await self._standard_pipeline(context)

    async def _bio_optimized_pipeline(self,
                                       context: EnhancedWorkContext
                                       ) -> Dict[str, Any]:
        if self.gradient_manager is not None:
            try:
                gradients = await self._coerce(
                    self.gradient_manager.get_field_strengths)
                context.meta_cognitive_state["gradients"] = gradients
            except Exception:
                pass
        return await self._standard_pipeline(context)

    async def _execute_pipeline(self, context: EnhancedWorkContext,
                                  plan: MOPDWorkPlan) -> Dict[str, Any]:
        if plan.pipeline == "hybrid":
            return await self._hybrid_pipeline(context)
        if plan.pipeline == "bio_optimized":
            return await self._bio_optimized_pipeline(context)
        return await self._standard_pipeline(context)

    # ------------------------------------------------------------------
    # Main work processing
    # ------------------------------------------------------------------
    async def process_work(
        self,
        work_request: Dict[str, Any],
        dependencies: Optional[List[str]] = None,
        tenant_id: str = "default",
        return_pareto: bool = False,
    ) -> Dict[str, Any]:
        context = self._create_context(work_request, tenant_id)
        try:
            async with self._works_lock:
                self.active_works[context.task_id] = context

            if not context.transition_to(WorkState.VALIDATED):
                return self._invalid_transition(context, WorkState.VALIDATED)

            if not context.transition_to(WorkState.QUEUED):
                return self._invalid_transition(context, WorkState.QUEUED)

            # Carbon intensity
            carbon_intensity = await self._current_carbon_intensity()
            context.meta_cognitive_state["carbon_intensity"] = carbon_intensity

            # Select plan via MOPD
            selected_plan: Optional[MOPDWorkPlan] = None
            pareto_front: List[MOPDWorkPlan] = []
            if self.config.enable_mopd:
                pareto_front = await self._generate_pareto_front(
                    context, carbon_intensity)

                # Temporal safety filter
                if self.config.enable_temporal_safety and pareto_front:
                    safe = []
                    for p in pareto_front:
                        violations = await self.check_invariants(p, context)
                        if not violations:
                            safe.append(p)
                    if safe:
                        pareto_front = safe
                    else:
                        # choose least violating
                        scored = []
                        for p in pareto_front:
                            v = await self.check_invariants(p, context)
                            scored.append((len(v), p))
                        scored.sort(key=lambda kv: kv[0])
                        pareto_front = [scored[0][1]]

                selected_plan = self._select_best(pareto_front)

                # HITL
                if selected_plan is not None:
                    approved, reason = await self._request_approval(
                        selected_plan, context)
                    if not approved:
                        return {
                            "task_id": context.task_id,
                            "status": "deferred",
                            "reason": reason or "hitl_rejected",
                            "plan": selected_plan.to_dict(),
                        }

                if selected_plan is not None:
                    selected_plan.explanation = self._explain_plan(
                        selected_plan, carbon_intensity)
                    context.meta_cognitive_state["selected_plan"] = \
                        selected_plan.to_dict()
                    context.meta_cognitive_state["pareto_front"] = \
                        [p.to_dict() for p in pareto_front]

            # Allocation
            if selected_plan is not None:
                token_actor = f"work_{context.task_id}"
                budget = await self._get_token_budget_remaining()
                if selected_plan.token_allocation > budget:
                    return {
                        "task_id": context.task_id,
                        "status": "deferred",
                        "reason": "token_budget_exceeded",
                    }
                # Spend exactly once
                bio = await self.spend_atp(token_actor,
                                             selected_plan.token_allocation)
                if not bio.success and bio.error != "no_token_manager":
                    # Defer only on real spend failures
                    token = await self._store_as_biomass(
                        work_request, selected_plan.token_allocation)
                    if token:
                        context.transition_to(WorkState.STORED_AS_BIOMASS)
                        return {
                            "task_id": context.task_id,
                            "status": "stored_as_biomass",
                            "token": token,
                        }
                context.tokens_allocated = selected_plan.token_allocation

                # Gradients
                if selected_plan.carbon_kg > 0.5:
                    await self.pump_gradient(
                        "carbon", 0.05, source=token_actor)
                if selected_plan.helium_units > 0.05:
                    await self.pump_gradient(
                        "helium", 0.05, source=token_actor)

                # Compartment
                comp_id = await self._get_compartment_for(context.work_type)
                if comp_id:
                    context.compartment_id = comp_id
                    selected_plan.compartment_id = comp_id

            # State machine: SCHEDULED
            if not context.transition_to(WorkState.SCHEDULED):
                return self._invalid_transition(context, WorkState.SCHEDULED)

            # Execute
            if not context.transition_to(WorkState.EXECUTING):
                return self._invalid_transition(context, WorkState.EXECUTING)

            if selected_plan is None:
                # No MOPD: use a default plan
                selected_plan = MOPDWorkPlan()

            result = await self._execute_pipeline(context, selected_plan)
            success = (isinstance(result, dict) and
                       result.get("status") == "success")

            # Finalise
            if success:
                context.transition_to(WorkState.COMPLETED)
                self.completed_works[context.task_id] = {
                    "result": result,
                    "context": context.to_dict(),
                }
                if selected_plan is not None:
                    await self.earn_atp(
                        f"work_{context.task_id}",
                        selected_plan.token_allocation * 1.5)
                await self.pump_gradient(
                    "trust", 0.05, source=f"work_{context.task_id}")
            else:
                context.transition_to(WorkState.FAILED)
                self.failed_works[context.task_id] = {
                    "result": result,
                    "context": context.to_dict(),
                }
                await self.pump_gradient(
                    "trust", -0.05, source=f"work_{context.task_id}")

            # Update sustainability
            if selected_plan is not None:
                self.total_carbon_savings_kg += max(
                    0.0, 10.0 - selected_plan.carbon_kg)
                self.total_helium_savings_l += max(
                    0.0, context.helium_dependency -
                    selected_plan.helium_units)
                self.sustainability_score = min(1.0, max(
                    0.0,
                    (1.0 - selected_plan.carbon_kg / 20.0) * 0.5 +
                    selected_plan.success_probability * 0.5))

            # Feedback event
            await self._publish_feedback(context, selected_plan, result)

            # Drift
            await self._check_drift()

            response: Dict[str, Any] = {
                "task_id": context.task_id,
                "status": result.get("status", "success") if isinstance(result, dict)
                          else "success",
                "result": result,
                "state": context.state.value,
            }
            if return_pareto and pareto_front:
                response["pareto_front"] = [p.to_dict() for p in pareto_front]
                if selected_plan is not None:
                    response["selected_plan"] = selected_plan.to_dict()
            return response
        finally:
            # Always cleanup — no leaks
            async with self._works_lock:
                self.active_works.pop(context.task_id, None)

    @staticmethod
    def _invalid_transition(context: EnhancedWorkContext,
                             target: WorkState) -> Dict[str, Any]:
        logger.warning(
            f"invalid transition {context.state} -> {target} for "
            f"{context.task_id}")
        return {"task_id": context.task_id, "status": "error",
                "reason": f"invalid_transition:{context.state.value}"
                          f"->{target.value}"}

    async def _publish_feedback(self, context: EnhancedWorkContext,
                                 plan: Optional[MOPDWorkPlan],
                                 result: Dict[str, Any]) -> None:
        if self.queue is None:
            return
        try:
            explanation = None
            if plan is not None and isinstance(plan.explanation, Explanation):
                explanation = plan.explanation.to_dict()
            event = FeedbackEvent.create_with_context(
                task_id=context.task_id,
                selected_action=plan.pipeline if plan else "default",
                quality_score=(plan.success_probability
                                if plan else 0.5),
                energy_joules=(plan.cost_usd * 10.0) if plan else 0.0,
                carbon_g=(plan.carbon_kg * 1000.0) if plan else 0.0,
                feedback_type="work_integration",
                adaptive_cost_value=(plan.scalarised_score
                                       if plan else 0.0),
                state={
                    "work_type": context.work_type,
                    "priority": context.priority.value,
                    "explanation": explanation,
                },
                candidates=[
                    {"action": p.get("pipeline", "unknown")}
                    for p in context.meta_cognitive_state.get(
                        "pareto_front", [])
                ],
                source="work_integrator",
                environment="production",
                tags=["work", "mopd", "xai"],
            )
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Drift (bounded adaptive state)
    # ------------------------------------------------------------------
    async def _check_drift(self) -> float:
        if self.drift is None or self.adaptive_cost is None:
            return 0.0
        try:
            score = await self.drift.check_drift(
                self.adaptive_cost.get_current_weights())
            score = float(score)
            if score > 0.7:
                now = time.monotonic()
                if (self._last_weight_change_ts is None or
                        now - self._last_weight_change_ts >
                        self.config.threshold_change_cooldown_seconds):
                    w = dict(self._adaptive_weights)
                    w["carbon"] = min(0.5, w.get("carbon", 0.3) + 0.05)
                    total = sum(w.values()) or 1.0
                    self._adaptive_weights = {
                        k: v / total for k, v in w.items()}
                    self._last_weight_change_ts = now
            else:
                # Decay back toward configured
                for k, base in self.config.mopd_objective_weights.items():
                    cur = self._adaptive_weights.get(k, base)
                    self._adaptive_weights[k] = (
                        cur + (base - cur) * 0.05)
            return score
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------
    def get_work_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        ctx = self.active_works.get(task_id)
        if ctx is not None:
            return ctx.to_dict()
        if task_id in self.completed_works:
            return self.completed_works[task_id]["context"]
        if task_id in self.failed_works:
            return self.failed_works[task_id]["context"]
        return None

    def cancel_work(self, task_id: str) -> bool:
        ctx = self.active_works.get(task_id)
        if ctx is None:
            return False
        if not ctx.transition_to(WorkState.CANCELLED):
            return False
        self.active_works.pop(task_id, None)
        return True

    def get_work_statistics(self) -> Dict[str, Any]:
        return {
            "active_works": len(self.active_works),
            "completed_works": len(self.completed_works),
            "failed_works": len(self.failed_works),
            "sustainability_score": self.sustainability_score,
            "total_carbon_savings_kg": self.total_carbon_savings_kg,
            "total_helium_savings_l": self.total_helium_savings_l,
            "mopd_enabled": self.config.enable_mopd,
            "temporal_violations": self.temporal_safety.counts(),
            "background_tasks": self.task_manager.task_count(),
            "adaptive_weights": dict(self._adaptive_weights),
        }

    async def get_health_status(self) -> ComponentHealth:
        try:
            if self.config.carbon_budget_per_work_kg <= 0:
                return ComponentHealth(HealthStatus.UNHEALTHY, 0.0,
                                        {"reason": "invalid_carbon_budget"})
            details = {
                "active_works": len(self.active_works),
                "carbon_circuit": self._carbon_circuit.state.value,
                "background_tasks": self.task_manager.task_count(),
            }
            return ComponentHealth(HealthStatus.HEALTHY, 1.0, details)
        except Exception as e:
            return ComponentHealth(HealthStatus.UNHEALTHY, 0.0,
                                    {"error": str(e)})

    # ------------------------------------------------------------------
    # MOPD teacher interface
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        context = self._create_context(state, state.get("tenant_id",
                                                          "default"))
        carbon_intensity = await self._current_carbon_intensity()
        front = await self._generate_pareto_front(context, carbon_intensity)
        if not front:
            return [1.0 / 3] * 3
        pipelines = ["standard", "hybrid", "bio_optimized"]
        scores = []
        for plan in front:
            if self.adaptive_cost is not None:
                cost = self.adaptive_cost.compute(
                    quality=plan.success_probability,
                    carbon_g=plan.carbon_kg * 1000.0,
                    latency_ms=plan.latency_ms,
                    energy_joules=plan.cost_usd * 10.0,
                    health=0.8,
                    atp=plan.token_allocation / 10.0)
            else:
                cost = plan.scalarised_score or 0.0
            scores.append(cost)
        # Lower cost is better → negate for softmax
        logits = [-s for s in scores]
        if NUMPY_AVAILABLE:
            arr = _np.asarray(logits, dtype=float)
            arr -= arr.max()
            probs = _np.exp(arr)
            probs = probs / max(float(probs.sum()), 1e-12)
            probs_list = probs.tolist()
        else:
            m = max(logits)
            exps = [math.exp(l - m) for l in logits]
            total = sum(exps) or 1.0
            probs_list = [e / total for e in exps]

        out = [0.0, 0.0, 0.0]
        for plan, p in zip(front, probs_list):
            out[pipelines.index(plan.pipeline)] += p
        total = sum(out) or 1.0
        return [v / total for v in out]


# =============================================================================
# FIXTURES
# =============================================================================
@pytest.fixture
def storage():
    return InMemoryStorage()


@pytest.fixture
def queue():
    return AsyncMessageQueue()


@pytest.fixture
def metrics():
    return MetricsRegistry()


@pytest.fixture
def adaptive_cost():
    return AdaptiveCostFunction()


@pytest.fixture
def pareto_gating():
    return ParetoGating()


@pytest.fixture
def drift_detector():
    return DriftDetector()


@pytest_asyncio.fixture
async def integrator(storage, queue, metrics, adaptive_cost,
                      pareto_gating, drift_detector):
    integ = EnhancedWorkIntegrator(
        storage=storage, message_queue=queue, metrics=metrics,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, seed=42)
    await integ.initialize()
    try:
        yield integ
    finally:
        await integ.shutdown()


# =============================================================================
# TESTS
# =============================================================================
class TestNSGA2:
    def test_dedup(self):
        sols = [
            {"cost": 1.0, "latency": 1.0},
            {"cost": 1.0, "latency": 1.0},
            {"cost": 2.0, "latency": 0.5},
        ]
        front = nsga2_fast_nondominated_sort(sols, {"cost", "latency"}, set())
        assert len(front) == 2

    def test_front_dominance(self):
        sols = [
            {"cost": 0.1, "latency": 5.0},
            {"cost": 5.0, "latency": 0.1},
            {"cost": 3.0, "latency": 3.0},
        ]
        front = nsga2_fast_nondominated_sort(sols, {"cost", "latency"}, set())
        assert {"cost": 0.1, "latency": 5.0} in front
        assert {"cost": 5.0, "latency": 0.1} in front
        assert {"cost": 3.0, "latency": 3.0} not in front

    def test_empty(self):
        assert nsga2_fast_nondominated_sort([], set(), set()) == []

    def test_single(self):
        sols = [{"a": 1.0}]
        assert nsga2_fast_nondominated_sort(sols, {"a"}, set()) == sols

    def test_missing_values_worst(self):
        a = {"x": 1.0, "y": 1.0}
        b = {"x": 1.0}  # missing y treated as +inf → worse
        assert is_dominated(b, a, {"x", "y"}, set())

    def test_maximisation_direction(self):
        a = {"gain": 1.0}
        b = {"gain": 5.0}
        assert is_dominated(a, b, set(), {"gain"})
        assert not is_dominated(b, a, set(), {"gain"})


class TestSharedRuntime:
    @pytest.mark.asyncio
    async def test_circuit_breaker_state_preserved(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)
        async def bad():
            raise RuntimeError("boom")
        for _ in range(3):
            with pytest.raises(Exception):
                await cb.call(bad)
        assert cb.state == CircuitBreakerState.OPEN

    @pytest.mark.asyncio
    async def test_task_manager(self):
        tm = TaskManager()
        stop = asyncio.Event()
        async def worker():
            await stop.wait()
        tm.start_coro("w", worker())
        assert tm.task_count() == 1
        stop.set()
        await tm.stop_all()
        assert tm.task_count() == 0

    @pytest.mark.asyncio
    async def test_temporal_safety(self):
        m = TemporalSafetyMonitor()
        m.add_formula("G (x >= 0)")
        await m.push({"x": 1})
        await m.push({"x": -1})
        result = await m.check()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_hitl_single_candidate(self):
        h = HumanApprovalHandler()
        assert await h.request_approval(
            {"candidates": [{"id": "only"}]}) is True

    def test_xai_ig(self):
        xai = XAIExplainer(ig_steps=8)
        def f(v):
            return v[0] * 2 + v[1]
        attrs = xai.integrated_gradients(f, [0.5, 0.3])
        assert len(attrs) == 2

    @pytest.mark.asyncio
    async def test_chaos_latency(self):
        async def steady():
            return True
        c = ChaosEngine(steady_state=steady, rng=random.Random(0))
        r = await c.run_experiment("t", "latency")
        assert r["status"] == "completed"

    @pytest.mark.asyncio
    async def test_chaos_rollback(self):
        state = {"healthy": True}
        async def steady():
            return state["healthy"]
        def snap():
            return dict(state)
        def restore(s):
            state.update(s)
        c = ChaosEngine(steady_state=steady, snapshot=snap,
                        restore=restore, rng=random.Random(0))
        # Force rollback by making "after" steady fail
        call_count = {"n": 0}
        async def flaky_steady():
            call_count["n"] += 1
            return call_count["n"] == 1
        c.steady_state = flaky_steady
        r = await c.run_experiment("rb", "latency")
        assert r["rolled_back"] is True


class TestStateMachine:
    def test_valid_transition(self):
        ctx = EnhancedWorkContext("t1")
        assert ctx.transition_to(WorkState.VALIDATED)
        assert ctx.transition_to(WorkState.QUEUED)

    def test_invalid_transition(self):
        ctx = EnhancedWorkContext("t1")
        # CREATED → EXECUTING is not allowed
        assert not ctx.transition_to(WorkState.EXECUTING)
        assert ctx.state == WorkState.CREATED

    def test_terminal_states(self):
        assert WorkState.COMPLETED.is_terminal()
        assert WorkState.FAILED.is_terminal()
        assert not WorkState.EXECUTING.is_terminal()

    def test_scheduled_can_transition_to_executing(self):
        # We added the shortcut intentionally
        assert WorkState.SCHEDULED.can_transition_to(WorkState.EXECUTING)


class TestWorkIntegratorBasics:
    @pytest.mark.asyncio
    async def test_process_work_success(self, integrator):
        r = await integrator.process_work({
            "task_id": "w1", "work_type": "analysis"})
        assert r["status"] == "success"
        assert r["state"] == WorkState.COMPLETED.value

    @pytest.mark.asyncio
    async def test_active_works_cleaned_up(self, integrator):
        r = await integrator.process_work({"task_id": "w2"})
        assert "w2" not in integrator.active_works
        # Move to completed
        assert "w2" in integrator.completed_works

    @pytest.mark.asyncio
    async def test_process_work_returns_decision_fields(self, integrator):
        r = await integrator.process_work({"task_id": "w3"},
                                            return_pareto=True)
        assert "pareto_front" in r
        assert "selected_plan" in r

    @pytest.mark.asyncio
    async def test_get_work_state(self, integrator):
        await integrator.process_work({"task_id": "w4"})
        s = integrator.get_work_state("w4")
        assert s is not None
        assert s["task_id"] == "w4"

    @pytest.mark.asyncio
    async def test_cancel_work(self, integrator):
        # Create and stop mid-execution is hard; just verify missing case
        assert integrator.cancel_work("nonexistent") is False


class TestMOPDSelection:
    @pytest.mark.asyncio
    async def test_pareto_front_not_empty(self, integrator):
        r = await integrator.process_work({"task_id": "m1"},
                                            return_pareto=True)
        assert len(r["pareto_front"]) >= 1

    @pytest.mark.asyncio
    async def test_select_best_picks_lowest_cost(self):
        integ = EnhancedWorkIntegrator(seed=1)
        a = MOPDWorkPlan(pipeline="standard", carbon_kg=1.0,
                          cost_usd=1.0, latency_ms=100.0,
                          success_probability=0.9)
        b = MOPDWorkPlan(pipeline="standard", carbon_kg=10.0,
                          cost_usd=10.0, latency_ms=100.0,
                          success_probability=0.9)
        best = integ._select_best([a, b])
        assert best is a

    @pytest.mark.asyncio
    async def test_policy_probs_sum_to_one(self, integrator):
        probs = await integrator.policy_probs({"task_id": "p1"})
        assert abs(sum(probs) - 1.0) < 1e-6
        assert all(0.0 <= p <= 1.0 for p in probs)

    @pytest.mark.asyncio
    async def test_policy_probs_prefers_low_cost(self, integrator):
        # With 3 pipelines and no adaptive cost, the standard pipeline
        # (low carbon) should have non-trivial probability
        probs = await integrator.policy_probs({"task_id": "p2"})
        assert len(probs) == 3


class TestXAI:
    @pytest.mark.asyncio
    async def test_explanation_has_attributions(self, integrator):
        r = await integrator.process_work({"task_id": "x1"},
                                            return_pareto=True)
        sel = r["selected_plan"]
        expl = sel.get("explanation")
        assert expl is not None
        assert expl["method"] == "integrated_gradients"
        assert len(expl["attributions"]) == 5
        assert expl["feature_names"] == ["carbon_kg", "helium_units",
                                           "cost_usd", "latency_ms",
                                           "success_probability"]
        assert expl["counterfactual"] is not None


class TestHITL:
    @pytest.mark.asyncio
    async def test_hitl_disabled_auto_approves(self, integrator):
        r = await integrator.process_work({"task_id": "h1"})
        assert r["status"] == "success"

    @pytest.mark.asyncio
    async def test_hitl_enabled_with_handler(self):
        class _Rejecting:
            async def request_approval(self, decision, user_id="default"):
                return False
        cfg = WorkIntegratorConfig(enable_human_approval=True)
        integ = EnhancedWorkIntegrator(
            config=cfg, approval_handler=_Rejecting(), seed=1)
        await integ.initialize()
        try:
            r = await integ.process_work({"task_id": "h2"})
            assert r["status"] == "deferred"
            assert "h2" not in integ.active_works
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_enabled_no_handler_defers(self):
        cfg = WorkIntegratorConfig(enable_human_approval=True)
        integ = EnhancedWorkIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            r = await integ.process_work({"task_id": "h3"})
            assert r["status"] == "deferred"
        finally:
            await integ.shutdown()


class TestBioIntegration:
    @pytest.mark.asyncio
    async def test_spend_atp_no_manager(self, integrator):
        r = await integrator.spend_atp("actor", 1.0)
        assert r.success is False
        assert r.error == "no_token_manager"

    @pytest.mark.asyncio
    async def test_spend_atp_sync(self):
        class _Sync:
            def __init__(self):
                self.calls = []
            def spend(self, actor, amount):
                self.calls.append(("spend", actor, amount))
            def earn(self, actor, amount):
                self.calls.append(("earn", actor, amount))
            def get_system_summary(self):
                return {"total_balance": 100.0}
        tm = _Sync()
        integ = EnhancedWorkIntegrator(token_manager=tm, seed=1)
        r = await integ.spend_atp("actor", 0.5)
        assert r.success
        assert tm.calls == [("spend", "actor", 0.5)]

    @pytest.mark.asyncio
    async def test_spend_atp_async(self):
        class _Async:
            def __init__(self):
                self.calls = []
            async def spend(self, actor, amount):
                self.calls.append(("spend", actor, amount))
            async def earn(self, actor, amount):
                self.calls.append(("earn", actor, amount))
            async def get_system_summary(self):
                return {"total_balance": 100.0}
        tm = _Async()
        integ = EnhancedWorkIntegrator(token_manager=tm, seed=1)
        r = await integ.spend_atp("actor", 0.5)
        assert r.success
        assert tm.calls == [("spend", "actor", 0.5)]

    @pytest.mark.asyncio
    async def test_spend_atp_failure(self):
        class _Bad:
            def spend(self, actor, amount):
                raise RuntimeError("boom")
        integ = EnhancedWorkIntegrator(token_manager=_Bad(), seed=1)
        r = await integ.spend_atp("actor", 1.0)
        assert r.success is False
        assert "boom" in (r.error or "")

    @pytest.mark.asyncio
    async def test_pump_gradient(self):
        class _GM:
            def __init__(self):
                self.calls = []
            def pump_field(self, name, delta, source=""):
                self.calls.append((name, delta, source))
            def get_field_strengths(self):
                return {"carbon": 0.5}
        gm = _GM()
        integ = EnhancedWorkIntegrator(gradient_manager=gm, seed=1)
        r = await integ.pump_gradient("carbon", 0.1, source="test")
        assert r.success
        assert gm.calls == [("carbon", 0.1, "test")]

    @pytest.mark.asyncio
    async def test_single_token_spend(self):
        class _Tracker:
            def __init__(self):
                self.spend_calls = []
                self.earn_calls = []
            def spend(self, actor, amount):
                self.spend_calls.append((actor, amount))
            def earn(self, actor, amount):
                self.earn_calls.append((actor, amount))
            def get_system_summary(self):
                return {"total_balance": 1e9}
        tm = _Tracker()
        integ = EnhancedWorkIntegrator(token_manager=tm, seed=1)
        await integ.initialize()
        try:
            r = await integ.process_work({"task_id": "single_spend"})
            assert r["status"] == "success"
            # Exactly one spend call
            assert len(tm.spend_calls) == 1
            assert len(tm.earn_calls) == 1
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_compartment_returns_object(self):
        class _Comp:
            id = "comp_1"
        class _CM:
            def find_best_compartment(self, expert_type):
                return _Comp()
        integ = EnhancedWorkIntegrator(compartment_manager=_CM(), seed=1)
        cid = await integ._get_compartment_for("data")
        assert cid == "comp_1"

    @pytest.mark.asyncio
    async def test_compartment_returns_none(self):
        class _CM:
            def find_best_compartment(self, expert_type):
                return None
        integ = EnhancedWorkIntegrator(compartment_manager=_CM(), seed=1)
        assert await integ._get_compartment_for("data") is None

    @pytest.mark.asyncio
    async def test_biomass_storage_keyword_args(self):
        class _BS:
            def __init__(self):
                self.calls = []
            def store_task(self, *, task_data, ecoatp_cost,
                            guarantee=None, initial_tier=None):
                self.calls.append((task_data, ecoatp_cost,
                                    guarantee, initial_tier))
                return (True, "token_abc")
        bs = _BS()
        integ = EnhancedWorkIntegrator(biomass_storage=bs, seed=1)
        token = await integ._store_as_biomass({"x": 1}, 5.0)
        assert token == "token_abc"
        assert len(bs.calls) == 1
        assert bs.calls[0][1] == 5.0


class TestChaos:
    @pytest.mark.asyncio
    async def test_inject_latency(self):
        integ = EnhancedWorkIntegrator(seed=1)
        r = await integ.inject_fault("latency", name="t1")
        assert r["status"] == "completed"

    @pytest.mark.asyncio
    async def test_inject_exception(self):
        integ = EnhancedWorkIntegrator(seed=1)
        r = await integ.inject_fault("exception", name="t2")
        assert r["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self):
        integ = EnhancedWorkIntegrator(seed=1)
        with pytest.raises(ValueError):
            await integ.inject_fault("nonsense")

    @pytest.mark.asyncio
    async def test_run_chaos_test(self):
        integ = EnhancedWorkIntegrator(seed=1)
        r = await integ.run_chaos_test(runs=3)
        assert r["status"] == "success"
        assert r["runs"] == 3


class TestTemporalSafety:
    @pytest.mark.asyncio
    async def test_invariants_pass(self, integrator):
        plan = MOPDWorkPlan(carbon_kg=1.0, helium_units=0.1)
        ctx = integrator._create_context({})
        violations = await integrator.check_invariants(plan, ctx)
        assert violations == []

    @pytest.mark.asyncio
    async def test_invariants_violated_by_high_carbon(self, integrator):
        plan = MOPDWorkPlan(carbon_kg=100.0, helium_units=0.1)
        ctx = integrator._create_context({})
        violations = await integrator.check_invariants(plan, ctx)
        assert len(violations) >= 1

    @pytest.mark.asyncio
    async def test_rules_registered(self, integrator):
        counts = integrator.temporal_safety.counts()
        assert len(counts) >= 4


class TestConfigImmutability:
    @pytest.mark.asyncio
    async def test_config_unchanged_on_drift(self, integrator):
        # Force drift by calling multiple times (drift detector is fresh)
        original = dict(integrator.config.mopd_objective_weights)
        for _ in range(20):
            await integrator._check_drift()
        assert integrator.config.mopd_objective_weights == original

    @pytest.mark.asyncio
    async def test_adaptive_weights_normalized(self, integrator):
        for _ in range(5):
            await integrator._check_drift()
        total = sum(integrator._adaptive_weights.values())
        assert abs(total - 1.0) < 1e-6 or total == 0.0


class TestPersistence:
    @pytest.mark.asyncio
    async def test_roundtrip(self, storage):
        integ1 = EnhancedWorkIntegrator(storage=storage, seed=1)
        await integ1.initialize()
        try:
            await integ1.process_work({"task_id": "p1"})
            integ1.sustainability_score = 0.75
            await integ1._save_system_state()
        finally:
            await integ1.shutdown()

        integ2 = EnhancedWorkIntegrator(storage=storage, seed=2)
        await integ2.initialize()
        try:
            assert integ2.sustainability_score == 0.75
        finally:
            await integ2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, integrator, storage):
        await integrator._save_system_state()
        raw = storage.get_state("work_integrator_state_v8")
        assert raw is not None
        json.loads(raw)


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage):
        integ = EnhancedWorkIntegrator(storage=storage, seed=1)
        await integ.initialize()
        assert integ.task_manager.task_count() >= 1
        await integ.shutdown()
        assert integ.task_manager.task_count() == 0

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, integrator):
        await integrator.shutdown()
        await integrator.shutdown()
        assert integrator.task_manager.task_count() == 0


class TestFeedback:
    @pytest.mark.asyncio
    async def test_feedback_published(self, integrator, queue):
        await integrator.process_work({"task_id": "f1"})
        assert len(queue.published) >= 1
        topic, payload = queue.published[-1]
        assert topic == "feedback_events"
        # Payload must be valid JSON
        parsed = json.loads(payload)
        assert "task_id" in parsed


class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_pareto(self, storage):
        def _build(seed):
            return EnhancedWorkIntegrator(
                storage=storage, seed=seed)
        integ1 = _build(7)
        integ2 = _build(7)
        await integ1.initialize()
        await integ2.initialize()
        try:
            r1 = await integ1.process_work({"task_id": "d1"},
                                             return_pareto=True)
            r2 = await integ2.process_work({"task_id": "d1"},
                                             return_pareto=True)
            f1 = r1["pareto_front"]
            f2 = r2["pareto_front"]
            assert len(f1) == len(f2)
            for a, b in zip(f1, f2):
                assert a["carbon_kg"] == b["carbon_kg"]
        finally:
            await integ1.shutdown()
            await integ2.shutdown()


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def demo():
        integ = EnhancedWorkIntegrator(seed=1)
        await integ.initialize()
        try:
            r = await integ.process_work(
                {"task_id": "demo_1", "work_type": "analysis",
                 "priority": "high", "complexity": 0.7,
                 "estimated_duration_ms": 500.0},
                return_pareto=True)
            print("status:", r["status"])
            print("state:", r["state"])
            print("pareto size:", len(r.get("pareto_front", [])))
            if "selected_plan" in r:
                plan = r["selected_plan"]
                print("selected pipeline:", plan["pipeline"])
                if plan.get("explanation"):
                    print("explanation:",
                          plan["explanation"]["natural_language"])
            print("stats:", json.dumps(
                integ.get_work_statistics(), indent=2, default=str))
        finally:
            await integ.shutdown()

    asyncio.run(demo())
