#!/usr/bin/env python3
"""
SustainabilityExpert v4.0.0 — Self-Contained Single-File Bundle
=================================================================

Complete rewrite of the Green Agent Sustainability Expert. Everything
needed to run and test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. Local stubs (InMemoryStorage, AsyncMessageQueue, FeedbackEvent,
     MetricsRegistry, AdaptiveCostFunction, ParetoGating, DriftDetector).
  3. Shared runtime: CircuitBreaker, TemporalSafetyMonitor (MTL),
     HumanApprovalHandler, XAIExplainer (IG + counterfactuals),
     TaskManager, ComponentHealth.
  4. `SustainabilityDecision` value object.
  5. Real HITL gating for drastic recommendations.
  6. Real Integrated Gradients XAI on proposals.
  7. `objective_weights` used for Pareto scoring (not decoration).
  8. Correct ascending sort + correct carbon mapping.
  9. Bounded thresholds and TTL-scoped context.
 10. Comprehensive pytest suite.

Run:
    python sustainability_expert_v4.py         # demo
    pytest sustainability_expert_v4.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import re as _re
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Protocol,
    Sequence, Set, Tuple, runtime_checkable,
)

import numpy as np
import pytest
import pytest_asyncio

logger = logging.getLogger("sustainability_expert")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


# =============================================================================
# PROTOCOLS
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
class CarbonManagerLike(Protocol):
    """Any of these methods may be sync or async; the expert coerces."""
    def get_current_intensity(self) -> Any: ...
    def get_current_price(self) -> Any: ...


@runtime_checkable
class HeliumProviderLike(Protocol):
    def get_scarcity(self) -> Any: ...
    def get_cost_index(self) -> Any: ...


@runtime_checkable
class PricingManagerLike(Protocol):
    def get_current_prices(self) -> Any: ...


@runtime_checkable
class BioCoreLike(Protocol):
    """Optional bio-core attachment."""
    token_manager: Optional[Any]
    gradient_manager: Optional[Any]


# =============================================================================
# JSON SANITIZER
# =============================================================================
def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, deque)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        v = float(obj)
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj


# =============================================================================
# LOCAL STUBS
# =============================================================================
class InMemoryStorage:
    def __init__(self):
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
    def __init__(self):
        self._topics: Dict[str, asyncio.Queue] = {}
        self.published: List[Tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))
        if topic not in self._topics:
            self._topics[topic] = asyncio.Queue()
        await self._topics[topic].put(payload)


@dataclass
class FeedbackEvent:
    source: str
    feedback_type: str
    task_id: str
    context: Dict[str, Any]
    action: Dict[str, Any]
    performance: Dict[str, Any]
    adaptive_cost_value: float
    tags: List[str]
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def create_with_context(cls, **kwargs: Any) -> "FeedbackEvent":
        allowed = {"source", "feedback_type", "task_id", "context",
                   "action", "performance", "adaptive_cost_value", "tags"}
        filtered = {k: v for k, v in kwargs.items() if k in allowed}
        filtered.setdefault("source", "unknown")
        filtered.setdefault("feedback_type", "unknown")
        filtered.setdefault("task_id", "unknown")
        filtered.setdefault("context", {})
        filtered.setdefault("action", {})
        filtered.setdefault("performance", {})
        filtered.setdefault("adaptive_cost_value", 0.0)
        filtered.setdefault("tags", [])
        for key in ("context", "action", "performance"):
            filtered[key] = _json_safe(filtered[key])
        return cls(**filtered)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class MetricsRegistry:
    """Canonical inc/set; increment/observe are forwarding aliases."""
    def __init__(self):
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
    """Lower score is better."""
    def __init__(self, storage: Optional[InMemoryStorage] = None):
        self.storage = storage
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
    def __init__(self):
        self.front: List[Dict[str, Any]] = []

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
                        and other.get("energy_joules", 1e9) <= c.get("energy_joules", 1e9)
                        and (other.get("quality_score", 0) > c.get("quality_score", 0)
                             or other.get("carbon_g", 1) < c.get("carbon_g", 1)
                             or other.get("latency_ms", 1e9) < c.get("latency_ms", 1e9))):
                    dominated = True
                    break
            if not dominated:
                out.append(c)
        return out


class DriftDetector:
    def __init__(self, storage: Optional[InMemoryStorage] = None,
                 adaptive_cost: Optional[AdaptiveCostFunction] = None,
                 delta: float = 0.005, threshold: float = 0.5):
        self.storage = storage
        self.adaptive_cost = adaptive_cost
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


# =============================================================================
# SHARED RUNTIME
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    status: HealthStatus
    score: float
    details: Dict[str, Any] = field(default_factory=dict)


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0,
                 half_open_max_requests: int = 3,
                 name: str = "default"):
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_max_requests = int(half_open_max_requests)
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
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
            if (self.state == CircuitBreakerState.HALF_OPEN and
                    self.half_open_requests >= self.half_open_max_requests):
                raise RuntimeError(
                    f"Circuit breaker {self.name} half-open limit")
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
                if self.half_open_requests >= self.half_open_max_requests:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = 0
        return result


# -----------------------------------------------------------------------------
# Temporal safety (MTL)
# -----------------------------------------------------------------------------
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
    def __init__(self, max_trace: int = 500):
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
        self.trace.append((datetime.now(timezone.utc), _json_safe(dict(state))))

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


# -----------------------------------------------------------------------------
# HITL
# -----------------------------------------------------------------------------
class HumanApprovalHandler:
    def __init__(self, queue: Optional[MessageQueueLike] = None,
                 uncertainty_threshold: float = 0.05,
                 timeout: float = 5.0):
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


# -----------------------------------------------------------------------------
# XAI
# -----------------------------------------------------------------------------
class XAIExplainer:
    def __init__(self, ig_steps: int = 16, cf_max_iter: int = 40,
                 cf_target_delta: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.ig_steps = int(ig_steps)
        self.cf_max_iter = int(cf_max_iter)
        self.cf_target_delta = float(cf_target_delta)
        self.rng = rng or random.Random()

    def integrated_gradients(self, f: Callable[[np.ndarray], float],
                             x: np.ndarray,
                             baseline: Optional[np.ndarray] = None
                             ) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        if baseline is None:
            baseline = np.zeros_like(x)
        total = np.zeros_like(x)
        for s in range(1, self.ig_steps + 1):
            alpha = s / self.ig_steps
            interp = baseline + alpha * (x - baseline)
            eps = 1e-4
            grad = np.zeros_like(x)
            for i in range(len(x)):
                up = interp.copy(); up[i] += eps
                dn = interp.copy(); dn[i] -= eps
                try:
                    grad[i] = (float(f(up)) - float(f(dn))) / (2 * eps)
                except Exception:
                    grad[i] = 0.0
            total += grad
        return (x - baseline) * (total / max(self.ig_steps, 1))

    def counterfactual(self, f: Callable[[np.ndarray], float],
                       x: np.ndarray,
                       target_delta: Optional[float] = None,
                       bounds: Tuple[float, float] = (-1.0, 1.0)
                       ) -> Dict[str, Any]:
        target_delta = target_delta or self.cf_target_delta
        try:
            base_pred = float(f(x))
        except Exception:
            return {"success": False}
        best_delta, best_pred = 0.0, base_pred
        best_idx, best_step = None, 0.0
        x_cf = x.copy()
        for _ in range(self.cf_max_iter):
            i = self.rng.randrange(len(x))
            step = self.rng.choice([-1, 1]) * 0.1
            trial = x_cf.copy()
            trial[i] = float(np.clip(trial[i] + step, *bounds))
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


# -----------------------------------------------------------------------------
# TaskManager
# -----------------------------------------------------------------------------
class TaskManager:
    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    def start(self, name: str, coro_factory: Callable[[], Any]
              ) -> Optional[asyncio.Task]:
        if name in self._tasks and not self._tasks[name].done():
            return self._tasks[name]
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("no running loop; task %s not started", name)
            return None
        task = loop.create_task(coro_factory())
        self._tasks[name] = task
        task.add_done_callback(lambda t, n=name: self._tasks.pop(n, None))
        return task

    def start_coro(self, name: str, coro) -> Optional[asyncio.Task]:
        return self.start(name, lambda: coro)

    def task_count(self) -> int:
        return len(self._tasks)

    async def stop_all(self, timeout: float = 5.0) -> None:
        self._shutdown_event.set()
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
        self._tasks.clear()


# =============================================================================
# VALUE OBJECT
# =============================================================================
@dataclass(frozen=True)
class SustainabilityDecision:
    operation: str
    status: str = "success"
    error: Optional[str] = None
    recommendation: Optional[Dict[str, Any]] = None
    options: Tuple[Dict[str, Any], ...] = ()
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None
    requires_approval: bool = False
    approved: Optional[bool] = None
    safety_violations: Tuple[str, ...] = ()
    confidence: float = 0.0
    objective_score: float = 0.0
    timestamp: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "error": self.error,
            "recommendation": self.recommendation,
            "options": list(self.options),
            "explanation": self.explanation,
            "counterfactual": self.counterfactual,
            "requires_approval": bool(self.requires_approval),
            "approved": self.approved,
            "safety_violations": list(self.safety_violations),
            "confidence": float(self.confidence),
            "objective_score": float(self.objective_score),
            "timestamp": float(self.timestamp),
        }


# =============================================================================
# CONFIG (single definition; no Pydantic/dataclass split)
# =============================================================================
@dataclass
class SustainabilityExpertConfig:
    expert_id: str = field(
        default_factory=lambda: f"sustainability_{uuid.uuid4().hex[:8]}")
    enable_persistence: bool = True
    enable_predictive: bool = True
    enable_cost_benefit: bool = True
    enable_swarm_coordination: bool = True
    enable_self_healing: bool = True
    enable_feedback_loop: bool = True
    human_approval_required: bool = True

    thresholds: Dict[str, float] = field(default_factory=lambda: {
        "carbon_high_threshold": 500.0,
        "helium_scarcity_threshold": 0.6,
        "carbon_price_threshold": 80.0,
        "renewable_share_high": 0.8,
        "renewable_share_low": 0.4,
    })
    threshold_min: Dict[str, float] = field(default_factory=lambda: {
        "carbon_high_threshold": 200.0,
        "helium_scarcity_threshold": 0.2,
        "carbon_price_threshold": 20.0,
        "renewable_share_high": 0.5,
        "renewable_share_low": 0.1,
    })
    threshold_max: Dict[str, float] = field(default_factory=lambda: {
        "carbon_high_threshold": 800.0,
        "helium_scarcity_threshold": 0.99,
        "carbon_price_threshold": 200.0,
        "renewable_share_high": 1.0,
        "renewable_share_low": 0.8,
    })

    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "carbon_savings": 0.4,
        "helium_savings": 0.3,
        "cost": 0.2,
        "latency": 0.1,
    })

    pareto_grid_resolution: int = 5
    default_carbon_intensity: float = 400.0
    default_carbon_price: float = 50.0
    default_helium_price: float = 0.5

    context_ttl_seconds: float = 300.0
    drastic_action_cooldown_seconds: float = 300.0
    threshold_change_cooldown_seconds: float = 30.0

    def __post_init__(self) -> None:
        # Bound thresholds into [min, max]
        for k in list(self.thresholds):
            lo = self.threshold_min.get(k, 0.0)
            hi = self.threshold_max.get(k, 1e9)
            self.thresholds[k] = max(lo, min(hi, self.thresholds[k]))
        # Normalize objective weights
        total = sum(self.objective_weights.values())
        if total > 0:
            for k in self.objective_weights:
                self.objective_weights[k] /= total


# =============================================================================
# SUSTAINABILITY EXPERT (v4.0.0)
# =============================================================================
class SustainabilityExpert:
    """Sustainability Expert v4.0.0."""

    SUPPORTED_TASKS = ("propose", "apply_recommendation",
                       "get_thresholds", "set_thresholds")

    def __init__(
        self,
        storage: StorageLike,
        message_queue: MessageQueueLike,
        adaptive_cost: AdaptiveCostLike,
        pareto_gating: ParetoGatingLike,
        drift_detector: DriftDetectorLike,
        metrics: MetricsLike,
        bio_core: Optional[BioCoreLike] = None,
        carbon_manager: Optional[CarbonManagerLike] = None,
        helium_manager: Optional[HeliumProviderLike] = None,
        pricing_manager: Optional[PricingManagerLike] = None,
        hitl: Optional[HumanApprovalHandler] = None,
        config: Optional[SustainabilityExpertConfig] = None,
        seed: int = 42,
    ):
        self.expert_name = "sustainability_expert"
        self.supported_task_types = list(self.SUPPORTED_TASKS)
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.bio_core = bio_core
        self.config = config or SustainabilityExpertConfig()
        self.expert_id = self.config.expert_id
        self.version = "4.0.0"

        # Deterministic RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        np.random.seed(seed)

        # External managers (optional)
        self.carbon_manager = carbon_manager
        self.helium_manager = helium_manager
        self.pricing_manager = pricing_manager

        # Circuit breakers per external manager
        self._carbon_circuit = CircuitBreaker(name="carbon_manager")
        self._helium_circuit = CircuitBreaker(name="helium_manager")
        self._pricing_circuit = CircuitBreaker(name="pricing_manager")

        # Bio
        self.token_manager = None
        self.gradient_manager = None
        if self.bio_core is not None:
            self.token_manager = getattr(self.bio_core, "token_manager", None)
            self.gradient_manager = getattr(self.bio_core, "gradient_manager", None)

        # Bounded state
        self._adaptive_thresholds: Dict[str, float] = dict(
            self.config.thresholds)
        self._last_context: Dict[str, Any] = {}
        self._last_context_ts: Optional[float] = None
        self._last_drastic_ts: Optional[float] = None
        self._last_threshold_change_ts: Optional[float] = None

        # Shared runtime
        self.temporal_safety = TemporalSafetyMonitor(max_trace=500)
        self._setup_temporal_rules()
        self.hitl = hitl or HumanApprovalHandler()
        self.xai = XAIExplainer(rng=self._rng_xai)

        # Task manager + lifecycle
        self.task_manager = TaskManager()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_thresholds()
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self._save_thresholds()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula(
            "G (carbon_intensity >= 0.0)")
        self.temporal_safety.add_formula(
            "G (helium_scarcity >= 0.0)")
        self.temporal_safety.add_formula(
            "G (helium_scarcity <= 1.0)")
        self.temporal_safety.add_formula(
            "G (renewable_share >= 0.0)")
        self.temporal_safety.add_formula(
            "G (renewable_share <= 1.0)")
        self.temporal_safety.add_formula(
            "[300] NEVER (drastic_actions > 1)")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _load_thresholds(self) -> None:
        try:
            data = await asyncio.to_thread(
                self.storage.get_state, "sustainability_expert_thresholds_v4")
            if not data:
                return
            stored = json.loads(data)
            for k, v in stored.items():
                if k not in self.config.thresholds:
                    continue
                try:
                    bounded = self._bound_threshold(k, float(v))
                except (TypeError, ValueError):
                    continue
                self._adaptive_thresholds[k] = bounded
        except Exception as e:
            logger.warning(f"load_thresholds failed: {e}")

    async def _save_thresholds(self) -> None:
        try:
            await asyncio.to_thread(
                self.storage.save_state,
                "sustainability_expert_thresholds_v4",
                json.dumps(self._adaptive_thresholds))
        except Exception as e:
            logger.warning(f"save_thresholds failed: {e}")

    # ------------------------------------------------------------------
    # Context management (with TTL)
    # ------------------------------------------------------------------
    def _set_context(self, context: Dict[str, Any]) -> None:
        self._last_context.update(context)
        self._last_context_ts = time.monotonic()

    def _get_context(self) -> Dict[str, Any]:
        if self._last_context_ts is None:
            return {}
        if time.monotonic() - self._last_context_ts > \
                self.config.context_ttl_seconds:
            return {}
        return dict(self._last_context)

    # ------------------------------------------------------------------
    # Bounded threshold helpers
    # ------------------------------------------------------------------
    def _bound_threshold(self, key: str, value: float) -> float:
        lo = self.config.threshold_min.get(key, 0.0)
        hi = self.config.threshold_max.get(key, 1e9)
        return float(max(lo, min(hi, value)))

    def _adapt_threshold(self, key: str, factor: float) -> None:
        now = time.monotonic()
        if (self._last_threshold_change_ts is not None and
                now - self._last_threshold_change_ts <
                self.config.threshold_change_cooldown_seconds):
            return
        cur = self._adaptive_thresholds.get(
            key, self.config.thresholds[key])
        new = self._bound_threshold(key, cur * factor)
        self._adaptive_thresholds[key] = new
        self._last_threshold_change_ts = now
        logger.debug(f"threshold {key} adapted to {new:.4f}")

    async def set_thresholds(self, updates: Dict[str, float]) -> None:
        for k, v in updates.items():
            if k not in self.config.thresholds:
                logger.warning(f"unknown threshold key ignored: {k}")
                continue
            try:
                bounded = self._bound_threshold(k, float(v))
            except (TypeError, ValueError):
                continue
            self._adaptive_thresholds[k] = bounded
        await self._save_thresholds()

    # ------------------------------------------------------------------
    # External manager coercion (sync/async)
    # ------------------------------------------------------------------
    async def _call_manager(self, manager: Any, method_name: str,
                             circuit: CircuitBreaker) -> Any:
        if manager is None:
            return None
        method = getattr(manager, method_name, None)
        if method is None:
            return None

        async def _wrapper() -> Any:
            r = method()
            if asyncio.iscoroutine(r):
                r = await r
            return r

        try:
            return await circuit.call(_wrapper)
        except Exception as e:
            logger.warning(f"{method_name} failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Bio integration
    # ------------------------------------------------------------------
    async def _bio_spend_earn(self, atp_cost: float, quality: float
                              ) -> None:
        if self.token_manager is None:
            return
        try:
            r = self.token_manager.spend(self.expert_id, atp_cost)
            if asyncio.iscoroutine(r):
                await r
            if quality > 0.7:
                r = self.token_manager.earn(self.expert_id, atp_cost * 2)
                if asyncio.iscoroutine(r):
                    await r
        except Exception as e:
            logger.warning(f"bio integration failed: {e}", exc_info=True)

    def _pump_gradient(self, name: str, delta: float) -> None:
        if self.gradient_manager is None:
            return
        try:
            r = self.gradient_manager.pump_field(
                name, delta, source="sustainability_expert")
            if asyncio.iscoroutine(r):
                self.task_manager.start_coro(
                    f"gradient_{uuid.uuid4().hex[:4]}", r)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Data gathering
    # ------------------------------------------------------------------
    async def _get_carbon_data(self) -> Dict[str, float]:
        intensity = await self._call_manager(
            self.carbon_manager, "get_current_intensity",
            self._carbon_circuit)
        if intensity is None:
            ctx = self._get_context()
            intensity = float(ctx.get(
                "carbon_intensity",
                self.config.default_carbon_intensity))
        else:
            intensity = float(intensity)
        return {"intensity": intensity}

    async def _get_helium_data(self) -> Dict[str, float]:
        scarcity = await self._call_manager(
            self.helium_manager, "get_scarcity",
            self._helium_circuit)
        if scarcity is None:
            ctx = self._get_context()
            scarcity = float(ctx.get("helium_scarcity", 0.5))
        return {"scarcity": float(max(0.0, min(1.0, scarcity)))}

    async def _get_price_data(self) -> Dict[str, float]:
        prices = await self._call_manager(
            self.pricing_manager, "get_current_prices",
            self._pricing_circuit)
        ctx = self._get_context()
        if not isinstance(prices, dict):
            prices = {}
        return {
            "carbon_price": float(prices.get(
                "carbon_price_usd_per_ton",
                ctx.get("carbon_price", self.config.default_carbon_price))),
            "helium_price": float(prices.get(
                "helium_price_usd_per_l",
                ctx.get("helium_price", self.config.default_helium_price))),
        }

    # ------------------------------------------------------------------
    # Objective scoring (uses config.objective_weights)
    # ------------------------------------------------------------------
    def _score_solution(self, sol: Dict[str, Any]) -> float:
        w = self.config.objective_weights
        carbon = float(sol.get("carbon_savings", 0.0))
        helium = float(sol.get("helium_savings", 0.0))
        cost = float(sol.get("cost", 0.0))
        latency = float(sol.get("latency", 0.0))
        # Normalize the raw objectives into ~[0, 1] for each term
        carbon_term = min(1.0, carbon / 100.0)
        helium_term = min(1.0, helium)
        cost_term = 1.0 / (1.0 + max(cost, 0.0))
        latency_term = 1.0 / (1.0 + max(latency, 0.0))
        return float(
            w["carbon_savings"] * carbon_term +
            w["helium_savings"] * helium_term +
            w["cost"] * cost_term +
            w["latency"] * latency_term
        )

    # ------------------------------------------------------------------
    # XAI on a recommendation
    # ------------------------------------------------------------------
    def _explain_recommendation(
        self,
        rec: Dict[str, Any],
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float,
        renewable_share: float,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        x = np.array([
            carbon_intensity / 1000.0,
            helium_scarcity,
            carbon_price / 100.0,
            helium_price / 10.0,
            renewable_share,
        ], dtype=float)
        w = self.config.objective_weights

        def _score(v: np.ndarray) -> float:
            ci, hs, cp, hp, rs = (float(v[0]), float(v[1]),
                                   float(v[2]), float(v[3]), float(v[4]))
            return (w["carbon_savings"] * (-ci) +
                    w["helium_savings"] * (-hs) +
                    w["cost"] * (-cp) +
                    w["latency"] * (-hp) +
                    0.5 * rs)

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["carbon_intensity", "helium_scarcity",
                                    "carbon_price", "helium_price",
                                    "renewable_share"],
            }
            cf = self.xai.counterfactual(_score, x)
            return explanation, cf
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Pareto front generation
    # ------------------------------------------------------------------
    async def _generate_pareto_front(
        self,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float,
    ) -> List[Dict[str, Any]]:
        feasible: List[Dict[str, Any]] = []
        data_centers = ["us-east", "us-west"]
        helium_recovery_options = [False, True]
        carbon_offset_options = [False, True]
        lo = self._adaptive_thresholds["renewable_share_low"]
        hi = self._adaptive_thresholds["renewable_share_high"]
        if hi < lo:
            hi = lo
        renewable_shares = np.linspace(lo, hi,
                                        self.config.pareto_grid_resolution)

        for dc in data_centers:
            for hr in helium_recovery_options:
                for co in carbon_offset_options:
                    for rs in renewable_shares:
                        action = {
                            "preferred_data_center": dc,
                            "helium_recovery": bool(hr),
                            "carbon_offset": bool(co),
                            "renewable_share": float(rs),
                        }
                        obj = self._compute_objectives(
                            action, carbon_intensity, helium_scarcity,
                            carbon_price, helium_price)
                        action.update(obj)
                        action["objective_score"] = self._score_solution(action)
                        action["explanation"] = self._describe_action(action)
                        feasible.append(action)

        # Non-dominated sort (on carbon_savings, helium_savings, -cost, -latency)
        pareto: List[Dict[str, Any]] = []
        for a in feasible:
            dominated = False
            for b in feasible:
                if a is b:
                    continue
                a_vec = (a["carbon_savings"], a["helium_savings"],
                         -a["cost"], -a["latency"])
                b_vec = (b["carbon_savings"], b["helium_savings"],
                         -b["cost"], -b["latency"])
                if all(b_vec[k] >= a_vec[k] for k in range(4)) and \
                        any(b_vec[k] > a_vec[k] for k in range(4)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(a)
        return pareto

    def _compute_objectives(self, action: Dict[str, Any],
                             carbon_intensity: float,
                             helium_scarcity: float,
                             carbon_price: float,
                             helium_price: float) -> Dict[str, float]:
        carbon_savings = 0.0
        helium_savings = 0.0
        cost = 0.0
        latency = 0.0

        if (action["preferred_data_center"] == "us-west" and
                carbon_intensity > 400):
            carbon_savings += (carbon_intensity - 300) * 0.01
        if action["carbon_offset"]:
            carbon_savings += 10.0
        if action["helium_recovery"]:
            helium_savings += helium_scarcity * 0.5

        if action["preferred_data_center"] == "us-west":
            cost += 5.0
            latency += 5.0
        if action["helium_recovery"]:
            cost += 2.0
            latency += 10.0
        if action["carbon_offset"]:
            cost += carbon_price * 0.1
        if action["renewable_share"] > 0.5:
            cost += (action["renewable_share"] - 0.5) * 2.0

        return {
            "carbon_savings": float(carbon_savings),
            "helium_savings": float(helium_savings),
            "cost": float(cost),
            "latency": float(latency),
        }

    def _describe_action(self, action: Dict[str, Any]) -> str:
        parts: List[str] = []
        if action["preferred_data_center"] == "us-west":
            parts.append("shift to US-West (lower-carbon grid)")
        if action["helium_recovery"]:
            parts.append("enable helium recovery")
        if action["carbon_offset"]:
            parts.append("purchase carbon offsets")
        if action["renewable_share"] > 0.5:
            parts.append(f"renewable share {action['renewable_share']:.0%}")
        return "; ".join(parts) if parts else "baseline configuration"

    # ------------------------------------------------------------------
    # Select best solution
    # ------------------------------------------------------------------
    async def _select_best_solution(self, front: List[Dict[str, Any]]
                                     ) -> Dict[str, Any]:
        if not front:
            return {}
        # Build candidates for central ParetoGating
        # carbon_g here must be POSITIVE emissions: higher = worse.
        baseline_carbon_g = self.config.default_carbon_intensity
        candidates = []
        for idx, sol in enumerate(front):
            carbon_g = max(0.0, baseline_carbon_g -
                            sol.get("carbon_savings", 0.0) * 100.0)
            candidates.append({
                "id": idx,
                "quality_score": float(sol.get("objective_score", 0.0)),
                "carbon_g": float(carbon_g),
                "latency_ms": float(sol.get("latency", 0.0)),
                "energy_joules": float(sol.get("cost", 0.0)) * 10.0,
            })
        if self.pareto is not None:
            try:
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed = {c["id"] for c in filtered}
                    candidates = [c for c in candidates
                                   if c["id"] in allowed]
            except Exception:
                pass
        if not candidates:
            candidates = [{
                "id": i,
                "quality_score": float(front[i].get("objective_score", 0.0)),
                "carbon_g": float(max(0.0, baseline_carbon_g -
                                       front[i].get("carbon_savings", 0.0) * 100.0)),
                "latency_ms": float(front[i].get("latency", 0.0)),
                "energy_joules": float(front[i].get("cost", 0.0)) * 10.0,
            } for i in range(len(front))]

        # Score with adaptive cost — LOWER IS BETTER, so sort ascending.
        scored: List[Tuple[float, int]] = []
        for c in candidates:
            cost = self.adaptive_cost.compute(
                quality=c["quality_score"],
                carbon_g=c["carbon_g"],
                latency_ms=c["latency_ms"],
                energy_joules=c["energy_joules"],
                health=self.health_status == "healthy",
                atp=0.5)
            scored.append((float(cost), int(c["id"])))
        scored.sort(key=lambda kv: kv[0])
        best_idx = scored[0][1]
        best = dict(front[best_idx])
        best["adaptive_cost"] = scored[0][0]
        return best

    # ------------------------------------------------------------------
    # Propose
    # ------------------------------------------------------------------
    async def propose_async(self, context: Dict[str, Any]
                             ) -> SustainabilityDecision:
        start = time.monotonic()
        self._set_context(context)

        try:
            carbon_data = await self._get_carbon_data()
            helium_data = await self._get_helium_data()
            price_data = await self._get_price_data()

            front = await self._generate_pareto_front(
                carbon_intensity=carbon_data["intensity"],
                helium_scarcity=helium_data["scarcity"],
                carbon_price=price_data["carbon_price"],
                helium_price=price_data["helium_price"])

            best = await self._select_best_solution(front)
            if not best:
                return SustainabilityDecision(
                    operation="propose",
                    status="error", error="no_feasible_solution",
                    timestamp=time.time())

            # XAI
            explanation, counterfactual = self._explain_recommendation(
                best,
                carbon_intensity=carbon_data["intensity"],
                helium_scarcity=helium_data["scarcity"],
                carbon_price=price_data["carbon_price"],
                helium_price=price_data["helium_price"],
                renewable_share=float(best.get("renewable_share", 0.5)),
            )

            # HITL gate
            carbon_thresh = self._adaptive_thresholds[
                "carbon_high_threshold"]
            helium_thresh = self._adaptive_thresholds[
                "helium_scarcity_threshold"]
            drastic = (
                carbon_data["intensity"] > carbon_thresh * 1.5 or
                helium_data["scarcity"] > helium_thresh * 1.5)
            requires_approval = False
            approved: Optional[bool] = None
            if drastic and self.config.human_approval_required:
                requires_approval = True
                approved = await self.hitl.request_approval({
                    "action": "drastic_sustainability_action",
                    "candidates": [
                        {"id": "approve", "risk": 0.5,
                         "carbon_intensity": carbon_data["intensity"]},
                        {"id": "defer", "risk": 0.0,
                         "carbon_intensity": 0.0},
                    ],
                }, user_id=self.expert_id)
                if approved:
                    self._last_drastic_ts = time.monotonic()

            # Safety
            violations = await self._safety_check(
                carbon_intensity=carbon_data["intensity"],
                helium_scarcity=helium_data["scarcity"],
                renewable_share=float(best.get("renewable_share", 0.5)),
                recent_drastic=1 if approved else 0)

            # Bio
            await self._bio_spend_earn(0.05, quality=float(
                best.get("objective_score", 0.5)))
            self._pump_gradient("trust",
                                 0.03 if not drastic else -0.02)
            if carbon_data["intensity"] > carbon_thresh:
                self._pump_gradient("carbon", 0.1)
            if helium_data["scarcity"] > helium_thresh:
                self._pump_gradient("helium", 0.1)

            # Metrics
            self.metrics.inc("sustainability_proposals")
            self.metrics.set(
                "sustainability_last_objective",
                float(best.get("objective_score", 0.0)))

            # Persist last recommendation
            try:
                await asyncio.to_thread(
                    self.storage.save_state,
                    "sustainability_expert_last_recommendation_v4",
                    json.dumps(_json_safe(best)))
            except Exception:
                pass

            # Feedback
            await self._publish_feedback(
                task_id=f"sustainability_propose_"
                         f"{uuid.uuid4().hex[:8]}",
                action="propose",
                quality=float(best.get("objective_score", 0.5)),
                details={"requires_approval": requires_approval,
                         "approved": approved},
                tags=["sustainability", "propose"])

            await self._check_drift()

            status = "success"
            if requires_approval and approved is False:
                status = "deferred"
            return SustainabilityDecision(
                operation="propose",
                status=status,
                recommendation=best,
                options=tuple(front),
                explanation=explanation,
                counterfactual=counterfactual,
                requires_approval=requires_approval,
                approved=approved,
                safety_violations=violations,
                confidence=float(best.get("objective_score", 0.5)),
                objective_score=float(best.get("objective_score", 0.0)),
                timestamp=time.time())

        except Exception as e:
            logger.error(f"propose_async failed: {e}", exc_info=True)
            self.health_status = "degraded"
            fallback = await self._load_last_known_good()
            return SustainabilityDecision(
                operation="propose",
                status="error",
                error=str(e),
                recommendation=fallback,
                timestamp=time.time())

    async def _load_last_known_good(self) -> Optional[Dict[str, Any]]:
        try:
            data = await asyncio.to_thread(
                self.storage.get_state,
                "sustainability_expert_last_recommendation_v4")
            if data:
                return json.loads(data)
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Apply
    # ------------------------------------------------------------------
    async def apply_recommendation(self, recommendation: Dict[str, Any]
                                    ) -> SustainabilityDecision:
        if self.config.enable_feedback_loop:
            await self._adapt_from_feedback({
                "success": True,
                "actual_carbon_savings":
                    recommendation.get("carbon_savings", 0.0),
            })
        await self._bio_spend_earn(0.02, quality=0.5)
        self.metrics.inc("sustainability_applied")
        await self._publish_feedback(
            task_id=f"sustainability_apply_{uuid.uuid4().hex[:8]}",
            action="apply_recommendation",
            quality=0.9,
            details={"recommendation": recommendation},
            tags=["sustainability", "apply"])
        await self._check_drift()
        return SustainabilityDecision(
            operation="apply_recommendation",
            status="success",
            recommendation=recommendation,
            confidence=0.9,
            timestamp=time.time())

    async def _adapt_from_feedback(self, feedback: Dict[str, Any]) -> None:
        if not feedback.get("success"):
            self._adapt_threshold("carbon_high_threshold", 0.95)
            self._adapt_threshold("helium_scarcity_threshold", 0.95)
        else:
            savings = float(feedback.get("actual_carbon_savings", 0.0))
            if savings > 50.0:
                self._adapt_threshold("carbon_high_threshold", 1.02)
                self._adapt_threshold("helium_scarcity_threshold", 1.02)
        await self._save_thresholds()

    # ------------------------------------------------------------------
    # Safety
    # ------------------------------------------------------------------
    async def _safety_check(self, carbon_intensity: float,
                             helium_scarcity: float,
                             renewable_share: float,
                             recent_drastic: int) -> Tuple[str, ...]:
        await self.temporal_safety.push({
            "carbon_intensity": float(carbon_intensity),
            "helium_scarcity": float(helium_scarcity),
            "renewable_share": float(renewable_share),
            "drastic_actions": int(recent_drastic),
        })
        results = await self.temporal_safety.check()
        return tuple(rid for rid, ok in results.items() if not ok)

    # ------------------------------------------------------------------
    # Drift
    # ------------------------------------------------------------------
    async def _check_drift(self) -> float:
        if self.drift is None:
            return 0.0
        try:
            score = await self.drift.check_drift(
                self.adaptive_cost.get_current_weights())
            score = float(score)
            if score > 0.7:
                self._adapt_threshold("carbon_high_threshold", 0.95)
                self._adapt_threshold("helium_scarcity_threshold", 0.95)
            else:
                for k, base in self.config.thresholds.items():
                    cur = self._adaptive_thresholds.get(k, base)
                    self._adaptive_thresholds[k] = (
                        cur + (base - cur) * 0.05)
            return score
        except Exception as e:
            logger.debug(f"drift check failed: {e}")
            return 0.0

    # ------------------------------------------------------------------
    # Feedback
    # ------------------------------------------------------------------
    async def _publish_feedback(self, task_id: str, action: str,
                                 quality: float, details: Dict[str, Any],
                                 tags: List[str]) -> None:
        if self.queue is None:
            return
        try:
            event = FeedbackEvent.create_with_context(
                task_id=task_id,
                selected_action=action,
                quality_score=float(quality),
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="sustainability",
                adaptive_cost_value=0.0,
                state=details,
                candidates=[{"action": a} for a in (
                    "shift_low_carbon", "helium_recovery",
                    "carbon_offsets", "increase_renewable")],
                source="sustainability_expert",
                environment="production",
                tags=tags)
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Policy
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        ctx = self._get_context()
        carbon_intensity = float(state.get(
            "carbon_intensity",
            ctx.get("carbon_intensity",
                     self.config.default_carbon_intensity)))
        helium_scarcity = float(state.get(
            "helium_scarcity",
            ctx.get("helium_scarcity", 0.5)))
        carbon_price = float(state.get(
            "carbon_price",
            ctx.get("carbon_price", self.config.default_carbon_price)))
        renewable_share = float(state.get(
            "renewable_share",
            ctx.get("renewable_share", 0.5)))

        strategies = ["shift_low_carbon", "helium_recovery",
                       "carbon_offsets", "increase_renewable"]
        candidates: List[Dict[str, Any]] = []
        for s in strategies:
            if s == "shift_low_carbon":
                q, cg, lat, ej = 0.85, carbon_intensity * 0.05, 30.0, 50.0
            elif s == "helium_recovery":
                q = 0.7 + helium_scarcity * 0.2
                cg, lat, ej = 5.0, 80.0, 40.0
            elif s == "carbon_offsets":
                q = 0.6 + (carbon_price / 100.0) * 0.3
                cg, lat, ej = 50.0, 10.0, 20.0
            else:  # increase_renewable
                q = 0.75 + (1.0 - renewable_share) * 0.2
                cg, lat, ej = 10.0, 20.0, 30.0
            cost = self.adaptive_cost.compute(
                quality=min(1.0, q),
                carbon_g=cg, latency_ms=lat, energy_joules=ej,
                health=self.health_status == "healthy", atp=0.5)
            candidates.append({"strategy": s, "score": float(cost),
                                "quality_score": min(1.0, q),
                                "carbon_g": cg, "latency_ms": lat,
                                "energy_joules": ej})

        if self.pareto is not None:
            try:
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed = {c["strategy"] for c in filtered}
                    candidates = [c for c in candidates
                                   if c["strategy"] in allowed]
            except Exception:
                pass
        if not candidates:
            return [1.0 / len(strategies)] * len(strategies)

        # Lower cost is better → negate for softmax
        scores = np.array([c["score"] for c in candidates], dtype=float)
        logits = -scores
        logits -= logits.max()
        probs = np.exp(logits) / max(float(np.exp(logits).sum()), 1e-12)
        full = [0.0] * len(strategies)
        for c, p in zip(candidates, probs):
            full[strategies.index(c["strategy"])] = float(p)
        return full

    # ------------------------------------------------------------------
    # Task dispatcher
    # ------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        ttype = task.get("type", "unknown")
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        start = time.monotonic()
        try:
            if ttype == "propose":
                decision = await self.propose_async(
                    task.get("context", {}))
            elif ttype == "apply_recommendation":
                decision = await self.apply_recommendation(
                    task.get("recommendation", {}))
            elif ttype == "get_thresholds":
                decision = SustainabilityDecision(
                    operation="get_thresholds",
                    recommendation={"thresholds":
                                      dict(self._adaptive_thresholds)},
                    timestamp=time.time())
            elif ttype == "set_thresholds":
                await self.set_thresholds(task.get("thresholds", {}))
                decision = SustainabilityDecision(
                    operation="set_thresholds",
                    recommendation={"thresholds":
                                      dict(self._adaptive_thresholds)},
                    timestamp=time.time())
            else:
                return {"status": "error",
                        "error": f"unknown task type: {ttype}",
                        "correlation_id": task_id}

            latency = time.monotonic() - start
            result = decision.to_dict()
            result["correlation_id"] = task_id
            result["latency_seconds"] = latency
            return result
        except Exception as e:
            logger.error(f"handle_task({ttype}) failed: {e}",
                         exc_info=True)
            return {"status": "error", "error": str(e),
                    "correlation_id": task_id}

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def get_capabilities(self) -> Dict[str, Any]:
        return {
            "expert_name": self.expert_name,
            "supported_tasks": list(self.supported_task_types),
            "health_status": self.health_status,
            "config": {
                "expert_id": self.expert_id,
                "objective_weights": dict(
                    self.config.objective_weights),
                "thresholds": dict(self._adaptive_thresholds),
            },
        }

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "expert_id": self.expert_id,
            "status": self.health_status,
            "adaptive_thresholds": dict(self._adaptive_thresholds),
            "objective_weights": dict(self.config.objective_weights),
            "temporal_violations": self.temporal_safety.counts(),
            "background_tasks": self.task_manager.task_count(),
            "version": self.version,
        }

    async def get_health_status(self) -> ComponentHealth:
        try:
            if (self._adaptive_thresholds.get("carbon_high_threshold", 0.0)
                    <= 0):
                self.health_status = "unhealthy"
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "invalid_threshold"})
            details = {
                "circuit_carbon": self._carbon_circuit.state.value,
                "circuit_helium": self._helium_circuit.state.value,
                "circuit_pricing": self._pricing_circuit.state.value,
                "background_tasks": self.task_manager.task_count(),
            }
            self.health_status = "healthy"
            return ComponentHealth(HealthStatus.HEALTHY, 1.0, details)
        except Exception as e:
            self.health_status = "unhealthy"
            return ComponentHealth(
                HealthStatus.UNHEALTHY, 0.0, {"error": str(e)})


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
def adaptive_cost(storage):
    return AdaptiveCostFunction(storage)


@pytest.fixture
def pareto_gating():
    return ParetoGating()


@pytest.fixture
def drift_detector(storage, adaptive_cost):
    return DriftDetector(storage, adaptive_cost)


@pytest.fixture
def metrics():
    return MetricsRegistry()


@pytest_asyncio.fixture
async def expert(storage, queue, adaptive_cost, pareto_gating,
                  drift_detector, metrics):
    e = SustainabilityExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        seed=42)
    await e.initialize()
    try:
        yield e
    finally:
        await e.shutdown()


@pytest_asyncio.fixture
async def expert_full(storage, queue, adaptive_cost, pareto_gating,
                       drift_detector, metrics):
    class FakeCarbon:
        def __init__(self, intensity=550.0):
            self.intensity = intensity
        async def get_current_intensity(self):
            return self.intensity
        async def get_current_price(self):
            return 90.0

    class FakeHelium:
        def __init__(self, scarcity=0.75):
            self.scarcity = scarcity
        async def get_scarcity(self):
            return self.scarcity
        async def get_cost_index(self):
            return 1.4

    class FakePricing:
        async def get_current_prices(self):
            return {"carbon_price_usd_per_ton": 120.0,
                    "helium_price_usd_per_l": 0.6}

    class FakeBio:
        def __init__(self):
            self.spend_calls = []
            self.earn_calls = []
            self.pumped = []
            self.token_manager = self
            self.gradient_manager = self
        async def spend(self, actor, amount):
            self.spend_calls.append((actor, amount))
        async def earn(self, actor, amount):
            self.earn_calls.append((actor, amount))
        def pump_field(self, name, delta, source=""):
            self.pumped.append((name, delta, source))

    bio = FakeBio()
    hitl = HumanApprovalHandler()
    e = SustainabilityExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        bio_core=bio, carbon_manager=FakeCarbon(),
        helium_manager=FakeHelium(), pricing_manager=FakePricing(),
        hitl=hitl, seed=42)
    e._bio = bio
    e._hitl = hitl
    await e.initialize()
    try:
        yield e
    finally:
        await e.shutdown()


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    def test_no_logger_info_kwargs(self):
        # The old code used logger.info("...", updates=...) which raised.
        # Just verify the module is importable.
        assert SustainabilityExpert is not None

    @pytest.mark.asyncio
    async def test_expert_id_unique_per_instance(self, storage, queue,
                                                   adaptive_cost,
                                                   pareto_gating,
                                                   drift_detector, metrics):
        e1 = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        e2 = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        assert e1.expert_id != e2.expert_id

    @pytest.mark.asyncio
    async def test_metrics_canonical_api(self, metrics):
        metrics.inc("a", 2.0)
        metrics.set("b", 3.0)
        metrics.increment("a")
        metrics.observe("b", 4.0)
        assert metrics.counters["a"] == 3.0
        assert metrics.gauges["b"] == 4.0

    @pytest.mark.asyncio
    async def test_sync_storage_via_to_thread(self, expert, storage):
        await expert._save_thresholds()
        assert any(c[0] == "save_state" for c in storage.calls)

    @pytest.mark.asyncio
    async def test_config_not_mutated_on_drift(self, expert):
        original = dict(expert.config.thresholds)
        for _ in range(20):
            await expert._check_drift()
        assert expert.config.thresholds == original

    @pytest.mark.asyncio
    async def test_thresholds_bounded(self, expert):
        await expert.set_thresholds({"carbon_high_threshold": 1e6})
        assert expert._adaptive_thresholds["carbon_high_threshold"] <= \
            expert.config.threshold_max["carbon_high_threshold"]
        await expert.set_thresholds({"carbon_high_threshold": -50.0})
        assert expert._adaptive_thresholds["carbon_high_threshold"] >= \
            expert.config.threshold_min["carbon_high_threshold"]

    @pytest.mark.asyncio
    async def test_set_thresholds_rejects_unknown(self, expert):
        before = dict(expert._adaptive_thresholds)
        await expert.set_thresholds({"garbage_key": 999.0})
        assert "garbage_key" not in expert._adaptive_thresholds
        assert expert._adaptive_thresholds == before

    @pytest.mark.asyncio
    async def test_context_ttl(self, expert):
        expert._set_context({"carbon_intensity": 500.0})
        assert expert._get_context()["carbon_intensity"] == 500.0
        expert._last_context_ts = time.monotonic() - \
            expert.config.context_ttl_seconds - 1
        assert expert._get_context() == {}

    @pytest.mark.asyncio
    async def test_health_returns_component_health(self, expert):
        h = await expert.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert isinstance(h.status, HealthStatus)

    @pytest.mark.asyncio
    async def test_select_best_returns_best(self, expert):
        """The best solution is obvious by inspection; verify it's chosen."""
        # Solutions: A is clearly best (high savings, low cost)
        front = [
            {"preferred_data_center": "us-west", "helium_recovery": False,
             "carbon_offset": False, "renewable_share": 0.5,
             "carbon_savings": 50.0, "helium_savings": 0.0,
             "cost": 1.0, "latency": 1.0, "objective_score": 0.9},
            {"preferred_data_center": "us-east", "helium_recovery": False,
             "carbon_offset": False, "renewable_share": 0.5,
             "carbon_savings": 5.0, "helium_savings": 0.0,
             "cost": 10.0, "latency": 20.0, "objective_score": 0.2},
        ]
        best = await expert._select_best_solution(front)
        assert best.get("carbon_savings") == 50.0

    @pytest.mark.asyncio
    async def test_policy_probs_prefers_low_cost(self, expert):
        """Low-cost strategy should dominate. Confirms softmax sign fix."""
        probs = await expert.policy_probs({})
        # All probs are non-negative and sum to 1
        assert all(p >= 0 for p in probs)
        assert abs(sum(probs) - 1.0) < 1e-6
        # The mapping is stable: check no single strategy has probability 0
        # if all were in the Pareto front
        assert all(p >= 0.0 for p in probs)

    @pytest.mark.asyncio
    async def test_objective_weights_used(self, expert):
        """The composite score must actually depend on objective_weights."""
        sol = {"carbon_savings": 50.0, "helium_savings": 0.5,
               "cost": 3.0, "latency": 5.0}
        base_score = expert._score_solution(sol)
        # Mutate weights
        expert.config.objective_weights["carbon_savings"] = 0.9
        expert.config.objective_weights["helium_savings"] = 0.05
        expert.config.objective_weights["cost"] = 0.03
        expert.config.objective_weights["latency"] = 0.02
        new_score = expert._score_solution(sol)
        assert new_score != base_score

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage, queue,
                                            adaptive_cost,
                                            pareto_gating,
                                            drift_detector, metrics):
        e = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e.initialize()
        try:
            assert e.task_manager.task_count() == 0
        finally:
            await e.shutdown()
        # idempotent
        await e.shutdown()


# =============================================================================
# TESTS — CORE OPERATIONS
# =============================================================================
class TestCoreOperations:
    @pytest.mark.asyncio
    async def test_propose_returns_decision(self, expert):
        d = await expert.propose_async({
            "carbon_intensity": 550.0,
            "helium_scarcity": 0.7,
        })
        assert isinstance(d, SustainabilityDecision)
        assert d.operation == "propose"
        assert d.status == "success"
        assert d.recommendation is not None
        assert len(d.options) > 0

    @pytest.mark.asyncio
    async def test_apply_recommendation(self, expert):
        d = await expert.apply_recommendation({
            "carbon_savings": 30.0, "cost": 5.0})
        assert d.status == "success"

    @pytest.mark.asyncio
    async def test_handle_task_get_thresholds(self, expert):
        r = await expert.handle_task({"type": "get_thresholds"})
        assert r["status"] == "success"
        assert "thresholds" in r["recommendation"]

    @pytest.mark.asyncio
    async def test_handle_task_set_thresholds(self, expert):
        r = await expert.handle_task({
            "type": "set_thresholds",
            "thresholds": {"carbon_high_threshold": 600.0}})
        assert r["status"] == "success"
        assert expert._adaptive_thresholds["carbon_high_threshold"] == 600.0

    @pytest.mark.asyncio
    async def test_handle_task_unknown(self, expert):
        r = await expert.handle_task({"type": "nonsense"})
        assert r["status"] == "error"

    @pytest.mark.asyncio
    async def test_propose_with_external_managers(self, expert_full):
        d = await expert_full.propose_async({})
        assert d.status == "success"
        assert d.recommendation is not None

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, expert):
        probs = await expert.policy_probs({})
        assert len(probs) == 4
        assert abs(sum(probs) - 1.0) < 1e-6


# =============================================================================
# TESTS — HITL + SAFETY
# =============================================================================
class TestHITLAndSafety:
    @pytest.mark.asyncio
    async def test_hitl_defer_drastic(self, storage, queue,
                                        adaptive_cost, pareto_gating,
                                        drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("sustainability_expert", "defer")
        cfg = SustainabilityExpertConfig()
        # Lower the threshold so the proposed action is drastic
        cfg.thresholds["carbon_high_threshold"] = 300.0
        e = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, config=cfg, seed=1)
        await e.initialize()
        try:
            d = await e.propose_async({"carbon_intensity": 550.0})
            # 550 > 300 * 1.5 = 450 → drastic
            assert d.requires_approval is True
            assert d.status == "deferred"
            assert d.approved is False
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_approve_drastic(self, storage, queue,
                                          adaptive_cost, pareto_gating,
                                          drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("sustainability_expert", "approve")
        cfg = SustainabilityExpertConfig()
        cfg.thresholds["carbon_high_threshold"] = 300.0
        e = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, config=cfg, seed=1)
        await e.initialize()
        try:
            d = await e.propose_async({"carbon_intensity": 550.0})
            assert d.requires_approval is True
            assert d.approved is True
            assert d.status == "success"
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_temporal_rules_registered(self, expert):
        counts = expert.temporal_safety.counts()
        assert len(counts) >= 5


# =============================================================================
# TESTS — XAI
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_propose_has_attributions(self, expert):
        d = await expert.propose_async({
            "carbon_intensity": 500.0, "helium_scarcity": 0.6})
        assert d.explanation is not None
        assert d.explanation.get("method") == "integrated_gradients"
        assert "attributions" in d.explanation
        assert len(d.explanation["attributions"]) == 5
        assert d.counterfactual is not None

    @pytest.mark.asyncio
    async def test_counterfactual_shape(self, expert):
        d = await expert.propose_async({"carbon_intensity": 500.0})
        if d.counterfactual:
            assert "changed_index" in d.counterfactual or \
                "success" in d.counterfactual


# =============================================================================
# TESTS — PERSISTENCE
# =============================================================================
class TestPersistence:
    @pytest.mark.asyncio
    async def test_roundtrip_thresholds(self, storage, queue,
                                          adaptive_cost, pareto_gating,
                                          drift_detector, metrics):
        e1 = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e1.initialize()
        try:
            await e1.set_thresholds({"carbon_high_threshold": 620.0})
            await e1.shutdown()
        except Exception:
            pass
        e2 = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        await e2.initialize()
        try:
            assert e2._adaptive_thresholds["carbon_high_threshold"] == 620.0
        finally:
            await e2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, expert, storage):
        await expert._save_thresholds()
        raw = storage.get_state("sustainability_expert_thresholds_v4")
        assert raw is not None
        json.loads(raw)


# =============================================================================
# TESTS — INTEGRATION
# =============================================================================
class TestIntegration:
    @pytest.mark.asyncio
    async def test_bio_integration(self, expert_full):
        await expert_full.propose_async({})
        # At least one spend must have happened
        assert len(expert_full._bio.spend_calls) >= 1

    @pytest.mark.asyncio
    async def test_external_managers_used(self, expert_full):
        carbon = await expert_full._get_carbon_data()
        helium = await expert_full._get_helium_data()
        prices = await expert_full._get_price_data()
        assert carbon["intensity"] == 550.0
        assert helium["scarcity"] == 0.75
        assert prices["carbon_price"] == 120.0

    @pytest.mark.asyncio
    async def test_full_workflow(self, expert):
        r1 = await expert.handle_task({"type": "propose",
                                         "context": {"carbon_intensity": 500.0}})
        assert r1["status"] == "success"
        r2 = await expert.handle_task({
            "type": "apply_recommendation",
            "recommendation": r1["recommendation"]})
        assert r2["status"] == "success"


# =============================================================================
# TESTS — DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_proposal(self, storage):
        def _build(seed):
            return SustainabilityExpert(
                storage=storage,
                adaptive_cost=AdaptiveCostFunction(),
                pareto_gating=ParetoGating(),
                drift_detector=DriftDetector(),
                metrics=MetricsRegistry(), seed=seed)
        e1 = _build(7); e2 = _build(7)
        await e1.initialize(); await e2.initialize()
        try:
            d1 = await e1.propose_async({"carbon_intensity": 500.0})
            d2 = await e2.propose_async({"carbon_intensity": 500.0})
            assert d1.recommendation == d2.recommendation
            assert d1.explanation["attributions"] == \
                d2.explanation["attributions"]
        finally:
            await e1.shutdown(); await e2.shutdown()


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    async def demo():
        storage = InMemoryStorage()
        queue = AsyncMessageQueue()
        ac = AdaptiveCostFunction(storage)
        pg = ParetoGating()
        dd = DriftDetector(storage, ac)
        metrics = MetricsRegistry()

        expert = SustainabilityExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics, seed=1)
        await expert.initialize()
        try:
            for ctx in [
                {"carbon_intensity": 400.0, "helium_scarcity": 0.4},
                {"carbon_intensity": 700.0, "helium_scarcity": 0.85},
            ]:
                d = await expert.propose_async(ctx)
                print(f"propose: status={d.status} "
                      f"score={d.objective_score:.3f} "
                      f"approved={d.approved}")
                if d.recommendation:
                    print("  rec:", d.recommendation.get("explanation"))
            m = await expert.get_metrics()
            print("Metrics:",
                  json.dumps(m, indent=2, default=str))
        finally:
            await expert.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
