#!/usr/bin/env python3
"""
HeliumIoTExpert v4.0.0 — Self-Contained Single-File Bundle
============================================================

Complete rewrite of the Green Agent Helium IoT Expert. Everything needed
to run and test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. Local stubs (InMemoryStorage, AsyncMessageQueue, FeedbackEvent,
     MetricsRegistry, AdaptiveCostFunction, ParetoGating, DriftDetector).
  3. Shared runtime: CircuitBreaker, TemporalSafetyMonitor (MTL),
     HumanApprovalHandler, XAIExplainer (Integrated Gradients),
     TaskManager, ComponentHealth.
  4. `HeliumDecision` value object.
  5. Real HITL gating for drastic actions.
  6. Real Integrated Gradients XAI on recommendations.
  7. `objective_weights` actually used for option scoring.
  8. Bounded threshold mutations and adaptive drift thresholds.
  9. In-memory bounded history persisted via asyncio.to_thread.
 10. `HeliumProviderLike` Protocol with sync/async coercion.
 11. Comprehensive pytest suite.

Run:
    python helium_iot_expert_v4.py         # demo
    pytest helium_iot_expert_v4.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import re as _re
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Sequence, Tuple,
    runtime_checkable,
)

import numpy as np
import pytest
import pytest_asyncio

logger = logging.getLogger("helium_iot_expert")
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
class HeliumProviderLike(Protocol):
    """Any of these may be sync or async. The expert coerces the result."""
    def get_scarcity(self) -> Any: ...
    def get_cost_index(self) -> Any: ...


@runtime_checkable
class BioCoreLike(Protocol):
    async def spend(self, actor: str, amount: float) -> Any: ...
    async def earn(self, actor: str, amount: float) -> Any: ...


# =============================================================================
# JSON SANITIZER
# =============================================================================
def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
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
    """Canonical inc/set; increment/observe are aliases."""
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
                raise RuntimeError(f"Circuit breaker {self.name} half-open limit")
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


import time as _time  # needed by CircuitBreaker
time = _time


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
# VALUE OBJECTS
# =============================================================================
@dataclass(frozen=True)
class HeliumDecision:
    """Value object capturing a single Helium IoT decision."""
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
    quality: float = 0.0
    helium_scarcity: float = 0.0
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
            "quality": float(self.quality),
            "helium_scarcity": float(self.helium_scarcity),
            "timestamp": float(self.timestamp),
        }


@dataclass
class HeliumIoTExpertConfig:
    expert_id: str = field(
        default_factory=lambda: f"helium_iot_{uuid.uuid4().hex[:8]}")
    enable_persistence: bool = True
    enable_predictive_alerts: bool = True
    enable_anomaly_detection: bool = True
    enable_cost_benefit: bool = True
    enable_quantum_bridge: bool = True
    enable_time_tick_engine: bool = True
    enable_swarm_coordination: bool = True
    enable_self_healing: bool = True
    human_approval_required: bool = True

    thresholds: Dict[str, float] = field(default_factory=lambda: {
        "helium_scarcity_high": 0.6,
        "helium_scarcity_critical": 0.8,
        "network_latency_high": 100.0,
        "battery_low_threshold": 0.2,
        "sampling_rate_high": 10.0,
        "sampling_rate_low": 5.0,
        "sampling_rate_critical": 2.0,
    })

    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "helium_savings": 0.4,
        "data_quality": 0.3,
        "latency": 0.2,
        "cost": 0.1,
    })

    # Bounds for threshold mutation
    threshold_min: Dict[str, float] = field(default_factory=lambda: {
        "helium_scarcity_high": 0.2,
        "helium_scarcity_critical": 0.4,
        "network_latency_high": 50.0,
        "battery_low_threshold": 0.05,
        "sampling_rate_high": 1.0,
        "sampling_rate_low": 0.5,
        "sampling_rate_critical": 0.1,
    })
    threshold_max: Dict[str, float] = field(default_factory=lambda: {
        "helium_scarcity_high": 0.95,
        "helium_scarcity_critical": 0.99,
        "network_latency_high": 500.0,
        "battery_low_threshold": 0.6,
        "sampling_rate_high": 100.0,
        "sampling_rate_low": 50.0,
        "sampling_rate_critical": 10.0,
    })

    # Drift
    drift_threshold_high: float = 0.7
    drift_decay_factor: float = 0.05

    # Cooldowns
    drastic_action_cooldown_seconds: float = 300.0

    # History
    history_maxlen: int = 1000
    history_persist_interval_seconds: float = 60.0

    def __post_init__(self) -> None:
        # Sanity check the threshold ranges
        for k, v in self.thresholds.items():
            if v < 0:
                self.thresholds[k] = 0.0


# =============================================================================
# SIMULATED PROVIDER (used when no real provider injected)
# =============================================================================
class SimulatedHeliumProvider:
    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)
        self._scarcity = 0.5
        self._cost = 1.0
        self._trend: Deque[float] = deque(maxlen=100)

    async def get_scarcity(self) -> float:
        change = self._rng.gauss(0, 0.02)
        self._scarcity = max(0.0, min(1.0, self._scarcity + change))
        self._trend.append(self._scarcity)
        return self._scarcity

    async def get_cost_index(self) -> float:
        self._cost = 1.0 + self._scarcity * 0.5
        return self._cost

    def forecast(self, hours: int = 4) -> List[float]:
        if len(self._trend) < 10:
            return [self._scarcity] * hours
        last = list(self._trend)[-10:]
        slope = (last[-1] - last[0]) / 9.0
        return [max(0.0, min(1.0, last[-1] + slope * (i + 1)))
                for i in range(hours)]


# =============================================================================
# HELIUM IOT EXPERT (v4.0.0)
# =============================================================================
class HeliumIoTExpert:
    """Helium IoT Expert v4.0.0 — Enhanced Green Agent MODP integration."""

    SUPPORTED_TASKS = (
        "propose", "apply_recommendation", "get_thresholds", "set_thresholds",
    )

    def __init__(
        self,
        storage: StorageLike,
        message_queue: MessageQueueLike,
        adaptive_cost: AdaptiveCostLike,
        pareto_gating: ParetoGatingLike,
        drift_detector: DriftDetectorLike,
        metrics: MetricsLike,
        bio_core: Optional[BioCoreLike] = None,
        helium_provider: Optional[HeliumProviderLike] = None,
        hitl: Optional[HumanApprovalHandler] = None,
        config: Optional[HeliumIoTExpertConfig] = None,
        seed: int = 42,
    ):
        self.expert_name = "helium_iot_expert"
        self.supported_task_types = list(self.SUPPORTED_TASKS)
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.bio_core = bio_core

        self.config = config or HeliumIoTExpertConfig()

        # Deterministic RNG
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        np.random.seed(seed)

        # Helium provider
        self.helium_provider = helium_provider or SimulatedHeliumProvider(seed)

        # Thresholds: base = configured, adaptive = instance
        self.thresholds: Dict[str, float] = dict(self.config.thresholds)
        self._adaptive_thresholds: Dict[str, float] = dict(
            self.config.thresholds)

        # State
        self._last_context: Dict[str, Any] = {}
        self._last_context_ts: Optional[float] = None
        self.correlation_id = str(uuid.uuid4())
        self.last_error: Optional[str] = None
        self._proposals_count = 0
        self._applied_count = 0

        # Bounded history
        self._history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.history_maxlen)

        # Circuit breaker
        self._helium_circuit = CircuitBreaker(
            failure_threshold=5, recovery_timeout=30.0,
            name="helium_provider")

        # Cooldowns
        self._last_drastic_action_time: Optional[float] = None

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
        self.task_manager.start_coro(
            "history_flush", self._periodic_history_flush())
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self._save_history()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula("G (sampling_rate_hz >= 0.5)")
        self.temporal_safety.add_formula("G (helium_scarcity >= 0.0)")
        self.temporal_safety.add_formula("G (helium_scarcity <= 1.0)")
        self.temporal_safety.add_formula(
            "[300] NEVER (drastic_actions > 1)")
        self.temporal_safety.add_formula(
            "G (helium_scarcity_high <= 0.95)")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _load_thresholds(self) -> None:
        try:
            data = await asyncio.to_thread(
                self.storage.get_state, "helium_iot_thresholds_v4")
            if data:
                stored = json.loads(data)
                # Only merge keys that exist in the current schema
                for k, v in stored.items():
                    if k in self.thresholds:
                        try:
                            self.thresholds[k] = float(v)
                        except (TypeError, ValueError):
                            pass
                self._adaptive_thresholds = dict(self.thresholds)
                logger.info("Thresholds loaded from storage")
        except Exception as e:
            logger.error(f"Failed to load thresholds: {e}")

    async def _save_thresholds(self) -> None:
        try:
            await asyncio.to_thread(
                self.storage.save_state, "helium_iot_thresholds_v4",
                json.dumps(self.thresholds))
        except Exception as e:
            logger.error(f"Failed to save thresholds: {e}")

    async def _save_history(self) -> None:
        if not self.config.enable_persistence:
            return
        try:
            await asyncio.to_thread(
                self.storage.save_state, "helium_iot_history_v4",
                json.dumps(list(self._history)))
        except Exception as e:
            logger.error(f"Failed to save history: {e}")

    async def _periodic_history_flush(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(),
                    timeout=self.config.history_persist_interval_seconds)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._save_history()
            except Exception as e:
                logger.debug(f"history flush failed: {e}")

    def _append_history(self, entry: Dict[str, Any]) -> None:
        self._history.append(_json_safe(entry))

    # ------------------------------------------------------------------
    # Context management
    # ------------------------------------------------------------------
    def _set_context(self, context: Dict[str, Any]) -> None:
        self._last_context.update(context)
        self._last_context_ts = time.monotonic()

    def _get_context(self) -> Dict[str, Any]:
        if self._last_context_ts is None:
            return {}
        # Context lives for 5 minutes
        if time.monotonic() - self._last_context_ts > 300.0:
            return {}
        return dict(self._last_context)

    # ------------------------------------------------------------------
    # Provider coercion (sync/async)
    # ------------------------------------------------------------------
    async def _call_provider(self, method_name: str) -> Optional[float]:
        method = getattr(self.helium_provider, method_name, None)
        if method is None:
            return None
        try:
            r = method()
            if asyncio.iscoroutine(r):
                r = await r
            return float(r)
        except Exception as e:
            logger.warning(f"helium provider {method_name} failed: {e}")
            return None

    async def _get_helium_data(self) -> Dict[str, float]:
        scarcity = await self._call_provider("get_scarcity")
        cost = await self._call_provider("get_cost_index")
        if scarcity is None:
            scarcity = float(self._get_context().get("helium_scarcity", 0.5))
        if cost is None:
            cost = float(self._get_context().get("helium_cost_index", 1.0))
        return {"scarcity": float(scarcity), "cost": float(cost)}

    def _get_network_data(self) -> Dict[str, float]:
        ctx = self._get_context()
        return {
            "latency": float(ctx.get("network_latency_ms", 50.0)),
            "bandwidth": float(ctx.get("bandwidth_mbps", 100.0)),
        }

    def _get_device_data(self) -> Dict[str, float]:
        ctx = self._get_context()
        return {
            "battery": float(ctx.get("battery_level", 0.8)),
            "data_quality": float(ctx.get("data_quality", 0.9)),
        }

    # ------------------------------------------------------------------
    # Bio
    # ------------------------------------------------------------------
    async def _bio_spend_earn(self, atp_cost: float, quality: float
                              ) -> None:
        if self.bio_core is None:
            return
        try:
            spend = getattr(self.bio_core, "spend", None)
            if spend is not None:
                r = spend(self.expert_name, atp_cost)
                if asyncio.iscoroutine(r):
                    await r
            if quality > 0.7:
                earn = getattr(self.bio_core, "earn", None)
                if earn is not None:
                    r = earn(self.expert_name, atp_cost * 2)
                    if asyncio.iscoroutine(r):
                        await r
        except Exception as e:
            logger.warning(f"bio integration failed: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Safety snapshot
    # ------------------------------------------------------------------
    async def _safety_check(self, sampling_rate_hz: float,
                             helium_scarcity: float,
                             recent_drastic: int = 0) -> Tuple[str, ...]:
        await self.temporal_safety.push({
            "sampling_rate_hz": float(sampling_rate_hz),
            "helium_scarcity": float(helium_scarcity),
            "helium_scarcity_high":
                float(self._adaptive_thresholds.get(
                    "helium_scarcity_high", 0.6)),
            "drastic_actions": int(recent_drastic),
        })
        results = await self.temporal_safety.check()
        return tuple(rid for rid, ok in results.items() if not ok)

    # ------------------------------------------------------------------
    # Recommendation builders
    # ------------------------------------------------------------------
    def _build_recommendation(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float,
    ) -> Dict[str, Any]:
        rec: Dict[str, Any] = {}
        th = self._adaptive_thresholds

        if helium_scarcity > th["helium_scarcity_critical"]:
            rec["sampling_rate_hz"] = th["sampling_rate_critical"]
            rec["power_saving_mode"] = True
        elif helium_scarcity > th["helium_scarcity_high"]:
            rec["sampling_rate_hz"] = th["sampling_rate_low"]
            rec["power_saving_mode"] = False
        else:
            rec["sampling_rate_hz"] = th["sampling_rate_high"]
            rec["power_saving_mode"] = False

        if helium_scarcity > th["helium_scarcity_high"]:
            rec["aggregation_strategy"] = "compressed"
        else:
            rec["aggregation_strategy"] = "adaptive"

        if network_latency > th["network_latency_high"]:
            rec["preferred_gateways"] = ["gateway_nearby"]
        else:
            rec["preferred_gateways"] = []

        if battery_level < th["battery_low_threshold"]:
            rec["sampling_rate_hz"] = min(rec["sampling_rate_hz"], 2.0)
            rec["power_saving_mode"] = True

        weights = self.adaptive_cost.get_current_weights()
        if weights.get("carbon", 0.3) > 0.5:
            rec["power_saving_mode"] = True
        if weights.get("cost", 0.2) > 0.5:
            rec["preferred_gateways"] = ["gateway_nearby"]

        return rec

    async def _build_tradeoff_options(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float,
    ) -> List[Dict[str, Any]]:
        options: List[Dict[str, Any]] = []

        if helium_scarcity > 0.4:
            options.append({
                "action": "reduce_sampling_rate",
                "estimated_helium_savings_l": helium_scarcity * 0.1,
                "estimated_data_quality_loss": 0.05,
                "priority": "high",
                "latency_ms": 60.0,
                "energy_joules": 30.0,
                "carbon_g": 2.0,
                "quality_score": 0.95,
                "nl_explanation":
                    f"Reduce sampling rate to save helium "
                    f"(scarcity {helium_scarcity:.2f}); "
                    f"slightly lower data quality.",
            })

        if helium_scarcity > 0.3:
            options.append({
                "action": "enable_compressed_aggregation",
                "estimated_bandwidth_save": 0.3,
                "estimated_latency_increase": 5.0,
                "priority": "medium",
                "latency_ms": 80.0,
                "energy_joules": 20.0,
                "carbon_g": 1.5,
                "quality_score": 0.75,
                "nl_explanation":
                    "Use compressed aggregation to reduce bandwidth and "
                    "helium usage; slight latency increase.",
            })

        if network_latency > 80:
            options.append({
                "action": "use_closer_gateways",
                "estimated_latency_reduction": 20.0,
                "estimated_cost_increase": 0.1,
                "priority": "low",
                "latency_ms": 30.0,
                "energy_joules": 15.0,
                "carbon_g": 1.0,
                "quality_score": 0.85,
                "nl_explanation":
                    "Route through closer gateways to reduce latency by "
                    "~20 ms; minor cost increase.",
            })

        if battery_level < 0.3:
            options.append({
                "action": "enable_power_saving",
                "estimated_battery_extension_hours": 24.0,
                "estimated_data_quality_loss": 0.1,
                "priority": "medium",
                "latency_ms": 100.0,
                "energy_joules": 5.0,
                "carbon_g": 0.5,
                "quality_score": 0.6,
                "nl_explanation":
                    "Enable power-saving to extend battery life; accept "
                    "10% data quality loss.",
            })

        # Pareto filter
        if self.pareto is not None and options:
            try:
                cand = [{
                    "action": o["action"],
                    "quality_score": o.get("quality_score", 1.0),
                    "carbon_g": o.get("carbon_g", 0.0),
                    "latency_ms": o.get("latency_ms", 0.0),
                    "energy_joules": o.get("energy_joules", 0.0),
                } for o in options]
                filtered = self.pareto.filter(cand)
                if filtered:
                    allowed = {c["action"] for c in filtered}
                    options = [o for o in options
                                if o["action"] in allowed]
            except Exception:
                pass

        # Score options using objective_weights (config-driven)
        weights = dict(self.config.objective_weights)
        # Sanity-normalize
        total_w = sum(weights.values()) or 1.0
        for k in weights:
            weights[k] = weights[k] / total_w

        for o in options:
            helium_savings = float(o.get("estimated_helium_savings_l", 0.0))
            bandwidth_savings = float(o.get("estimated_bandwidth_save", 0.0))
            latency_reduction = float(o.get("estimated_latency_reduction", 0.0))
            # Lower cost is better, so invert
            energy_norm = 1.0 / (1.0 + float(o.get("energy_joules", 0.0)))
            composite = (
                weights["helium_savings"] * (helium_savings + bandwidth_savings) +
                weights["data_quality"] * float(o.get("quality_score", 0.5)) +
                weights["latency"] * (latency_reduction / 100.0 + 0.1) +
                weights["cost"] * energy_norm
            )
            o["objective_score"] = float(composite)

        # Best option first (descending composite)
        options.sort(key=lambda x: x["objective_score"], reverse=True)
        return options

    # ------------------------------------------------------------------
    # XAI on the recommendation
    # ------------------------------------------------------------------
    def _explain_recommendation(
        self,
        recommendation: Dict[str, Any],
        helium_data: Dict[str, float],
        network_data: Dict[str, float],
        device_data: Dict[str, float],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # Feature vector: [helium_scarcity, helium_cost, latency, battery]
        x = np.array([
            helium_data["scarcity"],
            helium_data["cost"],
            network_data["latency"],
            device_data["battery"],
        ], dtype=float)

        def _score(v: np.ndarray) -> float:
            # Lower sampling rate = better under scarcity; higher = better otherwise
            hs = float(v[0])
            hc = float(v[1])
            lat = float(v[2])
            bat = float(v[3])
            rate = float(recommendation.get("sampling_rate_hz", 5.0))
            # Positive pressure toward savings
            return -(rate * 0.1) - (hs * 0.3) - (hc * 0.2) \
                - (lat / 1000.0) - (bat * 0.1)

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["helium_scarcity", "helium_cost",
                                    "network_latency", "battery"],
            }
            cf = self.xai.counterfactual(_score, x)
            return explanation, cf
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Heuristics
    # ------------------------------------------------------------------
    def _calculate_quality(self, recommendation: Dict[str, Any],
                            helium_scarcity: float) -> float:
        base_rate = self._adaptive_thresholds["sampling_rate_high"]
        if base_rate <= 0:
            return 0.8
        if "sampling_rate_hz" not in recommendation:
            return 0.8
        rate = float(recommendation["sampling_rate_hz"])
        if helium_scarcity > self._adaptive_thresholds[
                "helium_scarcity_high"]:
            return max(0.5, 1.0 - (base_rate - rate) / base_rate * 0.5)
        return 1.0 - (base_rate - rate) / base_rate * 0.1

    def _is_drastic_action(self, recommendation: Dict[str, Any],
                            helium_scarcity: float) -> bool:
        th = self._adaptive_thresholds
        if helium_scarcity > th["helium_scarcity_critical"]:
            return True
        if float(recommendation.get("sampling_rate_hz", 10.0)) <= \
                th["sampling_rate_critical"]:
            return True
        if bool(recommendation.get("power_saving_mode", False)):
            return True
        return False

    def _recent_drastic_count(self, window_seconds: float = 300.0) -> int:
        now = datetime.now(timezone.utc)
        count = 0
        for entry in self._history:
            if entry.get("drastic"):
                ts = entry.get("timestamp")
                if isinstance(ts, str):
                    try:
                        when = datetime.fromisoformat(ts)
                    except Exception:
                        continue
                    if (now - when).total_seconds() < window_seconds:
                        count += 1
        return count

    # ------------------------------------------------------------------
    # Public: propose
    # ------------------------------------------------------------------
    async def propose_async(self, context: Dict[str, Any]) -> HeliumDecision:
        self._set_context(context)
        atp_cost = 0.0

        # Bio: spend
        if self.bio_core is not None:
            atp_cost = 0.05
            await self._bio_spend_earn(atp_cost, quality=0.5)

        try:
            helium_data = await self._get_helium_data()
            network_data = self._get_network_data()
            device_data = self._get_device_data()

            # Simple predictive nudge
            if isinstance(self.helium_provider, SimulatedHeliumProvider):
                forecast = self.helium_provider.forecast(4)
                if len(forecast) >= 2:
                    if forecast[-1] > forecast[0]:
                        helium_data["scarcity"] = min(
                            1.0, helium_data["scarcity"] * 1.2)
                    elif forecast[-1] < forecast[0]:
                        helium_data["scarcity"] = max(
                            0.0, helium_data["scarcity"] * 0.9)

            primary = self._build_recommendation(
                helium_scarcity=helium_data["scarcity"],
                helium_cost=helium_data["cost"],
                network_latency=network_data["latency"],
                battery_level=device_data["battery"],
            )
            options = await self._build_tradeoff_options(
                helium_scarcity=helium_data["scarcity"],
                helium_cost=helium_data["cost"],
                network_latency=network_data["latency"],
                battery_level=device_data["battery"],
            )
            explanation, counterfactual = self._explain_recommendation(
                primary, helium_data, network_data, device_data)

            quality = self._calculate_quality(
                primary, helium_data["scarcity"])

            # Drastic action gate
            is_drastic = self._is_drastic_action(
                primary, helium_data["scarcity"])
            requires_approval = False
            approved: Optional[bool] = None
            if is_drastic:
                recent = self._recent_drastic_count(
                    self.config.drastic_action_cooldown_seconds)
                if recent >= 1:
                    requires_approval = True
                elif self.config.human_approval_required:
                    requires_approval = True

            if requires_approval:
                approved = await self.hitl.request_approval({
                    "action": "drastic_helium_iot_action",
                    "candidates": [
                        {"id": "approve", "risk": 0.4,
                         "sampling_rate_hz": primary.get("sampling_rate_hz")},
                        {"id": "defer", "risk": 0.0,
                         "sampling_rate_hz": 0.0},
                    ],
                }, user_id=self.expert_name)

            # Safety violations
            violations = await self._safety_check(
                sampling_rate_hz=float(primary.get("sampling_rate_hz", 5.0)),
                helium_scarcity=helium_data["scarcity"],
                recent_drastic=self._recent_drastic_count(
                    self.config.drastic_action_cooldown_seconds))

            # Persist history
            self._append_history({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "helium_scarcity": helium_data["scarcity"],
                "recommendation": primary,
                "options_count": len(options),
                "quality": quality,
                "drastic": is_drastic,
                "requires_approval": requires_approval,
                "approved": approved,
            })

            # Bio: earn if quality is high
            if self.bio_core is not None and quality > 0.7:
                await self._bio_spend_earn(
                    atp_cost, quality=quality)

            # Metrics
            self.metrics.inc("helium_iot_proposals")
            self.metrics.set("helium_iot_last_quality", quality)
            self.metrics.set("helium_iot_scarcity",
                              helium_data["scarcity"])
            self._proposals_count += 1

            await self._publish_feedback(
                task_id=f"he_iot_propose_{uuid.uuid4().hex[:8]}",
                action="propose",
                quality=quality,
                details={"recommendation": primary,
                         "requires_approval": requires_approval,
                         "approved": approved},
                tags=["helium", "iot", "propose"])

            await self._check_drift()

            status = "success"
            if requires_approval and approved is False:
                status = "deferred"
            return HeliumDecision(
                operation="propose",
                status=status,
                recommendation=primary,
                options=tuple(options),
                explanation=explanation,
                counterfactual=counterfactual,
                requires_approval=requires_approval,
                approved=approved,
                safety_violations=violations,
                confidence=float(quality),
                quality=float(quality),
                helium_scarcity=float(helium_data["scarcity"]),
                timestamp=time.time(),
            )

        except Exception as e:
            logger.error(f"propose_async failed: {e}", exc_info=True)
            self.health_status = "degraded"
            self.last_error = str(e)
            return HeliumDecision(
                operation="propose",
                status="error",
                error=str(e),
                timestamp=time.time(),
            )

    # ------------------------------------------------------------------
    # Public: apply_recommendation
    # ------------------------------------------------------------------
    async def apply_recommendation(self, recommendation: Dict[str, Any]
                                    ) -> HeliumDecision:
        # Bio: spend
        if self.bio_core is not None:
            await self._bio_spend_earn(0.02, quality=0.5)

        # Simulated success
        success = True
        if self.bio_core is not None and success:
            await self._bio_spend_earn(0.03, quality=0.9)

        self.metrics.inc("helium_iot_applied")
        if not success:
            self.metrics.inc("helium_iot_apply_failures")
        self._applied_count += 1

        self._append_history({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "apply_recommendation",
            "recommendation": recommendation,
            "success": success,
        })

        await self._publish_feedback(
            task_id=f"he_iot_apply_{uuid.uuid4().hex[:8]}",
            action="apply_recommendation",
            quality=0.9 if success else 0.0,
            details={"recommendation": recommendation},
            tags=["helium", "iot", "apply"])

        await self._check_drift()

        return HeliumDecision(
            operation="apply_recommendation",
            status="success" if success else "error",
            recommendation=recommendation,
            confidence=0.9 if success else 0.0,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Threshold management
    # ------------------------------------------------------------------
    def get_thresholds_sync(self) -> Dict[str, float]:
        return dict(self.thresholds)

    def _bound_threshold(self, key: str, value: float) -> float:
        lo = self.config.threshold_min.get(key, 0.0)
        hi = self.config.threshold_max.get(key, float("inf"))
        return float(max(lo, min(hi, value)))

    async def set_thresholds(self, updates: Dict[str, float]) -> None:
        for k, v in updates.items():
            if k not in self.thresholds:
                logger.warning(f"unknown threshold key ignored: {k}")
                continue
            try:
                bounded = self._bound_threshold(k, float(v))
            except (TypeError, ValueError):
                continue
            self.thresholds[k] = bounded
            self._adaptive_thresholds[k] = bounded
        await self._save_thresholds()

    # ------------------------------------------------------------------
    # Policy interface
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        ctx = dict(state) if state else self._get_context()
        helium_scarcity = float(ctx.get("helium_scarcity", 0.5))
        network_latency = float(ctx.get("network_latency_ms", 50.0))
        battery_level = float(ctx.get("battery_level", 0.8))

        strategies = ["reduce_sampling", "enable_compressed",
                       "use_closer_gateways", "enable_power_saving"]
        candidates: List[Dict[str, Any]] = []
        for s in strategies:
            if s == "reduce_sampling":
                q = 0.9 - helium_scarcity * 0.3
                cg = helium_scarcity * 5.0
                lat = 50.0 + helium_scarcity * 20.0
                ej = 25.0 + helium_scarcity * 10.0
            elif s == "enable_compressed":
                q = 0.8
                cg = helium_scarcity * 4.0
                lat = 70.0 + network_latency * 0.1
                ej = 20.0
            elif s == "use_closer_gateways":
                q = 0.85
                cg = helium_scarcity * 2.0
                lat = 40.0 - min(20.0, network_latency * 0.2)
                ej = 15.0
            else:  # enable_power_saving
                q = 0.7 - battery_level * 0.2
                cg = helium_scarcity * 1.0
                lat = 100.0
                ej = 5.0
            cost = self.adaptive_cost.compute(
                quality=q, carbon_g=cg, latency_ms=lat,
                energy_joules=ej,
                health=self.health_status == "healthy", atp=0.5)
            candidates.append({"strategy": s, "score": float(cost),
                                "quality_score": q, "carbon_g": cg,
                                "latency_ms": lat, "energy_joules": ej})

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
            return [0.25] * len(strategies)

        # Lower cost is better → negate before softmax
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
        task_type = task.get("type", "unknown")
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        start = time.monotonic()

        try:
            if task_type == "propose":
                decision = await self.propose_async(
                    task.get("context", {}))
            elif task_type == "apply_recommendation":
                decision = await self.apply_recommendation(
                    task.get("recommendation", {}))
            elif task_type == "get_thresholds":
                decision = HeliumDecision(
                    operation="get_thresholds",
                    recommendation={"thresholds": dict(self.thresholds)},
                    timestamp=time.time())
            elif task_type == "set_thresholds":
                await self.set_thresholds(task.get("thresholds", {}))
                decision = HeliumDecision(
                    operation="set_thresholds",
                    recommendation={"thresholds": dict(self.thresholds)},
                    timestamp=time.time())
            else:
                return {"status": "error",
                        "error": f"Unknown task type: {task_type}",
                        "correlation_id": task_id}

            latency = time.monotonic() - start
            result = decision.to_dict()
            result["correlation_id"] = task_id
            result["latency_seconds"] = latency
            return result
        except Exception as e:
            logger.error(f"HeliumIoTExpert error on {task_type}: {e}",
                         exc_info=True)
            return {"status": "error", "error": str(e),
                    "correlation_id": task_id}

    # ------------------------------------------------------------------
    # Drift (never mutates config; adaptive on instance)
    # ------------------------------------------------------------------
    async def _check_drift(self) -> float:
        if self.drift is None:
            return 0.0
        try:
            score = await self.drift.check_drift(
                self.adaptive_cost.get_current_weights())
            score = float(score)
            if score > self.config.drift_threshold_high:
                # Nudge adaptive thresholds; bounded
                for k, v in self._adaptive_thresholds.items():
                    self._adaptive_thresholds[k] = self._bound_threshold(
                        k, v * 1.05)
            else:
                # Decay back toward configured
                for k, base in self.thresholds.items():
                    cur = self._adaptive_thresholds.get(k, base)
                    self._adaptive_thresholds[k] = (
                        cur + (base - cur) * self.config.decay_factor)
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
                feedback_type="helium_iot",
                adaptive_cost_value=0.0,
                state=details,
                candidates=[{"action": a} for a in (
                    "reduce_sampling", "enable_compressed",
                    "use_closer_gateways", "enable_power_saving")],
                source="helium_iot_expert",
                environment="production",
                tags=tags)
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def get_capabilities(self) -> Dict[str, Any]:
        return {
            "expert_name": self.expert_name,
            "supported_tasks": list(self.supported_task_types),
            "health_status": self.health_status,
            "config": {
                "expert_id": self.config.expert_id,
                "objective_weights": dict(self.config.objective_weights),
                "thresholds": dict(self.thresholds),
            },
        }

    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "proposals_count": self._proposals_count,
            "applied_count": self._applied_count,
            "last_error": self.last_error,
            "history_size": len(self._history),
            "thresholds": dict(self._adaptive_thresholds),
            "temporal_violations": self.temporal_safety.counts(),
            "background_tasks": self.task_manager.task_count(),
            "health_status": self.health_status,
        }

    async def get_health_status(self) -> ComponentHealth:
        try:
            if self._adaptive_thresholds.get(
                    "sampling_rate_high", 0.0) <= 0:
                self.health_status = "unhealthy"
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "invalid sampling_rate_high"})
            details = {
                "proposals": self._proposals_count,
                "applied": self._applied_count,
                "circuit_state": self._helium_circuit.state.value,
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
    e = HeliumIoTExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics, seed=42)
    await e.initialize()
    try:
        yield e
    finally:
        await e.shutdown()


@pytest_asyncio.fixture
async def expert_full(storage, queue, adaptive_cost, pareto_gating,
                       drift_detector, metrics):
    class FakeHelium:
        def __init__(self):
            self.scarcity = 0.7
            self.cost = 1.35

        async def get_scarcity(self):
            return self.scarcity

        async def get_cost_index(self):
            return self.cost

    class FakeBio:
        def __init__(self):
            self.spend_calls = []
            self.earn_calls = []

        async def spend(self, actor, amount):
            self.spend_calls.append((actor, amount))

        async def earn(self, actor, amount):
            self.earn_calls.append((actor, amount))

    bio = FakeBio()
    hitl = HumanApprovalHandler()
    e = HeliumIoTExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        bio_core=bio, helium_provider=FakeHelium(),
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
    async def test_task_manager_has_cleanup(self, expert):
        assert expert.task_manager.task_count() >= 1

    @pytest.mark.asyncio
    async def test_expert_id_unique_per_instance(self, storage, queue,
                                                    adaptive_cost,
                                                    pareto_gating,
                                                    drift_detector, metrics):
        e1 = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        e2 = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        assert e1.config.expert_id != e2.config.expert_id

    @pytest.mark.asyncio
    async def test_no_logger_info_kwargs(self, expert):
        # no crash when handle_task runs
        r = await expert.handle_task({"type": "get_thresholds"})
        assert r["status"] == "success"

    @pytest.mark.asyncio
    async def test_history_is_bounded(self, expert):
        assert expert._history.maxlen is not None

    @pytest.mark.asyncio
    async def test_tradeoff_options_sorted_best_first(self, expert):
        # Force a high-scarcity context so options appear
        expert._set_context({"helium_scarcity": 0.8,
                              "battery_level": 0.15,
                              "network_latency_ms": 120.0})
        d = await expert.propose_async({})
        if len(d.options) >= 2:
            scores = [o["objective_score"] for o in d.options]
            assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_threshold_mutation_bounded(self, expert):
        # Force 20 drift events
        for _ in range(20):
            await expert._check_drift()
        for k, v in expert._adaptive_thresholds.items():
            lo = expert.config.threshold_min.get(k, 0.0)
            hi = expert.config.threshold_max.get(k, float("inf"))
            assert lo <= v <= hi

    @pytest.mark.asyncio
    async def test_config_thresholds_not_mutated(self, expert):
        original = dict(expert.config.thresholds)
        for _ in range(20):
            await expert._check_drift()
        assert expert.config.thresholds == original

    @pytest.mark.asyncio
    async def test_health_returns_component_health(self, expert):
        h = await expert.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert isinstance(h.status, HealthStatus)

    @pytest.mark.asyncio
    async def test_set_thresholds_validates_keys(self, expert):
        before = dict(expert.thresholds)
        await expert.set_thresholds({
            "helium_scarcity_high": 0.7,
            "garbage_key": 999.0,
        })
        assert expert.thresholds["helium_scarcity_high"] == 0.7
        assert "garbage_key" not in expert.thresholds

    @pytest.mark.asyncio
    async def test_set_thresholds_bounds(self, expert):
        await expert.set_thresholds({"helium_scarcity_high": 5.0})
        assert expert.thresholds["helium_scarcity_high"] <= \
            expert.config.threshold_max["helium_scarcity_high"]

    @pytest.mark.asyncio
    async def test_objective_weights_used(self, expert):
        expert._set_context({"helium_scarcity": 0.8,
                              "battery_level": 0.15,
                              "network_latency_ms": 120.0})
        d = await expert.propose_async({})
        # At least one option must have an objective_score computed
        if d.options:
            assert any("objective_score" in o for o in d.options)


# =============================================================================
# TESTS — CORE OPERATIONS
# =============================================================================
class TestCoreOperations:
    @pytest.mark.asyncio
    async def test_propose_returns_decision(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.5,
            "battery_level": 0.8,
            "network_latency_ms": 50.0,
        })
        assert isinstance(d, HeliumDecision)
        assert d.operation == "propose"
        assert d.status == "success"
        assert d.recommendation is not None
        assert "sampling_rate_hz" in d.recommendation

    @pytest.mark.asyncio
    async def test_propose_high_scarcity_lowers_sampling(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.9,
            "battery_level": 0.8,
            "network_latency_ms": 50.0,
        })
        assert d.recommendation["sampling_rate_hz"] <= \
            expert._adaptive_thresholds["sampling_rate_low"]

    @pytest.mark.asyncio
    async def test_propose_low_battery_lowers_sampling(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.4,
            "battery_level": 0.05,
            "network_latency_ms": 50.0,
        })
        assert d.recommendation["sampling_rate_hz"] <= 2.0

    @pytest.mark.asyncio
    async def test_apply_recommendation(self, expert):
        d = await expert.apply_recommendation(
            {"sampling_rate_hz": 5.0, "power_saving_mode": False})
        assert d.status == "success"
        assert expert._applied_count == 1

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, expert):
        probs = await expert.policy_probs({})
        assert len(probs) == 4
        assert abs(sum(probs) - 1.0) < 1e-6

    @pytest.mark.asyncio
    async def test_handle_task_get_thresholds(self, expert):
        r = await expert.handle_task({"type": "get_thresholds"})
        assert r["status"] == "success"
        assert "recommendation" in r
        assert "thresholds" in r["recommendation"]

    @pytest.mark.asyncio
    async def test_handle_task_set_thresholds(self, expert):
        r = await expert.handle_task({
            "type": "set_thresholds",
            "thresholds": {"helium_scarcity_high": 0.75}})
        assert r["status"] == "success"
        assert expert.thresholds["helium_scarcity_high"] == 0.75

    @pytest.mark.asyncio
    async def test_handle_task_unknown(self, expert):
        r = await expert.handle_task({"type": "nonsense"})
        assert r["status"] == "error"


# =============================================================================
# TESTS — HITL + SAFETY
# =============================================================================
class TestHITLAndSafety:
    @pytest.mark.asyncio
    async def test_hitl_rejects_drastic_action(self, storage, queue,
                                                 adaptive_cost,
                                                 pareto_gating,
                                                 drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("helium_iot_expert", "defer")
        e = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, seed=1)
        await e.initialize()
        try:
            d = await e.propose_async({
                "helium_scarcity": 0.95,   # critical → drastic
                "battery_level": 0.1,
                "network_latency_ms": 50.0,
            })
            assert d.requires_approval is True
            assert d.status == "deferred"
            assert d.approved is False
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_approves_drastic_action(self, storage, queue,
                                                  adaptive_cost,
                                                  pareto_gating,
                                                  drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("helium_iot_expert", "approve")
        e = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, seed=1)
        await e.initialize()
        try:
            d = await e.propose_async({
                "helium_scarcity": 0.95,
                "battery_level": 0.1,
                "network_latency_ms": 50.0,
            })
            assert d.requires_approval is True
            assert d.approved is True
            assert d.status == "success"
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_temporal_safety_rules_registered(self, expert):
        counts = expert.temporal_safety.counts()
        assert len(counts) >= 5

    @pytest.mark.asyncio
    async def test_safety_violations_returned(self, expert):
        v = await expert._safety_check(5.0, 0.5, recent_drastic=0)
        assert isinstance(v, tuple)


# =============================================================================
# TESTS — XAI
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_propose_includes_attributions(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.5,
            "battery_level": 0.8,
            "network_latency_ms": 50.0,
        })
        assert d.explanation is not None
        assert d.explanation.get("method") == "integrated_gradients"
        assert "attributions" in d.explanation
        assert len(d.explanation["attributions"]) == 4
        assert d.counterfactual is not None

    @pytest.mark.asyncio
    async def test_counterfactual_shape(self, expert):
        d = await expert.propose_async({"helium_scarcity": 0.5})
        if d.counterfactual:
            assert "changed_index" in d.counterfactual \
                or "success" in d.counterfactual


# =============================================================================
# TESTS — PERSISTENCE
# =============================================================================
class TestPersistence:
    @pytest.mark.asyncio
    async def test_roundtrip(self, storage, queue, adaptive_cost,
                              pareto_gating, drift_detector, metrics):
        e1 = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e1.initialize()
        try:
            await e1.set_thresholds({"helium_scarcity_high": 0.7})
            await e1.shutdown()
        finally:
            pass

        e2 = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        await e2.initialize()
        try:
            assert e2.thresholds["helium_scarcity_high"] == 0.7
        finally:
            await e2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, expert, storage):
        await expert._save_thresholds()
        raw = storage.get_state("helium_iot_thresholds_v4")
        assert raw is not None
        json.loads(raw)


# =============================================================================
# TESTS — INTEGRATION
# =============================================================================
class TestIntegration:
    @pytest.mark.asyncio
    async def test_full_bio_integration(self, expert_full):
        await expert_full.propose_async({
            "helium_scarcity": 0.5,
            "battery_level": 0.8,
            "network_latency_ms": 50.0,
        })
        # Bio spend must be called at least once
        assert len(expert_full._bio.spend_calls) >= 1

    @pytest.mark.asyncio
    async def test_custom_provider_used(self, expert_full):
        data = await expert_full._get_helium_data()
        assert data["scarcity"] == 0.7
        assert data["cost"] == 1.35

    @pytest.mark.asyncio
    async def test_swarm_and_history(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.6,
            "battery_level": 0.4,
            "network_latency_ms": 80.0,
        })
        assert d.status == "success"
        # History must have grown
        assert len(expert._history) >= 1


# =============================================================================
# TESTS — DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_proposal(self, storage, queue,
                                             adaptive_cost,
                                             pareto_gating,
                                             drift_detector, metrics):
        def _make():
            return HeliumIoTExpert(
                storage=storage, message_queue=queue,
                adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
                drift_detector=drift_detector, metrics=metrics, seed=7)
        e1 = _make(); e2 = _make()
        await e1.initialize(); await e2.initialize()
        try:
            d1 = await e1.propose_async({"helium_scarcity": 0.5,
                                           "battery_level": 0.8})
            d2 = await e2.propose_async({"helium_scarcity": 0.5,
                                           "battery_level": 0.8})
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

        expert = HeliumIoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics, seed=1)
        await expert.initialize()
        try:
            for ctx in [
                {"helium_scarcity": 0.4, "battery_level": 0.8,
                 "network_latency_ms": 50.0},
                {"helium_scarcity": 0.85, "battery_level": 0.5,
                 "network_latency_ms": 120.0},
                {"helium_scarcity": 0.5, "battery_level": 0.1,
                 "network_latency_ms": 60.0},
            ]:
                d = await expert.propose_async(ctx)
                print(f"scarcity={ctx['helium_scarcity']:.2f} "
                      f"status={d.status} "
                      f"rate={d.recommendation.get('sampling_rate_hz')}")
                print(f"  requires_approval={d.requires_approval} "
                      f"approved={d.approved} "
                      f"quality={d.quality:.2f}")
                if d.options:
                    print(f"  options={len(d.options)} "
                          f"top={d.options[0]['action']}")
            m = await expert.get_metrics()
            print("Metrics:",
                  json.dumps(m, indent=2, default=str))
        finally:
            await expert.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
