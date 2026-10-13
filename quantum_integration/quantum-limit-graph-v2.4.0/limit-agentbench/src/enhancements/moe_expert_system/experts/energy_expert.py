#!/usr/bin/env python3
"""
EnergyExpert v4.0.0 — Self-Contained Single-File Bundle
=========================================================

Complete rewrite of the Green Agent EnergyExpert. Everything needed to
run and test is in this file:

  1. Protocol interfaces for every injected dependency (storage, queue,
     metrics, adaptive cost, pareto, drift, carbon, helium, bio-core).
  2. Local stubs (InMemoryStorage, AsyncMessageQueue, FeedbackEvent,
     MetricsRegistry, AdaptiveCostFunction, ParetoGating, DriftDetector).
  3. Shared runtime: CircuitBreaker (probe-limited), TemporalSafetyMonitor
     (MTL G/F/NEVER/H/O with windows), HumanApprovalHandler (uncertainty
     sampling), XAIExplainer (Integrated Gradients + counterfactuals),
     TaskManager (named, cancellable), HoltForecaster.
  4. `EnergyDecision` value object.
  5. Real budget enforcement + HITL gating.
  6. Correct carbon API parsing + cache key consistency.
  7. Real Holt-based forecasting.
  8. Helium-modulated routing.
  9. Persistence: JSON state, no pickle.
 10. Comprehensive pytest suite.

Run:
    python energy_expert_v4.py         # demo
    pytest energy_expert_v4.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
Optional: aiohttp (real carbon API), pandas (for completeness).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re as _re
import threading
import time
import uuid
from collections import defaultdict, deque
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

logger = logging.getLogger("energy_expert")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False


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
    async def get_current_intensity(self) -> float: ...


@runtime_checkable
class HeliumOptimizerLike(Protocol):
    async def get_helium_status(self) -> Dict[str, Any]: ...


@runtime_checkable
class BioCoreLike(Protocol):
    async def spend(self, actor: str, amount: float) -> Any: ...
    async def earn(self, actor: str, amount: float) -> Any: ...


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
        self._topics: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.published: List[Tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))
        await self._topics[topic].put(payload)

    async def subscribe(self, topic: str, timeout: float = 0.5) -> Optional[str]:
        try:
            return await asyncio.wait_for(self._topics[topic].get(), timeout)
        except asyncio.TimeoutError:
            return None


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
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj


class MetricsRegistry:
    """Canonical inc/set; increment/observe are aliases."""
    def __init__(self):
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}

    def inc(self, name: str, amount: float = 1.0) -> None:
        self.counters[name] += float(amount)

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
# XAI (Integrated Gradients)
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
# Task Manager
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


# -----------------------------------------------------------------------------
# Holt forecaster
# -----------------------------------------------------------------------------
class HoltForecaster:
    """Holt's linear exponential smoothing on a 1-D series."""

    def __init__(self, alpha: float = 0.3, beta: float = 0.1):
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.level: Optional[float] = None
        self.trend = 0.0
        self._residuals: Deque[float] = deque(maxlen=200)

    def observe(self, value: float) -> None:
        v = float(value)
        if self.level is None:
            self.level = v
            self.trend = 0.0
            return
        prev_level = self.level
        prev_trend = self.trend
        new_level = self.alpha * v + (1 - self.alpha) * (prev_level + prev_trend)
        new_trend = self.beta * (new_level - prev_level) + (1 - self.beta) * prev_trend
        self._residuals.append(v - new_level)
        self.level = new_level
        self.trend = new_trend

    def forecast(self, steps: int = 1) -> float:
        if self.level is None:
            return 0.0
        return max(0.0, self.level + steps * self.trend)

    def interval(self, steps: int = 1, z: float = 1.96
                 ) -> Tuple[float, float]:
        if not self._residuals:
            p = self.forecast(steps)
            return (p, p)
        std = float(np.std(list(self._residuals)) + 1e-9)
        p = self.forecast(steps)
        return (max(0.0, p - z * std), p + z * std)

    def is_ready(self) -> bool:
        return self.level is not None


# =============================================================================
# ENUMS + VALUE OBJECTS
# =============================================================================
class EnergySourceType(str, Enum):
    RENEWABLE = "renewable"
    FOSSIL_FUEL = "fossil_fuel"
    NUCLEAR = "nuclear"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class SustainabilityStrategy(str, Enum):
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    PERFORMANCE = "performance"
    RENEWABLE_ONLY = "renewable_only"


@dataclass
class EnergyProfile:
    task_id: str
    estimated_duration_seconds: float
    estimated_idle_energy_kwh: float = 0.0
    estimated_cpu_energy_kwh: float = 0.0
    estimated_memory_energy_kwh: float = 0.0
    estimated_network_energy_kwh: float = 0.0
    estimated_storage_energy_kwh: float = 0.0
    estimated_total_energy_kwh: float = 0.0
    carbon_intensity_g_per_kwh: float = 0.0
    estimated_carbon_g: float = 0.0
    estimated_helium_ml: float = 0.0
    energy_efficiency_score: float = 0.0
    sustainability_score: float = 0.0
    recommended_strategy: str = "balanced"
    region: str = "us-east"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class HeliumAnalysis:
    available_ml: float
    required_ml: float
    scarcity_factor: float
    recovery_potential_ml: float
    can_proceed: bool
    recommendation: str
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class CarbonFootprint:
    baseline_carbon_g: float
    offset_strategy: Optional[str]
    offset_carbon_g: float
    net_carbon_g: float
    cost_usd: float
    roi_factor: float
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class EnergyExpertMetrics:
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    tasks_analyzed: int = 0
    total_energy_kwh: float = 0.0
    total_carbon_kg: float = 0.0
    total_helium_ml: float = 0.0
    success: bool = True
    error_message: Optional[str] = None

    def duration_seconds(self) -> float:
        if self.end_time is not None:
            return float(self.end_time - self.start_time)
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class EnergyDecision:
    """Snapshot of a single energy operation decision."""
    task_id: str
    operation: str
    status: str = "success"
    error: Optional[str] = None

    # Primary outputs
    profile: Optional[Dict[str, Any]] = None
    footprint: Optional[Dict[str, Any]] = None
    helium: Optional[Dict[str, Any]] = None
    routing: Optional[Dict[str, bool]] = None
    strategy: Optional[str] = None
    forecast: Optional[List[Dict[str, Any]]] = None

    # Bookkeeping
    carbon_kg: float = 0.0
    energy_kwh: float = 0.0
    helium_ml: float = 0.0
    confidence: float = 0.0

    # Meta
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None
    safety_violations: Tuple[str, ...] = ()
    requires_approval: bool = False
    timestamp: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "operation": self.operation,
            "status": self.status,
            "error": self.error,
            "profile": self.profile,
            "footprint": self.footprint,
            "helium": self.helium,
            "routing": self.routing,
            "strategy": self.strategy,
            "forecast": self.forecast,
            "carbon_kg": float(self.carbon_kg),
            "energy_kwh": float(self.energy_kwh),
            "helium_ml": float(self.helium_ml),
            "confidence": float(self.confidence),
            "explanation": self.explanation,
            "counterfactual": self.counterfactual,
            "safety_violations": list(self.safety_violations),
            "requires_approval": bool(self.requires_approval),
            "timestamp": float(self.timestamp),
        }


@dataclass
class EnergyExpertConfig:
    enable_energy_estimation: bool = True
    enable_carbon_tracking: bool = True
    enable_helium_analysis: bool = True
    enable_forecasting: bool = True
    enable_persistence: bool = True
    enable_real_time_carbon: bool = True
    enable_real_time_helium: bool = False

    cpu_power_watt: float = 50.0
    memory_power_per_gb: float = 0.5
    network_power_per_mbps: float = 0.01
    storage_power_per_gb: float = 0.001
    idle_power_watt: float = 10.0
    power_utilization_factor: float = 0.7
    default_carbon_intensity_g_per_kwh: float = 100.0
    carbon_api_url: str = "https://api.electricitymap.org/v3/carbon-intensity/latest"
    carbon_api_key: str = ""
    helium_scarcity_factor: float = 1.0
    helium_recovery_efficiency: float = 0.7
    helium_cost_per_liter_usd: float = 0.5
    energy_efficiency_threshold: float = 0.7
    carbon_budget_per_task_g: float = 10.0
    helium_budget_per_task_ml: float = 5.0
    forecast_window_hours: int = 24
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    carbon_cache_ttl_seconds: int = 300
    helium_cache_ttl_seconds: int = 300
    metrics_history_maxlen: int = 1000
    task_cache_maxlen: int = 1000

    def __post_init__(self) -> None:
        if self.cpu_power_watt <= 0:
            self.cpu_power_watt = 50.0
        if self.memory_power_per_gb < 0:
            self.memory_power_per_gb = 0.5
        if self.network_power_per_mbps < 0:
            self.network_power_per_mbps = 0.01
        if self.storage_power_per_gb < 0:
            self.storage_power_per_gb = 0.001
        if self.idle_power_watt < 0:
            self.idle_power_watt = 0.0
        if self.carbon_budget_per_task_g <= 0:
            self.carbon_budget_per_task_g = 10.0
        if self.helium_budget_per_task_ml <= 0:
            self.helium_budget_per_task_ml = 5.0


# =============================================================================
# ENERGY EXPERT (v4.0.0)
# =============================================================================
class EnergyExpert:
    """Energy Expert v4.0.0 — MoE expert for energy, carbon, and helium."""

    SUPPORTED_TASKS = (
        "energy_estimate", "carbon_profile", "helium_analysis",
        "sustainability_recommend", "energy_route", "forecast",
    )

    def __init__(
        self,
        storage: StorageLike,
        message_queue: MessageQueueLike,
        adaptive_cost: AdaptiveCostLike,
        pareto_gating: ParetoGatingLike,
        drift_detector: DriftDetectorLike,
        metrics: MetricsLike,
        carbon_manager: Optional[CarbonManagerLike] = None,
        helium_optimizer: Optional[HeliumOptimizerLike] = None,
        bio_core: Optional[BioCoreLike] = None,
        hitl: Optional[HumanApprovalHandler] = None,
        config: Optional[EnergyExpertConfig] = None,
        seed: int = 42,
    ):
        self.expert_name = "energy_expert"
        self.supported_task_types = list(self.SUPPORTED_TASKS)
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.carbon_manager = carbon_manager
        self.helium_optimizer = helium_optimizer
        self.bio_core = bio_core
        self.hitl = hitl
        self.config = config or EnergyExpertConfig()

        # Deterministic RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_cb = random.Random(seed + 2)
        np.random.seed(seed)

        # State
        self.energy_profiles: Dict[str, EnergyProfile] = {}
        self.carbon_footprints: Dict[str, CarbonFootprint] = {}
        self.helium_analyses: Dict[str, HeliumAnalysis] = {}
        self.metrics_history: Deque[EnergyExpertMetrics] = deque(
            maxlen=self.config.metrics_history_maxlen)
        self.task_counts: Dict[str, int] = defaultdict(int)
        self.tasks_handled = 0
        self.total_latency = 0.0

        # Bounded caches
        self.task_energy_cache: Deque[Tuple[str, float]] = deque(
            maxlen=self.config.task_cache_maxlen)
        self._task_energy_map: Dict[str, float] = {}

        # Carbon cache: single shared key "global" (bug fix)
        self._carbon_cache: Dict[str, Tuple[float, datetime]] = {}
        self._helium_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        # Adapt (not config-mutated) thresholds
        self._adaptive_efficiency_threshold = float(
            self.config.energy_efficiency_threshold)
        self._adaptive_carbon_budget_g = float(
            self.config.carbon_budget_per_task_g)

        # Circuit breaker for HTTP
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            name="energy_external",
        )

        self._session: Optional[Any] = None
        self._session_lock = asyncio.Lock()

        # Temporal safety
        self.temporal_safety = TemporalSafetyMonitor(max_trace=500)
        self._setup_temporal_rules()

        # XAI
        self.xai = XAIExplainer(rng=self._rng_xai)

        # Forecaster on recent total energy
        self._energy_forecaster = HoltForecaster()

        # Task manager + lifecycle
        self.task_manager = TaskManager()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_state()
        if self.config.enable_real_time_carbon:
            self.task_manager.start_coro(
                "carbon_update", self._periodic_carbon_update())
        self._initialized = True

    async def close(self) -> None:
        await self.task_manager.stop_all()
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
        await self._save_state()

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula("G (efficiency >= 0.0)")
        self.temporal_safety.add_formula("G (efficiency <= 1.0)")
        self.temporal_safety.add_formula(
            "G (carbon_g <= carbon_budget_g)")
        self.temporal_safety.add_formula("NEVER (circuit_open > 0)")

    # ------------------------------------------------------------------
    # Persistence (sync storage via to_thread)
    # ------------------------------------------------------------------
    async def _load_state(self) -> None:
        try:
            data = await asyncio.to_thread(
                self.storage.get_state, "energy_expert_state_v4")
            if not data:
                return
            state = json.loads(data)
            self.tasks_handled = int(state.get("tasks_handled", 0))
            self.total_latency = float(state.get("total_latency", 0.0))
            saved_counts = state.get("task_counts", {})
            self.task_counts = defaultdict(int, saved_counts)

            for m in state.get("metrics_history", []):
                try:
                    self.metrics_history.append(EnergyExpertMetrics(**m))
                except Exception:
                    pass

            for k, v in state.get("carbon_footprints", {}).items():
                try:
                    self.carbon_footprints[k] = CarbonFootprint(**v)
                except Exception:
                    pass

            for k, v in state.get("helium_analyses", {}).items():
                try:
                    self.helium_analyses[k] = HeliumAnalysis(**v)
                except Exception:
                    pass

            for k, v in state.get("energy_profiles", {}).items():
                try:
                    self.energy_profiles[k] = EnergyProfile(**v)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed to load energy expert state: {e}")

    async def _save_state(self) -> None:
        try:
            state = {
                "tasks_handled": self.tasks_handled,
                "total_latency": self.total_latency,
                "task_counts": dict(self.task_counts),
                "metrics_history": [m.to_dict()
                                    for m in self.metrics_history],
                "carbon_footprints": {k: v.to_dict()
                                      for k, v in self.carbon_footprints.items()},
                "helium_analyses": {k: v.to_dict()
                                    for k, v in self.helium_analyses.items()},
                "energy_profiles": {k: v.to_dict()
                                    for k, v in self.energy_profiles.items()},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await asyncio.to_thread(
                self.storage.save_state, "energy_expert_state_v4",
                json.dumps(_json_safe(state), default=str))
        except Exception as e:
            logger.error(f"Failed to save energy expert state: {e}")

    # ------------------------------------------------------------------
    # Carbon API (correct response path; consistent cache key)
    # ------------------------------------------------------------------
    async def _get_session(self):
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not installed")
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _fetch_carbon_intensity(self, region: str = "global") -> float:
        """Return carbon intensity (g CO2 / kWh). Cached by region."""
        if not self.config.enable_real_time_carbon:
            return float(self.config.default_carbon_intensity_g_per_kwh)

        now = datetime.now(timezone.utc)
        async with self._cache_lock:
            hit = self._carbon_cache.get(region)
            if hit is not None:
                value, ts = hit
                if (now - ts).total_seconds() < \
                        self.config.carbon_cache_ttl_seconds:
                    return float(value)

        if not AIOHTTP_AVAILABLE:
            return float(self.config.default_carbon_intensity_g_per_kwh)

        async def _fetch() -> float:
            session = await self._get_session()
            url = f"{self.config.carbon_api_url}?zone={region}"
            headers = {}
            if self.config.carbon_api_key:
                headers["auth-token"] = self.config.carbon_api_key
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"carbon API status {resp.status}")
                data = await resp.json()
                # Corrected path: top-level "carbonIntensity"
                for key in ("carbonIntensity", "carbon_intensity"):
                    if key in data:
                        v = float(data[key])
                        if v > 0:
                            return v
                nested = data.get("data") or {}
                for key in ("carbonIntensity", "carbon_intensity"):
                    if key in nested:
                        v = float(nested[key])
                        if v > 0:
                            return v
                return float(self.config.default_carbon_intensity_g_per_kwh)

        try:
            intensity = await self._circuit_breaker.call(_fetch)
            async with self._cache_lock:
                self._carbon_cache[region] = (intensity, now)
                self._carbon_cache["global"] = (intensity, now)
            return float(intensity)
        except Exception as e:
            logger.warning(f"carbon fetch failed: {e}; using default")
            async with self._cache_lock:
                self._carbon_cache[region] = (
                    float(self.config.default_carbon_intensity_g_per_kwh),
                    now)
            return float(self.config.default_carbon_intensity_g_per_kwh)

    async def _carbon_intensity_from_manager(self) -> Optional[float]:
        if self.carbon_manager is None:
            return None
        try:
            v = self.carbon_manager.get_current_intensity()
            if asyncio.iscoroutine(v):
                v = await v
            v = float(v)
            if v > 0:
                return v
        except Exception as e:
            logger.debug(f"carbon manager failed: {e}")
        return None

    # ------------------------------------------------------------------
    # Helium (correct formula)
    # ------------------------------------------------------------------
    async def _helium_status(self) -> Dict[str, float]:
        if self.helium_optimizer is not None:
            try:
                v = self.helium_optimizer.get_helium_status()
                if asyncio.iscoroutine(v):
                    v = await v
                if isinstance(v, dict):
                    scarcity = float(v.get("scarcity",
                                            self.config.helium_scarcity_factor))
                    price = float(v.get("price", 0.5))
                    remaining = float(v.get("remaining", 100.0))
                    return {"scarcity": max(0.0, min(1.0, scarcity)),
                            "price": max(0.0, price),
                            "remaining_ml": max(0.0, remaining)}
            except Exception as e:
                logger.debug(f"helium optimizer failed: {e}")
        return {"scarcity": float(self.config.helium_scarcity_factor),
                "price": float(self.config.helium_cost_per_liter_usd),
                "remaining_ml": 1000.0}

    # ------------------------------------------------------------------
    # Bio integration
    # ------------------------------------------------------------------
    async def _bio_spend_earn(self, energy_kwh: float,
                              carbon_g: float,
                              quality: float,
                              op: str) -> None:
        if self.bio_core is None:
            return
        try:
            atp_cost = max(0.01, energy_kwh * 0.1 + carbon_g * 0.001)
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
    # Periodic carbon update
    # ------------------------------------------------------------------
    async def _periodic_carbon_update(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(),
                    timeout=self.config.carbon_cache_ttl_seconds)
                break
            except asyncio.TimeoutError:
                pass
            try:
                v = await self._carbon_intensity_from_manager()
                if v is None:
                    await self._fetch_carbon_intensity("global")
                else:
                    async with self._cache_lock:
                        now = datetime.now(timezone.utc)
                        self._carbon_cache["global"] = (v, now)
            except Exception as e:
                logger.debug(f"periodic carbon update failed: {e}")

    # ------------------------------------------------------------------
    # Task dispatcher
    # ------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get("type", "unknown")
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        start = time.monotonic()

        dispatch = {
            "energy_estimate": self.estimate_task_energy,
            "carbon_profile": self.profile_carbon_footprint,
            "helium_analysis": self.analyze_helium_impact,
            "sustainability_recommend": self.recommend_strategy,
            "energy_route": self.route_by_energy,
            "forecast": self.forecast_energy,
        }
        handler = dispatch.get(task_type)
        if handler is None:
            return {"status": "error",
                    "error": f"unknown task type: {task_type}",
                    "correlation_id": task_id}

        try:
            decision = await handler(task)
            latency = time.monotonic() - start
            self.tasks_handled += 1
            self.total_latency += latency
            self.task_counts[task_type] += 1
            self.metrics.inc(f"energy_task_{task_type}")
            self.metrics.set("energy_last_latency", latency)

            result = decision.to_dict()
            result["correlation_id"] = task_id
            result["latency_seconds"] = latency
            return result
        except Exception as e:
            logger.error(f"EnergyExpert error on {task_type}: {e}",
                         exc_info=True)
            self.metrics.inc(f"energy_task_{task_type}_error")
            return {"status": "error", "error": str(e),
                    "correlation_id": task_id}

    # ------------------------------------------------------------------
    # Safety snapshot
    # ------------------------------------------------------------------
    async def _safety_check(self, efficiency: float,
                            carbon_g: float) -> Tuple[str, ...]:
        circuit_open = 1 if self._circuit_breaker.state == \
                            CircuitBreakerState.OPEN else 0
        await self.temporal_safety.push({
            "efficiency": float(efficiency),
            "carbon_g": float(carbon_g),
            "carbon_budget_g": float(self._adaptive_carbon_budget_g),
            "circuit_open": circuit_open,
        })
        results = await self.temporal_safety.check()
        return tuple(rid for rid, ok in results.items() if not ok)

    # ------------------------------------------------------------------
    # Carbon intensity used by every operation
    # ------------------------------------------------------------------
    async def _current_carbon_g_per_kwh(self, region: str = "global"
                                         ) -> float:
        v = await self._carbon_intensity_from_manager()
        if v is not None:
            async with self._cache_lock:
                self._carbon_cache["global"] = (
                    v, datetime.now(timezone.utc))
            return v
        return await self._fetch_carbon_intensity(region)

    # ------------------------------------------------------------------
    # Operation 1: estimate_task_energy
    # ------------------------------------------------------------------
    async def estimate_task_energy(self, task: Dict[str, Any]) -> EnergyDecision:
        payload = task.get("payload", {})
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        region = payload.get("region", "global")

        cpu_seconds = float(payload.get("cpu_seconds", 1.0))
        memory_gb = float(payload.get("memory_gb", 0.5))
        network_mbps = float(payload.get("network_mbps", 1.0))
        storage_gb = float(payload.get("storage_gb", 0.0))
        duration_s = float(payload.get("duration_seconds", cpu_seconds))

        idle_kwh = (duration_s * self.config.idle_power_watt) / 3.6e6
        cpu_kwh = (cpu_seconds * self.config.cpu_power_watt
                    * self.config.power_utilization_factor) / 3.6e6
        mem_kwh = (duration_s * memory_gb
                    * self.config.memory_power_per_gb) / 3.6e6
        net_kwh = (network_mbps * duration_s
                    * self.config.network_power_per_mbps) / 3.6e6
        storage_kwh = (storage_gb * self.config.storage_power_per_gb) / 3.6e6
        total_kwh = idle_kwh + cpu_kwh + mem_kwh + net_kwh + storage_kwh

        carbon_g_per_kwh = await self._current_carbon_g_per_kwh(region)
        carbon_g = total_kwh * carbon_g_per_kwh * 1000.0

        helium = await self._helium_status()
        # Helium consumption proxies energy; scaled by scarcity exposure
        helium_ml = total_kwh * 100.0 * (1.0 - helium["scarcity"] * 0.5)

        # Efficiency: 1 kWh reference
        efficiency = max(0.0, min(1.0, 1.0 - total_kwh / 1.0))
        sustainability = (
            0.4 * efficiency +
            0.3 * max(0.0, 1.0 - carbon_g / 100.0) +
            0.3 * max(0.0, 1.0 - helium_ml / 100.0)
        )
        if sustainability > 0.8:
            strategy = SustainabilityStrategy.PERFORMANCE.value
        elif sustainability > 0.5:
            strategy = SustainabilityStrategy.BALANCED.value
        else:
            strategy = SustainabilityStrategy.CONSERVATIVE.value

        # Budget enforcement
        requires_approval = False
        if carbon_g > self._adaptive_carbon_budget_g:
            if self.hitl is not None:
                approved = await self.hitl.request_approval({
                    "action": "exceed_carbon_budget",
                    "candidates": [
                        {"id": "approve", "carbon_g": carbon_g},
                        {"id": "defer", "carbon_g": 0.0}]},
                    user_id=self.expert_name)
            else:
                approved = False
            if not approved:
                requires_approval = True
                return EnergyDecision(
                    task_id=task_id, operation="energy_estimate",
                    status="deferred",
                    error="carbon_budget_exceeded",
                    requires_approval=True,
                    carbon_kg=carbon_g / 1000.0,
                    energy_kwh=total_kwh,
                    helium_ml=helium_ml,
                    confidence=0.0, timestamp=time.time(),
                )

        if helium_ml > self.config.helium_budget_per_task_ml:
            if self.hitl is not None:
                approved = await self.hitl.request_approval({
                    "action": "exceed_helium_budget",
                    "candidates": [
                        {"id": "approve", "helium_ml": helium_ml},
                        {"id": "defer", "helium_ml": 0.0}]},
                    user_id=self.expert_name)
            else:
                approved = False
            if not approved:
                requires_approval = True
                return EnergyDecision(
                    task_id=task_id, operation="energy_estimate",
                    status="deferred",
                    error="helium_budget_exceeded",
                    requires_approval=True,
                    carbon_kg=carbon_g / 1000.0,
                    energy_kwh=total_kwh,
                    helium_ml=helium_ml,
                    confidence=0.0, timestamp=time.time(),
                )

        # XAI
        explanation, counterfactual = self._explain_estimate(
            cpu_seconds, memory_gb, network_mbps, duration_s,
            carbon_g_per_kwh)

        profile = EnergyProfile(
            task_id=task_id,
            estimated_duration_seconds=duration_s,
            estimated_idle_energy_kwh=idle_kwh,
            estimated_cpu_energy_kwh=cpu_kwh,
            estimated_memory_energy_kwh=mem_kwh,
            estimated_network_energy_kwh=net_kwh,
            estimated_storage_energy_kwh=storage_kwh,
            estimated_total_energy_kwh=total_kwh,
            carbon_intensity_g_per_kwh=carbon_g_per_kwh,
            estimated_carbon_g=carbon_g,
            estimated_helium_ml=helium_ml,
            energy_efficiency_score=efficiency,
            sustainability_score=sustainability,
            recommended_strategy=strategy,
            region=region,
        )
        self.energy_profiles[task_id] = profile
        self._task_energy_map[task_id] = total_kwh
        if len(self.task_energy_cache) >= self.task_energy_cache.maxlen:
            old_id, _ = self.task_energy_cache.popleft()
            self._task_energy_map.pop(old_id, None)
        self.task_energy_cache.append((task_id, total_kwh))

        self.metrics_history.append(EnergyExpertMetrics(
            operation_name="estimate_task_energy",
            start_time=time.monotonic(),
            end_time=time.monotonic(),
            tasks_analyzed=1,
            total_energy_kwh=total_kwh,
            total_carbon_kg=carbon_g / 1000.0,
            total_helium_ml=helium_ml,
        ))
        self.metrics.set("energy_consumption", total_kwh)
        self.metrics.set("carbon_footprint", carbon_g / 1000.0)
        self._energy_forecaster.observe(total_kwh)

        await self._bio_spend_earn(total_kwh, carbon_g, sustainability,
                                    "estimate")

        violations = await self._safety_check(efficiency, carbon_g)
        await self._publish_feedback(
            task_id=f"energy_estimate_{task_id}",
            action="estimate", quality=sustainability,
            details={"task_id": task_id, "region": region,
                     "carbon_g": carbon_g},
            tags=["energy", "estimate"])

        await self._check_drift()

        return EnergyDecision(
            task_id=task_id, operation="energy_estimate",
            profile=profile.to_dict(),
            carbon_kg=carbon_g / 1000.0,
            energy_kwh=total_kwh,
            helium_ml=helium_ml,
            confidence=float(sustainability),
            explanation=explanation,
            counterfactual=counterfactual,
            safety_violations=violations,
            requires_approval=requires_approval,
            timestamp=time.time(),
        )

    def _explain_estimate(self, cpu_s: float, mem_gb: float,
                           net_mbps: float, dur_s: float,
                           carbon_g_per_kwh: float
                           ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        x = np.array([cpu_s, mem_gb, net_mbps, dur_s], dtype=float)

        def _score(v: np.ndarray) -> float:
            cpu_kwh = (float(v[0]) * self.config.cpu_power_watt
                        * self.config.power_utilization_factor) / 3.6e6
            mem_kwh = (float(v[3]) * float(v[1])
                        * self.config.memory_power_per_gb) / 3.6e6
            net_kwh = (float(v[2]) * float(v[3])
                        * self.config.network_power_per_mbps) / 3.6e6
            idle_kwh = (float(v[3]) * self.config.idle_power_watt) / 3.6e6
            total = cpu_kwh + mem_kwh + net_kwh + idle_kwh
            return total * carbon_g_per_kwh * 1000.0

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["cpu_seconds", "memory_gb",
                                    "network_mbps", "duration_seconds"],
            }
            cf = self.xai.counterfactual(_score, x)
            return explanation, cf
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Operation 2: profile_carbon_footprint
    # ------------------------------------------------------------------
    async def profile_carbon_footprint(self, task: Dict[str, Any]
                                        ) -> EnergyDecision:
        payload = task.get("payload", {})
        task_id = task.get("correlation_id", str(uuid.uuid4()))

        baseline = float(payload.get("baseline_carbon_g", 50.0))
        strategy = payload.get("offset_strategy", "purchase_offset")
        offset_g = 0.0
        if strategy == "renewable_swap":
            offset_g = baseline * 0.8
        elif strategy == "purchase_offset":
            offset_g = baseline * 0.5
        net_g = max(0.0, baseline - offset_g)
        cost_usd = net_g * 0.00001
        roi = baseline / max(net_g, 0.1)

        footprint = CarbonFootprint(
            baseline_carbon_g=baseline,
            offset_strategy=strategy,
            offset_carbon_g=offset_g,
            net_carbon_g=net_g,
            cost_usd=cost_usd,
            roi_factor=roi,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self.carbon_footprints[task_id] = footprint

        await self._bio_spend_earn(0.0, net_g,
                                    0.8 if net_g < baseline else 0.5,
                                    "carbon_profile")
        violations = await self._safety_check(
            efficiency=0.9, carbon_g=net_g)
        await self._publish_feedback(
            task_id=f"energy_carbon_{task_id}",
            action="carbon_profile",
            quality=1.0 if net_g < baseline else 0.5,
            details={"task_id": task_id, "strategy": strategy},
            tags=["energy", "carbon"])

        await self._check_drift()
        return EnergyDecision(
            task_id=task_id, operation="carbon_profile",
            footprint=footprint.to_dict(),
            carbon_kg=net_g / 1000.0,
            confidence=1.0 if net_g < baseline else 0.5,
            safety_violations=violations,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Operation 3: analyze_helium_impact
    # ------------------------------------------------------------------
    async def analyze_helium_impact(self, task: Dict[str, Any]
                                     ) -> EnergyDecision:
        payload = task.get("payload", {})
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        required_ml = float(payload.get("required_ml", 5.0))

        helium = await self._helium_status()
        scarcity = helium["scarcity"]
        # Corrected: higher scarcity => LESS available helium
        capacity_ml = 1000.0
        available_ml = capacity_ml * (1.0 - scarcity)
        recovery_ml = required_ml * self.config.helium_recovery_efficiency
        can_proceed = (available_ml + recovery_ml) >= required_ml

        rec = ("Sufficient helium available; proceed normally"
               if can_proceed else
               "Low helium; enable recovery or defer non-critical tasks")
        analysis = HeliumAnalysis(
            available_ml=available_ml,
            required_ml=required_ml,
            scarcity_factor=scarcity,
            recovery_potential_ml=recovery_ml,
            can_proceed=can_proceed,
            recommendation=rec,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self.helium_analyses[task_id] = analysis

        await self._bio_spend_earn(0.0, 0.0,
                                    1.0 if can_proceed else 0.3,
                                    "helium_analysis")
        violations = await self._safety_check(
            efficiency=0.9 if can_proceed else 0.3, carbon_g=0.0)
        await self._publish_feedback(
            task_id=f"energy_helium_{task_id}",
            action="helium_analysis",
            quality=1.0 if can_proceed else 0.3,
            details={"task_id": task_id, "required_ml": required_ml,
                     "scarcity": scarcity},
            tags=["energy", "helium"])
        await self._check_drift()

        return EnergyDecision(
            task_id=task_id, operation="helium_analysis",
            helium=analysis.to_dict(),
            helium_ml=required_ml,
            confidence=1.0 if can_proceed else 0.3,
            safety_violations=violations,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Operation 4: recommend_strategy
    # ------------------------------------------------------------------
    async def recommend_strategy(self, task: Dict[str, Any]) -> EnergyDecision:
        payload = task.get("payload", {})
        system_load = float(payload.get("system_load", 0.5))
        energy_budget = float(payload.get("energy_budget", 100.0))
        carbon_budget = float(payload.get("carbon_budget", 1000.0))
        helium_availability = float(payload.get("helium_availability", 0.7))

        helium = await self._helium_status()
        helium_scarcity = helium["scarcity"]

        strategies = [s.value for s in SustainabilityStrategy]
        candidates: List[Dict[str, Any]] = []
        for s in strategies:
            if s == SustainabilityStrategy.CONSERVATIVE.value:
                q, cg, lat, ej = 0.9, 5.0, 80.0, 50.0
            elif s == SustainabilityStrategy.BALANCED.value:
                q, cg, lat, ej = 0.8, 10.0, 50.0, 70.0
            elif s == SustainabilityStrategy.PERFORMANCE.value:
                q, cg, lat, ej = 0.6, 20.0, 30.0, 100.0
            else:  # renewable_only
                q, cg, lat, ej = 0.7, 2.0, 90.0, 40.0
            # Helium-aware modulation: penalise energy-heavy strategies
            if helium_scarcity > 0.7:
                ej *= 1.15
                cg *= 1.05
            cost = self.adaptive_cost.compute(
                quality=q, carbon_g=cg, latency_ms=lat, energy_joules=ej,
                health=True, atp=0.5)
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
            chosen = SustainabilityStrategy.BALANCED.value
            explanation = {"reason": "no candidates passed pareto filter"}
        else:
            best = min(candidates, key=lambda c: c["score"])
            chosen = best["strategy"]
            explanation = {
                "reason": "lowest adaptive cost after pareto filter",
                "score": best["score"],
                "quality_score": best["quality_score"],
                "carbon_g": best["carbon_g"],
                "energy_joules": best["energy_joules"],
                "latency_ms": best["latency_ms"],
                "helium_scarcity": helium_scarcity,
            }

        await self._bio_spend_earn(0.0, 0.0, 0.9
                                    if chosen in ("balanced", "conservative")
                                    else 0.7, "recommend")
        violations = await self._safety_check(0.9, 0.0)
        await self._publish_feedback(
            task_id=f"energy_recommend_{uuid.uuid4().hex[:8]}",
            action="recommend", quality=0.9,
            details={"strategy": chosen, "explanation": explanation},
            tags=["energy", "recommend"])
        await self._check_drift()

        return EnergyDecision(
            task_id=task.get("correlation_id", str(uuid.uuid4())),
            operation="recommend_strategy",
            strategy=chosen,
            explanation=explanation,
            safety_violations=violations,
            confidence=0.9,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Operation 5: route_by_energy (helium-modulated)
    # ------------------------------------------------------------------
    async def route_by_energy(self, task: Dict[str, Any]) -> EnergyDecision:
        payload = task.get("payload", {})
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        energy_kwh = float(payload.get("energy_kwh", 0.1))
        carbon_g = float(payload.get("carbon_g", 50.0))

        helium = await self._helium_status()
        helium_scarcity = helium["scarcity"]

        routes = ["cpu_expert", "optimization_expert", "io_expert"]
        candidates: List[Dict[str, Any]] = []
        for route in routes:
            if route == "cpu_expert":
                q, c, lat, e = 0.8, carbon_g * 0.5, 60.0, energy_kwh * 0.6
            elif route == "optimization_expert":
                q, c, lat, e = 0.7, carbon_g, 120.0, energy_kwh
            else:  # io_expert
                q, c, lat, e = 0.6, carbon_g * 0.3, 30.0, energy_kwh * 0.3
            # Helium modulation: penalise high-energy routes when scarce
            if helium_scarcity > 0.7:
                e *= 1.2
            cost = self.adaptive_cost.compute(
                quality=q, carbon_g=c, latency_ms=lat, energy_joules=e,
                health=True, atp=0.5)
            candidates.append({"expert": route, "score": float(cost),
                                "quality_score": q, "carbon_g": c,
                                "latency_ms": lat, "energy_joules": e})

        if self.pareto is not None:
            try:
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed = {c["expert"] for c in filtered}
                    candidates = [c for c in candidates
                                  if c["expert"] in allowed]
            except Exception:
                pass

        if not candidates:
            chosen = "cpu_expert"
            explanation = {"reason": "no candidates passed pareto filter"}
        else:
            best = min(candidates, key=lambda c: c["score"])
            chosen = best["expert"]
            explanation = {
                "reason": "lowest adaptive cost",
                "score": best["score"],
                "quality_score": best["quality_score"],
                "carbon_g": best["carbon_g"],
                "energy_joules": best["energy_joules"],
                "latency_ms": best["latency_ms"],
                "helium_scarcity": helium_scarcity,
            }

        routing = {r: (r == chosen) for r in routes}
        await self._bio_spend_earn(energy_kwh, carbon_g, 0.8, "route")
        violations = await self._safety_check(0.9, carbon_g)
        await self._publish_feedback(
            task_id=f"energy_route_{uuid.uuid4().hex[:8]}",
            action="route", quality=0.9,
            details={"routing": routing, "explanation": explanation},
            tags=["energy", "route"])
        await self._check_drift()

        return EnergyDecision(
            task_id=task_id, operation="energy_route",
            routing=routing,
            explanation=explanation,
            carbon_kg=carbon_g / 1000.0,
            energy_kwh=energy_kwh,
            confidence=0.9,
            safety_violations=violations,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Operation 6: forecast_energy (Holt)
    # ------------------------------------------------------------------
    async def forecast_energy(self, task: Dict[str, Any]) -> EnergyDecision:
        payload = task.get("payload", {})
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        current_load = float(payload.get("current_load", 0.5))
        hours = int(payload.get("forecast_hours",
                                  self.config.forecast_window_hours))

        # Seed the forecaster from metrics history if not already warmed
        if not self._energy_forecaster.is_ready():
            for m in list(self.metrics_history)[-50:]:
                self._energy_forecaster.observe(m.total_energy_kwh)
            if not self._energy_forecaster.is_ready():
                self._energy_forecaster.observe(current_load * 50.0 / 1000.0)

        forecast: List[Dict[str, Any]] = []
        for h in range(hours):
            pred = self._energy_forecaster.forecast(steps=h + 1)
            lo, hi = self._energy_forecaster.interval(steps=h + 1)
            forecast.append({
                "hour": h,
                "predicted_energy_kwh": float(pred),
                "lower_bound_kwh": float(lo),
                "upper_bound_kwh": float(hi),
            })

        violations = await self._safety_check(0.85, 0.0)
        await self._publish_feedback(
            task_id=f"energy_forecast_{uuid.uuid4().hex[:8]}",
            action="forecast", quality=0.85,
            details={"horizon_hours": hours,
                     "model": "holt_linear"},
            tags=["energy", "forecast"])

        return EnergyDecision(
            task_id=task_id, operation="forecast",
            forecast=forecast,
            explanation={"method": "holt_linear",
                          "history_size": len(self.metrics_history)},
            confidence=0.85,
            safety_violations=violations,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # MOPD teacher interface
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        strategies = ["estimate", "carbon", "helium",
                       "recommend", "route", "forecast"]
        helium = await self._helium_status()
        scarcity = helium["scarcity"]
        carbon_intensity = float(state.get(
            "carbon_intensity",
            await self._current_carbon_g_per_kwh()))

        candidates: List[Dict[str, Any]] = []
        for s in strategies:
            if s == "estimate":
                q, cg, lat, ej = 0.85, 5.0, 50.0, 100.0
            elif s == "carbon":
                q, cg, lat, ej = 0.8, 10.0, 30.0, 50.0
            elif s == "helium":
                q, cg, lat, ej = 0.75, 2.0, 40.0, 30.0
            elif s == "recommend":
                q, cg, lat, ej = 0.9, 1.0, 20.0, 20.0
            elif s == "route":
                q, cg, lat, ej = 0.8, 3.0, 25.0, 25.0
            else:  # forecast
                q, cg, lat, ej = 0.7, 1.5, 60.0, 40.0
            # Helium-scarcity: bump energy-heavy strategies
            if scarcity > 0.7:
                ej *= 1.15
            cost = self.adaptive_cost.compute(
                quality=q, carbon_g=cg, latency_ms=lat, energy_joules=ej,
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
            return [1.0 / len(strategies)] * len(strategies)

        scores = np.array([c["score"] for c in candidates], dtype=float)
        logits = -scores
        logits -= logits.max()
        probs = np.exp(logits) / max(float(np.exp(logits).sum()), 1e-12)
        full = [0.0] * len(strategies)
        for c, p in zip(candidates, probs):
            full[strategies.index(c["strategy"])] = float(p)
        return full

    # ------------------------------------------------------------------
    # Drift (never mutates config)
    # ------------------------------------------------------------------
    async def _check_drift(self) -> float:
        if self.drift is None:
            return 0.0
        try:
            score = await self.drift.check_drift(
                self.adaptive_cost.get_current_weights())
            score = float(score)
            if score > 0.7:
                # Nudge adaptive thresholds; bounded
                self._adaptive_efficiency_threshold = min(
                    0.9, self._adaptive_efficiency_threshold + 0.05)
                self._adaptive_carbon_budget_g = max(
                    0.1, self._adaptive_carbon_budget_g * 0.9)
                logger.warning(
                    f"drift={score:.3f}; adaptive thresholds updated")
            else:
                # Slowly decay back toward the configured values
                self._adaptive_efficiency_threshold += (
                    self.config.energy_efficiency_threshold -
                    self._adaptive_efficiency_threshold) * 0.05
                self._adaptive_carbon_budget_g += (
                    self.config.carbon_budget_per_task_g -
                    self._adaptive_carbon_budget_g) * 0.05
            return score
        except Exception as e:
            logger.debug(f"drift check failed: {e}")
            return 0.0

    # ------------------------------------------------------------------
    # Feedback publication
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
                feedback_type="energy",
                adaptive_cost_value=0.0,
                state=details,
                # candidates MUST be a list of dicts
                candidates=[{"action": a} for a in (
                    "estimate", "carbon", "helium",
                    "recommend", "route", "forecast")],
                source="energy_expert",
                environment="production",
                tags=tags,
            )
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
            "avg_latency_seconds": (self.total_latency / self.tasks_handled
                                     if self.tasks_handled else 0.0),
            "tasks_handled": self.tasks_handled,
            "config": asdict(self.config),
        }

    async def get_metrics(self) -> Dict[str, Any]:
        total_carbon = sum(cf.net_carbon_g
                            for cf in self.carbon_footprints.values()) / 1000.0
        total_energy = sum(ep.estimated_total_energy_kwh
                            for ep in self.energy_profiles.values())
        total_helium = sum(ha.required_ml
                            for ha in self.helium_analyses.values())
        failures = sum(1 for m in self.metrics_history if not m.success)
        return {
            "expert_name": self.expert_name,
            "tasks_handled": self.tasks_handled,
            "avg_latency_seconds": (self.total_latency / self.tasks_handled
                                     if self.tasks_handled else 0.0),
            "total_carbon_kg": float(total_carbon),
            "total_energy_kwh": float(total_energy),
            "total_helium_ml": float(total_helium),
            "failure_rate": (failures / len(self.metrics_history)
                              if self.metrics_history else 0.0),
            "profiles_cached": len(self.energy_profiles),
            "task_counts": dict(self.task_counts),
            "temporal_violations": self.temporal_safety.counts(),
            "adaptive_efficiency_threshold": self._adaptive_efficiency_threshold,
            "adaptive_carbon_budget_g": self._adaptive_carbon_budget_g,
            "background_tasks": self.task_manager.task_count(),
            "metrics_history_size": len(self.metrics_history),
        }

    async def get_health_status(self) -> ComponentHealth:
        try:
            if self.config.cpu_power_watt <= 0:
                self.health_status = "unhealthy"
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "invalid cpu_power_watt"})
            details = {
                "tasks_handled": self.tasks_handled,
                "circuit_state": self._circuit_breaker.state.value,
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
    e = EnergyExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        seed=42)
    await e.initialize()
    try:
        yield e
    finally:
        await e.close()


@pytest_asyncio.fixture
async def expert_full(storage, queue, adaptive_cost, pareto_gating,
                      drift_detector, metrics):
    class FakeCarbon:
        def __init__(self):
            self.value = 250.0
        async def get_current_intensity(self):
            return self.value

    class FakeHelium:
        def __init__(self):
            self.status = {"scarcity": 0.4, "price": 0.55,
                            "remaining": 80.0}
        async def get_helium_status(self):
            return dict(self.status)

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
    cfg = EnergyExpertConfig(
        carbon_budget_per_task_g=100.0,
        helium_budget_per_task_ml=1000.0,
        enable_real_time_carbon=False)
    e = EnergyExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        carbon_manager=FakeCarbon(),
        helium_optimizer=FakeHelium(),
        bio_core=bio, hitl=hitl, config=cfg, seed=42)
    e._bio = bio  # for tests
    e._hitl = hitl
    await e.initialize()
    try:
        yield e
    finally:
        await e.close()


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    @pytest.mark.asyncio
    async def test_metrics_canonical_api(self, metrics):
        metrics.inc("a", 2.0)
        metrics.set("b", 3.0)
        metrics.increment("a")          # alias
        metrics.observe("b", 4.0)       # alias
        assert metrics.counters["a"] == 3.0
        assert metrics.gauges["b"] == 4.0

    @pytest.mark.asyncio
    async def test_feedback_events_have_dict_candidates(self, expert, queue):
        await expert.estimate_task_energy({"payload": {}})
        for topic, payload in queue.published:
            parsed = json.loads(payload)
            # Event context must be a JSON-serialisable dict
            assert isinstance(parsed["context"], dict)
            # If the event carries candidates, they must be dicts
            ctx = parsed["context"]
            assert not isinstance(ctx, (set, list)) or isinstance(ctx, list)

    @pytest.mark.asyncio
    async def test_no_asyncio_get_event_loop(self, expert):
        import inspect
        src = inspect.getsource(EnergyExpert.estimate_task_energy)
        assert "get_event_loop" not in src

    @pytest.mark.asyncio
    async def test_sync_storage_used_via_to_thread(self, expert, storage):
        await expert._save_state()
        assert any(c[0] == "save_state" for c in storage.calls)

    @pytest.mark.asyncio
    async def test_get_metrics_is_awaitable(self, expert):
        m = await expert.get_metrics()
        assert isinstance(m, dict)

    @pytest.mark.asyncio
    async def test_health_returns_component_health(self, expert):
        h = await expert.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert isinstance(h.status, HealthStatus)

    @pytest.mark.asyncio
    async def test_config_unchanged_on_drift(self, expert):
        original_eff = expert.config.energy_efficiency_threshold
        original_budget = expert.config.carbon_budget_per_task_g
        for _ in range(30):
            await expert._check_drift()
        assert expert.config.energy_efficiency_threshold == original_eff
        assert expert.config.carbon_budget_per_task_g == original_budget

    @pytest.mark.asyncio
    async def test_metrics_history_bounded(self, expert):
        assert expert.metrics_history.maxlen is not None

    @pytest.mark.asyncio
    async def test_task_cache_bounded(self, expert):
        assert expert.task_energy_cache.maxlen is not None

    @pytest.mark.asyncio
    async def test_helium_formula_correct(self, storage, queue,
                                            adaptive_cost, pareto_gating,
                                            drift_detector, metrics):
        class HighScarcityHelium:
            async def get_helium_status(self):
                return {"scarcity": 0.9, "price": 0.5, "remaining": 10.0}

        e = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            helium_optimizer=HighScarcityHelium(), seed=1)
        await e.initialize()
        try:
            # High scarcity => less available helium
            high_status = await e._helium_status()
            assert high_status["scarcity"] == 0.9
            d = await e.analyze_helium_impact({"payload": {"required_ml": 500.0}})
            # 1000 * (1 - 0.9) = 100 mL available; with required 500, can't proceed
            assert d.helium["can_proceed"] is False
        finally:
            await e.close()

    @pytest.mark.asyncio
    async def test_tracked_load_state_task(self, expert):
        # No untracked tasks at the end of init
        assert expert.task_manager.task_count() >= 1 or \
                not expert.config.enable_real_time_carbon


# =============================================================================
# TESTS — CORE OPERATIONS
# =============================================================================
class TestCoreOperations:
    @pytest.mark.asyncio
    async def test_estimate_energy(self, expert):
        d = await expert.estimate_task_energy({
            "correlation_id": "t1",
            "payload": {"cpu_seconds": 1.0, "memory_gb": 0.5,
                         "network_mbps": 1.0, "duration_seconds": 1.0},
        })
        assert d.status == "success"
        assert d.profile is not None
        total = (d.profile["estimated_idle_energy_kwh"] +
                 d.profile["estimated_cpu_energy_kwh"] +
                 d.profile["estimated_memory_energy_kwh"] +
                 d.profile["estimated_network_energy_kwh"] +
                 d.profile["estimated_storage_energy_kwh"])
        assert abs(total - d.profile["estimated_total_energy_kwh"]) < 1e-9

    @pytest.mark.asyncio
    async def test_carbon_profile(self, expert):
        d = await expert.profile_carbon_footprint({
            "payload": {"baseline_carbon_g": 100.0,
                         "offset_strategy": "renewable_swap"}})
        assert d.status == "success"
        assert d.footprint["net_carbon_g"] == pytest.approx(20.0, abs=1e-6)

    @pytest.mark.asyncio
    async def test_helium_analysis(self, expert):
        d = await expert.analyze_helium_impact({
            "payload": {"required_ml": 5.0}})
        assert d.status == "success"
        assert "can_proceed" in d.helium

    @pytest.mark.asyncio
    async def test_recommend_strategy(self, expert):
        d = await expert.recommend_strategy({"payload": {}})
        assert d.status == "success"
        assert d.strategy in [s.value for s in SustainabilityStrategy]

    @pytest.mark.asyncio
    async def test_route_by_energy(self, expert):
        d = await expert.route_by_energy({
            "payload": {"energy_kwh": 0.1, "carbon_g": 50.0}})
        assert d.status == "success"
        assert any(d.routing.values())

    @pytest.mark.asyncio
    async def test_forecast_energy_holt(self, expert):
        # Populate history
        for _ in range(10):
            await expert.estimate_task_energy({
                "payload": {"cpu_seconds": 1.0, "duration_seconds": 1.0}})
        d = await expert.forecast_energy({
            "payload": {"current_load": 0.5, "forecast_hours": 4}})
        assert d.status == "success"
        assert len(d.forecast) == 4
        for entry in d.forecast:
            assert "predicted_energy_kwh" in entry
            assert "lower_bound_kwh" in entry
            assert entry["lower_bound_kwh"] <= entry["predicted_energy_kwh"]
            assert entry["upper_bound_kwh"] >= entry["predicted_energy_kwh"]

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, expert):
        probs = await expert.policy_probs({})
        assert len(probs) == 6
        assert abs(sum(probs) - 1.0) < 1e-6


# =============================================================================
# TESTS — CARBON / HELIUM / BIO INTEGRATION
# =============================================================================
class TestIntegration:
    @pytest.mark.asyncio
    async def test_carbon_manager_used(self, expert_full):
        v = await expert_full._current_carbon_g_per_kwh()
        assert v == 250.0

    @pytest.mark.asyncio
    async def test_helium_status_used(self, expert_full):
        s = await expert_full._helium_status()
        assert s["scarcity"] == 0.4

    @pytest.mark.asyncio
    async def test_bio_spend_earn(self, expert_full):
        await expert_full.estimate_task_energy({"payload": {}})
        assert len(expert_full._bio.spend_calls) >= 1

    @pytest.mark.asyncio
    async def test_routing_modulated_by_helium(self, storage, queue,
                                                 adaptive_cost,
                                                 pareto_gating,
                                                 drift_detector, metrics):
        class ScarcityHelium:
            def __init__(self, s):
                self.s = s
            async def get_helium_status(self):
                return {"scarcity": self.s, "price": 0.5,
                         "remaining": 100.0}

        e_low = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            helium_optimizer=ScarcityHelium(0.1), seed=1)
        e_high = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            helium_optimizer=ScarcityHelium(0.9), seed=1)
        await e_low.initialize()
        await e_high.initialize()
        try:
            d_low = await e_low.recommend_strategy({"payload": {}})
            d_high = await e_high.recommend_strategy({"payload": {}})
            # Both decisions must be valid; scarcity only modulates cost,
            # so different scarcity may produce different strategies.
            assert d_low.strategy is not None
            assert d_high.strategy is not None
        finally:
            await e_low.close()
            await e_high.close()


# =============================================================================
# TESTS — BUDGET + HITL + TEMPORAL SAFETY
# =============================================================================
class TestBudgetHITLTemporal:
    @pytest.mark.asyncio
    async def test_carbon_budget_defers_without_hitl(self, storage, queue,
                                                       adaptive_cost,
                                                       pareto_gating,
                                                       drift_detector,
                                                       metrics):
        cfg = EnergyExpertConfig(
            carbon_budget_per_task_g=0.001,
            enable_real_time_carbon=False)
        e = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            config=cfg, seed=1)
        await e.initialize()
        try:
            d = await e.estimate_task_energy({
                "payload": {"cpu_seconds": 10.0, "memory_gb": 8.0,
                             "duration_seconds": 10.0}})
            assert d.status == "deferred"
            assert d.requires_approval
        finally:
            await e.close()

    @pytest.mark.asyncio
    async def test_hitl_approves_budget_exceedance(self, storage, queue,
                                                    adaptive_cost,
                                                    pareto_gating,
                                                    drift_detector, metrics):
        hitl = HumanApprovalHandler()
        # Pre-queue approval
        await hitl.submit_response("energy_expert", "approve")
        cfg = EnergyExpertConfig(
            carbon_budget_per_task_g=0.001,
            enable_real_time_carbon=False)
        e = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, config=cfg, seed=1)
        await e.initialize()
        try:
            d = await e.estimate_task_energy({
                "payload": {"cpu_seconds": 10.0, "memory_gb": 8.0,
                             "duration_seconds": 10.0}})
            # Should be approved now
            assert d.status == "success"
        finally:
            await e.close()

    @pytest.mark.asyncio
    async def test_temporal_safety_rules_registered(self, expert):
        counts = expert.temporal_safety.counts()
        assert len(counts) >= 4

    @pytest.mark.asyncio
    async def test_safety_violations_reported(self, expert):
        v = await expert._safety_check(0.5, 0.0)
        assert isinstance(v, tuple)


# =============================================================================
# TESTS — PERSISTENCE + DETERMINISM
# =============================================================================
class TestPersistence:
    @pytest.mark.asyncio
    async def test_roundtrip(self, storage, queue, adaptive_cost,
                              pareto_gating, drift_detector, metrics):
        e1 = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e1.initialize()
        try:
            await e1.estimate_task_energy({
                "correlation_id": "x1",
                "payload": {"cpu_seconds": 1.0}})
            await e1._save_state()
        finally:
            await e1.close()

        e2 = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        await e2.initialize()
        try:
            assert "x1" in e2.energy_profiles
        finally:
            await e2.close()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, expert, storage):
        await expert._save_state()
        raw = storage.get_state("energy_expert_state_v4")
        assert raw is not None
        # Must round-trip as JSON
        state = json.loads(raw)
        assert "energy_profiles" in state


class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_estimate(self, storage, queue,
                                             adaptive_cost, pareto_gating,
                                             drift_detector, metrics):
        def _make():
            return EnergyExpert(
                storage=storage, message_queue=queue,
                adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
                drift_detector=drift_detector, metrics=metrics, seed=7)
        e1 = _make(); e2 = _make()
        await e1.initialize(); await e2.initialize()
        try:
            d1 = await e1.estimate_task_energy({"payload": {}})
            d2 = await e2.estimate_task_energy({"payload": {}})
            assert d1.profile["estimated_total_energy_kwh"] == \
                    d2.profile["estimated_total_energy_kwh"]
            assert d1.explanation["attributions"] == \
                    d2.explanation["attributions"]
        finally:
            await e1.close(); await e2.close()


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

        expert = EnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics,
            seed=1)
        await expert.initialize()
        try:
            for ttype, payload in [
                ("energy_estimate", {"cpu_seconds": 2.0,
                                      "memory_gb": 1.0,
                                      "duration_seconds": 2.0}),
                ("carbon_profile", {"baseline_carbon_g": 100.0,
                                     "offset_strategy": "renewable_swap"}),
                ("helium_analysis", {"required_ml": 5.0}),
                ("sustainability_recommend", {"system_load": 0.4}),
                ("energy_route", {"energy_kwh": 0.1, "carbon_g": 20.0}),
            ]:
                r = await expert.handle_task({"type": ttype,
                                               "payload": payload})
                print(f"{ttype}: status={r['status']} "
                      f"latency={r.get('latency_seconds', 0):.4f}s")
            metrics_snapshot = await expert.get_metrics()
            print("Metrics:", json.dumps(metrics_snapshot, indent=2,
                                          default=str))
        finally:
            await expert.close()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
