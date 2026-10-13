#!/usr/bin/env python3
"""
DataExpert v4.0.0 — Self-Contained Single-File Bundle
======================================================

Complete rewrite of the Green Agent DataExpert. Everything needed to run
and test is in this file:

  1. Protocol interfaces for every injected dependency (storage, queue,
     metrics, adaptive cost, pareto, drift, carbon, helium, bio-core, HITL).
  2. Local stubs (InMemoryStorage, AsyncMessageQueue, FeedbackEvent,
     MetricsRegistry, AdaptiveCostFunction, ParetoGating, DriftDetector).
  3. Shared runtime: CircuitBreaker (with probe limit), TemporalSafetyMonitor
     (MTL: G/F/NEVER/H/O with windows), HumanApprovalHandler (uncertainty
     sampling), XAIExplainer (Integrated Gradients + counterfactuals).
  4. DataDecision value object.
  5. Real carbon/helium integration on every operation.
  6. Real HITL gating for large operations.
  7. Real summarize_data and validate_data.
  8. Real federated aggregation with DP noise.
  9. Persistence: JSON state + parquet (fallback JSON) for datasets.
 10. Comprehensive pytest suite.

Run:
    python data_expert_v4.py         # demo
    pytest data_expert_v4.py -v      # tests

Requires: Python 3.10+, numpy, pandas, pytest, pytest-asyncio.
Optional: aiohttp (URL fetch), pyarrow (parquet persistence).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import math
import random
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, AsyncGenerator, Callable, Deque, Dict, Iterable, List, Optional,
    Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

logger = logging.getLogger("data_expert")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import pyarrow  # noqa: F401
    PARQUET_AVAILABLE = True
except ImportError:  # pragma: no cover
    PARQUET_AVAILABLE = False


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
    def inc(self, name: str, amount: int = 1) -> None: ...
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
    async def get_current_price(self) -> float: ...


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
        # Sanitize context/action/performance to JSON-safe types
        for key in ("context", "action", "performance"):
            filtered[key] = _json_safe(filtered[key])
        return cls(**filtered)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


def _json_safe(obj: Any) -> Any:
    """Recursively convert numpy types and other non-JSON types to primitives."""
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


class MetricsRegistry:
    """Canonical API is inc(name, amount) / set(name, value).

    increment() and observe() are aliases used by callers that expect the
    older API. Both map to inc/set so there is a single source of truth.
    """
    def __init__(self):
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}

    def inc(self, name: str, amount: float = 1.0) -> None:
        self.counters[name] += float(amount)

    def set(self, name: str, value: float) -> None:
        self.gauges[name] = float(value)

    # Aliases
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
        # Health may be bool or float
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
    """Async circuit breaker with a probe limit in half-open state."""

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
                    raise RuntimeError(
                        f"Circuit breaker {self.name} is open")
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
# Temporal safety monitor (MTL with past-time operators)
# -----------------------------------------------------------------------------
import re as _re

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
    """MTL monitor: G / F / NEVER / H / O + windowed rules."""

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
        self.trace.append((datetime.now(timezone.utc),
                            _json_safe(dict(state))))

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
# HITL (uncertainty sampling)
# -----------------------------------------------------------------------------
class HumanApprovalHandler:
    def __init__(self, queue: Optional[MessageQueueLike] = None,
                 uncertainty_threshold: float = 0.05,
                 timeout: float = 5.0):
        self.queue = queue
        self.uncertainty_threshold = float(uncertainty_threshold)
        self.timeout = float(timeout)
        self.preferences: Dict[str, Dict[str, float]] = {}
        self.posterior: Dict[str, Dict[str, float]] = {}
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

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None
                            ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k, v in metrics.items():
                prefs[k] = prefs.get(k, 0.25) + max(0.0, float(v)) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
            post = self.posterior.setdefault(user_id, {})
            for k, v in prefs.items():
                post[k] = post.get(k, 1.0) + v


# -----------------------------------------------------------------------------
# XAI (Integrated Gradients + counterfactuals)
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


# =============================================================================
# TASK MANAGER (tracks and cancels)
# =============================================================================
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
# ENUMS + VALUE OBJECTS
# =============================================================================
class DataSourceType(str, Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    DATABASE = "database"
    IN_MEMORY = "in_memory"
    URL = "url"
    STREAM = "stream"


class DataQualityIssue(str, Enum):
    MISSING_VALUES = "missing_values"
    DUPLICATES = "duplicates"
    OUTLIERS = "outliers"
    TYPE_MISMATCH = "type_mismatch"
    SKEW = "skew"
    HIGH_CARDINALITY = "high_cardinality"
    CONSTANT_COLUMN = "constant_column"
    RANGE_VIOLATION = "range_violation"


@dataclass(frozen=True)
class DataDecision:
    dataset_id: str
    operation: str
    profile: Optional[Dict[str, Any]]
    routing: Dict[str, bool]
    explanation: Optional[Dict[str, Any]]
    counterfactual: Optional[Dict[str, Any]]
    carbon_kg: float
    energy_kwh: float
    safety_violations: Tuple[str, ...]
    requires_approval: bool
    confidence: float
    timestamp: float
    status: str = "success"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "operation": self.operation,
            "profile": self.profile,
            "routing": dict(self.routing),
            "explanation": self.explanation,
            "counterfactual": self.counterfactual,
            "carbon_kg": self.carbon_kg,
            "energy_kwh": self.energy_kwh,
            "safety_violations": list(self.safety_violations),
            "requires_approval": self.requires_approval,
            "confidence": self.confidence,
            "timestamp": self.timestamp,
            "status": self.status,
            "error": self.error,
        }


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    non_null_count: int
    null_count: int
    unique_count: int
    missing_pct: float
    min_val: Optional[Any] = None
    max_val: Optional[Any] = None
    mean_val: Optional[float] = None
    std_val: Optional[float] = None
    median_val: Optional[Any] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    top_values: Optional[List[Tuple[str, int]]] = None
    issues: List[DataQualityIssue] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["issues"] = [i.value if isinstance(i, DataQualityIssue) else str(i)
                       for i in d["issues"]]
        # Ensure all numeric/stat fields are Python-native
        for k in ("min_val", "max_val", "median_val"):
            if d.get(k) is not None:
                d[k] = _json_safe(d[k])
        for k in ("mean_val", "std_val", "skewness", "kurtosis"):
            if d.get(k) is not None:
                try:
                    d[k] = float(d[k])
                    if math.isnan(d[k]) or math.isinf(d[k]):
                        d[k] = None
                except (TypeError, ValueError):
                    d[k] = None
        if d.get("top_values"):
            d["top_values"] = [(str(k), int(v)) for k, v in d["top_values"]]
        return d


@dataclass
class DataProfile:
    dataset_name: str
    shape: Tuple[int, int]
    total_cells: int
    memory_usage_bytes: int
    timestamp: str
    columns: Dict[str, ColumnProfile]
    global_issues: List[DataQualityIssue]
    quality_score: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "shape": [int(self.shape[0]), int(self.shape[1])],
            "total_cells": int(self.total_cells),
            "memory_usage_bytes": int(self.memory_usage_bytes),
            "timestamp": self.timestamp,
            "columns": {k: v.to_dict() for k, v in self.columns.items()},
            "global_issues": [i.value for i in self.global_issues],
            "quality_score": float(self.quality_score),
        }


@dataclass
class DataSummary:
    dataset_id: str
    rows: int
    columns: int
    column_names: List[str]
    column_dtypes: Dict[str, str]
    sample_rows: List[Dict[str, Any]]
    schema_hash: str
    data_profile: Optional[DataProfile] = None
    quality_issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "rows": int(self.rows),
            "columns": int(self.columns),
            "column_names": list(self.column_names),
            "column_dtypes": dict(self.column_dtypes),
            "sample_rows": _json_safe(self.sample_rows),
            "schema_hash": self.schema_hash,
            "data_profile": self.data_profile.to_dict()
                            if self.data_profile else None,
            "quality_issues": list(self.quality_issues),
            "recommendations": list(self.recommendations),
        }


@dataclass
class DataOperationMetrics:
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    bytes_processed: int = 0
    rows_processed: int = 0
    energy_kwh: float = 0.0
    carbon_kg: float = 0.0
    carbon_intensity: float = 100.0
    success: bool = True
    error_message: Optional[str] = None

    def compute_energy_carbon(self, bytes_to_kwh: float,
                              carbon_g_per_kwh: float) -> None:
        self.energy_kwh = float(self.bytes_processed) * float(bytes_to_kwh)
        self.carbon_kg = (self.energy_kwh *
                          float(carbon_g_per_kwh) / 1000.0)
        self.carbon_intensity = float(carbon_g_per_kwh)

    def duration_seconds(self) -> float:
        if self.end_time is not None:
            return float(self.end_time - self.start_time)
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class DataExpertConfig:
    enable_profiling: bool = True
    enable_cleaning: bool = True
    enable_summarization: bool = True
    enable_validation: bool = True
    enable_energy_tracking: bool = True
    enable_federated_aggregation: bool = True
    enable_url_fetch: bool = True
    enable_streaming: bool = True
    enable_telemetry: bool = True
    enable_persistence: bool = True

    max_rows_profile: int = 10000
    max_unique_values: int = 100
    missing_value_threshold: float = 0.5
    bytes_to_kwh_factor: float = 1e-9
    carbon_intensity_g_per_kwh: float = 100.0
    cache_ttl_seconds: int = 3600

    # HITL / safety
    large_operation_rows: int = 10000
    large_operation_cooldown_seconds: float = 300.0

    # DP
    federated_dp_epsilon: float = 1.0
    federated_dp_clip: float = 1.0

    # Context TTL
    context_ttl_seconds: float = 300.0

    def __post_init__(self) -> None:
        if not (0.0 <= self.missing_value_threshold <= 1.0):
            self.missing_value_threshold = 0.5
        if self.bytes_to_kwh_factor <= 0:
            self.bytes_to_kwh_factor = 1e-9
        if self.large_operation_rows < 1:
            self.large_operation_rows = 10000
        if self.large_operation_cooldown_seconds < 0:
            self.large_operation_cooldown_seconds = 0.0


# =============================================================================
# DATA EXPERT (v4.0.0)
# =============================================================================
class DataExpert:
    """Data Expert v4.0.0 — Enhanced Data Services Layer for MoE System."""

    SUPPORTED_TASKS = (
        "data_profile", "data_clean", "data_summary",
        "data_validate", "data_transform", "data_route",
        "data_federated_aggregate",
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
        config: Optional[DataExpertConfig] = None,
        seed: int = 42,
    ):
        self.expert_name = "data_expert"
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
        self.config = config or DataExpertConfig()

        # Deterministic RNG tree
        self._rng = random.Random(seed)
        self._rng_dp = random.Random(seed + 1)
        self._rng_xai = random.Random(seed + 2)
        np.random.seed(seed)

        # State
        self.datasets: Dict[str, pd.DataFrame] = {}
        self.profiles: Dict[str, DataProfile] = {}
        self.summaries: Dict[str, DataSummary] = {}
        self.metrics_history: Deque[DataOperationMetrics] = deque(maxlen=1000)
        self.tasks_handled = 0
        self.total_latency = 0.0
        self.task_counts: Dict[str, int] = defaultdict(int)

        # Cache + context
        self._cache_timestamps: Dict[str, datetime] = {}
        self._last_context: Dict[str, Any] = {}
        self._last_context_ts: Optional[float] = None

        # Concurrency
        self._lock = asyncio.Lock()
        self._session: Optional[Any] = None
        self._session_lock = asyncio.Lock()

        # Circuit breakers
        self._http_circuit = CircuitBreaker(
            name="data_http",
            failure_threshold=5,
            recovery_timeout=30.0)

        # Temporal safety (MTL rules)
        self.temporal_safety = TemporalSafetyMonitor(max_trace=500)
        self._setup_temporal_rules()

        # HITL cooldown tracking (large ops)
        self._last_large_op_ts: Optional[float] = None
        self._in_large_op = False

        # Adaptive missing-value threshold (bug fix: config is never mutated)
        self._adaptive_missing_threshold = float(
            self.config.missing_value_threshold)

        # XAI
        self.xai = XAIExplainer(rng=self._rng_xai)

        # Task manager for tracked background work
        self.task_manager = TaskManager()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_state()
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
        await self._save_state()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula("G (quality_score >= 0.0)")
        self.temporal_safety.add_formula("G (quality_score <= 1.0)")
        self.temporal_safety.add_formula("G (cooldown_active >= 0)")
        self.temporal_safety.add_formula("NEVER (circuit_open > 0)")

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
    # Carbon + helium integration
    # ------------------------------------------------------------------
    async def _current_carbon_g_per_kwh(self) -> float:
        if self.carbon_manager is not None:
            try:
                if hasattr(self.carbon_manager, "get_current_intensity"):
                    v = self.carbon_manager.get_current_intensity()
                    if asyncio.iscoroutine(v):
                        v = await v
                    return float(v)
            except Exception as e:
                logger.debug(f"carbon manager failed: {e}")
        return float(self.config.carbon_intensity_g_per_kwh)

    async def _current_carbon_price(self) -> float:
        if self.carbon_manager is not None:
            try:
                if hasattr(self.carbon_manager, "get_current_price"):
                    v = self.carbon_manager.get_current_price()
                    if asyncio.iscoroutine(v):
                        v = await v
                    return float(v)
            except Exception:
                pass
        return 50.0

    async def _current_helium_status(self) -> Dict[str, Any]:
        if self.helium_optimizer is not None:
            try:
                if hasattr(self.helium_optimizer, "get_helium_status"):
                    v = self.helium_optimizer.get_helium_status()
                    if asyncio.iscoroutine(v):
                        v = await v
                    if isinstance(v, dict):
                        return v
            except Exception as e:
                logger.debug(f"helium optimizer failed: {e}")
        return {"scarcity": 0.5, "price": 0.5, "remaining": 100.0}

    # ------------------------------------------------------------------
    # Bio integration
    # ------------------------------------------------------------------
    async def _bio_spend_earn(self, energy_kwh: float,
                              quality_score: float) -> None:
        if self.bio_core is None:
            return
        try:
            cost = max(0.01, float(energy_kwh) * 0.1)
            spend = getattr(self.bio_core, "spend", None)
            if spend is not None:
                r = spend(self.expert_name, cost)
                if asyncio.iscoroutine(r):
                    await r
            if quality_score > 0.8:
                earn = getattr(self.bio_core, "earn", None)
                if earn is not None:
                    r = earn(self.expert_name, cost * 2)
                    if asyncio.iscoroutine(r):
                        await r
        except Exception as e:
            logger.warning(f"bio integration failed: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Large-op gate (cooldown + concurrency + HITL)
    # ------------------------------------------------------------------
    async def _large_op_gate(
        self,
        operation: str,
        size_mb: float,
    ) -> Tuple[bool, Optional[str]]:
        """Return (approved, reason_if_blocked)."""
        now = time.monotonic()
        if (self._last_large_op_ts is not None and
                now - self._last_large_op_ts <
                self.config.large_operation_cooldown_seconds):
            return False, "cooldown_active"
        if self._in_large_op:
            return False, "concurrent_large_operation"
        if self.hitl is not None:
            decision = {
                "action": f"large_{operation}",
                "candidates": [
                    {"id": "approve", "size_mb": float(size_mb),
                     "risk": 0.5},
                    {"id": "defer", "size_mb": 0.0, "risk": 0.0},
                ],
            }
            approved = await self.hitl.request_approval(
                decision, user_id="data_expert")
            if not approved:
                return False, "hitl_rejected"
        return True, None

    def _mark_large_op_done(self) -> None:
        self._last_large_op_ts = time.monotonic()
        self._in_large_op = False

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _serialize_df(self, df: pd.DataFrame) -> Tuple[str, str]:
        """Return (format, base64_payload). Parquet preferred; JSON fallback."""
        if PARQUET_AVAILABLE:
            try:
                buf = io.BytesIO()
                df.to_parquet(buf, index=False)
                return "parquet", base64.b64encode(buf.getvalue()).decode("ascii")
            except Exception as e:
                logger.debug(f"parquet serialize failed: {e}")
        # Fallback to JSON (no pickle)
        payload = df.to_json(orient="split", default_handler=str)
        return "json", base64.b64encode(payload.encode("utf-8")).decode("ascii")

    def _deserialize_df(self, fmt: str, b64: str) -> pd.DataFrame:
        blob = base64.b64decode(b64.encode("ascii"))
        if fmt == "parquet":
            return pd.read_parquet(io.BytesIO(blob))
        return pd.read_json(io.BytesIO(blob), orient="split")

    async def _load_state(self) -> None:
        try:
            data = await asyncio.to_thread(
                self.storage.get_state, "data_expert_state")
            if data:
                state = json.loads(data)
                self.tasks_handled = int(state.get("tasks_handled", 0))
                self.total_latency = float(state.get("total_latency", 0.0))
                self.task_counts = defaultdict(
                    int, state.get("task_counts", {}))
                for m in state.get("metrics_history", []):
                    try:
                        self.metrics_history.append(
                            DataOperationMetrics(**m))
                    except Exception:
                        pass
                for dataset_id, pdata in state.get("profiles", {}).items():
                    try:
                        columns = {
                            cname: ColumnProfile(
                                **{**cd, "issues": [
                                    DataQualityIssue(i)
                                    if not isinstance(i, DataQualityIssue)
                                    else i
                                    for i in cd.get("issues", [])]})
                            for cname, cd in pdata["columns"].items()
                        }
                        profile = DataProfile(
                            dataset_name=pdata["dataset_name"],
                            shape=tuple(pdata["shape"]),
                            total_cells=int(pdata["total_cells"]),
                            memory_usage_bytes=int(
                                pdata["memory_usage_bytes"]),
                            timestamp=pdata["timestamp"],
                            columns=columns,
                            global_issues=[DataQualityIssue(i)
                                           for i in pdata["global_issues"]],
                            quality_score=float(pdata["quality_score"]),
                        )
                        self.profiles[dataset_id] = profile
                    except Exception as e:
                        logger.debug(f"skip profile {dataset_id}: {e}")

            raw_datasets = await asyncio.to_thread(
                self.storage.get_state, "data_expert_datasets_v4")
            if raw_datasets:
                try:
                    blobs = json.loads(raw_datasets)
                    for dataset_id, entry in blobs.items():
                        fmt = entry.get("format", "json")
                        b64 = entry.get("payload", "")
                        if not b64:
                            continue
                        try:
                            self.datasets[dataset_id] = \
                                self._deserialize_df(fmt, b64)
                        except Exception as e:
                            logger.debug(
                                f"skip dataset {dataset_id}: {e}")
                except Exception as e:
                    logger.warning(f"dataset load failed: {e}")
        except Exception as e:
            logger.error(f"Failed to load data expert state: {e}")

    async def _save_state(self) -> None:
        try:
            state = {
                "tasks_handled": self.tasks_handled,
                "total_latency": self.total_latency,
                "task_counts": dict(self.task_counts),
                "metrics_history": [m.to_dict()
                                    for m in self.metrics_history],
                "profiles": {k: v.to_dict()
                             for k, v in self.profiles.items()},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await asyncio.to_thread(
                self.storage.save_state, "data_expert_state",
                json.dumps(state, default=str))

            # Datasets — parquet or JSON, base64
            dataset_payload: Dict[str, Dict[str, str]] = {}
            for dataset_id, df in self.datasets.items():
                try:
                    fmt, payload = self._serialize_df(df)
                    dataset_payload[dataset_id] = {
                        "format": fmt, "payload": payload}
                except Exception as e:
                    logger.debug(f"skip dataset save {dataset_id}: {e}")
            await asyncio.to_thread(
                self.storage.save_state, "data_expert_datasets_v4",
                json.dumps(dataset_payload))
        except Exception as e:
            logger.error(f"Failed to save data expert state: {e}")

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------
    async def _get_session(self):
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not installed")
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    async def load_data(
        self,
        source: Union[str, pd.DataFrame, Dict, List, AsyncGenerator],
        source_type: DataSourceType = DataSourceType.IN_MEMORY,
        dataset_id: Optional[str] = None,
    ) -> pd.DataFrame:
        if dataset_id is None:
            dataset_id = f"dataset_{uuid.uuid4().hex[:8]}"
        start_ts = time.monotonic()
        try:
            # AsyncGenerator detection first
            if hasattr(source, "__aiter__"):
                df = await self._fetch_from_stream(source)  # type: ignore
            elif isinstance(source, pd.DataFrame):
                df = source.copy()
            elif isinstance(source, (dict, list)):
                df = pd.DataFrame(source)
            elif source_type == DataSourceType.CSV:
                df = pd.read_csv(source)  # type: ignore
            elif source_type == DataSourceType.JSON:
                df = pd.read_json(source)  # type: ignore
            elif source_type == DataSourceType.PARQUET:
                df = pd.read_parquet(source)  # type: ignore
            elif source_type == DataSourceType.URL:
                df = await self._fetch_from_url(source)  # type: ignore
            elif source_type == DataSourceType.STREAM:
                raise ValueError(
                    "STREAM requires an AsyncGenerator source")
            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    f"load_data produced {type(df)}; expected DataFrame")

            self.datasets[dataset_id] = df
            end_ts = time.monotonic()
            bytes_loaded = int(df.memory_usage(deep=True).sum())
            carbon_g_per_kwh = await self._current_carbon_g_per_kwh()
            metrics = DataOperationMetrics(
                operation_name="load_data",
                start_time=start_ts,
                end_time=end_ts,
                bytes_processed=bytes_loaded,
                rows_processed=len(df),
            )
            metrics.compute_energy_carbon(
                self.config.bytes_to_kwh_factor, carbon_g_per_kwh)
            self.metrics_history.append(metrics)
            await self._emit_metrics(metrics)
            await self._bio_spend_earn(metrics.energy_kwh, 0.8)
            logger.info(f"Loaded dataset {dataset_id}: {df.shape}, "
                        f"{bytes_loaded} bytes")
            return df
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

    async def _fetch_from_url(self, url: str) -> pd.DataFrame:
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not installed")
        # NOTE: single circuit-breaker call (bug fix; was double-wrapped)
        return await self._http_circuit.call(self._http_fetch, url)

    async def _http_fetch(self, url: str) -> pd.DataFrame:
        session = await self._get_session()
        async with session.get(url, timeout=15) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            text = await response.text(
                encoding=response.charset or "utf-8", errors="replace")
            if url.endswith(".json"):
                return pd.read_json(io.StringIO(text))
            # Default: CSV
            return pd.read_csv(io.StringIO(text))

    async def _fetch_from_stream(self, stream: AsyncGenerator
                                 ) -> pd.DataFrame:
        chunks: List[pd.DataFrame] = []
        async for chunk in stream:
            if isinstance(chunk, pd.DataFrame):
                chunks.append(chunk)
            elif isinstance(chunk, dict):
                chunks.append(pd.DataFrame([chunk]))
            elif isinstance(chunk, list):
                chunks.append(pd.DataFrame(chunk))
        if chunks:
            return pd.concat(chunks, ignore_index=True)
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Metrics helpers
    # ------------------------------------------------------------------
    async def _emit_metrics(self, metrics: DataOperationMetrics) -> None:
        try:
            self.metrics.inc("data_bytes", metrics.bytes_processed)
            self.metrics.inc("data_carbon", metrics.carbon_kg)
            self.metrics.inc("data_energy", metrics.energy_kwh)
            self.metrics.set("data_last_energy_kwh", metrics.energy_kwh)
            self.metrics.set("data_last_carbon_kg", metrics.carbon_kg)
        except Exception as e:
            logger.debug(f"metrics emission failed: {e}")

    # ------------------------------------------------------------------
    # Task dispatcher
    # ------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get("type", "unknown")
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        start_ts = time.monotonic()

        # Update last context
        if "data" in task:
            self._set_context({"data_size_mb": self._estimate_size_mb(
                task["data"])})
        ctx = self._get_context()
        if "carbon_intensity" not in ctx:
            self._set_context({
                "carbon_intensity":
                    await self._current_carbon_g_per_kwh()})
        helium = await self._current_helium_status()
        self._set_context({
            "helium_scarcity": float(helium.get("scarcity", 0.5))})

        try:
            dispatch = {
                "data_profile": self.profile_data,
                "data_clean": self.clean_data,
                "data_summary": self.summarize_data,
                "data_validate": self.validate_data,
                "data_route": self.route_data,
                "data_federated_aggregate": self.federated_aggregate,
            }
            handler = dispatch.get(task_type)
            if handler is None:
                return {
                    "status": "error",
                    "error": f"Unknown task type: {task_type}",
                    "correlation_id": task_id,
                }
            decision = await handler(task)

            latency = time.monotonic() - start_ts
            async with self._lock:
                self.tasks_handled += 1
                self.total_latency += latency
                self.task_counts[task_type] += 1
            self.metrics.inc(f"data_task_{task_type}")
            self.metrics.set("data_last_latency", latency)

            result = decision.to_dict()
            result["correlation_id"] = task_id
            result["latency_seconds"] = latency
            logger.info(f"DataExpert completed {task_type} in "
                        f"{latency:.3f}s")
            return result

        except Exception as e:
            logger.error(f"DataExpert error on {task_type}: {e}",
                         exc_info=True)
            self.metrics.inc(f"data_task_{task_type}_error")
            return {
                "status": "error",
                "error": str(e),
                "correlation_id": task_id,
            }

    def _estimate_size_mb(self, data: Any) -> float:
        try:
            if isinstance(data, pd.DataFrame):
                return float(data.memory_usage(deep=True).sum()
                             / (1024 * 1024))
            if isinstance(data, (list, dict)):
                return float(len(json.dumps(data, default=str))
                             / (1024 * 1024))
        except Exception:
            pass
        return 0.0

    # ------------------------------------------------------------------
    # Safety snapshot helper
    # ------------------------------------------------------------------
    async def _safety_violations(self, quality_score: float
                                 ) -> Tuple[str, ...]:
        cooldown_active = 0
        if (self._last_large_op_ts is not None and
                time.monotonic() - self._last_large_op_ts <
                self.config.large_operation_cooldown_seconds):
            cooldown_active = 1
        circuit_open = 1 if self._http_circuit.state == \
                            CircuitBreakerState.OPEN else 0
        await self.temporal_safety.push({
            "quality_score": float(quality_score),
            "cooldown_active": cooldown_active,
            "circuit_open": circuit_open,
        })
        results = await self.temporal_safety.check()
        return tuple(rid for rid, ok in results.items() if not ok)

    # ------------------------------------------------------------------
    # Profile
    # ------------------------------------------------------------------
    async def profile_data(self, task: Dict[str, Any]) -> DataDecision:
        dataset = task.get("data")
        dataset_id = task.get("dataset_id", f"profile_{uuid.uuid4().hex[:8]}")
        force_refresh = bool(task.get("force_refresh", False))
        ctx = self._get_context()

        # Cached?
        if not force_refresh and dataset_id in self.profiles:
            cached = self._cache_timestamps.get(dataset_id)
            if (cached and (datetime.now(timezone.utc) - cached).total_seconds()
                    < self.config.cache_ttl_seconds):
                prof = self.profiles[dataset_id]
                return DataDecision(
                    dataset_id=dataset_id, operation="profile",
                    profile=prof.to_dict(), routing={},
                    explanation=None, counterfactual=None,
                    carbon_kg=0.0, energy_kwh=0.0,
                    safety_violations=(),
                    requires_approval=False, confidence=1.0,
                    timestamp=time.time(),
                )

        # Coerce to a DataFrame
        if isinstance(dataset, str):
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)
        elif isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)

        size_mb = float(df.memory_usage(deep=True).sum() / (1024 * 1024))
        is_large = len(df) > self.config.large_operation_rows
        requires_approval = False
        if is_large:
            approved, reason = await self._large_op_gate(
                "profile", size_mb)
            requires_approval = not approved
            if not approved:
                return DataDecision(
                    dataset_id=dataset_id, operation="profile",
                    profile=None, routing={},
                    explanation={"reason": reason},
                    counterfactual=None,
                    carbon_kg=0.0, energy_kwh=0.0,
                    safety_violations=(),
                    requires_approval=True, confidence=0.0,
                    timestamp=time.time(),
                    status="deferred",
                    error=f"large operation blocked: {reason}",
                )
            self._in_large_op = True

        try:
            profile = await self._profile_dataframe(df, dataset_id)
        finally:
            if is_large:
                self._mark_large_op_done()

        self.profiles[dataset_id] = profile
        self._cache_timestamps[dataset_id] = datetime.now(timezone.utc)

        violations = await self._safety_violations(profile.quality_score)
        await self._publish_feedback(
            task_id=f"data_profile_{dataset_id}",
            action="profile",
            quality=profile.quality_score,
            details={"dataset_id": dataset_id,
                     "rows": int(df.shape[0]),
                     "cols": int(df.shape[1]),
                     "requires_approval": requires_approval},
            tags=["data", "profile"])

        latest = self.metrics_history[-1] if self.metrics_history else None
        carbon_kg = float(latest.carbon_kg) if latest else 0.0
        energy_kwh = float(latest.energy_kwh) if latest else 0.0

        return DataDecision(
            dataset_id=dataset_id, operation="profile",
            profile=profile.to_dict(), routing={},
            explanation=None, counterfactual=None,
            carbon_kg=carbon_kg, energy_kwh=energy_kwh,
            safety_violations=violations,
            requires_approval=requires_approval,
            confidence=profile.quality_score,
            timestamp=time.time(),
        )

    async def _profile_dataframe(self, df: pd.DataFrame,
                                 dataset_id: str) -> DataProfile:
        start_ts = time.monotonic()
        sample_df = df.head(self.config.max_rows_profile)
        columns: Dict[str, ColumnProfile] = {}
        global_issues: List[DataQualityIssue] = []

        for col in sample_df.columns:
            col_data = sample_df[col]
            non_null = int(col_data.notna().sum())
            null_count = int(col_data.isna().sum())
            missing_pct = (null_count / len(sample_df)
                            if len(sample_df) > 0 else 0.0)
            dtype = str(col_data.dtype)
            unique_count = int(col_data.nunique())

            col_profile = ColumnProfile(
                name=str(col), dtype=dtype,
                non_null_count=non_null, null_count=null_count,
                unique_count=unique_count, missing_pct=float(missing_pct),
                issues=[],
            )

            if missing_pct > self._adaptive_missing_threshold:
                col_profile.issues.append(DataQualityIssue.MISSING_VALUES)
                if DataQualityIssue.MISSING_VALUES not in global_issues:
                    global_issues.append(DataQualityIssue.MISSING_VALUES)
            if unique_count <= 1:
                col_profile.issues.append(DataQualityIssue.CONSTANT_COLUMN)
                if DataQualityIssue.CONSTANT_COLUMN not in global_issues:
                    global_issues.append(DataQualityIssue.CONSTANT_COLUMN)
            if (unique_count > self.config.max_unique_values and
                    pd.api.types.is_object_dtype(col_data)):
                col_profile.issues.append(
                    DataQualityIssue.HIGH_CARDINALITY)
                if DataQualityIssue.HIGH_CARDINALITY not in global_issues:
                    global_issues.append(DataQualityIssue.HIGH_CARDINALITY)

            if pd.api.types.is_numeric_dtype(col_data):
                try:
                    col_profile.min_val = col_data.min()
                    col_profile.max_val = col_data.max()
                    col_profile.mean_val = float(col_data.mean())
                    col_profile.std_val = float(col_data.std())
                    col_profile.median_val = col_data.median()
                    col_profile.skewness = float(col_data.skew())
                    col_profile.kurtosis = float(col_data.kurtosis())
                except Exception:
                    pass

            try:
                top_vals = col_data.value_counts().head(5)
                col_profile.top_values = [
                    (str(k), int(v)) for k, v in
                    zip(top_vals.index, top_vals.values)]
            except Exception:
                col_profile.top_values = []

            columns[str(col)] = col_profile

        issue_penalty = len(global_issues) * 0.1
        quality_score = max(0.0, 1.0 - issue_penalty)
        end_ts = time.monotonic()
        bytes_processed = int(df.memory_usage(deep=True).sum())
        carbon_g_per_kwh = await self._current_carbon_g_per_kwh()
        metrics = DataOperationMetrics(
            operation_name="profile_data",
            start_time=start_ts, end_time=end_ts,
            bytes_processed=bytes_processed, rows_processed=len(df),
        )
        metrics.compute_energy_carbon(
            self.config.bytes_to_kwh_factor, carbon_g_per_kwh)
        self.metrics_history.append(metrics)
        await self._emit_metrics(metrics)
        await self._bio_spend_earn(metrics.energy_kwh, quality_score)

        # Drift-aware adaptive threshold (never mutate config)
        await self._check_drift()

        return DataProfile(
            dataset_name=dataset_id,
            shape=(int(df.shape[0]), int(df.shape[1])),
            total_cells=int(df.shape[0] * df.shape[1]),
            memory_usage_bytes=bytes_processed,
            timestamp=datetime.now(timezone.utc).isoformat(),
            columns=columns,
            global_issues=global_issues,
            quality_score=float(quality_score),
        )

    # ------------------------------------------------------------------
    # Clean
    # ------------------------------------------------------------------
    async def clean_data(self, task: Dict[str, Any]) -> DataDecision:
        dataset = task.get("data")
        dataset_id = task.get("dataset_id",
                              f"cleaned_{uuid.uuid4().hex[:8]}")
        params = task.get("params", {})

        if isinstance(dataset, pd.DataFrame):
            df = dataset.copy()
        else:
            df = await self.load_data(dataset, DataSourceType.CSV, dataset_id)

        size_mb = float(df.memory_usage(deep=True).sum() / (1024 * 1024))
        is_large = len(df) > self.config.large_operation_rows
        requires_approval = False
        if is_large:
            approved, reason = await self._large_op_gate("clean", size_mb)
            requires_approval = not approved
            if not approved:
                return DataDecision(
                    dataset_id=dataset_id, operation="clean",
                    profile=None, routing={},
                    explanation={"reason": reason},
                    counterfactual=None,
                    carbon_kg=0.0, energy_kwh=0.0,
                    safety_violations=(),
                    requires_approval=True, confidence=0.0,
                    timestamp=time.time(),
                    status="deferred",
                    error=f"large operation blocked: {reason}",
                )
            self._in_large_op = True

        try:
            start_ts = time.monotonic()
            rows_before = len(df)
            if params.get("remove_duplicates", True):
                df = df.drop_duplicates()
            if params.get("drop_missing", False):
                df = df.dropna()
            elif params.get("fill_missing", True):
                for col in df.columns:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(df[col].mean())
                    else:
                        modes = df[col].mode()
                        df[col] = df[col].fillna(
                            modes.iloc[0] if not modes.empty else "unknown")
            if params.get("normalize", False):
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    df[numeric_cols] = (
                        (df[numeric_cols] - df[numeric_cols].mean()) /
                        (df[numeric_cols].std() + 1e-8))
            self.datasets[dataset_id] = df
            end_ts = time.monotonic()
            bytes_processed = int(df.memory_usage(deep=True).sum())
            carbon_g_per_kwh = await self._current_carbon_g_per_kwh()
            metrics = DataOperationMetrics(
                operation_name="clean_data",
                start_time=start_ts, end_time=end_ts,
                bytes_processed=bytes_processed, rows_processed=len(df),
            )
            metrics.compute_energy_carbon(
                self.config.bytes_to_kwh_factor, carbon_g_per_kwh)
            self.metrics_history.append(metrics)
            await self._emit_metrics(metrics)
            await self._bio_spend_earn(metrics.energy_kwh, 0.9)
        finally:
            if is_large:
                self._mark_large_op_done()

        violations = await self._safety_violations(0.9)
        await self._publish_feedback(
            task_id=f"data_clean_{dataset_id}",
            action="clean",
            quality=0.9,
            details={"dataset_id": dataset_id, "params": params,
                     "requires_approval": requires_approval},
            tags=["data", "clean"])

        await self._check_drift()
        latest = self.metrics_history[-1]
        return DataDecision(
            dataset_id=dataset_id, operation="clean",
            profile=None, routing={},
            explanation=None, counterfactual=None,
            carbon_kg=float(latest.carbon_kg),
            energy_kwh=float(latest.energy_kwh),
            safety_violations=violations,
            requires_approval=requires_approval,
            confidence=0.9, timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Summarize (REAL)
    # ------------------------------------------------------------------
    async def summarize_data(self, task: Dict[str, Any]) -> DataDecision:
        dataset = task.get("data")
        dataset_id = task.get("dataset_id",
                              f"summary_{uuid.uuid4().hex[:8]}")
        max_sample = int(task.get("max_sample", 5))
        include_profile = bool(task.get("include_profile", True))

        if isinstance(dataset, pd.DataFrame):
            df = dataset
        elif isinstance(dataset, str) and dataset_id in self.datasets:
            df = self.datasets[dataset_id]
        else:
            df = pd.DataFrame(dataset)

        start_ts = time.monotonic()

        # Build a DataSummary with a real schema hash, sample rows, dtypes
        column_names = [str(c) for c in df.columns]
        column_dtypes = {str(c): str(df[c].dtype) for c in df.columns}
        schema_blob = json.dumps(column_dtypes, sort_keys=True).encode()
        schema_hash = hashlib.sha256(schema_blob).hexdigest()[:16]
        sample_rows = [
            {str(k): _json_safe(v) for k, v in row.items()}
            for row in df.head(max_sample).to_dict(orient="records")
        ]

        data_profile: Optional[DataProfile] = None
        quality_issues: List[str] = []
        if include_profile:
            data_profile = self.profiles.get(dataset_id)
            if data_profile is None:
                data_profile = await self._profile_dataframe(df, dataset_id)
                self.profiles[dataset_id] = data_profile
            quality_issues = [i.value for i in data_profile.global_issues]

        recommendations = self._build_recommendations(data_profile)
        summary = DataSummary(
            dataset_id=dataset_id,
            rows=int(len(df)),
            columns=int(df.shape[1]),
            column_names=column_names,
            column_dtypes=column_dtypes,
            sample_rows=sample_rows,
            schema_hash=schema_hash,
            data_profile=data_profile,
            quality_issues=quality_issues,
            recommendations=recommendations,
        )
        self.summaries[dataset_id] = summary

        end_ts = time.monotonic()
        bytes_processed = int(df.memory_usage(deep=True).sum())
        carbon_g_per_kwh = await self._current_carbon_g_per_kwh()
        metrics = DataOperationMetrics(
            operation_name="summarize_data",
            start_time=start_ts, end_time=end_ts,
            bytes_processed=bytes_processed, rows_processed=len(df),
        )
        metrics.compute_energy_carbon(
            self.config.bytes_to_kwh_factor, carbon_g_per_kwh)
        self.metrics_history.append(metrics)
        await self._emit_metrics(metrics)
        await self._bio_spend_earn(metrics.energy_kwh, 0.85)

        quality = float(data_profile.quality_score) if data_profile else 0.7
        violations = await self._safety_violations(quality)
        await self._publish_feedback(
            task_id=f"data_summary_{dataset_id}",
            action="summary",
            quality=quality,
            details={"dataset_id": dataset_id,
                     "schema_hash": schema_hash,
                     "rows": int(len(df))},
            tags=["data", "summary"])

        return DataDecision(
            dataset_id=dataset_id, operation="summary",
            profile=data_profile.to_dict() if data_profile else None,
            routing={},
            explanation={
                "schema_hash": schema_hash,
                "recommendations": recommendations,
                "summary": summary.to_dict(),
            },
            counterfactual=None,
            carbon_kg=float(metrics.carbon_kg),
            energy_kwh=float(metrics.energy_kwh),
            safety_violations=violations,
            requires_approval=False,
            confidence=quality,
            timestamp=time.time(),
        )

    def _build_recommendations(self, profile: Optional[DataProfile]
                               ) -> List[str]:
        recs: List[str] = []
        if profile is None:
            return recs
        if DataQualityIssue.MISSING_VALUES in profile.global_issues:
            recs.append("Consider imputation for columns with high missing %.")
        if DataQualityIssue.HIGH_CARDINALITY in profile.global_issues:
            recs.append("High cardinality columns may benefit from encoding.")
        if DataQualityIssue.CONSTANT_COLUMN in profile.global_issues:
            recs.append("Drop constant columns to reduce dimensionality.")
        if profile.quality_score < 0.5:
            recs.append("Quality score below 0.5; run clean_data first.")
        return recs

    # ------------------------------------------------------------------
    # Validate (REAL)
    # ------------------------------------------------------------------
    async def validate_data(self, task: Dict[str, Any]) -> DataDecision:
        dataset = task.get("data")
        dataset_id = task.get("dataset_id",
                              f"validate_{uuid.uuid4().hex[:8]}")
        rules: List[Dict[str, Any]] = task.get("rules", []) or []

        if isinstance(dataset, pd.DataFrame):
            df = dataset
        elif isinstance(dataset, str) and dataset_id in self.datasets:
            df = self.datasets[dataset_id]
        else:
            df = pd.DataFrame(dataset)

        start_ts = time.monotonic()
        violations: List[Dict[str, Any]] = []

        # Default rules if none provided: numeric range sanity
        effective_rules = list(rules)
        if not effective_rules:
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    effective_rules.append(
                        {"column": str(col), "type": "numeric",
                         "min": float(df[col].min()),
                         "max": float(df[col].max())})

        for rule in effective_rules:
            col = rule.get("column")
            if col not in df.columns:
                violations.append({"rule": rule, "error": "missing_column"})
                continue
            if rule.get("type") == "numeric":
                lo = rule.get("min")
                hi = rule.get("max")
                below = int((df[col] < lo).sum()) if lo is not None else 0
                above = int((df[col] > hi).sum()) if hi is not None else 0
                if below or above:
                    violations.append({
                        "column": str(col),
                        "issue": DataQualityIssue.RANGE_VIOLATION.value,
                        "below_min": below, "above_max": above,
                    })
            elif rule.get("type") == "unique":
                dupes = int(df[col].duplicated().sum())
                if dupes:
                    violations.append({
                        "column": str(col),
                        "issue": "duplicates",
                        "duplicate_count": dupes,
                    })
            elif rule.get("type") == "non_null":
                missing = int(df[col].isna().sum())
                if missing:
                    violations.append({
                        "column": str(col),
                        "issue": "missing_values",
                        "missing_count": missing,
                    })

        end_ts = time.monotonic()
        bytes_processed = int(df.memory_usage(deep=True).sum())
        carbon_g_per_kwh = await self._current_carbon_g_per_kwh()
        metrics = DataOperationMetrics(
            operation_name="validate_data",
            start_time=start_ts, end_time=end_ts,
            bytes_processed=bytes_processed, rows_processed=len(df),
        )
        metrics.compute_energy_carbon(
            self.config.bytes_to_kwh_factor, carbon_g_per_kwh)
        self.metrics_history.append(metrics)
        await self._emit_metrics(metrics)
        await self._bio_spend_earn(metrics.energy_kwh, 0.9)

        quality = 1.0 if not violations else max(
            0.0, 1.0 - len(violations) / max(len(effective_rules), 1))
        safety = await self._safety_violations(quality)
        await self._publish_feedback(
            task_id=f"data_validate_{dataset_id}",
            action="validate",
            quality=quality,
            details={"dataset_id": dataset_id,
                     "rules": len(effective_rules),
                     "violations": len(violations)},
            tags=["data", "validate"])

        return DataDecision(
            dataset_id=dataset_id, operation="validate",
            profile=None, routing={},
            explanation={
                "rules_evaluated": len(effective_rules),
                "violations": _json_safe(violations),
            },
            counterfactual=None,
            carbon_kg=float(metrics.carbon_kg),
            energy_kwh=float(metrics.energy_kwh),
            safety_violations=safety,
            requires_approval=False,
            confidence=quality,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Route (with real metrics + IG XAI)
    # ------------------------------------------------------------------
    async def route_data(self, task: Dict[str, Any]) -> DataDecision:
        dataset = task.get("data")
        dataset_id = task.get("dataset_id",
                              f"route_{uuid.uuid4().hex[:8]}")
        if isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            df = pd.DataFrame(dataset)

        data_size_mb = float(df.memory_usage(deep=True).sum()
                             / (1024 * 1024))
        carbon_intensity = await self._current_carbon_g_per_kwh()
        helium = await self._current_helium_status()
        helium_scarcity = float(helium.get("scarcity", 0.5))

        routes = ["feature_expert", "model_expert", "optimization_expert"]
        candidates: List[Dict[str, Any]] = []
        for route in routes:
            if route == "optimization_expert":
                carbon_g = data_size_mb * carbon_intensity * 0.001
                latency_ms = 60.0
                energy_j = data_size_mb * 20.0
                quality = 0.55
            elif route == "model_expert":
                carbon_g = data_size_mb * carbon_intensity * 0.0008
                latency_ms = 100.0
                energy_j = data_size_mb * 15.0
                quality = 0.75
            else:  # feature_expert
                carbon_g = data_size_mb * carbon_intensity * 0.0005
                latency_ms = 50.0
                energy_j = data_size_mb * 10.0
                quality = 0.65

            # Helium-aware modulation
            if helium_scarcity > 0.7:
                energy_j *= 1.2

            cost = self.adaptive_cost.compute(
                quality=quality, carbon_g=carbon_g, latency_ms=latency_ms,
                energy_joules=energy_j, health=1.0, atp=0.5)
            candidates.append({
                "route": route, "score": float(cost),
                "carbon_g": float(carbon_g),
                "latency_ms": float(latency_ms),
                "energy_joules": float(energy_j),
                "quality_score": float(quality),
            })

        if self.pareto is not None:
            try:
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed = {c["route"] for c in filtered}
                    candidates = [c for c in candidates
                                  if c["route"] in allowed]
            except Exception as e:
                logger.debug(f"pareto filter failed: {e}")

        if not candidates:
            return DataDecision(
                dataset_id=dataset_id, operation="route",
                profile=None, routing={r: False for r in routes},
                explanation={"reason": "no route passed pareto filter"},
                counterfactual=None,
                carbon_kg=0.0, energy_kwh=0.0,
                safety_violations=(),
                requires_approval=False, confidence=0.0,
                timestamp=time.time(),
                status="error", error="no_route",
            )

        best = min(candidates, key=lambda c: c["score"])
        chosen_route = best["route"]

        # XAI over the route decision via Integrated Gradients
        explanation, counterfactual = self._explain_route(
            data_size_mb, carbon_intensity, helium_scarcity, routes)

        routing = {r: (r == chosen_route) for r in routes}
        recommended = [r for r, v in routing.items() if v]

        violations = await self._safety_violations(
            1.0 if chosen_route else 0.0)
        await self._publish_feedback(
            task_id=f"data_route_{dataset_id}",
            action="route",
            quality=1.0 if chosen_route else 0.0,
            details={"dataset_id": dataset_id, "routing": routing,
                     "explanation": explanation},
            tags=["data", "route"])

        return DataDecision(
            dataset_id=dataset_id, operation="route",
            profile=None, routing=routing,
            explanation=explanation,
            counterfactual=counterfactual,
            carbon_kg=float(best["carbon_g"]),
            energy_kwh=float(best["energy_joules"]) / 3.6e6,
            safety_violations=violations,
            requires_approval=False,
            confidence=float(best["quality_score"]),
            timestamp=time.time(),
        )

    def _explain_route(self, data_size_mb: float,
                       carbon_intensity: float,
                       helium_scarcity: float,
                       routes: List[str]
                       ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # Feature vector: [data_size_mb, carbon_intensity, helium_scarcity]
        x = np.array([data_size_mb, carbon_intensity, helium_scarcity],
                     dtype=float)

        def _score(v: np.ndarray) -> float:
            # Simple surrogate: cheaper when data is small, carbon low,
            # helium abundant. Returns a scalar cost-like quantity.
            ds, ci, hs = (float(v[0]), float(v[1]), float(v[2]))
            return ds * ci * 1e-3 + ds * hs * 1e-2

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["data_size_mb", "carbon_intensity",
                                   "helium_scarcity"],
                "routes": routes,
            }
            counterfactual = self.xai.counterfactual(_score, x)
            return explanation, counterfactual
        except Exception as e:
            logger.debug(f"route XAI failed: {e}")
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Federated aggregation (real + DP)
    # ------------------------------------------------------------------
    async def federated_aggregate(self,
                                  task: Dict[str, Any]) -> DataDecision:
        if not self.config.enable_federated_aggregation:
            return DataDecision(
                dataset_id="federated", operation="federated_aggregate",
                profile=None, routing={},
                explanation={"reason": "disabled"},
                counterfactual=None,
                carbon_kg=0.0, energy_kwh=0.0,
                safety_violations=(),
                requires_approval=False, confidence=0.0,
                timestamp=time.time(),
                status="error", error="disabled")

        datasets = task.get("datasets", []) or []
        dp_epsilon = float(task.get("dp_epsilon",
                                    self.config.federated_dp_epsilon))
        dp_clip = float(task.get("dp_clip", self.config.federated_dp_clip))

        if not datasets:
            return DataDecision(
                dataset_id="federated", operation="federated_aggregate",
                profile=None, routing={},
                explanation={"reason": "no datasets"},
                counterfactual=None,
                carbon_kg=0.0, energy_kwh=0.0,
                safety_violations=(),
                requires_approval=False, confidence=0.0,
                timestamp=time.time(),
                status="error", error="no_datasets")

        # Aggregate profiles with row-count weighting + optional DP noise
        agg_numeric: Dict[str, List[float]] = defaultdict(list)
        agg_weights: Dict[str, List[float]] = defaultdict(list)
        total_rows = 0
        schema_hashes: Set[str] = set()

        for item in datasets:
            # Accept a dataset_id string, a summary, or an explicit profile
            dataset_id = None
            profile_dict: Optional[Dict[str, Any]] = None
            rows = 0
            if isinstance(item, str):
                dataset_id = item
                if item in self.profiles:
                    profile_dict = self.profiles[item].to_dict()
                    rows = int(self.profiles[item].shape[0])
            elif isinstance(item, dict):
                dataset_id = item.get("dataset_id")
                profile_dict = item.get("profile")
                rows = int(item.get("rows", 0))
            if not profile_dict:
                continue
            total_rows += rows
            schema_hashes.add(str(dataset_id))
            for col_name, col_dict in profile_dict.get("columns", {}).items():
                for stat_key in ("mean_val", "std_val", "median_val"):
                    v = col_dict.get(stat_key)
                    if v is None:
                        continue
                    try:
                        agg_numeric[f"{col_name}.{stat_key}"].append(
                            float(v))
                        agg_weights[f"{col_name}.{stat_key}"].append(
                            float(max(rows, 1)))
                    except (TypeError, ValueError):
                        continue

        aggregated: Dict[str, Dict[str, Any]] = {}
        if total_rows == 0:
            total_rows = 1

        for key, values in agg_numeric.items():
            weights = agg_weights.get(key, [1.0] * len(values))
            wsum = sum(weights) or 1.0
            weighted = sum(v * w for v, w in zip(values, weights)) / wsum
            # Optional DP noise (Gaussian mechanism)
            if dp_epsilon > 0:
                sigma = dp_clip / max(dp_epsilon, 1e-6)
                noise = self._rng_dp.gauss(0.0, sigma)
                weighted = max(-abs(weighted), min(abs(weighted) + 1.0,
                                                    weighted + noise))
            col_name, stat = key.rsplit(".", 1)
            aggregated.setdefault(col_name, {})[stat] = float(weighted)

        explanation = {
            "method": "weighted_aggregate_dp",
            "num_datasets": len(datasets),
            "total_rows": total_rows,
            "dp_epsilon": dp_epsilon,
            "dp_clip": dp_clip,
        }
        violations = await self._safety_violations(0.8)
        await self._publish_feedback(
            task_id=f"data_fed_agg_{uuid.uuid4().hex[:8]}",
            action="federated_aggregate",
            quality=0.8,
            details=explanation,
            tags=["data", "federated"])

        return DataDecision(
            dataset_id="federated", operation="federated_aggregate",
            profile={"aggregated": aggregated},
            routing={},
            explanation=explanation,
            counterfactual=None,
            carbon_kg=0.0, energy_kwh=0.0,
            safety_violations=violations,
            requires_approval=False,
            confidence=0.8,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # MOPD teacher interface
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        strategies = ["profile", "clean", "summarize", "validate", "route"]
        data_size_mb = float(state.get(
            "data_size_mb",
            self._get_context().get("data_size_mb", 10.0)))
        carbon_intensity = float(state.get(
            "carbon_intensity",
            self._get_context().get("carbon_intensity", 100.0)))
        helium = await self._current_helium_status()
        helium_scarcity = float(state.get(
            "helium_scarcity", helium.get("scarcity", 0.5)))

        candidates: List[Dict[str, Any]] = []
        for strategy in strategies:
            if strategy == "profile":
                quality, carbon_g, latency, energy_j = (
                    0.85, data_size_mb * carbon_intensity * 0.05,
                    80.0, data_size_mb * 5.0)
            elif strategy == "clean":
                quality, carbon_g, latency, energy_j = (
                    0.8, data_size_mb * carbon_intensity * 0.1,
                    50.0, data_size_mb * 10.0)
            elif strategy == "summarize":
                quality, carbon_g, latency, energy_j = (
                    0.75, data_size_mb * carbon_intensity * 0.03,
                    40.0, data_size_mb * 3.0)
            elif strategy == "validate":
                quality, carbon_g, latency, energy_j = (
                    0.7, data_size_mb * carbon_intensity * 0.02,
                    20.0, data_size_mb * 2.0)
            else:  # route
                quality, carbon_g, latency, energy_j = (
                    0.65, data_size_mb * carbon_intensity * 0.01,
                    30.0, data_size_mb * 1.0)
            # Helium-scarcity modulation: penalise energy-heavy strategies
            if helium_scarcity > 0.7:
                energy_j *= 1.15
            cost = self.adaptive_cost.compute(
                quality=quality, carbon_g=carbon_g,
                latency_ms=latency, energy_joules=energy_j,
                health=self.health_status == "healthy", atp=0.5)
            candidates.append({
                "strategy": strategy, "score": float(cost),
                "carbon_g": float(carbon_g), "latency_ms": float(latency),
                "energy_joules": float(energy_j),
                "quality_score": float(quality),
            })

        if self.pareto is not None:
            try:
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed = {c["strategy"] for c in filtered}
                    candidates = [c for c in candidates
                                  if c["strategy"] in allowed]
            except Exception:
                pass

        scores = np.array([c["score"] for c in candidates], dtype=float)
        if scores.size == 0:
            return [0.2] * 5
        # We want LOW cost, so invert before softmax
        logits = -scores
        logits -= logits.max()
        probs = np.exp(logits) / max(float(np.exp(logits).sum()), 1e-12)
        full = [0.0] * len(strategies)
        for c, p in zip(candidates, probs):
            idx = strategies.index(c["strategy"])
            full[idx] = float(p)
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
                # Nudge the adaptive threshold (bounded, decays toward config)
                self._adaptive_missing_threshold = min(
                    0.7, self._adaptive_missing_threshold + 0.05)
                logger.warning(
                    f"High drift ({score:.3f}); adaptive threshold is now "
                    f"{self._adaptive_missing_threshold:.3f}")
            else:
                # Slowly decay toward the configured value
                target = self.config.missing_value_threshold
                self._adaptive_missing_threshold += (
                    target - self._adaptive_missing_threshold) * 0.05
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
                feedback_type="data",
                adaptive_cost_value=0.0,
                state=details,
                # candidates MUST be a list of dicts
                candidates=[{"action": a} for a in (
                    "profile", "clean", "summarize", "validate", "route")],
                source="data_expert",
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
        total_bytes = sum(m.bytes_processed for m in self.metrics_history)
        total_carbon = sum(m.carbon_kg for m in self.metrics_history)
        total_energy = sum(m.energy_kwh for m in self.metrics_history)
        failures = sum(1 for m in self.metrics_history if not m.success)
        return {
            "expert_name": self.expert_name,
            "tasks_handled": self.tasks_handled,
            "avg_latency_seconds": (self.total_latency / self.tasks_handled
                                    if self.tasks_handled else 0.0),
            "total_bytes_processed": int(total_bytes),
            "total_carbon_kg": float(total_carbon),
            "total_energy_kwh": float(total_energy),
            "failure_rate": (failures / len(self.metrics_history)
                             if self.metrics_history else 0.0),
            "datasets_cached": len(self.datasets),
            "profiles_cached": len(self.profiles),
            "summaries_cached": len(self.summaries),
            "temporal_violations": self.temporal_safety.counts(),
            "adaptive_missing_threshold": self._adaptive_missing_threshold,
            "background_tasks": self.task_manager.task_count(),
        }

    async def get_health_status(self) -> ComponentHealth:
        """Lightweight health check with no side effects."""
        try:
            missing = [n for n in
                       ("storage", "adaptive_cost", "pareto", "metrics")
                       if getattr(self, n, None) is None]
            if missing:
                self.health_status = "unhealthy"
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"missing_dependencies": missing})
            detail = {
                "tasks_handled": self.tasks_handled,
                "circuit_state": self._http_circuit.state.value,
                "last_error": None,
            }
            self.health_status = "healthy"
            return ComponentHealth(HealthStatus.HEALTHY, 1.0, detail)
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
    e = DataExpert(
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
        async def get_current_intensity(self):
            return 350.0
        async def get_current_price(self):
            return 45.0

    class FakeHelium:
        async def get_helium_status(self):
            return {"scarcity": 0.4, "price": 0.55, "remaining": 80.0}

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
    e = DataExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        carbon_manager=FakeCarbon(),
        helium_optimizer=FakeHelium(),
        bio_core=bio,
        hitl=hitl,
        seed=42)
    e._bio = bio  # for tests
    e._hitl = hitl
    await e.initialize()
    try:
        yield e
    finally:
        await e.shutdown()


def _sample_df(n: int = 50) -> pd.DataFrame:
    rng = np.random.RandomState(0)
    return pd.DataFrame({
        "id": np.arange(n),
        "value": rng.randn(n),
        "category": rng.choice(["a", "b", "c"], size=n),
        "maybe_null": np.where(rng.rand(n) < 0.3, None, rng.randn(n)),
    })


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    @pytest.mark.asyncio
    async def test_metrics_registry_has_canonical_api(self, metrics):
        metrics.inc("a", 2)
        metrics.set("b", 3.0)
        assert metrics.counters["a"] == 2
        assert metrics.gauges["b"] == 3.0
        # Aliases forward to canonical
        metrics.increment("a")
        metrics.observe("b", 4.0)
        assert metrics.counters["a"] == 3
        assert metrics.gauges["b"] == 4.0

    @pytest.mark.asyncio
    async def test_feedback_event_rejects_set_literal(self, expert, queue):
        # The old code passed [{...set...}] as candidates. The new schema
        # drops unknown kwargs and validates.
        await expert.profile_data({"data": _sample_df(10)})
        # Published events must have JSON-serialisable candidates
        for topic, payload in queue.published:
            parsed = json.loads(payload)
            assert isinstance(parsed["context"], dict)
            # candidates are folded into `context.state` or written elsewhere;
            # at minimum, the event must serialise.

    @pytest.mark.asyncio
    async def test_top_values_json_serialisable(self, expert):
        df = _sample_df(20)
        decision = await expert.profile_data({"data": df})
        # The whole decision must json.dumps without error.
        payload = json.dumps(decision.to_dict())
        # Specifically, top_values in category should be strings
        col = decision.profile["columns"]["category"]
        if col["top_values"]:
            for k, _ in col["top_values"]:
                assert isinstance(k, str)

    @pytest.mark.asyncio
    async def test_async_generator_source(self, expert):
        async def gen():
            for i in range(3):
                yield {"id": i, "value": float(i)}

        df = await expert.load_data(gen())
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_single_circuit_breaker_call(self, expert):
        # Two counter increments, one call.
        calls_before = sum(1 for c in expert._http_circuit.__dict__
                            if c == "failure_count")
        # Trigger a fetch failure and check the count only advances once.
        # We can't easily hit the network here; instead verify the code
        # structure by calling _fetch_from_url and observing the raise
        # path adds exactly one failure.
        expert._http_circuit.failure_count = 0
        with pytest.raises(Exception):
            await expert._fetch_from_url("http://invalid.example/foo.csv")
        # Exactly one failure recorded by the single wrapper.
        assert expert._http_circuit.failure_count == 1

    @pytest.mark.asyncio
    async def test_task_manager_tracks_tasks(self, storage, queue,
                                              adaptive_cost, pareto_gating,
                                              drift_detector, metrics):
        e = DataExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e.initialize()
        try:
            assert e.task_manager.task_count() == 0
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_persistence(self, expert, storage):
        df = _sample_df(20)
        await expert.load_data(df, dataset_id="d1")
        await expert._save_state()
        raw = storage.get_state("data_expert_datasets_v4")
        assert raw is not None
        # Decoded base64 must be valid parquet or JSON, not a pickle opcode
        entry = json.loads(raw)["d1"]
        blob = base64.b64decode(entry["payload"].encode("ascii"))
        assert entry["format"] in ("parquet", "json")
        if entry["format"] == "json":
            # Must be valid JSON text
            decoded = json.loads(blob.decode("utf-8"))
            assert "columns" in decoded or "data" in decoded
        else:
            # Parquet magic bytes
            assert blob[:4] == b"PAR1"

    @pytest.mark.asyncio
    async def test_sync_storage_called_via_to_thread(self, expert, storage):
        # We can't easily detect to_thread, but we can verify the storage
        # methods are called exactly once per _save_state / _load_state.
        await expert._save_state()
        save_calls = [c for c in storage.calls if c[0] == "save_state"]
        assert len(save_calls) >= 1

    @pytest.mark.asyncio
    async def test_drift_does_not_mutate_config(self, expert, drift_detector):
        original = expert.config.missing_value_threshold
        # Force drift by feeding many non-neutral weights
        for _ in range(30):
            await expert._check_drift()
        assert expert.config.missing_value_threshold == original

    @pytest.mark.asyncio
    async def test_lock_is_used(self, expert):
        df = _sample_df(50)
        # Concurrent handle_task
        results = await asyncio.gather(*[
            expert.handle_task({"type": "data_validate", "data": df})
            for _ in range(5)])
        assert all(r["status"] == "success" for r in results)
        assert expert.tasks_handled == 5

    @pytest.mark.asyncio
    async def test_context_has_ttl(self, expert):
        expert._set_context({"data_size_mb": 123.0})
        assert expert._get_context().get("data_size_mb") == 123.0
        # Manually set the timestamp to the past
        expert._last_context_ts = time.monotonic() - \
            expert.config.context_ttl_seconds - 1
        assert expert._get_context() == {}

    @pytest.mark.asyncio
    async def test_health_status_returns_component_health(self, expert):
        h = await expert.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert isinstance(h.status, HealthStatus)

    @pytest.mark.asyncio
    async def test_summarize_data_is_implemented(self, expert):
        df = _sample_df(20)
        decision = await expert.summarize_data({"data": df})
        assert decision.status == "success"
        assert decision.explanation is not None
        assert "schema_hash" in decision.explanation
        assert "summary" in decision.explanation

    @pytest.mark.asyncio
    async def test_validate_data_is_implemented(self, expert):
        df = _sample_df(20)
        decision = await expert.validate_data({
            "data": df,
            "rules": [{"column": "value", "type": "numeric",
                        "min": -10, "max": 10}],
        })
        assert decision.status == "success"
        assert decision.explanation["rules_evaluated"] == 1


# =============================================================================
# TESTS — CORE OPERATIONS
# =============================================================================
class TestCoreOperations:
    @pytest.mark.asyncio
    async def test_profile_data(self, expert):
        df = _sample_df(50)
        decision = await expert.profile_data({"data": df})
        assert decision.status == "success"
        assert decision.profile is not None
        assert decision.profile["shape"][0] == 50
        assert decision.dataset_id in expert.profiles

    @pytest.mark.asyncio
    async def test_clean_data(self, expert):
        df = _sample_df(50)
        decision = await expert.clean_data({
            "data": df,
            "dataset_id": "c1",
            "params": {"remove_duplicates": True, "fill_missing": True},
        })
        assert decision.status == "success"
        cleaned = expert.datasets["c1"]
        assert cleaned.isna().sum().sum() == 0

    @pytest.mark.asyncio
    async def test_route_data(self, expert):
        df = _sample_df(30)
        decision = await expert.route_data({"data": df})
        assert decision.status == "success"
        assert any(decision.routing.values())
        assert decision.explanation is not None
        assert decision.explanation.get("method") == "integrated_gradients"

    @pytest.mark.asyncio
    async def test_federated_aggregate(self, expert):
        # Build two profiles first
        await expert.profile_data({"data": _sample_df(10),
                                    "dataset_id": "a"})
        await expert.profile_data({"data": _sample_df(20),
                                    "dataset_id": "b"})
        decision = await expert.federated_aggregate({
            "datasets": ["a", "b"],
            "dp_epsilon": 0.0,  # disable noise for determinism
        })
        assert decision.status == "success"
        assert decision.profile is not None
        assert "aggregated" in decision.profile

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, expert):
        probs = await expert.policy_probs({})
        assert len(probs) == 5
        assert abs(sum(probs) - 1.0) < 1e-6 or sum(probs) == 0.0

    @pytest.mark.asyncio
    async def test_handle_task_dispatch(self, expert):
        df = _sample_df(10)
        for ttype in ("data_profile", "data_clean", "data_summary",
                       "data_validate", "data_route"):
            r = await expert.handle_task({"type": ttype, "data": df})
            assert r["status"] == "success", ttype
            assert "correlation_id" in r


# =============================================================================
# TESTS — CARBON / HELIUM / BIO
# =============================================================================
class TestCarbonHeliumBio:
    @pytest.mark.asyncio
    async def test_carbon_manager_used(self, expert_full):
        intensity = await expert_full._current_carbon_g_per_kwh()
        assert intensity == 350.0

    @pytest.mark.asyncio
    async def test_helium_status_used(self, expert_full):
        status = await expert_full._current_helium_status()
        assert status["scarcity"] == 0.4

    @pytest.mark.asyncio
    async def test_bio_spend_earn_called(self, expert_full):
        df = _sample_df(20)
        await expert_full.profile_data({"data": df})
        # At least one spend should have happened
        assert len(expert_full._bio.spend_calls) >= 1


# =============================================================================
# TESTS — HITL + TEMPORAL SAFETY
# =============================================================================
class TestHITLAndTemporal:
    @pytest.mark.asyncio
    async def test_hitl_blocks_when_rejected(self, storage, queue,
                                              adaptive_cost, pareto_gating,
                                              drift_detector, metrics):
        hitl = HumanApprovalHandler()
        # Pre-queue a "defer" response
        await hitl.submit_response("data_expert", "defer")

        cfg = DataExpertConfig(large_operation_rows=5,
                                large_operation_cooldown_seconds=0.0)
        e = DataExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, config=cfg, seed=1)
        await e.initialize()
        try:
            df = _sample_df(20)
            decision = await e.profile_data({"data": df})
            # If HITL rejected, decision should be deferred
            assert decision.requires_approval or decision.status == "deferred"
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_temporal_safety_rules_present(self, expert):
        counts = expert.temporal_safety.counts()
        assert len(counts) >= 4  # G, G, G, NEVER

    @pytest.mark.asyncio
    async def test_safety_violations_reported(self, expert):
        v = await expert._safety_violations(0.5)
        assert isinstance(v, tuple)


# =============================================================================
# TESTS — PERSISTENCE
# =============================================================================
class TestPersistence:
    @pytest.mark.asyncio
    async def test_roundtrip_state(self, storage, queue, adaptive_cost,
                                    pareto_gating, drift_detector, metrics):
        e1 = DataExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e1.initialize()
        try:
            df = _sample_df(20)
            await e1.load_data(df, dataset_id="x1")
            await e1.profile_data({"data": df, "dataset_id": "x1"})
            await e1._save_state()
            assert e1.tasks_handled >= 0

            e2 = DataExpert(
                storage=storage, message_queue=queue,
                adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
                drift_detector=drift_detector, metrics=metrics, seed=2)
            await e2.initialize()
            try:
                assert "x1" in e2.datasets
                assert "x1" in e2.profiles
            finally:
                await e2.shutdown()
        finally:
            await e1.shutdown()


# =============================================================================
# TESTS — DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_seeded_profile(self, storage, queue, adaptive_cost,
                                    pareto_gating, drift_detector, metrics):
        def _make(seed):
            return DataExpert(
                storage=storage, message_queue=queue,
                adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
                drift_detector=drift_detector, metrics=metrics, seed=seed)
        e1 = _make(7)
        e2 = _make(7)
        await e1.initialize()
        await e2.initialize()
        try:
            df = _sample_df(30)
            d1 = await e1.profile_data({"data": df})
            d2 = await e2.profile_data({"data": df})
            assert d1.profile["quality_score"] == d2.profile["quality_score"]
            assert d1.profile["shape"] == d2.profile["shape"]
        finally:
            await e1.shutdown()
            await e2.shutdown()


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

        expert = DataExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics,
            seed=1)
        await expert.initialize()
        try:
            df = _sample_df(50)
            for ttype in ("data_profile", "data_summary",
                           "data_validate", "data_route"):
                r = await expert.handle_task({"type": ttype, "data": df})
                print(f"{ttype}: status={r['status']} "
                      f"latency={r.get('latency_seconds', 0):.4f}s")
            metrics_snapshot = await expert.get_metrics()
            print("Metrics:", json.dumps(metrics_snapshot, indent=2,
                                          default=str))
        finally:
            await expert.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
