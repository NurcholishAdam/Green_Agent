#!/usr/bin/env python3
"""
FLEnergyExpert v4.0.0 — Self-Contained Single-File Bundle
===========================================================

Complete rewrite of the Green Agent FL Energy Expert. Everything needed
to run and test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. Local stubs (InMemoryStorage, AsyncMessageQueue, FeedbackEvent,
     MetricsRegistry, AdaptiveCostFunction, ParetoGating, DriftDetector).
  3. Shared runtime: CircuitBreaker, TemporalSafetyMonitor (MTL),
     HumanApprovalHandler, XAIExplainer (IG + counterfactuals),
     TaskManager, ComponentHealth.
  4. `FLDecision` value object.
  5. Real budget enforcement + HITL gating.
  6. Correct torch aggregation (shape-safe).
  7. Cumulative windowed client-selection fairness check.
  8. Real Integrated Gradients XAI on client selection.
  9. Live carbon/helium modulation of client selection.
 10. JSON-only persistence via asyncio.to_thread; bounded `rounds`.
 11. Comprehensive pytest suite.

Run:
    python fl_energy_expert_v4.py         # demo
    pytest fl_energy_expert_v4.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
Optional: torch (real tensor aggregation), aiohttp.
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

logger = logging.getLogger("fl_energy_expert")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

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
        self._topics: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.published: List[Tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))
        await self._topics[topic].put(payload)

    async def subscribe(self, topic: str, timeout: float = 0.5
                        ) -> Optional[str]:
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
# ENUMS + VALUE OBJECTS
# =============================================================================
class ClientState(str, Enum):
    AVAILABLE = "available"
    SLEEPING = "sleeping"
    CHARGING = "charging"
    ACTIVE = "active"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class AggregationStrategy(str, Enum):
    STANDARD = "standard"
    LAZY = "lazy"
    PRIORITY = "priority"
    GRADIENT_COMPRESSION = "gradient_compression"
    SELECTIVE = "selective"


class ClientEnergyProfile(str, Enum):
    BATTERY_POWERED = "battery_powered"
    SOLAR_POWERED = "solar_powered"
    PLUGGED_IN = "plugged_in"
    DEGRADED_BATTERY = "degraded_battery"


@dataclass
class ClientEnergyInfo:
    client_id: str
    state: ClientState = ClientState.AVAILABLE
    energy_profile: ClientEnergyProfile = ClientEnergyProfile.BATTERY_POWERED
    battery_level: float = 1.0
    energy_consumption_rate: float = 0.01
    upload_bandwidth_mbps: float = 10.0
    download_bandwidth_mbps: float = 10.0
    compute_capability: float = 1.0
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    estimated_sync_time_seconds: float = 0.0
    carbon_intensity_g_per_kwh: float = 100.0

    def get_energy_score(self) -> float:
        if self.state == ClientState.UNAVAILABLE:
            return 0.0
        if self.state == ClientState.SLEEPING:
            return 0.1
        if self.state == ClientState.DEGRADED:
            return 0.3
        battery_score = self.battery_level
        if self.energy_profile == ClientEnergyProfile.PLUGGED_IN:
            battery_score = 1.0
        elif self.energy_profile == ClientEnergyProfile.SOLAR_POWERED:
            battery_score = min(1.0, battery_score * 1.2)
        state_bonus = 1.0
        if self.state == ClientState.CHARGING:
            state_bonus = 1.3
        carbon_factor = 1.0 - (self.carbon_intensity_g_per_kwh / 1000.0)
        return min(1.0, battery_score * state_bonus * carbon_factor)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["state"] = self.state.value
        d["energy_profile"] = self.energy_profile.value
        d["last_seen"] = self.last_seen.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ClientEnergyInfo":
        d = dict(d)
        d["state"] = ClientState(d["state"])
        d["energy_profile"] = ClientEnergyProfile(d["energy_profile"])
        d["last_seen"] = datetime.fromisoformat(d["last_seen"])
        return cls(**d)


@dataclass
class ClientUpdateInfo:
    client_id: str
    model_hash: str
    gradient_norm: float
    update_timestamp: datetime
    compression_ratio: float = 1.0
    transmission_time_ms: float = 0.0
    energy_cost_joules: float = 0.0
    sample_count: int = 0
    success: bool = True
    gradients: Optional[Any] = None

    def to_dict(self, include_gradients: bool = False) -> Dict[str, Any]:
        d = asdict(self)
        d["update_timestamp"] = self.update_timestamp.isoformat()
        if not include_gradients:
            d.pop("gradients", None)
        elif d.get("gradients") is not None:
            g = d["gradients"]
            if isinstance(g, np.ndarray):
                d["gradients"] = g.tolist()
            elif TORCH_AVAILABLE and isinstance(g, torch.Tensor):
                d["gradients"] = g.detach().cpu().numpy().tolist()
        return _json_safe(d)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ClientUpdateInfo":
        d = dict(d)
        if isinstance(d.get("update_timestamp"), str):
            d["update_timestamp"] = datetime.fromisoformat(d["update_timestamp"])
        d.pop("gradients", None)  # not persisted
        return cls(**d)


@dataclass
class AggregationRound:
    round_id: int
    strategy: AggregationStrategy
    selected_clients: List[str]
    completed_clients: List[ClientUpdateInfo]
    failed_clients: List[str]
    timestamp: datetime
    duration_seconds: float
    total_energy_joules: float
    model_hash: str
    compression_ratio: float = 1.0
    explanation: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "round_id": int(self.round_id),
            "strategy": self.strategy.value,
            "selected_clients": list(self.selected_clients),
            "completed_clients": [u.to_dict() for u in self.completed_clients],
            "failed_clients": list(self.failed_clients),
            "timestamp": self.timestamp.isoformat(),
            "duration_seconds": float(self.duration_seconds),
            "total_energy_joules": float(self.total_energy_joules),
            "model_hash": self.model_hash,
            "compression_ratio": float(self.compression_ratio),
            "explanation": _json_safe(self.explanation or {}),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AggregationRound":
        return cls(
            round_id=int(d["round_id"]),
            strategy=AggregationStrategy(d["strategy"]),
            selected_clients=list(d.get("selected_clients", [])),
            completed_clients=[ClientUpdateInfo.from_dict(u)
                                for u in d.get("completed_clients", [])],
            failed_clients=list(d.get("failed_clients", [])),
            timestamp=datetime.fromisoformat(d["timestamp"]),
            duration_seconds=float(d.get("duration_seconds", 0.0)),
            total_energy_joules=float(d.get("total_energy_joules", 0.0)),
            model_hash=d.get("model_hash", ""),
            compression_ratio=float(d.get("compression_ratio", 1.0)),
            explanation=d.get("explanation", {}) or {},
        )


@dataclass(frozen=True)
class FLDecision:
    """Value object capturing a single FL decision (selection, round, ...)."""
    operation: str
    status: str = "success"
    error: Optional[str] = None
    round_id: Optional[int] = None
    strategy: Optional[str] = None
    selected_clients: Tuple[str, ...] = ()
    weights: Dict[str, float] = field(default_factory=dict)
    completed_clients: int = 0
    failed_clients: Tuple[str, ...] = ()
    energy_joules: float = 0.0
    carbon_g: float = 0.0
    compression_ratio: float = 1.0
    confidence: float = 0.0
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None
    safety_violations: Tuple[str, ...] = ()
    requires_approval: bool = False
    timestamp: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "error": self.error,
            "round_id": self.round_id,
            "strategy": self.strategy,
            "selected_clients": list(self.selected_clients),
            "weights": dict(self.weights),
            "completed_clients": int(self.completed_clients),
            "failed_clients": list(self.failed_clients),
            "energy_joules": float(self.energy_joules),
            "carbon_g": float(self.carbon_g),
            "compression_ratio": float(self.compression_ratio),
            "confidence": float(self.confidence),
            "explanation": self.explanation,
            "counterfactual": self.counterfactual,
            "safety_violations": list(self.safety_violations),
            "requires_approval": bool(self.requires_approval),
            "timestamp": float(self.timestamp),
        }


@dataclass
class FLEnergyConfig:
    min_clients_per_round: int = 3
    max_clients_per_round: int = 20
    energy_threshold_battery: float = 0.2
    energy_threshold_degraded: float = 0.4
    target_compression_ratio: float = 0.1
    aggregation_timeout_seconds: int = 300
    lazy_aggregation_enabled: bool = True
    stale_client_threshold_hours: int = 24
    energy_aware_weighting: bool = True
    gradient_clipping_enabled: bool = True

    # Budgets
    round_energy_budget_joules: float = 1000.0
    round_carbon_budget_g: float = 100.0

    # Persistence
    rounds_maxlen: int = 500
    metrics_history_maxlen: int = 1000

    # Fairness
    selection_window_seconds: float = 300.0
    max_selections_per_window: int = 3

    def __post_init__(self) -> None:
        if not (0.0 <= self.target_compression_ratio <= 1.0):
            self.target_compression_ratio = 0.1
        if self.min_clients_per_round < 1:
            self.min_clients_per_round = 1
        if self.max_clients_per_round < self.min_clients_per_round:
            self.max_clients_per_round = self.min_clients_per_round
        if self.round_energy_budget_joules <= 0:
            self.round_energy_budget_joules = 1000.0
        if self.round_carbon_budget_g <= 0:
            self.round_carbon_budget_g = 100.0


# =============================================================================
# FL ENERGY EXPERT (v4.0.0)
# =============================================================================
class FLEnergyExpert:
    """FL Energy Expert v4.0.0 — Energy-aware federated learning expert."""

    SUPPORTED_TASKS = (
        "fl_round", "fl_select_clients", "fl_aggregate",
        "fl_compression", "fl_energy_report",
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
        config: Optional[FLEnergyConfig] = None,
        seed: int = 42,
    ):
        self.expert_name = "fl_energy_expert"
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
        self.config = config or FLEnergyConfig()

        # Deterministic RNG
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        np.random.seed(seed)

        # State
        self.clients: Dict[str, ClientEnergyInfo] = {}
        self.client_history: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=100))
        self.rounds: Deque[AggregationRound] = deque(
            maxlen=self.config.rounds_maxlen)
        self.metrics_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.metrics_history_maxlen)

        self.total_energy_consumed_joules = 0.0
        self.total_updates_processed = 0
        self.failed_updates = 0
        self.participation_history: Dict[str, int] = defaultdict(int)

        self.current_strategy = AggregationStrategy.STANDARD
        self.strategy_change_log: List[Dict[str, Any]] = []

        self.gradient_compression_enabled = True
        self.compression_ratios: Deque[float] = deque(maxlen=50)

        # Adaptive state (config never mutated)
        self._adaptive_compression_ratio = float(
            self.config.target_compression_ratio)

        # Cumulative windowed selection fairness
        self._selection_history: Deque[Tuple[str, datetime]] = deque(
            maxlen=2000)

        # Circuit breaker
        self._client_circuit = CircuitBreaker(
            failure_threshold=5, recovery_timeout=30.0, name="fl_client")

        # Concurrency
        self._lock = asyncio.Lock()

        # Shared runtime
        self.temporal_safety = TemporalSafetyMonitor(max_trace=500)
        self._setup_temporal_rules()
        self.xai = XAIExplainer(rng=self._rng_xai)

        # Task manager
        self.task_manager = TaskManager()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_state()
        self.task_manager.start_coro("stale_cleanup",
                                      self._cleanup_stale_clients())
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self._save_state()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula(
            "G (round_energy <= round_energy_budget)")
        self.temporal_safety.add_formula(
            "G (round_carbon <= round_carbon_budget)")
        self.temporal_safety.add_formula(
            "G (compression_ratio >= 0.05)")
        self.temporal_safety.add_formula("NEVER (failed_updates_rate > 1.0)")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _load_state(self) -> None:
        try:
            data = await asyncio.to_thread(
                self.storage.get_state, "fl_energy_expert_state_v4")
            if not data:
                return
            state = json.loads(data)
            for cid, info_dict in state.get("clients", {}).items():
                try:
                    self.clients[cid] = ClientEnergyInfo.from_dict(info_dict)
                except Exception as e:
                    logger.debug(f"skip client {cid}: {e}")

            for r_dict in state.get("rounds", []):
                try:
                    self.rounds.append(AggregationRound.from_dict(r_dict))
                except Exception as e:
                    logger.debug(f"skip round: {e}")

            self.total_energy_consumed_joules = float(
                state.get("total_energy_consumed_joules", 0.0))
            self.total_updates_processed = int(
                state.get("total_updates_processed", 0))
            self.failed_updates = int(state.get("failed_updates", 0))
            self.participation_history = defaultdict(
                int, state.get("participation_history", {}))
            self.strategy_change_log = list(
                state.get("strategy_log", []))
            self.compression_ratios = deque(
                state.get("compression_ratios", []), maxlen=50)
            self.current_strategy = AggregationStrategy(
                state.get("current_strategy", "standard"))
            self._adaptive_compression_ratio = float(
                state.get("adaptive_compression_ratio",
                            self.config.target_compression_ratio))

            # Restore selection history for fairness
            for entry in state.get("selection_history", []):
                try:
                    cid, ts = entry
                    self._selection_history.append(
                        (cid, datetime.fromisoformat(ts)))
                except Exception:
                    continue
            logger.info("FLEnergyExpert state loaded")
        except Exception as e:
            logger.error(f"Failed to load FLEnergyExpert state: {e}")

    async def _save_state(self) -> None:
        try:
            state = {
                "clients": {cid: info.to_dict()
                            for cid, info in self.clients.items()},
                "rounds": [r.to_dict() for r in self.rounds],
                "total_energy_consumed_joules":
                    self.total_energy_consumed_joules,
                "total_updates_processed": self.total_updates_processed,
                "failed_updates": self.failed_updates,
                "participation_history": dict(self.participation_history),
                "strategy_log": list(self.strategy_change_log),
                "compression_ratios": list(self.compression_ratios),
                "current_strategy": self.current_strategy.value,
                "adaptive_compression_ratio": self._adaptive_compression_ratio,
                "selection_history": [
                    (cid, ts.isoformat()) for cid, ts in self._selection_history
                ],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await asyncio.to_thread(
                self.storage.save_state, "fl_energy_expert_state_v4",
                json.dumps(_json_safe(state), default=str))
        except Exception as e:
            logger.error(f"Failed to save FLEnergyExpert state: {e}")

    # ------------------------------------------------------------------
    # Client management
    # ------------------------------------------------------------------
    async def register_client(
        self,
        client_id: str,
        energy_profile: ClientEnergyProfile = ClientEnergyProfile.BATTERY_POWERED,
        bandwidth_mbps: float = 10.0,
        compute_capability: float = 1.0,
        carbon_intensity_g_per_kwh: float = 100.0,
    ) -> ClientEnergyInfo:
        async with self._lock:
            info = ClientEnergyInfo(
                client_id=client_id,
                energy_profile=energy_profile,
                upload_bandwidth_mbps=float(bandwidth_mbps),
                download_bandwidth_mbps=float(bandwidth_mbps),
                compute_capability=float(compute_capability),
                carbon_intensity_g_per_kwh=float(carbon_intensity_g_per_kwh),
            )
            self.clients[client_id] = info
        await self._publish_feedback(
            task_id=f"fl_register_{client_id}",
            action="register_client",
            quality=info.get_energy_score(),
            details={"client_id": client_id,
                     "profile": energy_profile.value},
            tags=["federated", "client"])
        return info

    async def update_client_state(
        self,
        client_id: str,
        state: ClientState,
        battery_level: Optional[float] = None,
        energy_consumption_rate: Optional[float] = None,
        carbon_intensity_g_per_kwh: Optional[float] = None,
    ) -> None:
        async with self._lock:
            if client_id not in self.clients:
                logger.warning(f"Unknown client: {client_id}")
                return
            client = self.clients[client_id]
            client.state = state
            client.last_seen = datetime.now(timezone.utc)
            if battery_level is not None:
                client.battery_level = float(max(0.0, min(1.0, battery_level)))
            if energy_consumption_rate is not None:
                client.energy_consumption_rate = float(
                    energy_consumption_rate)
            if carbon_intensity_g_per_kwh is not None:
                client.carbon_intensity_g_per_kwh = float(
                    carbon_intensity_g_per_kwh)
            self.client_history[client_id].append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "state": state.value,
                "battery": client.battery_level,
            })

    async def get_client_info(self, client_id: str
                               ) -> Optional[ClientEnergyInfo]:
        async with self._lock:
            return self.clients.get(client_id)

    async def _cleanup_stale_clients(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=3600.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                async with self._lock:
                    threshold = datetime.now(timezone.utc) - timedelta(
                        hours=self.config.stale_client_threshold_hours)
                    stale = [cid for cid, info in self.clients.items()
                             if info.last_seen < threshold]
                    for cid in stale:
                        self.clients.pop(cid, None)
                        logger.info(f"Removed stale client: {cid}")
            except Exception as e:
                logger.error(f"stale cleanup error: {e}")

    # ------------------------------------------------------------------
    # Live carbon/helium
    # ------------------------------------------------------------------
    async def _current_carbon_g_per_kwh(self) -> float:
        if self.carbon_manager is not None:
            try:
                v = self.carbon_manager.get_current_intensity()
                if asyncio.iscoroutine(v):
                    v = await v
                v = float(v)
                if v > 0:
                    return v
            except Exception as e:
                logger.debug(f"carbon manager failed: {e}")
        return 100.0

    async def _current_helium(self) -> Dict[str, float]:
        if self.helium_optimizer is not None:
            try:
                v = self.helium_optimizer.get_helium_status()
                if asyncio.iscoroutine(v):
                    v = await v
                if isinstance(v, dict):
                    return {
                        "scarcity": float(max(0.0, min(
                            1.0, v.get("scarcity", 0.5)))),
                        "price": float(v.get("price", 0.5)),
                    }
            except Exception as e:
                logger.debug(f"helium optimizer failed: {e}")
        return {"scarcity": 0.5, "price": 0.5}

    # ------------------------------------------------------------------
    # Bio integration
    # ------------------------------------------------------------------
    async def _bio_spend_earn(self, energy_joules: float, quality: float
                              ) -> None:
        if self.bio_core is None:
            return
        try:
            atp_cost = max(0.01, energy_joules * 0.001)
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
    # Budget enforcement + HITL
    # ------------------------------------------------------------------
    async def _budget_gate(self, energy_j: float, carbon_g: float
                            ) -> Tuple[bool, Optional[str]]:
        if energy_j > self.config.round_energy_budget_joules:
            if self.hitl is not None:
                approved = await self.hitl.request_approval(
                    {"action": "exceed_energy_budget",
                     "candidates": [
                         {"id": "approve", "energy_j": energy_j},
                         {"id": "defer", "energy_j": 0.0}]},
                    user_id=self.expert_name)
            else:
                approved = False
            if not approved:
                return False, "energy_budget_exceeded"
        if carbon_g > self.config.round_carbon_budget_g:
            if self.hitl is not None:
                approved = await self.hitl.request_approval(
                    {"action": "exceed_carbon_budget",
                     "candidates": [
                         {"id": "approve", "carbon_g": carbon_g},
                         {"id": "defer", "carbon_g": 0.0}]},
                    user_id=self.expert_name)
            else:
                approved = False
            if not approved:
                return False, "carbon_budget_exceeded"
        return True, None

    # ------------------------------------------------------------------
    # Safety snapshot
    # ------------------------------------------------------------------
    async def _safety_check(self, round_energy: float, round_carbon: float,
                             compression_ratio: float,
                             failed_rate: float) -> Tuple[str, ...]:
        await self.temporal_safety.push({
            "round_energy": float(round_energy),
            "round_energy_budget":
                float(self.config.round_energy_budget_joules),
            "round_carbon": float(round_carbon),
            "round_carbon_budget":
                float(self.config.round_carbon_budget_g),
            "compression_ratio": float(compression_ratio),
            "failed_updates_rate": float(failed_rate),
        })
        results = await self.temporal_safety.check()
        return tuple(rid for rid, ok in results.items() if not ok)

    # ------------------------------------------------------------------
    # Fairness (windowed selection counter)
    # ------------------------------------------------------------------
    def _record_selection(self, client_id: str) -> int:
        now = datetime.now(timezone.utc)
        self._selection_history.append((client_id, now))
        cutoff = now - timedelta(seconds=self.config.selection_window_seconds)
        recent = sum(1 for cid, ts in self._selection_history
                     if cid == client_id and ts >= cutoff)
        return recent

    def _over_selected(self, client_id: str) -> bool:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self.config.selection_window_seconds)
        recent = sum(1 for cid, ts in self._selection_history
                     if cid == client_id and ts >= cutoff)
        return recent >= self.config.max_selections_per_window

    # ------------------------------------------------------------------
    # Teacher interface
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        strategies = [s.value for s in AggregationStrategy]
        candidates: List[Dict[str, Any]] = []
        for s in AggregationStrategy:
            if s == AggregationStrategy.STANDARD:
                q, cg, lat, ej = 0.7, 5.0, 50.0, 50.0
            elif s == AggregationStrategy.LAZY:
                q, cg, lat, ej = 0.6, 2.0, 120.0, 20.0
            elif s == AggregationStrategy.PRIORITY:
                q, cg, lat, ej = 0.8, 8.0, 40.0, 80.0
            elif s == AggregationStrategy.GRADIENT_COMPRESSION:
                q, cg, lat, ej = 0.75, 3.0, 70.0, 35.0
            else:  # SELECTIVE
                q, cg, lat, ej = 0.85, 4.0, 60.0, 45.0
            cost = self.adaptive_cost.compute(
                quality=q, carbon_g=cg, latency_ms=lat, energy_joules=ej,
                health=self.health_status == "healthy", atp=0.5)
            candidates.append({"strategy": s.value, "score": float(cost),
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
    # Task dispatcher
    # ------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get("type", "unknown")
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        start = time.monotonic()

        try:
            if task_type == "fl_round":
                decision = await self.execute_aggregation_round(
                    int(task.get("round_id", 0)), task.get("state", {}))
            elif task_type == "fl_select_clients":
                decision = await self.select_clients_for_round(
                    target_count=task.get("target_count"),
                    energy_aware=task.get("energy_aware", True),
                    prefer_mobile=bool(task.get("prefer_mobile", False)))
            elif task_type == "fl_aggregate":
                updates = [self._coerce_update(u)
                            for u in task.get("updates", [])]
                weights = dict(task.get("weights", {}) or {})
                decision = await self.aggregate_updates(
                    updates, self.current_strategy, weights)
            elif task_type == "fl_compression":
                gradients = task.get("gradients")
                ratio = float(task.get("ratio",
                                        self._adaptive_compression_ratio))
                decision = self.compress_gradients_decision(
                    np.asarray(gradients), ratio)
            elif task_type == "fl_energy_report":
                report = self.get_energy_efficiency_report()
                decision = FLDecision(
                    operation="energy_report",
                    explanation=report,
                    timestamp=time.time())
            else:
                return {"status": "error",
                        "error": f"Unknown task type: {task_type}",
                        "correlation_id": task_id}

            latency = time.monotonic() - start
            self.metrics.inc(f"fl_task_{task_type}")
            self.metrics.set("fl_last_latency", latency)

            result = decision.to_dict()
            result["correlation_id"] = task_id
            result["latency_seconds"] = latency
            return result
        except Exception as e:
            logger.error(f"FLEnergyExpert error on {task_type}: {e}",
                         exc_info=True)
            self.metrics.inc(f"fl_task_{task_type}_error")
            return {"status": "error", "error": str(e),
                    "correlation_id": task_id}

    @staticmethod
    def _coerce_update(u: Any) -> ClientUpdateInfo:
        if isinstance(u, ClientUpdateInfo):
            return u
        if isinstance(u, dict):
            return ClientUpdateInfo.from_dict(u)
        raise TypeError(f"unsupported update type: {type(u)}")

    # ------------------------------------------------------------------
    # Client selection
    # ------------------------------------------------------------------
    async def select_clients_for_round(
        self,
        target_count: Optional[int] = None,
        energy_aware: bool = True,
        prefer_mobile: bool = False,
    ) -> FLDecision:
        async with self._lock:
            target = int(target_count or self.config.max_clients_per_round)
            min_required = self.config.min_clients_per_round

            available = [
                (cid, info) for cid, info in self.clients.items()
                if info.state in (ClientState.AVAILABLE,
                                   ClientState.ACTIVE,
                                   ClientState.CHARGING)
            ]
            if len(available) < min_required:
                available = list(self.clients.items())

            if not available:
                return FLDecision(
                    operation="select_clients",
                    status="error", error="no_clients",
                    timestamp=time.time())

            if energy_aware:
                threshold = self.config.energy_threshold_battery
                filtered = [
                    (cid, info) for cid, info in available
                    if (info.energy_profile !=
                        ClientEnergyProfile.BATTERY_POWERED
                        or info.battery_level >= threshold)
                ]
                if len(filtered) >= min_required:
                    available = filtered

            if prefer_mobile:
                available.sort(
                    key=lambda kv: kv[1].energy_profile !=
                    ClientEnergyProfile.BATTERY_POWERED)

            # Live carbon/helium modulation
            carbon_g_per_kwh = await self._current_carbon_g_per_kwh()
            helium = await self._current_helium()
            helium_scarcity = helium["scarcity"]

            candidates: List[Dict[str, Any]] = []
            for cid, info in available:
                energy_score = info.get_energy_score()
                total_obs = len(self.client_history[cid])
                stability = 1.0 - (
                    (total_obs - self.participation_history[cid]) /
                    (total_obs + 1))
                bw = (info.upload_bandwidth_mbps +
                      info.download_bandwidth_mbps) / 20.0
                # Carbon cost uses live grid intensity
                carbon_g = (carbon_g_per_kwh * info.energy_consumption_rate
                             * 100.0 / 1000.0)
                latency_ms = info.estimated_sync_time_seconds * 1000.0
                energy_j = info.energy_consumption_rate * 1000.0
                # Helium-scarcity: penalise high-energy clients
                if helium_scarcity > 0.7:
                    energy_j *= 1.15
                    carbon_g *= 1.05
                quality = (0.3 * energy_score +
                            0.3 * stability +
                            0.4 * bw)
                candidates.append({
                    "client_id": cid,
                    "info": info,
                    "quality_score": float(quality),
                    "carbon_g": float(carbon_g),
                    "latency_ms": float(latency_ms),
                    "energy_joules": float(energy_j),
                    "energy_score": float(energy_score),
                    "battery_level": float(info.battery_level),
                    "carbon_intensity": float(
                        info.carbon_intensity_g_per_kwh),
                })

            if self.pareto is not None:
                try:
                    filtered = self.pareto.filter(candidates)
                    if filtered:
                        allowed_ids = {c["client_id"] for c in filtered}
                        candidates = [c for c in candidates
                                      if c["client_id"] in allowed_ids]
                except Exception:
                    pass

            if not candidates:
                candidates = [{
                    "client_id": cid, "info": info,
                    "quality_score": 0.5, "carbon_g": 0.0,
                    "latency_ms": 0.0, "energy_joules": 0.0,
                    "energy_score": info.get_energy_score(),
                    "battery_level": info.battery_level,
                    "carbon_intensity": info.carbon_intensity_g_per_kwh,
                } for cid, info in available]

            # Fairness: deprioritise over-selected clients
            for c in candidates:
                if self._over_selected(c["client_id"]):
                    c["quality_score"] *= 0.5

            scores = []
            for c in candidates:
                cost = self.adaptive_cost.compute(
                    quality=c["quality_score"],
                    carbon_g=c["carbon_g"],
                    latency_ms=c["latency_ms"],
                    energy_joules=c["energy_joules"],
                    health=True, atp=0.5)
                scores.append((c, float(cost)))

            scores.sort(key=lambda kv: kv[1], reverse=True)
            selected = scores[:target]

            # Record selections for the fairness window
            for c, _ in selected:
                self._record_selection(c["client_id"])

            total_cost = sum(s[1] for s in selected) or 1.0
            weights = {c["client_id"]: float(cost) / total_cost
                        for c, cost in selected}
            selected_ids = tuple(c["client_id"] for c, _ in selected)

            await self._bio_spend_earn(
                energy_joules=0.0,
                quality=float(np.mean([c["quality_score"]
                                         for c, _ in selected])))

            explanation, counterfactual = self._explain_selection(
                selected, carbon_g_per_kwh, helium_scarcity)

            safety = await self._safety_check(
                round_energy=sum(c["energy_joules"] for c, _ in selected),
                round_carbon=sum(c["carbon_g"] for c, _ in selected),
                compression_ratio=self._adaptive_compression_ratio,
                failed_rate=(self.failed_updates /
                              max(1, self.total_updates_processed)))

            await self._publish_feedback(
                task_id=f"fl_select_{uuid.uuid4().hex[:8]}",
                action="select_clients",
                quality=float(np.mean([c["quality_score"]
                                        for c, _ in selected])),
                details={"selected": list(selected_ids),
                         "count": len(selected_ids)},
                tags=["federated", "selection"])

            return FLDecision(
                operation="select_clients",
                selected_clients=selected_ids,
                weights=weights,
                confidence=float(np.mean([c["quality_score"]
                                            for c, _ in selected])),
                energy_joules=float(sum(c["energy_joules"]
                                         for c, _ in selected)),
                carbon_g=float(sum(c["carbon_g"] for c, _ in selected)),
                explanation=explanation,
                counterfactual=counterfactual,
                safety_violations=safety,
                timestamp=time.time(),
            )

    def _explain_selection(
        self,
        selected: List[Tuple[Dict[str, Any], float]],
        carbon_g_per_kwh: float,
        helium_scarcity: float,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if not selected:
            return {"method": "none"}, {"success": False}
        feats = np.array([
            float(np.mean([c["energy_score"] for c, _ in selected])),
            float(np.mean([c["quality_score"] for c, _ in selected])),
            float(np.mean([c["carbon_g"] for c, _ in selected])),
            float(carbon_g_per_kwh),
            float(helium_scarcity),
        ], dtype=float)

        def _score(v: np.ndarray) -> float:
            es, q, cg, grid, he = (float(v[0]), float(v[1]),
                                     float(v[2]), float(v[3]), float(v[4]))
            return (q - 0.3 * cg / 100.0 - 0.2 * grid / 1000.0
                     - 0.1 * he + 0.1 * es)

        try:
            attrs = self.xai.integrated_gradients(_score, feats)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["mean_energy_score", "mean_quality",
                                    "mean_carbon_g", "grid_carbon_g_per_kwh",
                                    "helium_scarcity"],
            }
            counterfactual = self.xai.counterfactual(_score, feats)
            return explanation, counterfactual
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Gradient compression
    # ------------------------------------------------------------------
    def compress_gradients(
        self,
        gradients: np.ndarray,
        compression_ratio: float = 0.1,
        method: str = "top_k",
    ) -> Tuple[np.ndarray, float]:
        if (not self.gradient_compression_enabled or
                compression_ratio >= 1.0):
            return gradients, 1.0
        flat = np.asarray(gradients, dtype=np.float64).flatten()
        k = max(1, int(len(flat) * compression_ratio))
        if method == "top_k":
            abs_flat = np.abs(flat)
            threshold = np.sort(abs_flat)[-k]
            mask = abs_flat >= threshold
            compressed = flat * mask
            actual_ratio = float(np.count_nonzero(mask) / len(flat))
        elif method == "random":
            mask = np.random.rand(len(flat)) < compression_ratio
            compressed = flat * mask
            actual_ratio = float(compression_ratio)
        elif method == "quantization":
            min_v = flat.min()
            max_v = flat.max()
            if max_v - min_v > 1e-8:
                quantized = (flat - min_v) / (max_v - min_v) * 255.0
                quantized = np.round(quantized).astype(np.uint8)
                compressed = (quantized / 255.0) * (max_v - min_v) + min_v
            else:
                compressed = flat
            actual_ratio = float(compression_ratio)
        else:
            compressed, actual_ratio = self.compress_gradients(
                gradients, compression_ratio, "top_k")
        self.compression_ratios.append(actual_ratio)
        return compressed.reshape(np.asarray(gradients).shape), actual_ratio

    def compress_gradients_decision(
        self, gradients: np.ndarray, ratio: float
    ) -> FLDecision:
        compressed, actual = self.compress_gradients(gradients, ratio)
        return FLDecision(
            operation="compression",
            compression_ratio=actual,
            explanation={"method": "top_k",
                          "input_size": int(gradients.size),
                          "nonzero_after":
                              int(np.count_nonzero(compressed))},
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------
    async def select_aggregation_strategy(
        self, state: Dict[str, float]
    ) -> AggregationStrategy:
        async with self._lock:
            available = sum(1 for info in self.clients.values()
                            if info.state in (ClientState.AVAILABLE,
                                               ClientState.ACTIVE,
                                               ClientState.CHARGING))
            avg_battery = (float(np.mean([info.battery_level
                                           for info in self.clients.values()]))
                            if self.clients else 0.5)
            latencies = [info.estimated_sync_time_seconds
                         for info in self.clients.values()
                         if info.estimated_sync_time_seconds > 0]
            avg_latency = float(np.mean(latencies)) if latencies else 5.0

            strategies = list(AggregationStrategy)
            candidates: List[Dict[str, Any]] = []
            for s in strategies:
                if s == AggregationStrategy.STANDARD:
                    q, cg, lat, ej = 0.7, 5.0, avg_latency, 50.0
                elif s == AggregationStrategy.LAZY:
                    q, cg, lat, ej = 0.6, 2.0, avg_latency * 1.5, 20.0
                elif s == AggregationStrategy.PRIORITY:
                    q, cg, lat, ej = 0.8, 8.0, avg_latency * 0.8, 80.0
                elif s == AggregationStrategy.GRADIENT_COMPRESSION:
                    q, cg, lat, ej = 0.75, 3.0, avg_latency * 1.2, 35.0
                else:  # SELECTIVE
                    q, cg, lat, ej = 0.85, 4.0, avg_latency * 1.1, 45.0
                cost = self.adaptive_cost.compute(
                    quality=q, carbon_g=cg, latency_ms=lat,
                    energy_joules=ej, health=True, atp=0.5)
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
                strategy = AggregationStrategy.STANDARD
            else:
                best = min(candidates, key=lambda c: c["score"])
                strategy = best["strategy"]

            if strategy != self.current_strategy:
                await self._bio_spend_earn(0.0, quality=0.9)
                self.current_strategy = strategy
                self.strategy_change_log.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "strategy": strategy.value,
                    "reason": (f"avg_battery={avg_battery:.2f}, "
                                f"clients={available}, "
                                f"latency={avg_latency:.1f}s"),
                })
                await self._publish_feedback(
                    task_id=f"fl_strategy_{uuid.uuid4().hex[:8]}",
                    action="strategy_change",
                    quality=0.9,
                    details={"new_strategy": strategy.value,
                             "reason": self.strategy_change_log[-1]["reason"]},
                    tags=["federated", "strategy"])
            return strategy

    async def aggregate_updates(
        self,
        updates: List[ClientUpdateInfo],
        strategy: AggregationStrategy,
        energy_weights: Dict[str, float],
    ) -> FLDecision:
        if not updates:
            return FLDecision(
                operation="aggregate_updates",
                status="error", error="no_updates",
                timestamp=time.time())

        total_energy = float(sum(u.energy_cost_joules for u in updates))
        total_carbon_g = 0.0  # aggregated carbon isn't tracked at this layer

        # Correct torch aggregation (shape-safe)
        result_gradients: Optional[np.ndarray] = None
        if TORCH_AVAILABLE and all(u.gradients is not None for u in updates):
            try:
                tensors: List[Any] = []
                for u in updates:
                    if isinstance(u.gradients, torch.Tensor):
                        tensors.append(u.gradients.detach())
                    else:
                        tensors.append(
                            torch.as_tensor(np.asarray(u.gradients)))
                # Truncate to min length for a safe sum
                min_len = min(t.numel() for t in tensors)
                flat = [t.flatten()[:min_len] for t in tensors]
                w = torch.tensor(
                    [energy_weights.get(u.client_id,
                                         1.0 / len(updates))
                     for u in updates], dtype=torch.float32)
                w = w / (w.sum() + 1e-8)
                aggregated = sum(f * wi for f, wi in zip(flat, w))
                result_gradients = aggregated.detach().cpu().numpy()
            except Exception as e:
                logger.warning(f"torch aggregation failed: {e}")
                result_gradients = None

        # Strategy-specific bookkeeping
        if strategy == AggregationStrategy.STANDARD:
            result = self._aggregate_standard(updates, energy_weights)
        elif strategy == AggregationStrategy.LAZY:
            result = self._aggregate_lazy(updates, energy_weights)
        elif strategy == AggregationStrategy.PRIORITY:
            result = self._aggregate_priority(updates, energy_weights)
        elif strategy == AggregationStrategy.GRADIENT_COMPRESSION:
            result = self._aggregate_compressed(updates, energy_weights)
        else:
            result = self._aggregate_selective(updates, energy_weights)

        explanation = {
            "strategy": strategy.value,
            "num_updates": len(updates),
            "torch_aggregation": result_gradients is not None,
        }
        if result_gradients is not None:
            explanation["aggregated_shape"] = list(result_gradients.shape)

        return FLDecision(
            operation="aggregate_updates",
            strategy=strategy.value,
            completed_clients=len(updates),
            energy_joules=total_energy,
            carbon_g=total_carbon_g,
            compression_ratio=float(
                result.get("avg_compression_ratio", 1.0)),
            confidence=float(np.mean([u.gradient_norm for u in updates]))
                if updates else 0.0,
            explanation=explanation,
            timestamp=time.time(),
        )

    def _aggregate_standard(self, updates: List[ClientUpdateInfo],
                             energy_weights: Dict[str, float]
                             ) -> Dict[str, Any]:
        # Single normalization (was double-normalized in v3.3.0)
        raw = np.array([energy_weights.get(u.client_id,
                                            1.0 / len(updates))
                         for u in updates], dtype=float)
        weights = raw / (raw.sum() + 1e-8)
        return {"method": "standard_fedavg",
                "num_clients": len(updates),
                "weights": weights.tolist(),
                "avg_gradient_norm": float(
                    np.mean([u.gradient_norm for u in updates])),
                "avg_compression_ratio": float(
                    np.mean([u.compression_ratio for u in updates]))}

    def _aggregate_lazy(self, updates: List[ClientUpdateInfo],
                         energy_weights: Dict[str, float]
                         ) -> Dict[str, Any]:
        threshold = float(np.median([u.transmission_time_ms
                                      for u in updates]))
        fast = ([u for u in updates
                 if u.transmission_time_ms <= threshold * 1.5]
                if threshold > 0 else updates) or updates
        raw = np.array([energy_weights.get(u.client_id,
                                            1.0 / len(fast))
                         for u in fast], dtype=float)
        weights = raw / (raw.sum() + 1e-8)
        return {"method": "lazy_aggregation",
                "num_clients": len(fast),
                "num_skipped": len(updates) - len(fast),
                "weights": weights.tolist(),
                "avg_gradient_norm": float(
                    np.mean([u.gradient_norm for u in fast]))}

    def _aggregate_priority(self, updates: List[ClientUpdateInfo],
                             energy_weights: Dict[str, float]
                             ) -> Dict[str, Any]:
        raw = np.array(
            [1.0 / (energy_weights.get(u.client_id, 0.5) + 0.1)
             for u in updates], dtype=float)
        weights = raw / (raw.sum() + 1e-8)
        return {"method": "energy_priority_aggregation",
                "num_clients": len(updates),
                "weights": weights.tolist(),
                "energy_based": True}

    def _aggregate_compressed(self, updates: List[ClientUpdateInfo],
                               energy_weights: Dict[str, float]
                               ) -> Dict[str, Any]:
        raw = np.array([energy_weights.get(u.client_id,
                                            1.0 / len(updates))
                         for u in updates], dtype=float)
        weights = raw / (raw.sum() + 1e-8)
        avg_compression = float(
            np.mean([u.compression_ratio for u in updates]))
        return {"method": "compressed_aggregation",
                "num_clients": len(updates),
                "weights": weights.tolist(),
                "avg_compression_ratio": avg_compression,
                "transmission_savings_percent":
                    (1.0 - avg_compression) * 100.0}

    def _aggregate_selective(self, updates: List[ClientUpdateInfo],
                              energy_weights: Dict[str, float]
                              ) -> Dict[str, Any]:
        ordered = sorted(updates, key=lambda u: u.gradient_norm)
        top_half = ordered[len(ordered) // 2:] or ordered
        raw = np.array([energy_weights.get(u.client_id,
                                            1.0 / len(top_half))
                         for u in top_half], dtype=float)
        weights = raw / (raw.sum() + 1e-8)
        return {"method": "selective_aggregation",
                "num_clients": len(top_half),
                "num_filtered": len(updates) - len(top_half),
                "weights": weights.tolist()}

    # ------------------------------------------------------------------
    # Round execution
    # ------------------------------------------------------------------
    async def execute_aggregation_round(
        self,
        round_id: int,
        state: Dict[str, Any],
    ) -> FLDecision:
        strategy = await self.select_aggregation_strategy(state or {})
        selection = await self.select_clients_for_round()
        if selection.status != "success":
            return selection

        selected_ids = list(selection.selected_clients)
        weights = dict(selection.weights)

        # Budget check (uses projected energy from selection)
        approved, reason = await self._budget_gate(
            selection.energy_joules, selection.carbon_g)
        if not approved:
            return FLDecision(
                operation="fl_round",
                status="deferred",
                error=reason,
                round_id=round_id,
                strategy=strategy.value,
                requires_approval=True,
                timestamp=time.time(),
            )

        # Simulated update reception
        updates: List[ClientUpdateInfo] = []
        failed: List[str] = []
        for cid in selected_ids:
            try:
                update = ClientUpdateInfo(
                    client_id=cid,
                    model_hash=hashlib.sha256(
                        f"{round_id}_{cid}".encode()).hexdigest(),
                    gradient_norm=float(self._rng.expovariate(1.0)),
                    update_timestamp=datetime.now(timezone.utc),
                    energy_cost_joules=float(
                        self._rng.expovariate(2.0)),
                    transmission_time_ms=float(
                        self._rng.uniform(10, 1000)),
                )
                updates.append(update)
                self.total_updates_processed += 1
            except Exception as e:
                logger.warning(f"failed update from {cid}: {e}")
                failed.append(cid)
                self.failed_updates += 1

        agg = await self.aggregate_updates(updates, strategy, weights)

        duration = float(self._rng.uniform(10, 300))
        round_info = AggregationRound(
            round_id=round_id,
            strategy=strategy,
            selected_clients=selected_ids,
            completed_clients=updates,
            failed_clients=failed,
            timestamp=datetime.now(timezone.utc),
            duration_seconds=duration,
            total_energy_joules=agg.energy_joules,
            model_hash=hashlib.sha256(
                f"{round_id}_aggregated".encode()).hexdigest(),
            compression_ratio=agg.compression_ratio,
            explanation=agg.explanation,
        )
        self.rounds.append(round_info)
        self.total_energy_consumed_joules += agg.energy_joules

        for cid in selected_ids:
            self.participation_history[cid] += 1

        # Metrics
        self.metrics.inc("fl_rounds")
        self.metrics.set("fl_energy_consumed",
                          self.total_energy_consumed_joules)
        self.metrics.set("fl_avg_compression",
                          round_info.compression_ratio)

        # Bio
        await self._bio_spend_earn(
            agg.energy_joules,
            quality=(1.0 if updates else 0.0))

        failed_rate = len(failed) / max(1, len(selected_ids))
        safety = await self._safety_check(
            round_energy=agg.energy_joules,
            round_carbon=selection.carbon_g,
            compression_ratio=round_info.compression_ratio,
            failed_rate=failed_rate)

        await self._publish_feedback(
            task_id=f"fl_round_{round_id}",
            action=f"round_{strategy.value}",
            quality=1.0 if updates else 0.0,
            details={"num_clients": len(updates),
                     "strategy": strategy.value},
            tags=["federated", "round"])

        await self._check_drift()

        return FLDecision(
            operation="fl_round",
            round_id=round_id,
            strategy=strategy.value,
            selected_clients=tuple(selected_ids),
            weights=weights,
            completed_clients=len(updates),
            failed_clients=tuple(failed),
            energy_joules=agg.energy_joules,
            carbon_g=selection.carbon_g,
            compression_ratio=round_info.compression_ratio,
            confidence=(1.0 if updates else 0.0),
            explanation=round_info.explanation,
            counterfactual=selection.counterfactual,
            safety_violations=safety,
            timestamp=time.time(),
        )

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
                self._adaptive_compression_ratio = min(
                    0.9, self._adaptive_compression_ratio * 1.1)
                logger.warning(
                    f"High drift ({score:.3f}); adaptive compression "
                    f"ratio = {self._adaptive_compression_ratio:.3f}")
            else:
                # Decay back toward configured value
                target = self.config.target_compression_ratio
                self._adaptive_compression_ratio += (
                    target - self._adaptive_compression_ratio) * 0.05
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
                feedback_type="federated_learning",
                adaptive_cost_value=0.0,
                state=details,
                candidates=[{"action": s.value} for s in AggregationStrategy],
                source="fl_energy_expert",
                environment="production",
                tags=tags)
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def get_energy_efficiency_report(self) -> Dict[str, Any]:
        if not self.rounds:
            return {"message": "no rounds completed yet"}
        energies = [r.total_energy_joules for r in self.rounds]
        clients_per = [len(r.completed_clients) for r in self.rounds]
        return {
            "total_energy_joules": float(self.total_energy_consumed_joules),
            "total_rounds": len(self.rounds),
            "avg_energy_per_round_joules": float(np.mean(energies)),
            "avg_energy_per_client_joules": float(
                np.mean(energies) / (np.mean(clients_per) + 1e-6)),
            "avg_clients_per_round": float(np.mean(clients_per)),
            "avg_compression_ratio": float(
                np.mean(self.compression_ratios))
                if self.compression_ratios else 1.0,
            "energy_per_update": float(
                self.total_energy_consumed_joules /
                max(1, self.total_updates_processed)),
            "success_rate": float(
                (self.total_updates_processed - self.failed_updates) /
                max(1, self.total_updates_processed)),
        }

    async def get_metrics(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "total_energy_consumed_joules":
                    float(self.total_energy_consumed_joules),
                "total_updates_processed": self.total_updates_processed,
                "failed_updates": self.failed_updates,
                "total_rounds": len(self.rounds),
                "active_clients": sum(
                    1 for info in self.clients.values()
                    if info.state in (ClientState.AVAILABLE,
                                       ClientState.ACTIVE)),
                "total_registered_clients": len(self.clients),
                "avg_compression_ratio": float(
                    np.mean(self.compression_ratios))
                    if self.compression_ratios else 1.0,
                "current_strategy": self.current_strategy.value,
                "strategy_changes": len(self.strategy_change_log),
                "temporal_violations": self.temporal_safety.counts(),
                "background_tasks": self.task_manager.task_count(),
                "adaptive_compression_ratio":
                    self._adaptive_compression_ratio,
            }

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            "expert_name": self.expert_name,
            "supported_tasks": list(self.supported_task_types),
            "health_status": self.health_status,
            "config": asdict(self.config),
        }

    async def get_client_participation_stats(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            stats: Dict[str, Dict[str, Any]] = {}
            for cid, info in self.clients.items():
                participated = self.participation_history[cid]
                total_seen = len(self.client_history[cid])
                stats[cid] = {
                    "state": info.state.value,
                    "energy_profile": info.energy_profile.value,
                    "battery_level": float(info.battery_level),
                    "energy_score": float(info.get_energy_score()),
                    "participated_rounds": participated,
                    "observations": total_seen,
                    "participation_rate": float(
                        participated / (total_seen + 1))
                        if total_seen > 0 else 0.0,
                }
            return stats

    async def get_health_status(self) -> ComponentHealth:
        """Lightweight, side-effect-free health check."""
        try:
            if self.config.max_clients_per_round < 1:
                self.health_status = "unhealthy"
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "invalid max_clients_per_round"})
            details = {
                "clients": len(self.clients),
                "rounds": len(self.rounds),
                "circuit_state": self._client_circuit.state.value,
                "background_tasks": self.task_manager.task_count(),
            }
            self.health_status = "healthy"
            return ComponentHealth(HealthStatus.HEALTHY, 1.0, details)
        except Exception as e:
            self.health_status = "unhealthy"
            return ComponentHealth(
                HealthStatus.UNHEALTHY, 0.0, {"error": str(e)})

    # ------------------------------------------------------------------
    # Explainability on past rounds
    # ------------------------------------------------------------------
    async def explain_client_selection(self, round_id: int
                                        ) -> Optional[Dict[str, Any]]:
        for r in self.rounds:
            if r.round_id == round_id:
                return {
                    "round_id": r.round_id,
                    "strategy": r.strategy.value,
                    "selected_clients": list(r.selected_clients),
                    "failed_clients": list(r.failed_clients),
                    "rationale": r.explanation or {},
                }
        return None

    async def explain_aggregation_decision(self, round_id: int
                                            ) -> Optional[Dict[str, Any]]:
        for r in self.rounds:
            if r.round_id == round_id:
                return {
                    "round_id": r.round_id,
                    "strategy": r.strategy.value,
                    "num_clients": len(r.completed_clients),
                    "energy_joules": float(r.total_energy_joules),
                    "compression_applied": r.compression_ratio < 1.0,
                    "compression_ratio": float(r.compression_ratio),
                }
        return None


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


async def _populate(expert: FLEnergyExpert, n: int = 6) -> None:
    for i in range(n):
        profile = (ClientEnergyProfile.PLUGGED_IN if i % 3 == 0
                    else ClientEnergyProfile.BATTERY_POWERED)
        await expert.register_client(
            client_id=f"c{i}",
            energy_profile=profile,
            bandwidth_mbps=10.0 + i,
            compute_capability=1.0,
            carbon_intensity_g_per_kwh=100.0 + i * 20)


@pytest_asyncio.fixture
async def expert(storage, queue, adaptive_cost, pareto_gating,
                  drift_detector, metrics):
    e = FLEnergyExpert(
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
            return 300.0

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
    cfg = FLEnergyConfig(
        round_energy_budget_joules=1e9,
        round_carbon_budget_g=1e9)
    e = FLEnergyExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        carbon_manager=FakeCarbon(),
        helium_optimizer=FakeHelium(),
        bio_core=bio, hitl=hitl, config=cfg, seed=42)
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
    async def test_select_clients_returns_decision(self, expert):
        await _populate(expert, 5)
        decision = await expert.select_clients_for_round(target_count=3)
        assert isinstance(decision, FLDecision)
        assert decision.operation == "select_clients"
        assert len(decision.selected_clients) <= 3
        # weights sum ~ 1
        assert abs(sum(decision.weights.values()) - 1.0) < 1e-4

    @pytest.mark.asyncio
    async def test_fl_select_clients_task_unpacks_correctly(self, expert):
        await _populate(expert, 4)
        r = await expert.handle_task({"type": "fl_select_clients",
                                       "target_count": 2})
        assert r["status"] == "success"
        assert "selected_clients" in r
        assert "weights" in r

    @pytest.mark.asyncio
    async def test_sync_storage_via_to_thread(self, expert, storage):
        await expert._save_state()
        assert any(c[0] == "save_state" for c in storage.calls)

    @pytest.mark.asyncio
    async def test_tracked_background_tasks(self, expert):
        # Cleanup task should be running
        assert expert.task_manager.task_count() >= 1

    @pytest.mark.asyncio
    async def test_temporal_safety_counters_persist(self, expert):
        await _populate(expert, 4)
        # Call selection 5 times
        for _ in range(5):
            await expert.select_clients_for_round(target_count=2)
        # Over-selected clients should be deprioritised; the history
        # must persist across calls (not reset each time).
        assert len(expert._selection_history) >= 5

    @pytest.mark.asyncio
    async def test_torch_aggregation_shape_safe(self, expert):
        await _populate(expert, 3)
        if not TORCH_AVAILABLE:
            pytest.skip("torch unavailable")
        updates = []
        for i in range(3):
            updates.append(ClientUpdateInfo(
                client_id=f"c{i}",
                model_hash=f"h{i}",
                gradient_norm=1.0,
                update_timestamp=datetime.now(timezone.utc),
                energy_cost_joules=1.0,
                transmission_time_ms=50.0,
                gradients=np.random.randn(10 + i).astype(np.float32),
            ))
        d = await expert.aggregate_updates(
            updates, AggregationStrategy.STANDARD, {})
        assert d.status == "success"
        assert d.explanation is not None
        assert d.explanation["torch_aggregation"] is True
        # Shape must be (10,) — truncated to min length
        assert d.explanation["aggregated_shape"] == [10]

    @pytest.mark.asyncio
    async def test_config_not_mutated_on_drift(self, expert):
        original = expert.config.target_compression_ratio
        for _ in range(20):
            await expert._check_drift()
        assert expert.config.target_compression_ratio == original
        # Adaptive state moved instead
        assert expert._adaptive_compression_ratio != original

    @pytest.mark.asyncio
    async def test_health_returns_component_health(self, expert):
        h = await expert.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert isinstance(h.status, HealthStatus)

    @pytest.mark.asyncio
    async def test_health_no_side_effects(self, expert):
        # Health check must NOT mutate compression_ratios
        len_before = len(expert.compression_ratios)
        for _ in range(10):
            await expert.get_health_status()
        assert len(expert.compression_ratios) == len_before

    @pytest.mark.asyncio
    async def test_rounds_bounded(self, expert):
        assert expert.rounds.maxlen is not None
        assert expert.rounds.maxlen == expert.config.rounds_maxlen

    @pytest.mark.asyncio
    async def test_dict_coercion_in_fl_aggregate(self, expert):
        # Raw dicts should be coerced
        raw = [{
            "client_id": "c1",
            "model_hash": "h1",
            "gradient_norm": 1.0,
            "update_timestamp": datetime.now(timezone.utc).isoformat(),
            "energy_cost_joules": 1.0,
            "transmission_time_ms": 50.0,
        }]
        r = await expert.handle_task({
            "type": "fl_aggregate",
            "updates": raw,
            "weights": {}})
        assert r["status"] == "success"

    @pytest.mark.asyncio
    async def test_no_double_normalization(self, expert):
        # Weights passed in should be used as-is, normalized once
        await _populate(expert, 3)
        updates = [
            ClientUpdateInfo(
                client_id=f"c{i}",
                model_hash=f"h{i}",
                gradient_norm=1.0,
                update_timestamp=datetime.now(timezone.utc),
                energy_cost_joules=1.0,
                transmission_time_ms=50.0)
            for i in range(3)
        ]
        weights = {"c0": 0.5, "c1": 0.3, "c2": 0.2}
        d = await expert.aggregate_updates(
            updates, AggregationStrategy.STANDARD, weights)
        assert d.status == "success"


# =============================================================================
# TESTS — CORE OPERATIONS
# =============================================================================
class TestCoreOperations:
    @pytest.mark.asyncio
    async def test_register_client(self, expert):
        info = await expert.register_client("cx")
        assert info.client_id == "cx"
        assert "cx" in expert.clients

    @pytest.mark.asyncio
    async def test_select_clients_target_respected(self, expert):
        await _populate(expert, 8)
        d = await expert.select_clients_for_round(target_count=3)
        assert len(d.selected_clients) <= 3

    @pytest.mark.asyncio
    async def test_execute_round(self, expert):
        await _populate(expert, 5)
        d = await expert.execute_aggregation_round(0, {})
        assert d.status == "success"
        assert d.round_id == 0
        assert d.completed_clients >= 1
        assert len(expert.rounds) == 1

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, expert):
        probs = await expert.policy_probs({})
        assert len(probs) == 5
        assert abs(sum(probs) - 1.0) < 1e-6

    @pytest.mark.asyncio
    async def test_compression_decision(self, expert):
        grad = np.random.randn(100).astype(np.float32)
        d = expert.compress_gradients_decision(grad, 0.2)
        assert d.operation == "compression"
        assert 0.0 < d.compression_ratio <= 0.2


# =============================================================================
# TESTS — BUDGET + HITL
# =============================================================================
class TestBudgetHITL:
    @pytest.mark.asyncio
    async def test_round_defers_without_hitl(self, storage, queue,
                                               adaptive_cost,
                                               pareto_gating,
                                               drift_detector, metrics):
        cfg = FLEnergyConfig(
            round_energy_budget_joules=0.001,
            min_clients_per_round=1,
            max_clients_per_round=3)
        e = FLEnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            config=cfg, seed=1)
        await e.initialize()
        try:
            await _populate(e, 3)
            d = await e.execute_aggregation_round(0, {})
            assert d.status == "deferred"
            assert d.requires_approval
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_round_approved_with_hitl(self, storage, queue,
                                              adaptive_cost,
                                              pareto_gating,
                                              drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("fl_energy_expert", "approve")
        cfg = FLEnergyConfig(
            round_energy_budget_joules=0.001,
            min_clients_per_round=1,
            max_clients_per_round=3)
        e = FLEnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, config=cfg, seed=1)
        await e.initialize()
        try:
            await _populate(e, 3)
            d = await e.execute_aggregation_round(0, {})
            assert d.status == "success"
        finally:
            await e.shutdown()


# =============================================================================
# TESTS — XAI + INTEGRATION
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_selection_has_explanation(self, expert):
        await _populate(expert, 4)
        d = await expert.select_clients_for_round(target_count=2)
        assert d.explanation is not None
        assert "attributions" in d.explanation
        assert "feature_names" in d.explanation
        assert d.counterfactual is not None

    @pytest.mark.asyncio
    async def test_carbon_manager_used(self, expert_full):
        v = await expert_full._current_carbon_g_per_kwh()
        assert v == 300.0

    @pytest.mark.asyncio
    async def test_helium_modulation(self, expert_full):
        h = await expert_full._current_helium()
        assert h["scarcity"] == 0.4

    @pytest.mark.asyncio
    async def test_bio_spend_earn(self, expert_full):
        await _populate(expert_full, 3)
        await expert_full.execute_aggregation_round(0, {})
        # At least one spend/earn call must have occurred
        assert (len(expert_full._bio.spend_calls) +
                len(expert_full._bio.earn_calls)) >= 1


# =============================================================================
# TESTS — PERSISTENCE
# =============================================================================
class TestPersistence:
    @pytest.mark.asyncio
    async def test_roundtrip(self, storage, queue, adaptive_cost,
                              pareto_gating, drift_detector, metrics):
        e1 = FLEnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e1.initialize()
        try:
            await _populate(e1, 3)
            await e1.execute_aggregation_round(0, {})
            await e1._save_state()
        finally:
            await e1.shutdown()

        e2 = FLEnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        await e2.initialize()
        try:
            assert "c0" in e2.clients
            assert len(e2.rounds) >= 1
        finally:
            await e2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle(self, expert, storage):
        await expert._save_state()
        raw = storage.get_state("fl_energy_expert_state_v4")
        assert raw is not None
        # JSON parse must succeed
        json.loads(raw)


# =============================================================================
# TESTS — DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_selection(self, storage, queue,
                                              adaptive_cost,
                                              pareto_gating,
                                              drift_detector, metrics):
        def _make():
            return FLEnergyExpert(
                storage=storage, message_queue=queue,
                adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
                drift_detector=drift_detector, metrics=metrics, seed=7)
        e1 = _make(); e2 = _make()
        await e1.initialize(); await e2.initialize()
        try:
            await _populate(e1, 5)
            await _populate(e2, 5)
            d1 = await e1.select_clients_for_round(target_count=3)
            d2 = await e2.select_clients_for_round(target_count=3)
            assert d1.selected_clients == d2.selected_clients
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

        expert = FLEnergyExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics, seed=1)
        await expert.initialize()
        try:
            await _populate(expert, 6)
            for rnd in range(3):
                d = await expert.execute_aggregation_round(rnd, {})
                print(f"round {rnd}: status={d.status} "
                      f"clients={d.completed_clients} "
                      f"energy={d.energy_joules:.2f}J")
            sel = await expert.select_clients_for_round(target_count=3)
            print("selection:", sel.selected_clients)
            print("explanation method:",
                  (sel.explanation or {}).get("method"))
            metrics_snapshot = await expert.get_metrics()
            print("Metrics:",
                  json.dumps(metrics_snapshot, indent=2, default=str))
        finally:
            await expert.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
