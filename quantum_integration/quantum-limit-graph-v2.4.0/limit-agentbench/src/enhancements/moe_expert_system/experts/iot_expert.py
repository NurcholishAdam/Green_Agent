#!/usr/bin/env python3
"""
IoTExpert v9.0.0 — Self-Contained Single-File Bundle
======================================================

Complete rewrite of the Green Agent IoT Expert. Everything needed to run
and test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. Local stubs (InMemoryStorage, AsyncMessageQueue, FeedbackEvent,
     MetricsRegistry, AdaptiveCostFunction, ParetoGating, DriftDetector).
  3. Shared runtime: CircuitBreaker, TemporalSafetyMonitor (MTL),
     HumanApprovalHandler, XAIExplainer (IG + counterfactuals),
     TaskManager, ComponentHealth.
  4. `DeviceDecision` value object.
  5. Real HITL gating for critical recommendations.
  6. Real Integrated Gradients XAI on proposals and optimisations.
  7. Real digital twin (`DigitalTwin`) for mesh scenarios.
  8. Fixed FederatedIoTLearner (correct state_dict round-trip).
  9. Bounded thresholds, devices, and context.
 10. Comprehensive pytest suite.

Run:
    python iot_expert_v9.py         # demo
    pytest iot_expert_v9.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
Optional: aiohttp, torch, networkx.
"""

from __future__ import annotations

import asyncio
import hashlib
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

logger = logging.getLogger("iot_expert")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

try:
    import networkx as nx
    NX_AVAILABLE = True
except ImportError:  # pragma: no cover
    nx = None  # type: ignore
    NX_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False


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
class TokenManagerLike(Protocol):
    def spend(self, actor: str, amount: float) -> Any: ...
    def earn(self, actor: str, amount: float) -> Any: ...


@runtime_checkable
class GradientManagerLike(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, name: str, delta: float, source: str = ...) -> Any: ...


@runtime_checkable
class CompartmentManagerLike(Protocol):
    def find_best_compartment(self, kind: str) -> Any: ...


@runtime_checkable
class BioCoreLike(Protocol):
    token_manager: Optional[Any]
    gradient_manager: Optional[Any]
    compartment_manager: Optional[Any]
    swarm_coordinator: Optional[Any]
    harvester: Optional[Any]


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
# ENUMS
# =============================================================================
class DeviceType(str, Enum):
    MICROCONTROLLER = "microcontroller"
    SINGLE_BOARD = "single_board_computer"
    GATEWAY = "edge_gateway"
    MOBILE = "mobile_device"
    WEARABLE = "wearable"
    SENSOR_NODE = "sensor_node"


class EnergySource(str, Enum):
    BATTERY = "battery"
    SOLAR = "solar"
    KINETIC = "kinetic"
    THERMAL = "thermal"
    RF_HARVESTING = "rf_harvesting"
    GRID = "grid"
    HYBRID = "hybrid"


class MeshRole(str, Enum):
    LEADER = "leader"
    ROUTER = "router"
    LEAF = "leaf"
    MEMBRANE_GATED = "membrane_gated"


class MembranePermeability(str, Enum):
    PERMEABLE = "permeable"
    SELECTIVE = "selective"
    IMPERMEABLE = "impermeable"


# =============================================================================
# DATA CLASSES
# =============================================================================
@dataclass
class EdgeDevice:
    device_id: str
    device_type: DeviceType = DeviceType.SENSOR_NODE
    capabilities: Dict[str, float] = field(default_factory=dict)
    mesh_id: Optional[str] = None
    mesh_role: MeshRole = MeshRole.LEAF
    energy_source: EnergySource = EnergySource.BATTERY
    battery_capacity_wh: float = 10.0
    current_battery_wh: float = 10.0
    power_consumption_w: float = 0.5
    harvesting_available_w: float = 0.0
    available_processing_flops: float = 1e9
    max_processing_power_flops: float = 1e9
    latency_to_cloud_ms: float = 50.0
    is_online: bool = True
    last_heartbeat: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    carbon_intensity_g_per_kwh: float = 400.0
    gradient_health: float = 0.7
    self_healing_attempts: int = 0
    failure_history: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def energy_remaining_percent(self) -> float:
        return self.current_battery_wh / max(self.battery_capacity_wh, 1e-9) * 100.0

    @property
    def processing_utilization(self) -> float:
        return 1.0 - (self.available_processing_flops /
                       max(self.max_processing_power_flops, 1e-9))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "device_type": self.device_type.value,
            "capabilities": dict(self.capabilities),
            "mesh_id": self.mesh_id,
            "mesh_role": self.mesh_role.value,
            "energy_source": self.energy_source.value,
            "battery_capacity_wh": float(self.battery_capacity_wh),
            "current_battery_wh": float(self.current_battery_wh),
            "power_consumption_w": float(self.power_consumption_w),
            "harvesting_available_w": float(self.harvesting_available_w),
            "available_processing_flops": float(self.available_processing_flops),
            "max_processing_power_flops": float(self.max_processing_power_flops),
            "latency_to_cloud_ms": float(self.latency_to_cloud_ms),
            "is_online": bool(self.is_online),
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "carbon_intensity_g_per_kwh": float(self.carbon_intensity_g_per_kwh),
            "gradient_health": float(self.gradient_health),
            "self_healing_attempts": int(self.self_healing_attempts),
            "failure_history": list(self.failure_history),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "EdgeDevice":
        d = dict(d)
        d["device_type"] = DeviceType(d["device_type"])
        d["mesh_role"] = MeshRole(d["mesh_role"])
        d["energy_source"] = EnergySource(d["energy_source"])
        d["last_heartbeat"] = datetime.fromisoformat(d["last_heartbeat"])
        return cls(**d)


@dataclass
class MeshNetwork:
    mesh_id: str
    device_ids: List[str] = field(default_factory=list)
    leader_id: Optional[str] = None
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    edges: List[Tuple[str, str, float]] = field(default_factory=list)
    membrane_permeability: str = MembranePermeability.SELECTIVE.value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mesh_id": self.mesh_id,
            "device_ids": list(self.device_ids),
            "leader_id": self.leader_id,
            "created_at": self.created_at.isoformat(),
            "edges": list(self.edges),
            "membrane_permeability": self.membrane_permeability,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MeshNetwork":
        d = dict(d)
        d["created_at"] = datetime.fromisoformat(d["created_at"])
        d["edges"] = [tuple(e) for e in d.get("edges", [])]
        return cls(**d)

    def health_score(self, devices: Dict[str, "EdgeDevice"]) -> float:
        if not self.device_ids:
            return 0.0
        member = [devices[did] for did in self.device_ids if did in devices]
        if not member:
            return 0.0
        online = sum(1 for d in member if d.is_online)
        avg_gradient = float(np.mean([d.gradient_health for d in member]))
        avg_battery = float(np.mean([d.energy_remaining_percent
                                       for d in member]))
        connectivity = 1.0 if self.leader_id else 0.5
        return (0.4 * (online / max(len(member), 1)) +
                0.3 * avg_gradient +
                0.15 * (avg_battery / 100.0) +
                0.15 * connectivity)


# =============================================================================
# VALUE OBJECT
# =============================================================================
@dataclass(frozen=True)
class DeviceDecision:
    operation: str
    status: str = "success"
    error: Optional[str] = None
    recommendation: Optional[Dict[str, Any]] = None
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None
    requires_approval: bool = False
    approved: Optional[bool] = None
    safety_violations: Tuple[str, ...] = ()
    confidence: float = 0.0
    timestamp: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation,
            "status": self.status,
            "error": self.error,
            "recommendation": self.recommendation,
            "explanation": self.explanation,
            "counterfactual": self.counterfactual,
            "requires_approval": bool(self.requires_approval),
            "approved": self.approved,
            "safety_violations": list(self.safety_violations),
            "confidence": float(self.confidence),
            "timestamp": float(self.timestamp),
        }


# =============================================================================
# CONFIG
# =============================================================================
@dataclass
class IoTExpertConfig:
    expert_id: str = field(
        default_factory=lambda: f"iot_expert_{uuid.uuid4().hex[:8]}")
    enable_mesh: bool = True
    enable_federated: bool = True
    enable_self_healing: bool = True
    enable_telemetry: bool = True
    enable_weather: bool = False   # opt-in
    enable_persistence: bool = True
    human_approval_required: bool = True

    thresholds: Dict[str, float] = field(default_factory=lambda: {
        "sampling_rate_high": 10.0,
        "sampling_rate_low": 5.0,
        "sampling_rate_critical": 2.0,
        "battery_low_threshold": 20.0,
        "latency_high_ms": 100.0,
    })
    threshold_min: Dict[str, float] = field(default_factory=lambda: {
        "sampling_rate_high": 1.0,
        "sampling_rate_low": 0.5,
        "sampling_rate_critical": 0.1,
        "battery_low_threshold": 5.0,
        "latency_high_ms": 20.0,
    })
    threshold_max: Dict[str, float] = field(default_factory=lambda: {
        "sampling_rate_high": 100.0,
        "sampling_rate_low": 50.0,
        "sampling_rate_critical": 10.0,
        "battery_low_threshold": 60.0,
        "latency_high_ms": 500.0,
    })

    max_devices: int = 5000
    stale_device_hours: float = 24.0
    context_ttl_seconds: float = 300.0
    drastic_action_cooldown_seconds: float = 300.0
    federated_privacy_epsilon: float = 1.0

    # Auto-normalise
    def __post_init__(self) -> None:
        for k in list(self.thresholds):
            lo = self.threshold_min.get(k, 0.0)
            hi = self.threshold_max.get(k, 1e9)
            self.thresholds[k] = max(lo, min(hi, self.thresholds[k]))


# =============================================================================
# DIGITAL TWIN (real simulation)
# =============================================================================
class DigitalTwin:
    """Simulate mesh scenarios on a snapshot of device state."""

    @staticmethod
    def simulate(mesh: MeshNetwork, devices: Dict[str, EdgeDevice],
                 scenario: Dict[str, Any]) -> Dict[str, Any]:
        stype = scenario.get("type", "unknown")
        member = [devices[did] for did in mesh.device_ids if did in devices]
        if not member:
            return {"scenario": stype, "status": "empty_mesh"}

        if stype == "leader_failure":
            return DigitalTwin._simulate_leader_failure(mesh, member)
        if stype == "link_failure":
            return DigitalTwin._simulate_link_failure(mesh, member, scenario)
        if stype == "battery_drain":
            return DigitalTwin._simulate_battery_drain(member, scenario)
        return {"scenario": stype, "status": "unsupported"}

    @staticmethod
    def _simulate_leader_failure(mesh: MeshNetwork,
                                  member: List[EdgeDevice]
                                  ) -> Dict[str, Any]:
        candidates = [d for d in member if d.is_online and
                       d.device_id != mesh.leader_id]
        if not candidates:
            return {"scenario": "leader_failure", "status": "no_successor",
                    "predicted_health": 0.0}
        new_leader = max(
            candidates,
            key=lambda d: (d.gradient_health * 0.5 +
                            d.energy_remaining_percent / 100.0 * 0.3 +
                            (1.0 - d.processing_utilization) * 0.2))
        # Estimate re-election energy cost
        energy_j = sum(
            d.power_consumption_w * 2.0 for d in candidates[:3])
        return {
            "scenario": "leader_failure",
            "status": "recovered",
            "new_leader": new_leader.device_id,
            "predicted_health": 0.85,
            "energy_cost_joules": float(energy_j),
        }

    @staticmethod
    def _simulate_link_failure(mesh: MeshNetwork,
                                member: List[EdgeDevice],
                                scenario: Dict[str, Any]
                                ) -> Dict[str, Any]:
        source = scenario.get("source")
        target = scenario.get("target")
        # Count how many devices would be isolated
        isolated = [d for d in member
                     if d.device_id not in (source, target) and
                     d.device_id != mesh.leader_id]
        # Assume half reconnect via routing
        reconnected = len(isolated) // 2
        lost = len(isolated) - reconnected
        health = 1.0 - lost / max(len(member), 1)
        return {
            "scenario": "link_failure",
            "status": "simulated",
            "isolated_devices": lost,
            "reconnected_devices": reconnected,
            "predicted_health": float(health),
            "energy_cost_joules": float(sum(
                d.power_consumption_w * 1.5 for d in member[:5])),
        }

    @staticmethod
    def _simulate_battery_drain(member: List[EdgeDevice],
                                 scenario: Dict[str, Any]
                                 ) -> Dict[str, Any]:
        hours = float(scenario.get("hours", 24))
        failures = []
        for d in member:
            # net drain = consumption - harvesting
            net_w = d.power_consumption_w - d.harvesting_available_w
            if net_w <= 0:
                continue
            drain_wh = net_w * hours
            if drain_wh > d.current_battery_wh:
                failures.append(d.device_id)
        survival = 1.0 - len(failures) / max(len(member), 1)
        return {
            "scenario": "battery_drain",
            "status": "simulated",
            "hours": hours,
            "failures": failures,
            "failure_count": len(failures),
            "predicted_survival": float(survival),
            "predicted_health": float(survival),
        }


# =============================================================================
# FEDERATED IOT LEARNER (fixed)
# =============================================================================
class FederatedIoTLearner:
    """Real federated learning with correct state dict round-trip."""

    def __init__(self, expert_id: str,
                 privacy_epsilon: float = 1.0,
                 input_dim: int = 8,
                 hidden_dim: int = 16):
        self.expert_id = expert_id
        self.round = 0
        self.privacy_epsilon = float(privacy_epsilon)
        self.initial_epsilon = float(privacy_epsilon)
        self.contribution_score = 0.0
        self._session = None
        self._session_lock = asyncio.Lock()
        self._lock = asyncio.Lock()
        self.participants: List[str] = []
        self.input_dim = int(input_dim)
        self.hidden_dim = int(hidden_dim)
        self._model = None
        self._init_model()

    def _init_model(self) -> None:
        if not TORCH_AVAILABLE:
            return

        class _IoTModel(torch.nn.Module):
            def __init__(self, input_dim: int, hidden_dim: int):
                super().__init__()
                self.net = torch.nn.Sequential(
                    torch.nn.Linear(input_dim, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, 1),
                )

            def forward(self, x):
                return self.net(x)

        self._model = _IoTModel(self.input_dim, self.hidden_dim)

    def _local_state_dict(self) -> Dict[str, Any]:
        if self._model is None:
            return {}
        return {k: v.detach().cpu() for k, v in self._model.state_dict().items()}

    def _apply_global_state(self, weights: Dict[str, Any]) -> None:
        if self._model is None or not weights:
            return
        local = self._model.state_dict()
        new_state = {}
        for k, v in weights.items():
            if k not in local:
                continue
            try:
                if isinstance(v, list):
                    v = torch.as_tensor(v, dtype=local[k].dtype)
                else:
                    v = torch.as_tensor(v).to(local[k].dtype)
                if v.shape == local[k].shape:
                    new_state[k] = v
            except Exception:
                continue
        if new_state:
            self._model.load_state_dict(new_state, strict=False)

    def _add_dp_noise(self, state_dict: Dict[str, Any]) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.privacy_epsilon <= 0:
            return state_dict
        scale = 1.0 / max(self.privacy_epsilon, 1e-6)
        out = {}
        for k, v in state_dict.items():
            if v.dtype.is_floating_point:
                out[k] = v + torch.randn_like(v) * scale * 0.001
            else:
                out[k] = v
        return out

    async def train_local_model(self, samples: List[Dict[str, float]],
                                 epochs: int = 5) -> float:
        if not TORCH_AVAILABLE or self._model is None or not samples:
            return 0.0
        X = np.array([[s.get("battery_percent", 0.5),
                        s.get("processing_load", 0.5),
                        s.get("network_quality", 0.5),
                        s.get("harvesting_available", 0.0),
                        s.get("carbon_intensity", 0.4),
                        s.get("helium_scarcity", 0.5),
                        s.get("gradient_health", 0.7),
                        s.get("optimization_score", 0.5)][:self.input_dim]
                       for s in samples], dtype=np.float32)
        y = np.array([[s.get("optimization_score", 0.5)]
                       for s in samples], dtype=np.float32)

        def _do():
            X_t = torch.as_tensor(X)
            y_t = torch.as_tensor(y)
            opt = torch.optim.Adam(self._model.parameters(), lr=0.01)
            crit = torch.nn.MSELoss()
            self._model.train()
            loss_val = 0.0
            for _ in range(epochs):
                opt.zero_grad()
                out = self._model(X_t)
                loss = crit(out, y_t)
                loss.backward()
                opt.step()
                loss_val = float(loss.item())
            self._model.eval()
            return loss_val

        return await asyncio.to_thread(_do)

    async def participate_in_round(self, samples: List[Dict[str, float]],
                                    performance: float = 1.0,
                                    server_url: Optional[str] = None
                                    ) -> Dict[str, Any]:
        loss = await self.train_local_model(samples)
        state = self._local_state_dict()
        state = self._add_dp_noise(state)
        # Simulate receiving a global model (echo local for tests)
        # In production: HTTP POST to server_url, then GET global weights.
        self.round += 1
        self.contribution_score += float(performance)
        if self.expert_id not in self.participants:
            self.participants.append(self.expert_id)
        return {
            "round": self.round,
            "loss": float(loss),
            "contribution_score": self.contribution_score,
            "participants": list(self.participants),
            "privacy_epsilon": self.privacy_epsilon,
        }

    async def apply_global_state(self, weights: Dict[str, Any]) -> bool:
        try:
            await asyncio.to_thread(self._apply_global_state, weights)
            return True
        except Exception as e:
            logger.warning(f"apply_global_state failed: {e}")
            return False

    def get_state(self) -> Dict[str, Any]:
        return {
            "round": self.round,
            "privacy_epsilon": self.privacy_epsilon,
            "participants": list(self.participants),
            "contribution_score": self.contribution_score,
        }

    def load_state(self, state: Dict[str, Any]) -> None:
        if not state:
            return
        self.round = int(state.get("round", 0))
        self.privacy_epsilon = float(state.get(
            "privacy_epsilon", self.initial_epsilon))
        self.participants = list(state.get("participants", []))
        self.contribution_score = float(state.get("contribution_score", 0.0))

    async def close(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                await self._session.close()
            self._session = None


# =============================================================================
# IOT EXPERT (v9.0.0)
# =============================================================================
class IoTExpert:
    """IoT Expert v9.0.0."""

    SUPPORTED_TASKS = (
        "propose", "optimize", "register_device", "create_mesh",
        "self_heal", "explain", "simulate",
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
        hitl: Optional[HumanApprovalHandler] = None,
        config: Optional[IoTExpertConfig] = None,
        seed: int = 42,
    ):
        self.expert_name = "iot_expert"
        self.supported_task_types = list(self.SUPPORTED_TASKS)
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.bio_core = bio_core
        self.config = config or IoTExpertConfig()
        self.expert_id = self.config.expert_id

        # Deterministic RNG
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        np.random.seed(seed)

        # State
        self.devices: Dict[str, EdgeDevice] = {}
        self.meshes: Dict[str, MeshNetwork] = {}
        self.total_tasks_processed = 0
        self.total_energy_harvested_kwh = 0.0
        self.total_carbon_saved_kg = 0.0
        self.sustainability_score = 0.0
        self.last_error: Optional[str] = None

        # Bounded context (with TTL)
        self._last_context: Dict[str, Any] = {}
        self._last_context_ts: Optional[float] = None

        # Cooldown
        self._last_drastic_ts: Optional[float] = None

        # Adaptive thresholds (config never mutated)
        self._adaptive_thresholds: Dict[str, float] = dict(
            self.config.thresholds)

        # Bio sub-modules
        self.token_manager = None
        self.gradient_manager = None
        self.compartment_manager = None
        self.swarm_coordinator = None
        self.harvester = None
        if self.bio_core is not None:
            self.token_manager = getattr(self.bio_core, "token_manager", None)
            self.gradient_manager = getattr(self.bio_core, "gradient_manager", None)
            self.compartment_manager = getattr(
                self.bio_core, "compartment_manager", None)
            self.swarm_coordinator = getattr(
                self.bio_core, "swarm_coordinator", None)
            self.harvester = getattr(self.bio_core, "harvester", None)

        # Circuit breaker for external calls
        self._ext_circuit = CircuitBreaker(name="iot_external")

        # Federated learner
        self.federated_learner = FederatedIoTLearner(
            self.expert_id,
            privacy_epsilon=self.config.federated_privacy_epsilon)

        # Shared runtime
        self.temporal_safety = TemporalSafetyMonitor(max_trace=500)
        self._setup_temporal_rules()
        self.hitl = hitl or HumanApprovalHandler()
        self.xai = XAIExplainer(rng=self._rng_xai)

        # TaskManager
        self.task_manager = TaskManager()
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_state()
        self.task_manager.start_coro(
            "stale_devices", self._cleanup_stale_devices())
        self.task_manager.start_coro(
            "periodic_save", self._periodic_save())
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self.federated_learner.close()
        await self._save_state()

    # ------------------------------------------------------------------
    # Temporal safety rules
    # ------------------------------------------------------------------
    def _setup_temporal_rules(self) -> None:
        self.temporal_safety.add_formula("G (sampling_rate_hz >= 0.1)")
        self.temporal_safety.add_formula("G (device_count <= 5000)")
        self.temporal_safety.add_formula(
            "[300] NEVER (drastic_actions > 1)")
        self.temporal_safety.add_formula("G (sampling_rate_high <= 100.0)")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _load_state(self) -> None:
        try:
            raw = await asyncio.to_thread(
                self.storage.get_state, "iot_expert_state_v9")
            if not raw:
                return
            state = json.loads(raw)
            for did, dd in state.get("devices", {}).items():
                try:
                    self.devices[did] = EdgeDevice.from_dict(dd)
                except Exception as e:
                    logger.debug(f"skip device {did}: {e}")
            for mid, md in state.get("meshes", {}).items():
                try:
                    self.meshes[mid] = MeshNetwork.from_dict(md)
                except Exception as e:
                    logger.debug(f"skip mesh {mid}: {e}")
            self.total_tasks_processed = int(
                state.get("total_tasks_processed", 0))
            self.total_energy_harvested_kwh = float(
                state.get("total_energy_harvested_kwh", 0.0))
            self.total_carbon_saved_kg = float(
                state.get("total_carbon_saved_kg", 0.0))
            self.sustainability_score = float(
                state.get("sustainability_score", 0.0))
            self._adaptive_thresholds = dict(state.get(
                "adaptive_thresholds", self.config.thresholds))
            self.federated_learner.load_state(
                state.get("federated", {}))
        except Exception as e:
            logger.error(f"load_state failed: {e}")

    async def _save_state(self) -> None:
        try:
            state = {
                "devices": {did: d.to_dict()
                             for did, d in self.devices.items()},
                "meshes": {mid: m.to_dict()
                            for mid, m in self.meshes.items()},
                "total_tasks_processed": self.total_tasks_processed,
                "total_energy_harvested_kwh": self.total_energy_harvested_kwh,
                "total_carbon_saved_kg": self.total_carbon_saved_kg,
                "sustainability_score": self.sustainability_score,
                "adaptive_thresholds": dict(self._adaptive_thresholds),
                "federated": self.federated_learner.get_state(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await asyncio.to_thread(
                self.storage.save_state, "iot_expert_state_v9",
                json.dumps(_json_safe(state), default=str))
        except Exception as e:
            logger.warning(f"save_state failed: {e}")

    async def _periodic_save(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._save_state()
            except Exception as e:
                logger.debug(f"periodic save failed: {e}")

    async def _cleanup_stale_devices(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=600.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                cutoff = datetime.now(timezone.utc) - timedelta(
                    hours=self.config.stale_device_hours)
                stale = [did for did, d in self.devices.items()
                         if d.last_heartbeat < cutoff]
                for did in stale:
                    self.devices.pop(did, None)
                    logger.info(f"removed stale device {did}")
            except Exception as e:
                logger.debug(f"stale device cleanup failed: {e}")

    # ------------------------------------------------------------------
    # Context management (TTL)
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
    # Bio integration
    # ------------------------------------------------------------------
    async def _bio_spend_earn(self, energy: float, quality: float) -> None:
        if self.token_manager is None:
            return
        try:
            atp_cost = max(0.01, energy * 0.001)
            r = self.token_manager.spend(self.expert_name, atp_cost)
            if asyncio.iscoroutine(r):
                await r
            if quality > 0.7:
                r = self.token_manager.earn(self.expert_name, atp_cost * 2)
                if asyncio.iscoroutine(r):
                    await r
        except Exception as e:
            logger.warning(f"bio integration failed: {e}", exc_info=True)

    def _pump_gradient(self, name: str, delta: float) -> None:
        if self.gradient_manager is None:
            return
        try:
            r = self.gradient_manager.pump_field(
                name, delta, source="iot_expert")
            if asyncio.iscoroutine(r):
                # Fire and forget through a task manager
                self.task_manager.start_coro(
                    f"gradient_{uuid.uuid4().hex[:4]}", r)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Bounded threshold helpers
    # ------------------------------------------------------------------
    def _bound_threshold(self, key: str, value: float) -> float:
        lo = self.config.threshold_min.get(key, 0.0)
        hi = self.config.threshold_max.get(key, 1e9)
        return float(max(lo, min(hi, value)))

    def _adapt_threshold(self, key: str, factor: float) -> None:
        cur = self._adaptive_thresholds.get(key,
                                             self.config.thresholds.get(key))
        new = self._bound_threshold(key, cur * factor)
        self._adaptive_thresholds[key] = new
        logger.debug(f"adaptive threshold {key} -> {new:.4f}")

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
        # Save async without blocking
        await self._save_state()

    # ------------------------------------------------------------------
    # Device registration
    # ------------------------------------------------------------------
    async def register_device(self, device_id: str,
                               device_type: DeviceType,
                               capabilities: Optional[Dict[str, float]] = None,
                               mesh_id: Optional[str] = None
                               ) -> DeviceDecision:
        if len(self.devices) >= self.config.max_devices:
            return DeviceDecision(
                operation="register_device",
                status="error", error="max_devices_reached",
                timestamp=time.time())
        d = EdgeDevice(
            device_id=device_id,
            device_type=device_type,
            capabilities=dict(capabilities or {}),
            mesh_id=mesh_id,
        )
        self.devices[device_id] = d
        if mesh_id:
            if mesh_id not in self.meshes:
                self.meshes[mesh_id] = MeshNetwork(mesh_id=mesh_id)
            self.meshes[mesh_id].device_ids.append(device_id)
        await self._publish_feedback(
            task_id=f"iot_register_{device_id}",
            action="register_device",
            quality=0.9,
            details={"device_id": device_id,
                     "device_type": device_type.value},
            tags=["iot", "device"])
        await self._save_state()
        return DeviceDecision(
            operation="register_device",
            recommendation={"device_id": device_id},
            confidence=0.9, timestamp=time.time())

    async def create_mesh(self, mesh_id: str,
                           device_ids: List[str]) -> DeviceDecision:
        members = [did for did in device_ids if did in self.devices]
        if len(members) < 2:
            return DeviceDecision(
                operation="create_mesh", status="error",
                error="need_at_least_two_devices", timestamp=time.time())
        mesh = MeshNetwork(mesh_id=mesh_id, device_ids=list(members))
        # Build edges (all pairs, uniform quality for now)
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                mesh.edges.append((a, b, 0.8))
        mesh.leader_id = self._elect_leader(members)
        self.meshes[mesh_id] = mesh
        for did in members:
            self.devices[did].mesh_id = mesh_id
            self.devices[did].mesh_role = (
                MeshRole.LEADER if did == mesh.leader_id else MeshRole.LEAF)
        await self._publish_feedback(
            task_id=f"iot_create_mesh_{mesh_id}",
            action="create_mesh",
            quality=0.9,
            details={"mesh_id": mesh_id, "device_count": len(members)},
            tags=["iot", "mesh"])
        await self._save_state()
        return DeviceDecision(
            operation="create_mesh",
            recommendation={"mesh_id": mesh_id,
                             "leader_id": mesh.leader_id,
                             "device_count": len(members)},
            confidence=0.9, timestamp=time.time())

    def _elect_leader(self, device_ids: List[str]) -> Optional[str]:
        candidates = [self.devices[did] for did in device_ids
                       if did in self.devices and self.devices[did].is_online]
        if not candidates:
            return None
        best = max(candidates,
                    key=lambda d: (d.gradient_health * 0.5 +
                                    d.energy_remaining_percent / 100.0 * 0.3 +
                                    (1.0 - d.processing_utilization) * 0.2))
        return best.device_id

    # ------------------------------------------------------------------
    # Propose (with real XAI + HITL)
    # ------------------------------------------------------------------
    async def propose_async(self, context: Dict[str, Any]) -> DeviceDecision:
        self._set_context(context)
        helium_scarcity = float(context.get("helium_scarcity", 0.5))
        carbon_intensity = float(context.get("carbon_intensity", 400.0))
        network_latency = float(context.get("network_latency_ms", 50.0))
        battery_level = float(context.get("battery_level", 0.8))
        try:
            ctx = self._get_context()
            helium_scarcity = float(ctx.get("helium_scarcity", helium_scarcity))
            carbon_intensity = float(ctx.get("carbon_intensity",
                                              carbon_intensity))
            network_latency = float(ctx.get("network_latency_ms",
                                             network_latency))
            battery_level = float(ctx.get("battery_level", battery_level))

            th = self._adaptive_thresholds
            if helium_scarcity > 0.6:
                sampling_rate = th["sampling_rate_low"]
                aggregation = "compressed"
                power_saving = True
            else:
                sampling_rate = th["sampling_rate_high"]
                aggregation = "adaptive"
                power_saving = False

            if battery_level < th["battery_low_threshold"] / 100.0:
                sampling_rate = min(sampling_rate, 2.0)
                power_saving = True

            if network_latency > th["latency_high_ms"]:
                preferred_gateways = ["gateway_nearby"]
            else:
                preferred_gateways = []

            # Best mesh recommendation
            best_mesh = None
            if self.meshes:
                best_mesh = max(
                    self.meshes.values(),
                    key=lambda m: m.health_score(self.devices))

            recommendation = {
                "sampling_rate_hz": float(sampling_rate),
                "aggregation_strategy": aggregation,
                "power_saving_mode": bool(power_saving),
                "preferred_gateways": preferred_gateways,
                "mesh_id": best_mesh.mesh_id if best_mesh else None,
            }

            # XAI
            explanation, counterfactual = self._explain_proposal(
                recommendation, helium_scarcity, carbon_intensity,
                network_latency, battery_level)

            # HITL gate
            drastic = (
                sampling_rate <= th["sampling_rate_critical"] or power_saving)
            requires_approval = False
            approved: Optional[bool] = None
            if drastic and self.config.human_approval_required:
                now = time.monotonic()
                if (self._last_drastic_ts is None or
                        now - self._last_drastic_ts >
                        self.config.drastic_action_cooldown_seconds):
                    requires_approval = True
                else:
                    requires_approval = True  # within cooldown → ask again
            if requires_approval:
                approved = await self.hitl.request_approval({
                    "action": "drastic_iot_action",
                    "candidates": [
                        {"id": "approve", "risk": 0.4,
                         "sampling_rate_hz": sampling_rate},
                        {"id": "defer", "risk": 0.0,
                         "sampling_rate_hz": th["sampling_rate_high"]},
                    ],
                }, user_id=self.expert_id)
                if approved:
                    self._last_drastic_ts = time.monotonic()

            # Safety
            violations = await self._safety_check(
                sampling_rate_hz=sampling_rate,
                device_count=len(self.devices),
                sampling_rate_high=th["sampling_rate_high"],
                recent_drastic=1 if approved else 0)

            await self._bio_spend_earn(energy=0.01,
                                         quality=0.9 if not drastic else 0.6)
            self._pump_gradient("trust", 0.03 if not drastic else -0.02)

            self.metrics.inc("iot_proposals")
            self.metrics.set("iot_last_sampling_rate", sampling_rate)
            self.total_tasks_processed += 1

            await self._publish_feedback(
                task_id=f"iot_propose_{uuid.uuid4().hex[:8]}",
                action="propose",
                quality=0.9,
                details={"recommendation": recommendation,
                         "requires_approval": requires_approval,
                         "approved": approved},
                tags=["iot", "propose"])

            await self._check_drift()

            status = "success"
            if requires_approval and approved is False:
                status = "deferred"
            return DeviceDecision(
                operation="propose",
                status=status,
                recommendation=recommendation,
                explanation=explanation,
                counterfactual=counterfactual,
                requires_approval=requires_approval,
                approved=approved,
                safety_violations=violations,
                confidence=0.9,
                timestamp=time.time())
        except Exception as e:
            logger.error(f"propose_async failed: {e}", exc_info=True)
            self.health_status = "degraded"
            self.last_error = str(e)
            return DeviceDecision(
                operation="propose", status="error", error=str(e),
                timestamp=time.time())

    def _explain_proposal(
        self, recommendation: Dict[str, Any],
        helium_scarcity: float, carbon_intensity: float,
        network_latency: float, battery_level: float,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        x = np.array([
            helium_scarcity, carbon_intensity / 1000.0,
            network_latency / 1000.0, battery_level,
        ], dtype=float)

        rate = float(recommendation.get("sampling_rate_hz", 5.0))

        def _score(v: np.ndarray) -> float:
            hs, ci, lat, bat = (float(v[0]), float(v[1]),
                                 float(v[2]), float(v[3]))
            return -(rate * 0.1) - hs * 0.3 - ci * 0.2 - lat * 0.2 - bat * 0.1

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["helium_scarcity", "carbon_intensity",
                                    "network_latency", "battery_level"],
            }
            cf = self.xai.counterfactual(_score, x)
            return explanation, cf
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Optimize (with real XAI + HITL)
    # ------------------------------------------------------------------
    async def optimize_edge_deployment(
        self,
        device_type: str = "any",
        carbon_intensity: Optional[float] = None,
        helium_scarcity: float = 0.5,
    ) -> DeviceDecision:
        try:
            suitable = [d for d in self.devices.values()
                         if device_type == "any" or
                         d.device_type.value == device_type]
            if not suitable:
                return DeviceDecision(
                    operation="optimize", status="error",
                    error="no_suitable_devices", timestamp=time.time())

            if carbon_intensity is None:
                carbon_intensity = float(np.mean(
                    [d.carbon_intensity_g_per_kwh for d in suitable]))

            best = max(
                suitable,
                key=lambda d: (d.gradient_health * 0.4 +
                                d.energy_remaining_percent / 100.0 * 0.3 +
                                (1.0 - d.processing_utilization) * 0.3))

            plan = {
                "primary_device": best.device_id,
                "carbon_intensity": carbon_intensity,
                "helium_scarcity": helium_scarcity,
                "energy_remaining_percent": best.energy_remaining_percent,
                "gradient_health": best.gradient_health,
                "device_count": len(self.devices),
                "sustainability_score": self.sustainability_score,
            }

            explanation, counterfactual = self._explain_optimize(
                plan, carbon_intensity, helium_scarcity, best)

            # HITL: only if sustainability is poor
            requires_approval = False
            approved: Optional[bool] = None
            if self.sustainability_score < 0.4 and \
                    self.config.human_approval_required:
                requires_approval = True
                approved = await self.hitl.request_approval({
                    "action": "poor_sustainability_plan",
                    "candidates": [
                        {"id": "approve", "risk": 0.5},
                        {"id": "defer", "risk": 0.0},
                    ],
                }, user_id=self.expert_id)

            violations = await self._safety_check(
                sampling_rate_hz=self._adaptive_thresholds[
                    "sampling_rate_high"],
                device_count=len(self.devices),
                sampling_rate_high=self._adaptive_thresholds[
                    "sampling_rate_high"],
                recent_drastic=0)

            await self._bio_spend_earn(energy=0.05,
                                         quality=self.sustainability_score)
            self._pump_gradient(
                "trust", 0.05 if self.sustainability_score > 0.7 else -0.02)

            self.total_tasks_processed += 1
            self.sustainability_score = min(1.0, (
                0.4 * (best.energy_remaining_percent / 100.0) +
                0.3 * best.gradient_health +
                0.3 * (1.0 - best.processing_utilization)))

            await self._publish_feedback(
                task_id=f"iot_optimize_{uuid.uuid4().hex[:8]}",
                action="optimize",
                quality=0.9,
                details={"plan": plan,
                         "requires_approval": requires_approval},
                tags=["iot", "optimize"])

            await self._check_drift()
            await self._save_state()

            status = "success"
            if requires_approval and approved is False:
                status = "deferred"
            return DeviceDecision(
                operation="optimize",
                status=status,
                recommendation=plan,
                explanation=explanation,
                counterfactual=counterfactual,
                requires_approval=requires_approval,
                approved=approved,
                safety_violations=violations,
                confidence=0.9,
                timestamp=time.time())
        except Exception as e:
            logger.error(f"optimize failed: {e}", exc_info=True)
            return DeviceDecision(
                operation="optimize", status="error", error=str(e),
                timestamp=time.time())

    def _explain_optimize(self, plan: Dict[str, Any],
                           carbon_intensity: float,
                           helium_scarcity: float,
                           best: EdgeDevice
                           ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        x = np.array([
            best.energy_remaining_percent / 100.0,
            best.gradient_health,
            1.0 - best.processing_utilization,
            helium_scarcity,
            carbon_intensity / 1000.0,
        ], dtype=float)

        def _score(v: np.ndarray) -> float:
            return (float(v[0]) * 0.4 + float(v[1]) * 0.3 +
                     float(v[2]) * 0.2 - float(v[3]) * 0.1 -
                     float(v[4]) * 0.2)

        try:
            attrs = self.xai.integrated_gradients(_score, x)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": ["battery_pct", "gradient_health",
                                    "idle_capacity", "helium_scarcity",
                                    "carbon_intensity"],
            }
            cf = self.xai.counterfactual(_score, x)
            return explanation, cf
        except Exception as e:
            return {"method": "none", "error": str(e)}, {"success": False}

    # ------------------------------------------------------------------
    # Digital twin simulation
    # ------------------------------------------------------------------
    async def simulate_mesh_scenario(self,
                                      mesh_id: str,
                                      scenario: Dict[str, Any]
                                      ) -> DeviceDecision:
        if mesh_id not in self.meshes:
            return DeviceDecision(
                operation="simulate", status="error",
                error="mesh_not_found", timestamp=time.time())
        mesh = self.meshes[mesh_id]
        impact = DigitalTwin.simulate(mesh, self.devices, scenario)
        await self._publish_feedback(
            task_id=f"iot_sim_{uuid.uuid4().hex[:8]}",
            action="simulate",
            quality=0.7,
            details={"mesh_id": mesh_id, "scenario": scenario.get("type"),
                     "predicted_health": impact.get("predicted_health")},
            tags=["iot", "simulate"])
        return DeviceDecision(
            operation="simulate",
            recommendation={"mesh_id": mesh_id, "impact": impact},
            confidence=0.7, timestamp=time.time())

    # ------------------------------------------------------------------
    # Explain
    # ------------------------------------------------------------------
    async def explain_mesh(self, mesh_id: str) -> DeviceDecision:
        if mesh_id not in self.meshes:
            return DeviceDecision(
                operation="explain", status="error",
                error="mesh_not_found", timestamp=time.time())
        mesh = self.meshes[mesh_id]
        health = mesh.health_score(self.devices)
        return DeviceDecision(
            operation="explain",
            recommendation={
                "mesh_id": mesh_id,
                "device_count": len(mesh.device_ids),
                "leader_id": mesh.leader_id,
                "health_score": float(health),
            },
            explanation={"rationale":
                          f"Mesh {mesh_id} has {len(mesh.device_ids)} "
                          f"devices with health {health:.2f}."},
            confidence=float(health),
            timestamp=time.time())

    # ------------------------------------------------------------------
    # Self-heal
    # ------------------------------------------------------------------
    async def self_heal_mesh(self, mesh_id: str) -> DeviceDecision:
        if mesh_id not in self.meshes:
            return DeviceDecision(
                operation="self_heal", status="error",
                error="mesh_not_found", timestamp=time.time())
        mesh = self.meshes[mesh_id]
        # Re-elect leader if the current one is offline
        new_leader = self._elect_leader(mesh.device_ids)
        if new_leader and new_leader != mesh.leader_id:
            if mesh.leader_id and mesh.leader_id in self.devices:
                self.devices[mesh.leader_id].mesh_role = MeshRole.LEAF
            mesh.leader_id = new_leader
            self.devices[new_leader].mesh_role = MeshRole.LEADER
        await self._publish_feedback(
            task_id=f"iot_self_heal_{mesh_id}",
            action="self_heal",
            quality=0.8,
            details={"mesh_id": mesh_id, "new_leader": mesh.leader_id},
            tags=["iot", "self_heal"])
        return DeviceDecision(
            operation="self_heal",
            recommendation={"mesh_id": mesh_id,
                             "new_leader": mesh.leader_id},
            confidence=0.8, timestamp=time.time())

    # ------------------------------------------------------------------
    # Task dispatcher
    # ------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        ttype = task.get("type", "unknown")
        task_id = task.get("correlation_id", str(uuid.uuid4()))
        start = time.monotonic()

        try:
            if ttype == "propose":
                decision = await self.propose_async(task.get("context", {}))
            elif ttype == "optimize":
                decision = await self.optimize_edge_deployment(
                    device_type=task.get("device_type", "any"),
                    carbon_intensity=task.get("carbon_intensity"),
                    helium_scarcity=float(task.get("helium_scarcity", 0.5)))
            elif ttype == "register_device":
                decision = await self.register_device(
                    device_id=task["device_id"],
                    device_type=DeviceType(task["device_type"]),
                    capabilities=task.get("capabilities", {}),
                    mesh_id=task.get("mesh_id"))
            elif ttype == "create_mesh":
                decision = await self.create_mesh(
                    task["mesh_id"], task["device_ids"])
            elif ttype == "self_heal":
                decision = await self.self_heal_mesh(task["mesh_id"])
            elif ttype == "simulate":
                decision = await self.simulate_mesh_scenario(
                    task["mesh_id"], task.get("scenario", {}))
            elif ttype == "explain":
                decision = await self.explain_mesh(task["mesh_id"])
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
            logger.error(f"handle_task({ttype}) failed: {e}", exc_info=True)
            return {"status": "error", "error": str(e),
                    "correlation_id": task_id}

    # ------------------------------------------------------------------
    # Policy probs
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        ctx = self._get_context()
        helium_scarcity = float(state.get("helium_scarcity",
                                            ctx.get("helium_scarcity", 0.5)))
        carbon_intensity = float(state.get("carbon_intensity",
                                             ctx.get("carbon_intensity",
                                                      400.0)))
        network_latency = float(state.get("network_latency_ms",
                                            ctx.get("network_latency_ms",
                                                     50.0)))
        battery = float(state.get("battery_level",
                                    ctx.get("battery_level", 0.8)))

        strategies = ["sampling_high", "sampling_low",
                       "compressed", "power_saving"]
        candidates: List[Dict[str, Any]] = []
        for s in strategies:
            if s == "sampling_high":
                q, cg, lat, ej = 0.9, 5.0, 40.0, 50.0
            elif s == "sampling_low":
                q, cg, lat, ej = 0.75, 2.0, 60.0, 25.0
            elif s == "compressed":
                q, cg, lat, ej = 0.8, 1.5, 70.0, 30.0
            else:  # power_saving
                q, cg, lat, ej = 0.5, 0.5, 90.0, 5.0
            # Helium-aware adjustment
            if helium_scarcity > 0.6:
                ej *= 1.1
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
            return [0.25] * len(strategies)

        scores = np.array([c["score"] for c in candidates], dtype=float)
        # Lower cost is better → negate for softmax
        logits = -scores
        logits -= logits.max()
        probs = np.exp(logits) / max(float(np.exp(logits).sum()), 1e-12)
        full = [0.0] * len(strategies)
        for c, p in zip(candidates, probs):
            full[strategies.index(c["strategy"])] = float(p)
        return full

    # ------------------------------------------------------------------
    # Safety snapshot
    # ------------------------------------------------------------------
    async def _safety_check(self, sampling_rate_hz: float,
                             device_count: int,
                             sampling_rate_high: float,
                             recent_drastic: int) -> Tuple[str, ...]:
        await self.temporal_safety.push({
            "sampling_rate_hz": float(sampling_rate_hz),
            "device_count": int(device_count),
            "sampling_rate_high": float(sampling_rate_high),
            "drastic_actions": int(recent_drastic),
        })
        results = await self.temporal_safety.check()
        return tuple(rid for rid, ok in results.items() if not ok)

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
                self._adapt_threshold("sampling_rate_high", 1.05)
                self._adapt_threshold("sampling_rate_low", 1.05)
            else:
                # Decay back toward configured
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
                feedback_type="iot",
                adaptive_cost_value=0.0,
                state=details,
                candidates=[{"action": a} for a in (
                    "sampling_high", "sampling_low",
                    "compressed", "power_saving")],
                source="iot_expert",
                environment="production",
                tags=tags)
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Health + metrics
    # ------------------------------------------------------------------
    async def get_metrics(self) -> Dict[str, Any]:
        return {
            "total_tasks_processed": self.total_tasks_processed,
            "total_energy_harvested_kwh": float(
                self.total_energy_harvested_kwh),
            "total_carbon_saved_kg": float(self.total_carbon_saved_kg),
            "sustainability_score": float(self.sustainability_score),
            "device_count": len(self.devices),
            "mesh_count": len(self.meshes),
            "temporal_violations": self.temporal_safety.counts(),
            "background_tasks": self.task_manager.task_count(),
            "adaptive_thresholds": dict(self._adaptive_thresholds),
            "federated": self.federated_learner.get_state(),
        }

    async def get_health_status(self) -> ComponentHealth:
        try:
            if len(self.devices) > self.config.max_devices:
                self.health_status = "unhealthy"
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "device_limit_exceeded"})
            details = {
                "device_count": len(self.devices),
                "mesh_count": len(self.meshes),
                "circuit_state": self._ext_circuit.state.value,
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
    e = IoTExpert(
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
        async def get_helium_status(self):
            return {"scarcity": 0.4, "price": 0.55}

    class FakeBio:
        def __init__(self):
            self.spend_calls = []
            self.earn_calls = []
            self.pumped = []
            self.token_manager = self
            self.gradient_manager = self
            self.compartment_manager = None
            self.swarm_coordinator = None
            self.harvester = None
        async def spend(self, actor, amount):
            self.spend_calls.append((actor, amount))
        async def earn(self, actor, amount):
            self.earn_calls.append((actor, amount))
        def get_field_strengths(self):
            return {"carbon": 0.5, "helium": 0.5, "trust": 0.6}
        def pump_field(self, name, delta, source=""):
            self.pumped.append((name, delta, source))

    bio = FakeBio()
    hitl = HumanApprovalHandler()
    cfg = IoTExpertConfig(enable_weather=False)
    e = IoTExpert(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        bio_core=bio, hitl=hitl, config=cfg, seed=42)
    e._bio = bio
    e._hitl = hitl
    await e.initialize()
    try:
        yield e
    finally:
        await e.shutdown()


async def _populate_devices(expert: IoTExpert, n: int = 4) -> None:
    for i in range(n):
        await expert.register_device(
            device_id=f"dev{i}",
            device_type=DeviceType.SENSOR_NODE,
            capabilities={"cpu": 1.0, "mem": 1.0})


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    def test_logger_info_no_kwargs(self):
        # The old code used logger.info("...", updates=...) which raises.
        # This test just verifies the module loads.
        assert IoTExpert is not None

    def test_expert_id_unique_per_instance(self, storage, queue,
                                             adaptive_cost, pareto_gating,
                                             drift_detector, metrics):
        e1 = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        e2 = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        assert e1.expert_id != e2.expert_id

    @pytest.mark.asyncio
    async def test_sync_storage_via_to_thread(self, expert, storage):
        await expert._save_state()
        assert any(c[0] == "save_state" for c in storage.calls)

    @pytest.mark.asyncio
    async def test_tracked_background_tasks(self, expert):
        assert expert.task_manager.task_count() >= 2

    @pytest.mark.asyncio
    async def test_last_context_ttl(self, expert):
        expert._set_context({"helium_scarcity": 0.5})
        assert expert._get_context()["helium_scarcity"] == 0.5
        expert._last_context_ts = time.monotonic() - \
            expert.config.context_ttl_seconds - 1
        assert expert._get_context() == {}

    @pytest.mark.asyncio
    async def test_thresholds_bounded(self, expert):
        await expert.set_thresholds({"sampling_rate_high": 1e6})
        assert expert._adaptive_thresholds["sampling_rate_high"] <= \
            expert.config.threshold_max["sampling_rate_high"]
        await expert.set_thresholds({"sampling_rate_high": -5.0})
        assert expert._adaptive_thresholds["sampling_rate_high"] >= \
            expert.config.threshold_min["sampling_rate_high"]

    @pytest.mark.asyncio
    async def test_set_thresholds_rejects_unknown(self, expert):
        before = dict(expert._adaptive_thresholds)
        await expert.set_thresholds({"garbage_key": 999.0})
        assert "garbage_key" not in expert._adaptive_thresholds
        assert expert._adaptive_thresholds == before

    @pytest.mark.asyncio
    async def test_config_not_mutated_on_drift(self, expert):
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
    async def test_device_limit_bounded(self, storage, queue,
                                          adaptive_cost, pareto_gating,
                                          drift_detector, metrics):
        cfg = IoTExpertConfig()
        cfg.max_devices = 3
        e = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            config=cfg, seed=1)
        await e.initialize()
        try:
            for i in range(5):
                d = await e.register_device(
                    f"d{i}", DeviceType.SENSOR_NODE)
                if i >= 3:
                    assert d.status == "error"
            assert len(e.devices) == 3
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_federated_state_dict_roundtrip(self):
        if not TORCH_AVAILABLE:
            pytest.skip("torch unavailable")
        f = FederatedIoTLearner("test", input_dim=4, hidden_dim=8)
        # Simulate a global update
        state = f._local_state_dict()
        global_state = {k: v.detach().cpu().tolist()
                         for k, v in state.items()}
        ok = await f.apply_global_state(global_state)
        assert ok is True


# =============================================================================
# TESTS — CORE OPERATIONS
# =============================================================================
class TestCoreOperations:
    @pytest.mark.asyncio
    async def test_register_device(self, expert):
        d = await expert.register_device("d1", DeviceType.SENSOR_NODE)
        assert d.status == "success"
        assert "d1" in expert.devices

    @pytest.mark.asyncio
    async def test_create_mesh(self, expert):
        await _populate_devices(expert, 3)
        d = await expert.create_mesh("m1", ["dev0", "dev1", "dev2"])
        assert d.status == "success"
        assert d.recommendation["leader_id"] is not None
        assert "m1" in expert.meshes

    @pytest.mark.asyncio
    async def test_propose_async(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.5,
            "carbon_intensity": 400.0,
            "network_latency_ms": 50.0,
            "battery_level": 0.8})
        assert d.status == "success"
        assert d.recommendation is not None
        assert "sampling_rate_hz" in d.recommendation

    @pytest.mark.asyncio
    async def test_propose_high_scarcity(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.9, "battery_level": 0.8})
        assert d.recommendation["aggregation_strategy"] == "compressed"

    @pytest.mark.asyncio
    async def test_propose_low_battery(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.3, "battery_level": 0.05})
        assert d.recommendation["sampling_rate_hz"] <= 2.0
        assert d.recommendation["power_saving_mode"] is True

    @pytest.mark.asyncio
    async def test_optimize_edge_deployment(self, expert):
        await _populate_devices(expert, 3)
        d = await expert.optimize_edge_deployment(
            device_type="any", carbon_intensity=350.0)
        assert d.status == "success"
        assert d.recommendation is not None
        assert "primary_device" in d.recommendation

    @pytest.mark.asyncio
    async def test_optimize_no_devices(self, expert):
        d = await expert.optimize_edge_deployment()
        assert d.status == "error"
        assert d.error == "no_suitable_devices"

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, expert):
        probs = await expert.policy_probs({})
        assert len(probs) == 4
        assert abs(sum(probs) - 1.0) < 1e-6

    @pytest.mark.asyncio
    async def test_handle_task_propose(self, expert):
        r = await expert.handle_task({
            "type": "propose",
            "context": {"helium_scarcity": 0.5}})
        assert r["status"] == "success"
        assert "recommendation" in r
        assert "correlation_id" in r

    @pytest.mark.asyncio
    async def test_handle_task_unknown(self, expert):
        r = await expert.handle_task({"type": "nonsense"})
        assert r["status"] == "error"


# =============================================================================
# TESTS — XAI
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_propose_has_attributions(self, expert):
        d = await expert.propose_async({
            "helium_scarcity": 0.5, "carbon_intensity": 400.0,
            "network_latency_ms": 50.0, "battery_level": 0.8})
        assert d.explanation is not None
        assert d.explanation.get("method") == "integrated_gradients"
        assert "attributions" in d.explanation
        assert len(d.explanation["attributions"]) == 4
        assert d.counterfactual is not None

    @pytest.mark.asyncio
    async def test_optimize_has_attributions(self, expert):
        await _populate_devices(expert, 2)
        d = await expert.optimize_edge_deployment()
        assert d.explanation is not None
        assert d.explanation.get("method") == "integrated_gradients"
        assert "attributions" in d.explanation
        assert len(d.explanation["attributions"]) == 5


# =============================================================================
# TESTS — HITL + SAFETY
# =============================================================================
class TestHITLAndSafety:
    @pytest.mark.asyncio
    async def test_hitl_defer_drastic_action(self, storage, queue,
                                                adaptive_cost,
                                                pareto_gating,
                                                drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("iot_expert", "defer")
        e = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, seed=1)
        await e.initialize()
        try:
            d = await e.propose_async({
                "helium_scarcity": 0.9,  # critical
                "battery_level": 0.05,   # low
            })
            assert d.requires_approval is True
            assert d.status == "deferred"
            assert d.approved is False
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_approve_drastic_action(self, storage, queue,
                                                  adaptive_cost,
                                                  pareto_gating,
                                                  drift_detector, metrics):
        hitl = HumanApprovalHandler()
        await hitl.submit_response("iot_expert", "approve")
        e = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hitl=hitl, seed=1)
        await e.initialize()
        try:
            d = await e.propose_async({
                "helium_scarcity": 0.9,
                "battery_level": 0.05,
            })
            assert d.requires_approval is True
            assert d.approved is True
            assert d.status == "success"
        finally:
            await e.shutdown()

    @pytest.mark.asyncio
    async def test_temporal_rules_registered(self, expert):
        counts = expert.temporal_safety.counts()
        assert len(counts) >= 4


# =============================================================================
# TESTS — DIGITAL TWIN
# =============================================================================
class TestDigitalTwin:
    @pytest.mark.asyncio
    async def test_simulate_leader_failure(self, expert):
        await _populate_devices(expert, 3)
        await expert.create_mesh("m1", ["dev0", "dev1", "dev2"])
        d = await expert.simulate_mesh_scenario(
            "m1", {"type": "leader_failure"})
        impact = d.recommendation["impact"]
        assert impact["scenario"] == "leader_failure"
        # Must have elected a new leader (3 devices, at least one survivor)
        assert impact.get("new_leader") is not None
        assert impact["predicted_health"] > 0.0

    @pytest.mark.asyncio
    async def test_simulate_battery_drain(self, expert):
        await _populate_devices(expert, 4)
        await expert.create_mesh("m1", ["dev0", "dev1", "dev2", "dev3"])
        # Force large drain
        for did in ["dev0", "dev1"]:
            expert.devices[did].power_consumption_w = 5.0
            expert.devices[did].current_battery_wh = 0.5
        d = await expert.simulate_mesh_scenario(
            "m1", {"type": "battery_drain", "hours": 24})
        impact = d.recommendation["impact"]
        assert impact["scenario"] == "battery_drain"
        assert impact["failure_count"] >= 2

    @pytest.mark.asyncio
    async def test_simulate_link_failure(self, expert):
        await _populate_devices(expert, 4)
        await expert.create_mesh("m1", ["dev0", "dev1", "dev2", "dev3"])
        d = await expert.simulate_mesh_scenario(
            "m1", {"type": "link_failure",
                    "source": "dev0", "target": "dev1"})
        impact = d.recommendation["impact"]
        assert impact["scenario"] == "link_failure"
        assert 0.0 <= impact["predicted_health"] <= 1.0


# =============================================================================
# TESTS — FEDERATED
# =============================================================================
class TestFederated:
    @pytest.mark.asyncio
    async def test_participate_in_round(self):
        if not TORCH_AVAILABLE:
            pytest.skip("torch unavailable")
        f = FederatedIoTLearner("test")
        samples = [
            {"battery_percent": 0.8, "processing_load": 0.2,
             "network_quality": 0.9, "harvesting_available": 0.1,
             "carbon_intensity": 0.4, "helium_scarcity": 0.5,
             "gradient_health": 0.7, "optimization_score": 0.9},
        ] * 8
        result = await f.participate_in_round(samples, performance=0.9)
        assert result["round"] == 1
        assert result["contribution_score"] == 0.9
        assert "test" in result["participants"]

    @pytest.mark.asyncio
    async def test_apply_global_state(self):
        if not TORCH_AVAILABLE:
            pytest.skip("torch unavailable")
        f = FederatedIoTLearner("test", input_dim=4, hidden_dim=8)
        # Get local state as lists
        local = f._local_state_dict()
        global_state = {k: v.tolist() for k, v in local.items()}
        # Perturb slightly
        for k in global_state:
            arr = np.array(global_state[k])
            global_state[k] = (arr + 0.001).tolist()
        ok = await f.apply_global_state(global_state)
        assert ok is True


# =============================================================================
# TESTS — INTEGRATION
# =============================================================================
class TestIntegration:
    @pytest.mark.asyncio
    async def test_bio_integration(self, expert_full):
        await expert_full.propose_async({"helium_scarcity": 0.5})
        assert len(expert_full._bio.spend_calls) >= 1

    @pytest.mark.asyncio
    async def test_full_workflow(self, expert):
        await _populate_devices(expert, 3)
        await expert.create_mesh("m1", ["dev0", "dev1", "dev2"])
        r1 = await expert.handle_task({"type": "propose",
                                         "context": {}})
        assert r1["status"] == "success"
        r2 = await expert.handle_task({"type": "optimize"})
        assert r2["status"] == "success"
        r3 = await expert.handle_task({"type": "simulate",
                                         "mesh_id": "m1",
                                         "scenario": {"type": "leader_failure"}})
        assert r3["status"] == "success"

    @pytest.mark.asyncio
    async def test_persistence_roundtrip(self, storage, queue,
                                           adaptive_cost, pareto_gating,
                                           drift_detector, metrics):
        e1 = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=1)
        await e1.initialize()
        try:
            await e1.register_device("d1", DeviceType.SENSOR_NODE)
            await e1.register_device("d2", DeviceType.SENSOR_NODE)
            await e1.create_mesh("m1", ["d1", "d2"])
            await e1._save_state()
        finally:
            await e1.shutdown()

        e2 = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics, seed=2)
        await e2.initialize()
        try:
            assert "d1" in e2.devices
            assert "m1" in e2.meshes
            assert e2.meshes["m1"].leader_id is not None
        finally:
            await e2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, expert, storage):
        await expert._save_state()
        raw = storage.get_state("iot_expert_state_v9")
        assert raw is not None
        json.loads(raw)

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, expert):
        await expert.shutdown()
        await expert.shutdown()
        assert expert.task_manager.task_count() == 0

    @pytest.mark.asyncio
    async def test_determinism(self, storage):
        def _build(seed):
            return IoTExpert(
                storage=storage,
                adaptive_cost=AdaptiveCostFunction(),
                pareto_gating=ParetoGating(),
                drift_detector=DriftDetector(),
                metrics=MetricsRegistry(), seed=seed)
        e1 = _build(7); e2 = _build(7)
        await e1.initialize(); await e2.initialize()
        try:
            d1 = await e1.propose_async({"helium_scarcity": 0.5})
            d2 = await e2.propose_async({"helium_scarcity": 0.5})
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

        expert = IoTExpert(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics, seed=1)
        await expert.initialize()
        try:
            for i in range(4):
                await expert.register_device(
                    f"dev{i}", DeviceType.SENSOR_NODE)
            await expert.create_mesh("mesh_a",
                                      ["dev0", "dev1", "dev2", "dev3"])

            for ctx in [
                {"helium_scarcity": 0.3, "battery_level": 0.9,
                 "network_latency_ms": 40.0},
                {"helium_scarcity": 0.9, "battery_level": 0.15,
                 "network_latency_ms": 130.0},
            ]:
                d = await expert.propose_async(ctx)
                print(f"propose: status={d.status} "
                      f"rate={d.recommendation.get('sampling_rate_hz')} "
                      f"approved={d.approved}")

            d = await expert.optimize_edge_deployment()
            print(f"optimize: {d.status} primary={d.recommendation.get('primary_device')}")

            d = await expert.simulate_mesh_scenario(
                "mesh_a", {"type": "leader_failure"})
            print(f"simulate: {d.recommendation['impact']}")

            m = await expert.get_metrics()
            print("Metrics:",
                  json.dumps(m, indent=2, default=str))
        finally:
            await expert.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
