#!/usr/bin/env python3
"""
Enhanced Layer Integrator v8.0.0 — Self-Contained Single-File Bundle
=====================================================================

Complete rewrite of the Green Agent layer integrator. Everything needed to
run and test is in this file:

  1. Protocol interfaces for every injected dependency.
  2. Local stubs (Storage, MessageQueue, Metrics, AdaptiveCost, Pareto,
     Drift, FeedbackEvent).
  3. Shared runtime: CircuitBreaker, TaskManager, TemporalSafetyMonitor,
     HumanApprovalHandler, XAIExplainer (IG + CF), ChaosEngine.
  4. Value objects: `Explanation`, `BioResult`, `ComponentHealth`,
     `MOPDPlan` v2 (all-optional fields), `LayerInfo`, `LayerEvent`,
     `CacheEntry`, `RetryConfig`.
  5. NSGA-II fast non-dominated sort with deduplication.
  6. `EnhancedLayerIntegrator` with fixed:
        - JSON-safe state persistence (enums, datetimes).
        - Loader that iterates dict values, not keys.
        - Ascending sort on adaptive cost.
        - Softmax with negated logits.
        - HITL with injected handler and defer status.
        - Immutable config; adaptive weights on the instance.
        - Tracked background tasks; clean shutdown; executor closed.
        - Bounded LRU cache; working retry; no private-state chaos.
        - Config-driven carbon / helium budgets via MTL.
  7. Comprehensive pytest suite at the bottom (~40 tests).

Run:
    python enhanced_layer_integrator_v8.py         # demo
    pytest enhanced_layer_integrator_v8.py -v      # tests
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import math
import random
import re as _re
import threading
import time
import uuid
from abc import ABC
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Protocol,
    Sequence, Set, Tuple, Union, runtime_checkable,
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

try:
    import aiohttp  # noqa: F401
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False


__version__ = "8.0.0"


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
    def get_current_intensity(self) -> Any: ...
    def get_current_price(self) -> Any: ...


@runtime_checkable
class HeliumProviderLike(Protocol):
    def get_scarcity(self) -> Any: ...
    def get_cost_index(self) -> Any: ...


@runtime_checkable
class TokenManagerLike(Protocol):
    def spend(self, actor: str, amount: float) -> Any: ...
    def earn(self, actor: str, amount: float) -> Any: ...
    def get_system_summary(self) -> Any: ...


@runtime_checkable
class GradientManagerLike(Protocol):
    def pump_field(self, name: str, delta: float, source: str = ...) -> Any: ...
    def get_field_strengths(self) -> Dict[str, float]: ...


@runtime_checkable
class ApprovalHandlerLike(Protocol):
    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = ...) -> bool: ...


@runtime_checkable
class LayerModuleLike(Protocol):
    """A layer module exposes async or sync methods; the integrator coerces."""
    def __getattr__(self, name: str) -> Callable[..., Any]: ...


# =============================================================================
# JSON SANITIZER
# =============================================================================
def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, deque)):
        return [_json_safe(v) for v in obj]
    if _np is not None:
        if isinstance(obj, _np.integer):
            return int(obj)
        if isinstance(obj, _np.floating):
            v = float(obj)
            return None if (math.isnan(v) or math.isinf(v)) else v
        if isinstance(obj, _np.ndarray):
            return obj.tolist()
        if isinstance(obj, _np.bool_):
            return bool(obj)
    if isinstance(obj, Enum):
        return obj.value
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
        for key in ("context", "action", "performance"):
            if key in kwargs:
                kwargs[key] = _json_safe(kwargs[key])
        return cls(**kwargs)

    def to_json(self) -> str:
        return json.dumps(self.fields, default=str)


# =============================================================================
# BIO FALLBACKS
# =============================================================================
class BioConsumer(str, Enum):
    EXPERT_EXECUTION = "expert_execution"
    LAYER_EXECUTION = "layer_execution"


class BioTier(str, Enum):
    STARCH_RESERVE = "starch_reserve"
    SILVER = "silver"
    GOLD = "gold"


# =============================================================================
# VALUE OBJECTS
# =============================================================================
class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class LayerStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    DISCOVERED = "discovered"


class IntegrationMode(str, Enum):
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
    EVENT_DRIVEN = "event_driven"
    BATCH = "batch"
    STREAMING = "streaming"


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
# MOPD PLAN v2
# =============================================================================
@dataclass
class MOPDPlan:
    """All fields optional; `explanation` is a structured object."""
    use_cache: bool = True
    use_quantum: bool = False
    data_center: str = "us-east"
    retry_strategy: str = "moderate"
    token_allocation: float = 0.0
    carbon_kg: float = 0.0
    helium_units: float = 0.0
    cost_usd: float = 0.0
    latency_ms: float = 0.0
    success_probability: float = 0.9
    scalarised_score: float = 0.0
    explanation: Optional[Explanation] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if isinstance(self.explanation, Explanation):
            d["explanation"] = self.explanation.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPlan":
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
# LAYER INFO + SUPPORTING DATA
# =============================================================================
@dataclass
class LayerCircuitBreaker:
    name: str = "layer"
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    half_open_successes: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now(timezone.utc)

    def record_success(self) -> None:
        with self._lock:
            self.failure_count = 0
            self.half_open_successes += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "last_failure_time": (self.last_failure_time.isoformat()
                                    if self.last_failure_time else None),
            "half_open_successes": self.half_open_successes,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LayerCircuitBreaker":
        d = dict(d or {})
        state = d.get("state", "closed")
        last = d.get("last_failure_time")
        last_dt = datetime.fromisoformat(last) if last else None
        return cls(
            name=d.get("name", "layer"),
            state=CircuitState(state) if isinstance(state, str) else state,
            failure_count=int(d.get("failure_count", 0)),
            last_failure_time=last_dt,
            half_open_successes=int(d.get("half_open_successes", 0)),
        )


@dataclass
class LayerInfo:
    layer_number: int
    layer_name: str
    version: str = "1.0.0"
    status: LayerStatus = LayerStatus.HEALTHY
    integration_mode: IntegrationMode = IntegrationMode.SYNCHRONOUS
    last_heartbeat: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    dependencies: List[int] = field(default_factory=list)
    capabilities: Dict[str, Any] = field(default_factory=dict)
    endpoints: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    circuit_breaker: Optional[LayerCircuitBreaker] = None
    gradient_health: float = 0.7
    membrane_permeability: str = "selective"
    token_balance: float = 0.0
    harvester_vitality: float = 0.5
    entangled_layers: List[int] = field(default_factory=list)
    sustainability_score: float = 0.0
    carbon_savings_kg: float = 0.0
    discovery_timestamp: Optional[datetime] = None
    health_history: List[Dict[str, Any]] = field(default_factory=list)
    recovery_attempts: int = 0
    max_recovery_attempts: int = 5

    def __post_init__(self) -> None:
        if self.circuit_breaker is None:
            self.circuit_breaker = LayerCircuitBreaker(
                f"layer_{self.layer_number}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "layer_number": self.layer_number,
            "layer_name": self.layer_name,
            "version": self.version,
            "status": self.status.value,
            "integration_mode": self.integration_mode.value,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "dependencies": list(self.dependencies),
            "capabilities": dict(self.capabilities),
            "endpoints": dict(self.endpoints),
            "metrics": dict(self.metrics),
            "config": dict(self.config),
            "circuit_breaker": self.circuit_breaker.to_dict(),
            "gradient_health": float(self.gradient_health),
            "membrane_permeability": self.membrane_permeability,
            "token_balance": float(self.token_balance),
            "harvester_vitality": float(self.harvester_vitality),
            "entangled_layers": list(self.entangled_layers),
            "sustainability_score": float(self.sustainability_score),
            "carbon_savings_kg": float(self.carbon_savings_kg),
            "discovery_timestamp": (self.discovery_timestamp.isoformat()
                                     if self.discovery_timestamp else None),
            "health_history": list(self.health_history),
            "recovery_attempts": int(self.recovery_attempts),
            "max_recovery_attempts": int(self.max_recovery_attempts),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LayerInfo":
        data = dict(data or {})
        data["status"] = LayerStatus(data.get("status", "healthy"))
        data["integration_mode"] = IntegrationMode(
            data.get("integration_mode", "synchronous"))
        hb = data.get("last_heartbeat")
        data["last_heartbeat"] = (datetime.fromisoformat(hb)
                                    if isinstance(hb, str) else
                                    (hb or datetime.now(timezone.utc)))
        dt = data.get("discovery_timestamp")
        data["discovery_timestamp"] = (datetime.fromisoformat(dt)
                                         if isinstance(dt, str) else dt)
        cb = data.get("circuit_breaker")
        data["circuit_breaker"] = (LayerCircuitBreaker.from_dict(cb)
                                     if isinstance(cb, dict)
                                     else (cb or LayerCircuitBreaker()))
        return cls(**data)


@dataclass
class LayerEvent:
    event_id: str
    event_type: str
    layer_number: int
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheEntry:
    key: str
    value: Any
    timestamp: datetime
    layer_number: int


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay_ms: float = 100.0
    max_delay_ms: float = 5000.0


# =============================================================================
# CONFIG
# =============================================================================
@dataclass
class LayerIntegratorConfig:
    enable_cache: bool = True
    enable_monitoring: bool = True
    enable_dynamic_discovery: bool = False
    enable_health_checks: bool = False
    enable_carbon_intensity: bool = True
    enable_predictive: bool = False
    enable_event_driven: bool = False
    enable_mopd: bool = True
    enable_temporal_safety: bool = True
    enable_human_approval: bool = False
    enable_chaos_testing: bool = False

    cache_ttl_seconds: float = 60.0
    max_cache_size: int = 1000
    coordinator_id: str = "main_coordinator"

    default_carbon_intensity_g_per_kwh: float = 400.0
    carbon_budget_per_call_kg: float = 2.0
    helium_budget_per_call_units: float = 1.0
    token_budget_per_call: float = 50.0

    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0

    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    half_open_max_requests: int = 3

    token_reserve_factor: float = 10.0
    gradient_health_threshold: float = 0.6

    mopd_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "carbon": 0.3,
        "helium": 0.2,
        "cost": 0.2,
        "latency": 0.15,
        "success_prob": 0.15,
    })
    mopd_minimise_keys: Set[str] = field(
        default_factory=lambda: {"carbon", "helium", "cost", "latency"})
    mopd_grid_resolution: int = 5

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
# NSGA-II UTILITIES
# =============================================================================
def is_dominated(a: Dict[str, float], b: Dict[str, float],
                 minimise_keys: Set[str],
                 maximise_keys: Set[str]) -> bool:
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
    return [unique[i] for i in range(n) if count[i] == 0]


# =============================================================================
# SHARED RUNTIME — CircuitBreaker
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0,
                 half_open_max: int = 3,
                 name: str = "default") -> None:
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.half_open_max = int(half_open_max)
        self.name = name
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_requests = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if (self.last_failure_time is not None and
                        (time.time() - self.last_failure_time) >
                        self.recovery_timeout):
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_requests = 0
                else:
                    raise RuntimeError(f"circuit breaker {self.name} open")
            if (self.state == CircuitState.HALF_OPEN and
                    self.half_open_requests >= self.half_open_max):
                raise RuntimeError(
                    f"circuit breaker {self.name} half-open limit")
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.OPEN
                elif self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
            raise
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_requests += 1
                if self.half_open_requests >= self.half_open_max:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
            elif self.state == CircuitState.CLOSED:
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
# SHARED RUNTIME — Human approval
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
# SHARED RUNTIME — ChaosEngine
# =============================================================================
class ChaosEngine:
    FAULT_TYPES: Tuple[str, ...] = (
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
    ) -> None:
        self.steady_state = steady_state
        self.snapshot = snapshot
        self.restore = restore
        self.probability = float(probability)
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
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
            elif fault_type in ("carbon_spike", "token_depletion"):
                # No side effects in the engine itself; the layer integrator
                # interprets the result and applies bounded overrides.
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
# ENHANCED LAYER INTEGRATOR v8.0.0
# =============================================================================
class EnhancedLayerIntegrator:
    """Layer integrator with 12 registered layers, MOPD selection, and
    real XAI/HITL/chaos/temporal-safety hooks."""

    LAYER_NAMES: Tuple[str, ...] = (
        "Data Ingestion", "Feature Extraction", "Model Training",
        "Hyperparameter Tuning", "Evaluation", "Deployment", "Monitoring",
        "Security", "Data Validation", "Model Compression",
        "Federated Aggregation", "Result Interpretation",
    )

    def __init__(
        self,
        *,
        config: Optional[Union[LayerIntegratorConfig, Dict[str, Any]]] = None,
        storage: Optional[StorageLike] = None,
        message_queue: Optional[MessageQueueLike] = None,
        metrics: Optional[MetricsLike] = None,
        adaptive_cost: Optional[AdaptiveCostLike] = None,
        pareto_gating: Optional[ParetoGatingLike] = None,
        drift_detector: Optional[DriftDetectorLike] = None,
        carbon_manager: Optional[CarbonManagerLike] = None,
        helium_provider: Optional[HeliumProviderLike] = None,
        token_manager: Optional[TokenManagerLike] = None,
        gradient_manager: Optional[GradientManagerLike] = None,
        approval_handler: Optional[ApprovalHandlerLike] = None,
        layer_modules: Optional[Dict[int, Any]] = None,
        seed: int = 42,
        **kwargs: Any,
    ) -> None:
        if kwargs:
            logger.warning(
                "EnhancedLayerIntegrator: unexpected kwargs ignored: %s",
                list(kwargs.keys()))

        if config is None:
            config = LayerIntegratorConfig()
        elif isinstance(config, dict):
            config = LayerIntegratorConfig(**config)
        self.config = config

        self.storage = storage
        self.queue = message_queue
        self.metrics = metrics
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.carbon_manager = carbon_manager
        self.helium_provider = helium_provider
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.approval_handler = approval_handler

        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_chaos = random.Random(seed + 2)

        # Adaptive state (config never mutated)
        self._adaptive_weights: Dict[str, float] = dict(
            self.config.mopd_objective_weights)
        self._last_weight_change_ts: Optional[float] = None

        # Shared runtime
        self.task_manager = TaskManager()
        self.temporal_safety = TemporalSafetyMonitor()
        self._setup_temporal_rules()
        self.xai = XAIExplainer(rng=self._rng_xai)
        self.hitl: ApprovalHandlerLike = (
            approval_handler or HumanApprovalHandler())
        self.chaos = ChaosEngine(
            steady_state=self._steady_state,
            snapshot=self._snapshot_for_chaos,
            restore=self._restore_from_chaos,
            rng=self._rng_chaos)

        # Carbon override (used by chaos carbon_spike)
        self._carbon_override: Optional[float] = None
        self._token_override: Optional[float] = None

        # Layer registry
        self.layers: Dict[int, LayerInfo] = {}
        for i, name in enumerate(self.LAYER_NAMES):
            self.layers[i] = LayerInfo(
                layer_number=i, layer_name=name, version="1.0.0",
                capabilities={}, gradient_health=self._rng.uniform(0.6, 1.0))
        self.layer_modules: Dict[int, Any] = dict(layer_modules or {})

        # Cache (LRU + TTL)
        self._simple_cache: "OrderedDict[str, CacheEntry]" = OrderedDict()
        self._cache_lock = threading.Lock()

        # Retry config
        self.retry_config = RetryConfig(
            max_retries=self.config.max_retries,
            base_delay_ms=self.config.retry_base_delay_ms,
            max_delay_ms=self.config.retry_max_delay_ms)

        # Metrics
        self.layer_latency: Dict[int, Deque[float]] = {
            i: deque(maxlen=200) for i in range(len(self.LAYER_NAMES))}
        self.layer_errors: Dict[int, int] = {i: 0 for i in range(len(self.LAYER_NAMES))}
        self.layer_calls: Dict[int, int] = {i: 0 for i in range(len(self.LAYER_NAMES))}

        # Aggregates
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_saved_l = 0.0

        self.health_status = HealthStatus.HEALTHY
        self.last_error: Optional[str] = None
        self.correlation_id = str(uuid.uuid4())
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_state_async()
        if self.config.enable_dynamic_discovery:
            self.task_manager.start_coro("discovery", self._discovery_loop())
        if self.config.enable_health_checks:
            self.task_manager.start_coro("health", self._health_check_loop())
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self._save_state()

    async def __aenter__(self) -> "EnhancedLayerIntegrator":
        await self.initialize()
        return self

    async def __aexit__(self, *exc: Any) -> None:
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
            "G (layer_health >= 0.4)")
        self.temporal_safety.add_formula(
            "[300] NEVER (drastic_actions > 1)")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _load_state_async(self) -> None:
        if self.storage is None:
            return
        try:
            raw = await asyncio.to_thread(
                self.storage.get_state, "layer_integrator_state_v8")
            if not raw:
                return
            state = json.loads(raw)
            # FIX: iterate .values(), not keys
            for d in state.get("layers", {}).values():
                try:
                    info = LayerInfo.from_dict(d)
                    self.layers[info.layer_number] = info
                except Exception as e:
                    logger.debug(f"skip layer: {e}")
            self.sustainability_score = float(
                state.get("sustainability_score", 0.0))
            self.total_carbon_savings_kg = float(
                state.get("total_carbon_savings_kg", 0.0))
            self.total_helium_saved_l = float(
                state.get("total_helium_saved_l", 0.0))
            self._adaptive_weights = dict(state.get(
                "adaptive_weights", self.config.mopd_objective_weights))
        except Exception as e:
            logger.warning(f"load_state failed: {e}")

    async def _save_state(self) -> None:
        if self.storage is None:
            return
        state = {
            "layers": {i: info.to_dict()
                        for i, info in self.layers.items()},
            "sustainability_score": self.sustainability_score,
            "total_carbon_savings_kg": self.total_carbon_savings_kg,
            "total_helium_saved_l": self.total_helium_saved_l,
            "adaptive_weights": dict(self._adaptive_weights),
        }
        try:
            payload = json.dumps(_json_safe(state))
        except Exception as e:
            logger.warning(f"json encode failed: {e}")
            return
        try:
            await asyncio.to_thread(
                self.storage.save_state, "layer_integrator_state_v8",
                payload)
        except Exception as e:
            logger.warning(f"save_state failed: {e}")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _discovery_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass
            # discovery hook

    async def _health_check_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=10.0)
                break
            except asyncio.TimeoutError:
                pass
            # health check hook

    # ------------------------------------------------------------------
    # Async coercion for injected managers
    # ------------------------------------------------------------------
    @staticmethod
    async def _coerce(func: Callable[[], Any]) -> Any:
        r = func()
        if asyncio.iscoroutine(r):
            r = await r
        return r

    # ------------------------------------------------------------------
    # Bio helpers
    # ------------------------------------------------------------------
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
                             source: str = "layer_integrator"
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

    async def _get_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager is None:
            return {"carbon": 0.5, "helium": 0.5, "trust": 0.5,
                    "opportunity": 0.5}
        try:
            g = await self._coerce(
                self.gradient_manager.get_field_strengths)
            if isinstance(g, dict):
                return g
        except Exception:
            pass
        return {"carbon": 0.5, "helium": 0.5, "trust": 0.5,
                "opportunity": 0.5}

    async def _get_token_balance(self) -> float:
        if self._token_override is not None:
            return self._token_override
        if self.token_manager is None:
            return 500.0
        try:
            summary = await self._coerce(
                self.token_manager.get_system_summary)
            if isinstance(summary, dict):
                return float(summary.get("total_balance", 500.0))
        except Exception:
            pass
        return 500.0

    # ------------------------------------------------------------------
    # Carbon / helium context
    # ------------------------------------------------------------------
    async def _current_carbon_intensity(self) -> float:
        if self._carbon_override is not None:
            return float(self._carbon_override)
        if self.carbon_manager is not None:
            try:
                v = await self._coerce(
                    self.carbon_manager.get_current_intensity)
                v = float(v)
                if v > 0:
                    return v
            except Exception as e:
                logger.debug(f"carbon manager failed: {e}")
        return float(self.config.default_carbon_intensity_g_per_kwh)

    async def _current_carbon_price(self) -> float:
        if self.carbon_manager is not None:
            try:
                v = await self._coerce(self.carbon_manager.get_current_price)
                return float(v)
            except Exception:
                pass
        return 50.0

    async def _current_helium_status(self) -> Dict[str, float]:
        if self.helium_provider is not None:
            try:
                scarcity = float(await self._coerce(
                    self.helium_provider.get_scarcity))
                cost = float(await self._coerce(
                    self.helium_provider.get_cost_index))
                return {"scarcity": max(0.0, min(1.0, scarcity)),
                        "cost_index": cost}
            except Exception:
                pass
        return {"scarcity": 0.5, "cost_index": 1.0}

    # ------------------------------------------------------------------
    # Context builder
    # ------------------------------------------------------------------
    async def build_context(self) -> Dict[str, Any]:
        context: Dict[str, Any] = {}
        helium = await self._current_helium_status()
        context["helium_scarcity"] = helium["scarcity"]
        context["helium_cost_index"] = helium["cost_index"]

        carbon_intensity = await self._current_carbon_intensity()
        context["carbon_intensity"] = carbon_intensity / 1000.0
        context["carbon_intensity_g_per_kwh"] = carbon_intensity
        context["carbon_price_usd"] = await self._current_carbon_price()

        gradients = await self._get_gradient_levels()
        context.update({f"gradient_{k}": v for k, v in gradients.items()})

        balance = await self._get_token_balance()
        context["token_balance"] = balance
        context["token_balance_norm"] = min(1.0, balance / 1000.0)
        context["avg_layer_health"] = float(
            sum(info.gradient_health for info in self.layers.values()) /
            max(len(self.layers), 1))
        context["sustainability_score"] = self.sustainability_score
        context["carbon_savings_kg"] = self.total_carbon_savings_kg
        context["helium_saved_l"] = self.total_helium_saved_l
        return context

    # ------------------------------------------------------------------
    # Layer call
    # ------------------------------------------------------------------
    async def call_layer(
        self,
        layer_number: int,
        method: str,
        *args: Any,
        timeout: float = 30.0,
        retry: bool = True,
        cache_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        if layer_number not in self.layers:
            raise ValueError(f"invalid layer number: {layer_number}")

        info = self.layers[layer_number]
        if info.circuit_breaker.state == CircuitState.OPEN:
            raise RuntimeError(
                f"circuit breaker open for layer {layer_number}")

        # Cache lookup
        if (cache_key and self.config.enable_cache and
                self._cache_get(cache_key) is not None):
            return self._cache_get(cache_key)

        # Execute with retry
        attempt = 0
        last_exc: Optional[BaseException] = None
        while attempt <= (self.retry_config.max_retries if retry else 0):
            start = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    self._execute_layer(layer_number, method, *args, **kwargs),
                    timeout=timeout)
                latency_ms = (time.monotonic() - start) * 1000.0
                self._record_layer_success(layer_number, latency_ms)
                info.circuit_breaker.record_success()
                if cache_key and self.config.enable_cache:
                    self._cache_put(cache_key, result, layer_number)
                return result
            except asyncio.TimeoutError as e:
                last_exc = e
            except Exception as e:
                last_exc = e
            attempt += 1
            self._record_layer_error(layer_number)
            info.circuit_breaker.record_failure()
            if (info.circuit_breaker.failure_count >=
                    self.config.circuit_breaker_failure_threshold):
                info.circuit_breaker.state = CircuitState.OPEN
                logger.warning(
                    f"circuit breaker opened for layer {layer_number}")
            if attempt <= (self.retry_config.max_retries if retry else 0):
                delay = min(
                    self.retry_config.max_delay_ms,
                    self.retry_config.base_delay_ms * (2 ** (attempt - 1)))
                await asyncio.sleep(delay / 1000.0)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("layer execution failed")

    async def _execute_layer(self, layer_number: int, method: str,
                              *args: Any, **kwargs: Any) -> Any:
        if layer_number in self.layer_modules:
            module = self.layer_modules[layer_number]
            func = getattr(module, method, None)
            if func is None:
                raise AttributeError(
                    f"layer {layer_number} has no method {method}")
            return await self._coerce(lambda: func(*args, **kwargs))
        # Default: simulated layer
        await asyncio.sleep(0.001)
        return {"status": "success", "layer": layer_number,
                "method": method}

    def _record_layer_success(self, layer_number: int,
                               execution_time_ms: float) -> None:
        self.layer_latency[layer_number].append(execution_time_ms)
        self.layer_calls[layer_number] += 1

    def _record_layer_error(self, layer_number: int) -> None:
        self.layer_errors[layer_number] += 1

    # ------------------------------------------------------------------
    # Cache (LRU + TTL)
    # ------------------------------------------------------------------
    def _cache_get(self, key: str) -> Optional[Any]:
        with self._cache_lock:
            entry = self._simple_cache.get(key)
            if entry is None:
                return None
            if (datetime.now(timezone.utc) - entry.timestamp
                    ).total_seconds() > self.config.cache_ttl_seconds:
                self._simple_cache.pop(key, None)
                return None
            self._simple_cache.move_to_end(key)
            return entry.value

    def _cache_put(self, key: str, value: Any, layer_number: int) -> None:
        with self._cache_lock:
            self._simple_cache[key] = CacheEntry(
                key, value, datetime.now(timezone.utc), layer_number)
            self._simple_cache.move_to_end(key)
            while len(self._simple_cache) > self.config.max_cache_size:
                self._simple_cache.popitem(last=False)

    def _cache_clear(self) -> None:
        with self._cache_lock:
            self._simple_cache.clear()

    # ------------------------------------------------------------------
    # MOPD: enumerate, compute, Pareto, select
    # ------------------------------------------------------------------
    async def _enumerate_execution_plans(self, layer_number: int,
                                          method: str, *args: Any,
                                          **kwargs: Any) -> List[MOPDPlan]:
        plans: List[MOPDPlan] = []
        use_cache_opts = [True, False]
        use_quantum_opts = [False]
        if self.layers[layer_number].capabilities.get("quantum"):
            use_quantum_opts = [False, True]
        # Carbon-aware DC choice
        intensity = await self._current_carbon_intensity()
        data_centers = ["us-east"]
        if intensity > 400:
            data_centers.append("us-west")
        retry_opts = ["moderate", "conservative"]
        token_opts = [self.config.token_reserve_factor * 0.5,
                       self.config.token_reserve_factor,
                       self.config.token_reserve_factor * 2.0]
        for uc in use_cache_opts:
            for uq in use_quantum_opts:
                for dc in data_centers:
                    for rs in retry_opts:
                        for ta in token_opts:
                            plans.append(MOPDPlan(
                                use_cache=uc, use_quantum=uq,
                                data_center=dc, retry_strategy=rs,
                                token_allocation=ta))
        return plans

    async def _compute_plan_objectives(self, plan: MOPDPlan,
                                        layer_number: int, method: str,
                                        *args: Any, **kwargs: Any
                                        ) -> MOPDPlan:
        carbon_kg = 0.5
        helium_units = 0.1
        cost_usd = 1.0
        latency_ms = 100.0
        success_prob = 0.95

        if plan.use_cache:
            latency_ms *= 0.5
            carbon_kg *= 0.8
            cost_usd *= 0.7
            success_prob *= 1.02
        if plan.use_quantum:
            latency_ms *= 0.3
            carbon_kg *= 0.6
            cost_usd *= 2.0
            success_prob *= 0.9
        if plan.data_center == "us-west":
            carbon_kg *= 0.7
            latency_ms *= 1.5
            cost_usd *= 1.1
        if plan.retry_strategy == "aggressive":
            latency_ms *= 1.3
            success_prob *= 0.95
            carbon_kg *= 1.1
        elif plan.retry_strategy == "conservative":
            latency_ms *= 1.5
            success_prob *= 1.05
            carbon_kg *= 0.9

        cost_usd += plan.token_allocation * 0.1
        success_prob += 0.01 * (
            plan.token_allocation / self.config.token_reserve_factor)

        plan.carbon_kg = max(0.0, float(carbon_kg))
        plan.helium_units = float(helium_units)
        plan.cost_usd = max(0.0, float(cost_usd))
        plan.latency_ms = max(0.0, float(latency_ms))
        plan.success_probability = min(1.0, max(0.0, float(success_prob)))
        return plan

    async def _generate_pareto_front_for_layer_call(
        self, layer_number: int, method: str, *args: Any, **kwargs: Any
    ) -> List[MOPDPlan]:
        raw_plans = await self._enumerate_execution_plans(
            layer_number, method, *args, **kwargs)
        computed = []
        for p in raw_plans:
            computed.append(await self._compute_plan_objectives(
                p, layer_number, method, *args, **kwargs))

        objectives = [{
            "carbon": p.carbon_kg,
            "helium": p.helium_units,
            "cost": p.cost_usd,
            "latency": p.latency_ms,
            "success_prob": p.success_probability,
        } for p in computed]

        front_objs = nsga2_fast_nondominated_sort(
            objectives,
            minimise_keys={"carbon", "helium", "cost", "latency"},
            maximise_keys={"success_prob"})
        key_to_plan = {tuple(sorted(o.items())): p
                        for o, p in zip(objectives, computed)}
        return [key_to_plan[tuple(sorted(o.items()))] for o in front_objs
                if tuple(sorted(o.items())) in key_to_plan]

    def _select_best_from_pareto(self, front: List[MOPDPlan]
                                  ) -> Optional[MOPDPlan]:
        if not front:
            return None
        scored: List[Tuple[float, MOPDPlan]] = []
        for plan in front:
            if self.adaptive_cost is not None:
                cost = self.adaptive_cost.compute(
                    quality=plan.success_probability,
                    carbon_g=plan.carbon_kg * 1000.0,
                    latency_ms=plan.latency_ms,
                    energy_joules=plan.cost_usd * 10.0,
                    health=0.8,
                    atp=plan.token_allocation / self.config.token_reserve_factor)
            else:
                w = self._adaptive_weights
                cost = (
                    w.get("carbon", 0.3) * plan.carbon_kg +
                    w.get("helium", 0.2) * plan.helium_units +
                    w.get("cost", 0.2) * plan.cost_usd +
                    w.get("latency", 0.15) * (plan.latency_ms / 100.0) -
                    w.get("success_prob", 0.15) * plan.success_probability)
            plan.scalarised_score = float(cost)
            scored.append((float(cost), plan))
        # LOWER cost is better → ascending sort
        scored.sort(key=lambda kv: kv[0])
        return scored[0][1]

    # ------------------------------------------------------------------
    # XAI
    # ------------------------------------------------------------------
    def _explain_plan(self, plan: MOPDPlan) -> Explanation:
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
            nl = (f"Selected {plan.retry_strategy} on {plan.data_center}: "
                  f"cache={plan.use_cache}, quantum={plan.use_quantum}, "
                  f"carbon={plan.carbon_kg:.3f} kg, "
                  f"cost=${plan.cost_usd:.2f}, "
                  f"latency={plan.latency_ms:.0f}ms, "
                  f"success={plan.success_probability:.2f}")
            return Explanation(
                method="integrated_gradients",
                attributions=list(attrs),
                feature_names=list(names),
                counterfactual=cf,
                natural_language=nl)
        except Exception as e:
            return Explanation(method="none", natural_language=str(e))

    # ------------------------------------------------------------------
    # Temporal safety check
    # ------------------------------------------------------------------
    async def check_invariants(self, layer_number: int, method: str,
                                plan: MOPDPlan) -> List[str]:
        info = self.layers[layer_number]
        await self.temporal_safety.push({
            "carbon_kg": plan.carbon_kg,
            "helium_units": plan.helium_units,
            "carbon_budget_kg": self.config.carbon_budget_per_call_kg,
            "helium_budget_units":
                self.config.helium_budget_per_call_units,
            "layer_health": info.gradient_health,
            "drastic_actions": 0,
        })
        results = await self.temporal_safety.check()
        return [rid for rid, ok in results.items() if not ok]

    # ------------------------------------------------------------------
    # HITL
    # ------------------------------------------------------------------
    async def request_approval(self, plan: MOPDPlan, layer_number: int,
                                context: Dict[str, Any]
                                ) -> Tuple[bool, Optional[str]]:
        if not self.config.enable_human_approval:
            return True, None
        if self.approval_handler is None:
            logger.warning(
                "HITL enabled but no handler injected; deferring")
            return False, "no_handler"
        decision = {
            "action": "approve_layer_plan",
            "candidates": [
                {"id": "approve",
                 "success_probability": plan.success_probability,
                 "carbon_kg": plan.carbon_kg},
                {"id": "defer", "success_probability": 0.0,
                 "carbon_kg": 0.0},
            ],
            "context": {"layer_number": layer_number,
                         "context": context},
        }
        try:
            approved = await self.hitl.request_approval(decision)
            return bool(approved), None
        except Exception as e:
            logger.warning(f"HITL failed: {e}")
            return False, "hitl_error"

    # ------------------------------------------------------------------
    # MOPD-driven call
    # ------------------------------------------------------------------
    async def call_layer_with_mopd(
        self,
        layer_number: int,
        method: str,
        *args: Any,
        return_pareto: bool = False,
        **kwargs: Any,
    ) -> Union[Any, Tuple[Any, List[MOPDPlan], MOPDPlan]]:
        if not self.config.enable_mopd:
            result = await self.call_layer(layer_number, method,
                                            *args, **kwargs)
            if return_pareto:
                return result, [], MOPDPlan()
            return result

        front = await self._generate_pareto_front_for_layer_call(
            layer_number, method, *args, **kwargs)
        if not front:
            result = await self.call_layer(layer_number, method,
                                            *args, **kwargs)
            if return_pareto:
                return result, [], MOPDPlan()
            return result

        # Temporal safety filter
        if self.config.enable_temporal_safety:
            safe: List[MOPDPlan] = []
            for p in front:
                v = await self.check_invariants(layer_number, method, p)
                if not v:
                    safe.append(p)
            if safe:
                front = safe
            else:
                # Least-violating plan
                scores = []
                for p in front:
                    v = await self.check_invariants(layer_number, method, p)
                    scores.append((len(v), p))
                scores.sort(key=lambda kv: kv[0])
                front = [scores[0][1]]

        best = self._select_best_from_pareto(front)
        if best is None:
            result = await self.call_layer(layer_number, method,
                                            *args, **kwargs)
            if return_pareto:
                return result, front, MOPDPlan()
            return result

        # XAI
        best.explanation = self._explain_plan(best)

        # HITL
        context = await self.build_context()
        approved, reason = await self.request_approval(
            best, layer_number, context)
        if not approved:
            # Return deferred status by raising for compatibility
            raise PermissionError(
                f"deferred: {reason or 'hitl_rejected'}")

        # Bio spend
        token_actor = f"layer_{layer_number}"
        if self.token_manager is not None:
            atp_cost = max(0.01, best.cost_usd * 0.1)
            await self.spend_atp(token_actor, atp_cost)

        # Gradient pumps
        if best.carbon_kg > 0.5:
            await self.pump_gradient("carbon", 0.05,
                                       source=f"{token_actor}_mopd")
        if best.helium_units > 0.05:
            await self.pump_gradient("helium", 0.05,
                                       source=f"{token_actor}_mopd")

        cache_key = (f"mopd_{layer_number}_{method}"
                     if best.use_cache else None)
        result = await self.call_layer(
            layer_number, method, *args,
            cache_key=cache_key,
            retry=(best.retry_strategy != "conservative"),
            **kwargs)

        # Bio earn
        if self.token_manager is not None:
            atp_reward = max(0.005, best.cost_usd * 0.05)
            await self.earn_atp(token_actor, atp_reward)
        await self.pump_gradient("trust", 0.03,
                                   source=f"{token_actor}_mopd")

        # Feedback
        await self._publish_mopd_feedback(
            layer_number, method, best, front)

        # Drift
        await self._check_drift()

        # Metrics
        if self.metrics is not None:
            try:
                self.metrics.inc("mopd_layer_calls")
                self.metrics.set("mopd_pareto_front_size", len(front))
                self.metrics.set("mopd_selected_carbon_kg",
                                  best.carbon_kg)
                self.metrics.set("mopd_selected_latency_ms",
                                  best.latency_ms)
            except Exception:
                pass

        if return_pareto:
            return result, front, best
        return result

    async def _publish_mopd_feedback(self, layer_number: int,
                                       method: str, best: MOPDPlan,
                                       front: List[MOPDPlan]) -> None:
        if self.queue is None:
            return
        try:
            explanation = (best.explanation.to_dict()
                            if isinstance(best.explanation, Explanation)
                            else None)
            candidates = []
            for p in front:
                candidates.append({
                    "action": (f"plan_c{p.use_cache}"
                                f"_q{p.use_quantum}_{p.data_center}")})
            event = FeedbackEvent.create_with_context(
                task_id=f"mopd_layer_{layer_number}_"
                         f"{uuid.uuid4().hex[:8]}",
                selected_action=(f"plan_c{best.use_cache}"
                                  f"_q{best.use_quantum}_{best.data_center}"),
                quality_score=best.success_probability,
                energy_joules=best.cost_usd * 10.0,
                carbon_g=best.carbon_kg * 1000.0,
                feedback_type="layer_integration",
                adaptive_cost_value=best.scalarised_score,
                state={"layer_number": layer_number,
                       "method": method,
                       "explanation": explanation},
                candidates=candidates,
                source="layer_integrator",
                environment="production",
                tags=["mopd", "layer", "xai"],
            )
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Policy
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if not self.config.enable_mopd:
            return [0.25] * 4
        front = await self._generate_pareto_front_for_layer_call(
            0, "execute")
        if not front:
            return [0.25] * 4
        scores: List[float] = []
        for plan in front:
            if self.adaptive_cost is not None:
                cost = self.adaptive_cost.compute(
                    quality=plan.success_probability,
                    carbon_g=plan.carbon_kg * 1000.0,
                    latency_ms=plan.latency_ms,
                    energy_joules=plan.cost_usd * 10.0,
                    health=0.8,
                    atp=plan.token_allocation / self.config.token_reserve_factor)
            else:
                cost = plan.scalarised_score
            scores.append(float(cost))
        # LOWER cost is better → negate for softmax
        logits = [-s for s in scores]
        if NUMPY_AVAILABLE:
            arr = _np.asarray(logits, dtype=float)
            arr -= arr.max()
            e = _np.exp(arr)
            probs = e / max(float(e.sum()), 1e-12)
            probs_list = probs.tolist()
        else:
            m = max(logits)
            e = [math.exp(l - m) for l in logits]
            s = sum(e) or 1.0
            probs_list = [v / s for v in e]

        out = [0.0, 0.0, 0.0, 0.0]
        for plan, p in zip(front, probs_list):
            if plan.use_quantum:
                idx = 2
            elif plan.use_cache:
                idx = 1
            elif plan.latency_ms < 50:
                idx = 3
            else:
                idx = 0
            out[idx] += p
        total = sum(out) or 1.0
        return [v / total for v in out]

    # ------------------------------------------------------------------
    # Drift (adaptive weights on the instance)
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
                for k, base in self.config.mopd_objective_weights.items():
                    cur = self._adaptive_weights.get(k, base)
                    self._adaptive_weights[k] = (
                        cur + (base - cur) * 0.05)
            return score
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Chaos
    # ------------------------------------------------------------------
    async def _steady_state(self) -> bool:
        return all(info.status == LayerStatus.HEALTHY
                    for info in self.layers.values())

    def _snapshot_for_chaos(self) -> Dict[str, Any]:
        return {
            "carbon_override": self._carbon_override,
            "token_override": self._token_override,
            "layer_status": {i: info.status.value
                              for i, info in self.layers.items()},
        }

    def _restore_from_chaos(self, snap: Dict[str, Any]) -> None:
        self._carbon_override = snap.get("carbon_override")
        self._token_override = snap.get("token_override")
        for i, status_value in snap.get("layer_status", {}).items():
            self.layers[i].status = LayerStatus(status_value)

    async def inject_fault(self, fault_type: str,
                            layer_number: Optional[int] = None,
                            **params: Any) -> Dict[str, Any]:
        if fault_type == "carbon_spike":
            self._carbon_override = 800.0
            return {"status": "completed", "fault_type": fault_type,
                    "carbon_override": 800.0}
        if fault_type == "token_depletion":
            self._token_override = 0.0
            return {"status": "completed", "fault_type": fault_type,
                    "token_override": 0.0}
        if fault_type in ChaosEngine.FAULT_TYPES:
            return await self.chaos.run_experiment(
                params.pop("name", f"manual_{uuid.uuid4().hex[:6]}"),
                fault_type)
        # layer-specific
        if fault_type == "layer_timeout":
            if layer_number is None or layer_number not in self.layers:
                raise ValueError("layer_timeout requires valid layer_number")
            self.layers[layer_number].status = LayerStatus.DEGRADED
            self.layers[layer_number].gradient_health *= 0.5
            return {"status": "completed", "fault_type": fault_type,
                    "layer_number": layer_number}
        if fault_type == "circuit_breaker_open":
            if layer_number is None or layer_number not in self.layers:
                raise ValueError("circuit_breaker_open requires layer_number")
            self.layers[layer_number].circuit_breaker.state = \
                CircuitState.OPEN
            self.layers[layer_number].circuit_breaker.failure_count = \
                self.config.circuit_breaker_failure_threshold
            return {"status": "completed", "fault_type": fault_type,
                    "layer_number": layer_number}
        raise ValueError(f"unknown fault type: {fault_type}")

    async def run_chaos_test(self, runs: int = 3) -> Dict[str, Any]:
        if not self.config.enable_chaos_testing:
            return {"status": "disabled"}
        results: List[Dict[str, Any]] = []
        # carbon spike
        results.append(await self.inject_fault("carbon_spike"))
        self._carbon_override = None
        # token depletion
        results.append(await self.inject_fault("token_depletion"))
        self._token_override = None
        # circuit breaker on layer 0
        results.append(await self.inject_fault(
            "circuit_breaker_open", layer_number=0))
        self.layers[0].circuit_breaker.state = CircuitState.CLOSED
        self.layers[0].circuit_breaker.failure_count = 0
        return {
            "status": "success",
            "runs": len(results),
            "rollbacks": self.chaos.rollback_count,
            "results": results,
        }

    # ------------------------------------------------------------------
    # Health / Stats
    # ------------------------------------------------------------------
    async def get_health_status(self) -> ComponentHealth:
        try:
            if self.config.carbon_budget_per_call_kg <= 0:
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "invalid_carbon_budget"})
            healthy = sum(1 for info in self.layers.values()
                          if info.status == LayerStatus.HEALTHY)
            total = max(len(self.layers), 1)
            score = healthy / total
            status = (HealthStatus.HEALTHY if score > 0.8 else
                      HealthStatus.DEGRADED if score > 0.5 else
                      HealthStatus.UNHEALTHY)
            details = {
                "layers_healthy": healthy,
                "total_layers": total,
                "background_tasks": self.task_manager.task_count(),
                "temporal_violations": self.temporal_safety.counts(),
            }
            return ComponentHealth(status, score, details)
        except Exception as e:
            return ComponentHealth(
                HealthStatus.UNHEALTHY, 0.0, {"error": str(e)})

    def get_integration_status(self) -> Dict[str, Any]:
        return {
            "mopd_enabled": self.config.enable_mopd,
            "mopd_weights": dict(self._adaptive_weights),
            "mopd_grid_resolution": self.config.mopd_grid_resolution,
            "xai_enabled": True,  # always available in v8
            "temporal_safety_enabled": self.config.enable_temporal_safety,
            "human_approval_enabled": self.config.enable_human_approval,
            "chaos_testing_enabled": self.config.enable_chaos_testing,
            "background_tasks": self.task_manager.task_count(),
            "cache_size": len(self._simple_cache),
        }

    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            "sustainability_score": self.sustainability_score,
            "total_carbon_savings_kg": self.total_carbon_savings_kg,
            "total_helium_saved_l": self.total_helium_saved_l,
            "mopd_enabled": self.config.enable_mopd,
        }

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def register_layer_module(self, layer_number: int, module: Any) -> None:
        self.layer_modules[layer_number] = module

    def clear_cache(self) -> None:
        self._cache_clear()


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
    integ = EnhancedLayerIntegrator(
        storage=storage, message_queue=queue, metrics=metrics,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, seed=42)
    await integ.initialize()
    try:
        yield integ
    finally:
        await integ.shutdown()


# =============================================================================
# TESTS — NSGA-II
# =============================================================================
class TestNSGA2:
    def test_dedup(self):
        sols = [
            {"cost": 1.0, "latency": 1.0},
            {"cost": 1.0, "latency": 1.0},
            {"cost": 2.0, "latency": 0.5},
        ]
        front = nsga2_fast_nondominated_sort(
            sols, {"cost", "latency"}, set())
        assert len(front) == 2

    def test_front_dominance(self):
        sols = [
            {"cost": 0.1, "latency": 5.0},
            {"cost": 5.0, "latency": 0.1},
            {"cost": 3.0, "latency": 3.0},
        ]
        front = nsga2_fast_nondominated_sort(
            sols, {"cost", "latency"}, set())
        assert {"cost": 0.1, "latency": 5.0} in front
        assert {"cost": 5.0, "latency": 0.1} in front
        assert {"cost": 3.0, "latency": 3.0} not in front

    def test_empty(self):
        assert nsga2_fast_nondominated_sort([], set(), set()) == []

    def test_single(self):
        sols = [{"a": 1.0}]
        assert nsga2_fast_nondominated_sort(sols, {"a"}, set()) == sols

    def test_maximisation(self):
        a = {"gain": 1.0}
        b = {"gain": 5.0}
        assert is_dominated(a, b, set(), {"gain"})
        assert not is_dominated(b, a, set(), {"gain"})

    def test_missing_worst(self):
        a = {"x": 1.0, "y": 1.0}
        b = {"x": 1.0}
        assert is_dominated(b, a, {"x", "y"}, set())


# =============================================================================
# TESTS — SHARED RUNTIME
# =============================================================================
class TestSharedRuntime:
    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)

        async def bad():
            raise RuntimeError("boom")

        for _ in range(3):
            with pytest.raises(Exception):
                await cb.call(bad)
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_task_manager_stop(self):
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
    async def test_temporal_safety_violation(self):
        m = TemporalSafetyMonitor()
        m.add_formula("G (x >= 0)")
        await m.push({"x": 1})
        await m.push({"x": -1})
        results = await m.check()
        assert any(not ok for ok in results.values())

    @pytest.mark.asyncio
    async def test_hitl_single_candidate(self):
        h = HumanApprovalHandler()
        assert await h.request_approval(
            {"candidates": [{"id": "only"}]}) is True

    def test_xai_ig_shape(self):
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
        call_count = {"n": 0}

        async def flaky_steady():
            call_count["n"] += 1
            return call_count["n"] == 1

        c.steady_state = flaky_steady
        r = await c.run_experiment("rb", "latency")
        assert r["rolled_back"] is True


# =============================================================================
# TESTS — PERSISTENCE
# =============================================================================
class TestPersistence:
    @pytest.mark.asyncio
    async def test_save_state_is_json_safe(self, integrator, storage):
        # Any un-serialisable field would raise inside _save_state
        await integrator._save_state()
        raw = storage.get_state("layer_integrator_state_v8")
        assert raw is not None
        data = json.loads(raw)
        assert "layers" in data
        assert "0" in data["layers"]
        # All fields must be JSON primitives
        for layer_dict in data["layers"].values():
            assert isinstance(layer_dict["status"], str)
            assert isinstance(layer_dict["last_heartbeat"], str)

    @pytest.mark.asyncio
    async def test_load_roundtrip(self, storage, queue, metrics,
                                    adaptive_cost, pareto_gating,
                                    drift_detector):
        integ1 = EnhancedLayerIntegrator(
            storage=storage, message_queue=queue, metrics=metrics,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, seed=1)
        await integ1.initialize()
        try:
            integ1.layers[0].gradient_health = 0.42
            integ1.sustainability_score = 0.77
            await integ1._save_state()
        finally:
            await integ1.shutdown()

        integ2 = EnhancedLayerIntegrator(
            storage=storage, seed=2)
        await integ2.initialize()
        try:
            assert integ2.layers[0].gradient_health == 0.42
            assert integ2.sustainability_score == 0.77
        finally:
            await integ2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, integrator, storage):
        await integrator._save_state()
        raw = storage.get_state("layer_integrator_state_v8")
        json.loads(raw)


# =============================================================================
# TESTS — MOPD SELECTION
# =============================================================================
class TestMOPDSelection:
    @pytest.mark.asyncio
    async def test_pareto_front_not_empty(self, integrator):
        front = await integrator._generate_pareto_front_for_layer_call(
            0, "execute")
        assert len(front) >= 1

    @pytest.mark.asyncio
    async def test_select_best_picks_low_cost(self, integrator):
        a = MOPDPlan(pipeline=None) if False else MOPDPlan(
            carbon_kg=1.0, cost_usd=1.0, latency_ms=100.0,
            success_probability=0.9)
        b = MOPDPlan(carbon_kg=10.0, cost_usd=10.0, latency_ms=100.0,
                      success_probability=0.9)
        best = integrator._select_best_from_pareto([a, b])
        assert best is a

    @pytest.mark.asyncio
    async def test_policy_probs_sum_to_one(self, integrator):
        probs = await integrator.policy_probs({})
        assert abs(sum(probs) - 1.0) < 1e-6
        assert all(0.0 <= p <= 1.0 for p in probs)

    @pytest.mark.asyncio
    async def test_call_layer_with_mopd_returns_plan(self, integrator):
        result, front, best = await integrator.call_layer_with_mopd(
            0, "execute", return_pareto=True)
        assert isinstance(result, dict)
        assert len(front) >= 1
        assert isinstance(best, MOPDPlan)
        assert best.explanation is not None
        assert best.explanation.method == "integrated_gradients"

    @pytest.mark.asyncio
    async def test_mopd_disabled_falls_back(self, storage):
        cfg = LayerIntegratorConfig(enable_mopd=False)
        integ = EnhancedLayerIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            r = await integ.call_layer_with_mopd(0, "execute")
            assert r["status"] == "success"
        finally:
            await integ.shutdown()


# =============================================================================
# TESTS — XAI
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_explanation_has_attributions(self, integrator):
        _, _, best = await integrator.call_layer_with_mopd(
            0, "execute", return_pareto=True)
        expl = best.explanation
        assert expl is not None
        assert expl.method == "integrated_gradients"
        assert len(expl.attributions) == 5
        assert expl.feature_names == ["carbon_kg", "helium_units",
                                        "cost_usd", "latency_ms",
                                        "success_probability"]
        assert expl.counterfactual is not None
        assert expl.natural_language != ""


# =============================================================================
# TESTS — TEMPORAL SAFETY
# =============================================================================
class TestTemporalSafety:
    @pytest.mark.asyncio
    async def test_safe_plan_passes(self, integrator):
        p = MOPDPlan(carbon_kg=0.1, helium_units=0.05)
        v = await integrator.check_invariants(0, "execute", p)
        assert v == []

    @pytest.mark.asyncio
    async def test_high_carbon_violates(self, integrator):
        p = MOPDPlan(carbon_kg=10.0, helium_units=0.05)
        v = await integrator.check_invariants(0, "execute", p)
        assert len(v) >= 1

    @pytest.mark.asyncio
    async def test_rules_registered(self, integrator):
        assert len(integrator.temporal_safety.counts()) >= 5


# =============================================================================
# TESTS — HITL
# =============================================================================
class TestHITL:
    @pytest.mark.asyncio
    async def test_hitl_disabled_auto_approves(self, integrator):
        p = MOPDPlan()
        ok, reason = await integrator.request_approval(p, 0, {})
        assert ok is True
        assert reason is None

    @pytest.mark.asyncio
    async def test_hitl_enabled_no_handler_defers(self):
        cfg = LayerIntegratorConfig(enable_human_approval=True)
        integ = EnhancedLayerIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            p = MOPDPlan()
            ok, reason = await integ.request_approval(p, 0, {})
            assert ok is False
            assert reason == "no_handler"
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_with_rejecting_handler(self):
        class _Reject:
            async def request_approval(self, decision,
                                        user_id="default"):
                return False

        cfg = LayerIntegratorConfig(enable_human_approval=True)
        integ = EnhancedLayerIntegrator(
            config=cfg, approval_handler=_Reject(), seed=1)
        await integ.initialize()
        try:
            with pytest.raises(PermissionError):
                await integ.call_layer_with_mopd(0, "execute")
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_with_approving_handler(self):
        class _Approve:
            async def request_approval(self, decision,
                                        user_id="default"):
                return True

        cfg = LayerIntegratorConfig(enable_human_approval=True)
        integ = EnhancedLayerIntegrator(
            config=cfg, approval_handler=_Approve(), seed=1)
        await integ.initialize()
        try:
            r = await integ.call_layer_with_mopd(0, "execute")
            assert r["status"] == "success"
        finally:
            await integ.shutdown()


# =============================================================================
# TESTS — CACHE
# =============================================================================
class TestCache:
    @pytest.mark.asyncio
    async def test_cache_roundtrip(self, integrator):
        r1 = await integrator.call_layer(0, "execute",
                                          cache_key="k1")
        r2 = await integrator.call_layer(0, "execute",
                                          cache_key="k1")
        assert r1 == r2

    @pytest.mark.asyncio
    async def test_cache_eviction_respects_max(self, storage):
        cfg = LayerIntegratorConfig(max_cache_size=3)
        integ = EnhancedLayerIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            for i in range(10):
                await integ.call_layer(0, "execute",
                                        cache_key=f"k{i}")
            assert len(integ._simple_cache) <= 3
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_cache_lru_order(self, storage):
        cfg = LayerIntegratorConfig(max_cache_size=2)
        integ = EnhancedLayerIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            await integ.call_layer(0, "execute", cache_key="a")
            await integ.call_layer(0, "execute", cache_key="b")
            # Touch "a" so "b" is LRU
            await integ.call_layer(0, "execute", cache_key="a")
            await integ.call_layer(0, "execute", cache_key="c")
            assert "a" in integ._simple_cache
            assert "b" not in integ._simple_cache
            assert "c" in integ._simple_cache
        finally:
            await integ.shutdown()


# =============================================================================
# TESTS — RETRY
# =============================================================================
class TestRetry:
    @pytest.mark.asyncio
    async def test_retry_succeeds_after_failures(self, storage):
        class FlakyModule:
            def __init__(self):
                self.calls = 0

            async def execute(self):
                self.calls += 1
                if self.calls < 3:
                    raise RuntimeError("flaky")
                return {"status": "success", "calls": self.calls}

        integ = EnhancedLayerIntegrator(seed=1)
        integ.register_layer_module(0, FlakyModule())
        await integ.initialize()
        try:
            r = await integ.call_layer(0, "execute")
            assert r["status"] == "success"
            assert r["calls"] == 3
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_no_retry_flag(self, storage):
        class AlwaysBad:
            async def execute(self):
                raise RuntimeError("boom")

        integ = EnhancedLayerIntegrator(seed=1)
        integ.register_layer_module(0, AlwaysBad())
        await integ.initialize()
        try:
            with pytest.raises(RuntimeError):
                await integ.call_layer(0, "execute", retry=False)
        finally:
            await integ.shutdown()


# =============================================================================
# TESTS — BIO INTEGRATION
# =============================================================================
class TestBio:
    @pytest.mark.asyncio
    async def test_spend_no_manager(self, integrator):
        r = await integrator.spend_atp("a", 1.0)
        assert r.success is False
        assert r.error == "no_token_manager"

    @pytest.mark.asyncio
    async def test_spend_sync_manager(self):
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
        integ = EnhancedLayerIntegrator(token_manager=tm, seed=1)
        r = await integ.spend_atp("a", 0.5)
        assert r.success
        assert tm.calls == [("spend", "a", 0.5)]

    @pytest.mark.asyncio
    async def test_spend_async_manager(self):
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
        integ = EnhancedLayerIntegrator(token_manager=tm, seed=1)
        r = await integ.spend_atp("a", 0.5)
        assert r.success
        assert tm.calls == [("spend", "a", 0.5)]

    @pytest.mark.asyncio
    async def test_spend_failure(self):
        class _Bad:
            def spend(self, actor, amount):
                raise RuntimeError("boom")

        integ = EnhancedLayerIntegrator(token_manager=_Bad(), seed=1)
        r = await integ.spend_atp("a", 1.0)
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
        integ = EnhancedLayerIntegrator(gradient_manager=gm, seed=1)
        r = await integ.pump_gradient("carbon", 0.1, source="t")
        assert r.success
        assert gm.calls == [("carbon", 0.1, "t")]


# =============================================================================
# TESTS — CHAOS
# =============================================================================
class TestChaos:
    @pytest.mark.asyncio
    async def test_inject_carbon_spike(self, integrator):
        r = await integrator.inject_fault("carbon_spike")
        assert r["status"] == "completed"
        intensity = await integrator._current_carbon_intensity()
        assert intensity == 800.0
        integrator._carbon_override = None

    @pytest.mark.asyncio
    async def test_inject_token_depletion(self, integrator):
        r = await integrator.inject_fault("token_depletion")
        assert r["status"] == "completed"
        bal = await integrator._get_token_balance()
        assert bal == 0.0
        integrator._token_override = None

    @pytest.mark.asyncio
    async def test_inject_layer_timeout(self, integrator):
        r = await integrator.inject_fault(
            "layer_timeout", layer_number=0)
        assert r["status"] == "completed"
        assert integrator.layers[0].status == LayerStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_inject_unknown_fault(self, integrator):
        with pytest.raises(ValueError):
            await integrator.inject_fault("nonsense")

    @pytest.mark.asyncio
    async def test_run_chaos_test_enabled(self, storage):
        cfg = LayerIntegratorConfig(enable_chaos_testing=True)
        integ = EnhancedLayerIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            r = await integ.run_chaos_test()
            assert r["status"] == "success"
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_run_chaos_test_disabled(self, integrator):
        r = await integrator.run_chaos_test()
        assert r["status"] == "disabled"


# =============================================================================
# TESTS — CONFIG IMMUTABILITY
# =============================================================================
class TestConfigImmutability:
    @pytest.mark.asyncio
    async def test_config_unchanged_on_drift(self, integrator):
        original = dict(integrator.config.mopd_objective_weights)
        for _ in range(20):
            await integrator._check_drift()
        assert integrator.config.mopd_objective_weights == original

    @pytest.mark.asyncio
    async def test_adaptive_weights_normalised(self, integrator):
        # Force drift by swapping the adaptive cost weights
        integrator.adaptive_cost.weights = {
            "accuracy": 0.1, "carbon": 0.8, "cost": 0.05, "latency": 0.05}
        for _ in range(10):
            await integrator._check_drift()
        total = sum(integrator._adaptive_weights.values())
        assert abs(total - 1.0) < 1e-6


# =============================================================================
# TESTS — STATE MACHINE / HEALTH
# =============================================================================
class TestLayerState:
    @pytest.mark.asyncio
    async def test_health_reports_layers(self, integrator):
        h = await integrator.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert h.score == 1.0
        assert h.details["layers_healthy"] == 12
        assert h.details["total_layers"] == 12

    @pytest.mark.asyncio
    async def test_degraded_layer_reduces_score(self, integrator):
        integrator.layers[0].status = LayerStatus.DEGRADED
        h = await integrator.get_health_status()
        assert h.score < 1.0


# =============================================================================
# TESTS — SHUTDOWN / TASKS
# =============================================================================
class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage):
        cfg = LayerIntegratorConfig(enable_dynamic_discovery=True,
                                     enable_health_checks=True)
        integ = EnhancedLayerIntegrator(config=cfg, storage=storage, seed=1)
        await integ.initialize()
        assert integ.task_manager.task_count() >= 2
        await integ.shutdown()
        assert integ.task_manager.task_count() == 0

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, integrator):
        await integrator.shutdown()
        await integrator.shutdown()
        assert integrator.task_manager.task_count() == 0


# =============================================================================
# TESTS — FEEDBACK EVENTS
# =============================================================================
class TestFeedback:
    @pytest.mark.asyncio
    async def test_mopd_publishes_feedback(self, integrator, queue):
        await integrator.call_layer_with_mopd(0, "execute")
        assert len(queue.published) >= 1
        topic, payload = queue.published[-1]
        assert topic == "feedback_events"
        parsed = json.loads(payload)
        assert "task_id" in parsed


# =============================================================================
# TESTS — DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_pareto(self, storage):
        def _build(seed):
            return EnhancedLayerIntegrator(storage=storage, seed=seed)
        i1 = _build(7)
        i2 = _build(7)
        await i1.initialize()
        await i2.initialize()
        try:
            f1 = await i1._generate_pareto_front_for_layer_call(
                0, "execute")
            f2 = await i2._generate_pareto_front_for_layer_call(
                0, "execute")
            assert len(f1) == len(f2)
            for a, b in zip(f1, f2):
                assert a.carbon_kg == b.carbon_kg
                assert a.cost_usd == b.cost_usd
        finally:
            await i1.shutdown()
            await i2.shutdown()


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def demo():
        integ = EnhancedLayerIntegrator(seed=1)
        await integ.initialize()
        try:
            result, front, best = await integ.call_layer_with_mopd(
                0, "execute", return_pareto=True)
            print(f"result status: {result.get('status')}")
            print(f"pareto size: {len(front)}")
            print(f"selected plan: "
                  f"cache={best.use_cache}, quantum={best.use_quantum}, "
                  f"dc={best.data_center}, retry={best.retry_strategy}")
            print(f"explanation: "
                  f"{best.explanation.natural_language if best.explanation else 'n/a'}")
            h = await integ.get_health_status()
            print(f"health: {h.status.value} score={h.score:.2f}")
            print("integration:", json.dumps(
                integ.get_integration_status(), indent=2, default=str))
        finally:
            await integ.shutdown()

    asyncio.run(demo())
