#!/usr/bin/env python3
"""
Quantum LIMIT Graph Integrator v8.0.0 — Self-Contained Single-File Bundle
==========================================================================

Complete rewrite of the Green Agent quantum limit-graph integrator.
Everything needed to run and test is in this file:

  1. Guarded optional imports (aiohttp, numpy, qiskit).
  2. Protocol interfaces for every injected dependency.
  3. Local stubs (Storage, MessageQueue, Metrics, AdaptiveCost, Pareto,
     Drift, FeedbackEvent).
  4. Bio fallbacks (`BioConsumer`, `BioTier`) so the module imports without
     the bio-inspired core.
  5. Shared runtime: CircuitBreaker, TaskManager, TemporalSafetyMonitor,
     HumanApprovalHandler, XAIExplainer (IG + CF), ChaosEngine.
  6. Value objects: Explanation, BioResult, ComponentHealth, MOPDPlan v2
     (all-optional fields).
  7. NSGA-II fast non-dominated sort with deduplication.
  8. `QuantumLimitGraphIntegrator` with:
        - Correct AdaptiveBoundary initialisation (soft ≤ hard).
        - Ascending sort on adaptive cost.
        - Softmax with negated logits.
        - HITL with injected handler and defer status.
        - Immutable config; adaptive weights on the instance.
        - Tracked background tasks; `_shutdown_event`; clean shutdown.
        - JSON-safe persistence via `to_dict`/`from_dict` per dataclass.
        - Multiple backends registered; dynamic availability.
        - Sync/async coercion for token manager, gradient manager, etc.
        - Chaos through ChaosEngine; no private state access.
        - Finite token budget sentinel.
        - Real MTL temporal safety rules.
 9. Comprehensive pytest suite (~40 tests).

Run:
    python quantum_limit_integrator_v8.py         # demo
    pytest quantum_limit_integrator_v8.py -v      # tests
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
from collections import OrderedDict, defaultdict, deque
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

try:
    from qiskit import QuantumCircuit  # noqa: F401
    QISKIT_AVAILABLE = True
except ImportError:  # pragma: no cover
    QISKIT_AVAILABLE = False


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
    def reserve_tokens(self, *, account_id: str, amount: float,
                       consumer: Any = ...) -> Any: ...
    def get_system_summary(self) -> Any: ...


@runtime_checkable
class GradientManagerLike(Protocol):
    def pump_field(self, name: str, delta: float, source: str = ...) -> Any: ...
    def get_field_strengths(self) -> Dict[str, float]: ...
    fields: Dict[str, Any]


@runtime_checkable
class ApprovalHandlerLike(Protocol):
    async def request_approval(self, decision: Dict[str, Any],
                               user_id: str = ...) -> bool: ...


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
    QUANTUM_COMPUTING = "quantum_computing"
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


class QuantumBackend(str, Enum):
    SIMULATOR = "simulator"
    IBM_SHERBROOKE = "ibm_sherbrooke"
    IBM_KYIV = "ibm_kyiv"
    IBM_BRISBANE = "ibm_brisbane"
    RIGETTI_ASPEN = "rigetti_aspen"
    IONQ_ARIA = "ionq_aria"
    DWAVE_ADVANTAGE = "dwave_advantage"
    LOCAL_SIMULATOR = "local_simulator"


class QuantumAlgorithm(str, Enum):
    QAOA = "qaoa"
    VQE = "vqe"
    GROVER = "grover"
    QNN = "qnn"
    QSVM = "qsvm"
    HYBRID = "hybrid"


class QuantumErrorMitigation(str, Enum):
    NONE = "none"
    ZNE = "zero_noise_extrapolation"
    PEC = "probabilistic_error_cancellation"
    DD = "dynamical_decoupling"
    M3 = "measurement_error_mitigation"


class BoundarySource(str, Enum):
    STATIC = "static"
    GRADIENT_FIELD = "gradient_field"
    TOKEN_ECONOMY = "token_economy"
    BIOMASS_RESERVE = "biomass_reserve"
    HARVESTER_SIGNAL = "harvester_signal"
    PREDICTIVE = "predictive"


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
# QUANTUM DATA CLASSES
# =============================================================================
@dataclass
class QuantumResource:
    backend: QuantumBackend
    qubits_available: int = 32
    qubits_in_use: int = 0
    circuit_depth_max: int = 1000
    t1_time_us: float = 100.0
    t2_time_us: float = 100.0
    gate_error_rate: float = 0.001
    readout_error_rate: float = 0.01
    queue_depth: int = 0
    estimated_wait_seconds: float = 0.0
    carbon_per_second: float = 0.001
    helium_per_second: float = 0.0001
    ecoatp_cost_per_second: float = 50.0
    is_available: bool = True
    last_calibration: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    carbon_price_usd_per_ton: float = 50.0
    helium_price_usd_per_l: float = 0.5
    token_exchange_rate: float = 1.0

    @property
    def qubits_free(self) -> int:
        return self.qubits_available - self.qubits_in_use

    @property
    def utilization(self) -> float:
        return self.qubits_in_use / max(self.qubits_available, 1)

    @property
    def total_token_cost_per_second(self) -> float:
        return self.ecoatp_cost_per_second * self.token_exchange_rate

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backend": self.backend.value,
            "qubits_available": self.qubits_available,
            "qubits_in_use": self.qubits_in_use,
            "circuit_depth_max": self.circuit_depth_max,
            "t1_time_us": self.t1_time_us,
            "t2_time_us": self.t2_time_us,
            "gate_error_rate": self.gate_error_rate,
            "readout_error_rate": self.readout_error_rate,
            "queue_depth": self.queue_depth,
            "estimated_wait_seconds": self.estimated_wait_seconds,
            "carbon_per_second": self.carbon_per_second,
            "helium_per_second": self.helium_per_second,
            "ecoatp_cost_per_second": self.ecoatp_cost_per_second,
            "is_available": self.is_available,
            "last_calibration": self.last_calibration.isoformat(),
            "carbon_price_usd_per_ton": self.carbon_price_usd_per_ton,
            "helium_price_usd_per_l": self.helium_price_usd_per_l,
            "token_exchange_rate": self.token_exchange_rate,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "QuantumResource":
        d = dict(d or {})
        d["backend"] = QuantumBackend(d["backend"])
        lc = d.get("last_calibration")
        d["last_calibration"] = (datetime.fromisoformat(lc)
                                  if isinstance(lc, str)
                                  else (lc or datetime.now(timezone.utc)))
        return cls(**d)


@dataclass
class AdaptiveBoundary:
    boundary_id: str
    resource_type: str
    current_value: float = 0.5
    hard_limit: float = 1.0
    soft_limit: float = 0.8
    trend: float = 0.0
    seasonality: float = 0.0
    confidence_interval: Tuple[float, float] = (0.0, 0.0)
    last_updated: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    ml_prediction: Optional[float] = None
    prediction_horizon_hours: int = 24
    boundary_source: BoundarySource = BoundarySource.STATIC
    gradient_strength: float = 0.0
    token_availability: float = 0.5
    sustainability_score: float = 0.0
    price_trend: float = 0.0
    scarcity_index: float = 0.0
    forecast_confidence: float = 0.0

    def __post_init__(self) -> None:
        # Enforce monotone ordering: current <= soft <= hard
        if self.soft_limit > self.hard_limit:
            self.soft_limit = self.hard_limit
        if self.current_value > self.hard_limit:
            self.current_value = self.hard_limit

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["last_updated"] = self.last_updated.isoformat()
        d["boundary_source"] = self.boundary_source.value
        d["confidence_interval"] = list(self.confidence_interval)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AdaptiveBoundary":
        d = dict(d or {})
        d["boundary_source"] = BoundarySource(
            d.get("boundary_source", "static"))
        lu = d.get("last_updated")
        d["last_updated"] = (datetime.fromisoformat(lu)
                              if isinstance(lu, str)
                              else (lu or datetime.now(timezone.utc)))
        if "confidence_interval" in d:
            d["confidence_interval"] = tuple(d["confidence_interval"])
        return cls(**d)


# =============================================================================
# MOPD PLAN v2
# =============================================================================
@dataclass
class MOPDPlan:
    """All-optional fields; `explanation` is a structured object."""
    backend: QuantumBackend = QuantumBackend.SIMULATOR
    error_mitigation: QuantumErrorMitigation = QuantumErrorMitigation.NONE
    shots: int = 1000
    priority: int = 0
    use_quantum_bridge: bool = False
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
        d["backend"] = self.backend.value
        d["error_mitigation"] = self.error_mitigation.value
        if isinstance(self.explanation, Explanation):
            d["explanation"] = self.explanation.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPlan":
        data = dict(data or {})
        if "backend" in data and isinstance(data["backend"], str):
            data["backend"] = QuantumBackend(data["backend"])
        if ("error_mitigation" in data and
                isinstance(data["error_mitigation"], str)):
            data["error_mitigation"] = QuantumErrorMitigation(
                data["error_mitigation"])
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
# CONFIG
# =============================================================================
@dataclass
class QuantumLimitIntegratorConfig:
    enable_mopd: bool = True
    enable_carbon_intensity: bool = True
    enable_temporal_safety: bool = True
    enable_human_approval: bool = False
    enable_chaos_testing: bool = False
    enable_persistence: bool = True
    enable_telemetry: bool = False
    enable_predictive: bool = False
    enable_cross_federation: bool = False
    enable_dynamic_pricing: bool = False

    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    carbon_budget_per_call_kg: float = 2.0
    helium_budget_per_call_units: float = 1.0
    token_budget_per_call: float = 100.0
    token_normalization_factor: float = 1000.0
    quantum_cost_multiplier: float = 5.0

    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0

    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    mopd_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "carbon": 0.3,
        "helium": 0.2,
        "cost": 0.2,
        "latency": 0.15,
        "success_prob": 0.15,
    })
    mopd_minimise_keys: Set[str] = field(
        default_factory=lambda: {"carbon", "helium", "cost", "latency"})

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
    a_ok = True
    b_better = False
    for key in maximise_keys:
        av = float(a.get(key, float("-inf")))
        bv = float(b.get(key, float("-inf")))
        if bv < av:
            a_ok = False
            break
        if bv > av:
            b_better = True
    if a_ok:
        for key in minimise_keys:
            av = float(a.get(key, float("inf")))
            bv = float(b.get(key, float("inf")))
            if bv > av:
                a_ok = False
                break
            if bv < av:
                b_better = True
    return a_ok and b_better


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
                    raise RuntimeError(
                        f"circuit breaker {self.name} open")
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
                logger.warning(
                    "TaskManager: no running loop; %s skipped", name)
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
# SHARED RUNTIME — XAI (IG + counterfactuals)
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
        "backend_unavailable", "network_drop",
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
            elif fault_type in ("carbon_spike", "token_depletion",
                                 "backend_unavailable"):
                # No side effects in the engine; the integrator interprets
                # the result and applies bounded overrides via public API.
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
# MAIN INTEGRATOR
# =============================================================================
class QuantumLimitGraphIntegrator:
    """Quantum LIMIT Graph Integrator v8.0.0."""

    def __init__(
        self,
        *,
        config: Optional[Union[QuantumLimitIntegratorConfig,
                                Dict[str, Any]]] = None,
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
        expert_id: Optional[str] = None,
        seed: int = 42,
        **kwargs: Any,
    ) -> None:
        if kwargs:
            logger.warning(
                "QuantumLimitGraphIntegrator: unexpected kwargs ignored: %s",
                list(kwargs.keys()))

        if config is None:
            config = QuantumLimitIntegratorConfig()
        elif isinstance(config, dict):
            config = QuantumLimitIntegratorConfig(**config)
        self.config = config
        self.expert_id = expert_id or \
            f"quantum_limit_integrator_{uuid.uuid4().hex[:8]}"

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

        # Adaptive weights (config is never mutated)
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

        # Overrides used by chaos, applied through public API
        self._carbon_override: Optional[float] = None
        self._token_override: Optional[float] = None

        # Carbon circuit breaker (single instance)
        self._carbon_circuit = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout,
            name="carbon_manager")

        # Backends
        self.backends: Dict[QuantumBackend, QuantumResource] = {}
        self._register_backends()

        # Boundaries
        self.boundaries: Dict[str, AdaptiveBoundary] = {}
        self._initialize_boundaries()

        # Graph nodes (real graph, not a stub)
        self.graph_nodes: Dict[str, Any] = {}
        self._initialize_quantum_graph()

        # Aggregates
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_saved_l = 0.0

        self.health_status = HealthStatus.HEALTHY
        self.last_error: Optional[str] = None

        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._load_state_async()
        if self.config.enable_persistence:
            self.task_manager.start_coro(
                "persistence", self._persistence_save_loop())
        self._initialized = True

    async def shutdown(self) -> None:
        await self.task_manager.stop_all()
        await self._save_state()

    async def __aenter__(self) -> "QuantumLimitGraphIntegrator":
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
        self.temporal_safety.add_formula("G (token_allocation >= 0.0)")
        self.temporal_safety.add_formula(
            "G (token_allocation <= token_budget)")
        self.temporal_safety.add_formula(
            "[300] NEVER (drastic_actions > 1)")

    # ------------------------------------------------------------------
    # Backends
    # ------------------------------------------------------------------
    def _register_backends(self) -> None:
        self.backends[QuantumBackend.SIMULATOR] = QuantumResource(
            backend=QuantumBackend.SIMULATOR,
            qubits_available=32, qubits_in_use=0, circuit_depth_max=1000,
            t1_time_us=200.0, t2_time_us=200.0, gate_error_rate=0.0005,
            readout_error_rate=0.005, queue_depth=0,
            estimated_wait_seconds=0.0, carbon_per_second=0.001,
            helium_per_second=0.0001, ecoatp_cost_per_second=10.0)
        self.backends[QuantumBackend.LOCAL_SIMULATOR] = QuantumResource(
            backend=QuantumBackend.LOCAL_SIMULATOR,
            qubits_available=24, qubits_in_use=0, circuit_depth_max=500,
            t1_time_us=200.0, t2_time_us=200.0, gate_error_rate=0.001,
            readout_error_rate=0.01, queue_depth=0,
            estimated_wait_seconds=0.5, carbon_per_second=0.002,
            helium_per_second=0.0002, ecoatp_cost_per_second=15.0)
        self.backends[QuantumBackend.IBM_SHERBROOKE] = QuantumResource(
            backend=QuantumBackend.IBM_SHERBROOKE,
            qubits_available=127, qubits_in_use=0, circuit_depth_max=500,
            t1_time_us=180.0, t2_time_us=120.0, gate_error_rate=0.003,
            readout_error_rate=0.02, queue_depth=5,
            estimated_wait_seconds=120.0, carbon_per_second=0.005,
            helium_per_second=0.0005, ecoatp_cost_per_second=50.0)
        self.backends[QuantumBackend.IBM_KYIV] = QuantumResource(
            backend=QuantumBackend.IBM_KYIV,
            qubits_available=127, qubits_in_use=0, circuit_depth_max=500,
            t1_time_us=180.0, t2_time_us=120.0, gate_error_rate=0.003,
            readout_error_rate=0.02, queue_depth=3,
            estimated_wait_seconds=90.0, carbon_per_second=0.005,
            helium_per_second=0.0005, ecoatp_cost_per_second=50.0)
        self.backends[QuantumBackend.IBM_BRISBANE] = QuantumResource(
            backend=QuantumBackend.IBM_BRISBANE,
            qubits_available=127, qubits_in_use=0, circuit_depth_max=500,
            t1_time_us=180.0, t2_time_us=120.0, gate_error_rate=0.004,
            readout_error_rate=0.025, queue_depth=8,
            estimated_wait_seconds=180.0, carbon_per_second=0.006,
            helium_per_second=0.0006, ecoatp_cost_per_second=55.0)

    def _set_backend_availability(self, backend: QuantumBackend,
                                    available: bool) -> None:
        if backend in self.backends:
            self.backends[backend].is_available = available

    # ------------------------------------------------------------------
    # Boundaries
    # ------------------------------------------------------------------
    def _initialize_boundaries(self) -> None:
        # Values here are normalized (0..1) for budgets or g/kWh for carbon.
        # Note: current_value <= soft_limit <= hard_limit is enforced in
        # AdaptiveBoundary.__post_init__.
        self.boundaries = {
            "carbon": AdaptiveBoundary(
                boundary_id="carbon",
                resource_type="carbon",
                current_value=350.0,   # observed g/kWh
                soft_limit=500.0,
                hard_limit=800.0),
            "helium": AdaptiveBoundary(
                boundary_id="helium",
                resource_type="helium",
                current_value=0.4,      # scarcity index 0..1
                soft_limit=0.8,
                hard_limit=1.0),
            "energy": AdaptiveBoundary(
                boundary_id="energy",
                resource_type="energy",
                current_value=0.4,
                soft_limit=0.8,
                hard_limit=1.0),
            "compute": AdaptiveBoundary(
                boundary_id="compute",
                resource_type="compute",
                current_value=0.5,
                soft_limit=0.9,
                hard_limit=1.0),
            "token": AdaptiveBoundary(
                boundary_id="token",
                resource_type="token",
                current_value=0.5,
                soft_limit=0.9,
                hard_limit=1.0),
        }

    # ------------------------------------------------------------------
    # Quantum graph (real)
    # ------------------------------------------------------------------
    def _initialize_quantum_graph(self) -> None:
        # A graph over resources: node = resource_type, edges = coupling
        for name in ("carbon", "helium", "energy", "compute", "token"):
            self.graph_nodes[name] = {
                "id": name,
                "current_value": 0.5,
                "limit_value": 1.0,
                "weight": 1.0,
            }
        self.graph_edges: List[Tuple[str, str, float]] = [
            ("carbon", "energy", 0.7),
            ("carbon", "helium", 0.3),
            ("helium", "energy", 0.5),
            ("compute", "energy", 0.6),
            ("token", "carbon", 0.4),
        ]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _load_state_async(self) -> None:
        if self.storage is None:
            return
        try:
            raw = await asyncio.to_thread(
                self.storage.get_state, "quantum_limit_integrator_state_v8")
            if not raw:
                return
            state = json.loads(raw)
            self.sustainability_score = float(
                state.get("sustainability_score", 0.0))
            self.total_carbon_savings_kg = float(
                state.get("total_carbon_savings_kg", 0.0))
            self.total_helium_saved_l = float(
                state.get("total_helium_saved_l", 0.0))
            self._adaptive_weights = dict(state.get(
                "adaptive_weights", self.config.mopd_objective_weights))
            # Round-trip boundaries
            for bid, bd in state.get("boundaries", {}).items():
                try:
                    self.boundaries[bid] = AdaptiveBoundary.from_dict(bd)
                except Exception as e:
                    logger.debug(f"skip boundary {bid}: {e}")
        except Exception as e:
            logger.warning(f"load_state failed: {e}")

    async def _save_state(self) -> None:
        if self.storage is None:
            return
        state = {
            "sustainability_score": self.sustainability_score,
            "total_carbon_savings_kg": self.total_carbon_savings_kg,
            "total_helium_saved_l": self.total_helium_saved_l,
            "adaptive_weights": dict(self._adaptive_weights),
            "boundaries": {bid: bd.to_dict()
                            for bid, bd in self.boundaries.items()},
        }
        try:
            payload = json.dumps(_json_safe(state))
        except Exception as e:
            logger.warning(f"json encode failed: {e}")
            return
        try:
            await asyncio.to_thread(
                self.storage.save_state,
                "quantum_limit_integrator_state_v8", payload)
        except Exception as e:
            logger.warning(f"save_state failed: {e}")

    async def _persistence_save_loop(self) -> None:
        while not self.task_manager.shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self.task_manager.shutdown_event.wait(), timeout=300.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self._save_state()
            except Exception as e:
                logger.debug(f"periodic save failed: {e}")

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
    # Carbon / helium / tokens
    # ------------------------------------------------------------------
    async def _current_carbon_intensity(self) -> float:
        if self._carbon_override is not None:
            return float(self._carbon_override)
        if self.carbon_manager is None:
            return 400.0
        try:
            v = await self._carbon_circuit.call(
                lambda: self._coerce(
                    self.carbon_manager.get_current_intensity))
            v = float(v)
            if v > 0:
                return v
        except Exception as e:
            logger.debug(f"carbon intensity failed: {e}")
        return 400.0

    async def _current_carbon_price(self) -> float:
        if self.carbon_manager is None:
            return 50.0
        try:
            v = await self._coerce(self.carbon_manager.get_current_price)
            return float(v)
        except Exception:
            return 50.0

    async def _current_helium_status(self) -> Dict[str, float]:
        if self.helium_provider is None:
            return {"scarcity": 0.5, "cost_index": 1.0}
        try:
            s = float(await self._coerce(
                self.helium_provider.get_scarcity))
            c = float(await self._coerce(
                self.helium_provider.get_cost_index))
            return {"scarcity": max(0.0, min(1.0, s)), "cost_index": c}
        except Exception:
            return {"scarcity": 0.5, "cost_index": 1.0}

    async def _get_token_balance(self) -> float:
        if self._token_override is not None:
            return float(self._token_override)
        if self.token_manager is None:
            return float(self.config.token_budget_per_call)
        try:
            summary = await self._coerce(
                self.token_manager.get_system_summary)
            if isinstance(summary, dict):
                return float(summary.get(
                    "total_balance", self.config.token_budget_per_call))
        except Exception:
            pass
        # Finite sentinel: bounded by config budget.
        return float(self.config.token_budget_per_call)

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

    async def reserve_atp(self, actor: str, amount: float,
                           consumer: Any) -> BioResult:
        if self.token_manager is None:
            return BioResult(success=False, error="no_token_manager")
        try:
            r = await self._coerce(
                lambda: self.token_manager.reserve_tokens(
                    account_id=actor, amount=amount, consumer=consumer))
            # Accept tuple (success, tokens) or bool
            if isinstance(r, tuple):
                ok = bool(r[0])
                amt = float(r[1]) if len(r) > 1 else float(amount)
            else:
                ok = bool(r)
                amt = float(amount)
            return BioResult(success=ok, amount=amt if ok else 0.0)
        except Exception as e:
            logger.warning(f"ATP reserve failed: {e}", exc_info=True)
            return BioResult(success=False, error=str(e))

    async def pump_gradient(self, field: str, delta: float,
                             source: str = "quantum_integrator"
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

    # ------------------------------------------------------------------
    # Context
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
        context["token_balance_norm"] = min(
            1.0, balance / self.config.token_normalization_factor)

        return context

    # ------------------------------------------------------------------
    # MOPD: enumerate, compute, Pareto, select
    # ------------------------------------------------------------------
    async def _enumerate_execution_plans(self,
                                          job_requirements: Dict[str, Any]
                                          ) -> List[MOPDPlan]:
        available_backends = [
            b for b, r in self.backends.items() if r.is_available]
        if not available_backends:
            available_backends = [QuantumBackend.SIMULATOR]

        em_opts = list(QuantumErrorMitigation)
        shots_opts = [100, 500, 1000, 2000]
        priority_opts = [0, 1, 2]
        bridge_opts = [False, True]

        base_tokens = max(0.001, float(job_requirements.get(
            "estimated_energy_kwh", 0.001))) * 1000 * \
            self.config.quantum_cost_multiplier
        token_opts = [base_tokens * 0.5, base_tokens, base_tokens * 2.0]

        plans: List[MOPDPlan] = []
        for backend in available_backends:
            for em in em_opts:
                for shots in shots_opts:
                    for prio in priority_opts:
                        for bridge in bridge_opts:
                            for tokens in token_opts:
                                plans.append(MOPDPlan(
                                    backend=backend,
                                    error_mitigation=em,
                                    shots=shots,
                                    priority=prio,
                                    use_quantum_bridge=bridge,
                                    token_allocation=tokens,
                                ))
        return plans

    async def _compute_plan_objectives(self, plan: MOPDPlan,
                                        job_requirements: Dict[str, Any]
                                        ) -> MOPDPlan:
        base_carbon = 0.001
        base_helium = 0.0001
        base_cost = 1.0
        base_latency = 100.0
        base_success = 0.95

        if plan.backend in self.backends:
            br = self.backends[plan.backend]
            base_carbon = br.carbon_per_second
            base_helium = br.helium_per_second
            base_cost = br.total_token_cost_per_second
            base_latency = br.estimated_wait_seconds * 1000.0 + 100.0
            base_success = max(0.5, 1.0 - br.gate_error_rate * 10.0)

        if plan.error_mitigation == QuantumErrorMitigation.ZNE:
            base_latency *= 1.5; base_cost *= 1.2
            base_success = min(1.0, base_success * 1.05)
        elif plan.error_mitigation == QuantumErrorMitigation.PEC:
            base_latency *= 2.0; base_cost *= 1.5
            base_success = min(1.0, base_success * 1.1)
        elif plan.error_mitigation == QuantumErrorMitigation.DD:
            base_latency *= 1.2; base_cost *= 1.1
            base_success = min(1.0, base_success * 1.03)
        elif plan.error_mitigation == QuantumErrorMitigation.M3:
            base_latency *= 1.1; base_cost *= 1.1
            base_success = min(1.0, base_success * 1.02)

        shot_factor = max(0.1, plan.shots / 1000.0)
        base_latency *= shot_factor
        base_cost *= shot_factor
        base_carbon *= shot_factor
        base_helium *= shot_factor

        if plan.priority > 0:
            base_latency *= (1.0 - 0.1 * plan.priority)
            base_cost *= (1.0 + 0.2 * plan.priority)
            base_success = min(1.0, base_success *
                                 (1.0 + 0.02 * plan.priority))

        if plan.use_quantum_bridge:
            base_latency *= 0.8
            base_cost *= 0.9
            base_success = min(1.0, base_success * 0.98)

        base_cost += plan.token_allocation * 0.1
        base_success = min(1.0, base_success + 0.01 * (
            plan.token_allocation /
            max(self.config.token_normalization_factor / 10.0, 1e-6)))

        plan.carbon_kg = max(0.0, float(base_carbon))
        plan.helium_units = max(0.0, float(base_helium))
        plan.cost_usd = max(0.0, float(base_cost))
        plan.latency_ms = max(0.0, float(base_latency))
        plan.success_probability = min(1.0, max(0.0,
                                                  float(base_success)))
        return plan

    async def _generate_pareto_front_for_quantum_job(
        self, job_requirements: Dict[str, Any]
    ) -> List[MOPDPlan]:
        raw = await self._enumerate_execution_plans(job_requirements)
        computed = [await self._compute_plan_objectives(p, job_requirements)
                     for p in raw]
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
                    atp=plan.token_allocation /
                        self.config.token_normalization_factor)
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
        # Ascending on cost: lower is better
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
            nl = (f"Selected {plan.backend.value} with "
                  f"{plan.error_mitigation.value} mitigation, "
                  f"shots={plan.shots}, priority={plan.priority}, "
                  f"bridge={plan.use_quantum_bridge}; "
                  f"carbon={plan.carbon_kg:.4f}kg, "
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
    async def check_invariants(self, plan: MOPDPlan) -> List[str]:
        token_budget = await self._get_token_budget_remaining()
        await self.temporal_safety.push({
            "carbon_kg": plan.carbon_kg,
            "helium_units": plan.helium_units,
            "token_allocation": plan.token_allocation,
            "carbon_budget_kg": self.config.carbon_budget_per_call_kg,
            "helium_budget_units":
                self.config.helium_budget_per_call_units,
            "token_budget": token_budget,
            "drastic_actions": 0,
        })
        results = await self.temporal_safety.check()
        return [rid for rid, ok in results.items() if not ok]

    async def _get_token_budget_remaining(self) -> float:
        # Never return inf — use the config budget as a bound when the
        # manager is absent, otherwise the manager's balance.
        return await self._get_token_balance()

    # ------------------------------------------------------------------
    # HITL
    # ------------------------------------------------------------------
    async def request_approval(self, plan: MOPDPlan,
                                context: Dict[str, Any]
                                ) -> Tuple[bool, Optional[str]]:
        if not self.config.enable_human_approval:
            return True, None
        if self.approval_handler is None:
            logger.warning(
                "HITL enabled but no handler injected; deferring")
            return False, "no_handler"
        decision = {
            "action": "approve_quantum_plan",
            "candidates": [
                {"id": "approve",
                 "success_probability": plan.success_probability,
                 "carbon_kg": plan.carbon_kg},
                {"id": "defer", "success_probability": 0.0,
                 "carbon_kg": 0.0},
            ],
            "context": {"plan": plan.to_dict(), "context": context},
        }
        try:
            approved = await self.hitl.request_approval(decision)
            return bool(approved), None
        except Exception as e:
            logger.warning(f"HITL failed: {e}")
            return False, "hitl_error"

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
        return all(r.is_available for r in self.backends.values())

    def _snapshot_for_chaos(self) -> Dict[str, Any]:
        return {
            "carbon_override": self._carbon_override,
            "token_override": self._token_override,
            "backend_availability": {
                b.value: r.is_available
                for b, r in self.backends.items()},
        }

    def _restore_from_chaos(self, snap: Dict[str, Any]) -> None:
        self._carbon_override = snap.get("carbon_override")
        self._token_override = snap.get("token_override")
        for name, avail in snap.get("backend_availability", {}).items():
            try:
                b = QuantumBackend(name)
                if b in self.backends:
                    self.backends[b].is_available = bool(avail)
            except Exception:
                continue

    async def inject_fault(self, fault_type: str,
                            **params: Any) -> Dict[str, Any]:
        if fault_type == "carbon_spike":
            self._carbon_override = 800.0
            return {"status": "completed", "fault_type": fault_type,
                    "carbon_override": 800.0}
        if fault_type == "token_depletion":
            self._token_override = 0.0
            return {"status": "completed", "fault_type": fault_type,
                    "token_override": 0.0}
        if fault_type == "backend_unavailable":
            backend_name = params.get("backend",
                                        QuantumBackend.SIMULATOR.value)
            try:
                backend = (backend_name
                            if isinstance(backend_name, QuantumBackend)
                            else QuantumBackend(backend_name))
            except ValueError:
                raise ValueError(
                    f"unknown backend: {backend_name}")
            self._set_backend_availability(backend, False)
            return {"status": "completed", "fault_type": fault_type,
                    "backend": backend.value}
        if fault_type in ChaosEngine.FAULT_TYPES:
            return await self.chaos.run_experiment(
                params.pop("name",
                            f"manual_{uuid.uuid4().hex[:6]}"),
                fault_type)
        raise ValueError(f"unknown fault type: {fault_type}")

    async def run_chaos_test(self, runs: int = 3) -> Dict[str, Any]:
        if not self.config.enable_chaos_testing:
            return {"status": "disabled"}
        results: List[Dict[str, Any]] = []
        results.append(await self.inject_fault("carbon_spike"))
        self._carbon_override = None
        results.append(await self.inject_fault("token_depletion"))
        self._token_override = None
        results.append(await self.inject_fault(
            "backend_unavailable",
            backend=QuantumBackend.SIMULATOR.value))
        self._set_backend_availability(QuantumBackend.SIMULATOR, True)
        return {
            "status": "success",
            "runs": len(results),
            "rollbacks": self.chaos.rollback_count,
            "results": results,
        }

    # ------------------------------------------------------------------
    # Propose (teacher interface for MOPD)
    # ------------------------------------------------------------------
    async def propose_async(self, context: Dict[str, Any]) -> Dict[str, Any]:
        try:
            carbon_intensity = float(context.get(
                "carbon_intensity_g_per_kwh",
                context.get("carbon_intensity", 0.5) * 1000))
            helium = await self._current_helium_status()
            helium_scarcity = float(context.get(
                "helium_scarcity", helium["scarcity"]))

            token_balance = await self._get_token_balance()

            recommendations = {
                "carbon_budget_kg": 5.0 if carbon_intensity > 400 else 10.0,
                "helium_recovery": helium_scarcity > 0.6,
                "renewable_share": 0.8 if carbon_intensity < 300 else 0.4,
                "token_optimization":
                    token_balance > self.config.token_normalization_factor,
            }
            explanation = (
                f"Carbon intensity {carbon_intensity:.0f} g/kWh, "
                f"helium scarcity {helium_scarcity:.2f}.")

            pareto_front: List[MOPDPlan] = []
            selected_plan: Optional[MOPDPlan] = None
            deferred_reason: Optional[str] = None

            if self.config.enable_mopd and "job_requirements" in context:
                job_reqs = context["job_requirements"]
                pareto_front = await \
                    self._generate_pareto_front_for_quantum_job(job_reqs)

                if self.config.enable_temporal_safety and pareto_front:
                    safe = []
                    for p in pareto_front:
                        violations = await self.check_invariants(p)
                        if not violations:
                            safe.append(p)
                    if safe:
                        pareto_front = safe
                    else:
                        scored = []
                        for p in pareto_front:
                            v = await self.check_invariants(p)
                            scored.append((len(v), p))
                        scored.sort(key=lambda kv: kv[0])
                        pareto_front = [scored[0][1]]

                selected_plan = self._select_best_from_pareto(pareto_front)
                if selected_plan is not None:
                    selected_plan.explanation = self._explain_plan(
                        selected_plan)
                    approved, reason = await self.request_approval(
                        selected_plan, context)
                    if not approved:
                        deferred_reason = reason or "hitl_rejected"
                        recommendations["best_plan"] = \
                            selected_plan.to_dict()
                        recommendations["deferred"] = True
                    else:
                        recommendations["best_plan"] = \
                            selected_plan.to_dict()
                    explanation += f" Selected plan: " \
                        f"{selected_plan.explanation.natural_language if selected_plan.explanation else ''}"

            await self._publish_feedback(
                task_id=f"quantum_propose_{uuid.uuid4().hex[:8]}",
                action="propose",
                quality=0.9,
                details={"context": context,
                          "explanation": explanation},
                tags=["quantum", "proposal", "xai"])

            await self._check_drift()

            return {
                "recommendations": recommendations,
                "options": [p.to_dict() for p in pareto_front],
                "explanation": explanation,
                "pareto_front": [p.to_dict() for p in pareto_front]
                    if pareto_front else None,
                "requires_approval":
                    selected_plan is not None and
                    self.config.enable_human_approval,
                "deferred": deferred_reason,
            }
        except Exception as e:
            logger.error(f"propose_async failed: {e}", exc_info=True)
            return {
                "recommendations": {
                    "carbon_budget_kg": 10.0,
                    "helium_recovery": False,
                    "renewable_share": 0.5,
                    "token_optimization": False,
                },
                "options": [],
                "explanation": f"Proposal failed: {e}",
                "requires_approval": False,
                "error": str(e),
            }

    # ------------------------------------------------------------------
    # Policy probs
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if not self.config.enable_mopd:
            return [0.25] * 4
        job_reqs = {
            "estimated_energy_kwh": float(state.get(
                "estimated_energy_kwh", 0.001)),
        }
        front = await self._generate_pareto_front_for_quantum_job(job_reqs)
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
                    atp=plan.token_allocation /
                        self.config.token_normalization_factor)
            else:
                cost = plan.scalarised_score
            scores.append(float(cost))

        # Lower cost is better → negate logits before softmax
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
            if plan.carbon_kg < 0.0005:
                idx = 0
            elif plan.latency_ms < 50:
                idx = 1
            elif plan.cost_usd < 1.5:
                idx = 2
            else:
                idx = 3
            out[idx] += p
        total = sum(out) or 1.0
        return [v / total for v in out]

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
                feedback_type="quantum_limit",
                adaptive_cost_value=0.0,
                state=details,
                candidates=[{"action": a} for a in (
                    "low_carbon", "low_latency", "low_cost", "balanced")],
                source="quantum_limit_integrator",
                environment="production",
                tags=tags)
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.debug(f"feedback publish failed: {e}")

    # ------------------------------------------------------------------
    # Health / stats
    # ------------------------------------------------------------------
    async def get_health_status(self) -> ComponentHealth:
        try:
            if self.config.carbon_budget_per_call_kg <= 0:
                return ComponentHealth(
                    HealthStatus.UNHEALTHY, 0.0,
                    {"reason": "invalid_carbon_budget"})
            backends_ok = sum(1 for r in self.backends.values()
                               if r.is_available)
            total = max(len(self.backends), 1)
            score = backends_ok / total
            status = (HealthStatus.HEALTHY if score > 0.8 else
                      HealthStatus.DEGRADED if score > 0.5 else
                      HealthStatus.UNHEALTHY)
            details = {
                "backends_available": backends_ok,
                "total_backends": total,
                "background_tasks": self.task_manager.task_count(),
                "temporal_violations": self.temporal_safety.counts(),
                "carbon_circuit": self._carbon_circuit.state.value,
            }
            return ComponentHealth(status, score, details)
        except Exception as e:
            return ComponentHealth(
                HealthStatus.UNHEALTHY, 0.0, {"error": str(e)})

    def get_integration_status(self) -> Dict[str, Any]:
        return {
            "expert_id": self.expert_id,
            "mopd_enabled": self.config.enable_mopd,
            "mopd_weights": dict(self._adaptive_weights),
            "temporal_safety_enabled":
                self.config.enable_temporal_safety,
            "human_approval_enabled":
                self.config.enable_human_approval,
            "chaos_testing_enabled":
                self.config.enable_chaos_testing,
            "backends": [b.value for b in self.backends.keys()],
            "background_tasks": self.task_manager.task_count(),
        }

    async def self_heal(self) -> None:
        logger.info("Self-healing quantum integrator")
        self._initialize_boundaries()
        for r in self.backends.values():
            r.is_available = True
        self.health_status = HealthStatus.HEALTHY
        self.last_error = None
        await self._save_state()


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
    integ = QuantumLimitGraphIntegrator(
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


class TestSharedRuntime:
    @pytest.mark.asyncio
    async def test_circuit_breaker_opens(self):
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
        call_count = {"n": 0}

        async def flaky_steady():
            call_count["n"] += 1
            return call_count["n"] == 1

        def snap():
            return dict(state)

        def restore(s):
            state.update(s)

        c = ChaosEngine(steady_state=flaky_steady, snapshot=snap,
                        restore=restore, rng=random.Random(0))
        r = await c.run_experiment("rb", "latency")
        assert r["rolled_back"] is True


class TestBoundaryInit:
    def test_boundary_order(self):
        # __post_init__ must clamp soft ≤ hard and current ≤ hard
        b = AdaptiveBoundary("b", "r", current_value=10.0,
                             soft_limit=5.0, hard_limit=1.0)
        assert b.soft_limit <= b.hard_limit
        assert b.current_value <= b.hard_limit

    @pytest.mark.asyncio
    async def test_default_boundaries_valid(self, integrator):
        for b in integrator.boundaries.values():
            assert b.current_value <= b.hard_limit
            assert b.soft_limit <= b.hard_limit


class TestBackendRegistry:
    @pytest.mark.asyncio
    async def test_multiple_backends_registered(self, integrator):
        assert len(integrator.backends) >= 4

    @pytest.mark.asyncio
    async def test_backend_availability_toggle(self, integrator):
        integrator._set_backend_availability(
            QuantumBackend.SIMULATOR, False)
        assert not integrator.backends[QuantumBackend.SIMULATOR].is_available
        integrator._set_backend_availability(
            QuantumBackend.SIMULATOR, True)
        assert integrator.backends[QuantumBackend.SIMULATOR].is_available


class TestMOPDSelection:
    @pytest.mark.asyncio
    async def test_pareto_front_not_empty(self, integrator):
        front = await integrator._generate_pareto_front_for_quantum_job(
            {"estimated_energy_kwh": 0.001})
        assert len(front) >= 1

    @pytest.mark.asyncio
    async def test_select_best_picks_low_cost(self, integrator):
        a = MOPDPlan(carbon_kg=1.0, cost_usd=1.0, latency_ms=100.0,
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
    async def test_propose_async_returns_dict(self, integrator):
        r = await integrator.propose_async({
            "carbon_intensity_g_per_kwh": 400.0,
            "job_requirements": {"estimated_energy_kwh": 0.001},
        })
        assert isinstance(r, dict)
        assert "recommendations" in r
        assert "explanation" in r

    @pytest.mark.asyncio
    async def test_propose_async_includes_pareto(self, integrator):
        r = await integrator.propose_async({
            "job_requirements": {"estimated_energy_kwh": 0.001},
        })
        assert r.get("pareto_front") is not None
        assert len(r["pareto_front"]) >= 1


class TestXAI:
    @pytest.mark.asyncio
    async def test_explanation_has_attributions(self, integrator):
        r = await integrator.propose_async({
            "job_requirements": {"estimated_energy_kwh": 0.001},
        })
        best = r["recommendations"].get("best_plan")
        assert best is not None
        expl = best.get("explanation")
        assert expl is not None
        assert expl["method"] == "integrated_gradients"
        assert len(expl["attributions"]) == 5
        assert expl["counterfactual"] is not None


class TestTemporalSafety:
    @pytest.mark.asyncio
    async def test_safe_plan_passes(self, integrator):
        p = MOPDPlan(carbon_kg=0.1, helium_units=0.05,
                      token_allocation=5.0)
        v = await integrator.check_invariants(p)
        assert v == []

    @pytest.mark.asyncio
    async def test_high_carbon_violates(self, integrator):
        p = MOPDPlan(carbon_kg=100.0, helium_units=0.05,
                      token_allocation=5.0)
        v = await integrator.check_invariants(p)
        assert len(v) >= 1

    @pytest.mark.asyncio
    async def test_rules_registered(self, integrator):
        assert len(integrator.temporal_safety.counts()) >= 6


class TestHITL:
    @pytest.mark.asyncio
    async def test_hitl_disabled_auto_approves(self, integrator):
        p = MOPDPlan()
        ok, reason = await integrator.request_approval(p, {})
        assert ok is True
        assert reason is None

    @pytest.mark.asyncio
    async def test_hitl_enabled_no_handler_defers(self):
        cfg = QuantumLimitIntegratorConfig(enable_human_approval=True)
        integ = QuantumLimitGraphIntegrator(config=cfg, seed=1)
        await integ.initialize()
        try:
            ok, reason = await integ.request_approval(MOPDPlan(), {})
            assert ok is False
            assert reason == "no_handler"
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_rejecting_handler_defers_propose(self):
        class _Reject:
            async def request_approval(self, decision,
                                        user_id="default"):
                return False

        cfg = QuantumLimitIntegratorConfig(enable_human_approval=True)
        integ = QuantumLimitGraphIntegrator(
            config=cfg, approval_handler=_Reject(), seed=1)
        await integ.initialize()
        try:
            r = await integ.propose_async({
                "job_requirements": {"estimated_energy_kwh": 0.001},
            })
            assert r["recommendations"].get("deferred") is True
        finally:
            await integ.shutdown()

    @pytest.mark.asyncio
    async def test_hitl_approving_handler_succeeds(self):
        class _Approve:
            async def request_approval(self, decision,
                                        user_id="default"):
                return True

        cfg = QuantumLimitIntegratorConfig(enable_human_approval=True)
        integ = QuantumLimitGraphIntegrator(
            config=cfg, approval_handler=_Approve(), seed=1)
        await integ.initialize()
        try:
            r = await integ.propose_async({
                "job_requirements": {"estimated_energy_kwh": 0.001},
            })
            assert r["recommendations"].get("deferred") is None
        finally:
            await integ.shutdown()


class TestPersistence:
    @pytest.mark.asyncio
    async def test_save_state_json_safe(self, integrator, storage):
        await integrator._save_state()
        raw = storage.get_state("quantum_limit_integrator_state_v8")
        assert raw is not None
        data = json.loads(raw)
        assert "boundaries" in data
        for b in data["boundaries"].values():
            assert isinstance(b["boundary_source"], str)
            assert isinstance(b["last_updated"], str)

    @pytest.mark.asyncio
    async def test_roundtrip_adaptive_weights(self, storage):
        integ1 = QuantumLimitGraphIntegrator(storage=storage, seed=1)
        await integ1.initialize()
        try:
            integ1._adaptive_weights = {"carbon": 0.5, "helium": 0.2,
                                          "cost": 0.15, "latency": 0.1,
                                          "success_prob": 0.05}
            await integ1._save_state()
        finally:
            await integ1.shutdown()

        integ2 = QuantumLimitGraphIntegrator(storage=storage, seed=2)
        await integ2.initialize()
        try:
            assert integ2._adaptive_weights["carbon"] == 0.5
        finally:
            await integ2.shutdown()

    @pytest.mark.asyncio
    async def test_no_pickle_in_state(self, integrator, storage):
        await integrator._save_state()
        raw = storage.get_state("quantum_limit_integrator_state_v8")
        json.loads(raw)


class TestConfigImmutability:
    @pytest.mark.asyncio
    async def test_config_unchanged_on_drift(self, integrator):
        original = dict(integrator.config.mopd_objective_weights)
        for _ in range(20):
            await integrator._check_drift()
        assert integrator.config.mopd_objective_weights == original

    @pytest.mark.asyncio
    async def test_adaptive_weights_normalised(self, integrator):
        for _ in range(10):
            await integrator._check_drift()
        total = sum(integrator._adaptive_weights.values())
        assert abs(total - 1.0) < 1e-6 or total == 0.0


class TestChaos:
    @pytest.mark.asyncio
    async def test_inject_carbon_spike(self, integrator):
        r = await integrator.inject_fault("carbon_spike")
        assert r["status"] == "completed"
        v = await integrator._current_carbon_intensity()
        assert v == 800.0
        integrator._carbon_override = None

    @pytest.mark.asyncio
    async def test_inject_token_depletion(self, integrator):
        r = await integrator.inject_fault("token_depletion")
        assert r["status"] == "completed"
        bal = await integrator._get_token_balance()
        assert bal == 0.0
        integrator._token_override = None

    @pytest.mark.asyncio
    async def test_inject_backend_unavailable(self, integrator):
        r = await integrator.inject_fault(
            "backend_unavailable",
            backend=QuantumBackend.SIMULATOR.value)
        assert r["status"] == "completed"
        assert not integrator.backends[QuantumBackend.SIMULATOR].is_available
        integrator._set_backend_availability(
            QuantumBackend.SIMULATOR, True)

    @pytest.mark.asyncio
    async def test_unknown_fault_raises(self, integrator):
        with pytest.raises(ValueError):
            await integrator.inject_fault("nonsense")

    @pytest.mark.asyncio
    async def test_run_chaos_test_disabled(self, integrator):
        r = await integrator.run_chaos_test()
        assert r["status"] == "disabled"

    @pytest.mark.asyncio
    async def test_run_chaos_test_enabled(self, storage):
        cfg = QuantumLimitIntegratorConfig(enable_chaos_testing=True)
        integ = QuantumLimitGraphIntegrator(config=cfg, storage=storage,
                                              seed=1)
        await integ.initialize()
        try:
            r = await integ.run_chaos_test()
            assert r["status"] == "success"
            assert r["runs"] >= 3
        finally:
            await integ.shutdown()


class TestBioIntegration:
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

            def reserve_tokens(self, *, account_id, amount,
                                 consumer=None):
                self.calls.append(("reserve", account_id, amount))
                return True, amount

            def get_system_summary(self):
                return {"total_balance": 1000.0}

        tm = _Sync()
        integ = QuantumLimitGraphIntegrator(token_manager=tm, seed=1)
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

            async def reserve_tokens(self, *, account_id, amount,
                                       consumer=None):
                self.calls.append(("reserve", account_id, amount))
                return True, amount

            async def get_system_summary(self):
                return {"total_balance": 1000.0}

        tm = _Async()
        integ = QuantumLimitGraphIntegrator(token_manager=tm, seed=1)
        r = await integ.spend_atp("a", 0.5)
        assert r.success
        assert tm.calls == [("spend", "a", 0.5)]

    @pytest.mark.asyncio
    async def test_reserve_tuple_result(self):
        class _Sync:
            def reserve_tokens(self, *, account_id, amount,
                                 consumer=None):
                return True, amount

            def get_system_summary(self):
                return {"total_balance": 100.0}

            def spend(self, a, b):
                pass

            def earn(self, a, b):
                pass

        integ = QuantumLimitGraphIntegrator(
            token_manager=_Sync(), seed=1)
        r = await integ.reserve_atp("actor", 5.0, BioConsumer.QUANTUM_COMPUTING)
        assert r.success
        assert r.amount == 5.0

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
        integ = QuantumLimitGraphIntegrator(gradient_manager=gm, seed=1)
        r = await integ.pump_gradient("carbon", 0.1, source="t")
        assert r.success
        assert gm.calls == [("carbon", 0.1, "t")]

    @pytest.mark.asyncio
    async def test_finite_budget_when_no_manager(self, integrator):
        budget = await integrator._get_token_budget_remaining()
        assert math.isfinite(budget)
        assert budget >= 0


class TestHealthAndShutdown:
    @pytest.mark.asyncio
    async def test_health_component(self, integrator):
        h = await integrator.get_health_status()
        assert isinstance(h, ComponentHealth)
        assert h.score > 0
        assert h.details["backends_available"] >= 1

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage):
        cfg = QuantumLimitIntegratorConfig(enable_persistence=True)
        integ = QuantumLimitGraphIntegrator(config=cfg, storage=storage,
                                              seed=1)
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
        await integrator.propose_async({
            "job_requirements": {"estimated_energy_kwh": 0.001},
        })
        assert len(queue.published) >= 1
        topic, payload = queue.published[-1]
        assert topic == "feedback_events"
        parsed = json.loads(payload)
        assert "task_id" in parsed


class TestDeterminism:
    @pytest.mark.asyncio
    async def test_same_seed_same_front(self, storage):
        def _build(seed):
            return QuantumLimitGraphIntegrator(storage=storage, seed=seed)
        i1 = _build(7); i2 = _build(7)
        await i1.initialize(); await i2.initialize()
        try:
            f1 = await i1._generate_pareto_front_for_quantum_job(
                {"estimated_energy_kwh": 0.001})
            f2 = await i2._generate_pareto_front_for_quantum_job(
                {"estimated_energy_kwh": 0.001})
            assert len(f1) == len(f2)
            for a, b in zip(f1, f2):
                assert a.carbon_kg == b.carbon_kg
                assert a.cost_usd == b.cost_usd
        finally:
            await i1.shutdown(); await i2.shutdown()


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def demo():
        integ = QuantumLimitGraphIntegrator(seed=1)
        await integ.initialize()
        try:
            r = await integ.propose_async({
                "carbon_intensity_g_per_kwh": 550.0,
                "job_requirements": {"estimated_energy_kwh": 0.005},
            })
            print("recommendations:",
                  json.dumps(r["recommendations"], indent=2, default=str))
            print("pareto size:", len(r.get("pareto_front") or []))
            h = await integ.get_health_status()
            print(f"health: {h.status.value} score={h.score:.2f}")
            print("integration:",
                  json.dumps(integ.get_integration_status(),
                             indent=2, default=str))
        finally:
            await integ.shutdown()

    asyncio.run(demo())
