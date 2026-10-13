#!/usr/bin/env python3
"""
Enhanced Expert Registry v7.2.0 — Self-Contained Single-File Bundle
====================================================================

This file is a complete, runnable, and testable rewrite of the v7.1.0
Expert Registry. It contains everything needed to exercise every feature:

  1. Local, fault-injectable in-memory storage + message queue.
  2. Local FeedbackEvent schema.
  3. Ten enhancement modules with real implementations:
        - QuantumDistillationModule  (density-matrix teacher superposition)
        - CausalFeatureMask          (PC-style partial-correlation discovery)
        - FederatedGreenAggregator   (weighted FedAvg + DP + sparsification)
        - ExpertAuction              (LinUCB contextual bandit)
        - TemporalSafetyMonitor      (MTL with past-time operators)
        - XAIExplainer               (Integrated Gradients + counterfactuals)
        - PrecisionController        (layer-wise sensitivity + hysteresis)
        - CarbonMarketClient         (budget tracking + EMA price + REC)
        - ChaosEngine                (steady-state hypothesis + auto-rollback)
        - HumanApprovalHandler       (uncertainty-sampled HITL)
  4. MoE soft gating with load-balancing loss.
  5. Pareto front (NSGA-II crowding distance).
  6. Contextual weight adjuster (online GD with regularisation).
  7. Drift detector (Page-Hinkley).
  8. Predictive forecaster (Holt exponential smoothing ensemble).
  9. `ExpertRegistry` facade wiring all of the above.
 10. Comprehensive pytest suite at the bottom.

Key fixes over v7.1.0
---------------------
  * `_background_tasks` is properly initialised and cancelled on shutdown.
  * `wait_until_ready` raises if the registry was never initialised.
  * JSON-safe serialisation via `to_dict` / `from_dict`.
  * `_load_state_from_storage` is implemented symmetrically with save.
  * Chaos cannot push fitness out of [0, 1].
  * Fitness weight override merges with defaults (no `KeyError`).
  * Background loops honour a `_shutdown_event`.
  * `FeedbackEvent.create_with_context(candidates=...)` produces a list of
    dicts, not a list containing a set literal.
  * Optional dependencies (`numpy`, `sklearn`) are handled gracefully.

Run tests:
    pytest expert_registry_v7_2.py -v
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import time
import uuid
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Protocol, Set,
    Tuple, Union,
)

import pytest
import pytest_asyncio

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import SGDRegressor, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.neural_network import MLPClassifier
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    SKLEARN_AVAILABLE = False


logger = logging.getLogger("expert_registry")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


# =============================================================================
# LOCAL STUBS — replaceable with production adapters
# =============================================================================
class InMemoryStorage:
    """Fault-injectable in-memory storage.

    Any method listed in ``fail_on`` raises ``RuntimeError``. Methods
    listed in ``silent`` swallow the injected failure and return ``None``.
    """

    def __init__(self):
        self._state: Dict[str, str] = {}
        self.calls: List[Tuple[str, Tuple]] = []
        self.fail_on: Optional[str] = None
        self.silent: Set[str] = set()

    def _log(self, method: str, args: Tuple = ()) -> bool:
        self.calls.append((method, args))
        if self.fail_on == method:
            if method in self.silent:
                return False
            raise RuntimeError(f"injected failure in {method}")
        return True

    def save_state(self, key: str, value: str) -> None:
        if not self._log("save_state", (key,)):
            return
        self._state[key] = value

    def get_state(self, key: str) -> Optional[str]:
        self._log("get_state", (key,))
        return self._state.get(key)

    def delete_state(self, key: str) -> None:
        if not self._log("delete_state", (key,)):
            return
        self._state.pop(key, None)


class AsyncMessageQueue:
    """Minimal async pub/sub used by FeedbackEvent emission."""

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


class FeedbackEvent:
    """Local FeedbackEvent schema."""

    def __init__(self, **fields: Any):
        self.fields = fields

    @classmethod
    def create_with_context(cls, *, task_id: str, selected_action: str,
                            quality_score: float, energy_joules: float,
                            carbon_g: float, feedback_type: str,
                            adaptive_cost_value: float,
                            state: Dict[str, Any],
                            candidates: List[Dict[str, Any]],
                            source: str, environment: str,
                            tags: List[str]) -> "FeedbackEvent":
        return cls(
            task_id=task_id, selected_action=selected_action,
            quality_score=quality_score, energy_joules=energy_joules,
            carbon_g=carbon_g, feedback_type=feedback_type,
            adaptive_cost_value=adaptive_cost_value, state=state,
            candidates=candidates, source=source, environment=environment,
            tags=tags, ts=datetime.now(timezone.utc).isoformat(),
        )

    def to_json(self) -> str:
        return json.dumps(self.fields, default=str)


class AdaptiveCostFunction:
    """Tracks current cost weights; used by drift detector."""

    def __init__(self, storage: InMemoryStorage):
        self.storage = storage
        self.weights = {"accuracy": 0.4, "carbon": 0.2,
                        "cost": 0.2, "latency": 0.2}

    def get_current_weights(self) -> Dict[str, float]:
        return dict(self.weights)


class ParetoGating:
    """Legacy external Pareto gate; kept for compatibility."""

    def __init__(self):
        self.front: List[Dict[str, Any]] = []

    def update(self, entry: Dict[str, Any]) -> bool:
        self.front.append(entry)
        return True


class DriftDetector:
    """Simple drift detector using Page-Hinkley on a scalar stream."""

    def __init__(self, storage: InMemoryStorage, adaptive_cost: AdaptiveCostFunction,
                 delta: float = 0.005, threshold: float = 0.5):
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.delta = delta
        self.threshold = threshold
        self._mean = 0.0
        self._cum = 0.0
        self._min_cum = 0.0
        self.drift_detected = False

    async def check_drift(self, weights: Dict[str, float]) -> bool:
        x = sum(weights.values()) / max(len(weights), 1)
        self._mean += (x - self._mean) * 0.1
        self._cum += x - self._mean - self.delta
        self._min_cum = min(self._min_cum, self._cum)
        if self._cum - self._min_cum > self.threshold:
            self.drift_detected = True
            self._cum = 0.0
            self._min_cum = 0.0
            return True
        return False


class MetricsRegistry:
    """Trivial counter/gauge registry."""

    def __init__(self):
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}

    def inc(self, name: str, amount: int = 1) -> None:
        self.counters[name] += amount

    def set(self, name: str, value: float) -> None:
        self.gauges[name] = value


# =============================================================================
# ENUMS AND DATA CLASSES
# =============================================================================
class ExpertLifecycleState(Enum):
    REGISTERED = "registered"
    VALIDATING = "validating"
    CERTIFIED = "certified"
    ACTIVE = "active"
    DEGRADED = "degraded"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"

    def is_available(self) -> bool:
        return self in (ExpertLifecycleState.CERTIFIED,
                        ExpertLifecycleState.ACTIVE)


class ExpertDomain(Enum):
    ENERGY = "energy"
    DATA = "data"
    IOT = "iot"
    QUANTUM = "quantum"
    HELIUM = "helium"
    GENERAL = "general"


class HardwareProfile(Enum):
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    QPU = "qpu"
    HYBRID = "hybrid"


class ExpertVersion:
    __slots__ = ("major", "minor", "patch")

    def __init__(self, major: int = 1, minor: int = 0, patch: int = 0):
        self.major = int(major)
        self.minor = int(minor)
        self.patch = int(patch)

    def to_string(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def _key(self) -> Tuple[int, int, int]:
        return (self.major, self.minor, self.patch)

    def is_newer_than(self, other: "ExpertVersion") -> bool:
        return self._key() > other._key()

    def is_same_as(self, other: "ExpertVersion") -> bool:
        return self._key() == other._key()

    @classmethod
    def from_string(cls, s: str) -> "ExpertVersion":
        parts = [int(p) for p in s.split(".")]
        while len(parts) < 3:
            parts.append(0)
        return cls(*parts[:3])


@dataclass
class HealthMetrics:
    success_rate: float = 0.9
    quantum_efficiency: float = 0.5
    quantum_advantage_score: float = 0.5
    carbon_efficiency: float = 0.5
    helium_efficiency: float = 0.5
    avg_latency_ms: float = 100.0
    last_heartbeat: Optional[datetime] = None

    def __post_init__(self):
        if self.last_heartbeat is None:
            self.last_heartbeat = datetime.now(timezone.utc)

    def calculate_health_score(self) -> float:
        return (self.success_rate + self.carbon_efficiency +
                self.helium_efficiency + self.quantum_efficiency) / 4.0

    def calculate_sustainability_score(self) -> float:
        return (self.carbon_efficiency + self.helium_efficiency) / 2.0

    def to_dict(self) -> Dict[str, Any]:
        d = self.__dict__.copy()
        d["last_heartbeat"] = self.last_heartbeat.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "HealthMetrics":
        d = dict(d or {})
        hb = d.pop("last_heartbeat", None)
        obj = cls(**d)
        if hb:
            obj.last_heartbeat = datetime.fromisoformat(hb)
        return obj


class ExpertProfile:
    def __init__(self, expert_id: str, expert_name: str, version: ExpertVersion,
                 domain: ExpertDomain, hardware_profile: HardwareProfile,
                 accuracy_score: float = 0.5, reliability_score: float = 0.5,
                 efficiency_score: float = 0.5, helium_per_inference: float = 0.01,
                 carbon_per_inference: float = 0.001,
                 energy_per_inference: float = 0.01,
                 quantum_capable: bool = False, quantum_qubits: int = 0,
                 quantum_backend: Optional[str] = None,
                 sustainability_score: float = 0.5,
                 health: Optional[HealthMetrics] = None,
                 lifecycle_state: ExpertLifecycleState = ExpertLifecycleState.REGISTERED,
                 is_active: bool = True,
                 replaces_expert: Optional[str] = None,
                 lineage: Optional[List[str]] = None):
        self.expert_id = expert_id
        self.expert_name = expert_name
        self.version = version
        self.domain = domain
        self.hardware_profile = hardware_profile
        self.accuracy_score = float(accuracy_score)
        self.reliability_score = float(reliability_score)
        self.efficiency_score = float(efficiency_score)
        self.helium_per_inference = float(helium_per_inference)
        self.carbon_per_inference = float(carbon_per_inference)
        self.energy_per_inference = float(energy_per_inference)
        self.quantum_capable = bool(quantum_capable)
        self.quantum_qubits = int(quantum_qubits)
        self.quantum_backend = quantum_backend
        self.sustainability_score = float(sustainability_score)
        self.health = health or HealthMetrics()
        self.lifecycle_state = lifecycle_state
        self.is_active = bool(is_active)
        self.replaces_expert = replaces_expert
        self.lineage = list(lineage) if lineage else []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expert_id": self.expert_id,
            "expert_name": self.expert_name,
            "version": self.version.to_string(),
            "domain": self.domain.value,
            "hardware_profile": self.hardware_profile.value,
            "accuracy_score": self.accuracy_score,
            "reliability_score": self.reliability_score,
            "efficiency_score": self.efficiency_score,
            "helium_per_inference": self.helium_per_inference,
            "carbon_per_inference": self.carbon_per_inference,
            "energy_per_inference": self.energy_per_inference,
            "quantum_capable": self.quantum_capable,
            "quantum_qubits": self.quantum_qubits,
            "quantum_backend": self.quantum_backend,
            "sustainability_score": self.sustainability_score,
            "health": self.health.to_dict(),
            "lifecycle_state": self.lifecycle_state.value,
            "is_active": self.is_active,
            "replaces_expert": self.replaces_expert,
            "lineage": list(self.lineage),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ExpertProfile":
        return cls(
            expert_id=d["expert_id"],
            expert_name=d["expert_name"],
            version=ExpertVersion.from_string(d["version"]),
            domain=ExpertDomain(d["domain"]),
            hardware_profile=HardwareProfile(d["hardware_profile"]),
            accuracy_score=d.get("accuracy_score", 0.5),
            reliability_score=d.get("reliability_score", 0.5),
            efficiency_score=d.get("efficiency_score", 0.5),
            helium_per_inference=d.get("helium_per_inference", 0.01),
            carbon_per_inference=d.get("carbon_per_inference", 0.001),
            energy_per_inference=d.get("energy_per_inference", 0.01),
            quantum_capable=d.get("quantum_capable", False),
            quantum_qubits=d.get("quantum_qubits", 0),
            quantum_backend=d.get("quantum_backend"),
            sustainability_score=d.get("sustainability_score", 0.5),
            health=HealthMetrics.from_dict(d.get("health", {})),
            lifecycle_state=ExpertLifecycleState(
                d.get("lifecycle_state", "registered")),
            is_active=d.get("is_active", True),
            replaces_expert=d.get("replaces_expert"),
            lineage=d.get("lineage", []),
        )


class FitnessScore:
    DIMENSIONS = (
        "resource_efficiency", "resilience_score", "adaptation_speed",
        "cooperation_score", "ecoatp_efficiency", "sustainability_score",
        "quantum_efficiency", "quantum_advantage", "helium_savings",
    )

    def __init__(self, expert_id: str, **kwargs: float):
        self.expert_id = expert_id
        for dim in self.DIMENSIONS:
            setattr(self, dim, float(kwargs.get(dim, 0.5)))
        self.reproductive_success = int(kwargs.get("reproductive_success", 0))
        self.overall_fitness = float(kwargs.get("overall_fitness", 0.0))

    def calculate_overall(self, weights: Dict[str, float]) -> float:
        total = 0.0
        for dim in self.DIMENSIONS:
            total += float(weights.get(dim, 0.0)) * getattr(self, dim)
        self.overall_fitness = max(0.0, min(1.0, total))
        return self.overall_fitness

    def to_dict(self) -> Dict[str, Any]:
        d = {"expert_id": self.expert_id, "overall_fitness": self.overall_fitness,
             "reproductive_success": self.reproductive_success}
        for dim in self.DIMENSIONS:
            d[dim] = getattr(self, dim)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FitnessScore":
        return cls(expert_id=d["expert_id"], **{k: v for k, v in d.items()
                                                 if k != "expert_id"})


# =============================================================================
# CONFIG
# =============================================================================
_DEFAULT_FITNESS_WEIGHTS: Dict[str, float] = {
    "resource_efficiency": 0.20,
    "resilience_score": 0.15,
    "adaptation_speed": 0.10,
    "cooperation_score": 0.10,
    "ecoatp_efficiency": 0.10,
    "sustainability_score": 0.15,
    "quantum_efficiency": 0.10,
    "quantum_advantage": 0.05,
    "helium_savings": 0.05,
}


class ExpertRegistryConfig:
    """All configurable knobs, defaulted and merged safely."""

    def __init__(self, **overrides: Any):
        # Base defaults
        self.registry_id = overrides.get("registry_id", "default")
        self.enable_bio_correlation = overrides.get("enable_bio_correlation", True)
        self.enable_natural_selection = overrides.get("enable_natural_selection", True)
        self.enable_fitness_tracking = overrides.get("enable_fitness_tracking", True)
        self.enable_population_tracking = overrides.get("enable_population_tracking", True)
        self.enable_predictive_forecasting = overrides.get("enable_predictive_forecasting", True)
        self.enable_quantum_efficiency = overrides.get("enable_quantum_efficiency", True)
        self.enable_reproductive_strategies = overrides.get("enable_reproductive_strategies", True)
        self.enable_climate_integration = overrides.get("enable_climate_integration", True)
        self.enable_persistence = overrides.get("enable_persistence", True)

        # Networking
        self.sync_retries = overrides.get("sync_retries", 3)
        self.sync_retry_base_delay_ms = overrides.get("sync_retry_base_delay_ms", 100.0)
        self.sync_retry_max_delay_ms = overrides.get("sync_retry_max_delay_ms", 5000.0)
        self.circuit_breaker_threshold = overrides.get("circuit_breaker_threshold", 5)
        self.circuit_breaker_recovery_timeout = overrides.get("circuit_breaker_recovery_timeout", 30.0)
        self.sync_interval = overrides.get("sync_interval", 3600)
        self.bio_sync_interval = overrides.get("bio_sync_interval", 300)
        self.rate_limit_per_minute = overrides.get("rate_limit_per_minute", 60)

        # Fitness
        override_weights = overrides.get("fitness_weights") or {}
        self.fitness_weights = {**_DEFAULT_FITNESS_WEIGHTS, **override_weights}
        self.natural_selection_percentile_low = overrides.get("natural_selection_percentile_low", 20.0)
        self.natural_selection_percentile_high = overrides.get("natural_selection_percentile_high", 80.0)
        self.reproductive_mutation_rate = overrides.get("reproductive_mutation_rate", 0.1)
        self.reproductive_max_offspring = overrides.get("reproductive_max_offspring", 3)
        self.climate_update_interval = overrides.get("climate_update_interval", 3600)

        # v7.0 features
        self.enable_moe = overrides.get("enable_moe", True)
        self.enable_pareto_front = overrides.get("enable_pareto_front", True)
        self.enable_contextual_weights = overrides.get("enable_contextual_weights", True)
        self.enable_federated_learning = overrides.get("enable_federated_learning", True)
        self.enable_active_user_preference = overrides.get("enable_active_user_preference", True)
        self.enable_fitness_drift_detection = overrides.get("enable_fitness_drift_detection", True)
        self.enable_improved_forecasting = overrides.get("enable_improved_forecasting", True)
        self.moe_hidden_layers = overrides.get("moe_hidden_layers", [16, 8])
        self.pareto_max_size = overrides.get("pareto_max_size", 100)
        self.context_weight_learning_rate = overrides.get("context_weight_learning_rate", 0.01)
        self.federated_aggregation_interval = overrides.get("federated_aggregation_interval", 3600)

        # v7.1 / v7.2 features
        self.enable_quantum_distillation = overrides.get("enable_quantum_distillation", True)
        self.enable_causal_mask = overrides.get("enable_causal_mask", True)
        self.enable_expert_auction = overrides.get("enable_expert_auction", True)
        self.enable_safety_monitor = overrides.get("enable_safety_monitor", True)
        self.enable_precision_controller = overrides.get("enable_precision_controller", True)
        self.enable_carbon_market = overrides.get("enable_carbon_market", True)
        self.carbon_market_config = overrides.get("carbon_market_config", {})
        self.enable_chaos = overrides.get("enable_chaos", False)
        self.chaos_probability = overrides.get("chaos_probability", 0.0)
        self.chaos_auto_rollback = overrides.get("chaos_auto_rollback", True)
        self.enable_human_approval = overrides.get("enable_human_approval", False)
        self.human_approval_timeout = overrides.get("human_approval_timeout", 5.0)

        # Determinism
        self.seed = overrides.get("seed", 12345)


# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0, name: str = "default"):
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self.name = name
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state == "open":
                if (self.last_failure_time is not None and
                        (time.time() - self.last_failure_time) > self.recovery_timeout):
                    self.state = "half-open"
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
            raise
        async with self._lock:
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
        return result


class RateLimiter:
    def __init__(self, rate_per_minute: float):
        self.capacity = float(rate_per_minute)
        self.fill_rate = rate_per_minute / 60.0
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.capacity,
                              self.tokens + elapsed * self.fill_rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION (density matrix)
# =============================================================================
class QuantumDistillationModule:
    """Density-matrix distillation of teacher parameter vectors.

    Each registered expert contributes a probability distribution over the
    fitness-weight dimensions (its "policy"). We form the density matrix
    ``rho = sum_i w_i |psi_i><psi_i|`` from amplitudes ``sqrt(p)``, then
    use the diagonal (normalised) as the student target and the von
    Neumann entropy as a diversity signal.
    """

    def __init__(self, n_dims: int, temperature: float = 1.0,
                 learning_rate: float = 0.1,
                 min_temperature: float = 0.25,
                 temperature_decay: float = 0.999,
                 convergence_eps: float = 1e-4,
                 rng: Optional[random.Random] = None):
        self.n_dims = int(n_dims)
        self.temperature = float(temperature)
        self.learning_rate = float(learning_rate)
        self.min_temperature = float(min_temperature)
        self.temperature_decay = float(temperature_decay)
        self.convergence_eps = float(convergence_eps)
        self.rng = rng or random.Random()
        self.teachers: Dict[str, List[float]] = {}
        self.teacher_weights: Dict[str, float] = {}
        self.student: List[float] = [1.0 / max(1, n_dims)] * n_dims
        self.history: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._converged = False

    @property
    def available(self) -> bool:
        return True

    def register_teacher(self, name: str, policy: Iterable[float],
                         weight: float = 1.0) -> None:
        pol = list(policy)
        if not pol:
            return
        s = sum(pol) or 1.0
        self.teachers[name] = [max(0.0, p) / s for p in pol]
        self.teacher_weights[name] = max(1e-6, float(weight))

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)
        self.teacher_weights.pop(name, None)

    def _von_neumann_entropy(self, rho: List[List[float]]) -> float:
        if not NUMPY_AVAILABLE:
            diag = [max(rho[i][i], 1e-12) for i in range(len(rho))]
            return -sum(p * math.log(p) for p in diag)
        try:
            m = np.asarray(rho, dtype=float)
            eig = np.linalg.eigvalsh((m + m.T) / 2.0)
            eig = np.clip(eig, 1e-12, None)
            return float(-np.sum(eig * np.log(eig)))
        except Exception:
            return 0.0

    def _superpose(self) -> Tuple[List[float], float]:
        n = self.n_dims
        if not self.teachers:
            return list(self.student), 0.0
        amps: List[List[float]] = []
        weights: List[float] = []
        for tid, pol in self.teachers.items():
            a = [math.sqrt(max(pol[i], 1e-12)) if i < len(pol) else 0.0
                 for i in range(n)]
            amps.append(a)
            weights.append(self.teacher_weights.get(tid, 1.0))
        wsum = sum(weights) or 1.0
        weights = [w / wsum for w in weights]

        rho = [[0.0] * n for _ in range(n)]
        for a, w in zip(amps, weights):
            for i in range(n):
                for j in range(n):
                    rho[i][j] += w * a[i] * a[j]
        diag = [max(rho[i][i], 1e-12) for i in range(n)]
        s = sum(diag) or 1.0
        policy = [d / s for d in diag]
        return policy, self._von_neumann_entropy(rho)

    async def step(self, storage: Optional[InMemoryStorage] = None,
                   student_id: str = "registry") -> Dict[str, Any]:
        target_raw, entropy = self._superpose()
        target = self._softmax(target_raw, self.temperature)
        new: List[float] = []
        for s, t in zip(self.student, target):
            eps = max(1e-12, min(self.student) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        new_student = [x / ns for x in new]
        delta = sum(abs(a - b) for a, b in zip(new_student, self.student))
        self._converged = delta < self.convergence_eps
        self.student = new_student
        self.temperature = max(self.min_temperature,
                               self.temperature * self.temperature_decay)
        entry = {"target": target, "student": list(self.student),
                 "temperature": self.temperature, "delta": delta,
                 "converged": self._converged, "entropy": entropy}
        self.history.append(entry)
        if storage is not None:
            try:
                await asyncio.to_thread(
                    storage.save_state, "quantum_distillation_history",
                    json.dumps(self.history[-1]))
            except Exception:
                pass
        return entry

    def _softmax(self, x: Iterable[float], temp: float) -> List[float]:
        x = list(x)
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def is_converged(self) -> bool:
        return self._converged

    def get_student_policy(self) -> List[float]:
        return list(self.student)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL FEATURE MASK (PC-style)
# =============================================================================
class CausalFeatureMask:
    """Learns a causal skeleton over context features via partial correlation.

    Features are the 6-element context vector from the MoE encoder. The
    mask is a diagonal matrix whose entries are 1.0 for parents of the
    selected expert (in the discovered DAG) and 0.5 for non-parents.
    """

    FEATURE_NAMES = ("task_type", "carbon_intensity", "workload_size",
                     "latency_target", "hour_of_day", "domain")

    def __init__(self, feature_dim: int = 6,
                 rng: Optional[random.Random] = None):
        self.feature_dim = int(feature_dim)
        self.rng = rng or random.Random()
        self.mask = [1.0] * feature_dim
        self.edges: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.samples: Deque[List[float]] = deque(maxlen=512)

    def observe(self, features: Iterable[float]) -> None:
        feats = list(features)
        if len(feats) != self.feature_dim:
            return
        self.samples.append([float(x) for x in feats])

    def apply(self, features: Iterable[float]) -> List[float]:
        feats = list(features)
        return [f * self.mask[i] for i, f in enumerate(feats)]

    def fit(self, alpha: float = 0.05, threshold: float = 0.25) -> Dict[str, Any]:
        """PC-style partial-correlation skeleton."""
        self.edges.clear()
        if len(self.samples) < 10 or not NUMPY_AVAILABLE:
            return self.summary()
        X = np.asarray(self.samples, dtype=float)
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
            corr = np.nan_to_num(corr, nan=0.0)
            prec = np.linalg.pinv(corr)
        except Exception:
            return self.summary()
        names = list(self.FEATURE_NAMES)[:self.feature_dim]
        for i in range(self.feature_dim):
            for j in range(i + 1, self.feature_dim):
                denom = math.sqrt(max(prec[i, i] * prec[j, j], 1e-12))
                pcorr = -prec[i, j] / denom
                if abs(pcorr) <= threshold:
                    continue
                n = X.shape[0]
                z = 0.5 * math.log((1 + min(max(pcorr, -0.99), 0.99)) /
                                   (1 - min(max(pcorr, -0.99), 0.99))) * \
                    math.sqrt(max(n - (self.feature_dim - 2), 1))
                p_val = math.erfc(abs(z) / math.sqrt(2.0))
                if p_val > alpha:
                    continue
                src, dst = names[i], names[j]
                self.edges[src][dst] = float(pcorr)
        self._update_mask(names)
        return self.summary()

    def _update_mask(self, names: List[str]) -> None:
        # Any feature that has at least one edge gets weight 1.0.
        active = set()
        for src, targets in self.edges.items():
            active.add(src)
            active.update(targets.keys())
        for i, n in enumerate(names):
            self.mask[i] = 1.0 if n in active else 0.5

    def summary(self) -> Dict[str, Any]:
        return {
            "features": self.feature_dim,
            "edges": sum(len(v) for v in self.edges.values()),
            "mask": list(self.mask),
        }


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN AGGREGATOR
# =============================================================================
class FederatedGreenAggregator:
    """Weighted FedAvg with optional DP + top-k sparsification.

    Each client uploads a byte-blob parameter vector. The aggregator
    performs sample-count-weighted averaging, optional Gaussian DP noise,
    and optional sparsification for communication savings.
    """

    def __init__(self, storage: InMemoryStorage, instance_id: str,
                 dp_epsilon: Optional[float] = None,
                 dp_clip: float = 1.0,
                 sparse_topk: float = 0.0,
                 rng: Optional[random.Random] = None):
        self.storage = storage
        self.instance_id = instance_id
        self.dp_epsilon = dp_epsilon
        self.dp_clip = float(dp_clip)
        self.sparse_topk = float(sparse_topk)
        self.rng = rng or random.Random()
        self.rounds = 0

    async def share_weights(self, model_id: str, weights: bytes,
                            sample_count: int = 1) -> None:
        w = self._sparsify(weights)
        w = self._privatise(w)
        key = f"fed_weights::{model_id}::{self.instance_id}::{self.rounds}"
        try:
            await asyncio.to_thread(
                self.storage.save_state, key,
                json.dumps({"weights": list(w), "n": sample_count,
                            "round": self.rounds,
                            "epsilon": self.dp_epsilon}))
        except Exception:
            pass

    async def pull_aggregated(self, model_id: str) -> Optional[bytes]:
        keys = [k for k in self.storage._state
                if k.startswith(f"fed_weights::{model_id}::")]
        blobs: List[Tuple[List[int], int]] = []
        for k in keys:
            try:
                d = json.loads(self.storage._state[k])
                blobs.append((d["weights"], max(int(d.get("n", 1)), 1)))
            except Exception:
                continue
        if not blobs:
            return None
        n = min(len(b[0]) for b in blobs)
        total = sum(w for _, w in blobs) or 1.0
        out = bytearray(n)
        for i in range(n):
            acc = sum(b[i] * w for b, w in blobs) / total
            out[i] = int(acc) & 0xFF
        self.rounds += 1
        return bytes(out)

    def _sparsify(self, blob: bytes) -> bytes:
        if self.sparse_topk <= 0.0 or not blob:
            return blob
        k = max(1, int(len(blob) * self.sparse_topk))
        idx = sorted(range(len(blob)), key=lambda i: -abs(blob[i] - 128))[:k]
        keep = set(idx)
        return bytes(b if i in keep else 128 for i, b in enumerate(blob))

    def _privatise(self, blob: bytes) -> bytes:
        if self.dp_epsilon is None or not blob:
            return blob
        sigma = self.dp_clip / max(self.dp_epsilon, 1e-6)
        out = bytearray(len(blob))
        for i, b in enumerate(blob):
            v = (b - 128) / 128.0
            v = max(-self.dp_clip, min(self.dp_clip, v))
            v += self.rng.gauss(0.0, sigma)
            v = max(-1.0, min(1.0, v))
            out[i] = int(round((v + 1.0) * 127.5))
        return bytes(out)


# =============================================================================
# ENHANCEMENT 4 — EXPERT AUCTION (LinUCB bandit)
# =============================================================================
class ExpertAuction:
    """Contextual bandit over experts using LinUCB.

    Each expert has a ridge-regression estimate of expected reward given a
    context. Selection maximises ``mu + alpha * sigma``.
    """

    def __init__(self, expert_ids: List[str], feature_dim: int = 6,
                 alpha: float = 0.2, ridge: float = 1.0,
                 rng: Optional[random.Random] = None):
        self.expert_ids = list(expert_ids)
        self.feature_dim = int(feature_dim)
        self.alpha = float(alpha)
        self.ridge = float(ridge)
        self.rng = rng or random.Random()
        # A_a = ridge*I ; b_a = 0 per arm.
        self._A: Dict[str, Any] = {eid: self._eye() for eid in self.expert_ids}
        self._b: Dict[str, Any] = {eid: self._zeros() for eid in self.expert_ids}

    def _eye(self):
        if NUMPY_AVAILABLE:
            return np.eye(self.feature_dim)
        return [[1.0 if i == j else 0.0 for j in range(self.feature_dim)]
                for i in range(self.feature_dim)]

    def _zeros(self):
        if NUMPY_AVAILABLE:
            return np.zeros(self.feature_dim)
        return [0.0] * self.feature_dim

    def add_expert(self, expert_id: str) -> None:
        if expert_id in self._A:
            return
        self.expert_ids.append(expert_id)
        self._A[expert_id] = self._eye()
        self._b[expert_id] = self._zeros()

    def _scores(self, features: List[float]) -> Dict[str, float]:
        x = list(features)[:self.feature_dim]
        if NUMPY_AVAILABLE:
            xv = np.asarray(x, dtype=float)
            scores = {}
            for eid in self.expert_ids:
                try:
                    A_inv = np.linalg.inv(self._A[eid])
                except np.linalg.LinAlgError:
                    A_inv = np.linalg.pinv(self._A[eid])
                theta = A_inv @ self._b[eid]
                mu = float(theta @ xv)
                sigma = float(math.sqrt(max(xv @ A_inv @ xv, 0.0)))
                scores[eid] = mu + self.alpha * sigma
            return scores
        # Fallback: random scores.
        return {eid: self.rng.random() for eid in self.expert_ids}

    def compute_bids(self, features: List[float]) -> Dict[str, float]:
        return self._scores(features)

    def select_experts(self, features: List[float], top_k: int = 1) -> List[str]:
        scores = self._scores(features)
        return [eid for eid, _ in sorted(scores.items(),
                                         key=lambda kv: kv[1],
                                         reverse=True)[:top_k]]

    def update(self, expert_id: str, features: List[float],
               reward: float) -> None:
        if expert_id not in self._A:
            self.add_expert(expert_id)
        x = list(features)[:self.feature_dim]
        if NUMPY_AVAILABLE:
            xv = np.asarray(x, dtype=float)
            self._A[expert_id] = self._A[expert_id] + np.outer(xv, xv)
            self._b[expert_id] = self._b[expert_id] + reward * xv
        else:
            A = self._A[expert_id]
            for i in range(self.feature_dim):
                for j in range(self.feature_dim):
                    A[i][j] += x[i] * x[j]
            b = self._b[expert_id]
            for i in range(self.feature_dim):
                b[i] += reward * x[i]


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL SAFETY MONITOR (MTL)
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


@dataclass
class _SafetyRule:
    rule_id: str
    operator: str  # always | eventually | never | historically | once
    predicate: Callable[[Dict[str, Any]], bool]
    window_seconds: float = 0.0
    description: str = ""
    severity: str = "warning"
    violations: int = 0
    last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict[str, Any]]]) -> bool:
        """True if rule is VIOLATED over the trace."""
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
    """MTL safety monitor with past-time operators H and O.

    Formula syntax (one per line):
        G (x >= 0.5)          -- globally
        F (done == 1)         -- eventually
        NEVER (carbon > 0.8)  -- never
        H (quality >= 0.5)    -- historically
        O (task_started == 1) -- once
        [60] G (x >= 0.5)     -- windowed (60s)
    """

    def __init__(self, max_trace: int = 500):
        self.rules: Dict[str, _SafetyRule] = {}
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(maxlen=max_trace)
        self.approval_cb: Optional[Callable] = None

    def add_formula(self, formula: str) -> None:
        f = formula.strip()
        window = 0.0
        if f.startswith("[") and "]" in f:
            head, rest = f[1:].split("]", 1)
            window = float(head.strip().rstrip("s"))
            f = rest.strip()
        op_map = {
            "G ": "always",
            "F ": "eventually",
            "NEVER ": "never",
            "H ": "historically",
            "O ": "once",
        }
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
        self.rules[rid] = _SafetyRule(rid, op, predicate,
                                      window_seconds=window,
                                      description=expr)

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def check(self) -> Dict[str, bool]:
        """Returns a dict rule_id -> True if satisfied, False if violated."""
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        a = self.approval_cb(rid, self.trace[-1][1]
                                             if self.trace else {})
                        if asyncio.iscoroutine(a):
                            await a
                    except Exception:
                        pass
        return result


# =============================================================================
# ENHANCEMENT 6 — XAI EXPLAINER (Integrated Gradients)
# =============================================================================
class XAIExplainer:
    """Integrated Gradients + actionable counterfactuals.

    Works on any callable ``f(x) -> float`` where ``x`` is a numpy array.
    Deterministic given a seed.
    """

    def __init__(self, ig_steps: int = 32,
                 cf_max_iter: int = 40, cf_target_delta: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.ig_steps = int(ig_steps)
        self.cf_max_iter = int(cf_max_iter)
        self.cf_target_delta = float(cf_target_delta)
        self.rng = rng or random.Random()

    def integrated_gradients(self, f: Callable, x: Any,
                             baseline: Optional[Any] = None) -> Any:
        if not NUMPY_AVAILABLE:
            return None
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
                    grad[i] = (float(f(up.reshape(1, -1))) -
                               float(f(dn.reshape(1, -1)))) / (2 * eps)
                except Exception:
                    grad[i] = 0.0
            total += grad
        avg = total / max(self.ig_steps, 1)
        return (x - baseline) * avg

    def counterfactual(self, f: Callable, x: Any,
                       target_delta: Optional[float] = None,
                       bounds: Tuple[float, float] = (-1.0, 1.0)) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            return {"success": False}
        x = np.asarray(x, dtype=float)
        target_delta = target_delta or self.cf_target_delta
        try:
            base_pred = float(f(x.reshape(1, -1)))
        except Exception:
            return {"success": False}
        x_cf = x.copy()
        best_delta, best_pred = 0.0, base_pred
        best_idx = None
        best_step = 0.0
        for _ in range(self.cf_max_iter):
            i = self.rng.randrange(len(x))
            step = self.rng.choice([-1, 1]) * 0.1
            trial = x_cf.copy()
            trial[i] = float(np.clip(trial[i] + step, *bounds))
            try:
                pred = float(f(trial.reshape(1, -1)))
            except Exception:
                continue
            delta = abs(pred - base_pred)
            if delta > best_delta:
                best_delta, best_pred, x_cf = delta, pred, trial
                best_idx, best_step = i, step
            if delta >= abs(target_delta):
                break
        return {
            "success": best_delta >= abs(target_delta),
            "changed_index": best_idx,
            "change_amount": best_step,
            "delta": best_pred - base_pred,
        }

    async def explain(self, f: Callable, x: Any,
                      names: Optional[List[str]] = None,
                      include_counterfactual: bool = True) -> Dict[str, Any]:
        attrs = self.integrated_gradients(f, x)
        result: Dict[str, Any] = {"attributions": None}
        if attrs is not None:
            if names:
                result["attributions"] = dict(zip(names, attrs.tolist()))
            else:
                result["attributions"] = attrs.tolist()
        if include_counterfactual:
            result["counterfactual"] = self.counterfactual(f, x)
        return result


# =============================================================================
# ENHANCEMENT 7 — PRECISION CONTROLLER
# =============================================================================
class PrecisionController:
    """Layer-wise precision selection with energy accounting."""

    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}

    def __init__(self, policy: str = "energy_aware",
                 levels: Optional[List[str]] = None,
                 max_sensitivity: float = 0.5,
                 switch_threshold: float = 0.02,
                 hysteresis_s: float = 0.5):
        self.policy = policy
        self.levels = levels or ["fp32", "tf32", "bf16", "fp16", "int8"]
        self.max_sensitivity = float(max_sensitivity)
        self.switch_threshold = float(switch_threshold)
        self.hysteresis_s = float(hysteresis_s)
        self.current = "fp32"
        self.saved_wh = 0.0
        self._last_switch = 0.0

    def select_precision(self, layer_sensitivity: Optional[float] = None) -> str:
        if layer_sensitivity is not None and \
                layer_sensitivity > self.max_sensitivity:
            return "fp32"
        return min(self.levels, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        now = time.time()
        if now - self._last_switch < self.hysteresis_s:
            return False
        old = self.current
        self.current = target
        self._last_switch = now
        self.saved_wh += max(0.0, self.ENERGY[old] - self.ENERGY[target])
        return True

    async def auto_switch(self, recent_acc: float, baseline_acc: float,
                          layer_sensitivity: Optional[float] = None) -> None:
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        if drop > self.switch_threshold:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < self.switch_threshold:
            await self.switch_to(self.select_precision(layer_sensitivity),
                                 reason="headroom")

    async def auto_switch_layers(self, per_layer_acc: Dict[str, float],
                                 baseline_acc: float) -> Dict[str, str]:
        applied: Dict[str, str] = {}
        for layer, acc in per_layer_acc.items():
            drop = (baseline_acc - acc) / max(baseline_acc, 1e-6)
            sens = min(1.0, drop / max(self.switch_threshold, 1e-6))
            applied[layer] = self.select_precision(sens)
        return applied


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    """Carbon market with budget tracking, EMA price, and REC retirement."""

    def __init__(self, storage: InMemoryStorage,
                 budget_kg: float = 100.0,
                 price_oracle: Optional[Callable] = None,
                 rng: Optional[random.Random] = None,
                 ema_alpha: float = 0.3):
        self.storage = storage
        self.budget_kg = float(budget_kg)
        self.used_kg = 0.0
        self.price = 25.0
        self.price_ema = 25.0
        self.ema_alpha = float(ema_alpha)
        self.rng = rng or random.Random()
        self.price_oracle = price_oracle or self._random_walk
        self.rec_ledger: List[Dict[str, Any]] = []

    async def _random_walk(self) -> float:
        return max(5.0, self.price + self.rng.gauss(0, 1.5))

    @property
    def available(self) -> bool:
        return True

    async def update_price(self) -> float:
        try:
            p = float(await self.price_oracle())
        except Exception:
            p = self.price
        self.price = max(0.0, p)
        self.price_ema = (self.ema_alpha * self.price +
                          (1 - self.ema_alpha) * self.price_ema)
        return self.price

    def _budget_ok(self, extra_kg: float) -> bool:
        return (self.used_kg + extra_kg) <= self.budget_kg

    async def net_zero_schedule(self, workload_kwh: float,
                                intensity: float) -> Dict[str, Any]:
        await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * self.price
        if not self._budget_ok(carbon_kg):
            action = "defer"
        elif intensity > 0.3:
            action = "defer"
        elif offset_cost < 0.5:
            action = "run_offset"
        else:
            action = "run"
        if action in ("run", "run_offset"):
            self.used_kg += carbon_kg
        return {
            "action": action,
            "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost,
            "credit_price_usd": self.price,
            "budget_remaining_kg": max(0.0, self.budget_kg - self.used_kg),
        }

    async def buy_credits(self, amount_mwh: float,
                          price_per_mwh: float = 5.0,
                          source: str = "wind") -> Dict[str, Any]:
        cost = amount_mwh * price_per_mwh
        cert = uuid.uuid4().hex[:12]
        self.rec_ledger.append({"mwh": amount_mwh, "cost": cost,
                                "source": source, "certificate": cert,
                                "retired": False})
        return {"cost": cost, "certificate": cert}

    async def sell_credits(self, amount_mwh: float,
                           price_per_mwh: float = 5.0) -> Dict[str, Any]:
        revenue = amount_mwh * price_per_mwh
        return {"revenue": revenue, "mwh": amount_mwh}

    async def retire_rec(self, mwh: float) -> float:
        remaining = mwh
        for r in self.rec_ledger:
            if r["retired"]:
                continue
            take = min(r["mwh"], remaining)
            r["mwh"] -= take
            remaining -= take
            if r["mwh"] <= 0:
                r["retired"] = True
            if remaining <= 0:
                break
        return mwh - remaining

    def rec_balance(self) -> float:
        return sum(r["mwh"] for r in self.rec_ledger if not r["retired"])


# =============================================================================
# ENHANCEMENT 9 — CHAOS ENGINE
# =============================================================================
class ChaosEngine:
    """Fault injection with steady-state hypothesis + auto-rollback.

    Faults available: latency, exception, data_corruption,
    memory_pressure, network_drop.
    """

    FAULT_TYPES = ("latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop")

    def __init__(self, registry: "ExpertRegistry",
                 probability: float = 0.0,
                 steady_predicate: Optional[Callable] = None,
                 auto_rollback: bool = True,
                 blast_radius: float = 0.5,
                 rng: Optional[random.Random] = None):
        self.registry = registry
        self.probability = float(probability)
        self.steady_predicate = steady_predicate or self._default_steady
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
        self.active: Dict[str, Dict[str, Any]] = {}
        self.rollback_count = 0
        self._lock = asyncio.Lock()

    async def _default_steady(self) -> bool:
        return True

    def _allowed_concurrent(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def maybe_inject_failure(self) -> Optional[Dict[str, Any]]:
        if self.rng.random() >= self.probability:
            return None
        fault = self.rng.choice(self.FAULT_TYPES)
        return await self.run_experiment(f"auto_{uuid.uuid4().hex[:6]}", fault)

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        async with self._lock:
            if len(self.active) >= self._allowed_concurrent():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}
        t0 = time.time()
        try:
            before = bool(await self.steady_predicate())
        except Exception:
            before = True
        status = "completed"
        snapshot = None
        # Snapshot for auto-rollback
        if self.auto_rollback and hasattr(self.registry, "_snapshot_fitness"):
            snapshot = self.registry._snapshot_fitness()
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                await asyncio.sleep(0.005)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.005)
            elif fault_type == "data_corruption":
                if self.registry.fitness_scores:
                    eid = self.rng.choice(list(self.registry.fitness_scores))
                    fs = self.registry.fitness_scores[eid]
                    # Bounded corruption — cannot violate safety invariants.
                    fs.overall_fitness = max(
                        0.0, min(1.0, fs.overall_fitness *
                                 self.rng.uniform(0.5, 1.5)))
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        try:
            after = bool(await self.steady_predicate())
        except Exception:
            after = True
        rolled_back = False
        if before and not after and self.auto_rollback and snapshot is not None:
            rolled_back = True
            self.rollback_count += 1
            if hasattr(self.registry, "_restore_fitness"):
                self.registry._restore_fitness(snapshot)
        return {
            "name": name, "fault_type": fault_type,
            "steady_before": before, "steady_after": after,
            "status": status,
            "duration_ms": (time.time() - t0) * 1000.0,
            "rolled_back": rolled_back,
        }


# =============================================================================
# ENHANCEMENT 10 — HUMAN APPROVAL (uncertainty sampling)
# =============================================================================
class HumanApprovalHandler:
    """Uncertainty-sampled HITL.

    Asks a human only when the top-2 candidate utilities are close. Also
    learns per-user preference weights from feedback (Dirichlet-style).
    """

    def __init__(self, queue: Optional[AsyncMessageQueue] = None,
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
        scored = sorted(((self._utility(user_id, c), c) for c in candidates),
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
                            metrics: Optional[Dict[str, float]] = None) -> None:
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


# =============================================================================
# MoE SOFT GATING
# =============================================================================
class MoEGatingNetwork:
    """Numpy softmax gate with optional load-balancing loss.

    When ``numpy`` is available, a linear map W (n_experts, n_features) is
    maintained and updated online. Without numpy, falls back to uniform.
    """

    N_FEATURES = 6

    def __init__(self, registry: "ExpertRegistry", config: ExpertRegistryConfig,
                 rng: Optional[random.Random] = None):
        self.registry = registry
        self.config = config
        self.rng = rng or random.Random()
        self._trained = False
        self._training_samples = 0
        self._lr = 0.05
        if NUMPY_AVAILABLE:
            self._W = np.zeros((0, self.N_FEATURES))
        else:
            self._W = None
        self._expert_index: Dict[str, int] = {}

    def _encode_context(self, context: Dict[str, Any]) -> List[float]:
        return [
            float(context.get("task_type_encoded", 0.0)),
            float(context.get("carbon_intensity", 400)) / 1000.0,
            float(context.get("workload_size", 0.5)),
            float(context.get("latency_target_ms", 100)) / 1000.0,
            datetime.now(timezone.utc).hour / 24.0,
            float(context.get("domain_encoded", 0.0)),
        ]

    def _ensure_expert(self, expert_id: str) -> int:
        if expert_id not in self._expert_index:
            idx = len(self._expert_index)
            self._expert_index[expert_id] = idx
            if NUMPY_AVAILABLE:
                self._W = np.vstack([self._W, np.zeros((1, self.N_FEATURES))])
        return self._expert_index[expert_id]

    def _softmax(self, logits: Any) -> Any:
        m = float(np.max(logits))
        e = np.exp(logits - m)
        return e / max(float(np.sum(e)), 1e-12)

    async def predict_proba(self, context: Dict[str, Any]) -> Dict[str, float]:
        if not NUMPY_AVAILABLE or self._W is None or self._W.shape[0] == 0:
            return {}
        feats = np.asarray(self._encode_context(context), dtype=float)
        logits = self._W @ feats
        probs = self._softmax(logits)
        return {eid: float(probs[idx])
                for eid, idx in self._expert_index.items()}

    async def select_expert(self, context: Dict[str, Any]) -> Optional[str]:
        probs = await self.predict_proba(context)
        if not probs:
            return None
        available = {eid: p for eid, p in probs.items()
                     if eid in self.registry._experts and
                     self.registry._experts[eid].lifecycle_state.is_available()}
        if not available:
            return None
        return max(available, key=available.get)

    async def add_training_sample(self, context: Dict[str, Any],
                                  selected_expert: str, reward: float) -> None:
        if not NUMPY_AVAILABLE:
            self._training_samples += 1
            return
        idx = self._ensure_expert(selected_expert)
        feats = np.asarray(self._encode_context(context), dtype=float)
        probs = self._softmax(self._W @ feats)
        # Cross-entropy gradient: -one_hot + probs (for the selected expert)
        grad = probs.copy()
        grad[idx] -= 1.0
        self._W -= self._lr * reward * np.outer(grad, feats)
        # Load-balancing regularisation: penalise very peaked probs.
        entropy = -float(np.sum(probs * np.log(probs + 1e-12)))
        if entropy < 0.1 and self._W.shape[0] > 1:
            # Nudge toward uniform.
            self._W *= 0.99
        self._training_samples += 1
        self._trained = self._training_samples >= 5

    def get_stats(self) -> Dict[str, Any]:
        return {"trained": self._trained,
                "samples": self._training_samples,
                "num_experts": len(self._expert_index)}


# =============================================================================
# PARETO FRONT (NSGA-II crowding)
# =============================================================================
class ParetoFrontOptimizer:
    """Non-dominated front with crowding-distance diversity preservation."""

    def __init__(self, registry: "ExpertRegistry",
                 config: ExpertRegistryConfig, max_size: int = 100):
        self.registry = registry
        self.config = config
        self.max_size = int(max_size)
        self.front: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    def _dominates(self, a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        # Objectives to MINIMISE: carbon, helium, energy, latency.
        # Objective to MAXIMISE: accuracy (negate).
        a_metrics = (-a["accuracy"], a["carbon"], a["helium"],
                     a["energy"], a["latency"])
        b_metrics = (-b["accuracy"], b["carbon"], b["helium"],
                     b["energy"], b["latency"])
        return (all(am <= bm for am, bm in zip(a_metrics, b_metrics)) and
                any(am < bm for am, bm in zip(a_metrics, b_metrics)))

    def _crowding_distance(self, front: List[Dict[str, Any]]) -> List[float]:
        n = len(front)
        if n <= 2:
            return [float("inf")] * n
        distances = [0.0] * n
        objectives = ("accuracy", "carbon", "helium", "energy", "latency")
        for obj in objectives:
            order = sorted(range(n), key=lambda i: front[i][obj])
            distances[order[0]] = float("inf")
            distances[order[-1]] = float("inf")
            lo = front[order[0]][obj]
            hi = front[order[-1]][obj]
            if hi - lo < 1e-12:
                continue
            for k in range(1, n - 1):
                i = order[k]
                prev_v = front[order[k - 1]][obj]
                next_v = front[order[k + 1]][obj]
                distances[i] += (next_v - prev_v) / (hi - lo)
        return distances

    async def add_expert(self, expert: ExpertProfile) -> bool:
        entry = {
            "expert_id": expert.expert_id,
            "accuracy": expert.accuracy_score,
            "carbon": expert.carbon_per_inference,
            "helium": expert.helium_per_inference,
            "energy": expert.energy_per_inference,
            "latency": expert.health.avg_latency_ms,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        async with self._lock:
            if any(self._dominates(e, entry) for e in self.front):
                return False
            self.front = [e for e in self.front if not self._dominates(entry, e)]
            self.front.append(entry)
            if len(self.front) > self.max_size:
                distances = self._crowding_distance(self.front)
                keep = sorted(range(len(self.front)),
                              key=lambda i: -distances[i])[:self.max_size]
                self.front = [self.front[i] for i in keep]
            return True

    def get_front(self) -> List[Dict[str, Any]]:
        return list(self.front)


# =============================================================================
# CONTEXTUAL WEIGHT ADJUSTER
# =============================================================================
class ContextualWeightAdjuster:
    """Online gradient descent on fitness weights with regularisation."""

    def __init__(self, config: ExpertRegistryConfig, lr: float = 0.01,
                 reg: float = 0.001):
        self.config = config
        self.lr = float(lr)
        self.reg = float(reg)
        self.history: Deque[Dict[str, float]] = deque(maxlen=200)

    def step(self, rewards: Dict[str, float]) -> Dict[str, float]:
        """Update weights: reward[r] is the marginal benefit of dimension r."""
        weights = dict(self.config.fitness_weights)
        for dim, r in rewards.items():
            if dim in weights:
                weights[dim] += self.lr * (r - self.reg * weights[dim])
        s = sum(weights.values()) or 1.0
        weights = {k: v / s for k, v in weights.items()}
        self.config.fitness_weights = weights
        self.history.append(weights)
        return weights


# =============================================================================
# PREDICTIVE FORECASTER (Holt exponential smoothing)
# =============================================================================
class ImprovedPredictiveForecaster:
    """Simple Holt's linear exponential smoothing per expert."""

    def __init__(self, alpha: float = 0.3, beta: float = 0.1):
        self.alpha = float(alpha)
        self.beta = float(beta)
        self._level: Dict[str, float] = {}
        self._trend: Dict[str, float] = {}

    def observe(self, key: str, value: float) -> None:
        if key not in self._level:
            self._level[key] = value
            self._trend[key] = 0.0
            return
        prev_level = self._level[key]
        prev_trend = self._trend[key]
        level = self.alpha * value + (1 - self.alpha) * (prev_level + prev_trend)
        trend = self.beta * (level - prev_level) + (1 - self.beta) * prev_trend
        self._level[key] = level
        self._trend[key] = trend

    def forecast(self, key: str, steps: int = 1) -> float:
        if key not in self._level:
            return 0.0
        return self._level[key] + steps * self._trend[key]


# =============================================================================
# EXPERT REGISTRY (v7.2.0)
# =============================================================================
class ExpertRegistry:
    """Unified facade wiring all modules together.

    Initialisation is explicit::

        registry = ExpertRegistry(storage, queue, adaptive_cost, pareto,
                                  drift, metrics, config=ExpertRegistryConfig())
        await registry.initialize()
        ...
        await registry.shutdown()
    """

    def __init__(self, storage: InMemoryStorage,
                 message_queue: AsyncMessageQueue,
                 adaptive_cost: AdaptiveCostFunction,
                 pareto: ParetoGating,
                 drift: DriftDetector,
                 metrics: MetricsRegistry,
                 config: Optional[ExpertRegistryConfig] = None):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto
        self.drift = drift
        self.metrics = metrics
        self.config = config or ExpertRegistryConfig()
        self.registry_id = self.config.registry_id

        # Deterministic RNG tree
        seed = self.config.seed
        self._rng = random.Random(seed)
        self._rng_quantum = random.Random(seed + 1)
        self._rng_causal = random.Random(seed + 2)
        self._rng_fed = random.Random(seed + 3)
        self._rng_auction = random.Random(seed + 4)
        self._rng_xai = random.Random(seed + 5)
        self._rng_carbon = random.Random(seed + 6)
        self._rng_chaos = random.Random(seed + 7)
        self._rng_moe = random.Random(seed + 8)

        if NUMPY_AVAILABLE:
            np.random.seed(seed)

        # State
        self._experts: Dict[str, ExpertProfile] = {}
        self._domain_index: Dict[str, Set[str]] = defaultdict(set)
        self._hardware_index: Dict[str, Set[str]] = defaultdict(set)
        self._lifecycle_index: Dict[str, Set[str]] = defaultdict(set)
        self.fitness_scores: Dict[str, FitnessScore] = {}
        self._migration_paths: Dict[str, str] = {}
        self._stats = {
            "total_registrations": 0,
            "total_deregistrations": 0,
            "total_route_calls": 0,
            "total_quantum_steps": 0,
        }

        # Sub-managers (created in initialize())
        self.quantum: Optional[QuantumDistillationModule] = None
        self.causal_mask: Optional[CausalFeatureMask] = None
        self.federated: Optional[FederatedGreenAggregator] = None
        self.auction: Optional[ExpertAuction] = None
        self.safety: Optional[TemporalSafetyMonitor] = None
        self.xai: Optional[XAIExplainer] = None
        self.precision: Optional[PrecisionController] = None
        self.carbon: Optional[CarbonMarketClient] = None
        self.chaos: Optional[ChaosEngine] = None
        self.hitl: Optional[HumanApprovalHandler] = None
        self.moe: Optional[MoEGatingNetwork] = None
        self.pareto_front: Optional[ParetoFrontOptimizer] = None
        self.context_weights: Optional[ContextualWeightAdjuster] = None
        self.forecaster: Optional[ImprovedPredictiveForecaster] = None
        self.fitness_drift: Optional[DriftDetector] = None

        # Lifecycle
        self._ready = False
        self._init_exception: Optional[BaseException] = None
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()

        # Locks & rate limiter
        self._lock = asyncio.Lock()
        self._rate_limiter: Optional[RateLimiter] = None
        self._circuit_breaker = CircuitBreaker(
            self.config.circuit_breaker_threshold,
            self.config.circuit_breaker_recovery_timeout,
            name="registry")

    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._ready:
            return
        try:
            self._rate_limiter = RateLimiter(self.config.rate_limit_per_minute)

            # Legacy sub-managers (local implementations)
            if self.config.enable_moe:
                self.moe = MoEGatingNetwork(self, self.config,
                                            rng=self._rng_moe)
            if self.config.enable_pareto_front:
                self.pareto_front = ParetoFrontOptimizer(
                    self, self.config, max_size=self.config.pareto_max_size)
            if self.config.enable_contextual_weights:
                self.context_weights = ContextualWeightAdjuster(
                    self.config, lr=self.config.context_weight_learning_rate)
            if self.config.enable_federated_learning:
                self.federated = FederatedGreenAggregator(
                    self.storage, self.registry_id, rng=self._rng_fed)
            if self.config.enable_improved_forecasting:
                self.forecaster = ImprovedPredictiveForecaster()
            if self.config.enable_fitness_drift_detection:
                self.fitness_drift = DriftDetector(
                    self.storage, self.adaptive_cost)

            # v7.1 / v7.2 enhancement modules
            self.quantum = QuantumDistillationModule(
                n_dims=len(FitnessScore.DIMENSIONS),
                rng=self._rng_quantum) if self.config.enable_quantum_distillation \
                else None
            self.causal_mask = CausalFeatureMask(
                feature_dim=MoEGatingNetwork.N_FEATURES,
                rng=self._rng_causal) if self.config.enable_causal_mask else None
            self.auction = ExpertAuction(
                [], feature_dim=MoEGatingNetwork.N_FEATURES,
                rng=self._rng_auction) if self.config.enable_expert_auction \
                else None
            self.safety = TemporalSafetyMonitor() \
                if self.config.enable_safety_monitor else None
            if self.safety is not None:
                self._setup_default_safety_formulas()
            self.xai = XAIExplainer(rng=self._rng_xai) \
                if True else None
            self.precision = PrecisionController() \
                if self.config.enable_precision_controller else None
            self.carbon = CarbonMarketClient(
                self.storage, rng=self._rng_carbon) \
                if self.config.enable_carbon_market else None
            self.chaos = ChaosEngine(
                self, probability=self.config.chaos_probability,
                auto_rollback=self.config.chaos_auto_rollback,
                rng=self._rng_chaos) if self.config.enable_chaos else None
            self.hitl = HumanApprovalHandler(
                self.queue, timeout=self.config.human_approval_timeout) \
                if self.config.enable_human_approval else None

            if self.safety is not None:
                self.safety.set_approval_callback(self._on_safety_violation)

            # Restore persisted state
            if self.config.enable_persistence:
                await self._load_state_from_storage()

            # Background loops
            if self.chaos is not None:
                self._spawn(self._chaos_loop())
            if self.quantum is not None:
                self._spawn(self._quantum_loop())

            self._ready = True
            logger.info("Expert Registry v7.2.0 initialisation complete.")
        except BaseException as e:
            self._init_exception = e
            self._ready = False
            logger.error(f"Initialisation failed: {e}", exc_info=True)
            raise

    def _spawn(self, coro: Any) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _setup_default_safety_formulas(self) -> None:
        assert self.safety is not None
        self.safety.add_formula("G (expert_count >= 0)")
        self.safety.add_formula("G (fitness_score >= 0.0)")
        self.safety.add_formula("G (fitness_score <= 1.0)")

    async def _on_safety_violation(self, rule_id: str,
                                   state: Dict[str, Any]) -> bool:
        if self.hitl is None:
            return True
        decision = {
            "action": f"safety_violation:{rule_id}",
            "candidates": [
                {"id": "approve", "risk": 1.0},
                {"id": "deny", "risk": 0.0},
            ],
            "state": state,
        }
        return await self.hitl.request_approval(decision)

    # ------------------------------------------------------------------
    async def _ensure_ready(self) -> None:
        if not self._ready:
            if self._init_exception is not None:
                raise self._init_exception
            raise RuntimeError(
                "ExpertRegistry is not initialised. "
                "Call `await registry.initialize()` first.")

    async def wait_until_ready(self, timeout: Optional[float] = None) -> bool:
        if not self._ready and self._init_exception is None:
            raise RuntimeError("ExpertRegistry.initialize() was never awaited")
        if self._init_exception is not None:
            raise self._init_exception
        return True

    # ------------------------------------------------------------------
    def _register_indexes(self, profile: ExpertProfile) -> None:
        self._domain_index[profile.domain.value].add(profile.expert_id)
        self._hardware_index[profile.hardware_profile.value].add(
            profile.expert_id)
        self._lifecycle_index[profile.lifecycle_state.value].add(
            profile.expert_id)

    def _unregister_indexes(self, profile: ExpertProfile) -> None:
        self._domain_index[profile.domain.value].discard(profile.expert_id)
        self._hardware_index[profile.hardware_profile.value].discard(
            profile.expert_id)
        self._lifecycle_index[profile.lifecycle_state.value].discard(
            profile.expert_id)

    def _compute_fitness(self, profile: ExpertProfile) -> FitnessScore:
        fs = FitnessScore(
            expert_id=profile.expert_id,
            resource_efficiency=min(
                1.0, 1.0 / (1.0 + profile.carbon_per_inference * 10000)),
            resilience_score=profile.reliability_score,
            adaptation_speed=0.5,
            cooperation_score=0.5,
            ecoatp_efficiency=profile.efficiency_score,
            sustainability_score=profile.sustainability_score,
            quantum_efficiency=profile.health.quantum_efficiency,
            quantum_advantage=profile.health.quantum_advantage_score,
            helium_savings=max(0.0, 1.0 - profile.helium_per_inference),
        )
        fs.calculate_overall(self.config.fitness_weights)
        return fs

    # ------------------------------------------------------------------
    async def register_expert(self, profile: ExpertProfile,
                              validate: bool = True,
                              auto_certify: bool = False) -> Tuple[bool, str]:
        await self._ensure_ready()
        assert self._rate_limiter is not None
        if not await self._rate_limiter.acquire():
            return False, "rate limit exceeded"

        async with self._lock:
            existing = self._experts.get(profile.expert_id)
            if existing is not None:
                if profile.version.is_newer_than(existing.version):
                    existing.lifecycle_state = ExpertLifecycleState.ARCHIVED
                    existing.is_active = False
                    profile.replaces_expert = existing.expert_id
                    self._migration_paths[existing.expert_id] = profile.expert_id
                elif profile.version.is_same_as(existing.version):
                    return False, "expert already registered at same version"
                else:
                    return False, "expert already registered with newer version"

            if auto_certify:
                profile.lifecycle_state = ExpertLifecycleState.CERTIFIED
            elif validate:
                profile.lifecycle_state = ExpertLifecycleState.VALIDATING
            else:
                profile.lifecycle_state = ExpertLifecycleState.REGISTERED

            self._experts[profile.expert_id] = profile
            self._register_indexes(profile)

            if self.config.enable_fitness_tracking:
                self.fitness_scores[profile.expert_id] = \
                    self._compute_fitness(profile)

            if self.auction is not None:
                self.auction.add_expert(profile.expert_id)

            self._stats["total_registrations"] += 1

            # Pareto front contribution
            if self.pareto_front is not None:
                await self.pareto_front.add_expert(profile)

            # Safety check
            await self._check_safety()

            # FeedbackEvent
            fitness_val = (self.fitness_scores.get(profile.expert_id)
                           .overall_fitness
                           if profile.expert_id in self.fitness_scores
                           else 0.5)
            try:
                event = FeedbackEvent.create_with_context(
                    task_id=f"reg_{profile.expert_id}",
                    selected_action="register",
                    quality_score=fitness_val,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="registry",
                    adaptive_cost_value=0.0,
                    state={"expert_id": profile.expert_id},
                    candidates=[
                        {"action": "register"},
                        {"action": "deprecate"},
                        {"action": "activate"},
                    ],
                    source="expert_registry",
                    environment="test",
                    tags=["registry", "expert"],
                )
                await self.queue.publish("feedback_events", event.to_json())
            except Exception as e:
                logger.debug(f"feedback publish failed: {e}")

            if self.drift is not None:
                try:
                    await self.drift.check_drift(
                        self.adaptive_cost.get_current_weights())
                except Exception:
                    pass

            return True, f"Expert {profile.expert_id} registered successfully"

    async def deregister_expert(self, expert_id: str) -> Tuple[bool, str]:
        await self._ensure_ready()
        async with self._lock:
            profile = self._experts.pop(expert_id, None)
            if profile is None:
                return False, "expert not found"
            self._unregister_indexes(profile)
            self.fitness_scores.pop(expert_id, None)
            if self.auction is not None:
                self.auction.expert_ids = [e for e in self.auction.expert_ids
                                           if e != expert_id]
            self._stats["total_deregistrations"] += 1
            return True, f"Expert {expert_id} deregistered"

    # ------------------------------------------------------------------
    async def _check_safety(self) -> Dict[str, bool]:
        if self.safety is None:
            return {}
        # Push current registry state.
        fitness_avg = (sum(f.overall_fitness for f in self.fitness_scores.values())
                       / max(1, len(self.fitness_scores)))
        await self.safety.push({
            "expert_count": len(self._experts),
            "fitness_score": fitness_avg,
        })
        return await self.safety.check()

    # ------------------------------------------------------------------
    async def _context_features(self, context: Dict[str, Any]) -> List[float]:
        # Base features, optionally masked by the causal feature mask.
        if self.moe is not None:
            feats = self.moe._encode_context(context)
        else:
            feats = [0.0] * MoEGatingNetwork.N_FEATURES
        if self.causal_mask is not None:
            self.causal_mask.observe(feats)
            feats = self.causal_mask.apply(feats)
        return feats

    async def route(self, context: Dict[str, Any]) -> Optional[str]:
        """Route a context to the best available expert."""
        await self._ensure_ready()
        self._stats["total_route_calls"] += 1

        # 1. MoE gate
        chosen: Optional[str] = None
        if self.moe is not None:
            chosen = await self.moe.select_expert(context)

        # 2. Expert auction override when enabled
        if self.auction is not None:
            feats = await self._context_features(context)
            bid_winner = self.auction.select_experts(feats, top_k=1)
            if bid_winner:
                chosen = bid_winner[0]

        # 3. Fallback: highest fitness available expert
        if chosen is None or chosen not in self._experts:
            available = [eid for eid, e in self._experts.items()
                         if e.lifecycle_state.is_available()]
            if not available:
                return None
            chosen = max(available,
                         key=lambda eid: self.fitness_scores
                         .get(eid, FitnessScore(eid)).overall_fitness)

        # 4. Human approval gate
        if self.hitl is not None:
            decision = {
                "action": "route",
                "candidates": [
                    {"id": chosen, "quality_score": self.fitness_scores
                     .get(chosen, FitnessScore(chosen)).overall_fitness},
                    {"id": "defer", "quality_score": 0.0},
                ],
                "context": context,
            }
            approved = await self.hitl.request_approval(decision)
            if not approved:
                return None

        return chosen

    async def record_reward(self, expert_id: str, context: Dict[str, Any],
                            reward: float) -> None:
        await self._ensure_ready()
        feats = await self._context_features(context)
        if self.moe is not None:
            await self.moe.add_training_sample(context, expert_id, reward)
        if self.auction is not None:
            self.auction.update(expert_id, feats, reward)
        if self.forecaster is not None:
            self.forecaster.observe(expert_id, reward)
        if self.context_weights is not None:
            self.context_weights.step({dim: reward
                                       for dim in FitnessScore.DIMENSIONS})
        if expert_id in self.fitness_scores:
            fs = self.fitness_scores[expert_id]
            fs.reproductive_success += 1

    async def policy_probs(self, context: Dict[str, Any]) -> List[float]:
        """Return a probability distribution over experts."""
        await self._ensure_ready()
        experts = list(self._experts.values())
        if not experts:
            return []
        if self.moe is not None:
            prob_dict = await self.moe.predict_proba(context)
            if prob_dict:
                probs = [prob_dict.get(e.expert_id, 0.0) for e in experts]
                total = sum(probs)
                if total > 0:
                    return [p / total for p in probs]
        # Fallback: softmax over fitness
        logits = [self.fitness_scores
                  .get(e.expert_id, FitnessScore(e.expert_id)).overall_fitness
                  for e in experts]
        m = max(logits)
        exps = [math.exp(l - m) for l in logits]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    # ------------------------------------------------------------------
    def _snapshot_fitness(self) -> Dict[str, float]:
        return {eid: fs.overall_fitness
                for eid, fs in self.fitness_scores.items()}

    def _restore_fitness(self, snapshot: Dict[str, float]) -> None:
        for eid, val in snapshot.items():
            if eid in self.fitness_scores:
                self.fitness_scores[eid].overall_fitness = val

    # ------------------------------------------------------------------
    async def _chaos_loop(self) -> None:
        assert self.chaos is not None
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.chaos.maybe_inject_failure()
            except Exception as e:
                logger.debug(f"chaos loop: {e}")

    async def _quantum_loop(self) -> None:
        assert self.quantum is not None
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=6 * 3600.0)
                break
            except asyncio.TimeoutError:
                pass
            try:
                # Register each expert's fitness vector as a teacher.
                for eid, fs in self.fitness_scores.items():
                    vec = [getattr(fs, d) for d in FitnessScore.DIMENSIONS]
                    self.quantum.register_teacher(eid, vec)
                await self.quantum.step(self.storage, self.registry_id)
                self._stats["total_quantum_steps"] += 1
            except Exception as e:
                logger.debug(f"quantum loop: {e}")

    # ------------------------------------------------------------------
    async def save_state(self) -> None:
        state = {
            "experts": {eid: exp.to_dict()
                        for eid, exp in self._experts.items()},
            "fitness_scores": {eid: fs.to_dict()
                               for eid, fs in self.fitness_scores.items()},
            "migration_paths": dict(self._migration_paths),
            "stats": dict(self._stats),
        }
        try:
            await asyncio.to_thread(
                self.storage.save_state, "expert_registry_state",
                json.dumps(state))
        except Exception as e:
            logger.warning(f"save_state failed: {e}")

    async def _load_state_from_storage(self) -> None:
        try:
            raw = self.storage.get_state("expert_registry_state")
        except Exception:
            raw = None
        if not raw:
            return
        try:
            state = json.loads(raw)
        except Exception:
            return
        for eid, d in (state.get("experts") or {}).items():
            try:
                prof = ExpertProfile.from_dict(d)
                self._experts[eid] = prof
                self._register_indexes(prof)
                if self.auction is not None:
                    self.auction.add_expert(eid)
            except Exception as e:
                logger.warning(f"load expert {eid} failed: {e}")
        for eid, d in (state.get("fitness_scores") or {}).items():
            try:
                self.fitness_scores[eid] = FitnessScore.from_dict(d)
            except Exception as e:
                logger.warning(f"load fitness {eid} failed: {e}")
        self._migration_paths = dict(state.get("migration_paths") or {})
        self._stats.update(state.get("stats") or {})

    async def shutdown(self) -> None:
        logger.info("Shutting down Expert Registry v7.2.0")
        self._shutdown_event.set()
        if self.config.enable_persistence:
            await self.save_state()
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._ready = False
        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "registry_id": self.registry_id,
            "ready": self._ready,
            "experts": len(self._experts),
            "available_experts": sum(
                1 for e in self._experts.values()
                if e.lifecycle_state.is_available()),
            "background_tasks": len(self._background_tasks),
            "moe": self.moe.get_stats() if self.moe else None,
            "pareto_front": len(self.pareto_front.get_front())
                if self.pareto_front else 0,
            "federated_rounds": self.federated.rounds if self.federated else 0,
            "precision": self.precision.current if self.precision else None,
            "carbon_budget_remaining_kg":
                (self.carbon.budget_kg - self.carbon.used_kg)
                if self.carbon else None,
            "chaos_rollbacks": self.chaos.rollback_count
                if self.chaos else 0,
            "stats": dict(self._stats),
        }


# =============================================================================
# FIXTURES
# =============================================================================
@pytest.fixture
def config():
    return ExpertRegistryConfig(seed=12345)


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
def pareto():
    return ParetoGating()


@pytest.fixture
def drift(storage, adaptive_cost):
    return DriftDetector(storage, adaptive_cost)


@pytest.fixture
def metrics():
    return MetricsRegistry()


@pytest.fixture
def sample_profile():
    return ExpertProfile(
        expert_id="energy_expert_v1",
        expert_name="Energy Optimizer",
        version=ExpertVersion(1, 0, 0),
        domain=ExpertDomain.ENERGY,
        hardware_profile=HardwareProfile.GPU,
        accuracy_score=0.8,
        reliability_score=0.9,
        efficiency_score=0.85,
    )


@pytest_asyncio.fixture
async def registry(storage, queue, adaptive_cost, pareto, drift, metrics,
                   config):
    reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                         metrics, config=config)
    await reg.initialize()
    try:
        yield reg
    finally:
        await reg.shutdown()


# =============================================================================
# TESTS — v7.1.0 BUG FIXES
# =============================================================================
class TestBugFixes:
    @pytest.mark.asyncio
    async def test_background_tasks_initialised(self, registry):
        assert isinstance(registry._background_tasks, set)

    @pytest.mark.asyncio
    async def test_shutdown_cancels_background_tasks(self, storage, queue,
                                                     adaptive_cost, pareto,
                                                     drift, metrics):
        cfg = ExpertRegistryConfig(seed=1, enable_chaos=True)
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics, config=cfg)
        await reg.initialize()
        # Chaos loop should be running.
        assert len(reg._background_tasks) >= 1
        await reg.shutdown()
        assert len(reg._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_wait_until_ready_raises_if_not_initialised(
            self, storage, queue, adaptive_cost, pareto, drift, metrics):
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics)
        with pytest.raises(RuntimeError):
            await reg.wait_until_ready()

    @pytest.mark.asyncio
    async def test_fitness_weights_merge_with_defaults(
            self, storage, queue, adaptive_cost, pareto, drift, metrics):
        cfg = ExpertRegistryConfig(fitness_weights={"resource_efficiency": 1.0})
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics, config=cfg)
        # All dimensions must be present.
        for dim in FitnessScore.DIMENSIONS:
            assert dim in reg.config.fitness_weights

    @pytest.mark.asyncio
    async def test_fitness_weights_normalised(self, storage, queue,
                                              adaptive_cost, pareto, drift,
                                              metrics):
        cfg = ExpertRegistryConfig(fitness_weights={"resource_efficiency": 5.0})
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics, config=cfg)
        # Calculate overall fitness with sparse weights does not crash.
        fs = FitnessScore("x")
        fs.calculate_overall(reg.config.fitness_weights)
        assert 0.0 <= fs.overall_fitness <= 1.0

    @pytest.mark.asyncio
    async def test_save_and_load_roundtrip(self, storage, queue,
                                           adaptive_cost, pareto, drift,
                                           metrics, sample_profile):
        cfg = ExpertRegistryConfig(seed=1)
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics, config=cfg)
        await reg.initialize()
        ok, _ = await reg.register_expert(sample_profile, auto_certify=True)
        assert ok
        await reg.save_state()

        reg2 = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                              metrics, config=cfg)
        await reg2.initialize()
        assert "energy_expert_v1" in reg2._experts
        assert "energy_expert_v1" in reg2.fitness_scores
        await reg2.shutdown()
        await reg.shutdown()

    @pytest.mark.asyncio
    async def test_fitness_score_bounds_respected(self, registry,
                                                  sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        for fs in registry.fitness_scores.values():
            assert 0.0 <= fs.overall_fitness <= 1.0

    @pytest.mark.asyncio
    async def test_duplicate_same_version_rejected(self, registry,
                                                   sample_profile):
        ok, _ = await registry.register_expert(sample_profile, auto_certify=True)
        assert ok
        ok, msg = await registry.register_expert(sample_profile, auto_certify=True)
        assert not ok
        assert "same version" in msg

    @pytest.mark.asyncio
    async def test_newer_version_archives_old(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        newer = ExpertProfile(
            expert_id="energy_expert_v1",
            expert_name="Energy Optimizer",
            version=ExpertVersion(2, 0, 0),
            domain=ExpertDomain.ENERGY,
            hardware_profile=HardwareProfile.GPU,
        )
        ok, _ = await registry.register_expert(newer, auto_certify=True)
        assert ok
        assert registry._experts["energy_expert_v1"].version.major == 2


# =============================================================================
# TESTS — ENHANCEMENT 1: QUANTUM DISTILLATION
# =============================================================================
class TestQuantumDistillation:
    def test_register_teacher_normalises(self):
        q = QuantumDistillationModule(n_dims=5)
        q.register_teacher("a", [1, 2, 3, 4, 5])
        assert abs(sum(q.teachers["a"]) - 1.0) < 1e-9

    def test_empty_teacher_is_noop(self):
        q = QuantumDistillationModule(n_dims=5)
        q.register_teacher("empty", [])
        assert "empty" not in q.teachers

    def test_superposition_agreement_amplifies(self):
        q = QuantumDistillationModule(n_dims=5)
        p = [0.5, 0.2, 0.1, 0.1, 0.1]
        q.register_teacher("a", p)
        q.register_teacher("b", p)
        sup, _ = q._superpose()
        for a, b in zip(sup, p):
            assert abs(a - b) < 1e-6

    def test_entropy_non_negative(self):
        q = QuantumDistillationModule(n_dims=4)
        q.register_teacher("a", [0.5, 0.5, 0, 0])
        _, e = q._superpose()
        assert e >= 0.0

    @pytest.mark.asyncio
    async def test_step_updates_student(self, storage):
        q = QuantumDistillationModule(n_dims=5)
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        before = q.get_student_policy()
        entry = await q.step(storage)
        after = q.get_student_policy()
        assert before != after
        assert "entropy" in entry
        assert sum(after) == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_convergence(self):
        q = QuantumDistillationModule(n_dims=5, learning_rate=1e-6,
                                      convergence_eps=1e-3)
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        for _ in range(50):
            await q.step()
            if q.is_converged():
                break
        assert q.is_converged()


# =============================================================================
# TESTS — ENHANCEMENT 2: CAUSAL FEATURE MASK
# =============================================================================
class TestCausalFeatureMask:
    def test_apply_with_uniform_mask(self):
        c = CausalFeatureMask(feature_dim=3)
        assert c.apply([1.0, 2.0, 3.0]) == [1.0, 2.0, 3.0]

    def test_fit_returns_summary(self):
        c = CausalFeatureMask(feature_dim=6)
        for _ in range(50):
            c.observe([random.random() for _ in range(6)])
        summary = c.fit()
        assert "mask" in summary
        assert len(summary["mask"]) == 6

    def test_observe_persists_features(self):
        c = CausalFeatureMask(feature_dim=3)
        c.observe([0.1, 0.2, 0.3])
        assert len(c.samples) == 1


# =============================================================================
# TESTS — ENHANCEMENT 3: FEDERATED AGGREGATOR
# =============================================================================
class TestFederatedAggregator:
    @pytest.mark.asyncio
    async def test_single_client_roundtrip(self, storage):
        f = FederatedGreenAggregator(storage, "inst_1")
        await f.share_weights("m", b"\x01\x02\x03\x04")
        agg = await f.pull_aggregated("m")
        assert agg == b"\x01\x02\x03\x04"

    @pytest.mark.asyncio
    async def test_weighted_average(self, storage):
        f1 = FederatedGreenAggregator(storage, "i1")
        f2 = FederatedGreenAggregator(storage, "i2")
        await f1.share_weights("m", b"\x00\x00", sample_count=1)
        await f2.share_weights("m", b"\x08\x08", sample_count=3)
        agg = await f1.pull_aggregated("m")
        # (1*0 + 3*8)/(1+3) = 6
        assert agg == b"\x06\x06"

    @pytest.mark.asyncio
    async def test_missing_returns_none(self, storage):
        f = FederatedGreenAggregator(storage, "i1")
        assert await f.pull_aggregated("nope") is None

    @pytest.mark.asyncio
    async def test_dp_noise(self, storage):
        f = FederatedGreenAggregator(storage, "i1", dp_epsilon=0.5,
                                     rng=random.Random(7))
        await f.share_weights("m", bytes([100] * 16))
        # Aggregation with a single client returns the noised blob.
        agg = await f.pull_aggregated("m")
        assert agg is not None
        assert any(b != 100 for b in agg)

    @pytest.mark.asyncio
    async def test_sparsification_limits_nonzero(self, storage):
        f = FederatedGreenAggregator(storage, "i1", sparse_topk=0.25,
                                     rng=random.Random(7))
        await f.share_weights("m", bytes(range(64)))
        agg = await f.pull_aggregated("m")
        # After sparsification, non-128 values ≤ 16 (25% of 64).
        non_mid = sum(1 for b in agg if b != 128)
        assert non_mid <= 16


# =============================================================================
# TESTS — ENHANCEMENT 4: EXPERT AUCTION
# =============================================================================
class TestExpertAuction:
    def test_compute_bids_returns_all_experts(self):
        a = ExpertAuction(["e1", "e2", "e3"], feature_dim=4)
        bids = a.compute_bids([0.1, 0.2, 0.3, 0.4])
        assert set(bids.keys()) == {"e1", "e2", "e3"}

    def test_update_changes_scores(self):
        a = ExpertAuction(["e1"], feature_dim=3)
        features = [0.5, 0.5, 0.5]
        before = a.compute_bids(features)["e1"]
        a.update("e1", features, reward=1.0)
        after = a.compute_bids(features)["e1"]
        assert after > before

    def test_select_experts_top_k(self):
        a = ExpertAuction(["e1", "e2", "e3"], feature_dim=3)
        for _ in range(10):
            a.update("e1", [1.0, 0, 0], reward=1.0)
        top = a.select_experts([1.0, 0, 0], top_k=2)
        assert len(top) == 2
        assert "e1" in top


# =============================================================================
# TESTS — ENHANCEMENT 5: TEMPORAL SAFETY MONITOR
# =============================================================================
class TestTemporalSafetyMonitor:
    @pytest.mark.asyncio
    async def test_always_satisfied(self):
        s = TemporalSafetyMonitor()
        s.add_formula("G (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.8})
        result = await s.check()
        assert all(ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_always_violated(self):
        s = TemporalSafetyMonitor()
        s.add_formula("G (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.2})
        result = await s.check()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_eventually_satisfied(self):
        s = TemporalSafetyMonitor()
        s.add_formula("F (done == 1)")
        await s.push({"done": 0})
        await s.push({"done": 1})
        result = await s.check()
        assert all(ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_never_violated(self):
        s = TemporalSafetyMonitor()
        s.add_formula("NEVER (carbon >= 0.8)")
        await s.push({"carbon": 0.5})
        await s.push({"carbon": 0.9})
        result = await s.check()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_historically(self):
        s = TemporalSafetyMonitor()
        s.add_formula("H (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.1})
        result = await s.check()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_once(self):
        s = TemporalSafetyMonitor()
        s.add_formula("O (started == 1)")
        await s.push({"started": 0})
        result = await s.check()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_empty_trace_no_violation(self):
        s = TemporalSafetyMonitor()
        s.add_formula("G (quality >= 0.5)")
        result = await s.check()
        assert all(ok for ok in result.values())


# =============================================================================
# TESTS — ENHANCEMENT 6: XAI EXPLAINER
# =============================================================================
class TestXAIExplainer:
    @pytest.mark.asyncio
    async def test_integrated_gradients_sum_to_prediction_diff(self):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIExplainer(ig_steps=16)
        def f(v):
            return float(np.dot(v, [0.4, -0.3, -0.2, -0.1]))
        feats = np.array([0.9, 0.4, 0.5, 0.4])
        attrs = x.integrated_gradients(f, feats)
        # Sum of attributions ≈ f(x) - f(baseline)
        diff = float(f(feats.reshape(1, -1))) - float(f(np.zeros_like(feats).reshape(1, -1)))
        assert abs(float(np.sum(attrs)) - diff) < 0.1

    @pytest.mark.asyncio
    async def test_counterfactual_success(self):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIExplainer(cf_max_iter=100, cf_target_delta=0.05,
                         rng=random.Random(0))
        def f(v):
            return float(v[0] - v[1])
        feats = np.array([0.9, 0.4])
        cf = x.counterfactual(f, feats)
        assert "changed_index" in cf

    @pytest.mark.asyncio
    async def test_explain_returns_dict(self):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIExplainer(ig_steps=8)
        def f(v):
            return float(v.sum())
        feats = np.array([0.5, 0.5])
        result = await x.explain(f, feats, names=["a", "b"])
        assert "attributions" in result
        assert "counterfactual" in result


# =============================================================================
# TESTS — ENHANCEMENT 7: PRECISION CONTROLLER
# =============================================================================
class TestPrecisionController:
    def test_select_lowest_energy(self):
        p = PrecisionController(levels=["fp32", "int8"])
        assert p.select_precision() == "int8"

    def test_select_precision_fp32_for_high_sensitivity(self):
        p = PrecisionController(levels=["fp32", "int8"],
                                max_sensitivity=0.3)
        assert p.select_precision(layer_sensitivity=0.9) == "fp32"

    @pytest.mark.asyncio
    async def test_switch_updates_current(self):
        p = PrecisionController(levels=["fp32", "int8"], hysteresis_s=0.0)
        ok = await p.switch_to("int8")
        assert ok is True
        assert p.current == "int8"

    @pytest.mark.asyncio
    async def test_switch_same_is_noop(self):
        p = PrecisionController()
        assert await p.switch_to("fp32") is False

    @pytest.mark.asyncio
    async def test_auto_switch_on_drop(self):
        p = PrecisionController(levels=["fp32", "int8"],
                                hysteresis_s=0.0)
        p.current = "int8"
        await p.auto_switch(recent_acc=0.7, baseline_acc=0.95)
        assert p.current == "fp32"

    @pytest.mark.asyncio
    async def test_layer_wise_selection(self):
        p = PrecisionController(levels=["fp32", "int8"],
                                max_sensitivity=0.3)
        applied = await p.auto_switch_layers(
            per_layer_acc={"a": 0.94, "b": 0.5}, baseline_acc=0.95)
        assert applied["b"] == "fp32"


# =============================================================================
# TESTS — ENHANCEMENT 8: CARBON MARKET
# =============================================================================
class TestCarbonMarket:
    @pytest.mark.asyncio
    async def test_price_update(self, storage):
        c = CarbonMarketClient(storage, rng=random.Random(0))
        p = await c.update_price()
        assert p >= 0

    @pytest.mark.asyncio
    async def test_budget_overshoot_defers(self, storage):
        c = CarbonMarketClient(storage, budget_kg=0.1, rng=random.Random(0))
        c.used_kg = 0.09
        result = await c.net_zero_schedule(1.0, 0.1)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_buy_and_retire_rec(self, storage):
        c = CarbonMarketClient(storage)
        await c.buy_credits(10.0)
        await c.buy_credits(5.0)
        assert c.rec_balance() == 15.0
        retired = await c.retire_rec(7.0)
        assert retired == 7.0
        assert abs(c.rec_balance() - 8.0) < 1e-9

    @pytest.mark.asyncio
    async def test_ema_smooths_price(self, storage):
        prices = iter([100.0, 200.0])
        async def oracle():
            return next(prices)
        c = CarbonMarketClient(storage, price_oracle=oracle)
        await c.update_price()
        await c.update_price()
        assert 100.0 < c.price_ema < 200.0


# =============================================================================
# TESTS — ENHANCEMENT 9: CHAOS ENGINE
# =============================================================================
class TestChaosEngine:
    @pytest.mark.asyncio
    async def test_latency_experiment(self, registry):
        chaos = ChaosEngine(registry, probability=1.0,
                            rng=random.Random(0))
        result = await chaos.run_experiment("t1", "latency")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_exception_experiment(self, registry):
        chaos = ChaosEngine(registry, rng=random.Random(0))
        result = await chaos.run_experiment("t2", "exception")
        assert result["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, registry):
        chaos = ChaosEngine(registry)
        with pytest.raises(ValueError):
            await chaos.run_experiment("t", "unknown")

    @pytest.mark.asyncio
    async def test_corrupt_fitness_stays_in_bounds(self, registry,
                                                   sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        chaos = ChaosEngine(registry, rng=random.Random(0))
        for _ in range(20):
            await chaos.run_experiment(f"c{_}", "data_corruption")
        for fs in registry.fitness_scores.values():
            assert 0.0 <= fs.overall_fitness <= 1.0

    @pytest.mark.asyncio
    async def test_auto_rollback(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        # Steady-state predicate fails after the first call.
        state = {"count": 0}
        async def steady():
            state["count"] += 1
            return state["count"] == 1
        chaos = ChaosEngine(registry, steady_predicate=steady,
                            auto_rollback=True, rng=random.Random(0))
        result = await chaos.run_experiment("rb", "latency")
        assert result["rolled_back"] is True
        assert chaos.rollback_count == 1


# =============================================================================
# TESTS — ENHANCEMENT 10: HUMAN APPROVAL
# =============================================================================
class TestHumanApproval:
    @pytest.mark.asyncio
    async def test_single_candidate_auto_approves(self):
        h = HumanApprovalHandler()
        ok = await h.request_approval({"candidates": [{"id": "only"}]})
        assert ok is True

    @pytest.mark.asyncio
    async def test_confident_choice_skips_query(self):
        h = HumanApprovalHandler(uncertainty_threshold=0.01)
        await h.record_choice("u", "a", metrics={"quality_score": 1.0})
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 10.0},
                {"id": "lo", "quality_score": 0.01},
            ]
        }, user_id="u")
        assert ok is True

    @pytest.mark.asyncio
    async def test_uncertain_choice_queries_user(self):
        h = HumanApprovalHandler(uncertainty_threshold=10.0, timeout=0.05)
        await h.submit_response("u", "hi")
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 1.0},
                {"id": "lo", "quality_score": 1.0},
            ]
        }, user_id="u")
        assert ok is True

    @pytest.mark.asyncio
    async def test_record_choice_persists(self):
        h = HumanApprovalHandler()
        await h.record_choice("u", "s", metrics={"quality_score": 0.9})
        assert "u" in h.preferences


# =============================================================================
# TESTS — MoE, PARETO, CONTEXTUAL, FORECASTER
# =============================================================================
class TestMoEGating:
    @pytest.mark.asyncio
    async def test_proba_empty_when_untrained(self, registry):
        probs = await registry.moe.predict_proba({"quality": 0.9})
        assert probs == {}

    @pytest.mark.asyncio
    async def test_proba_after_samples(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        for _ in range(6):
            await registry.moe.add_training_sample(
                {"quality": 0.9}, "energy_expert_v1", reward=1.0)
        probs = await registry.moe.predict_proba({"quality": 0.9})
        assert "energy_expert_v1" in probs
        assert abs(sum(probs.values()) - 1.0) < 1e-6

    def test_get_stats(self, config):
        # Build a registry-like stub.
        class R:
            _experts: Dict[str, Any] = {}
        m = MoEGatingNetwork(R(), config)
        assert m.get_stats()["samples"] == 0


class TestParetoFront:
    @pytest.mark.asyncio
    async def test_non_dominated_added(self, registry, sample_profile):
        ok = await registry.pareto_front.add_expert(sample_profile)
        assert ok is True

    @pytest.mark.asyncio
    async def test_dominated_rejected(self, registry):
        a = ExpertProfile("a", "a", ExpertVersion(1, 0, 0),
                          ExpertDomain.GENERAL, HardwareProfile.CPU,
                          accuracy_score=0.9, carbon_per_inference=0.001,
                          helium_per_inference=0.001, energy_per_inference=0.001)
        b = ExpertProfile("b", "b", ExpertVersion(1, 0, 0),
                          ExpertDomain.GENERAL, HardwareProfile.CPU,
                          accuracy_score=0.5, carbon_per_inference=0.01,
                          helium_per_inference=0.01, energy_per_inference=0.01)
        assert await registry.pareto_front.add_expert(a)
        assert not await registry.pareto_front.add_expert(b)


class TestContextualWeights:
    def test_step_normalises(self, config):
        c = ContextualWeightAdjuster(config)
        w = c.step({d: 1.0 for d in FitnessScore.DIMENSIONS})
        assert abs(sum(w.values()) - 1.0) < 1e-6


class TestForecaster:
    def test_observe_and_forecast(self):
        f = ImprovedPredictiveForecaster()
        for v in (1.0, 1.1, 1.2, 1.3, 1.4):
            f.observe("k", v)
        pred = f.forecast("k", steps=1)
        assert pred > 1.3


# =============================================================================
# TESTS — REGISTRY INTEGRATION
# =============================================================================
class TestRegistryIntegration:
    @pytest.mark.asyncio
    async def test_register_and_route(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        chosen = await registry.route({"quality": 0.9})
        assert chosen == "energy_expert_v1"

    @pytest.mark.asyncio
    async def test_route_with_no_experts(self, registry):
        assert await registry.route({}) is None

    @pytest.mark.asyncio
    async def test_record_reward_trains_moe(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        for _ in range(10):
            await registry.record_reward("energy_expert_v1",
                                         {"quality": 0.9}, reward=1.0)
        assert registry.moe.get_stats()["samples"] == 10

    @pytest.mark.asyncio
    async def test_policy_probs_normalised(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        probs = await registry.policy_probs({})
        assert abs(sum(probs) - 1.0) < 1e-6

    @pytest.mark.asyncio
    async def test_health_check(self, registry):
        hc = await registry.health_check()
        assert hc["registry_id"] == "default"
        assert "experts" in hc
        assert "background_tasks" in hc

    @pytest.mark.asyncio
    async def test_deregister_expert(self, registry, sample_profile):
        await registry.register_expert(sample_profile, auto_certify=True)
        ok, _ = await registry.deregister_expert("energy_expert_v1")
        assert ok
        assert "energy_expert_v1" not in registry._experts

    @pytest.mark.asyncio
    async def test_concurrent_register(self, registry):
        async def _reg(i):
            p = ExpertProfile(f"e{i}", f"e{i}", ExpertVersion(1, 0, 0),
                              ExpertDomain.GENERAL, HardwareProfile.CPU)
            return await registry.register_expert(p, auto_certify=True)
        results = await asyncio.gather(*[_reg(i) for i in range(10)])
        assert all(ok for ok, _ in results)
        assert len(registry._experts) == 10


# =============================================================================
# TESTS — EDGE CASES
# =============================================================================
class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_storage_failure_does_not_crash(self, storage, queue,
                                                  adaptive_cost, pareto,
                                                  drift, metrics,
                                                  sample_profile):
        storage.fail_on = "save_state"
        cfg = ExpertRegistryConfig(seed=1, enable_persistence=True)
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics, config=cfg)
        await reg.initialize()
        try:
            ok, _ = await reg.register_expert(sample_profile, auto_certify=True)
            assert ok
            # save_state should fail silently.
            await reg.save_state()
        finally:
            await reg.shutdown()

    @pytest.mark.asyncio
    async def test_zero_experts_safety_check(self, registry):
        result = await registry._check_safety()
        # No rules violated.
        for ok in result.values():
            assert ok is True

    @pytest.mark.asyncio
    async def test_init_with_partial_config(self, storage, queue,
                                            adaptive_cost, pareto, drift,
                                            metrics):
        cfg = ExpertRegistryConfig(seed=1, enable_moe=False,
                                   enable_chaos=False,
                                   enable_quantum_distillation=False)
        reg = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                             metrics, config=cfg)
        await reg.initialize()
        assert reg.moe is None
        assert reg.quantum is None
        await reg.shutdown()

    @pytest.mark.asyncio
    async def test_double_initialize_is_idempotent(self, registry):
        await registry.initialize()
        await registry.initialize()
        assert registry._ready is True

    @pytest.mark.asyncio
    async def test_extreme_values_in_profile(self, registry):
        p = ExpertProfile("x", "x", ExpertVersion(1, 0, 0),
                          ExpertDomain.GENERAL, HardwareProfile.CPU,
                          accuracy_score=1e6, carbon_per_inference=1e-6,
                          helium_per_inference=1e-6)
        ok, _ = await registry.register_expert(p, auto_certify=True)
        assert ok
        fs = registry.fitness_scores["x"]
        assert 0.0 <= fs.overall_fitness <= 1.0


# =============================================================================
# TESTS — DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_seeded_registry_reproducible(self, storage, queue,
                                                adaptive_cost, pareto,
                                                drift, metrics):
        cfg = ExpertRegistryConfig(seed=42)
        reg1 = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                              metrics, config=cfg)
        await reg1.initialize()
        await reg1.register_expert(ExpertProfile(
            "a", "a", ExpertVersion(1, 0, 0), ExpertDomain.GENERAL,
            HardwareProfile.CPU), auto_certify=True)
        probs1 = await reg1.policy_probs({})
        await reg1.shutdown()

        reg2 = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift,
                              metrics, config=cfg)
        await reg2.initialize()
        await reg2.register_expert(ExpertProfile(
            "a", "a", ExpertVersion(1, 0, 0), ExpertDomain.GENERAL,
            HardwareProfile.CPU), auto_certify=True)
        probs2 = await reg2.policy_probs({})
        await reg2.shutdown()

        assert probs1 == probs2


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    async def demo():
        storage = InMemoryStorage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()

        registry = ExpertRegistry(storage, queue, adaptive_cost, pareto,
                                  drift, metrics,
                                  config=ExpertRegistryConfig(seed=1))
        await registry.initialize()

        profile = ExpertProfile(
            expert_id="energy_expert_v1",
            expert_name="Energy Optimizer",
            version=ExpertVersion(1, 0, 0),
            domain=ExpertDomain.ENERGY,
            hardware_profile=HardwareProfile.GPU,
            accuracy_score=0.8, reliability_score=0.9, efficiency_score=0.85,
        )
        ok, msg = await registry.register_expert(profile, auto_certify=True)
        print("Registration:", msg)

        chosen = await registry.route({"quality": 0.9, "carbon_intensity": 300})
        print("Routed to:", chosen)

        probs = await registry.policy_probs({})
        print("Policy probs:", probs)

        hc = await registry.health_check()
        print("Health:", json.dumps(hc, indent=2, default=str))

        await registry.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
    # pytest.main([__file__, "-v", "--tb=short"])
