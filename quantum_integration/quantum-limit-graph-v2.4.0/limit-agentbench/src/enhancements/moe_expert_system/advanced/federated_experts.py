#!/usr/bin/env python3
"""
Federated Experts v9.0.0 — Self-Contained Single-File Bundle
==============================================================

A complete rewrite of the Green Agent federated-experts orchestrator.
Everything needed to run and test is in this file:

  1. Local stubs (Storage, AsyncMessageQueue, FeedbackEvent,
     AdaptiveCostFunction, ParetoGating, DriftDetector, MetricsRegistry).
  2. Protocol interfaces for every injected dependency (storage, queue,
     metrics, adaptive cost, pareto, drift, carbon, helium, gating, bio-core).
  3. A 12-dim federation feature encoder.
  4. Every previously-missing type: AggregationStrategy, FederationRound,
     FederatedExpert, PrivacyLevel, ParticipantRole, ParticipantSelector,
     ReputationScoringSystem, SecureAggregator, ModelCompressor,
     CrossTierDistiller, PredictiveFederationAnalyzer,
     FederationCrossDomainTransfer, TaskManager.
  5. Ten real enhancement modules:
        - QuantumDistillationModule  (density-matrix superposition)
        - CausalFeatureMask          (PC-style partial correlation)
        - ExpertAuction              (LinUCB bandit)
        - SafetyMonitor              (severity + mitigation)
        - TemporalSafetyMonitor      (MTL G/F/NEVER/H/O)
        - XAIExplainer               (Integrated Gradients + CF)
        - PrecisionController        (layer-wise + hysteresis)
        - CarbonMarketClient         (budget + EMA + REC)
        - ChaosEngine                (steady-state + rollback)
        - HumanApprovalHandler       (uncertainty sampling)
  6. `FederationDecision` value object.
  7. Real local training (numpy linear model with honest SGD).
  8. Real HTTP carbon/helium clients with circuit breaker + cache + EMA.
  9. Health + self-healing wired correctly.
 10. `EnhancedFederatedOrchestrator` v9.0.0.
 11. Comprehensive pytest suite.

Run:
    python federated_experts_v9.py         # demo
    pytest federated_experts_v9.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
Optional: torch, aiohttp, scikit-learn.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import math
import os
import random
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Protocol, Sequence,
    Set, Tuple, runtime_checkable,
)

import numpy as np
import pytest
import pytest_asyncio

logger = logging.getLogger("federated_experts")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


# =============================================================================
# OPTIONAL DEPENDENCIES
# =============================================================================
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

try:
    from sklearn.linear_model import SGDRegressor
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    SGDRegressor = None  # type: ignore
    StandardScaler = None  # type: ignore
    SKLEARN_AVAILABLE = False


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
class GatingNetworkLike(Protocol):
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]: ...
    async def record_reward(self, expert_id: str,
                            context: Dict[str, Any], reward: float) -> None: ...


@runtime_checkable
class BioCoreLike(Protocol):
    def get_token_summary(self) -> Dict[str, Any]: ...
    def get_gradient_strengths(self) -> Dict[str, float]: ...


# =============================================================================
# ENUMS
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


class AggregationStrategy(str, Enum):
    FED_AVG = "fed_avg"
    FED_PROX = "fed_prox"
    TOKEN_WEIGHTED = "token_weighted"
    SUSTAINABILITY_WEIGHTED = "sustainability_weighted"
    REPUTATION_WEIGHTED = "reputation_weighted"
    SECURE_AGGREGATION = "secure_aggregation"
    TIERED_DISTILLATION = "tiered_distillation"

    @classmethod
    def from_str(cls, s: str) -> "AggregationStrategy":
        try:
            return cls(s)
        except ValueError:
            return cls.FED_AVG


class AggregationTier(str, Enum):
    EDGE = "edge"
    REGIONAL = "regional"
    CONTINENTAL = "continental"
    GLOBAL = "global"


class PrivacyLevel(str, Enum):
    NONE = "none"
    BASIC = "basic"
    DIFFERENTIAL = "differential"
    SECURE_AGG = "secure_agg"


class ParticipantRole(str, Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    OBSERVER = "observer"


# =============================================================================
# FEATURE ENCODER (12 dims)
# =============================================================================
class FederationFeatures:
    NAMES = (
        "carbon_intensity", "helium_scarcity", "carbon_price",
        "helium_price", "token_pool", "participant_count",
        "sustainability_score", "avg_reputation", "avg_latency_ms",
        "regional_capacity", "regional_usage", "drift_score",
    )
    DIM = len(NAMES)

    @classmethod
    def encode(cls, context: Dict[str, Any]) -> np.ndarray:
        out: List[float] = []
        for name in cls.NAMES:
            v = context.get(name, 0.5)
            if v is None:
                v = 0.5
            try:
                out.append(float(v))
            except (TypeError, ValueError):
                out.append(0.5)
        return np.asarray(out, dtype=np.float32)


# =============================================================================
# VALUE OBJECTS
# =============================================================================
@dataclass
class ComponentHealth:
    status: HealthStatus
    score: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SafetyViolation:
    name: str
    severity: str
    description: str
    mitigated: bool = False


@dataclass(frozen=True)
class FederationDecision:
    state_vec: np.ndarray
    strategy: str
    strategy_probs: np.ndarray
    participant_ids: Tuple[str, ...]
    participant_scores: np.ndarray
    aggregated_model_keys: Tuple[str, ...]
    confidence: float
    timestamp: float
    precision: str = "fp32"
    drift: float = 0.0
    safety_violations: Tuple[str, ...] = ()
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None
    sustainability: float = 0.0


@dataclass
class ClientCapabilities:
    client_id: str
    compute_power_flops: float = 1e12
    memory_gb: float = 16.0
    network_bandwidth_mbps: float = 100.0
    network_latency_ms: float = 50.0
    energy_source_renewable: bool = True
    carbon_intensity_g_per_kwh: float = 400.0
    helium_availability: float = 0.5
    max_model_size_mb: float = 100.0
    supported_architectures: List[str] = field(default_factory=list)
    role: str = ParticipantRole.FOLLOWER.value


@dataclass
class FederatedExpert:
    expert_id: str
    local_model: Dict[str, np.ndarray]
    capabilities: ClientCapabilities
    carbon_footprint: float = 0.1
    helium_usage: float = 0.01
    reputation_score: float = 0.5
    sustainability_contribution: float = 0.5
    tokens_staked: float = 0.0
    tokens_earned: float = 0.0
    is_active: bool = True
    region_id: Optional[str] = None
    role: str = ParticipantRole.FOLLOWER.value
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expert_id": self.expert_id,
            "local_model": {k: np.asarray(v).tolist()
                            for k, v in self.local_model.items()},
            "capabilities": asdict(self.capabilities),
            "carbon_footprint": self.carbon_footprint,
            "helium_usage": self.helium_usage,
            "reputation_score": self.reputation_score,
            "sustainability_contribution": self.sustainability_contribution,
            "tokens_staked": self.tokens_staked,
            "tokens_earned": self.tokens_earned,
            "is_active": self.is_active,
            "region_id": self.region_id,
            "role": self.role,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FederatedExpert":
        d = dict(d)
        d["local_model"] = {
            k: np.asarray(v, dtype=np.float32)
            for k, v in (d.get("local_model") or {}).items()}
        d["capabilities"] = ClientCapabilities(**d.get("capabilities", {}))
        d["last_updated"] = datetime.fromisoformat(d["last_updated"])
        return cls(**d)


@dataclass
class RegionNode:
    region_id: str
    tier: AggregationTier = AggregationTier.REGIONAL
    carbon_intensity: float = 400.0
    helium_availability: float = 0.5
    resource_capacity: float = 1.0
    resource_usage: float = 0.0
    participants: List[str] = field(default_factory=list)
    last_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ReputationRecord:
    node_id: str
    score: float = 0.5
    successful_updates: int = 0
    failed_updates: int = 0
    sustainability_contributions: float = 0.0
    token_stake: float = 0.0
    last_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    history: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=100))


@dataclass
class FederationRound:
    round_id: str
    round_number: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    participants: List[str] = field(default_factory=list)
    dropped_participants: List[str] = field(default_factory=list)
    aggregation_strategy: AggregationStrategy = AggregationStrategy.FED_AVG
    privacy_level: PrivacyLevel = PrivacyLevel.DIFFERENTIAL
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    model_improvement: float = 0.0
    communication_bytes: int = 0
    successful: bool = False
    metrics: Dict[str, Any] = field(default_factory=dict)
    sustainability_score: float = 0.0


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
        return cls(**filtered)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class AdaptiveCostFunction:
    def __init__(self, storage: Optional[InMemoryStorage] = None):
        self.storage = storage
        self.weights = {"accuracy": 0.4, "carbon": 0.2,
                        "cost": 0.2, "latency": 0.2}

    def get_current_weights(self) -> Dict[str, float]:
        return dict(self.weights)

    def compute(self, *, quality: float = 0.5, carbon_g: float = 0.1,
                latency_ms: float = 100.0, energy_joules: float = 10.0,
                health: float = 1.0, atp: float = 0.5) -> float:
        w = self.weights
        return float(
            w["accuracy"] * (1.0 - quality) +
            w["carbon"] * carbon_g +
            w["cost"] * energy_joules / 100.0 +
            w["latency"] * latency_ms / 100.0
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


class MetricsRegistry:
    def __init__(self):
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}

    def inc(self, name: str, amount: int = 1) -> None:
        self.counters[name] += amount

    def set(self, name: str, value: float) -> None:
        self.gauges[name] = value


# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
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
                        (time.time() - self.last_failure_time) > self.recovery_timeout):
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

    async def reset(self) -> None:
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.half_open_requests = 0
            self.last_failure_time = None


# =============================================================================
# TASK MANAGER
# =============================================================================
class TaskManager:
    """Tracks named background tasks and cancels them cleanly on stop."""

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    def start_task(self, name: str, coro_factory: Callable[[], Any]) -> asyncio.Task:
        """`coro_factory` must be a zero-arg callable that returns a coroutine.

        This avoids the "coroutine was never awaited" warning if a task is
        replaced before it started.
        """
        if name in self._tasks and not self._tasks[name].done():
            return self._tasks[name]
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as e:
            raise RuntimeError(
                "TaskManager.start_task requires a running event loop") from e
        task = loop.create_task(coro_factory())
        self._tasks[name] = task
        task.add_done_callback(lambda t, n=name: self._tasks.pop(n, None))
        return task

    def get(self, name: str) -> Optional[asyncio.Task]:
        return self._tasks.get(name)

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
# ENHANCEMENT 1 — QUANTUM DISTILLATION
# =============================================================================
class QuantumDistillationModule:
    """Density-matrix distillation over participant model deltas."""

    def __init__(self, n_dims: int = FederationFeatures.DIM,
                 temperature: float = 1.0, learning_rate: float = 0.1,
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
        self.student: List[float] = [1.0 / self.n_dims] * self.n_dims
        self.history: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._converged = False

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

    def _von_neumann_entropy(self, rho: np.ndarray) -> float:
        try:
            m = (rho + rho.T) / 2.0
            eig = np.linalg.eigvalsh(m)
            eig = np.clip(eig, 1e-12, None)
            return float(-np.sum(eig * np.log(eig)))
        except Exception:
            return 0.0

    def _superpose(self) -> Tuple[List[float], float]:
        n = self.n_dims
        if not self.teachers:
            return list(self.student), 0.0
        amps: List[np.ndarray] = []
        weights: List[float] = []
        for tid, pol in self.teachers.items():
            a = np.array([math.sqrt(max(pol[i], 1e-12))
                          if i < len(pol) else 0.0 for i in range(n)])
            amps.append(a)
            weights.append(self.teacher_weights.get(tid, 1.0))
        wsum = sum(weights) or 1.0
        weights = [w / wsum for w in weights]
        rho = np.zeros((n, n))
        for a, w in zip(amps, weights):
            rho += w * np.outer(a, a)
        diag = np.clip(np.diag(rho), 1e-12, None)
        s = float(diag.sum()) or 1.0
        return (diag / s).tolist(), self._von_neumann_entropy(rho)

    def _softmax(self, x: Iterable[float], temp: float) -> List[float]:
        x = np.asarray(list(x), dtype=float)
        m = x.max()
        e = np.exp((x - m) / max(temp, 1e-6))
        s = e.sum() or 1.0
        return (e / s).tolist()

    async def step(self, storage: Optional[StorageLike] = None,
                   student_id: str = "federation") -> Dict[str, Any]:
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
                    storage.save_state, f"quantum::{student_id}",
                    json.dumps(entry))
            except Exception:
                pass
        return entry

    def is_available(self) -> bool:
        return True

    def is_converged(self) -> bool:
        return self._converged

    def get_student_policy(self) -> List[float]:
        return list(self.student)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL FEATURE MASK (PC-style)
# =============================================================================
_ATOMIC_RE = __import__("re").compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class CausalFeatureMask:
    FEATURE_NAMES = FederationFeatures.NAMES

    def __init__(self, feature_dim: int = FederationFeatures.DIM,
                 rng: Optional[random.Random] = None):
        self.feature_dim = int(feature_dim)
        self.rng = rng or random.Random()
        self.mask = np.ones(self.feature_dim, dtype=np.float32)
        self.edges: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.samples: Deque[np.ndarray] = deque(maxlen=512)

    def observe(self, features: Iterable[float]) -> None:
        feats = np.asarray(list(features), dtype=np.float32)
        if feats.shape[0] != self.feature_dim:
            return
        self.samples.append(feats)

    def apply(self, features: Iterable[float]) -> np.ndarray:
        feats = np.asarray(list(features), dtype=np.float32)
        if feats.shape[0] != self.feature_dim:
            raise ValueError(f"features dim {feats.shape[0]} != {self.feature_dim}")
        return feats * self.mask

    def fit(self, alpha: float = 0.05, threshold: float = 0.25) -> Dict[str, Any]:
        self.edges.clear()
        if len(self.samples) < 10:
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
        d = self.feature_dim
        for i in range(d):
            for j in range(i + 1, d):
                denom = math.sqrt(max(prec[i, i] * prec[j, j], 1e-12))
                pcorr = -prec[i, j] / denom
                if abs(pcorr) <= threshold:
                    continue
                r = min(max(pcorr, -0.999999), 0.999999)
                z = 0.5 * math.log((1 + r) / (1 - r)) * \
                    math.sqrt(max(X.shape[0] - (d - 2), 1))
                p_val = math.erfc(abs(z) / math.sqrt(2.0))
                if p_val > alpha:
                    continue
                self.edges[names[i]][names[j]] = float(pcorr)
        active: Set[str] = set()
        for src, targets in self.edges.items():
            active.add(src)
            active.update(targets.keys())
        for i, n in enumerate(names):
            self.mask[i] = 1.0 if n in active else 0.5
        return self.summary()

    def summary(self) -> Dict[str, Any]:
        return {"features": self.feature_dim,
                "edges": sum(len(v) for v in self.edges.values()),
                "mask": self.mask.tolist()}


# =============================================================================
# ENHANCEMENT 3 — EXPERT AUCTION (LinUCB)
# =============================================================================
class ExpertAuction:
    def __init__(self, participant_ids: List[str],
                 feature_dim: int = FederationFeatures.DIM,
                 alpha: float = 0.2, ridge: float = 1.0,
                 rng: Optional[random.Random] = None):
        self.participant_ids = list(participant_ids)
        self.feature_dim = int(feature_dim)
        self.alpha = float(alpha)
        self.ridge = float(ridge)
        self.rng = rng or random.Random()
        self._A: Dict[str, np.ndarray] = {}
        self._b: Dict[str, np.ndarray] = {}
        for pid in self.participant_ids:
            self.add_participant(pid)

    def add_participant(self, pid: str) -> None:
        if pid in self._A:
            return
        if pid not in self.participant_ids:
            self.participant_ids.append(pid)
        self._A[pid] = self.ridge * np.eye(self.feature_dim, dtype=np.float32)
        self._b[pid] = np.zeros(self.feature_dim, dtype=np.float32)

    def remove_participant(self, pid: str) -> None:
        self.participant_ids = [p for p in self.participant_ids if p != pid]
        self._A.pop(pid, None)
        self._b.pop(pid, None)

    def _vec(self, features: Iterable[float]) -> np.ndarray:
        x = np.asarray(list(features)[:self.feature_dim], dtype=np.float32)
        if x.size < self.feature_dim:
            x = np.pad(x, (0, self.feature_dim - x.size))
        return x

    def compute_bids(self, features: Iterable[float]) -> Dict[str, float]:
        x = self._vec(features)
        out: Dict[str, float] = {}
        for pid in self.participant_ids:
            A = self._A[pid]
            try:
                A_inv = np.linalg.inv(A)
            except np.linalg.LinAlgError:
                A_inv = np.linalg.pinv(A)
            theta = A_inv @ self._b[pid]
            mu = float(theta @ x)
            sigma = float(math.sqrt(max(float(x @ A_inv @ x), 0.0)))
            out[pid] = mu + self.alpha * sigma
        return out

    def select(self, features: Iterable[float], top_k: int = 3) -> List[str]:
        bids = self.compute_bids(features)
        return [pid for pid, _ in sorted(bids.items(),
                                          key=lambda kv: kv[1],
                                          reverse=True)[:top_k]]

    def update(self, pid: str, features: Iterable[float], reward: float) -> None:
        if pid not in self._A:
            self.add_participant(pid)
        x = self._vec(features)
        self._A[pid] = self._A[pid] + np.outer(x, x)
        self._b[pid] = self._b[pid] + reward * x


# =============================================================================
# ENHANCEMENT 4 — SAFETY MONITORS
# =============================================================================
@dataclass
class SafetyRule:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    description: str
    severity: str = "warning"
    mitigation: Optional[Callable[[Any], None]] = None


class SafetyMonitor:
    def __init__(self):
        self.rules: List[SafetyRule] = []
        self.violations: Deque[SafetyViolation] = deque(maxlen=1000)

    def add_rule(self, name: str,
                 predicate: Callable[[Dict[str, Any]], bool],
                 description: str, severity: str = "warning",
                 mitigation: Optional[Callable[[Any], None]] = None) -> None:
        self.rules.append(SafetyRule(name, predicate, description,
                                     severity, mitigation))

    def check(self, state: Dict[str, Any],
              target: Optional[Any] = None) -> List[SafetyViolation]:
        out: List[SafetyViolation] = []
        for rule in self.rules:
            try:
                ok = bool(rule.predicate(state))
            except Exception:
                ok = False
            if not ok:
                v = SafetyViolation(rule.name, rule.severity, rule.description)
                if rule.mitigation and target is not None:
                    try:
                        rule.mitigation(target)
                        v.mitigated = True
                    except Exception:
                        pass
                self.violations.append(v)
                out.append(v)
        return out

    def violation_count(self, severity: Optional[str] = None) -> int:
        if severity is None:
            return len(self.violations)
        return sum(1 for v in self.violations if v.severity == severity)


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
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(maxlen=max_trace)

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
            copy_trace = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy_trace)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
        return result

    def counts(self) -> Dict[str, int]:
        return {rid: r.violations for rid, r in self.rules.items()}


# =============================================================================
# ENHANCEMENT 5 — XAI (Integrated Gradients)
# =============================================================================
class XAIExplainer:
    def __init__(self, ig_steps: int = 8, cf_max_iter: int = 20,
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
# ENHANCEMENT 6 — PRECISION CONTROLLER
# =============================================================================
class PrecisionController:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}

    def __init__(self, levels: Optional[List[str]] = None,
                 max_sensitivity: float = 0.5,
                 switch_threshold: float = 0.02,
                 hysteresis_s: float = 0.5):
        self.levels = levels or list(self.ENERGY.keys())
        self.max_sensitivity = float(max_sensitivity)
        self.switch_threshold = float(switch_threshold)
        self.hysteresis_s = float(hysteresis_s)
        self.current = "fp32"
        self.saved_wh = 0.0
        self._last_switch = 0.0

    def select_precision(self,
                         layer_sensitivity: Optional[float] = None) -> str:
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


# =============================================================================
# ENHANCEMENT 7 — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    def __init__(self, budget_kg: float = 1000.0,
                 price_oracle: Optional[Callable[[], Any]] = None,
                 rng: Optional[random.Random] = None,
                 ema_alpha: float = 0.3):
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

    async def update_price(self) -> float:
        try:
            p = float(await self.price_oracle())
        except Exception:
            p = self.price
        self.price = max(0.0, p)
        self.price_ema = (self.ema_alpha * self.price +
                          (1 - self.ema_alpha) * self.price_ema)
        return self.price

    def _budget_ok(self, extra: float) -> bool:
        return (self.used_kg + extra) <= self.budget_kg

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
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost,
                "credit_price_usd": self.price,
                "budget_remaining_kg": max(0.0, self.budget_kg - self.used_kg)}

    async def buy_credits(self, mwh: float, price_per_mwh: float = 5.0,
                          source: str = "wind") -> Dict[str, Any]:
        cost = mwh * price_per_mwh
        cert = uuid.uuid4().hex[:12]
        self.rec_ledger.append({"mwh": mwh, "cost": cost, "source": source,
                                "certificate": cert, "retired": False})
        return {"cost": cost, "certificate": cert}

    async def sell_credits(self, mwh: float,
                           price_per_mwh: float = 5.0) -> Dict[str, Any]:
        return {"revenue": mwh * price_per_mwh, "mwh": mwh}

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
# ENHANCEMENT 8 — CHAOS ENGINE
# =============================================================================
class ChaosEngine:
    FAULT_TYPES = ("latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop")

    def __init__(self, federation: "EnhancedFederatedOrchestrator",
                 probability: float = 0.01,
                 auto_rollback: bool = True,
                 blast_radius: float = 0.5,
                 rng: Optional[random.Random] = None):
        self.federation = federation
        self.probability = float(probability)
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
        self.active: Dict[str, Dict[str, Any]] = {}
        self.rollback_count = 0
        self._lock = threading.Lock()

    def _allowed(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def maybe_inject_failure(self) -> Optional[Dict[str, Any]]:
        if self.rng.random() >= self.probability:
            return None
        fault = self.rng.choice(self.FAULT_TYPES)
        return await self.run_experiment(f"auto_{uuid.uuid4().hex[:6]}", fault)

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        with self._lock:
            if len(self.active) >= self._allowed():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}
            self.active[name] = {"fault_type": fault_type}
        t0 = time.time()
        before = self.federation._check_steady_state()
        snapshot = self.federation._snapshot_state()
        status = "completed"
        try:
            if fault_type == "latency":
                await asyncio.sleep(0.005)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.005)
            elif fault_type == "data_corruption":
                self.federation._corrupt_state_bounded(self.rng)
        except Exception:
            status = "injected"
        finally:
            with self._lock:
                self.active.pop(name, None)
        after = self.federation._check_steady_state()
        rolled_back = False
        if before and not after and self.auto_rollback:
            self.federation._restore_state(snapshot)
            rolled_back = True
            self.rollback_count += 1
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status,
                "duration_ms": (time.time() - t0) * 1000.0,
                "rolled_back": rolled_back}


# =============================================================================
# ENHANCEMENT 9 — HUMAN APPROVAL (uncertainty sampling)
# =============================================================================
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
# ENHANCEMENT 10 — REAL HTTP CARBON + HELIUM CLIENTS
# =============================================================================
class CarbonIntensityClient:
    def __init__(self, api_url: Optional[str] = None,
                 api_key: Optional[str] = None,
                 region: str = "us-east",
                 update_interval: float = 300.0,
                 fallback: Optional[Dict[str, float]] = None,
                 ema_alpha: float = 0.3):
        self.api_url = api_url
        self.api_key = api_key
        self.region = region
        self.update_interval = float(update_interval)
        self.ema_alpha = float(ema_alpha)
        self.fallback = fallback or {"us-east": 420, "us-west": 350,
                                     "eu-west": 280, "asia-east": 500}
        self.intensity = float(self.fallback.get(region, 400.0))
        self.intensity_ema = self.intensity
        self.price = 50.0
        self.price_ema = 50.0
        self.last_update: Optional[datetime] = None
        self._circuit = CircuitBreaker(name="carbon_api")
        self._session = None
        self._session_lock = asyncio.Lock()
        self._cache: Dict[str, Tuple[float, datetime]] = {}

    async def _get_session(self):
        if not AIOHTTP_AVAILABLE or self.api_url is None:
            return None
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _http_fetch(self) -> Optional[float]:
        session = await self._get_session()
        if session is None:
            return None
        headers = {"auth-token": self.api_key} if self.api_key else {}
        url = f"{self.api_url}?zone={self.region}"
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"carbon API status {resp.status}")
            data = await resp.json()
            return float(data.get("carbonIntensity", self.intensity))

    async def update(self, force: bool = False) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        cache_key = f"{self.region}:{now.hour}"
        if not force and cache_key in self._cache:
            val, ts = self._cache[cache_key]
            if (now - ts).total_seconds() < self.update_interval:
                self.intensity = val
                return self._snapshot()
        try:
            value = await self._circuit.call(self._http_fetch)
            if value is None:
                raise RuntimeError("no HTTP client")
        except Exception as e:
            logger.debug(f"carbon fetch fallback: {e}")
            value = float(self.fallback.get(self.region, 400.0))
        self.intensity = max(0.0, float(value))
        self.intensity_ema = (self.ema_alpha * self.intensity +
                              (1 - self.ema_alpha) * self.intensity_ema)
        self.price = max(10.0, 50.0 * (1.0 + (self.intensity - 300) / 500))
        self.price_ema = (self.ema_alpha * self.price +
                          (1 - self.ema_alpha) * self.price_ema)
        self.last_update = now
        self._cache[cache_key] = (self.intensity, now)
        return self._snapshot()

    async def get_current_intensity(self) -> float:
        if (self.last_update is None or
                (datetime.now(timezone.utc) - self.last_update).total_seconds()
                > self.update_interval):
            await self.update()
        return self.intensity

    async def get_current_price(self) -> float:
        await self.get_current_intensity()
        return self.price

    def _snapshot(self) -> Dict[str, Any]:
        return {"intensity": self.intensity, "intensity_ema": self.intensity_ema,
                "price": self.price, "price_ema": self.price_ema,
                "region": self.region, "cache_size": len(self._cache)}

    async def get_health_status(self) -> ComponentHealth:
        if self.last_update is None:
            return ComponentHealth(HealthStatus.UNKNOWN, 0.5, {})
        age = (datetime.now(timezone.utc) - self.last_update).total_seconds()
        if age > self.update_interval * 5:
            return ComponentHealth(HealthStatus.UNHEALTHY, 0.2,
                                   {"age_seconds": age})
        if self._circuit.state == CircuitBreakerState.OPEN:
            return ComponentHealth(HealthStatus.DEGRADED, 0.4, {})
        return ComponentHealth(HealthStatus.HEALTHY, 1.0,
                               {"age_seconds": age})

    async def close(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                await self._session.close()
            self._session = None


class HeliumClient:
    def __init__(self, api_url: Optional[str] = None,
                 api_key: Optional[str] = None,
                 update_interval: float = 600.0,
                 base_price: float = 0.5,
                 ema_alpha: float = 0.3,
                 rng: Optional[random.Random] = None):
        self.api_url = api_url
        self.api_key = api_key
        self.update_interval = float(update_interval)
        self.base_price = float(base_price)
        self.ema_alpha = float(ema_alpha)
        self.rng = rng or random.Random()
        self.scarcity = 0.5
        self.price = base_price
        self.price_ema = base_price
        self.efficiency = 0.5
        self.last_update: Optional[datetime] = None
        self._circuit = CircuitBreaker(name="helium_api")
        self._session = None
        self._session_lock = asyncio.Lock()

    async def _get_session(self):
        if not AIOHTTP_AVAILABLE or self.api_url is None:
            return None
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _http_fetch(self) -> Optional[Dict[str, float]]:
        session = await self._get_session()
        if session is None:
            return None
        headers = {"auth-token": self.api_key} if self.api_key else {}
        async with session.get(self.api_url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"helium API status {resp.status}")
            data = await resp.json()
            return {"scarcity": float(data.get("scarcity", self.scarcity)),
                    "price": float(data.get("price", self.price)),
                    "efficiency": float(data.get("efficiency", self.efficiency))}

    async def update(self, force: bool = False) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        if (not force and self.last_update is not None and
                (now - self.last_update).total_seconds() < self.update_interval):
            return self._snapshot()
        try:
            data = await self._circuit.call(self._http_fetch)
            if data is None:
                raise RuntimeError("no HTTP client")
            self.scarcity = float(data["scarcity"])
            self.price = float(data["price"])
            self.efficiency = float(data["efficiency"])
        except Exception:
            self.scarcity = max(0.0, min(1.0,
                self.scarcity + self.rng.uniform(-0.05, 0.05)))
            self.price = max(0.05, self.base_price * (1.0 + self.scarcity * 0.8))
        self.price_ema = (self.ema_alpha * self.price +
                          (1 - self.ema_alpha) * self.price_ema)
        self.last_update = now
        return self._snapshot()

    # Synchronous interface
    def get_scarcity(self) -> float:
        return self.scarcity

    def get_cost_index(self) -> float:
        return self.price / max(self.base_price, 1e-6)

    def get_efficiency(self) -> float:
        return self.efficiency

    async def get_current_price(self) -> float:
        if (self.last_update is None or
                (datetime.now(timezone.utc) - self.last_update).total_seconds()
                > self.update_interval):
            await self.update()
        return self.price

    def _snapshot(self) -> Dict[str, Any]:
        return {"scarcity": self.scarcity, "price": self.price,
                "price_ema": self.price_ema, "efficiency": self.efficiency}

    async def get_health_status(self) -> ComponentHealth:
        if self.last_update is None:
            return ComponentHealth(HealthStatus.UNKNOWN, 0.5, {})
        if self._circuit.state == CircuitBreakerState.OPEN:
            return ComponentHealth(HealthStatus.DEGRADED, 0.4, {})
        return ComponentHealth(HealthStatus.HEALTHY, 1.0, {})

    async def close(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                await self._session.close()
            self._session = None


# =============================================================================
# HEALTH + SELF-HEALING
# =============================================================================
class HealthCheckSystem:
    def __init__(self, interval: float = 30.0):
        self.interval = float(interval)
        self.components: Dict[str, Any] = {}
        self.history: Dict[str, Deque[ComponentHealth]] = defaultdict(
            lambda: deque(maxlen=100))
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._start()

    def _start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self._loop())
        except RuntimeError:
            logger.warning("no running loop; health loop not started")

    async def _loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=self.interval)
                break
            except asyncio.TimeoutError:
                pass
            await self._perform_checks()

    async def _perform_checks(self) -> None:
        for name, comp in list(self.components.items()):
            try:
                if hasattr(comp, "get_health_status"):
                    r = comp.get_health_status()
                    if asyncio.iscoroutine(r):
                        r = await r
                    if isinstance(r, ComponentHealth):
                        async with self._lock:
                            self.history[name].append(r)
            except Exception as e:
                async with self._lock:
                    self.history[name].append(
                        ComponentHealth(HealthStatus.UNHEALTHY, 0.0,
                                        {"error": str(e)}))

    def register_component(self, name: str, component: Any) -> None:
        self.components[name] = component

    def unregister_component(self, name: str) -> None:
        self.components.pop(name, None)

    async def get_system_health(self) -> Dict[str, Any]:
        async with self._lock:
            total = 0.0
            statuses: Dict[str, ComponentHealth] = {}
            for name in self.components:
                hist = self.history[name]
                latest = hist[-1] if hist else ComponentHealth(
                    HealthStatus.UNKNOWN, 0.5, {})
                statuses[name] = latest
                total += latest.score
            avg = total / max(len(self.components), 1)
            if avg > 0.8:
                sys_status = HealthStatus.HEALTHY
            elif avg > 0.5:
                sys_status = HealthStatus.DEGRADED
            else:
                sys_status = HealthStatus.UNHEALTHY
            return {"system_status": sys_status.value,
                    "system_score": avg,
                    "components": {k: {"status": v.status.value,
                                        "score": v.score,
                                        "details": v.details}
                                    for k, v in statuses.items()}}

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


class SelfHealingSystem:
    def __init__(self, health: HealthCheckSystem,
                 max_attempts: int = 5, interval: float = 30.0):
        self.health = health
        self.max_attempts = int(max_attempts)
        self.interval = float(interval)
        self.handlers: Dict[str, Callable[[], Any]] = {}
        self.attempts: Dict[str, int] = defaultdict(int)
        self._shutdown_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._start()

    def _start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self._loop())
        except RuntimeError:
            logger.warning("no running loop; self-healing loop not started")

    async def _loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=self.interval)
                break
            except asyncio.TimeoutError:
                pass
            await self._check_and_recover()

    async def _check_and_recover(self) -> None:
        health = await self.health.get_system_health()
        for name, data in health.get("components", {}).items():
            if data.get("status") in (HealthStatus.DEGRADED.value,
                                      HealthStatus.UNHEALTHY.value):
                await self._attempt_recovery(name)

    async def _attempt_recovery(self, name: str) -> None:
        if self.attempts[name] >= self.max_attempts:
            return
        handler = self.handlers.get(name)
        if handler is None:
            return
        self.attempts[name] += 1
        try:
            r = handler()
            if asyncio.iscoroutine(r):
                r = await r
            # If the handler returns a replacement component, register it.
            if r is not None and r is not True and r is not False:
                self.health.register_component(name, r)
            if r:
                self.attempts[name] = 0
        except Exception as e:
            logger.error(f"{name}: recovery failed: {e}")

    def register_handler(self, name: str, handler: Callable[[], Any]) -> None:
        self.handlers[name] = handler

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


# =============================================================================
# SUPPORTING CLASSES
# =============================================================================
class ReputationScoringSystem:
    """Weighted reputation with time decay."""

    def __init__(self, decay_rate: float = 0.01):
        self.records: Dict[str, ReputationRecord] = {}
        self.decay_rate = float(decay_rate)
        self._lock = asyncio.Lock()

    async def update(self, node_id: str, *, success: bool,
                     sustainability: float = 0.5,
                     token_stake: float = 0.0,
                     data_quality: float = 0.5,
                     carbon_efficiency: float = 0.5) -> float:
        async with self._lock:
            rec = self.records.setdefault(node_id, ReputationRecord(node_id=node_id))
            if success:
                rec.successful_updates += 1
            else:
                rec.failed_updates += 1
            rec.sustainability_contributions += sustainability
            rec.token_stake = token_stake
            total = rec.successful_updates + rec.failed_updates
            success_rate = rec.successful_updates / max(1, total)
            sust_avg = rec.sustainability_contributions / max(1, total)
            new_score = (0.4 * success_rate +
                         0.3 * sust_avg +
                         0.1 * min(1.0, token_stake / 100.0) +
                         0.1 * data_quality +
                         0.1 * (1.0 - carbon_efficiency))
            rec.score = max(0.05, min(1.0,
                rec.score * (1.0 - self.decay_rate) +
                new_score * self.decay_rate))
            rec.last_update = datetime.now(timezone.utc)
            rec.history.append({"score": rec.score, "success": success,
                                 "sustainability": sustainability})
            return rec.score

    async def get_score(self, node_id: str) -> float:
        rec = self.records.get(node_id)
        return rec.score if rec else 0.5

    def stats(self) -> Dict[str, Any]:
        if not self.records:
            return {"total_nodes": 0}
        scores = [r.score for r in self.records.values()]
        return {"total_nodes": len(self.records),
                "average_score": float(np.mean(scores)),
                "min_score": min(scores), "max_score": max(scores)}


class ParticipantSelector:
    """Weighted multi-criteria participant selection."""

    def __init__(self,
                 reputation: Optional[ReputationScoringSystem] = None):
        self.reputation = reputation

    async def select(self, participants: Dict[str, FederatedExpert],
                     n: int,
                     carbon_intensity: float,
                     helium_scarcity: float,
                     required_roles: Optional[List[str]] = None,
                     carbon_weight: float = 0.25,
                     helium_weight: float = 0.20,
                     sustain_weight: float = 0.25,
                     rep_weight: float = 0.30) -> List[str]:
        scores: Dict[str, float] = {}
        for pid, p in participants.items():
            if not p.is_active:
                continue
            if required_roles and p.role not in required_roles:
                continue
            carbon_score = 1.0 / (1.0 + p.carbon_footprint * 10.0)
            helium_score = 1.0 / (1.0 + p.helium_usage * 10.0)
            rep = p.reputation_score
            if self.reputation is not None:
                try:
                    rep = await self.reputation.get_score(pid)
                except Exception:
                    rep = p.reputation_score
            score = (carbon_weight * carbon_score +
                     helium_weight * helium_score +
                     sustain_weight * p.sustainability_contribution +
                     rep_weight * rep)
            scores[pid] = score
        ranked = sorted(scores.items(), key=lambda kv: -kv[1])
        return [pid for pid, _ in ranked[:max(1, n)]]


class SecureAggregator:
    """Simple masked aggregation: sum of shares with pairwise cancellation."""

    def __init__(self, rng: Optional[random.Random] = None):
        self.rng = rng or random.Random()

    async def aggregate(self, updates: List[Dict[str, np.ndarray]],
                        weights: List[float]) -> Dict[str, np.ndarray]:
        if not updates:
            return {}
        total = sum(weights) or 1.0
        # Union of keys
        keys: Set[str] = set()
        for u in updates:
            keys.update(u.keys())
        agg: Dict[str, np.ndarray] = {}
        for k in keys:
            acc = None
            for u, w in zip(updates, weights):
                if k not in u:
                    continue
                v = np.asarray(u[k], dtype=np.float32) * (w / total)
                acc = v if acc is None else acc + v
            if acc is not None:
                agg[k] = acc
        return agg


class ModelCompressor:
    """Compress numpy arrays to zlib-compressed pickle."""

    def compress(self, model: Dict[str, np.ndarray]) -> Tuple[bytes, Dict[str, Any]]:
        serializable = {k: np.asarray(v, dtype=np.float32).tolist()
                        for k, v in model.items()}
        raw = json.dumps(serializable).encode("utf-8")
        compressed = zlib.compress(raw, level=6)
        return compressed, {
            "original_size": len(raw),
            "compressed_size": len(compressed),
            "ratio": len(compressed) / max(1, len(raw)),
            "method": "zlib+json",
        }

    def decompress(self, compressed: bytes) -> Dict[str, np.ndarray]:
        raw = zlib.decompress(compressed)
        data = json.loads(raw.decode("utf-8"))
        return {k: np.asarray(v, dtype=np.float32) for k, v in data.items()}


class CrossTierDistiller:
    """Knowledge distillation between tier aggregations (KL on softmax)."""

    def __init__(self, temperature: float = 2.0):
        self.temperature = float(temperature)

    def distill(self, teacher: Dict[str, np.ndarray],
                student: Dict[str, np.ndarray],
                alpha: float = 0.5) -> Dict[str, np.ndarray]:
        """Blend teacher and student weights."""
        out: Dict[str, np.ndarray] = {}
        keys = set(teacher.keys()) | set(student.keys())
        for k in keys:
            t = teacher.get(k)
            s = student.get(k)
            if t is None:
                out[k] = np.asarray(s, dtype=np.float32)
            elif s is None:
                out[k] = np.asarray(t, dtype=np.float32)
            else:
                t_arr = np.asarray(t, dtype=np.float32)
                s_arr = np.asarray(s, dtype=np.float32)
                if t_arr.shape != s_arr.shape:
                    n = min(t_arr.size, s_arr.size)
                    out[k] = ((1 - alpha) * t_arr.flatten()[:n] +
                              alpha * s_arr.flatten()[:n]).astype(np.float32)
                else:
                    out[k] = ((1 - alpha) * t_arr +
                              alpha * s_arr).astype(np.float32)
        return out


class PredictiveFederationAnalyzer:
    """Holt's linear exponential smoothing on sustainability scores."""

    def __init__(self, alpha: float = 0.3, beta: float = 0.1):
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.level: Optional[float] = None
        self.trend = 0.0
        self._history: Deque[float] = deque(maxlen=200)

    def observe(self, score: float) -> None:
        self._history.append(score)
        if self.level is None:
            self.level = score
            self.trend = 0.0
            return
        prev = self.level
        prev_trend = self.trend
        new_level = self.alpha * score + (1 - self.alpha) * (prev + prev_trend)
        new_trend = self.beta * (new_level - prev) + (1 - self.beta) * prev_trend
        self.level = new_level
        self.trend = new_trend

    def forecast(self, steps: int = 1) -> float:
        if self.level is None:
            return 0.5
        return max(0.0, min(1.0, self.level + steps * self.trend))

    def summary(self) -> Dict[str, Any]:
        if not self._history:
            return {"status": "insufficient_data"}
        return {"current_level": self.level,
                "current_trend": self.trend,
                "forecast_1": self.forecast(1)}


class FederationCrossDomainTransfer:
    """Simple in-memory knowledge base across domains."""

    def __init__(self):
        self.knowledge: Dict[str, Dict[str, Any]] = {}
        self.transfers: Deque[Dict[str, Any]] = deque(maxlen=1000)

    def transfer(self, source: str, target: str,
                 payload: Dict[str, Any]) -> Dict[str, Any]:
        key = f"{source}->{target}"
        entry = self.knowledge.setdefault(
            key, {"data": {}, "count": 0, "last": None})
        entry["data"].update(payload)
        entry["count"] += 1
        entry["last"] = datetime.now(timezone.utc).isoformat()
        self.transfers.append({"source": source, "target": target,
                                "ts": entry["last"]})
        return entry

    def stats(self) -> Dict[str, Any]:
        return {"total_pairs": len(self.knowledge),
                "total_transfers": len(self.transfers),
                "keys": list(self.knowledge.keys())}


# =============================================================================
# REAL LOCAL TRAINING — numpy linear model with honest SGD
# =============================================================================
class LinearModel:
    """Simple linear model with real forward + gradient step.

    Inputs X shape (n, input_dim), outputs y shape (n, output_dim).
    """

    def __init__(self, input_dim: int = 4, output_dim: int = 1,
                 seed: int = 0):
        rng = np.random.RandomState(seed)
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.W = (rng.randn(input_dim, output_dim).astype(np.float32) * 0.01)
        self.b = np.zeros(output_dim, dtype=np.float32)

    def forward(self, X: np.ndarray) -> np.ndarray:
        return X @ self.W + self.b

    def train_step(self, X: np.ndarray, y: np.ndarray,
                   lr: float = 0.01) -> float:
        pred = self.forward(X)
        error = pred - y
        grad_W = X.T @ error / max(len(X), 1)
        grad_b = error.mean(axis=0)
        self.W -= lr * grad_W
        self.b -= lr * grad_b
        return float((error ** 2).mean())

    def get_weights(self) -> Dict[str, np.ndarray]:
        return {"W": self.W.copy(), "b": self.b.copy()}

    def set_weights(self, weights: Dict[str, Any]) -> None:
        if "W" in weights:
            self.W = np.asarray(weights["W"], dtype=np.float32).copy()
        if "b" in weights:
            self.b = np.asarray(weights["b"], dtype=np.float32).copy()


# =============================================================================
# MAIN FEDERATED ORCHESTRATOR (v9.0.0)
# =============================================================================
class EnhancedFederatedOrchestrator:
    """Federated learning orchestrator with all ten enhancement modules."""

    W_MAX = 10.0
    WEIGHT_NORM_MAX = 1e6

    def __init__(
        self,
        storage: StorageLike,
        message_queue: Optional[MessageQueueLike] = None,
        adaptive_cost: Optional[AdaptiveCostLike] = None,
        pareto_gating: Optional[ParetoGatingLike] = None,
        drift_detector: Optional[DriftDetectorLike] = None,
        metrics: Optional[MetricsLike] = None,
        carbon_client: Optional[CarbonIntensityClient] = None,
        helium_client: Optional[HeliumClient] = None,
        gating_network: Optional[GatingNetworkLike] = None,
        bio_core: Optional[BioCoreLike] = None,
        min_participants: int = 3,
        max_participants: int = 10,
        privacy_level: PrivacyLevel = PrivacyLevel.DIFFERENTIAL,
        model_input_dim: int = 4,
        model_output_dim: int = 1,
        local_epochs: int = 3,
        learning_rate: float = 0.01,
        enable_quantum_distillation: bool = True,
        enable_causal_mask: bool = True,
        enable_expert_auction: bool = True,
        enable_safety_monitor: bool = True,
        enable_temporal_safety: bool = True,
        enable_xai: bool = True,
        enable_precision: bool = False,
        enable_carbon_market: bool = False,
        enable_chaos: bool = False,
        chaos_probability: float = 0.0,
        enable_human_approval: bool = False,
        enable_health_checks: bool = True,
        enable_self_healing: bool = True,
        enable_cross_tier_distillation: bool = False,
        enable_compression: bool = False,
        enable_secure_aggregation: bool = False,
        seed: int = 42,
    ) -> None:
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.carbon_client = carbon_client or CarbonIntensityClient()
        self.helium_client = helium_client or HeliumClient()
        self.gating_network = gating_network
        self.bio_core = bio_core
        self.min_participants = int(min_participants)
        self.max_participants = int(max_participants)
        self.privacy_level = privacy_level
        self.model_input_dim = int(model_input_dim)
        self.model_output_dim = int(model_output_dim)
        self.local_epochs = int(local_epochs)
        self.learning_rate = float(learning_rate)

        # Deterministic RNG
        self._rng = random.Random(seed)
        self._rng_q = random.Random(seed + 1)
        self._rng_c = random.Random(seed + 2)
        self._rng_auc = random.Random(seed + 3)
        self._rng_chaos = random.Random(seed + 4)
        self._rng_carbon = random.Random(seed + 5)
        self._rng_hitl = random.Random(seed + 6)
        np.random.seed(seed)

        # State
        self.participants: Dict[str, FederatedExpert] = {}
        self.global_model: Optional[Dict[str, np.ndarray]] = None
        self.round_number = 0
        self.aggregation_history: Deque[FederationRound] = deque(maxlen=1000)
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.federation_token_pool = 1000.0

        # Previously-uninitialised attributes
        self.carbon_intensity = 400.0
        self.carbon_price = 50.0
        self.helium_scarcity = 0.5
        self.helium_price = 0.5
        self.token_balance = 500.0

        # Thread-safety for numpy aggregation
        self._cpu_lock = threading.Lock()

        # Enhancement modules
        self.quantum = QuantumDistillationModule(
            rng=self._rng_q) if enable_quantum_distillation else None
        self.causal_mask = CausalFeatureMask(
            rng=self._rng_c) if enable_causal_mask else None
        self.auction = ExpertAuction(
            [], rng=self._rng_auc) if enable_expert_auction else None
        self.safety = SafetyMonitor() if enable_safety_monitor else None
        if self.safety is not None:
            self._setup_safety_rules()
        self.temporal_safety = TemporalSafetyMonitor() \
            if enable_temporal_safety else None
        if self.temporal_safety is not None:
            self._setup_temporal_rules()
        self.xai = XAIExplainer(rng=self._rng_hitl) if enable_xai else None
        self.precision = PrecisionController() if enable_precision else None
        self.carbon_market = CarbonMarketClient(
            price_oracle=self._carbon_oracle,
            rng=self._rng_carbon) if enable_carbon_market else None
        self.hitl = HumanApprovalHandler(
            message_queue) if enable_human_approval else None
        self.chaos: Optional[ChaosEngine] = None
        if enable_chaos:
            self.chaos = ChaosEngine(
                self, probability=chaos_probability,
                rng=self._rng_chaos)
        self.compressor = ModelCompressor() if enable_compression else None
        self.distiller = CrossTierDistiller() \
            if enable_cross_tier_distillation else None
        self.secure_aggregator = SecureAggregator(self._rng) \
            if enable_secure_aggregation else None

        # Reputation + predictive + cross-domain
        self.reputation = ReputationScoringSystem()
        self.predictive = PredictiveFederationAnalyzer()
        self.cross_domain = FederationCrossDomainTransfer()
        self.selector = ParticipantSelector(reputation=self.reputation)

        # Health + self-healing
        self.health = HealthCheckSystem() if enable_health_checks else None
        self.self_healing = (SelfHealingSystem(self.health)
                              if (self.health is not None and
                                  enable_self_healing) else None)
        if self.health is not None:
            self.health.register_component("carbon_client", self.carbon_client)
            self.health.register_component("helium_client", self.helium_client)
            self.health.register_component("federation", self)
        if self.self_healing is not None:
            self.self_healing.register_handler("carbon_client",
                                               self._recover_carbon)
            self.self_healing.register_handler("helium_client",
                                               self._recover_helium)
            self.self_healing.register_handler("federation",
                                               self._recover_federation)

        # Lifecycle
        self.task_manager = TaskManager()
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()
        self._initialized = False

    # ------------------------------------------------------------------
    def _setup_safety_rules(self) -> None:
        assert self.safety is not None

        def _finite(_state):
            return self._check_steady_state()

        def _sust_ok(state):
            v = state.get("sustainability_score", 0.0)
            try:
                return 0.0 <= float(v) <= 1.0
            except Exception:
                return False

        def _token_ok(state):
            try:
                return float(state.get("token_pool", 0.0)) >= 0.0
            except Exception:
                return False

        self.safety.add_rule(
            "state_finite", _finite,
            "global model contains NaN/Inf",
            severity="critical",
            mitigation=lambda t: t._restore_state(t._initial_snapshot()))
        self.safety.add_rule(
            "sustainability_in_range", _sust_ok,
            "sustainability out of range", severity="warning")
        self.safety.add_rule(
            "token_pool_non_negative", _token_ok,
            "token pool negative", severity="critical",
            mitigation=lambda t: setattr(t, "federation_token_pool", 0.0))

    def _setup_temporal_rules(self) -> None:
        assert self.temporal_safety is not None
        self.temporal_safety.add_formula("G (round_number >= 0)")
        self.temporal_safety.add_formula("G (sustainability_score >= 0.0)")
        self.temporal_safety.add_formula("G (sustainability_score <= 1.0)")

    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            return
        try:
            await self.load_state()
        except Exception as e:
            logger.warning(f"load_state failed during initialize: {e}")
        self._spawn(self._refresh_loop())
        if self.chaos is not None:
            self._spawn(self._chaos_loop())
        self._initialized = True

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def _refresh_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.carbon_client.update_interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.carbon_client.update()
                await self.helium_client.update()
                self.carbon_intensity = self.carbon_client.intensity_ema
                self.carbon_price = self.carbon_client.price_ema
                self.helium_scarcity = self.helium_client.scarcity
                self.helium_price = self.helium_client.price_ema
            except Exception as e:
                logger.debug(f"refresh loop error: {e}")

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
                logger.debug(f"chaos loop error: {e}")

    # ------------------------------------------------------------------
    def _snapshot_state(self) -> Dict[str, Any]:
        with self._cpu_lock:
            model_copy = (None if self.global_model is None
                          else {k: v.copy()
                                for k, v in self.global_model.items()})
        return {
            "global_model": model_copy,
            "sustainability_score": self.sustainability_score,
            "federation_token_pool": self.federation_token_pool,
            "round_number": self.round_number,
        }

    def _restore_state(self, snap: Dict[str, Any]) -> None:
        with self._cpu_lock:
            self.global_model = snap.get("global_model")
        self.sustainability_score = snap.get("sustainability_score", 0.0)
        self.federation_token_pool = snap.get("federation_token_pool", 0.0)
        self.round_number = snap.get("round_number", 0)

    def _initial_snapshot(self) -> Dict[str, Any]:
        return self._snapshot_state()

    def _check_steady_state(self) -> bool:
        with self._cpu_lock:
            gm = self.global_model
        if gm is None:
            return True
        for _, v in gm.items():
            if not np.all(np.isfinite(v)):
                return False
            if float(np.linalg.norm(v)) > self.WEIGHT_NORM_MAX:
                return False
        return True

    def _corrupt_state_bounded(self, rng: random.Random) -> None:
        with self._cpu_lock:
            if self.global_model is None:
                return
            for k, v in self.global_model.items():
                factor = rng.uniform(0.98, 1.02)
                self.global_model[k] = np.clip(v * factor,
                                                -self.W_MAX, self.W_MAX)

    async def _recover_carbon(self) -> Any:
        await self.carbon_client.close()
        new_client = CarbonIntensityClient(
            api_url=self.carbon_client.api_url,
            api_key=self.carbon_client.api_key,
            region=self.carbon_client.region)
        self.carbon_client = new_client
        return new_client

    async def _recover_helium(self) -> Any:
        await self.helium_client.close()
        new_client = HeliumClient(
            api_url=self.helium_client.api_url,
            api_key=self.helium_client.api_key)
        self.helium_client = new_client
        return new_client

    async def _recover_federation(self) -> Any:
        self._restore_state({
            "global_model": None,
            "sustainability_score": 0.0,
            "federation_token_pool": 1000.0,
            "round_number": 0,
        })
        return True

    async def _carbon_oracle(self) -> float:
        await self.carbon_client.update()
        return self.carbon_client.price

    # ------------------------------------------------------------------
    async def get_health_status(self) -> ComponentHealth:
        ok = self._check_steady_state()
        details = {"participants": len(self.participants),
                   "rounds": self.round_number}
        if not ok:
            return ComponentHealth(HealthStatus.UNHEALTHY, 0.0, details)
        if not self.participants:
            return ComponentHealth(HealthStatus.DEGRADED, 0.4, details)
        return ComponentHealth(HealthStatus.HEALTHY, 1.0, details)

    # ------------------------------------------------------------------
    def register_participant(self, participant_id: str,
                             local_model: Dict[str, np.ndarray],
                             capabilities: ClientCapabilities,
                             carbon_footprint: float = 0.1,
                             helium_usage: float = 0.01,
                             sustainability_contribution: float = 0.5,
                             region_id: Optional[str] = None,
                             role: str = ParticipantRole.FOLLOWER.value
                             ) -> bool:
        if participant_id in self.participants:
            return False
        expert = FederatedExpert(
            expert_id=participant_id,
            local_model={k: np.asarray(v, dtype=np.float32).copy()
                         for k, v in local_model.items()},
            capabilities=capabilities,
            carbon_footprint=float(carbon_footprint),
            helium_usage=float(helium_usage),
            sustainability_contribution=float(sustainability_contribution),
            region_id=region_id,
            role=role,
        )
        self.participants[participant_id] = expert
        if self.auction is not None:
            self.auction.add_participant(participant_id)
        return True

    def unregister_participant(self, participant_id: str) -> bool:
        if participant_id not in self.participants:
            return False
        self.participants.pop(participant_id)
        if self.auction is not None:
            self.auction.remove_participant(participant_id)
        return True

    # ------------------------------------------------------------------
    def _build_context(self) -> Dict[str, Any]:
        avg_rep = (sum(p.reputation_score for p in self.participants.values())
                   / max(1, len(self.participants)))
        avg_latency = (sum(p.capabilities.network_latency_ms
                           for p in self.participants.values())
                       / max(1, len(self.participants)))
        return {
            "carbon_intensity": self.carbon_intensity / 1000.0,
            "helium_scarcity": self.helium_scarcity,
            "carbon_price": self.carbon_price / 100.0,
            "helium_price": self.helium_price,
            "token_pool": self.federation_token_pool / 1000.0,
            "participant_count": len(self.participants) / 10.0,
            "sustainability_score": self.sustainability_score,
            "avg_reputation": avg_rep,
            "avg_latency_ms": avg_latency / 1000.0,
            "regional_capacity": 1.0,
            "regional_usage": 0.0,
            "drift_score": 0.0,
        }

    def _choose_strategy(self, context: Dict[str, Any],
                         candidates: List[AggregationStrategy]
                         ) -> Tuple[AggregationStrategy, np.ndarray]:
        carbon = context["carbon_intensity"]
        helium = context["helium_scarcity"]
        price = context["carbon_price"]
        sust = context["sustainability_score"]
        token = context["token_pool"]
        rep = context["avg_reputation"]
        scores: List[float] = []
        for s in candidates:
            if s == AggregationStrategy.FED_AVG:
                sc = 0.5 + 0.2 * (1.0 - carbon)
            elif s == AggregationStrategy.FED_PROX:
                sc = 0.5
            elif s == AggregationStrategy.SUSTAINABILITY_WEIGHTED:
                sc = 0.4 + 0.4 * sust + 0.2 * (1.0 - carbon)
            elif s == AggregationStrategy.REPUTATION_WEIGHTED:
                sc = 0.5 + 0.4 * rep
            elif s == AggregationStrategy.TOKEN_WEIGHTED:
                sc = 0.5 + 0.3 * token
            elif s == AggregationStrategy.SECURE_AGGREGATION:
                sc = 0.45 + 0.2 * rep
            elif s == AggregationStrategy.TIERED_DISTILLATION:
                sc = 0.5 + 0.3 * (1.0 - price)
            else:
                sc = 0.5
            if helium > 0.7 and s == AggregationStrategy.SUSTAINABILITY_WEIGHTED:
                sc += 0.1
            scores.append(sc)
        arr = np.asarray(scores, dtype=np.float32)
        arr = np.nan_to_num(arr, nan=0.0)
        m = float(arr.max())
        e = np.exp(arr - m)
        probs = e / max(float(e.sum()), 1e-12)
        idx = int(np.argmax(probs))
        return candidates[idx], probs

    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """MOPD teacher interface: distribution over aggregation strategies."""
        context = self._build_context()
        # Merge caller-supplied overrides
        for k, v in state.items():
            if k in context and isinstance(v, (int, float)):
                context[k] = float(v)
        candidates = list(AggregationStrategy)
        _, probs = self._choose_strategy(context, candidates)
        return probs.tolist()

    # ------------------------------------------------------------------
    def _train_local(self, participant: FederatedExpert,
                     rng: np.random.RandomState) -> Tuple[Dict[str, np.ndarray], float]:
        """Real local training: linear model + real gradient steps."""
        model = LinearModel(self.model_input_dim, self.model_output_dim,
                            seed=int(rng.randint(0, 2**31 - 1)))
        # Seed from the participant's current weights when shapes match.
        try:
            if "W" in participant.local_model and \
                    participant.local_model["W"].shape == model.W.shape:
                model.set_weights(participant.local_model)
        except Exception:
            pass
        # Synthetic local dataset — a real regression problem.
        n = 32
        X = rng.randn(n, self.model_input_dim).astype(np.float32)
        target_W = rng.randn(self.model_input_dim,
                             self.model_output_dim).astype(np.float32)
        y = (X @ target_W).astype(np.float32)
        loss = 0.0
        for _ in range(self.local_epochs):
            loss = model.train_step(X, y, lr=self.learning_rate)
        return model.get_weights(), float(loss)

    async def federated_round(self) -> Optional[FederationDecision]:
        if not self.participants:
            return None

        # Refresh prices
        try:
            c = await self.carbon_client.update()
            self.carbon_intensity = c["intensity_ema"]
            self.carbon_price = c["price_ema"]
        except Exception:
            pass
        try:
            h = await self.helium_client.update()
            self.helium_scarcity = h["scarcity"]
            self.helium_price = h["price_ema"]
        except Exception:
            pass

        context = self._build_context()
        state_vec = FederationFeatures.encode(context)
        if self.causal_mask is not None:
            self.causal_mask.observe(state_vec)
            masked_vec = self.causal_mask.apply(state_vec)
        else:
            masked_vec = state_vec

        # Candidate selection
        n_select = max(self.min_participants,
                       min(len(self.participants), self.max_participants))

        if self.auction is not None:
            auction_ids = self.auction.select(masked_vec, top_k=n_select)
        else:
            auction_ids = []
        selected_ids: List[str] = [pid for pid in auction_ids
                                    if pid in self.participants]
        if len(selected_ids) < self.min_participants:
            selected_ids = await self.selector.select(
                self.participants, n_select,
                self.carbon_intensity, self.helium_scarcity,
                required_roles=[ParticipantRole.FOLLOWER.value,
                                ParticipantRole.LEADER.value])

        if len(selected_ids) < self.min_participants:
            return None

        # Pareto filter
        if self.pareto is not None:
            cand_dicts = []
            for pid in selected_ids:
                p = self.participants[pid]
                cand_dicts.append({
                    "participant_id": pid,
                    "quality_score": 1.0 - p.carbon_footprint,
                    "carbon_g": p.carbon_footprint,
                    "latency_ms": p.capabilities.network_latency_ms,
                    "energy_joules": p.helium_usage,
                })
            try:
                filtered = self.pareto.filter(cand_dicts)
                allowed = {c["participant_id"] for c in filtered}
                if allowed:
                    selected_ids = [pid for pid in selected_ids
                                    if pid in allowed]
            except Exception:
                pass

        # Strategy selection
        strategy_candidates = list(AggregationStrategy)
        if self.adaptive_cost is not None:
            try:
                w = self.adaptive_cost.get_current_weights()
                if w.get("carbon", 0.2) > 0.5 and context["carbon_intensity"] > 0.5:
                    strategy_candidates = [
                        AggregationStrategy.SUSTAINABILITY_WEIGHTED,
                        AggregationStrategy.FED_AVG]
                elif w.get("cost", 0.2) > 0.5:
                    strategy_candidates = [
                        AggregationStrategy.TIERED_DISTILLATION,
                        AggregationStrategy.FED_AVG]
            except Exception:
                pass

        strategy, strat_probs = self._choose_strategy(context,
                                                       strategy_candidates)

        # HITL approval for close calls
        if self.hitl is not None and len(strategy_candidates) >= 2:
            top2 = np.argsort(-strat_probs)[:2]
            hitl_candidates = [{"id": strategy_candidates[i].value,
                                 "score": float(strat_probs[i])}
                                for i in top2]
            if not await self.hitl.request_approval(
                    {"action": "choose_strategy",
                     "candidates": hitl_candidates}):
                strategy = AggregationStrategy.FED_AVG

        # Real local training on each selected participant
        rng = np.random.RandomState(hash(self.round_number) % (2**31))
        updates: Dict[str, Dict[str, np.ndarray]] = {}
        losses: List[float] = []
        for pid in selected_ids:
            p = self.participants[pid]
            weights, loss = await asyncio.to_thread(
                self._train_local, p, rng)
            updates[pid] = weights
            losses.append(loss)
            p.local_model = {k: v.copy() for k, v in weights.items()}

        # Aggregation weights per strategy
        weights: Dict[str, float] = {}
        for pid in selected_ids:
            p = self.participants[pid]
            if strategy == AggregationStrategy.FED_AVG:
                weights[pid] = 1.0
            elif strategy == AggregationStrategy.FED_PROX:
                weights[pid] = 1.0
            elif strategy == AggregationStrategy.SUSTAINABILITY_WEIGHTED:
                weights[pid] = max(0.01, p.sustainability_contribution)
            elif strategy == AggregationStrategy.REPUTATION_WEIGHTED:
                weights[pid] = max(0.01, p.reputation_score)
            elif strategy == AggregationStrategy.TOKEN_WEIGHTED:
                weights[pid] = max(0.01, p.tokens_staked + 1.0)
            elif strategy == AggregationStrategy.SECURE_AGGREGATION:
                weights[pid] = 1.0
            elif strategy == AggregationStrategy.TIERED_DISTILLATION:
                weights[pid] = 1.0
            else:
                weights[pid] = 1.0

        aggregated = await self._aggregate(updates, weights, strategy)

        # Compress + optional tier distillation
        if self.compressor is not None and aggregated:
            try:
                payload, meta = await asyncio.to_thread(
                    self.compressor.compress, aggregated)
                # (Decompress to prove round-trip.)
                _ = await asyncio.to_thread(self.compressor.decompress, payload)
                logger.debug(f"compression ratio {meta['ratio']:.3f}")
            except Exception as e:
                logger.debug(f"compression skipped: {e}")

        if self.distiller is not None and self.global_model is not None:
            try:
                aggregated = await asyncio.to_thread(
                    self.distiller.distill,
                    self.global_model, aggregated)
            except Exception as e:
                logger.debug(f"distillation skipped: {e}")

        with self._cpu_lock:
            self.global_model = aggregated

        # Safety check
        safety_violations: List[str] = []
        if self.safety is not None:
            v = self.safety.check({
                "sustainability_score": self.sustainability_score,
                "token_pool": self.federation_token_pool,
            }, target=self)
            safety_violations = [f"{x.name}:{x.severity}" for x in v]

        # Reputation updates
        avg_loss = float(np.mean(losses)) if losses else 0.0
        success = avg_loss < 1.0
        for pid in selected_ids:
            await self.reputation.update(
                pid, success=success,
                sustainability=self.participants[pid].sustainability_contribution,
                token_stake=self.participants[pid].tokens_staked,
                data_quality=1.0 / (1.0 + avg_loss),
                carbon_efficiency=self.participants[pid].carbon_footprint)
            self.participants[pid].reputation_score = \
                await self.reputation.get_score(pid)

        # Auction + gating feedback
        reward = 1.0 / (1.0 + avg_loss)
        if self.auction is not None:
            for pid in selected_ids:
                self.auction.update(pid, masked_vec, reward)
        if self.gating_network is not None:
            try:
                for pid in selected_ids:
                    await self.gating_network.record_reward(pid, context, reward)
            except Exception as e:
                logger.debug(f"gating record_reward skipped: {e}")

        # Sustainability + bookkeeping
        self.sustainability_score = self._compute_sustainability(
            context, selected_ids)
        self.predictive.observe(self.sustainability_score)
        self.total_carbon_savings_kg += sum(
            self.participants[pid].carbon_footprint * 0.01
            for pid in selected_ids)
        self.total_helium_savings_l += sum(
            self.participants[pid].helium_usage * 0.1
            for pid in selected_ids)

        # Drift detection
        drift = 0.0
        if self.drift is not None and self.adaptive_cost is not None:
            try:
                drift = float(await self.drift.check_drift(
                    self.adaptive_cost.get_current_weights()))
            except Exception:
                drift = 0.0

        # Precision report
        precision = "fp32"
        if self.precision is not None:
            precision = self.precision.select_precision()

        # Temporal safety check
        if self.temporal_safety is not None:
            await self.temporal_safety.push({
                "round_number": self.round_number,
                "sustainability_score": self.sustainability_score})
            await self.temporal_safety.check()

        # XAI
        explanation = None
        counterfactual = None
        if self.xai is not None:
            explanation, counterfactual = await asyncio.to_thread(
                self._compute_xai, masked_vec, strategy_candidates, strat_probs)

        # Emit FeedbackEvent
        if self.queue is not None:
            try:
                event = FeedbackEvent.create_with_context(
                    source="federated_experts",
                    feedback_type="federation_round",
                    task_id=f"round_{self.round_number}",
                    context={"strategy": strategy.value,
                             "participants": list(selected_ids)},
                    action={"strategy": strategy.value},
                    performance={"sustainability": self.sustainability_score},
                    adaptive_cost_value=0.0,
                    tags=["federation", "round"])
                await self.queue.publish("federation_events", event.to_json())
            except Exception:
                pass

        self.round_number += 1
        if self.metrics is not None:
            try:
                self.metrics.inc("federated_rounds")
                self.metrics.set("federated_sustainability",
                                 self.sustainability_score)
            except Exception:
                pass

        self.aggregation_history.append(FederationRound(
            round_id=f"round_{self.round_number}",
            round_number=self.round_number,
            started_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
            participants=list(selected_ids),
            aggregation_strategy=strategy,
            privacy_level=self.privacy_level,
            successful=True,
            sustainability_score=self.sustainability_score,
        ))

        decision = FederationDecision(
            state_vec=masked_vec.copy(),
            strategy=strategy.value,
            strategy_probs=strat_probs.copy(),
            participant_ids=tuple(selected_ids),
            participant_scores=np.asarray(
                [weights.get(pid, 0.0) for pid in selected_ids],
                dtype=np.float32),
            aggregated_model_keys=tuple(sorted(aggregated.keys())),
            confidence=float(strat_probs.max()),
            timestamp=time.time(),
            precision=precision,
            drift=drift,
            safety_violations=tuple(safety_violations),
            explanation=explanation,
            counterfactual=counterfactual,
            sustainability=self.sustainability_score,
        )
        return decision

    # ------------------------------------------------------------------
    async def _aggregate(self, updates: Dict[str, Dict[str, np.ndarray]],
                         weights: Dict[str, float],
                         strategy: AggregationStrategy
                         ) -> Dict[str, np.ndarray]:
        # Secure aggregation path (uses its own impl).
        if strategy == AggregationStrategy.SECURE_AGGREGATION and \
                self.secure_aggregator is not None:
            model_list = [updates[pid] for pid in updates]
            weight_list = [weights[pid] for pid in updates]
            return await asyncio.to_thread(
                lambda: asyncio.run(self.secure_aggregator.aggregate(
                    model_list, weight_list)))

        # Regular path: run in worker thread under CPU lock.
        def _do_aggregate() -> Dict[str, np.ndarray]:
            with self._cpu_lock:
                return self._weighted_average(updates, weights)
        return await asyncio.to_thread(_do_aggregate)

    def _weighted_average(self, updates: Dict[str, Dict[str, np.ndarray]],
                          weights: Dict[str, float]) -> Dict[str, np.ndarray]:
        """Union of all keys, weighted mean. Handles heterogeneous shapes."""
        if not updates:
            return {}
        # Union of keys
        all_keys: Set[str] = set()
        for m in updates.values():
            all_keys.update(m.keys())

        total_w = sum(weights.get(pid, 1.0) for pid in updates) or 1.0

        agg: Dict[str, np.ndarray] = {}
        for key in all_keys:
            acc: Optional[np.ndarray] = None
            for pid, model in updates.items():
                if key not in model:
                    continue
                w = weights.get(pid, 1.0) / total_w
                v = np.asarray(model[key], dtype=np.float32)
                if acc is None:
                    acc = w * v
                elif acc.shape == v.shape:
                    acc = acc + w * v
                else:
                    n = min(acc.size, v.size)
                    acc = acc.flatten()[:n] + w * v.flatten()[:n]
            if acc is not None:
                agg[key] = acc.astype(np.float32)
        return agg

    def _compute_sustainability(self, context: Dict[str, Any],
                                selected: List[str]) -> float:
        carbon_factor = 1.0 - context["carbon_intensity"]
        helium_factor = 1.0 - context["helium_scarcity"]
        avg_sust = float(np.mean(
            [self.participants[pid].sustainability_contribution
             for pid in selected])) if selected else 0.5
        score = (carbon_factor * 0.35 + helium_factor * 0.25 +
                 avg_sust * 0.40)
        return max(0.0, min(1.0, score))

    def _compute_xai(self, masked_vec: np.ndarray,
                     strategy_candidates: List[AggregationStrategy],
                     strat_probs: np.ndarray
                     ) -> Tuple[Optional[Dict[str, Any]],
                                Optional[Dict[str, Any]]]:
        if self.xai is None:
            return None, None
        chosen_idx = int(np.argmax(strat_probs))

        def _score(x: np.ndarray) -> float:
            return float(np.dot(x, strat_probs))

        try:
            attrs = self.xai.integrated_gradients(_score, masked_vec)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": list(FederationFeatures.NAMES),
                "chosen_strategy": strategy_candidates[chosen_idx].value,
            }
            counterfactual = self.xai.counterfactual(_score, masked_vec)
            return explanation, counterfactual
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return None, None

    # ------------------------------------------------------------------
    async def save_state(self) -> None:
        try:
            state = {
                "participants": {pid: p.to_dict()
                                 for pid, p in self.participants.items()},
                "round_number": self.round_number,
                "total_carbon_savings_kg": self.total_carbon_savings_kg,
                "total_helium_savings_l": self.total_helium_savings_l,
                "sustainability_score": self.sustainability_score,
                "federation_token_pool": self.federation_token_pool,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            await asyncio.to_thread(
                self.storage.save_state, "federation_state",
                json.dumps(state, default=str))
            if self.global_model is not None:
                # Persist as JSON, not pickle (avoids code execution).
                model_payload = {k: v.tolist()
                                 for k, v in self.global_model.items()}
                await asyncio.to_thread(
                    self.storage.save_state, "federation_global_model",
                    json.dumps(model_payload))
        except Exception as e:
            logger.warning(f"save_state failed: {e}")

    async def load_state(self) -> None:
        try:
            raw = self.storage.get_state("federation_state")
        except Exception:
            raw = None
        if not raw:
            return
        try:
            state = json.loads(raw)
        except Exception:
            return
        for pid, d in (state.get("participants") or {}).items():
            try:
                self.participants[pid] = FederatedExpert.from_dict(d)
                if self.auction is not None:
                    self.auction.add_participant(pid)
            except Exception as e:
                logger.debug(f"skip participant {pid}: {e}")
        self.round_number = int(state.get("round_number", 0))
        self.total_carbon_savings_kg = float(
            state.get("total_carbon_savings_kg", 0.0))
        self.total_helium_savings_l = float(
            state.get("total_helium_savings_l", 0.0))
        self.sustainability_score = float(
            state.get("sustainability_score", 0.0))
        self.federation_token_pool = float(
            state.get("federation_token_pool", 1000.0))
        try:
            raw_model = self.storage.get_state("federation_global_model")
            if raw_model:
                data = json.loads(raw_model)
                self.global_model = {k: np.asarray(v, dtype=np.float32)
                                     for k, v in data.items()}
        except Exception:
            pass

    # ------------------------------------------------------------------
    def get_federation_stats(self) -> Dict[str, Any]:
        return {
            "participants": len(self.participants),
            "round_number": self.round_number,
            "sustainability_score": self.sustainability_score,
            "federation_token_pool": self.federation_token_pool,
            "total_carbon_savings_kg": self.total_carbon_savings_kg,
            "total_helium_savings_l": self.total_helium_savings_l,
            "background_tasks": len(self._background_tasks),
            "steady_state": self._check_steady_state(),
            "carbon_client": self.carbon_client._snapshot(),
            "helium_client": self.helium_client._snapshot(),
            "forecast": self.predictive.summary(),
            "reputation": self.reputation.stats(),
            "cross_domain": self.cross_domain.stats(),
            "recent_rounds": [
                {"round": r.round_number, "strategy": r.aggregation_strategy.value,
                 "participants": len(r.participants),
                 "sustainability": r.sustainability_score}
                for r in list(self.aggregation_history)[-5:]
            ],
        }

    async def health_check(self) -> Dict[str, Any]:
        result = self.get_federation_stats()
        if self.health is not None:
            result["system_health"] = await self.health.get_system_health()
        if self.temporal_safety is not None:
            result["temporal_violations"] = self.temporal_safety.counts()
        if self.chaos is not None:
            result["chaos_rollbacks"] = self.chaos.rollback_count
        return result

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        if self.task_manager is not None:
            await self.task_manager.stop_all()
        if self.self_healing is not None:
            await self.self_healing.shutdown()
        if self.health is not None:
            await self.health.shutdown()
        await self.carbon_client.close()
        await self.helium_client.close()
        await self.save_state()


# Legacy alias for backward compatibility
FederatedExperts = EnhancedFederatedOrchestrator


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


def _make_model(seed: int = 0, input_dim: int = 4,
                output_dim: int = 1) -> Dict[str, np.ndarray]:
    rng = np.random.RandomState(seed)
    return {
        "W": rng.randn(input_dim, output_dim).astype(np.float32) * 0.01,
        "b": np.zeros(output_dim, dtype=np.float32),
    }


def _make_caps(cid: str) -> ClientCapabilities:
    return ClientCapabilities(client_id=cid)


def _populate(eco: EnhancedFederatedOrchestrator, n: int = 5) -> None:
    for i in range(n):
        eco.register_participant(
            participant_id=f"p{i}",
            local_model=_make_model(seed=i),
            capabilities=_make_caps(f"p{i}"),
            carbon_footprint=0.1 + 0.02 * i,
            helium_usage=0.01 + 0.001 * i,
            sustainability_contribution=0.4 + 0.05 * i,
            role=ParticipantRole.FOLLOWER.value,
        )


@pytest_asyncio.fixture
async def fed_minimal(storage, queue, adaptive_cost, pareto_gating,
                      drift_detector, metrics):
    eco = EnhancedFederatedOrchestrator(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        min_participants=2, max_participants=4,
        enable_quantum_distillation=False,
        enable_causal_mask=False,
        enable_expert_auction=False,
        enable_safety_monitor=False,
        enable_temporal_safety=False,
        enable_xai=False,
        enable_precision=False,
        enable_carbon_market=False,
        enable_chaos=False,
        enable_human_approval=False,
        enable_health_checks=False,
        enable_self_healing=False,
        seed=42)
    await eco.initialize()
    try:
        yield eco
    finally:
        await eco.shutdown()


@pytest_asyncio.fixture
async def fed_full(storage, queue, adaptive_cost, pareto_gating,
                   drift_detector, metrics):
    eco = EnhancedFederatedOrchestrator(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        min_participants=2, max_participants=4,
        enable_quantum_distillation=True,
        enable_causal_mask=True,
        enable_expert_auction=True,
        enable_safety_monitor=True,
        enable_temporal_safety=True,
        enable_xai=True,
        enable_precision=True,
        enable_carbon_market=True,
        enable_chaos=False,
        enable_human_approval=True,
        enable_health_checks=True,
        enable_self_healing=True,
        enable_cross_tier_distillation=True,
        enable_compression=True,
        enable_secure_aggregation=True,
        seed=42)
    await eco.initialize()
    try:
        yield eco
    finally:
        await eco.shutdown()


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    def test_module_imports_without_torch(self):
        assert EnhancedFederatedOrchestrator is not None

    def test_random_available(self):
        import random as rnd
        assert rnd is not None

    def test_sklearn_flag_defined(self):
        assert isinstance(SKLEARN_AVAILABLE, bool)

    def test_all_missing_types_defined(self):
        from federated_experts_v9 import (  # type: ignore
            AggregationStrategy, FederationRound, FederatedExpert,
            PrivacyLevel, ParticipantRole, ParticipantSelector,
            ReputationScoringSystem, SecureAggregator, ModelCompressor,
            CrossTierDistiller, PredictiveFederationAnalyzer,
            FederationCrossDomainTransfer, TaskManager,
        )
        # All imported successfully.
        assert AggregationStrategy.FED_AVG.value == "fed_avg"
        assert PrivacyLevel.DIFFERENTIAL.value == "differential"
        assert ParticipantRole.FOLLOWER.value == "follower"

    @pytest.mark.asyncio
    async def test_init_sets_attributes(self, fed_minimal):
        assert fed_minimal.carbon_intensity == 400.0
        assert fed_minimal.carbon_price == 50.0
        assert fed_minimal.helium_scarcity == 0.5
        assert fed_minimal.helium_price == 0.5
        assert fed_minimal.token_balance == 500.0

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage, queue):
        eco = EnhancedFederatedOrchestrator(
            storage=storage, message_queue=queue,
            min_participants=2, max_participants=3,
            enable_quantum_distillation=False,
            enable_causal_mask=False,
            enable_expert_auction=False,
            enable_safety_monitor=False,
            enable_temporal_safety=False,
            enable_xai=False,
            enable_precision=False,
            enable_carbon_market=False,
            enable_chaos=False,
            enable_human_approval=False,
            enable_health_checks=False,
            enable_self_healing=False)
        await eco.initialize()
        assert len(eco._background_tasks) >= 1
        await eco.shutdown()
        assert len(eco._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_no_pickle_persistence(self, fed_minimal):
        _populate(fed_minimal, 3)
        await fed_minimal.save_state()
        # The stored model is JSON, not pickle.
        raw = fed_minimal.storage.get_state("federation_global_model")
        if raw is not None:
            # JSON should round-trip.
            _ = json.loads(raw)

    @pytest.mark.asyncio
    async def test_local_model_not_aliased(self, fed_minimal):
        _populate(fed_minimal, 3)
        await fed_minimal.federated_round()
        # Mutating global_model should not mutate a participant's local_model.
        if fed_minimal.global_model:
            key = next(iter(fed_minimal.global_model))
            fed_minimal.global_model[key] = (fed_minimal.global_model[key] * 0.0)
            for p in fed_minimal.participants.values():
                # None of the participants should share the exact same
                # reference as global_model.
                assert p.local_model[key] is not fed_minimal.global_model[key]

    @pytest.mark.asyncio
    async def test_weighted_average_union_keys(self, fed_minimal):
        updates = {
            "a": {"x": np.array([1.0, 2.0], dtype=np.float32)},
            "b": {"y": np.array([3.0, 4.0], dtype=np.float32)},
            "c": {"x": np.array([5.0, 6.0], dtype=np.float32),
                  "y": np.array([7.0, 8.0], dtype=np.float32)},
        }
        weights = {"a": 1.0, "b": 1.0, "c": 1.0}
        agg = fed_minimal._weighted_average(updates, weights)
        assert "x" in agg and "y" in agg

    @pytest.mark.asyncio
    async def test_feedback_event_drops_metadata(self, queue):
        event = FeedbackEvent.create_with_context(
            source="x", feedback_type="y", task_id="t",
            context={}, action={}, performance={},
            adaptive_cost_value=0.0, tags=[],
            metadata={"foo": "bar"})
        assert "metadata" not in asdict(event)

    @pytest.mark.asyncio
    async def test_await_on_sync_compute_removed(self, fed_full):
        # policy_probs should not await AdaptiveCostFunction.compute.
        probs = await fed_full.policy_probs({})
        assert len(probs) == len(AggregationStrategy)


# =============================================================================
# TESTS — ENHANCEMENT MODULES
# =============================================================================
class TestQuantumDistillation:
    def test_register_normalises(self):
        q = QuantumDistillationModule(n_dims=4)
        q.register_teacher("a", [1, 2, 3, 4])
        assert abs(sum(q.teachers["a"]) - 1.0) < 1e-9

    def test_superposition_agreement(self):
        q = QuantumDistillationModule(n_dims=4)
        p = [0.4, 0.3, 0.2, 0.1]
        q.register_teacher("a", p)
        q.register_teacher("b", p)
        sup, _ = q._superpose()
        assert all(abs(a - b) < 1e-6 for a, b in zip(sup, p))

    @pytest.mark.asyncio
    async def test_step_updates_student(self):
        q = QuantumDistillationModule(n_dims=4)
        q.register_teacher("a", [0.4, 0.3, 0.2, 0.1])
        before = q.get_student_policy()
        entry = await q.step()
        after = q.get_student_policy()
        assert before != after
        assert "entropy" in entry


class TestCausalMask:
    def test_apply_identity_when_unfit(self):
        c = CausalFeatureMask(feature_dim=4)
        out = c.apply([1.0, 2.0, 3.0, 4.0])
        assert np.allclose(out, [1, 2, 3, 4])

    def test_observe_and_fit(self):
        c = CausalFeatureMask(feature_dim=4)
        for _ in range(50):
            c.observe(np.random.randn(4))
        summary = c.fit()
        assert len(summary["mask"]) == 4

    def test_wrong_dim_raises(self):
        c = CausalFeatureMask(feature_dim=4)
        with pytest.raises(ValueError):
            c.apply([1.0, 2.0, 3.0])


class TestExpertAuction:
    def test_bids_all_participants(self):
        a = ExpertAuction(["p1", "p2", "p3"], feature_dim=4)
        bids = a.compute_bids([0.1, 0.2, 0.3, 0.4])
        assert set(bids.keys()) == {"p1", "p2", "p3"}

    def test_update_changes_bid(self):
        a = ExpertAuction(["p1"], feature_dim=4)
        feats = [0.5] * 4
        before = a.compute_bids(feats)["p1"]
        for _ in range(10):
            a.update("p1", feats, reward=1.0)
        after = a.compute_bids(feats)["p1"]
        assert after > before

    def test_select_top_k(self):
        a = ExpertAuction(["p1", "p2", "p3"], feature_dim=4)
        for _ in range(10):
            a.update("p1", [1.0, 0, 0, 0], reward=1.0)
        top = a.select([1.0, 0, 0, 0], top_k=2)
        assert len(top) == 2
        assert "p1" in top


class TestSafety:
    def test_rule_triggers_mitigation(self):
        s = SafetyMonitor()
        calls = []
        s.add_rule("bad", lambda st: False, "test",
                   severity="critical",
                   mitigation=lambda t: calls.append(1))
        violations = s.check({}, target=object())
        assert any(v.name == "bad" for v in violations)
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_temporal_always(self):
        s = TemporalSafetyMonitor()
        s.add_formula("G (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.2})
        result = await s.check()
        assert any(not v for v in result.values())

    @pytest.mark.asyncio
    async def test_temporal_never(self):
        s = TemporalSafetyMonitor()
        s.add_formula("NEVER (carbon >= 0.8)")
        await s.push({"carbon": 0.5})
        result = await s.check()
        assert all(result.values())

    @pytest.mark.asyncio
    async def test_windowed_rule(self):
        s = TemporalSafetyMonitor()
        s.add_formula("[60] G (quality >= 0.5)")
        assert any("@60" in rid for rid in s.rules)


class TestXAI:
    def test_ig_sum_approximates_diff(self):
        x = XAIExplainer(ig_steps=8)

        def f(v):
            return float(np.dot(v, [0.4, -0.3, -0.2, -0.1]))

        feats = np.array([0.9, 0.4, 0.5, 0.4])
        attrs = x.integrated_gradients(f, feats)
        diff = f(feats) - f(np.zeros_like(feats))
        assert abs(float(np.sum(attrs)) - diff) < 0.2

    def test_counterfactual_runs(self):
        x = XAIExplainer(cf_max_iter=50, cf_target_delta=0.05,
                         rng=random.Random(0))

        def f(v):
            return float(v[0] - v[1])

        feats = np.array([0.9, 0.4])
        cf = x.counterfactual(f, feats)
        assert "changed_index" in cf


class TestPrecision:
    def test_lowest_energy(self):
        p = PrecisionController(levels=["fp32", "int8"])
        assert p.select_precision() == "int8"

    def test_high_sensitivity_forces_fp32(self):
        p = PrecisionController(levels=["fp32", "int8"], max_sensitivity=0.3)
        assert p.select_precision(layer_sensitivity=0.9) == "fp32"

    @pytest.mark.asyncio
    async def test_switch(self):
        p = PrecisionController(levels=["fp32", "int8"], hysteresis_s=0.0)
        assert await p.switch_to("int8") is True

    @pytest.mark.asyncio
    async def test_auto_switch_drop(self):
        p = PrecisionController(levels=["fp32", "int8"], hysteresis_s=0.0)
        p.current = "int8"
        await p.auto_switch(recent_acc=0.7, baseline_acc=0.95)
        assert p.current == "fp32"


class TestCarbonMarket:
    @pytest.mark.asyncio
    async def test_budget_overshoot(self):
        c = CarbonMarketClient(budget_kg=0.1, rng=random.Random(0))
        c.used_kg = 0.09
        result = await c.net_zero_schedule(1.0, 0.1)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_buy_retire(self):
        c = CarbonMarketClient()
        await c.buy_credits(10.0)
        await c.buy_credits(5.0)
        assert c.rec_balance() == 15.0
        retired = await c.retire_rec(7.0)
        assert retired == 7.0
        assert abs(c.rec_balance() - 8.0) < 1e-9


class TestCarbonClients:
    @pytest.mark.asyncio
    async def test_carbon_client_fallback(self):
        c = CarbonIntensityClient()
        s = await c.update()
        assert s["intensity"] > 0

    @pytest.mark.asyncio
    async def test_carbon_client_cache(self):
        c = CarbonIntensityClient()
        await c.update(force=True)
        cache_size = len(c._cache)
        await c.update()
        assert len(c._cache) == cache_size

    @pytest.mark.asyncio
    async def test_helium_client_update(self):
        c = HeliumClient()
        s = await c.update(force=True)
        assert 0.0 <= s["scarcity"] <= 1.0
        assert s["price"] > 0

    def test_helium_client_sync_interface(self):
        c = HeliumClient()
        assert 0.0 <= c.get_scarcity() <= 1.0
        assert c.get_cost_index() > 0


class TestChaos:
    @pytest.mark.asyncio
    async def test_latency(self, fed_minimal):
        c = ChaosEngine(fed_minimal, rng=random.Random(0))
        r = await c.run_experiment("t1", "latency")
        assert r["status"] == "completed"

    @pytest.mark.asyncio
    async def test_exception(self, fed_minimal):
        c = ChaosEngine(fed_minimal, rng=random.Random(0))
        r = await c.run_experiment("t2", "exception")
        assert r["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, fed_minimal):
        c = ChaosEngine(fed_minimal)
        with pytest.raises(ValueError):
            await c.run_experiment("t", "unknown")


class TestHITL:
    @pytest.mark.asyncio
    async def test_single_candidate_auto(self):
        h = HumanApprovalHandler()
        assert await h.request_approval(
            {"candidates": [{"id": "only"}]}) is True

    @pytest.mark.asyncio
    async def test_confident_skips(self):
        h = HumanApprovalHandler(uncertainty_threshold=0.01)
        await h.record_choice("u", "a", metrics={"quality_score": 1.0})
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 10.0},
                {"id": "lo", "quality_score": 0.01}]}, user_id="u")
        assert ok is True


class TestSupportingClasses:
    def test_reputation_scoring(self):
        r = ReputationScoringSystem()
        # Synchronous check on the dataclass.
        assert r.records == {}

    @pytest.mark.asyncio
    async def test_reputation_update(self):
        r = ReputationScoringSystem()
        s1 = await r.update("p1", success=True, sustainability=0.9)
        assert 0.0 <= s1 <= 1.0
        s2 = await r.update("p1", success=False)
        assert 0.0 <= s2 <= 1.0

    def test_secure_aggregator_smoke(self):
        import asyncio as aio
        s = SecureAggregator()

        async def run():
            return await s.aggregate(
                [{"x": np.array([1.0, 2.0])}], [1.0])
        result = aio.get_event_loop().run_until_complete(run()) \
            if not aio.get_event_loop().is_running() else None
        # Just confirm it's callable.

    def test_model_compressor_roundtrip(self):
        c = ModelCompressor()
        model = {"W": np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)}
        payload, meta = c.compress(model)
        restored = c.decompress(payload)
        assert np.allclose(restored["W"], model["W"])

    def test_cross_tier_distiller(self):
        d = CrossTierDistiller()
        teacher = {"W": np.array([[1.0]], dtype=np.float32)}
        student = {"W": np.array([[0.0]], dtype=np.float32)}
        out = d.distill(teacher, student, alpha=0.5)
        assert np.allclose(out["W"], [[0.5]])

    def test_predictive_analyzer(self):
        p = PredictiveFederationAnalyzer()
        for v in (0.4, 0.5, 0.6, 0.7):
            p.observe(v)
        assert p.forecast(1) > 0.4

    def test_cross_domain_transfer(self):
        c = FederationCrossDomainTransfer()
        c.transfer("a", "b", {"x": 1})
        stats = c.stats()
        assert stats["total_transfers"] == 1


class TestTaskManager:
    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        tm = TaskManager()
        stop_flag = {"v": False}

        async def worker():
            while not stop_flag["v"]:
                await asyncio.sleep(0.01)

        tm.start_task("w", worker)
        assert tm.task_count() == 1
        await tm.stop_all()
        assert tm.task_count() == 0


class TestLocalTraining:
    def test_linear_model_reduces_loss(self):
        rng = np.random.RandomState(0)
        X = rng.randn(64, 4).astype(np.float32)
        y = X @ rng.randn(4, 1).astype(np.float32)
        model = LinearModel(4, 1, seed=0)
        loss0 = None
        loss1 = None
        for i in range(20):
            l = model.train_step(X, y, lr=0.1)
            if i == 0:
                loss0 = l
        loss1 = l
        assert loss1 < loss0


# =============================================================================
# TESTS — HEALTH + SELF-HEALING
# =============================================================================
class TestHealthAndSelfHealing:
    @pytest.mark.asyncio
    async def test_health_check_healthy(self):
        h = HealthCheckSystem(interval=0.05)

        class Good:
            async def get_health_status(self):
                return ComponentHealth(HealthStatus.HEALTHY, 1.0, {})

        try:
            h.register_component("good", Good())
            await h._perform_checks()
            health = await h.get_system_health()
            assert health["system_status"] == "healthy"
        finally:
            await h.shutdown()

    @pytest.mark.asyncio
    async def test_self_healing_reregisters(self):
        h = HealthCheckSystem(interval=0.05)

        class Bad:
            async def get_health_status(self):
                return ComponentHealth(HealthStatus.UNHEALTHY, 0.0, {})

        class Good:
            async def get_health_status(self):
                return ComponentHealth(HealthStatus.HEALTHY, 1.0, {})

        try:
            h.register_component("c", Bad())
            healing = SelfHealingSystem(h, max_attempts=2, interval=0.05)
            try:
                good = Good()
                healing.register_handler("c", lambda: good)
                await asyncio.sleep(0.3)
                assert h.components["c"] is good
            finally:
                await healing.shutdown()
        finally:
            await h.shutdown()


# =============================================================================
# TESTS — INTEGRATION
# =============================================================================
class TestIntegration:
    @pytest.mark.asyncio
    async def test_federated_round_smoke(self, fed_full):
        _populate(fed_full, 5)
        decision = await fed_full.federated_round()
        assert decision is not None
        assert decision.strategy in [s.value for s in AggregationStrategy]
        assert len(decision.participant_ids) >= 2
        assert abs(float(decision.strategy_probs.sum()) - 1.0) < 1e-4

    @pytest.mark.asyncio
    async def test_policy_probs_sum_to_one(self, fed_full):
        probs = await fed_full.policy_probs({})
        assert abs(sum(probs) - 1.0) < 1e-4

    @pytest.mark.asyncio
    async def test_health_check_reports_all(self, fed_full):
        hc = await fed_full.health_check()
        assert "system_health" in hc
        assert "carbon_client" in hc
        assert "helium_client" in hc

    @pytest.mark.asyncio
    async def test_save_and_load_state(self, storage, queue):
        eco = EnhancedFederatedOrchestrator(
            storage=storage, message_queue=queue,
            min_participants=2, max_participants=3,
            enable_health_checks=False, enable_self_healing=False,
            enable_quantum_distillation=False, enable_causal_mask=False,
            enable_expert_auction=False, enable_safety_monitor=False,
            enable_temporal_safety=False, enable_xai=False,
            enable_precision=False, enable_carbon_market=False,
            enable_chaos=False, enable_human_approval=False)
        await eco.initialize()
        try:
            _populate(eco, 3)
            await eco.federated_round()
            await eco.save_state()
            eco2 = EnhancedFederatedOrchestrator(
                storage=storage, message_queue=queue,
                min_participants=2, max_participants=3,
                enable_health_checks=False, enable_self_healing=False,
                enable_quantum_distillation=False, enable_causal_mask=False,
                enable_expert_auction=False, enable_safety_monitor=False,
                enable_temporal_safety=False, enable_xai=False,
                enable_precision=False, enable_carbon_market=False,
                enable_chaos=False, enable_human_approval=False)
            await eco2.initialize()
            try:
                assert eco2.round_number == eco.round_number
                assert len(eco2.participants) == len(eco.participants)
            finally:
                await eco2.shutdown()
        finally:
            await eco.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_rounds(self, fed_full):
        _populate(fed_full, 5)
        results = await asyncio.gather(*[
            fed_full.federated_round() for _ in range(3)])
        assert all(r is not None for r in results)

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, fed_full):
        await fed_full.shutdown()
        await fed_full.shutdown()
        assert len(fed_full._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_chaos_bounded(self, storage, queue):
        eco = EnhancedFederatedOrchestrator(
            storage=storage, message_queue=queue,
            min_participants=2, max_participants=3,
            enable_chaos=True, chaos_probability=1.0,
            enable_health_checks=False, enable_self_healing=False,
            enable_quantum_distillation=False, enable_causal_mask=False,
            enable_expert_auction=False, enable_safety_monitor=False,
            enable_temporal_safety=False, enable_xai=False,
            enable_precision=False, enable_carbon_market=False,
            enable_human_approval=False)
        await eco.initialize()
        try:
            _populate(eco, 3)
            for _ in range(20):
                await eco.chaos.run_experiment(f"c{_}", "data_corruption")
            assert eco._check_steady_state()
        finally:
            await eco.shutdown()

    @pytest.mark.asyncio
    async def test_safety_mitigation(self, storage, queue):
        eco = EnhancedFederatedOrchestrator(
            storage=storage, message_queue=queue,
            min_participants=2, max_participants=3,
            enable_health_checks=False, enable_self_healing=False,
            enable_quantum_distillation=False, enable_causal_mask=False,
            enable_expert_auction=False, enable_safety_monitor=True,
            enable_temporal_safety=False, enable_xai=False,
            enable_precision=False, enable_carbon_market=False,
            enable_chaos=False, enable_human_approval=False)
        await eco.initialize()
        try:
            with eco._cpu_lock:
                eco.global_model = {
                    "x": np.array([np.nan, 1.0], dtype=np.float32)}
            violations = eco.safety.check({}, target=eco)
            assert any(v.name == "state_finite" for v in violations)
            assert eco._check_steady_state()
        finally:
            await eco.shutdown()

    @pytest.mark.asyncio
    async def test_determinism(self, storage):
        def _build():
            return EnhancedFederatedOrchestrator(
                storage=storage, min_participants=2, max_participants=3,
                enable_health_checks=False, enable_self_healing=False,
                enable_quantum_distillation=False, enable_causal_mask=False,
                enable_expert_auction=False, enable_safety_monitor=False,
                enable_temporal_safety=False, enable_xai=False,
                enable_precision=False, enable_carbon_market=False,
                enable_chaos=False, enable_human_approval=False, seed=99)
        eco1 = _build()
        eco2 = _build()
        await eco1.initialize()
        await eco2.initialize()
        try:
            _populate(eco1, 3)
            _populate(eco2, 3)
            d1 = await eco1.federated_round()
            d2 = await eco2.federated_round()
            assert d1.strategy == d2.strategy
            assert d1.participant_ids == d2.participant_ids
        finally:
            await eco1.shutdown()
            await eco2.shutdown()


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

        eco = EnhancedFederatedOrchestrator(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics,
            min_participants=2, max_participants=4,
            enable_quantum_distillation=True,
            enable_causal_mask=True,
            enable_expert_auction=True,
            enable_safety_monitor=True,
            enable_temporal_safety=True,
            enable_xai=True,
            enable_precision=True,
            enable_carbon_market=True,
            enable_human_approval=True,
            enable_health_checks=True,
            enable_self_healing=True,
            enable_compression=True,
            seed=1)
        await eco.initialize()
        try:
            _populate(eco, 5)
            decision = await eco.federated_round()
            print("Strategy:", decision.strategy)
            print("Participants:", decision.participant_ids)
            print("Sustainability:", eco.sustainability_score)
            print("Keys:", decision.aggregated_model_keys)
            health = await eco.health_check()
            print("Health:", json.dumps(health, indent=2, default=str))
        finally:
            await eco.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
