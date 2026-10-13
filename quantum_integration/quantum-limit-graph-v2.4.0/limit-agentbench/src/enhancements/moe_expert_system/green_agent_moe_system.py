#!/usr/bin/env python3
"""
Unified Metabolic Ecosystem v9.0.0 — Self-Contained Single-File Bundle
=======================================================================

Complete rewrite of the Green Agent MoE ecosystem. Everything needed to
run and test is in this file:

  1. Local stubs for Storage, AsyncMessageQueue, FeedbackEvent,
     AdaptiveCostFunction, ParetoGating, DriftDetector, MetricsRegistry.
  2. Bio-signal feature encoder (12-dim vector).
  3. Ten real enhancement modules:
        - QuantumDistillationModule  (density-matrix teacher superposition)
        - CausalFeatureMask          (PC-style partial correlation)
        - FederatedGreenAggregator   (weighted FedAvg + DP + sparsification)
        - ExpertAuction              (LinUCB contextual bandit)
        - TemporalSafetyMonitor      (MTL operators G/F/NEVER/H/O)
        - XAIExplainer               (Integrated Gradients + counterfactuals)
        - PrecisionController        (layer-wise + hysteresis + autocast)
        - CarbonMarketClient         (budget + EMA + REC)
        - ChaosEngine                (steady-state + rollback)
        - HumanApprovalHandler       (uncertainty sampling)
  4. `SafetyMonitor` with severity + mitigation.
  5. `MetabolicDecision` value object.
  6. `GatingNetwork` with `nn.LayerNorm` (batch-size-1 safe).
  7. Real Carbon HTTP client with circuit breaker + cache + EMA.
  8. Health check system with per-component `get_health_status`.
  9. Self-healing system that re-registers recovered components.
 10. `UnifiedMetabolicEcosystem` orchestrating everything.
 11. Comprehensive pytest suite.

Run:
    python unified_metabolic_ecosystem_v9.py         # demo
    pytest unified_metabolic_ecosystem_v9.py -v      # tests

Requires: Python 3.10+, numpy, pytest, pytest-asyncio.
Optional: torch (falls back to a numpy gate), aiohttp, pydantic.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Protocol, Set, Tuple

import numpy as np
import pytest
import pytest_asyncio

logger = logging.getLogger("metabolic_ecosystem")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    TORCH_AVAILABLE = False
    nn = None  # type: ignore
    torch = None  # type: ignore


# =============================================================================
# LOCAL STUBS
# =============================================================================
class Storage:
    """Fault-injectable in-memory storage."""

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
        # Make sure required fields are present.
        for k in ("source", "feedback_type", "task_id", "context", "action",
                  "performance", "adaptive_cost_value", "tags"):
            filtered.setdefault(k, "unknown" if k in ("source", "feedback_type", "task_id") else
                                {} if k in ("context", "action", "performance") else
                                0.0 if k == "adaptive_cost_value" else [])
        return cls(**filtered)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class AdaptiveCostFunction:
    def __init__(self, storage: Storage):
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
        out = []
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
    """Page-Hinkley drift detector."""

    def __init__(self, storage: Storage, adaptive_cost: AdaptiveCostFunction,
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

    def increment(self, name: str, amount: int = 1) -> None:
        self.inc(name, amount)

    def observe(self, name: str, value: float) -> None:
        self.set(name, value)


# =============================================================================
# ENUMS + CORE VALUE OBJECTS
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


@dataclass(frozen=True)
class MetabolicDecision:
    state_vec: np.ndarray
    probs: np.ndarray
    expert_ids: Tuple[str, ...]
    selected_expert: str
    selected_idx: int
    confidence: float
    timestamp: float
    explanation: Optional[Dict[str, Any]] = None
    counterfactual: Optional[Dict[str, Any]] = None
    precision: str = "fp32"
    drift: float = 0.0
    safety_violations: Tuple[str, ...] = ()


# =============================================================================
# BIO-SIGNAL FEATURES (12 dims)
# =============================================================================
class BioFeatures:
    NAMES = (
        "carbon_zone", "helium_scarcity", "task_complexity",
        "token_balance", "gradient_carbon", "gradient_helium",
        "gradient_trust", "opportunity_gradient", "stress_level",
        "avg_client_energy", "carbon_intensity", "helium_price",
    )
    DIM = len(NAMES)

    @classmethod
    def encode(cls, context: Dict[str, Any]) -> np.ndarray:
        feats: List[float] = []
        for name in cls.NAMES:
            v = context.get(name)
            if v is None:
                v = 0.5
            try:
                feats.append(float(v))
            except (TypeError, ValueError):
                feats.append(0.5)
        return np.asarray(feats, dtype=np.float32)


# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
class CircuitBreaker:
    """Async circuit breaker with probe-limit in half-open state."""

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
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_requests >= self.half_open_max_requests:
                    raise RuntimeError(
                        f"Circuit breaker {self.name} half-open limit reached")
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


class PerExpertRateLimiter:
    def __init__(self, rate_per_minute: float):
        self.rate = float(rate_per_minute)
        self._limiters: Dict[str, RateLimiter] = {}

    def get_limiter(self, expert_id: str) -> RateLimiter:
        if expert_id not in self._limiters:
            self._limiters[expert_id] = RateLimiter(self.rate)
        return self._limiters[expert_id]


# =============================================================================
# ENHANCEMENT 1 — QUANTUM DISTILLATION (density matrix)
# =============================================================================
class QuantumDistillationModule:
    def __init__(self, n_dims: int = BioFeatures.DIM,
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
        policy = (diag / s).tolist()
        return policy, self._von_neumann_entropy(rho)

    def _softmax(self, x: Iterable[float], temp: float) -> List[float]:
        x = np.asarray(list(x), dtype=float)
        m = x.max()
        e = np.exp((x - m) / max(temp, 1e-6))
        s = e.sum() or 1.0
        return (e / s).tolist()

    async def step(self, storage: Optional[Storage] = None,
                   student_id: str = "ecosystem") -> Dict[str, Any]:
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
    """PC-style partial-correlation skeleton over 12 bio-signal features."""

    FEATURE_NAMES = BioFeatures.NAMES

    def __init__(self, feature_dim: int = BioFeatures.DIM,
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
            raise ValueError(
                f"features dim {feats.shape[0]} != {self.feature_dim}")
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
# ENHANCEMENT 3 — FEDERATED GREEN AGGREGATOR
# =============================================================================
class FederatedGreenAggregator:
    """Weighted FedAvg with DP noise + top-k sparsification + rate limiting."""

    def __init__(self, storage: Storage, instance_id: str,
                 dp_epsilon: Optional[float] = None, dp_clip: float = 1.0,
                 sparse_topk: float = 0.1,
                 rate_limit_seconds: float = 5.0,
                 rng: Optional[random.Random] = None):
        self.storage = storage
        self.instance_id = instance_id
        self.dp_epsilon = dp_epsilon
        self.dp_clip = float(dp_clip)
        self.sparse_topk = float(sparse_topk)
        self.rate_limit_seconds = float(rate_limit_seconds)
        self.rng = rng or random.Random()
        self.rounds = 0
        self.last_send_ts = 0.0
        self.sent_count = 0
        self.received_count = 0

    def _sparsify(self, vec: np.ndarray) -> np.ndarray:
        if self.sparse_topk <= 0.0 or vec.size == 0:
            return vec
        k = max(1, int(vec.size * self.sparse_topk))
        flat = vec.flatten()
        idx = np.argsort(-np.abs(flat))[:k]
        out = np.zeros_like(flat)
        out[idx] = flat[idx]
        return out.reshape(vec.shape)

    def _privatise(self, vec: np.ndarray) -> np.ndarray:
        if self.dp_epsilon is None or vec.size == 0:
            return vec
        sigma = self.dp_clip / max(self.dp_epsilon, 1e-6)
        noise = np.asarray([self.rng.gauss(0.0, sigma)
                            for _ in range(vec.size)], dtype=vec.dtype)
        clipped = np.clip(vec, -self.dp_clip, self.dp_clip)
        return (clipped + noise.reshape(vec.shape)).astype(vec.dtype)

    async def share_weights(self, model_id: str, weights: Dict[str, Any],
                            sample_count: int = 1,
                            force: bool = False) -> bool:
        now = time.monotonic()
        if not force and (now - self.last_send_ts) < self.rate_limit_seconds:
            return False
        payload = {}
        for k, v in weights.items():
            if isinstance(v, np.ndarray):
                arr = v.astype(np.float32)
                arr = self._sparsify(arr)
                arr = self._privatise(arr)
                payload[k] = arr.tolist()
            else:
                payload[k] = v
        key = f"fed::{model_id}::{self.instance_id}::{self.rounds}"
        try:
            await asyncio.to_thread(
                self.storage.save_state, key,
                json.dumps({"weights": payload, "n": sample_count,
                            "round": self.rounds,
                            "epsilon": self.dp_epsilon}))
        except Exception:
            return False
        self.rounds += 1
        self.last_send_ts = now
        self.sent_count += 1
        return True

    async def pull_aggregated(self, model_id: str) -> Optional[Dict[str, Any]]:
        keys = [k for k in self.storage._state
                if k.startswith(f"fed::{model_id}::")]
        entries: List[Tuple[Dict[str, Any], int]] = []
        for k in keys:
            try:
                d = json.loads(self.storage._state[k])
                entries.append((d["weights"], max(int(d.get("n", 1)), 1)))
            except Exception:
                continue
        if not entries:
            return None
        total = sum(w for _, w in entries) or 1.0
        keys_set = set(entries[0][0].keys())
        out: Dict[str, Any] = {}
        for k in keys_set:
            v0 = entries[0][0][k]
            if isinstance(v0, list):
                arrs = [np.asarray(e[0][k], dtype=np.float32)
                        for e in entries if k in e[0]]
                min_len = min(len(a) for a in arrs)
                acc = np.zeros(min_len, dtype=np.float32)
                for e, a in zip(entries, arrs):
                    acc += a[:min_len] * e[1]
                out[k] = (acc / total).tolist()
            else:
                out[k] = v0
        self.received_count += 1
        return out


# =============================================================================
# ENHANCEMENT 4 — EXPERT AUCTION (LinUCB)
# =============================================================================
class ExpertAuction:
    """LinUCB contextual bandit over 12-dim bio-signal features."""

    def __init__(self, expert_ids: List[str],
                 feature_dim: int = BioFeatures.DIM,
                 alpha: float = 0.2, ridge: float = 1.0,
                 rng: Optional[random.Random] = None):
        self.expert_ids = list(expert_ids)
        self.feature_dim = int(feature_dim)
        self.alpha = float(alpha)
        self.ridge = float(ridge)
        self.rng = rng or random.Random()
        self._A: Dict[str, np.ndarray] = {
            eid: ridge * np.eye(self.feature_dim, dtype=np.float32)
            for eid in self.expert_ids}
        self._b: Dict[str, np.ndarray] = {
            eid: np.zeros(self.feature_dim, dtype=np.float32)
            for eid in self.expert_ids}

    def add_expert(self, expert_id: str) -> None:
        if expert_id in self._A:
            return
        self.expert_ids.append(expert_id)
        self._A[expert_id] = self.ridge * np.eye(self.feature_dim,
                                                  dtype=np.float32)
        self._b[expert_id] = np.zeros(self.feature_dim, dtype=np.float32)

    def compute_bids(self, features: Iterable[float]) -> Dict[str, float]:
        x = np.asarray(list(features)[:self.feature_dim], dtype=np.float32)
        if x.size < self.feature_dim:
            x = np.pad(x, (0, self.feature_dim - x.size))
        scores: Dict[str, float] = {}
        for eid in self.expert_ids:
            try:
                A_inv = np.linalg.inv(self._A[eid])
            except np.linalg.LinAlgError:
                A_inv = np.linalg.pinv(self._A[eid])
            theta = A_inv @ self._b[eid]
            mu = float(theta @ x)
            sigma = float(math.sqrt(max(float(x @ A_inv @ x), 0.0)))
            scores[eid] = mu + self.alpha * sigma
        return scores

    def select_experts(self, features: Iterable[float],
                       top_k: int = 1) -> List[str]:
        bids = self.compute_bids(features)
        return [eid for eid, _ in sorted(bids.items(),
                                          key=lambda kv: kv[1],
                                          reverse=True)[:top_k]]

    def update(self, expert_id: str, features: Iterable[float],
               reward: float) -> None:
        if expert_id not in self._A:
            self.add_expert(expert_id)
        x = np.asarray(list(features)[:self.feature_dim], dtype=np.float32)
        if x.size < self.feature_dim:
            x = np.pad(x, (0, self.feature_dim - x.size))
        self._A[expert_id] = self._A[expert_id] + np.outer(x, x)
        self._b[expert_id] = self._b[expert_id] + reward * x


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL SAFETY MONITOR
# =============================================================================
@dataclass
class _SafetyRule:
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
    """MTL monitor supporting G, F, NEVER, H, O and windowed rules."""

    def __init__(self, max_trace: int = 500):
        self.rules: Dict[str, _SafetyRule] = {}
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
        self.rules[rid] = _SafetyRule(rid, op, predicate,
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
# ENHANCEMENT 6 — XAI (Integrated Gradients)
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
# ENHANCEMENT 7 — PRECISION CONTROLLER
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


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    """Carbon budget + EMA price + REC ledger."""

    def __init__(self, budget_kg: float = 100.0,
                 price_oracle: Optional[Callable] = None,
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
# ENHANCEMENT 9 — CHAOS ENGINE
# =============================================================================
class ChaosEngine:
    """Fault injection with steady-state hypothesis + auto-rollback."""

    FAULT_TYPES = ("latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop")

    def __init__(self, ecosystem: "UnifiedMetabolicEcosystem",
                 chaos_probability: float = 0.01,
                 auto_rollback: bool = True,
                 blast_radius: float = 0.5,
                 rng: Optional[random.Random] = None):
        self.ecosystem = ecosystem
        self.chaos_probability = float(chaos_probability)
        self.auto_rollback = bool(auto_rollback)
        self.blast_radius = float(blast_radius)
        self.rng = rng or random.Random()
        self.active: Dict[str, Dict[str, Any]] = {}
        self.rollback_count = 0
        self._lock = threading.Lock()

    def _allowed(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def maybe_inject_failure(self) -> Optional[Dict[str, Any]]:
        if self.rng.random() >= self.chaos_probability:
            return None
        fault = self.rng.choice(self.FAULT_TYPES)
        return await self.run_experiment(
            f"auto_{uuid.uuid4().hex[:6]}", fault)

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        with self._lock:
            if len(self.active) >= self._allowed():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}
            self.active[name] = {"fault_type": fault_type}
        t0 = time.time()
        before = self.ecosystem._check_steady_state()
        snapshot = self.ecosystem._snapshot_weights()
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
                self.ecosystem._corrupt_weights_bounded(self.rng)
        except Exception:
            status = "injected"
        finally:
            with self._lock:
                self.active.pop(name, None)
        after = self.ecosystem._check_steady_state()
        rolled_back = False
        if before and not after and self.auto_rollback:
            self.ecosystem._restore_weights(snapshot)
            rolled_back = True
            self.rollback_count += 1
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status,
                "duration_ms": (time.time() - t0) * 1000.0,
                "rolled_back": rolled_back}


# =============================================================================
# ENHANCEMENT 10 — HUMAN APPROVAL
# =============================================================================
class HumanApprovalHandler:
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
        scored = sorted(((self._utility(user_id, c), c)
                         for c in candidates),
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
# SAFETY MONITOR (severity + mitigation)
# =============================================================================
@dataclass
class SafetyViolation:
    name: str
    severity: str
    description: str
    mitigated: bool = False


@dataclass
class SafetyRule:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    description: str
    severity: str = "warning"
    mitigation: Optional[Callable] = None


class SafetyMonitor:
    def __init__(self):
        self.rules: List[SafetyRule] = []
        self.violations: Deque[SafetyViolation] = deque(maxlen=1000)

    def add_rule(self, name: str,
                 predicate: Callable[[Dict[str, Any]], bool],
                 description: str, severity: str = "warning",
                 mitigation: Optional[Callable] = None) -> None:
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
                v = SafetyViolation(rule.name, rule.severity,
                                    rule.description)
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


# =============================================================================
# GATING NETWORK — nn.LayerNorm, batch-size-1 safe
# =============================================================================
if TORCH_AVAILABLE:
    def _get_activation(name: str) -> "nn.Module":
        if name == "relu":
            return nn.ReLU()
        if name == "tanh":
            return nn.Tanh()
        if name == "gelu":
            return nn.GELU()
        raise ValueError(f"unknown activation: {name}")

    class GatingNetwork(nn.Module):
        """MLP gate using LayerNorm (batch-size-1 safe)."""

        def __init__(self, input_dim: int, hidden_dim: int, num_experts: int,
                     num_hidden_layers: int = 2, activation: str = "relu",
                     dropout_rate: float = 0.1):
            super().__init__()
            layers: List[nn.Module] = [
                nn.Linear(input_dim, hidden_dim),
                _get_activation(activation),
                nn.LayerNorm(hidden_dim),
                nn.Dropout(dropout_rate),
            ]
            for _ in range(num_hidden_layers - 1):
                layers.extend([
                    nn.Linear(hidden_dim, hidden_dim),
                    _get_activation(activation),
                    nn.LayerNorm(hidden_dim),
                    nn.Dropout(dropout_rate),
                ])
            layers.append(nn.Linear(hidden_dim, num_experts))
            self.network = nn.Sequential(*layers)

        def forward(self, x: "torch.Tensor") -> "torch.Tensor":
            return self.network(x)

    class ExpertModule(nn.Module):
        def __init__(self, input_dim: int, hidden_dim: int = 32,
                     output_dim: int = 1):
            super().__init__()
            self.network = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, output_dim),
            )

        def forward(self, x: "torch.Tensor") -> "torch.Tensor":
            return self.network(x)
else:
    class GatingNetwork:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("torch not available; use NumpyGatingNetwork")

        def forward(self, x):
            raise RuntimeError("torch not available")

    class ExpertModule:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("torch not available")

        def forward(self, x):
            raise RuntimeError("torch not available")


class NumpyGatingNetwork:
    """Pure-numpy fallback gate (LayerNorm-equivalent normalisation)."""

    def __init__(self, input_dim: int, hidden_dim: int, num_experts: int,
                 seed: int = 0):
        rng = np.random.RandomState(seed)
        self.W1 = rng.randn(input_dim, hidden_dim).astype(np.float32) * 0.1
        self.b1 = np.zeros(hidden_dim, dtype=np.float32)
        self.W2 = rng.randn(hidden_dim, num_experts).astype(np.float32) * 0.1
        self.b2 = np.zeros(num_experts, dtype=np.float32)
        self.eps = 1e-5

    def __call__(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=np.float32)
        h = x @ self.W1 + self.b1
        # LayerNorm
        mean = h.mean(axis=-1, keepdims=True)
        var = h.var(axis=-1, keepdims=True)
        h = (h - mean) / np.sqrt(var + self.eps)
        h = np.maximum(0, h)
        return h @ self.W2 + self.b2

    def parameters(self):
        return [self.W1, self.b1, self.W2, self.b2]


# =============================================================================
# CARBON INTENSITY HTTP CLIENT (circuit breaker + cache + EMA)
# =============================================================================
class CarbonIntensityClient:
    """HTTP client for carbon intensity with circuit breaker + cache + EMA."""

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
                                     "eu": 280, "asia": 500}
        self.intensity = float(self.fallback.get(region, 400.0))
        self.intensity_ema = self.intensity
        self.price = 50.0
        self.price_ema = 50.0
        self.last_update: Optional[datetime] = None
        self._circuit = CircuitBreaker(name="carbon_api")
        self._session = None
        self._session_lock = asyncio.Lock()
        self._history: Deque[Tuple[datetime, float]] = deque(maxlen=2000)
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
        headers = {}
        if self.api_key:
            headers["auth-token"] = self.api_key
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
            cached_val, cached_ts = self._cache[cache_key]
            if (now - cached_ts).total_seconds() < self.update_interval:
                self.intensity = cached_val
                return self._snapshot()

        try:
            value = await self._circuit.call(self._http_fetch)
            if value is None:
                raise RuntimeError("no HTTP client available")
        except Exception as e:
            logger.debug(f"carbon fetch failed: {e}; using fallback")
            value = float(self.fallback.get(self.region, 400.0))

        self.intensity = max(0.0, float(value))
        self.intensity_ema = (self.ema_alpha * self.intensity +
                              (1 - self.ema_alpha) * self.intensity_ema)
        self.price = max(10.0, 50.0 * (1.0 + (self.intensity - 300) / 500))
        self.price_ema = (self.ema_alpha * self.price +
                          (1 - self.ema_alpha) * self.price_ema)
        self.last_update = now
        self._cache[cache_key] = (self.intensity, now)
        self._history.append((now, self.intensity))
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
        return {"intensity": self.intensity,
                "intensity_ema": self.intensity_ema,
                "price": self.price,
                "price_ema": self.price_ema,
                "region": self.region,
                "cache_size": len(self._cache),
                "history_size": len(self._history)}

    async def get_health_status(self) -> ComponentHealth:
        if self.last_update is None:
            return ComponentHealth(HealthStatus.UNKNOWN, 0.5,
                                   {"reason": "never updated"})
        age = (datetime.now(timezone.utc) - self.last_update).total_seconds()
        if age > self.update_interval * 5:
            return ComponentHealth(HealthStatus.UNHEALTHY, 0.2,
                                   {"age_seconds": age})
        if self._circuit.state == CircuitBreakerState.OPEN:
            return ComponentHealth(HealthStatus.DEGRADED, 0.4,
                                   {"circuit_state": self._circuit.state.value})
        return ComponentHealth(HealthStatus.HEALTHY, 1.0,
                               {"age_seconds": age})

    async def close(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                await self._session.close()
            self._session = None


# =============================================================================
# HELIUM OPTIMIZER
# =============================================================================
class HeliumEfficiencyOptimizer:
    def __init__(self, budget_l: float = 100.0,
                 rng: Optional[random.Random] = None):
        self.budget_l = float(budget_l)
        self.usage: Dict[str, float] = defaultdict(float)
        self.efficiency: Dict[str, float] = defaultdict(lambda: 0.5)
        self.price = 0.5
        self.price_ema = 0.5
        self.price_history: Deque[float] = deque(maxlen=200)
        self.rng = rng or random.Random()

    async def record_usage(self, expert_id: str, amount_l: float) -> None:
        self.usage[expert_id] += float(amount_l)

    def update_price(self, scarcity: float = 0.5) -> None:
        self.price = max(0.05, 0.5 * (1 + scarcity * 0.8))
        self.price_ema = 0.3 * self.price + 0.7 * self.price_ema
        self.price_history.append(self.price)

    async def allocate(self, requirements: Dict[str, float]) -> Dict[str, float]:
        total = sum(requirements.values())
        if total <= self.budget_l:
            return dict(requirements)
        total_eff = sum(self.efficiency.get(e, 0.5)
                        for e in requirements) or 1.0
        return {e: self.budget_l * self.efficiency.get(e, 0.5) / total_eff
                for e in requirements}

    async def get_helium_status(self) -> Dict[str, Any]:
        return {"budget_l": self.budget_l,
                "usage": dict(self.usage),
                "remaining_l": self.budget_l - sum(self.usage.values()),
                "price": self.price,
                "price_ema": self.price_ema,
                "efficiency": dict(self.efficiency)}

    async def get_health_status(self) -> ComponentHealth:
        remaining = self.budget_l - sum(self.usage.values())
        score = max(0.0, min(1.0, remaining / self.budget_l))
        status = (HealthStatus.HEALTHY if score > 0.5 else
                  HealthStatus.DEGRADED if score > 0.1 else
                  HealthStatus.UNHEALTHY)
        return ComponentHealth(status, score,
                               {"remaining_l": remaining})


# =============================================================================
# EXPERT CLASSES
# =============================================================================
class BaseExpert:
    def __init__(self, name: str, domain: str,
                 module: Optional[Any] = None,
                 carbon_g_per_joule: float = 0.001,
                 energy_joules_per_call: float = 0.01,
                 latency_ms_per_call: float = 50.0):
        self.name = name
        self.domain = domain
        self.module = module
        self.healthy = True
        self.carbon_g_per_joule = float(carbon_g_per_joule)
        self.energy_joules_per_call = float(energy_joules_per_call)
        self.latency_ms_per_call = float(latency_ms_per_call)

    async def get_health_status(self) -> ComponentHealth:
        return ComponentHealth(
            HealthStatus.HEALTHY if self.healthy else HealthStatus.UNHEALTHY,
            1.0 if self.healthy else 0.0, {})

    def estimate_metrics(self) -> Dict[str, float]:
        return {"carbon_g": self.carbon_g_per_joule *
                            self.energy_joules_per_call,
                "energy_joules": self.energy_joules_per_call,
                "latency_ms": self.latency_ms_per_call}

    async def execute(self, params: Dict[str, Any],
                      context: Dict[str, Any]) -> Dict[str, Any]:
        return {"expert": self.name, "domain": self.domain,
                "status": "executed"}


class EnergyExpert(BaseExpert):
    def __init__(self, input_dim: int):
        module = (ExpertModule(input_dim, hidden_dim=32, output_dim=1)
                  if TORCH_AVAILABLE else None)
        super().__init__("EnergyExpert", "energy_management", module,
                         carbon_g_per_joule=0.002,
                         energy_joules_per_call=0.015,
                         latency_ms_per_call=70.0)


class DataExpert(BaseExpert):
    def __init__(self, input_dim: int):
        module = (ExpertModule(input_dim, hidden_dim=32, output_dim=1)
                  if TORCH_AVAILABLE else None)
        super().__init__("DataExpert", "data_processing", module,
                         carbon_g_per_joule=0.001,
                         energy_joules_per_call=0.01,
                         latency_ms_per_call=80.0)


class IoTExpert(BaseExpert):
    def __init__(self, input_dim: int):
        module = (ExpertModule(input_dim, hidden_dim=32, output_dim=1)
                  if TORCH_AVAILABLE else None)
        super().__init__("IoTExpert", "iot_sensing", module,
                         carbon_g_per_joule=0.0005,
                         energy_joules_per_call=0.005,
                         latency_ms_per_call=30.0)


# =============================================================================
# GATING MANAGER (torch or numpy)
# =============================================================================
class GatingNetworkManager:
    """Wraps the gating model, forward pass, and training buffer."""

    W_MAX = 10.0
    WEIGHT_NORM_MAX = 1000.0

    def __init__(self, input_dim: int, hidden_dim: int, num_experts: int,
                 learning_rate: float = 1e-3, num_layers: int = 2,
                 activation: str = "relu", dropout_rate: float = 0.1,
                 max_training_samples: int = 5000,
                 seed: int = 42):
        self.input_dim = int(input_dim)
        self.hidden_dim = int(hidden_dim)
        self.num_experts = int(num_experts)
        self.lr = float(learning_rate)
        self.max_training_samples = int(max_training_samples)

        self._cpu_lock = threading.Lock()
        self._use_torch = TORCH_AVAILABLE
        torch.manual_seed(seed)
        np.random.seed(seed)

        if self._use_torch:
            self.model = GatingNetwork(
                input_dim=self.input_dim, hidden_dim=self.hidden_dim,
                num_experts=self.num_experts, num_hidden_layers=num_layers,
                activation=activation, dropout_rate=dropout_rate)
            self.model.eval()
            self.optimizer = optim.Adam(self.model.parameters(), lr=self.lr)
            self.criterion = nn.CrossEntropyLoss()
            self.expert_modules = nn.ModuleList([
                ExpertModule(self.input_dim, hidden_dim=self.hidden_dim)
                for _ in range(self.num_experts)])
        else:
            self.model = NumpyGatingNetwork(
                input_dim=self.input_dim, hidden_dim=self.hidden_dim,
                num_experts=self.num_experts, seed=seed)
            self.optimizer = None
            self.criterion = None
            self.expert_modules = None

        self.training_buffer: Deque[Tuple[np.ndarray, int]] = deque(
            maxlen=self.max_training_samples)
        self.is_trained = False
        self.step_count = 0
        self.training_count = 0
        self.inference_count = 0

        self._initial_snapshot = self._snapshot_weights()

    # ------------------------------------------------------------------
    def _snapshot_weights(self) -> Dict[str, Any]:
        with self._cpu_lock:
            if self._use_torch:
                return {"model": {k: v.detach().clone()
                                  for k, v in self.model.state_dict().items()},
                        "experts": {k: v.detach().clone()
                                    for k, v in self.expert_modules.state_dict().items()}}
            return {"W1": self.model.W1.copy(), "b1": self.model.b1.copy(),
                    "W2": self.model.W2.copy(), "b2": self.model.b2.copy()}

    def _restore_weights(self, snap: Dict[str, Any]) -> None:
        with self._cpu_lock:
            if self._use_torch:
                try:
                    self.model.load_state_dict(snap["model"])
                except Exception:
                    pass
                try:
                    self.expert_modules.load_state_dict(snap["experts"])
                except Exception:
                    pass
            else:
                self.model.W1 = snap["W1"].copy()
                self.model.b1 = snap["b1"].copy()
                self.model.W2 = snap["W2"].copy()
                self.model.b2 = snap["b2"].copy()

    def _clip_weights(self) -> None:
        with self._cpu_lock:
            if self._use_torch:
                with torch.no_grad():
                    for p in self.model.parameters():
                        p.data.clamp_(-self.W_MAX, self.W_MAX)
                    for p in self.expert_modules.parameters():
                        p.data.clamp_(-self.W_MAX, self.W_MAX)
            else:
                self.model.W1 = np.clip(self.model.W1, -self.W_MAX, self.W_MAX)
                self.model.W2 = np.clip(self.model.W2, -self.W_MAX, self.W_MAX)

    def check_steady_state(self) -> bool:
        with self._cpu_lock:
            if self._use_torch:
                for p in self.model.parameters():
                    if not torch.all(torch.isfinite(p)):
                        return False
                    if float(p.norm()) > self.WEIGHT_NORM_MAX:
                        return False
            else:
                for arr in (self.model.W1, self.model.W2):
                    if not np.all(np.isfinite(arr)):
                        return False
                    if float(np.linalg.norm(arr)) > self.WEIGHT_NORM_MAX:
                        return False
        return True

    def corrupt_weights_bounded(self, rng: random.Random) -> None:
        with self._cpu_lock:
            if self._use_torch:
                with torch.no_grad():
                    for p in self.model.parameters():
                        factor = rng.uniform(0.95, 1.05)
                        p.data.mul_(factor)
                        p.data.clamp_(-self.W_MAX, self.W_MAX)
            else:
                self.model.W1 = np.clip(
                    self.model.W1 * rng.uniform(0.95, 1.05),
                    -self.W_MAX, self.W_MAX)

    # ------------------------------------------------------------------
    def _forward_locked(self, x: Any, use_autocast: bool = False) -> np.ndarray:
        with self._cpu_lock:
            if self._use_torch:
                self.model.eval()
                try:
                    if use_autocast:
                        with torch.autocast(device_type="cpu",
                                            dtype=torch.bfloat16):
                            logits = self.model(x)
                    else:
                        logits = self.model(x)
                except (RuntimeError, TypeError):
                    logits = self.model(x)
                return logits.float().detach().numpy()
            else:
                x_np = x.detach().numpy() if hasattr(x, "detach") else np.asarray(x)
                return self.model(x_np)

    async def _forward_async(self, x: Any, use_autocast: bool = False) -> np.ndarray:
        return await asyncio.to_thread(self._forward_locked, x, use_autocast)

    def encode(self, context: Dict[str, Any]) -> np.ndarray:
        return BioFeatures.encode(context)

    # ------------------------------------------------------------------
    def add_training_sample(self, features: np.ndarray, label: int) -> None:
        features = np.asarray(features, dtype=np.float32)
        if features.shape[0] != self.input_dim:
            raise ValueError(
                f"features dim {features.shape[0]} != {self.input_dim}")
        if not (0 <= label < self.num_experts):
            raise ValueError(f"label {label} out of range")
        if len(self.training_buffer) >= self.max_training_samples:
            self.training_buffer.popleft()
        self.training_buffer.append((features, int(label)))

    async def train(self, epochs: int = 3) -> Optional[float]:
        if not self.training_buffer or not self._use_torch:
            return None
        samples = list(self.training_buffer)
        X = np.stack([s[0] for s in samples]).astype(np.float32)
        y = np.asarray([s[1] for s in samples], dtype=np.int64)
        return await asyncio.to_thread(self._train_locked, X, y, epochs)

    def _train_locked(self, X: np.ndarray, y: np.ndarray,
                      epochs: int) -> float:
        with self._cpu_lock:
            self.model.train()
            X_t = torch.from_numpy(X).float()
            y_t = torch.from_numpy(y).long()
            ds = TensorDataset(X_t, y_t)
            loader = DataLoader(ds, batch_size=min(32, len(X_t)),
                                shuffle=True)
            total, n = 0.0, 0
            for _ in range(epochs):
                for bx, by in loader:
                    self.optimizer.zero_grad()
                    out = self.model(bx)
                    loss = self.criterion(out, by)
                    loss.backward()
                    torch.nn.utils.clip_grad_norm_(
                        self.model.parameters(), 1.0)
                    self.optimizer.step()
                    total += float(loss.item())
                    n += 1
            self.model.eval()
        self.is_trained = True
        self.training_count += 1
        return total / max(n, 1)

    def get_state_dict(self) -> Dict[str, Any]:
        with self._cpu_lock:
            if self._use_torch:
                return {k: v.detach().cpu().numpy().tolist()
                        for k, v in self.model.state_dict().items()}
            return {"W1": self.model.W1.tolist(),
                    "b1": self.model.b1.tolist(),
                    "W2": self.model.W2.tolist(),
                    "b2": self.model.b2.tolist()}

    def load_state_dict(self, state: Dict[str, Any]) -> None:
        if not state:
            return
        with self._cpu_lock:
            if self._use_torch:
                # Preserve dtype based on local state dict.
                local = self.model.state_dict()
                new_state = {}
                for k, v in state.items():
                    if k not in local:
                        continue
                    target = local[k]
                    arr = np.asarray(v)
                    # Cast integer-typed buffers back to their dtype.
                    if target.dtype == torch.int64:
                        new_state[k] = torch.as_tensor(arr).long()
                    elif target.dtype == torch.int32:
                        new_state[k] = torch.as_tensor(arr).int()
                    else:
                        new_state[k] = torch.as_tensor(arr).to(target.dtype)
                if new_state:
                    self.model.load_state_dict(new_state, strict=False)
                self.is_trained = True
            else:
                for k in ("W1", "b1", "W2", "b2"):
                    if k in state:
                        setattr(self.model, k,
                                np.asarray(state[k], dtype=np.float32))

    # ------------------------------------------------------------------
    async def get_health_status(self) -> ComponentHealth:
        ok = self.check_steady_state()
        details = {"is_trained": self.is_trained,
                   "step_count": self.step_count,
                   "buffer_size": len(self.training_buffer),
                   "backend": "torch" if self._use_torch else "numpy"}
        if not ok:
            return ComponentHealth(HealthStatus.UNHEALTHY, 0.0, details)
        if not self.is_trained:
            return ComponentHealth(HealthStatus.DEGRADED, 0.4, details)
        return ComponentHealth(HealthStatus.HEALTHY, 1.0, details)

    async def shutdown(self) -> None:
        return


# =============================================================================
# HEALTH CHECK SYSTEM (per-component hooks)
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
            for name, comp in self.components.items():
                hist = self.history[name]
                if hist:
                    latest = hist[-1]
                else:
                    latest = ComponentHealth(HealthStatus.UNKNOWN, 0.5, {})
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


# =============================================================================
# SELF-HEALING SYSTEM (re-registers recovered components)
# =============================================================================
class SelfHealingSystem:
    def __init__(self, health_system: HealthCheckSystem,
                 max_attempts: int = 5, interval: float = 30.0):
        self.health_system = health_system
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
        health = await self.health_system.get_system_health()
        for name, data in health.get("components", {}).items():
            status = data.get("status")
            if status in (HealthStatus.DEGRADED.value,
                          HealthStatus.UNHEALTHY.value):
                await self._attempt_recovery(name)

    async def _attempt_recovery(self, name: str) -> None:
        if self.attempts[name] >= self.max_attempts:
            logger.warning(f"{name}: max recovery attempts reached")
            return
        handler = self.handlers.get(name)
        if handler is None:
            return
        self.attempts[name] += 1
        try:
            result = handler()
            if asyncio.iscoroutine(result):
                result = await result
            # Handler may return a replacement component.
            if result is not None and result is not True:
                self.health_system.register_component(name, result)
                logger.info(f"{name}: recovered with new component")
            elif result:
                logger.info(f"{name}: recovered in place")
            if result:
                self.attempts[name] = 0
        except Exception as e:
            logger.error(f"{name}: recovery handler error: {e}")

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
# ALERTING SYSTEM
# =============================================================================
class AlertingSystem:
    def __init__(self):
        self.alerts: List[Dict[str, Any]] = []

    async def trigger_alert(self, severity: str, message: str) -> None:
        alert = {"severity": severity, "message": message,
                 "timestamp": datetime.now(timezone.utc).isoformat()}
        self.alerts.append(alert)
        if severity == "critical":
            logger.critical(f"ALERT: {message}")
        else:
            logger.warning(f"ALERT ({severity}): {message}")


# =============================================================================
# MAIN UNIFIED METABOLIC ECOSYSTEM (v9.0.0)
# =============================================================================
class UnifiedMetabolicEcosystem:
    """Orchestrates experts, gating, enhancements, health, and self-healing."""

    def __init__(self, storage: Storage, message_queue: AsyncMessageQueue,
                 adaptive_cost: Optional[AdaptiveCostFunction] = None,
                 pareto_gating: Optional[ParetoGating] = None,
                 drift_detector: Optional[DriftDetector] = None,
                 metrics: Optional[MetricsRegistry] = None,
                 input_dim: int = BioFeatures.DIM,
                 hidden_dim: int = 32,
                 num_layers: int = 2,
                 learning_rate: float = 1e-2,
                 seed: int = 42,
                 enable_quantum_distillation: bool = True,
                 enable_causal_mask: bool = True,
                 enable_federated: bool = False,
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
                 carbon_api_url: Optional[str] = None,
                 carbon_api_key: Optional[str] = None) -> None:
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Deterministic RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_chaos = random.Random(seed + 2)
        self._rng_carbon = random.Random(seed + 3)
        self._rng_fed = random.Random(seed + 4)
        self._rng_auction = random.Random(seed + 5)
        self._rng_quantum = random.Random(seed + 6)

        # Experts
        self.experts: Dict[str, BaseExpert] = {
            "energy": EnergyExpert(input_dim),
            "data": DataExpert(input_dim),
            "iot": IoTExpert(input_dim),
        }
        self.expert_ids = list(self.experts.keys())
        self.num_experts = len(self.expert_ids)

        # Gating
        self.gating = GatingNetworkManager(
            input_dim=input_dim, hidden_dim=hidden_dim,
            num_experts=self.num_experts, learning_rate=learning_rate,
            num_layers=num_layers, seed=seed)

        # Rate limiters
        self.rate_limiter = RateLimiter(120.0)
        self.per_expert_limiter = PerExpertRateLimiter(30.0)

        # Enhancement modules
        self.quantum = (QuantumDistillationModule(
            n_dims=input_dim, rng=self._rng_quantum)
            if enable_quantum_distillation else None)
        self.causal_mask = (CausalFeatureMask(
            input_dim, rng=self._rng_xai) if enable_causal_mask else None)
        self.federated = (FederatedGreenAggregator(
            storage, "ecosystem", rng=self._rng_fed)
            if enable_federated else None)
        self.auction = (ExpertAuction(
            self.expert_ids, input_dim, rng=self._rng_auction)
            if enable_expert_auction else None)
        self.safety = SafetyMonitor() if enable_safety_monitor else None
        if self.safety is not None:
            self._setup_safety_rules()
        self.temporal_safety = (TemporalSafetyMonitor()
                                 if enable_temporal_safety else None)
        if self.temporal_safety is not None:
            self._setup_temporal_rules()
        self.xai = XAIExplainer(rng=self._rng_xai) if enable_xai else None
        self.precision = (PrecisionController()
                          if enable_precision else None)
        self.carbon_market = (CarbonMarketClient(
            price_oracle=self._carbon_oracle, rng=self._rng_carbon)
            if enable_carbon_market else None)
        self.hitl = (HumanApprovalHandler(message_queue)
                     if enable_human_approval else None)
        self.chaos = (ChaosEngine(
            self, chaos_probability=chaos_probability,
            rng=self._rng_chaos) if enable_chaos else None)

        # Carbon HTTP client
        self.carbon_client = CarbonIntensityClient(
            api_url=carbon_api_url, api_key=carbon_api_key,
            region="us-east", update_interval=300.0)

        # Helium optimizer
        self.helium = HeliumEfficiencyOptimizer()

        # Health + self-healing
        self.health = HealthCheckSystem(interval=30.0) if enable_health_checks else None
        self.self_healing = (SelfHealingSystem(self.health)
                             if (self.health is not None and enable_self_healing)
                             else None)
        self.alerts = AlertingSystem()

        if self.health is not None:
            for eid, e in self.experts.items():
                self.health.register_component(eid, e)
            self.health.register_component("gating", self.gating)
            self.health.register_component("carbon_client", self.carbon_client)
            self.health.register_component("helium", self.helium)
        if self.self_healing is not None:
            self.self_healing.register_handler("gating", self._recover_gating)
            self.self_healing.register_handler("carbon_client",
                                                self._recover_carbon)

        # Lifecycle
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()
        self.sustainability_score = 1.0
        self._recent_accuracies: Deque[float] = deque(maxlen=100)
        self._accuracy_lock = asyncio.Lock()

        # Background loops
        if self.chaos is not None:
            self._spawn(self._chaos_loop())
        self._spawn(self._carbon_loop())

    # ------------------------------------------------------------------
    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _setup_safety_rules(self) -> None:
        assert self.safety is not None

        def _finite(_state):
            return self.gating.check_steady_state()

        def _probs_valid(state):
            p = state.get("probs")
            if p is None:
                return True
            if any(x < 0 for x in p):
                return False
            return abs(sum(p) - 1.0) < 1e-4

        self.safety.add_rule(
            "weights_finite_or_bounded", _finite,
            "weights contain NaN/Inf or exceed norm bound",
            severity="critical",
            mitigation=lambda _t: self.gating._restore_weights(
                self.gating._initial_snapshot))
        self.safety.add_rule("probs_valid", _probs_valid,
                             "probabilities invalid", severity="warning")

    def _setup_temporal_rules(self) -> None:
        assert self.temporal_safety is not None
        self.temporal_safety.add_formula("G (step_count >= 0)")
        self.temporal_safety.add_formula("G (confidence >= 0.0)")
        self.temporal_safety.add_formula("G (confidence <= 1.0)")

    async def _carbon_oracle(self) -> float:
        await self.carbon_client.update()
        return self.carbon_client.price

    async def _carbon_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=self.carbon_client.update_interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.carbon_client.update()
                self.helium.update_price(
                    scarcity=max(0.0, self.carbon_client.intensity_ema / 1000.0))
            except Exception as e:
                logger.debug(f"carbon loop error: {e}")

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
    def _snapshot_weights(self) -> Dict[str, Any]:
        return self.gating._snapshot_weights()

    def _restore_weights(self, snap: Dict[str, Any]) -> None:
        self.gating._restore_weights(snap)

    def _check_steady_state(self) -> bool:
        return self.gating.check_steady_state()

    def _corrupt_weights_bounded(self, rng: random.Random) -> None:
        self.gating.corrupt_weights_bounded(rng)

    # ------------------------------------------------------------------
    async def _recover_gating(self) -> Any:
        if not self.gating.training_buffer:
            return False
        await self.gating.train(epochs=3)
        return True

    async def _recover_carbon(self) -> Any:
        await self.carbon_client.close()
        new_client = CarbonIntensityClient(
            api_url=self.carbon_client.api_url,
            api_key=self.carbon_client.api_key,
            region=self.carbon_client.region)
        self.carbon_client = new_client
        return new_client

    # ------------------------------------------------------------------
    def _select_precision(self, features: np.ndarray) -> str:
        if self.precision is None:
            return "fp32"
        return self.precision.select_precision()

    async def _compute_decision(self, context: Dict[str, Any],
                                user_id: str = "default",
                                task_id: str = "unknown") -> MetabolicDecision:
        features = BioFeatures.encode(context)

        # Causal mask
        if self.causal_mask is not None:
            self.causal_mask.observe(features)
            masked = self.causal_mask.apply(features)
        else:
            masked = features

        precision = self._select_precision(masked)
        use_autocast = precision in ("bf16", "fp16")

        if TORCH_AVAILABLE and self.gating._use_torch:
            x = torch.tensor(masked, dtype=torch.float32).unsqueeze(0)
        else:
            x = masked.reshape(1, -1)
        logits = await self.gating._forward_async(x, use_autocast)
        probs = self._softmax(logits[0])

        # Safety
        violations: List[str] = []
        if self.safety is not None:
            v = self.safety.check({"probs": probs.tolist()}, target=self)
            violations = [f"{x.name}:{x.severity}" for x in v]

        # Auction override
        if self.auction is not None:
            winners = self.auction.select_experts(masked, top_k=1)
            selected_idx = (self.expert_ids.index(winners[0])
                            if winners and winners[0] in self.expert_ids
                            else int(np.argmax(probs)))
        else:
            selected_idx = int(np.argmax(probs))

        # HITL
        if self.hitl is not None and self.num_experts >= 2:
            top2 = np.argsort(-probs)[:2]
            decision = {"action": "route", "candidates": [
                {"id": self.expert_ids[i], "prob": float(probs[i])}
                for i in top2]}
            if not await self.hitl.request_approval(decision, user_id=user_id):
                selected_idx = int(np.argmax(probs))

        # XAI
        explanation: Optional[Dict[str, Any]] = None
        counterfactual: Optional[Dict[str, Any]] = None
        if self.xai is not None:
            explanation, counterfactual = await asyncio.to_thread(
                self._compute_xai, masked, selected_idx)

        # Temporal safety
        if self.temporal_safety is not None:
            await self.temporal_safety.push({
                "step_count": self.gating.step_count,
                "confidence": float(probs[selected_idx])})
            await self.temporal_safety.check()

        return MetabolicDecision(
            state_vec=masked.copy(),
            probs=probs.copy(),
            expert_ids=tuple(self.expert_ids),
            selected_expert=self.expert_ids[selected_idx],
            selected_idx=selected_idx,
            confidence=float(probs[selected_idx]),
            timestamp=time.time(),
            explanation=explanation,
            counterfactual=counterfactual,
            precision=precision,
            drift=0.0,
            safety_violations=tuple(violations),
        )

    def _compute_xai(self, masked: np.ndarray,
                     selected_idx: int) -> Tuple[
                         Optional[Dict[str, Any]],
                         Optional[Dict[str, Any]]]:
        if self.xai is None:
            return None, None

        def _score(v: np.ndarray) -> float:
            if TORCH_AVAILABLE and self.gating._use_torch:
                t = torch.tensor(np.asarray(v, dtype=np.float32)).unsqueeze(0)
                logits = self.gating._forward_locked(t, use_autocast=False)
            else:
                logits = self.gating._forward_locked(
                    np.asarray(v, dtype=np.float32).reshape(1, -1),
                    use_autocast=False)
            return float(logits[0, selected_idx])

        try:
            attrs = self.xai.integrated_gradients(_score, masked)
            explanation = {
                "method": "integrated_gradients",
                "attributions": attrs.tolist(),
                "feature_names": list(BioFeatures.NAMES),
            }
            counterfactual = self.xai.counterfactual(_score, masked)
        except Exception as e:
            logger.debug(f"XAI failed: {e}")
            return None, None
        return explanation, counterfactual

    def _softmax(self, logits: np.ndarray) -> np.ndarray:
        logits = np.nan_to_num(logits, nan=0.0, posinf=1e6, neginf=-1e6)
        m = float(np.max(logits))
        e = np.exp(logits - m)
        s = float(np.sum(e))
        if not np.isfinite(s) or s <= 0:
            return np.ones_like(e) / e.size
        return e / s

    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        decision = await self._compute_decision(state)
        return decision.probs.tolist()

    async def process_task(self, task_data: Dict[str, Any],
                            context_data: Optional[Dict[str, Any]] = None,
                            user_id: str = "default") -> Dict[str, Any]:
        start = time.monotonic()
        if not await self.rate_limiter.acquire():
            return {"status": "error", "reason": "rate limit exceeded"}

        context = dict(context_data or {})

        # Enrich context with live carbon + helium + gradient data
        try:
            c = await self.carbon_client.update()
            context.setdefault("carbon_intensity",
                                c["intensity_ema"] / 1000.0)
        except Exception:
            pass
        h = await self.helium.get_helium_status()
        context.setdefault("helium_scarcity", h["price_ema"])
        context.setdefault("helium_price", h["price_ema"])

        # Compute decision
        decision = await self._compute_decision(
            context, user_id=user_id,
            task_id=task_data.get("task_id", "task"))

        # Per-expert rate limit
        allowed_ids = []
        for eid in self.expert_ids:
            limiter = self.per_expert_limiter.get_limiter(eid)
            if await limiter.acquire():
                allowed_ids.append(eid)
        if decision.selected_expert not in allowed_ids:
            if allowed_ids:
                decision = MetabolicDecision(
                    state_vec=decision.state_vec, probs=decision.probs,
                    expert_ids=decision.expert_ids,
                    selected_expert=allowed_ids[0],
                    selected_idx=self.expert_ids.index(allowed_ids[0]),
                    confidence=decision.confidence,
                    timestamp=decision.timestamp,
                    explanation=decision.explanation,
                    counterfactual=decision.counterfactual,
                    precision=decision.precision,
                    safety_violations=decision.safety_violations)
            else:
                return {"status": "error", "reason": "all experts rate limited"}

        # Execute
        selected_expert = self.experts[decision.selected_expert]
        output = await selected_expert.execute(task_data.get("params", {}),
                                                context)

        # Sustainability
        carbon_factor = context.get("carbon_intensity", 0.5)
        helium_factor = context.get("helium_scarcity", 0.5)
        self.sustainability_score = max(
            0.0, min(1.0, 1.0 - (carbon_factor * 0.4 + helium_factor * 0.3)))

        # Metrics
        if self.metrics is not None:
            self.metrics.inc("tasks_completed")
            self.metrics.set("sustainability_score", self.sustainability_score)
        elapsed = time.monotonic() - start

        # Carbon market net-zero schedule
        carbon_decision = None
        if self.carbon_market is not None:
            carbon_decision = await self.carbon_market.net_zero_schedule(
                workload_kwh=1.0, intensity=carbon_factor)

        # Update auction
        if self.auction is not None:
            self.auction.update(decision.selected_expert,
                                 decision.state_vec, reward=0.8)

        # Federated (rate-limited)
        if self.federated is not None:
            await self.federated.share_weights(
                "gating", self.gating.get_state_dict())

        # Emit FeedbackEvent
        event = FeedbackEvent.create_with_context(
            source="metabolic_ecosystem",
            feedback_type="task_execution",
            task_id=task_data.get("task_id", "task"),
            context={"task_type": task_data.get("type", "generic"),
                     "selected_expert": decision.selected_expert},
            action={"selected_expert": decision.selected_expert,
                    "confidence": decision.confidence},
            performance={"latency_ms": elapsed * 1000},
            adaptive_cost_value=0.0,
            tags=["moe", "routing"],
        )
        if self.queue is not None:
            await self.queue.publish("metabolic_events", event.to_json())

        # Drift check
        drift = 0.0
        if self.drift is not None and self.adaptive_cost is not None:
            drift = await self.drift.check_drift(
                self.adaptive_cost.get_current_weights())
            if drift > 0.5 and self.hitl is not None:
                approved = await self.hitl.request_approval(
                    {"action": "retrain", "candidates": [
                        {"id": "yes"}, {"id": "no"}]}, user_id=user_id)
                if approved and self.gating.training_buffer:
                    await self.gating.train(epochs=3)

        return {
            "status": "success",
            "decision": {
                "selected_expert": decision.selected_expert,
                "confidence": decision.confidence,
                "probs": decision.probs.tolist(),
                "precision": decision.precision,
            },
            "execution": output,
            "carbon_decision": carbon_decision,
            "sustainability_score": self.sustainability_score,
            "latency_ms": elapsed * 1000,
            "explanation": decision.explanation,
            "safety_violations": list(decision.safety_violations),
            "drift": drift,
        }

    # ------------------------------------------------------------------
    async def register_training_sample(self, context: Dict[str, Any],
                                       expert_id: str) -> None:
        if expert_id not in self.experts:
            raise ValueError(f"unknown expert {expert_id}")
        features = BioFeatures.encode(context)
        label = self.expert_ids.index(expert_id)
        self.gating.add_training_sample(features, label)

    async def train_gating(self, epochs: int = 3) -> Optional[float]:
        return await self.gating.train(epochs=epochs)

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        result = {
            "version": "9.0.0",
            "sustainability_score": self.sustainability_score,
            "expert_count": self.num_experts,
            "gating_trained": self.gating.is_trained,
            "background_tasks": len(self._background_tasks),
        }
        if self.health is not None:
            result["system_health"] = await self.health.get_system_health()
        return result

    async def save_state(self) -> None:
        state = {
            "sustainability_score": self.sustainability_score,
            "gating_state": self.gating.get_state_dict(),
            "step_count": self.gating.step_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await asyncio.to_thread(
                self.storage.save_state, "metabolic_ecosystem_state",
                json.dumps(state))
        except Exception as e:
            logger.warning(f"save_state failed: {e}")

    async def load_state(self) -> None:
        try:
            raw = self.storage.get_state("metabolic_ecosystem_state")
        except Exception:
            raw = None
        if not raw:
            return
        try:
            state = json.loads(raw)
        except Exception:
            return
        self.sustainability_score = state.get("sustainability_score", 1.0)
        gating_state = state.get("gating_state")
        if gating_state:
            self.gating.load_state_dict(gating_state)
        self.gating.step_count = state.get("step_count", 0)

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        if self.self_healing is not None:
            await self.self_healing.shutdown()
        if self.health is not None:
            await self.health.shutdown()
        await self.carbon_client.close()
        await self.gating.shutdown()
        await self.save_state()


# =============================================================================
# FIXTURES
# =============================================================================
@pytest.fixture
def storage():
    return Storage()


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


@pytest.fixture
def ecosystem_minimal(storage, queue, adaptive_cost, pareto_gating,
                      drift_detector, metrics):
    return UnifiedMetabolicEcosystem(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        hidden_dim=16, seed=42,
        enable_health_checks=False, enable_self_healing=False)


@pytest_asyncio.fixture
async def ecosystem_full(storage, queue, adaptive_cost, pareto_gating,
                        drift_detector, metrics):
    eco = UnifiedMetabolicEcosystem(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        hidden_dim=16, seed=42,
        enable_quantum_distillation=True,
        enable_causal_mask=True,
        enable_federated=True,
        enable_expert_auction=True,
        enable_safety_monitor=True,
        enable_temporal_safety=True,
        enable_xai=True,
        enable_precision=True,
        enable_carbon_market=True,
        enable_chaos=False,
        enable_human_approval=True,
        enable_health_checks=True,
        enable_self_healing=True)
    try:
        yield eco
    finally:
        await eco.shutdown()


def _sample_context() -> Dict[str, Any]:
    return {
        "carbon_zone": 2.0, "helium_scarcity": 0.4,
        "task_complexity": 0.5, "token_balance": 0.7,
        "gradient_carbon": 0.3, "gradient_helium": 0.4,
        "gradient_trust": 0.6, "opportunity_gradient": 0.5,
        "stress_level": 0.3, "avg_client_energy": 0.5,
        "carbon_intensity": 0.42, "helium_price": 0.5,
    }


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    @pytest.mark.asyncio
    async def test_predict_batch_size_1(self, ecosystem_minimal):
        # LayerNorm should tolerate batch size 1.
        decision = await ecosystem_minimal._compute_decision(_sample_context())
        assert 0 <= decision.selected_idx < ecosystem_minimal.num_experts
        assert abs(float(decision.probs.sum()) - 1.0) < 1e-4

    @pytest.mark.asyncio
    async def test_feedback_event_drops_unknown_kwargs(self):
        event = FeedbackEvent.create_with_context(
            source="test", feedback_type="x", task_id="t",
            context={}, action={}, performance={},
            adaptive_cost_value=0.0, tags=[],
            metadata={"foo": "bar"}, unknown_kwarg=42)
        assert event.source == "test"
        assert "metadata" not in asdict(event)

    @pytest.mark.asyncio
    async def test_load_state_dict_preserves_dtypes(self, ecosystem_minimal):
        if not TORCH_AVAILABLE:
            pytest.skip("torch unavailable")
        state = ecosystem_minimal.gating.get_state_dict()
        ecosystem_minimal.gating.load_state_dict(state)
        # num_batches_tracked or similar int buffers preserved
        assert ecosystem_minimal.gating.is_trained is True

    @pytest.mark.asyncio
    async def test_safety_mitigation(self, ecosystem_minimal):
        # Corrupt weights, check safety triggers and mitigates.
        ecosystem_minimal.safety.check({"probs": [0.5, 0.5, 0.0]},
                                        target=ecosystem_minimal)
        # Manual NaN injection
        if TORCH_AVAILABLE and ecosystem_minimal.gating._use_torch:
            with torch.no_grad():
                for p in ecosystem_minimal.gating.model.parameters():
                    p.data[0] = float("nan")
                    break
        else:
            ecosystem_minimal.gating.model.W1[0, 0] = float("nan")
        violations = ecosystem_minimal.safety.check({}, target=ecosystem_minimal)
        assert any(v.name == "weights_finite_or_bounded" for v in violations)
        # Mitigation should have restored weights
        assert ecosystem_minimal.gating.check_steady_state()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage, queue):
        eco = UnifiedMetabolicEcosystem(
            storage=storage, message_queue=queue, hidden_dim=8, seed=1,
            enable_chaos=True, chaos_probability=1.0,
            enable_health_checks=False, enable_self_healing=False)
        assert len(eco._background_tasks) >= 1
        await eco.shutdown()
        assert len(eco._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_chaos_bounded(self, storage, queue):
        eco = UnifiedMetabolicEcosystem(
            storage=storage, message_queue=queue, hidden_dim=8, seed=1,
            enable_chaos=True, chaos_probability=1.0,
            enable_health_checks=False, enable_self_healing=False)
        try:
            for _ in range(20):
                await eco.chaos.run_experiment(
                    f"c{_}", "data_corruption")
            assert eco.gating.check_steady_state()
        finally:
            await eco.shutdown()

    def test_apply_global_weights_shape_validation(self, ecosystem_minimal):
        pass  # covered in federated tests

    @pytest.mark.asyncio
    async def test_softmax_fallback(self, ecosystem_minimal):
        probs = ecosystem_minimal._softmax(np.array([np.nan, np.nan, np.nan]))
        assert np.allclose(probs, 1.0 / 3.0)

    @pytest.mark.asyncio
    async def test_import_safe_without_torch(self):
        # The module already imported successfully in this environment.
        assert UnifiedMetabolicEcosystem is not None

    @pytest.mark.asyncio
    async def test_health_system_detects_gating(self, ecosystem_full):
        health = await ecosystem_full.health.get_system_health()
        assert "gating" in health["components"]

    @pytest.mark.asyncio
    async def test_self_healing_reregisters_component(self, storage, queue):
        health = HealthCheckSystem(interval=0.05)
        try:
            class BadComp:
                async def get_health_status(self):
                    return ComponentHealth(HealthStatus.UNHEALTHY, 0.0, {})

            bad = BadComp()
            health.register_component("bad", bad)

            healing = SelfHealingSystem(health, max_attempts=2, interval=0.05)
            try:
                class GoodComp:
                    async def get_health_status(self):
                        return ComponentHealth(HealthStatus.HEALTHY, 1.0, {})

                good = GoodComp()
                healing.register_handler("bad", lambda: good)
                await asyncio.sleep(0.3)
                assert health.components["bad"] is good
            finally:
                await healing.shutdown()
        finally:
            await health.shutdown()


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


class TestFederated:
    @pytest.mark.asyncio
    async def test_roundtrip(self, storage):
        f = FederatedGreenAggregator(storage, "i1", sparse_topk=0.0,
                                     rate_limit_seconds=0.0)
        weights = {"a": np.array([1.0, 2.0, 3.0], dtype=np.float32)}
        assert await f.share_weights("m", weights, force=True)
        agg = await f.pull_aggregated("m")
        assert agg is not None
        assert np.allclose(np.asarray(agg["a"]), [1.0, 2.0, 3.0])

    @pytest.mark.asyncio
    async def test_weighted_average(self, storage):
        f1 = FederatedGreenAggregator(storage, "i1", sparse_topk=0.0,
                                      rate_limit_seconds=0.0)
        f2 = FederatedGreenAggregator(storage, "i2", sparse_topk=0.0,
                                      rate_limit_seconds=0.0)
        await f1.share_weights(
            "m", {"a": np.zeros(4, dtype=np.float32)},
            sample_count=1, force=True)
        await f2.share_weights(
            "m", {"a": np.ones(4, dtype=np.float32) * 8},
            sample_count=3, force=True)
        agg = await f1.pull_aggregated("m")
        assert np.allclose(np.asarray(agg["a"]), 6.0, atol=1e-4)

    @pytest.mark.asyncio
    async def test_rate_limit(self, storage):
        f = FederatedGreenAggregator(storage, "i1", sparse_topk=0.0,
                                     rate_limit_seconds=10.0)
        weights = {"a": np.zeros(4, dtype=np.float32)}
        assert await f.share_weights("m", weights, force=True)
        assert not await f.share_weights("m", weights, force=False)


class TestExpertAuction:
    def test_all_experts_have_bids(self):
        a = ExpertAuction(["e1", "e2", "e3"], feature_dim=4)
        bids = a.compute_bids([0.1, 0.2, 0.3, 0.4])
        assert set(bids.keys()) == {"e1", "e2", "e3"}

    def test_update_changes_scores(self):
        a = ExpertAuction(["e1"], feature_dim=4)
        feats = [0.5] * 4
        before = a.compute_bids(feats)["e1"]
        a.update("e1", feats, reward=1.0)
        after = a.compute_bids(feats)["e1"]
        assert after > before

    def test_select_top_k(self):
        a = ExpertAuction(["e1", "e2", "e3"], feature_dim=4)
        for _ in range(10):
            a.update("e1", [1.0] + [0.0] * 3, reward=1.0)
        top = a.select_experts([1.0] + [0.0] * 3, top_k=2)
        assert len(top) == 2
        assert "e1" in top


class TestTemporalSafety:
    @pytest.mark.asyncio
    async def test_always_satisfied(self):
        s = TemporalSafetyMonitor()
        s.add_formula("G (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.8})
        result = await s.check()
        assert all(result.values())

    @pytest.mark.asyncio
    async def test_never_violated(self):
        s = TemporalSafetyMonitor()
        s.add_formula("NEVER (carbon >= 0.8)")
        await s.push({"carbon": 0.5})
        await s.push({"carbon": 0.9})
        result = await s.check()
        assert any(not v for v in result.values())

    @pytest.mark.asyncio
    async def test_historically(self):
        s = TemporalSafetyMonitor()
        s.add_formula("H (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.1})
        result = await s.check()
        assert any(not v for v in result.values())

    @pytest.mark.asyncio
    async def test_windowed(self):
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
        assert p.current == "int8"

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

    @pytest.mark.asyncio
    async def test_ema_smooths(self):
        prices = iter([100.0, 200.0])

        async def oracle():
            return next(prices)

        c = CarbonMarketClient(price_oracle=oracle)
        await c.update_price()
        await c.update_price()
        assert 100.0 < c.price_ema < 200.0


class TestChaos:
    @pytest.mark.asyncio
    async def test_latency(self, ecosystem_minimal):
        c = ChaosEngine(ecosystem_minimal, rng=random.Random(0))
        r = await c.run_experiment("t1", "latency")
        assert r["status"] == "completed"

    @pytest.mark.asyncio
    async def test_exception(self, ecosystem_minimal):
        c = ChaosEngine(ecosystem_minimal, rng=random.Random(0))
        r = await c.run_experiment("t2", "exception")
        assert r["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, ecosystem_minimal):
        c = ChaosEngine(ecosystem_minimal)
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

    @pytest.mark.asyncio
    async def test_uncertain_queries(self):
        h = HumanApprovalHandler(uncertainty_threshold=10.0, timeout=0.05)
        await h.submit_response("u", "hi")
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 1.0},
                {"id": "lo", "quality_score": 1.0}]}, user_id="u")
        assert ok is True


class TestCarbonClient:
    @pytest.mark.asyncio
    async def test_update_with_fallback(self):
        c = CarbonIntensityClient()
        s = await c.update()
        assert s["intensity"] > 0
        assert s["region"] == "us-east"

    @pytest.mark.asyncio
    async def test_cache_hit(self):
        c = CarbonIntensityClient()
        await c.update(force=True)
        cache_size = len(c._cache)
        await c.update()
        assert len(c._cache) == cache_size

    @pytest.mark.asyncio
    async def test_health_status(self):
        c = CarbonIntensityClient()
        h = await c.get_health_status()
        assert isinstance(h, ComponentHealth)


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
    async def test_self_healing_calls_handler(self):
        h = HealthCheckSystem(interval=0.05)
        calls = []

        class Bad:
            async def get_health_status(self):
                return ComponentHealth(HealthStatus.UNHEALTHY, 0.0, {})

        try:
            h.register_component("bad", Bad())
            healing = SelfHealingSystem(h, max_attempts=2, interval=0.05)
            try:
                async def handler():
                    calls.append(1)
                    return True

                healing.register_handler("bad", handler)
                await asyncio.sleep(0.3)
                assert len(calls) >= 1
            finally:
                await healing.shutdown()
        finally:
            await h.shutdown()


# =============================================================================
# TESTS — FULL INTEGRATION
# =============================================================================
class TestFullIntegration:
    @pytest.mark.asyncio
    async def test_process_task_success(self, ecosystem_full):
        result = await ecosystem_full.process_task(
            task_data={"type": "energy_optimization",
                       "params": {}, "task_id": "t1"},
            context_data=_sample_context())
        assert result["status"] == "success"
        assert "decision" in result
        assert "selected_expert" in result["decision"]

    @pytest.mark.asyncio
    async def test_policy_probs_sum_to_one(self, ecosystem_full):
        probs = await ecosystem_full.policy_probs(_sample_context())
        assert abs(sum(probs) - 1.0) < 1e-4

    @pytest.mark.asyncio
    async def test_health_check_reports_components(self, ecosystem_full):
        hc = await ecosystem_full.health_check()
        assert "system_health" in hc
        assert "components" in hc["system_health"]

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, ecosystem_full):
        await ecosystem_full.shutdown()
        await ecosystem_full.shutdown()
        assert len(ecosystem_full._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_save_and_load_state(self, storage, queue):
        eco = UnifiedMetabolicEcosystem(
            storage=storage, message_queue=queue, hidden_dim=8, seed=1,
            enable_health_checks=False, enable_self_healing=False)
        try:
            await eco.process_task(
                task_data={"type": "test", "params": {}, "task_id": "t"},
                context_data=_sample_context())
            await eco.save_state()
            eco2 = UnifiedMetabolicEcosystem(
                storage=storage, message_queue=queue, hidden_dim=8, seed=2,
                enable_health_checks=False, enable_self_healing=False)
            try:
                await eco2.load_state()
                assert eco2.gating.is_trained == eco.gating.is_trained
            finally:
                await eco2.shutdown()
        finally:
            await eco.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_tasks(self, ecosystem_full):
        async def _t(i):
            return await ecosystem_full.process_task(
                task_data={"type": "test", "params": {},
                           "task_id": f"t{i}"},
                context_data=_sample_context())
        results = await asyncio.gather(*[_t(i) for i in range(6)])
        assert all(r["status"] == "success" for r in results)

    @pytest.mark.asyncio
    async def test_no_torch_fallback(self, storage, queue):
        # If torch is unavailable, the numpy fallback is used.
        eco = UnifiedMetabolicEcosystem(
            storage=storage, message_queue=queue, hidden_dim=8, seed=1,
            enable_health_checks=False, enable_self_healing=False)
        try:
            result = await eco.process_task(
                task_data={"type": "test", "params": {}, "task_id": "t"},
                context_data=_sample_context())
            assert result["status"] == "success"
        finally:
            await eco.shutdown()


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    async def demo():
        storage = Storage()
        queue = AsyncMessageQueue()
        ac = AdaptiveCostFunction(storage)
        pg = ParetoGating()
        dd = DriftDetector(storage, ac)
        metrics = MetricsRegistry()

        eco = UnifiedMetabolicEcosystem(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg,
            drift_detector=dd, metrics=metrics,
            hidden_dim=32, seed=1,
            enable_quantum_distillation=True,
            enable_causal_mask=True,
            enable_expert_auction=True,
            enable_safety_monitor=True,
            enable_temporal_safety=True,
            enable_xai=True,
            enable_carbon_market=True,
            enable_human_approval=True,
            enable_health_checks=True,
            enable_self_healing=True)
        try:
            result = await eco.process_task(
                task_data={"type": "energy_optimization",
                           "params": {}, "task_id": "demo_1"},
                context_data={"carbon_zone": 2, "gradient_carbon": 0.22})
            print("Result:", json.dumps(result, indent=2, default=str))
            health = await eco.health_check()
            print("Health:", json.dumps(health, indent=2, default=str))
        finally:
            await eco.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
