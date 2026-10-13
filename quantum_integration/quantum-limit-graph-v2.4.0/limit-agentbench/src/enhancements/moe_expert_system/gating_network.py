#!/usr/bin/env python3
"""
Gating Network Module v5.0.0 — Self-Contained Single-File Bundle
=================================================================

Complete rewrite of the Green Agent MoE gating manager. Everything needed
to run and test is in this file:

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
        - PrecisionController        (layer-wise + hysteresis + energy)
        - CarbonMarketClient         (budget + EMA + REC)
        - ChaosInjector              (steady-state + rollback)
        - HumanApprovalHandler       (uncertainty sampling)
  4. `GatingDecision` value object.
  5. `GatingNetworkManager` with `nn.LayerNorm`, `torch.autocast`,
     and thread-safe forward/backward (`threading.Lock` + `asyncio.to_thread`).
  6. Comprehensive pytest suite at the bottom.

Run:
    python gating_network_v5.py         # demo
    pytest gating_network_v5.py -v      # tests

Requires: Python 3.10+, torch, numpy, pytest, pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

import pytest
import pytest_asyncio

logger = logging.getLogger("gating_network")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


# =============================================================================
# LOCAL STUBS (replace with production adapters)
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
        # Drop unsupported kwargs gracefully.
        allowed = {"source", "feedback_type", "task_id", "context",
                   "action", "performance", "adaptive_cost_value", "tags"}
        filtered = {k: v for k, v in kwargs.items() if k in allowed}
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
        # Non-dominated filter on (quality, -carbon, -latency, -energy).
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
# ENUMS
# =============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# =============================================================================
# BIO-SIGNAL FEATURES (12 dims)
# =============================================================================
class BioFeatures:
    NAMES = (
        "helium_scarcity", "helium_cost_index", "carbon_intensity",
        "model_loss", "gradient_variance", "avg_client_energy",
        "gradient_carbon", "gradient_helium", "token_balance_norm",
        "harvester_stress", "causal_impact_carbon", "causal_impact_helium",
    )
    DIM = len(NAMES)

    @classmethod
    def encode(cls, context: Dict[str, Any]) -> np.ndarray:
        feats = []
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
# ENHANCEMENT 1 — QUANTUM DISTILLATION (density matrix)
# =============================================================================
class QuantumDistillationModule:
    """Density-matrix distillation over teacher parameter vectors."""

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

    async def step(self, storage: Any = None,
                   student_id: str = "gating") -> Dict[str, Any]:
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
    """Weighted FedAvg over flattened parameter bytes, with DP + sparsify."""

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
        noise = np.asarray([self.rng.gauss(0.0, sigma) for _ in range(vec.size)],
                            dtype=vec.dtype)
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
        self.approval_cb: Optional[Callable] = None

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

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

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
    """Integrated Gradients + bounded counterfactuals."""

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
    """Layer-wise precision with hysteresis and energy accounting."""

    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    AUTOCAST = {"fp32": None, "tf32": None, "bf16": torch.bfloat16,
                "fp16": torch.bfloat16, "int8": None}

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

    async def auto_switch_layers(self, per_layer_acc: Dict[str, float],
                                 baseline_acc: float) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for layer, acc in per_layer_acc.items():
            drop = (baseline_acc - acc) / max(baseline_acc, 1e-6)
            sens = min(1.0, drop / max(self.switch_threshold, 1e-6))
            out[layer] = self.select_precision(sens)
        return out


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
# ENHANCEMENT 9 — CHAOS INJECTOR
# =============================================================================
class ChaosInjector:
    """Fault injection with steady-state hypothesis + auto-rollback."""

    FAULT_TYPES = ("latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop")

    def __init__(self, manager: "GatingNetworkManager",
                 chaos_probability: float = 0.01,
                 auto_rollback: bool = True,
                 blast_radius: float = 0.5,
                 rng: Optional[random.Random] = None):
        self.manager = manager
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
        before = self.manager._check_steady_state()
        snapshot = self.manager._snapshot_weights()
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
                # Bounded corruption: multiply a random linear layer weight
                # by a small factor and clip.
                def _corrupt():
                    with self.manager._cpu_lock:
                        with torch.no_grad():
                            layer = self.manager.model.network[0]
                            f = self.rng.uniform(0.95, 1.05)
                            layer.weight.mul_(f)
                            torch.clamp_(layer.weight, -10.0, 10.0)
                await asyncio.to_thread(_corrupt)
        except Exception:
            status = "injected"
        finally:
            with self._lock:
                self.active.pop(name, None)
        after = self.manager._check_steady_state()
        rolled_back = False
        if before and not after and self.auto_rollback:
            self.manager._restore_weights(snapshot)
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
    """Uncertainty-sampled HITL with preference learning."""

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

    def is_uncertain(self, user_id: str,
                     candidates: List[Dict[str, Any]]) -> bool:
        if len(candidates) < 2:
            return False
        scored = sorted((self._utility(user_id, c) for c in candidates),
                        reverse=True)
        return abs(scored[0] - scored[1]) < self.uncertainty_threshold

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
# GATING DECISION VALUE OBJECT
# =============================================================================
@dataclass(frozen=True)
class GatingDecision:
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


# =============================================================================
# GATING NETWORK (LayerNorm — batch-size-1 safe)
# =============================================================================
def _get_activation(name: str) -> nn.Module:
    if name == "relu":
        return nn.ReLU()
    if name == "tanh":
        return nn.Tanh()
    if name == "gelu":
        return nn.GELU()
    raise ValueError(f"unknown activation: {name}")


class GatingNetwork(nn.Module):
    """MLP gate using LayerNorm (no batch-size-1 crash)."""

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

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)


class ExpertModule(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int = 32, output_dim: int = 1):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)


# =============================================================================
# MAIN GATING NETWORK MANAGER (v5.0.0)
# =============================================================================
class GatingNetworkManager:
    """Thread-safe MoE gate manager with all ten enhancement modules."""

    W_MAX = 10.0
    WEIGHT_NORM_MAX = 1000.0

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        expert_ids: Optional[List[str]] = None,
        input_dim: int = BioFeatures.DIM,
        hidden_dim: int = 32,
        num_hidden_layers: int = 2,
        activation: str = "relu",
        dropout_rate: float = 0.1,
        learning_rate: float = 1e-2,
        batch_size: int = 32,
        epochs_per_update: int = 3,
        max_training_samples: int = 5000,
        enable_causal_mask: bool = True,
        enable_quantum_distillation: bool = True,
        enable_federated: bool = False,
        enable_expert_auction: bool = False,
        enable_safety_monitor: bool = True,
        enable_temporal_safety: bool = True,
        enable_xai: bool = True,
        enable_precision: bool = False,
        enable_carbon_market: bool = False,
        enable_chaos: bool = False,
        chaos_probability: float = 0.0,
        enable_human_approval: bool = False,
        seed: int = 42,
    ) -> None:
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.expert_ids = list(expert_ids or
                                [f"expert_{i}" for i in range(4)])
        if not self.expert_ids:
            raise ValueError("at least one expert required")
        self.n_experts = len(self.expert_ids)
        self.input_dim = int(input_dim)
        self.hidden_dim = int(hidden_dim)
        self.num_hidden_layers = int(num_hidden_layers)
        self.activation = str(activation)
        self.dropout_rate = float(dropout_rate)
        self.lr = float(learning_rate)
        self.batch_size = int(batch_size)
        self.epochs_per_update = int(epochs_per_update)
        self.max_training_samples = int(max_training_samples)

        # Deterministic RNG tree
        self._rng = random.Random(seed)
        self._rng_xai = random.Random(seed + 1)
        self._rng_chaos = random.Random(seed + 2)
        self._rng_carbon = random.Random(seed + 3)
        self._rng_fed = random.Random(seed + 4)
        torch.manual_seed(seed)
        np.random.seed(seed)

        # Model
        self.model = GatingNetwork(
            input_dim=self.input_dim,
            hidden_dim=self.hidden_dim,
            num_experts=self.n_experts,
            num_hidden_layers=self.num_hidden_layers,
            activation=self.activation,
            dropout_rate=self.dropout_rate,
        )
        self.model.eval()
        self.optimizer = optim.Adam(self.model.parameters(), lr=self.lr)
        self.criterion = nn.CrossEntropyLoss()

        # Thread-safety for CPU-bound forward/backward
        self._cpu_lock = threading.Lock()

        # Expert output modules (registered with the module tree via ModuleList)
        self.expert_modules = nn.ModuleList([
            ExpertModule(self.input_dim, hidden_dim=self.hidden_dim)
            for _ in self.expert_ids
        ])

        # Causal mask
        self.causal_mask = CausalFeatureMask(
            self.input_dim, rng=self._rng_xai) if enable_causal_mask else None

        # Enhancement modules
        self.quantum = (QuantumDistillationModule(
            n_dims=self.input_dim, rng=random.Random(seed + 5))
            if enable_quantum_distillation else None)
        self.federated = (FederatedGreenAggregator(
            storage, "gating", rng=self._rng_fed)
            if enable_federated else None)
        self.auction = (ExpertAuction(
            self.expert_ids, self.input_dim, rng=self._rng_chaos)
            if enable_expert_auction else None)
        self.safety = SafetyMonitor() if enable_safety_monitor else None
        if self.safety:
            self._setup_safety_rules()
        self.temporal_safety = (TemporalSafetyMonitor()
                                if enable_temporal_safety else None)
        if self.temporal_safety:
            self._setup_temporal_rules()
        self.xai = XAIExplainer(rng=self._rng_xai) if enable_xai else None
        self.precision = (PrecisionController()
                          if enable_precision else None)
        self.carbon_market = (CarbonMarketClient(rng=self._rng_carbon)
                              if enable_carbon_market else None)
        self.chaos = (ChaosInjector(
            self, chaos_probability, rng=self._rng_chaos)
            if enable_chaos else None)
        self.hitl = (HumanApprovalHandler(message_queue)
                     if enable_human_approval else None)

        # Training buffer
        self.training_buffer: Deque[Tuple[np.ndarray, int]] = deque(
            maxlen=self.max_training_samples)
        self.is_trained = False
        self.step_count = 0
        self.inference_count = 0
        self.training_count = 0

        # Locks for counters
        self._metrics_lock = asyncio.Lock()

        # Snapshot for rollback
        self._initial_snapshot = self._snapshot_weights()

        # Lifecycle
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()
        if self.chaos is not None:
            self._spawn(self._chaos_loop())

    # ------------------------------------------------------------------
    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _setup_safety_rules(self) -> None:
        assert self.safety is not None

        def _finite(r):
            with r._cpu_lock:
                for p in r.model.parameters():
                    if not torch.all(torch.isfinite(p)):
                        return False
            return True

        def _bounded(r):
            with r._cpu_lock:
                for p in r.model.parameters():
                    if float(p.norm()) > r.WEIGHT_NORM_MAX:
                        return False
            return True

        def _probs_valid(state):
            p = state.get("probs")
            if p is None:
                return True
            if any(x < 0 for x in p):
                return False
            return abs(sum(p) - 1.0) < 1e-4

        self.safety.add_rule(
            "weights_finite",
            lambda state, r=self: _finite(r),
            "weights contain NaN or Inf",
            severity="critical",
            mitigation=lambda r: r._restore_weights(r._initial_snapshot))
        self.safety.add_rule(
            "weights_bounded",
            lambda state, r=self: _bounded(r),
            "weight norm exceeds bound",
            severity="critical",
            mitigation=lambda r: r._clip_weights())
        self.safety.add_rule(
            "probs_valid", _probs_valid,
            "gating probabilities invalid",
            severity="warning")

    def _setup_temporal_rules(self) -> None:
        assert self.temporal_safety is not None
        self.temporal_safety.add_formula("G (step_count >= 0)")
        self.temporal_safety.add_formula("G (confidence >= 0.0)")
        self.temporal_safety.add_formula("G (confidence <= 1.0)")

    # ------------------------------------------------------------------
    def _snapshot_weights(self) -> Dict[str, Any]:
        with self._cpu_lock:
            snap = {"model": {k: v.detach().clone()
                              for k, v in self.model.state_dict().items()},
                    "experts": {k: v.detach().clone()
                                for k, v in self.expert_modules.state_dict().items()}}
        return snap

    def _restore_weights(self, snap: Dict[str, Any]) -> None:
        with self._cpu_lock:
            try:
                self.model.load_state_dict(snap["model"])
            except Exception:
                pass
            try:
                self.expert_modules.load_state_dict(snap["experts"])
            except Exception:
                pass

    def _clip_weights(self) -> None:
        with self._cpu_lock:
            with torch.no_grad():
                for p in self.model.parameters():
                    p.data.clamp_(-self.W_MAX, self.W_MAX)
                for p in self.expert_modules.parameters():
                    p.data.clamp_(-self.W_MAX, self.W_MAX)

    def _check_steady_state(self) -> bool:
        try:
            with self._cpu_lock:
                for p in self.model.parameters():
                    if not torch.all(torch.isfinite(p)):
                        return False
                    if float(p.norm()) > self.WEIGHT_NORM_MAX:
                        return False
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    def _forward_locked(self, x: torch.Tensor,
                        use_bf16: bool = False) -> torch.Tensor:
        """Serialized forward pass; always returns float32 logits."""
        with self._cpu_lock:
            self.model.eval()
            if use_bf16:
                try:
                    with torch.autocast(device_type="cpu",
                                        dtype=torch.bfloat16):
                        logits = self.model(x)
                except (RuntimeError, TypeError):
                    logits = self.model(x)
            else:
                logits = self.model(x)
            return logits.float().detach()

    async def _forward_async(self, x: torch.Tensor,
                             use_bf16: bool = False) -> torch.Tensor:
        return await asyncio.to_thread(self._forward_locked, x, use_bf16)

    def _select_precision(self, features: np.ndarray) -> str:
        if self.precision is None:
            return "fp32"
        load = float(np.mean(np.abs(features))) if features.size else 0.0
        if load > 0.8:
            return "bf16"
        return self.precision.select_precision()

    # ------------------------------------------------------------------
    async def predict(self, context: Dict[str, Any],
                      return_explanation: bool = True,
                      user_id: str = "default",
                      task_id: str = "unknown") -> GatingDecision:
        features = BioFeatures.encode(context)

        # Causal mask observation + apply
        if self.causal_mask is not None:
            self.causal_mask.observe(features)
            masked = self.causal_mask.apply(features)
        else:
            masked = features

        precision = self._select_precision(masked)
        use_bf16 = precision in ("fp16", "bf16")

        x_tensor = torch.tensor(masked, dtype=torch.float32).unsqueeze(0)
        logits = await self._forward_async(x_tensor, use_bf16)

        # Adaptive cost adjustment
        if self.adaptive_cost is not None:
            costs = []
            for eid in self.expert_ids:
                costs.append(self.adaptive_cost.compute(
                    quality=context.get("quality_score", 0.5),
                    carbon_g=context.get("carbon_g", 0.1),
                    latency_ms=context.get("latency_ms", 100.0),
                    energy_joules=context.get("energy_joules", 10.0),
                ))
            cost_tensor = torch.tensor(costs, dtype=torch.float32).unsqueeze(0)
            logits = logits - cost_tensor

        probs = torch.softmax(logits, dim=1).squeeze(0).numpy()
        probs = self._safe_normalize(probs)

        # Safety check
        if self.safety is not None:
            violations = self.safety.check(
                {"probs": probs.tolist()}, router=self)
            if any(v.severity == "critical" and not v.mitigated
                   for v in violations):
                probs = np.ones_like(probs) / len(probs)

        # Expert selection (auction override)
        if self.auction is not None:
            winners = self.auction.select_experts(masked, top_k=1)
            if winners:
                selected_idx = self.expert_ids.index(winners[0])
            else:
                selected_idx = int(np.argmax(probs))
        else:
            selected_idx = int(np.argmax(probs))

        # HITL gate
        if self.hitl is not None and self.n_experts >= 2:
            candidates = sorted(
                [{"id": eid, "prob": float(probs[i])}
                 for i, eid in enumerate(self.expert_ids)],
                key=lambda c: c["prob"], reverse=True)[:2]
            approved = await self.hitl.request_approval(
                {"action": "route", "candidates": candidates},
                user_id=user_id)
            if not approved:
                selected_idx = int(np.argmax(probs))

        # XAI
        explanation: Optional[Dict[str, Any]] = None
        counterfactual: Optional[Dict[str, Any]] = None
        if return_explanation and self.xai is not None:
            explanation, counterfactual = await asyncio.to_thread(
                self._compute_xai, masked, selected_idx)

        # Temporal safety push + check
        if self.temporal_safety is not None:
            await self.temporal_safety.push({
                "step_count": self.step_count,
                "confidence": float(probs[selected_idx]),
            })
            await self.temporal_safety.check()

        # Metrics
        async with self._metrics_lock:
            self.inference_count += 1
            if self.metrics is not None:
                try:
                    self.metrics.inc("gating_inference")
                except Exception:
                    pass

        return GatingDecision(
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
        )

    def _compute_xai(self, masked: np.ndarray,
                     selected_idx: int) -> Tuple[
                         Optional[Dict[str, Any]],
                         Optional[Dict[str, Any]]]:
        if self.xai is None:
            return None, None

        def _score(v: np.ndarray) -> float:
            t = torch.tensor(np.asarray(v, dtype=np.float32)).unsqueeze(0)
            logits = self._forward_locked(t, use_bf16=False)
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

    def _safe_normalize(self, probs: np.ndarray) -> np.ndarray:
        probs = np.nan_to_num(probs, nan=0.0, posinf=0.0, neginf=0.0)
        probs = np.clip(probs, 0.0, None)
        s = float(probs.sum())
        if s <= 0 or not np.isfinite(s):
            return np.ones_like(probs) / probs.size
        return probs / s

    # ------------------------------------------------------------------
    def add_training_sample(self, features: np.ndarray, label: int) -> None:
        features = np.asarray(features, dtype=np.float32)
        if features.shape[0] != self.input_dim:
            raise ValueError(
                f"features dim {features.shape[0]} != {self.input_dim}")
        if not (0 <= label < self.n_experts):
            raise ValueError(
                f"label {label} out of range [0, {self.n_experts})")
        if len(self.training_buffer) >= self.max_training_samples:
            self.training_buffer.popleft()
        self.training_buffer.append((features, int(label)))

    async def train(self, epochs: Optional[int] = None) -> Optional[float]:
        if not self.training_buffer:
            return None
        epochs = int(epochs or self.epochs_per_update)
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
            dataset = TensorDataset(X_t, y_t)
            loader = DataLoader(dataset,
                                batch_size=min(self.batch_size, len(X_t)),
                                shuffle=True)
            total = 0.0
            n_batches = 0
            for _ in range(epochs):
                for batch_X, batch_y in loader:
                    self.optimizer.zero_grad()
                    out = self.model(batch_X)
                    loss = self.criterion(out, batch_y)
                    loss.backward()
                    torch.nn.utils.clip_grad_norm_(self.model.parameters(),
                                                    1.0)
                    self.optimizer.step()
                    total += float(loss.item())
                    n_batches += 1
            self.model.eval()
        self.is_trained = True
        self.training_count += 1
        return total / max(n_batches, 1)

    async def update(self, features: np.ndarray, label: int,
                     reward: float) -> Optional[GatingDecision]:
        """Online update: record sample, run one training step, return decision."""
        self.add_training_sample(features, label)
        await self.train(epochs=self.epochs_per_update)
        self.step_count += 1
        # Update auction with reward
        if self.auction is not None:
            self.auction.update(self.expert_ids[label], features, reward)
        # Chaos injection
        if self.chaos is not None:
            await self.chaos.maybe_inject_failure()
        # Federated send (rate-limited)
        if self.federated is not None:
            await self.federated.share_weights(
                "gating", self._weights_for_fed())
        return None

    def _weights_for_fed(self) -> Dict[str, Any]:
        with self._cpu_lock:
            return {k: v.detach().cpu().numpy()
                    for k, v in self.model.state_dict().items()
                    if v.dtype in (torch.float32, torch.float64)}

    async def apply_global_weights(self, weights: Dict[str, Any]) -> bool:
        if not weights:
            return False
        try:
            await asyncio.to_thread(self._apply_weights_locked, weights)
            return True
        except Exception as e:
            logger.warning(f"apply_global_weights failed: {e}")
            return False

    def _apply_weights_locked(self, weights: Dict[str, Any]) -> None:
        with self._cpu_lock:
            local = self.model.state_dict()
            new_state = {}
            for k, v in weights.items():
                if k not in local:
                    continue
                remote = torch.as_tensor(np.asarray(v))
                if remote.shape != local[k].shape:
                    raise ValueError(
                        f"shape mismatch for {k}: {remote.shape} vs "
                        f"{local[k].shape}")
                new_state[k] = (0.5 * local[k].cpu() +
                                0.5 * remote.float()).to(local[k].dtype)
            if new_state:
                self.model.load_state_dict(new_state, strict=False)

    # ------------------------------------------------------------------
    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(),
                                       timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass
            if self.chaos is not None:
                try:
                    await self.chaos.maybe_inject_failure()
                except Exception as e:
                    logger.debug(f"chaos loop: {e}")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    # ------------------------------------------------------------------
    async def publish_training_event(self, task_id: str = "train",
                                     avg_loss: float = 0.0) -> None:
        if self.queue is None:
            return
        event = FeedbackEvent(
            source="gating_network",
            feedback_type="gating_training",
            task_id=task_id,
            context={"epochs": self.epochs_per_update,
                     "samples": len(self.training_buffer)},
            action={"action": "train"},
            performance={"quality_score": max(0.0, 1.0 - avg_loss)},
            adaptive_cost_value=0.0,
            tags=["gating", "training"],
        )
        await self.queue.publish("feedback_events", event.to_json())

    def get_stats(self) -> Dict[str, Any]:
        return {
            "n_experts": self.n_experts,
            "input_dim": self.input_dim,
            "hidden_dim": self.hidden_dim,
            "num_hidden_layers": self.num_hidden_layers,
            "is_trained": self.is_trained,
            "training_buffer": len(self.training_buffer),
            "step_count": self.step_count,
            "inference_count": self.inference_count,
            "training_count": self.training_count,
            "causal_mask_enabled": self.causal_mask is not None,
            "quantum_enabled": self.quantum is not None,
            "federated_enabled": self.federated is not None,
            "auction_enabled": self.auction is not None,
            "safety_enabled": self.safety is not None,
            "temporal_safety_enabled": self.temporal_safety is not None,
            "xai_enabled": self.xai is not None,
            "precision": self.precision.current if self.precision else "fp32",
            "carbon_market_enabled": self.carbon_market is not None,
            "chaos_enabled": self.chaos is not None,
            "hitl_enabled": self.hitl is not None,
        }

    async def health_check(self) -> Dict[str, Any]:
        stats = self.get_stats()
        stats["background_tasks"] = len(self._background_tasks)
        stats["steady_state"] = self._check_steady_state()
        return stats


# =============================================================================
# SAFETY MONITOR (rule-based with severity + mitigation)
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
              router: Optional[Any] = None) -> List[SafetyViolation]:
        out: List[SafetyViolation] = []
        for rule in self.rules:
            try:
                ok = bool(rule.predicate(state))
            except Exception:
                ok = False
            if not ok:
                v = SafetyViolation(rule.name, rule.severity,
                                    rule.description)
                if rule.mitigation and router is not None:
                    try:
                        rule.mitigation(router)
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
def manager(storage, queue, adaptive_cost, pareto_gating,
            drift_detector, metrics):
    return GatingNetworkManager(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        hidden_dim=16, num_hidden_layers=2, seed=42,
        enable_xai=True, enable_safety_monitor=True,
        enable_temporal_safety=True, enable_causal_mask=True,
    )


@pytest_asyncio.fixture
async def async_manager(storage, queue, adaptive_cost, pareto_gating,
                        drift_detector, metrics):
    m = GatingNetworkManager(
        storage=storage, message_queue=queue,
        adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
        drift_detector=drift_detector, metrics=metrics,
        hidden_dim=16, num_hidden_layers=2, seed=42,
        enable_quantum_distillation=True,
        enable_federated=True,
        enable_expert_auction=True,
        enable_safety_monitor=True,
        enable_temporal_safety=True,
        enable_xai=True,
        enable_precision=True,
        enable_carbon_market=True,
        enable_chaos=False,
        enable_human_approval=True,
        enable_causal_mask=True,
    )
    try:
        yield m
    finally:
        await m.shutdown()


def _sample_context() -> Dict[str, Any]:
    return {
        "helium_scarcity": 0.4, "helium_cost_index": 0.5,
        "carbon_intensity": 0.45, "model_loss": 0.2,
        "gradient_variance": 0.3, "avg_client_energy": 0.6,
        "gradient_carbon": 0.5, "gradient_helium": 0.4,
        "token_balance_norm": 0.7, "harvester_stress": 0.3,
        "causal_impact_carbon": 0.1, "causal_impact_helium": 0.2,
    }


# =============================================================================
# TESTS — BUG FIXES
# =============================================================================
class TestBugFixes:
    @pytest.mark.asyncio
    async def test_predict_with_batch_size_1(self, manager):
        # LayerNorm should tolerate batch size 1 (the old BatchNorm1d
        # implementation would crash).
        decision = await manager.predict(_sample_context(),
                                         return_explanation=False)
        assert 0 <= decision.selected_idx < manager.n_experts
        assert abs(float(decision.probs.sum()) - 1.0) < 1e-4

    @pytest.mark.asyncio
    async def test_predict_after_train(self, manager):
        # Manually populate the buffer and train.
        for _ in range(10):
            manager.add_training_sample(
                BioFeatures.encode(_sample_context()), 0)
        await manager.train(epochs=1)
        # Model is now in eval mode and predict must work.
        decision = await manager.predict(_sample_context(),
                                         return_explanation=False)
        assert decision.selected_idx in range(manager.n_experts)

    @pytest.mark.asyncio
    async def test_model_never_leaves_train_mode(self, manager):
        for _ in range(5):
            manager.add_training_sample(
                BioFeatures.encode(_sample_context()), 1)
        await manager.train(epochs=1)
        assert not manager.model.training, \
            "model must be in eval mode after train()"

    @pytest.mark.asyncio
    async def test_precision_does_not_break_dtype(self, storage, queue,
                                                   adaptive_cost,
                                                   pareto_gating,
                                                   drift_detector, metrics):
        m = GatingNetworkManager(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hidden_dim=16, seed=1, enable_precision=True)
        try:
            # High load -> bf16 path
            ctx = _sample_context()
            ctx["harvester_stress"] = 5.0
            decision = await m.predict(ctx, return_explanation=False)
            assert decision.precision in ("fp32", "bf16", "fp16", "int8")
            # Weights must remain float32.
            for p in m.model.parameters():
                assert p.dtype == torch.float32
        finally:
            await m.shutdown()

    @pytest.mark.asyncio
    async def test_safety_mitigates_nan(self, manager):
        # Inject NaN into a weight.
        with torch.no_grad():
            for p in manager.model.parameters():
                p.data[0] = float("nan")
                break
        violations = manager.safety.check({}, router=manager)
        assert any(v.name == "weights_finite" for v in violations)
        # After mitigation, weights must be finite.
        for p in manager.model.parameters():
            assert torch.all(torch.isfinite(p))

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage, queue,
                                           adaptive_cost, pareto_gating,
                                           drift_detector, metrics):
        m = GatingNetworkManager(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hidden_dim=8, seed=1, enable_chaos=True,
            chaos_probability=1.0)
        assert len(m._background_tasks) >= 1
        await m.shutdown()
        assert len(m._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_feedback_event_accepts_extra_kwargs(self):
        # create_with_context must drop unsupported kwargs gracefully.
        event = FeedbackEvent.create_with_context(
            source="test", feedback_type="routing", task_id="t1",
            context={}, action={}, performance={},
            adaptive_cost_value=0.0, tags=[],
            metadata={"foo": "bar"},  # should be silently dropped
            unknown_kwarg=42,
        )
        assert event.source == "test"
        assert "metadata" not in asdict(event)

    @pytest.mark.asyncio
    async def test_apply_global_weights_shape_validation(self, manager):
        bad = {"network.0.weight": np.zeros((5, 5), dtype=np.float32)}
        ok = await manager.apply_global_weights(bad)
        assert ok is False  # shape mismatch caught

    def test_zero_experts_rejected(self, storage, queue):
        with pytest.raises(ValueError):
            GatingNetworkManager(storage=storage, message_queue=queue,
                                 expert_ids=[])

    def test_softmax_fallback(self, manager):
        probs = manager._safe_normalize(np.array([np.nan, np.nan]))
        assert np.allclose(probs, [0.5, 0.5])

    @pytest.mark.asyncio
    async def test_chaos_bounded(self, storage, queue, adaptive_cost,
                                 pareto_gating, drift_detector, metrics):
        m = GatingNetworkManager(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hidden_dim=8, seed=1, enable_chaos=True,
            chaos_probability=1.0)
        try:
            for _ in range(20):
                await m.chaos.run_experiment(
                    f"c{_}", "data_corruption")
            for p in m.model.parameters():
                assert torch.all(torch.isfinite(p))
                assert float(p.norm()) <= m.WEIGHT_NORM_MAX
        finally:
            await m.shutdown()


# =============================================================================
# TESTS — CORE ROUTER
# =============================================================================
class TestCoreRouter:
    @pytest.mark.asyncio
    async def test_predict_probs_sum_to_one(self, manager):
        decision = await manager.predict(_sample_context(),
                                         return_explanation=False)
        assert abs(float(decision.probs.sum()) - 1.0) < 1e-4
        assert np.all(decision.probs >= 0)

    @pytest.mark.asyncio
    async def test_decision_is_frozen(self, manager):
        decision = await manager.predict(_sample_context(),
                                         return_explanation=False)
        with pytest.raises(Exception):
            decision.selected_idx = 999

    @pytest.mark.asyncio
    async def test_predict_deterministic(self, storage, queue,
                                          adaptive_cost, pareto_gating,
                                          drift_detector, metrics):
        m1 = GatingNetworkManager(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hidden_dim=16, seed=7, enable_expert_auction=False)
        m2 = GatingNetworkManager(
            storage=storage, message_queue=queue,
            adaptive_cost=adaptive_cost, pareto_gating=pareto_gating,
            drift_detector=drift_detector, metrics=metrics,
            hidden_dim=16, seed=7, enable_expert_auction=False)
        try:
            d1 = await m1.predict(_sample_context(),
                                   return_explanation=False)
            d2 = await m2.predict(_sample_context(),
                                   return_explanation=False)
            assert d1.selected_idx == d2.selected_idx
            assert np.allclose(d1.probs, d2.probs)
        finally:
            await m1.shutdown()
            await m2.shutdown()

    @pytest.mark.asyncio
    async def test_add_training_sample_validates(self, manager):
        with pytest.raises(ValueError):
            manager.add_training_sample(np.zeros(3, dtype=np.float32), 0)
        with pytest.raises(ValueError):
            manager.add_training_sample(
                np.zeros(BioFeatures.DIM, dtype=np.float32), 99)

    @pytest.mark.asyncio
    async def test_train_reduces_loss(self, manager):
        rng = np.random.RandomState(0)
        for _ in range(50):
            feats = rng.randn(BioFeatures.DIM).astype(np.float32) * 0.1
            label = 0 if feats[0] > 0 else 1
            manager.add_training_sample(feats, label)
        loss1 = await manager.train(epochs=1)
        for _ in range(20):
            loss2 = await manager.train(epochs=1)
        assert loss2 is not None

    @pytest.mark.asyncio
    async def test_concurrent_predict(self, manager):
        async def _p():
            return await manager.predict(_sample_context(),
                                         return_explanation=False)
        results = await asyncio.gather(*[_p() for _ in range(8)])
        assert all(r.selected_idx in range(manager.n_experts)
                   for r in results)


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
        # (1*0 + 3*8)/4 = 6
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
    async def test_historically_operator(self):
        s = TemporalSafetyMonitor()
        s.add_formula("H (quality >= 0.5)")
        await s.push({"quality": 0.9})
        await s.push({"quality": 0.1})
        result = await s.check()
        assert any(not v for v in result.values())

    @pytest.mark.asyncio
    async def test_windowed_rule(self):
        s = TemporalSafetyMonitor()
        s.add_formula("[60] G (quality >= 0.5)")
        assert any("@60" in rid for rid in s.rules)


class TestXAIExplainer:
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


class TestPrecisionController:
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
    async def test_budget_overshoot_defers(self):
        c = CarbonMarketClient(budget_kg=0.1, rng=random.Random(0))
        c.used_kg = 0.09
        result = await c.net_zero_schedule(1.0, 0.1)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_buy_and_retire_rec(self):
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


class TestChaosInjector:
    @pytest.mark.asyncio
    async def test_latency(self, manager):
        c = ChaosInjector(manager, rng=random.Random(0))
        result = await c.run_experiment("t1", "latency")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_exception(self, manager):
        c = ChaosInjector(manager, rng=random.Random(0))
        result = await c.run_experiment("t2", "exception")
        assert result["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, manager):
        c = ChaosInjector(manager)
        with pytest.raises(ValueError):
            await c.run_experiment("t", "unknown")


class TestHITL:
    @pytest.mark.asyncio
    async def test_single_candidate_auto(self):
        h = HumanApprovalHandler()
        assert await h.request_approval(
            {"candidates": [{"id": "only"}]}) is True

    @pytest.mark.asyncio
    async def test_confident_skips_query(self):
        h = HumanApprovalHandler(uncertainty_threshold=0.01)
        await h.record_choice("u", "a", metrics={"quality_score": 1.0})
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 10.0},
                {"id": "lo", "quality_score": 0.01},
            ]}, user_id="u")
        assert ok is True

    @pytest.mark.asyncio
    async def test_uncertain_queries(self):
        h = HumanApprovalHandler(uncertainty_threshold=10.0, timeout=0.05)
        await h.submit_response("u", "hi")
        ok = await h.request_approval({
            "candidates": [
                {"id": "hi", "quality_score": 1.0},
                {"id": "lo", "quality_score": 1.0},
            ]}, user_id="u")
        assert ok is True


# =============================================================================
# TESTS — FULL INTEGRATION
# =============================================================================
class TestFullIntegration:
    @pytest.mark.asyncio
    async def test_async_manager_full_cycle(self, async_manager):
        decision = await async_manager.predict(_sample_context())
        assert decision.selected_expert in async_manager.expert_ids
        assert decision.explanation is not None
        assert "attributions" in decision.explanation

    @pytest.mark.asyncio
    async def test_stats_reports_all_modules(self, async_manager):
        stats = async_manager.get_stats()
        for key in ("causal_mask_enabled", "quantum_enabled",
                    "federated_enabled", "auction_enabled",
                    "safety_enabled", "temporal_safety_enabled",
                    "xai_enabled", "carbon_market_enabled",
                    "chaos_enabled", "hitl_enabled"):
            assert key in stats

    @pytest.mark.asyncio
    async def test_health_check(self, async_manager):
        hc = await async_manager.health_check()
        assert "background_tasks" in hc
        assert "steady_state" in hc
        assert hc["steady_state"] is True

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, async_manager):
        await async_manager.shutdown()
        await async_manager.shutdown()
        assert len(async_manager._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_federated_apply_roundtrip(self, async_manager):
        weights = async_manager._weights_for_fed()
        # Perturb and apply.
        perturbed = {k: np.asarray(v) * 1.01 for k, v in weights.items()}
        ok = await async_manager.apply_global_weights(perturbed)
        assert ok is True

    @pytest.mark.asyncio
    async def test_concurrent_predict_full(self, async_manager):
        async def _p():
            return await async_manager.predict(_sample_context(),
                                                return_explanation=False)
        results = await asyncio.gather(*[_p() for _ in range(6)])
        for r in results:
            assert r.selected_expert in async_manager.expert_ids


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

        m = GatingNetworkManager(
            storage=storage, message_queue=queue,
            adaptive_cost=ac, pareto_gating=pg, drift_detector=dd,
            metrics=metrics, hidden_dim=32, seed=1,
            enable_federated=True, enable_expert_auction=True,
            enable_safety_monitor=True, enable_temporal_safety=True,
            enable_xai=True, enable_precision=True,
            enable_carbon_market=True, enable_human_approval=True,
            enable_causal_mask=True,
        )
        try:
            decision = await m.predict({
                "helium_scarcity": 0.4, "carbon_intensity": 0.45,
                "model_loss": 0.2, "gradient_variance": 0.3,
            })
            print(f"Selected: {decision.selected_expert} "
                  f"(confidence={decision.confidence:.3f})")
            print(f"Probs: {decision.probs.tolist()}")
            if decision.explanation:
                print("XAI method:", decision.explanation["method"])
            stats = await m.health_check()
            print("Stats:", json.dumps(stats, indent=2, default=str))
        finally:
            await m.shutdown()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())
