#!/usr/bin/env python3
"""
Lightweight LLM client with all ten Green Agent enhancements built in:

  1. Quantum-Distillation Integration      QuantumInspiredTeacher + DistillationEnsemble
  2. Causal RL for Policy Adaptation       CausalCounterfactualEstimator
  3. Federated Green Learning              FederatedAggregator
  4. Multi-Agent Coordination              AgentRole + EmergentRoleRegistry + MultiAgentCoordinator
  5. Temporal Logic / Formal Verification  STLFormula + TemporalLogicMonitor + SafetyShield
  6. Explainable AI                        DecisionExplainer + Explanation
  7. Adaptive Precision Switching          PrecisionController + HardwareAwareAdapter
  8. Carbon Markets / RECs                 CarbonMarketClient + RECInventory
  9. Resilience / Chaos                    ChaosEngineer (+ circuit breaker, retry)
 10. HITL / Active Learning                UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler

Fixes over the previous version:
  - SemanticCache stores (value, timestamp); eviction uses the timestamp, not a str index
  - MOERouter no longer holds a circular reference to the primary client
  - MOERouter.update is called for primary as well as secondary teachers
  - GA arm_history is populated and evolution runs every N calls, not every call
  - Reward uses a real quality proxy (length + entropy + key-phrase bonus) instead of pure length
  - MOO outcome uses real per-call latency instead of a constant
  - Vault uses the v2 read_secret_version API and runs in an executor
  - _compute_reward guards against None
  - Circuit breaker is per-endpoint, including teachers
  - All ten enhancements are wired into generate_explanation end to end
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import hashlib
import json
import logging
import math
import os
import random
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any, AsyncIterable, Callable, Dict, List, Optional, Sequence,
    Tuple, Union,
)

import numpy as np

import aiohttp

try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type,
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

    def retry(*a, **kw):
        def deco(fn):
            async def wrapper(*args, **kwargs):
                return await fn(*args, **kwargs)
            return wrapper
        return deco

    def stop_after_attempt(*a, **kw): return None
    def wait_exponential(*a, **kw): return None
    def retry_if_exception_type(*a, **kw): return None


# ---------- Optional dependencies ----------
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from hvac import Client as VaultClient
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


# ---------- Logger ----------
logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# ---------- Prometheus metrics ----------
if PROMETHEUS_AVAILABLE:
    from prometheus_client import CollectorRegistry
    REGISTRY = CollectorRegistry()
    METRIC_REQUESTS = Counter("llm_requests_total", "Total LLM requests", ["status"], registry=REGISTRY)
    METRIC_DURATION = Histogram("llm_request_duration_seconds", "LLM request duration", registry=REGISTRY)
    METRIC_CB_STATE = Gauge("llm_circuit_breaker_state", "Circuit breaker state", ["endpoint"], registry=REGISTRY)
    METRIC_CACHE_HITS = Counter("llm_cache_hits_total", "Cache hits", registry=REGISTRY)
    METRIC_CACHE_MISSES = Counter("llm_cache_misses_total", "Cache misses", registry=REGISTRY)
    METRIC_FALLBACK = Counter("llm_fallback_usage_total", "Fallback usage", registry=REGISTRY)
    METRIC_RETRY = Counter("llm_retry_count_total", "Retry count", registry=REGISTRY)
    METRIC_TOKENS = Counter("llm_token_usage_total", "Token usage", registry=REGISTRY)
    METRIC_CARBON = Gauge("llm_carbon_intensity", "Carbon intensity", registry=REGISTRY)
    METRIC_CARBON_PRICE = Gauge("llm_carbon_price_per_kg", "Carbon price", registry=REGISTRY)
    METRIC_REC = Gauge("llm_rec_inventory_kwh", "REC inventory kWh", registry=REGISTRY)
    METRIC_PRECISION = Counter("llm_precision_switches_total", "Precision switches", ["level"], registry=REGISTRY)
    METRIC_CHAOS = Counter("llm_chaos_events_total", "Chaos events", ["type"], registry=REGISTRY)
    METRIC_HITL = Counter("llm_hitl_approvals_total", "HITL approvals", ["decision"], registry=REGISTRY)
    METRIC_SAFETY = Counter("llm_safety_violations_total", "Safety violations", ["formula"], registry=REGISTRY)
    METRIC_FEDERATED = Counter("llm_federated_rounds_total", "Federated rounds", registry=REGISTRY)
    METRIC_MOE_GATE = Gauge("llm_moe_gating_weight", "MOE gating weight", ["teacher"], registry=REGISTRY)
else:
    class _DummyMetric:
        def inc(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self

    METRIC_REQUESTS = METRIC_DURATION = METRIC_CB_STATE = _DummyMetric()
    METRIC_CACHE_HITS = METRIC_CACHE_MISSES = METRIC_FALLBACK = _DummyMetric()
    METRIC_RETRY = METRIC_TOKENS = METRIC_CARBON = METRIC_CARBON_PRICE = _DummyMetric()
    METRIC_REC = METRIC_PRECISION = METRIC_CHAOS = METRIC_HITL = _DummyMetric()
    METRIC_SAFETY = METRIC_FEDERATED = METRIC_MOE_GATE = _DummyMetric()


# ===========================================================================
# ===========================================================================
# TEN ENHANCEMENT MODULES
# ===========================================================================
# ===========================================================================


# ------------------------------------------------------------
# Enhancement 7: Adaptive Precision Switching
# ------------------------------------------------------------
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    INT8 = "int8"
    INT4 = "int4"


PRECISION_COST: Dict[PrecisionLevel, Dict[str, float]] = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "bits": 4.0},
}


class PrecisionController:
    def __init__(self, supported: Optional[Sequence[PrecisionLevel]] = None,
                 quality_floor: float = 0.95, carbon_aware: bool = True):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware

    def select(self, carbon_intensity: float, latency_headroom_ratio: float,
               carbon_price: float = 0.0) -> PrecisionLevel:
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        agg = 0.5 * stress + 0.3 * price_stress + 0.2 * (1.0 - headroom)
        ordered = [p for p in PrecisionLevel if p in self.supported]
        ordered.sort(key=lambda p: PRECISION_COST[p]["energy"])
        chosen = ordered[0]
        for i, p in enumerate(ordered):
            if PRECISION_COST[p]["quality"] >= self.quality_floor and agg >= 0.25 * i:
                chosen = p
        return chosen


class HardwareAwareAdapter:
    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(self, params: Dict[str, Any], carbon_intensity: float,
              latency_headroom: float, carbon_price: float) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(params)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        METRIC_PRECISION.labels(level=level.value).inc()
        return out, level


# ------------------------------------------------------------
# Enhancement 8: Carbon Markets / RECs
# ------------------------------------------------------------
class CarbonMarketClient:
    def __init__(self, base_price: float = 0.05, sensitivity: float = 0.0005):
        self.base_price = base_price
        self.sensitivity = sensitivity

    def price(self, carbon_intensity: float, hour_of_day: Optional[int] = None) -> float:
        tod = 1.0
        if hour_of_day is not None:
            tod = 1.0 + 0.3 * math.sin((hour_of_day / 24.0) * 2 * math.pi)
        return self.base_price + self.sensitivity * carbon_intensity * tod


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"


class RECInventory:
    def __init__(self, grid_kg_co2_per_kwh: float = 0.4):
        self.grid_factor = grid_kg_co2_per_kwh
        self._records: List[RECRecord] = []

    def add(self, kwh: float, source: str = "solar") -> None:
        self._records.append(RECRecord(kwh=kwh, issued_at=time.time(), source=source))

    def total_kwh(self) -> float:
        return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
        remaining = kwh
        offset = 0.0
        new_records: List[RECRecord] = []
        for r in self._records:
            if remaining <= 0:
                new_records.append(r)
                continue
            take = min(r.kwh, remaining)
            remaining -= take
            offset += take * self.grid_factor
            if r.kwh - take > 1e-9:
                new_records.append(RECRecord(kwh=r.kwh - take,
                                             issued_at=r.issued_at, source=r.source))
        self._records = new_records
        return offset


# ------------------------------------------------------------
# Enhancement 9: Chaos
# ------------------------------------------------------------
@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    seed: int = 0

    def enabled(self) -> bool:
        return (self.fault_prob > 0
                or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
                or self.carbon_spike_prob > 0)


class ChaosEngineer:
    def __init__(self, config: Optional[ChaosConfig] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fault(self) -> bool:
        if self.rng.random() < self.config.fault_prob:
            self.events.append({"type": "fault", "t": time.time()})
            METRIC_CHAOS.labels(type="fault").inc()
            return True
        return False

    def maybe_latency(self) -> float:
        if (self.config.latency_inject_ms > 0
                and self.rng.random() < self.config.latency_inject_prob):
            self.events.append({"type": "latency", "t": time.time()})
            METRIC_CHAOS.labels(type="latency").inc()
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            METRIC_CHAOS.labels(type="carbon_spike").inc()
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity


# ------------------------------------------------------------
# Enhancement 5: Temporal Logic / Formal Verification
# ------------------------------------------------------------
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10


class TemporalLogicMonitor:
    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=horizon * 4)
        self.formulas: List[STLFormula] = []

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        self._history.append(record)

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = list(self._history)[-f.horizon:]
            if not window:
                results[f.name] = True
                continue
            sat = [bool(f.predicate(r)) for r in window]
            if f.operator == STLOperator.ALWAYS:
                results[f.name] = all(sat)
            elif f.operator in (STLOperator.EVENTUALLY, STLOperator.UNTIL):
                results[f.name] = any(sat)
            else:
                results[f.name] = True
            if not results[f.name]:
                METRIC_SAFETY.labels(formula=f.name).inc()
        return results


class SafetyShield:
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self, selected: Dict[str, Any],
               candidates: Sequence[Dict[str, Any]],
               score_key: str = "score") -> Tuple[Dict[str, Any], bool, Dict[str, bool]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected, True, verdict
        if not candidates:
            return selected, False, verdict
        safe = min(candidates, key=lambda p: p.get(score_key, float("inf")))
        self.violations.append({"t": time.time(), "violated": violated})
        return dict(safe), False, verdict


# ------------------------------------------------------------
# Enhancement 2: Causal RL
# ------------------------------------------------------------
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        self._A = np.eye(state_dim + n_actions + 1) * ridge
        self._b = np.zeros(state_dim + n_actions + 1)
        self._n = 0

    def _features(self, s: np.ndarray, a: int) -> np.ndarray:
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n: int) -> None:
        if n == self.n_actions:
            return
        self._A = np.eye(self.state_dim + n + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n + 1)
        self.n_actions = n
        self._n = 0

    def update(self, t: CausalTransition) -> None:
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._n += 1

    def predict(self, s: np.ndarray, a: int) -> float:
        if self._n < 2:
            return 0.0
        try:
            theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError:
            return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s: np.ndarray, n_actions: int) -> Dict[int, float]:
        self._ensure_actions(n_actions)
        return {a: self.predict(s, a) for a in range(n_actions)}


# ------------------------------------------------------------
# Enhancement 3: Federated Green Learning
# ------------------------------------------------------------
@dataclass
class FederatedUpdate:
    node_id: str
    weights: Dict[str, np.ndarray]
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None

    def submit(self, u: FederatedUpdate) -> None:
        self._updates[u.node_id] = u

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        return [u for u in self._updates.values()
                if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self) -> Optional[Dict[str, np.ndarray]]:
        fresh = self._fresh()
        if not fresh:
            return self._global
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0)
                            for u in fresh])
        weights /= max(weights.sum(), 1e-12)
        agg: Dict[str, np.ndarray] = {}
        for k in fresh[0].weights.keys():
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            blended = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                blended = blended + np.random.normal(0.0, self.dp_sigma,
                                                     size=blended.shape)
            agg[k] = blended
        self._global = agg
        METRIC_FEDERATED.inc()
        return agg

    def global_weights(self) -> Optional[Dict[str, np.ndarray]]:
        return self._global


# ------------------------------------------------------------
# Enhancement 4: Multi-Agent Coordination
# ------------------------------------------------------------
class AgentRole(str, Enum):
    EXPLORER = "explorer"
    EXPLOITER = "exploiter"
    SAFETY_OFFICER = "safety_officer"
    CARBON_BROKER = "carbon_broker"
    VERIFIER = "verifier"


@dataclass
class AgentBid:
    agent_id: str
    role: AgentRole
    confidence: float
    proposed_action: int
    rationale: str
    carbon_score: float


class EmergentRoleRegistry:
    def __init__(self, decay: float = 0.95):
        self.decay = decay
        self._scores = {r: 1.0 for r in AgentRole}
        self._counts = {r: 0 for r in AgentRole}

    def record(self, role: AgentRole, success: bool, reward: float) -> None:
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = reward if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1

    def weights(self) -> Dict[AgentRole, float]:
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self) -> AgentRole:
        return max(self._scores, key=self._scores.get)


class MultiAgentCoordinator:
    def __init__(self, registry: EmergentRoleRegistry,
                 role_bias: Optional[Dict[AgentRole, float]] = None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.2,
            AgentRole.CARBON_BROKER: 1.1,
            AgentRole.VERIFIER: 1.0,
        }

    def vote(self, bids: Sequence[AgentBid], n_candidates: int) -> int:
        if not bids or n_candidates <= 0:
            return 0
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        for b in bids:
            idx = b.proposed_action % n_candidates
            weight = (role_w.get(b.role, 0.1)
                      * self.role_bias.get(b.role, 1.0)
                      * max(b.confidence, 0.0)
                      * (1.0 + b.carbon_score))
            scores[idx] += weight
        return int(np.argmax(scores))


# ------------------------------------------------------------
# Enhancement 6: XAI
# ------------------------------------------------------------
@dataclass
class Explanation:
    decision_id: str
    chosen_idx: int
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    def __init__(self, feature_names: Optional[Sequence[str]] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "prompt_len", "num_words", "avg_word_len", "hour_norm",
        ]

    def _attributions(self, chosen: Dict[str, Any],
                      pareto: Sequence[Dict[str, Any]]) -> List[Tuple[str, float]]:
        if not pareto:
            return []
        arr = np.array([[float(c.get(f, 0.0)) for f in self.feature_names]
                        for c in pareto])
        chosen_vec = np.array([float(chosen.get(f, 0.0))
                               for f in self.feature_names])
        mean = arr.mean(axis=0)
        std = arr.std(axis=0) + 1e-9
        z = (chosen_vec - mean) / std
        attrs = list(zip(self.feature_names, z.tolist()))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def explain(self, chosen_idx: int, chosen: Dict[str, Any],
                pareto: Sequence[Dict[str, Any]],
                counterfactuals: Dict[int, float],
                safety_ok: bool, carbon_price: float,
                confidence: float = 0.5) -> Explanation:
        attrs = self._attributions(chosen, pareto)
        cf_list: List[Dict[str, Any]] = []
        for idx, score in sorted(counterfactuals.items(),
                                 key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({"alternative_idx": idx,
                                "expected_reward": float(score)})
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose arm #{chosen_idx} (confidence={confidence:.2f}). "
            f"Top deviations: {top_feat}. Carbon price={carbon_price:.4f}. "
            f"Safety={'ok' if safety_ok else 'OVERRIDE'}. "
            f"Counterfactuals: {len(cf_list)}."
        )
        return Explanation(
            decision_id=f"dec-{uuid.uuid4().hex[:8]}",
            chosen_idx=chosen_idx,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=confidence,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
        )


# ------------------------------------------------------------
# Enhancement 1: Quantum-Distillation
# ------------------------------------------------------------
class QuantumInspiredTeacher:
    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._params = self.rng.normal(size=(n_qubits, 2)) * 0.5

    def _ry(self, state: np.ndarray, theta: float, q: int) -> np.ndarray:
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state)
        new = state.copy()
        step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b
                new[i | step] = s * a + c * b
        return new

    def _simulate(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)]
                          + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features: np.ndarray, n: int) -> np.ndarray:
        if n <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features: np.ndarray, other: Optional[np.ndarray],
              n: int) -> np.ndarray:
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other)


# ------------------------------------------------------------
# Enhancement 10: HITL / Active Learning
# ------------------------------------------------------------
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float


class UncertaintyEstimator:
    def __init__(self, entropy_threshold: float = 0.75,
                 variance_threshold: float = 0.05):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self._rewards: deque = deque(maxlen=32)

    def observe(self, reward: float) -> None:
        self._rewards.append(float(reward))

    def entropy(self, probs: Optional[np.ndarray]) -> float:
        if probs is None or len(probs) == 0:
            return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self) -> float:
        if len(self._rewards) < 3:
            return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def is_uncertain(self, probs: Optional[np.ndarray]) -> Tuple[bool, float]:
        h = self.entropy(probs)
        v = self.variance()
        score = 0.6 * h + 0.4 * min(1.0, v / max(self.variance_threshold, 1e-9))
        return (h > self.entropy_threshold or v > self.variance_threshold), score


class HumanInTheLoopGate:
    def __init__(self, approver: Optional[Callable[[HITLRequest], bool]] = None,
                 timeout_s: float = 2.0, auto_approve_on_timeout: bool = True):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: List[Dict[str, Any]] = []

    async def request(self, req: HITLRequest) -> bool:
        if self.approver is None:
            self.audit.append({"request_id": req.request_id,
                               "approved": True, "reason": "no_approver"})
            METRIC_HITL.labels(decision="auto_approved").inc()
            return True
        loop = asyncio.get_running_loop()
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req),
                timeout=self.timeout_s)
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
        self.audit.append({"request_id": req.request_id,
                           "approved": bool(approved), "reason": req.reason})
        METRIC_HITL.labels(decision="approved" if approved else "rejected").inc()
        return bool(approved)


class ActiveLearningSampler:
    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []

    def maybe_store(self, record: Dict[str, Any], uncertainty: float,
                    threshold: float = 0.5) -> bool:
        if uncertainty < threshold:
            return False
        if len(self._buffer) >= self.capacity:
            self._buffer.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        if not self._buffer:
            return []
        return random.sample(self._buffer, min(k, len(self._buffer)))

    def __len__(self) -> int:
        return len(self._buffer)


# ===========================================================================
# Circuit breaker
# ===========================================================================
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == "open":
                if (time.time() - (self.last_failure_time or 0)) > self.recovery_timeout:
                    self.state = "half-open"
                    METRIC_CB_STATE.labels(endpoint=self.name).set(0.5)
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
                    METRIC_CB_STATE.labels(endpoint=self.name).set(0)
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    METRIC_CB_STATE.labels(endpoint=self.name).set(1)
            raise

    def get_status(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state,
            "failure_count": self.failure_count,
            "recovery_timeout": self.recovery_timeout,
        }


# ===========================================================================
# Contextual bandit (LinUCB) + Multi-objective optimizer
# ===========================================================================
class LinUCB:
    def __init__(self, num_arms: int, feature_dim: int, alpha: float = 0.1):
        self.num_arms = num_arms
        self.feature_dim = feature_dim
        self.alpha = alpha
        self.A = [np.eye(feature_dim) for _ in range(num_arms)]
        self.b = [np.zeros(feature_dim) for _ in range(num_arms)]
        self.theta = [np.zeros(feature_dim) for _ in range(num_arms)]

    def select_arm(self, features: np.ndarray) -> int:
        p = np.zeros(self.num_arms)
        for a in range(self.num_arms):
            A_inv = np.linalg.inv(self.A[a])
            self.theta[a] = A_inv.dot(self.b[a])
            p[a] = (self.theta[a].dot(features)
                    + self.alpha * np.sqrt(features.dot(A_inv).dot(features)))
        return int(np.argmax(p))

    def update(self, arm: int, features: np.ndarray, reward: float) -> None:
        self.A[arm] += np.outer(features, features)
        self.b[arm] += reward * features


class MultiObjectiveOptimizer:
    def __init__(self, weights: Optional[List[float]] = None):
        self.weights = weights or [0.4, 0.3, 0.3]
        self.adaptive_weights = True
        self.learning_rate = 0.01
        self.recent_outcomes: deque = deque(maxlen=100)

    def score_arms(self, arms: List[Dict[str, Any]],
                   context: Dict[str, Any]) -> List[float]:
        scores = []
        for arm in arms:
            quality = arm.get("quality_estimate", 0.5)
            latency = arm.get("latency_estimate", 1.0)
            cost = arm.get("cost_estimate", 100)
            norm_latency = 1.0 / (1.0 + latency)
            norm_cost = 1.0 / (1.0 + cost / 100)
            weighted = (self.weights[0] * quality
                        + self.weights[1] * norm_latency
                        + self.weights[2] * norm_cost)
            scores.append(weighted)
        return scores

    def update_weights(self, outcomes: Sequence[float]) -> None:
        self.recent_outcomes.append(list(outcomes))
        if len(self.recent_outcomes) >= 10:
            avg_outcome = np.mean(self.recent_outcomes, axis=0)
            target = np.mean(avg_outcome)
            error = avg_outcome - target
            self.weights = [w - self.learning_rate * e
                            for w, e in zip(self.weights, error)]
            total = sum(self.weights)
            if total > 0:
                self.weights = [w / total for w in self.weights]


# ===========================================================================
# MOE router (no circular reference, updates primary too)
# ===========================================================================
class MOERouter:
    def __init__(self, feature_dim: int = 4, epsilon: float = 0.1):
        self.teachers: List[Tuple[str, "LLMClient"]] = []
        self.epsilon = epsilon
        self.gating_weights: Optional[Any] = None
        self.scaler: Optional[Any] = None
        self._trained = False
        self.rewards: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.context_history: deque = deque(maxlen=1000)
        self.distiller: Optional[Any] = None
        self.feature_dim = feature_dim

    def add_teacher(self, name: str, client: "LLMClient") -> None:
        self.teachers.append((name, client))
        self.rewards[name] = 0.0
        self.counts[name] = 0

    def _extract_features(self, prompt: str) -> np.ndarray:
        words = prompt.split()
        return np.array([
            len(prompt),
            len(words),
            float(np.mean([len(w) for w in words])) if words else 0.0,
            datetime.now().hour / 24.0,
        ])

    def _teacher_by_name(self, name: Optional[str]) -> Optional["LLMClient"]:
        if name is None:
            return None
        for n, c in self.teachers:
            if n == name:
                return c
        return None

    async def select_teacher(self, prompt: str) -> Tuple[str, "LLMClient"]:
        if not self.teachers:
            raise RuntimeError("MOERouter has no teachers")
        # Distillation override (opt-in)
        if self.distiller is not None:
            features = self._extract_features(prompt)
            try:
                teacher_name = self.distiller.distill(features)
            except Exception:
                teacher_name = None
            client = self._teacher_by_name(teacher_name)
            if client is not None:
                return teacher_name, client

        if random.random() < self.epsilon:
            return random.choice(self.teachers)

        # Exploit by gating model if trained
        if self._trained and self.gating_weights is not None:
            try:
                features = self._extract_features(prompt).reshape(1, -1)
                X_scaled = self.scaler.transform(features)
                probs = self.gating_weights.predict_proba(X_scaled)[0]
                idx = int(np.random.choice(len(self.teachers), p=probs))
                name, client = self.teachers[idx]
                METRIC_MOE_GATE.labels(teacher=name).set(float(probs[idx]))
                return name, client
            except Exception:
                pass

        # Fallback: best average reward
        best_name = max(
            self.rewards,
            key=lambda n: self.rewards[n] / max(self.counts[n], 1))
        for n, c in self.teachers:
            if n == best_name:
                return n, c
        return self.teachers[0]

    async def update(self, teacher_name: str, reward: float, prompt: str) -> None:
        self.rewards[teacher_name] += reward
        self.counts[teacher_name] += 1
        features = self._extract_features(prompt)
        self.context_history.append((features, teacher_name, reward))
        if len(self.context_history) % 50 == 0 and len(self.context_history) >= 50:
            self._retrain_gating()

    def _retrain_gating(self) -> None:
        if not SKLEARN_AVAILABLE or len(self.context_history) < 50:
            return
        X = []
        y = []
        names = [n for n, _ in self.teachers]
        for features, name, _ in list(self.context_history)[-200:]:
            if name in names:
                X.append(features)
                y.append(names.index(name))
        if len(set(y)) < 2:
            return
        try:
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(np.array(X))
            self.gating_weights = LogisticRegression(max_iter=1000)
            self.gating_weights.fit(X_scaled, np.array(y))
            self._trained = True
        except Exception as e:
            logger.warning(f"Could not train gating model: {e}")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "teachers": [name for name, _ in self.teachers],
            "rewards": dict(self.rewards),
            "counts": dict(self.counts),
            "gating_trained": self._trained,
            "distillation_active": self.distiller is not None,
        }


# ===========================================================================
# Genetic algorithm (arm_history now populated)
# ===========================================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict[str, Any]] = []
        self.bounds = {
            "temp": (0.1, 1.0),
            "max_tokens": (50, 500),
            "model": ["small", "medium", "large"],
        }
        self.arm_history: deque = deque(maxlen=100)

    def initialize(self) -> None:
        self.population = [
            {
                "temp": random.uniform(0.1, 1.0),
                "max_tokens": random.randint(50, 500),
                "model": random.choice(self.bounds["model"]),
            }
            for _ in range(self.pop_size)
        ]

    def evaluate(self, fitness_func: Callable[[Dict[str, Any]], float]) -> List[float]:
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness: List[float], num_parents: int) -> List[Dict[str, Any]]:
        selected = []
        for _ in range(num_parents):
            a, b = np.random.choice(len(self.population), 2, replace=False)
            selected.append(self.population[a] if fitness[a] > fitness[b]
                            else self.population[b])
        return selected

    def crossover(self, p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
        if random.random() < self.crossover_rate:
            child = {}
            for key in p1:
                if key == "model":
                    child[key] = random.choice([p1[key], p2[key]])
                else:
                    child[key] = p1[key] if random.random() < 0.5 else p2[key]
            return child
        return dict(p1)

    def mutate(self, ind: Dict[str, Any]) -> Dict[str, Any]:
        if random.random() < self.mutation_rate:
            key = random.choice(list(self.bounds.keys()))
            if key == "model":
                ind[key] = random.choice(self.bounds["model"])
            else:
                low, high = self.bounds[key]
                if key == "temp":
                    ind[key] = random.uniform(low, high)
                else:
                    ind[key] = random.randint(int(low), int(high))
        return ind

    def evolve(self, fitness_func: Callable[[Dict[str, Any]], float],
               generations: int = 20) -> Dict[str, Any]:
        self.initialize()
        best = None
        for _ in range(generations):
            fitness = self.evaluate(fitness_func)
            best_idx = int(np.argmax(fitness))
            best = dict(self.population[best_idx])
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                c1 = self.crossover(parents[i], parents[i + 1])
                c2 = self.crossover(parents[i + 1], parents[i])
                offspring.append(self.mutate(c1))
                offspring.append(self.mutate(c2))
            self.population = offspring[: self.pop_size - 1] + [best]
        result = best or {}
        self.arm_history.append(result)
        return result


# ===========================================================================
# Carbon-aware scheduling
# ===========================================================================
class CarbonIntensityManager:
    def __init__(self, api_key: Optional[str] = None, region: str = "global",
                 market: Optional[CarbonMarketClient] = None,
                 recs: Optional[RECInventory] = None):
        self.api_key = api_key
        self.region = region
        self.current_intensity = 400.0
        self.market = market or CarbonMarketClient()
        self.recs = recs or RECInventory()
        self._last_update: Optional[float] = None

    async def get_current_intensity(self) -> float:
        # Simulated; in production call electricitymap.org
        self.current_intensity = 350 + random.uniform(-50, 50)
        self._last_update = time.time()
        METRIC_CARBON.set(self.current_intensity)
        return self.current_intensity

    async def get_current_price(self, hour_of_day: Optional[int] = None) -> float:
        price = self.market.price(self.current_intensity, hour_of_day)
        METRIC_CARBON_PRICE.set(price)
        METRIC_REC.set(self.recs.total_kwh())
        return price

    async def close(self) -> None:
        pass


class CarbonAwareScheduler:
    def __init__(self, carbon_manager: CarbonIntensityManager,
                 threshold: float = 400.0):
        self.carbon_manager = carbon_manager
        self.threshold = threshold

    async def adjust_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        intensity = await self.carbon_manager.get_current_intensity()
        price = await self.carbon_manager.get_current_price()
        if intensity > self.threshold or price > 0.1:
            params["max_tokens"] = min(params.get("max_tokens", 150), 100)
            if params.get("model") != "small":
                params["model"] = "small"
        return params


# ===========================================================================
# Self-healing
# ===========================================================================
class SelfHealingManager:
    def __init__(self, contamination: float = 0.1):
        self.contamination = contamination
        self.quality_history: deque = deque(maxlen=500)
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self._trained = False
        self._lock = asyncio.Lock()
        self.recovery_actions: deque = deque(maxlen=100)
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors = [
                ("iforest", IsolationForest(contamination=contamination)),
                ("ocsvm", OneClassSVM(nu=contamination)),
            ]

    async def record_quality(self, quality: float) -> None:
        async with self._lock:
            self.quality_history.append(quality)
            if len(self.quality_history) >= 100 and not self._trained:
                await self._train()

    async def _train(self) -> None:
        if not self.anomaly_detectors or len(self.quality_history) < 100:
            return
        X = np.array(list(self.quality_history)).reshape(-1, 1)
        for _, model in self.anomaly_detectors:
            try:
                model.fit(X)
            except Exception as e:
                logger.warning(f"Failed to train detector: {e}")
        self._trained = True

    async def detect_anomaly(self, quality: float) -> Tuple[bool, float]:
        if not self._trained or not self.anomaly_detectors:
            return quality < 0.3, 0.0
        X = np.array([[quality]])
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        score = sum(votes) / len(votes)
        return score > 0.5, score

    async def trigger_recovery(self) -> None:
        async with self._lock:
            self.recovery_actions.append({
                "action": "reset_bandit",
                "timestamp": datetime.now().isoformat(),
            })
        logger.warning("Self-healing triggered: resetting bandit and gating.")

    def get_stats(self) -> Dict[str, Any]:
        return {
            "trained": self._trained,
            "history_len": len(self.quality_history),
            "recent_actions": list(self.recovery_actions)[-5:],
        }


# ===========================================================================
# Semantic cache (fixed eviction)
# ===========================================================================
@dataclass
class _CacheEntry:
    value: str
    timestamp: float
    embedding: Optional[np.ndarray] = None
    prompt: str = ""


class SemanticCache:
    def __init__(self, similarity_threshold: float = 0.95, max_size: int = 1000):
        self.similarity_threshold = similarity_threshold
        self.max_size = max_size
        self.cache: Dict[str, _CacheEntry] = {}
        self._lock = asyncio.Lock()
        self.embedding_model = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
                logger.info("SentenceTransformer loaded for semantic caching")
            except Exception as e:
                logger.warning(f"Could not load SentenceTransformer: {e}")

    async def get(self, prompt: str) -> Optional[str]:
        async with self._lock:
            prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
            if prompt_hash in self.cache:
                METRIC_CACHE_HITS.inc()
                return self.cache[prompt_hash].value

            if self.embedding_model is not None and self.cache:
                emb = self.embedding_model.encode(prompt)
                for entry in self.cache.values():
                    if entry.embedding is None:
                        continue
                    sim = float(
                        np.dot(emb, entry.embedding)
                        / (np.linalg.norm(emb) * np.linalg.norm(entry.embedding) + 1e-8)
                    )
                    if sim >= self.similarity_threshold:
                        METRIC_CACHE_HITS.inc()
                        return entry.value
            METRIC_CACHE_MISSES.inc()
            return None

    async def set(self, prompt: str, response: str) -> None:
        async with self._lock:
            if len(self.cache) >= self.max_size:
                # Evict by smallest timestamp
                oldest_hash = min(self.cache.keys(),
                                  key=lambda k: self.cache[k].timestamp)
                del self.cache[oldest_hash]
            prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
            emb = None
            if self.embedding_model is not None:
                emb = self.embedding_model.encode(prompt)
            self.cache[prompt_hash] = _CacheEntry(
                value=response, timestamp=time.time(),
                embedding=emb, prompt=prompt,
            )


# ===========================================================================
# Templated fallback
# ===========================================================================
class TemplatedFallback:
    def __init__(self):
        self.templates = [
            "Based on available data, the recommended action is to proceed with caution.",
            "Due to current scarcity constraints, helium usage should be minimized.",
            "The system suggests optimizing workflows to reduce helium consumption.",
            "No specific recommendation can be generated at this time.",
        ]

    def generate(self, prompt: str) -> str:
        lower = prompt.lower()
        if "scarcity" in lower or "shortage" in lower:
            return ("Helium scarcity is currently high. Please reduce usage and "
                    "consider alternatives.")
        if "price" in lower:
            return "Helium prices are volatile. We recommend monitoring market trends."
        if "optimize" in lower or "efficiency" in lower:
            return ("Optimizing helium usage can lead to significant cost savings "
                    "and sustainability improvements.")
        return random.choice(self.templates)


# ===========================================================================
# Enhanced LLM Client
# ===========================================================================
class LLMClient:
    """
    Enhanced LLM client with all ten Green Agent enhancements wired in.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:8000/generate",
        model: str = "small",
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        retry_attempts: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0,
        fallback_generator: Optional[Callable[[str], str]] = None,
        # Feature toggles (kept compatible)
        enable_contextual_bandit: bool = True,
        enable_moe: bool = True,
        enable_ga: bool = True,
        enable_carbon_aware: bool = True,
        enable_self_healing: bool = True,
        enable_cache: bool = True,
        enable_metrics: bool = True,
        enable_lineage: bool = False,
        vault_url: Optional[str] = None,
        vault_token: Optional[str] = None,
        vault_secret_path: str = "llm/api_key",
        extra_endpoints: Optional[List[Tuple[str, str]]] = None,
        carbon_api_key: Optional[str] = None,
        carbon_region: str = "global",
        # Ten-enhancement toggles
        enable_limit_graph: bool = True,
        enable_rlhf: bool = True,
        enable_distillation: bool = True,
        enable_quantum_teacher: bool = True,
        enable_causal: bool = True,
        enable_federated: bool = True,
        enable_multi_agent: bool = True,
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_precision_switching: bool = True,
        enable_carbon_market: bool = True,
        enable_chaos: bool = False,
        chaos_fault_prob: float = 0.0,
        chaos_latency_ms: float = 0.0,
        chaos_carbon_spike_prob: float = 0.0,
        enable_hitl: bool = True,
        hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
        hitl_timeout_s: float = 2.0,
        # RLHF / distillation / LIMIT configuration
        rlhf_action_space: Optional[List[int]] = None,
        ga_evolve_every: int = 10,
        # Federated node id
        node_id: Optional[str] = None,
    ):
        self.endpoint = endpoint
        self.model = model
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.fallback_generator = fallback_generator or TemplatedFallback().generate
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self.ga_evolve_every = max(1, ga_evolve_every)

        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit_breaker = CircuitBreaker(
            name="primary",
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=circuit_breaker_timeout,
        )

        # ---- Core decision layers ----
        self.contextual_bandit_enabled = enable_contextual_bandit
        self.arms: List[Dict[str, Any]] = [
            {"temp": 0.7, "max_tokens": 150, "model": "small"},
            {"temp": 0.5, "max_tokens": 100, "model": "small"},
            {"temp": 0.9, "max_tokens": 200, "model": "medium"},
            {"temp": 0.3, "max_tokens": 50,  "model": "small"},
            {"temp": 0.8, "max_tokens": 300, "model": "large"},
        ]
        if enable_contextual_bandit:
            self.linucb = LinUCB(num_arms=len(self.arms), feature_dim=4, alpha=0.1)
            self.moo = MultiObjectiveOptimizer()
        else:
            self.linucb = None
            self.moo = None

        # ---- MOE router ----
        self.moe_enabled = enable_moe
        self.router: Optional[MOERouter] = None
        if enable_moe:
            self.router = MOERouter()
            # Register primary by reference; the router will call back via this self.
            self.router.add_teacher("primary", self)
            if extra_endpoints:
                for name, url in extra_endpoints:
                    teacher_client = LLMClient(
                        endpoint=url,
                        model=model,
                        headers=headers,
                        timeout=timeout,
                        retry_attempts=retry_attempts,
                        circuit_breaker_threshold=circuit_breaker_threshold,
                        circuit_breaker_timeout=circuit_breaker_timeout,
                        fallback_generator=fallback_generator,
                        enable_contextual_bandit=False,
                        enable_moe=False,
                        enable_ga=False,
                        enable_carbon_aware=False,
                        enable_self_healing=False,
                        enable_cache=False,
                        enable_metrics=False,
                        enable_lineage=False,
                        enable_quantum_teacher=False,
                        enable_causal=False,
                        enable_federated=False,
                        enable_multi_agent=False,
                        enable_temporal_logic=False,
                        enable_xai=False,
                        enable_precision_switching=False,
                        enable_carbon_market=False,
                        enable_chaos=False,
                        enable_hitl=False,
                        enable_rlhf=False,
                        enable_distillation=False,
                        enable_limit_graph=False,
                    )
                    self.router.add_teacher(name, teacher_client)

        # ---- GA ----
        self.ga_enabled = enable_ga
        self.ga: Optional[GeneticAlgorithmOptimizer] = None
        if enable_ga:
            self.ga = GeneticAlgorithmOptimizer()
            self.ga.initialize()
        self._calls_since_ga = 0

        # ---- Carbon market / REC / intensity ----
        self.carbon_aware_enabled = enable_carbon_aware
        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.recs = RECInventory() if enable_carbon_market else None
        self.carbon_manager = CarbonIntensityManager(
            api_key=carbon_api_key, region=carbon_region,
            market=self.carbon_market, recs=self.recs,
        )
        self.carbon_scheduler = CarbonAwareScheduler(self.carbon_manager)

        # ---- Self-healing ----
        self.self_healing_enabled = enable_self_healing
        self.self_healing = SelfHealingManager() if enable_self_healing else None

        # ---- Semantic cache ----
        self.cache_enabled = enable_cache
        self.cache = SemanticCache() if enable_cache else None

        # ---- Metrics / lineage ----
        self.metrics_enabled = enable_metrics
        self.lineage_enabled = enable_lineage
        self.lineage_records: deque = deque(maxlen=1000)

        # ---- Vault (v2 API, executor) ----
        self.vault_client = None
        self.vault_secret_path = vault_secret_path
        if VAULT_AVAILABLE and vault_url and vault_token:
            try:
                self.vault_client = VaultClient(url=vault_url, token=vault_token)
                # Do not block on init; fetch lazily in _do_request
                logger.info("Vault client initialized for key rotation")
            except Exception as e:
                logger.warning(f"Vault init failed: {e}")

        # =========== TEN ENHANCEMENTS ===========

        # Enhancement 1: Quantum-Distillation
        self.quantum_teacher = (QuantumInspiredTeacher(seed=0)
                                if enable_quantum_teacher else None)
        self.distill_ensemble = (DistillationEnsemble(self.quantum_teacher, alpha=0.5)
                                 if self.quantum_teacher else None)
        self.arm_distiller: Optional[MultiTeacherDistiller] = None
        if enable_distillation and enable_contextual_bandit:
            self.arm_distiller = MultiTeacherDistiller(
                teachers=[self._teacher_linucb, self._teacher_rule, self._teacher_static],
                weights=[1.0, 1.0, 0.5],
            )

        # Enhancement 2: Causal RL
        self.causal = (CausalCounterfactualEstimator(
            state_dim=4, n_actions=len(self.arms))
            if enable_causal else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # Enhancement 3: Federated
        self.federated = FederatedAggregator() if enable_federated else None

        # Enhancement 4: Multi-Agent
        self.roles = EmergentRoleRegistry() if enable_multi_agent else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # Enhancement 5: Temporal Logic
        self.temporal: Optional[TemporalLogicMonitor] = None
        self.shield: Optional[SafetyShield] = None
        if enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="latency_reasonable",
                predicate=lambda r: r.get("latency_ms", 0.0) <= 30000.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self.temporal.add_formula(STLFormula(
                name="eventually_success",
                predicate=lambda r: r.get("success", False),
                operator=STLOperator.EVENTUALLY, horizon=5))
            self.shield = SafetyShield(self.temporal)

        # Enhancement 6: XAI
        self.explainer = DecisionExplainer() if enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        # Enhancement 7: Adaptive Precision
        self.precision_controller = (PrecisionController(quality_floor=0.95)
                                     if enable_precision_switching else None)
        self.precision_adapter = (HardwareAwareAdapter(self.precision_controller)
                                  if self.precision_controller else None)
        self.current_precision: Optional[PrecisionLevel] = None

        # Enhancement 8: Carbon market / REC
        # (self.carbon_market and self.recs already created above)

        # Enhancement 9: Chaos
        self.chaos = (ChaosEngineer(ChaosConfig(
            fault_prob=chaos_fault_prob,
            latency_inject_ms=chaos_latency_ms,
            carbon_spike_prob=chaos_carbon_spike_prob))
            if enable_chaos else None)

        # Enhancement 10: HITL
        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = (HumanInTheLoopGate(approver=hitl_approver,
                                        timeout_s=hitl_timeout_s)
                     if enable_hitl else None)
        self.active_learner = ActiveLearningSampler() if enable_hitl else None

        # ---- RLHF (light integration; arm-level preference) ----
        self.rlhf_enabled = enable_rlhf
        if enable_rlhf and enable_contextual_bandit:
            self.rlhf = RLHFOptimizer(
                action_space=rlhf_action_space or list(range(len(self.arms))))
        else:
            self.rlhf = None

        # ---- LIMIT graph (static clip) ----
        self.limit_graph_enabled = enable_limit_graph
        self.limit_graph = LimitGraph() if enable_limit_graph else None

        atexit.register(self._atexit_cleanup)

    def _atexit_cleanup(self):
        try:
            if self._session and not self._session.closed:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._session.close())
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Teachers for the distillation ensemble
    # ------------------------------------------------------------------
    def _teacher_linucb(self, features: np.ndarray) -> int:
        if self.linucb is not None:
            return int(self.linucb.select_arm(features))
        return 0

    def _teacher_rule(self, features: np.ndarray) -> int:
        length = float(features[0]) if len(features) > 0 else 50.0
        if length > 200:
            return 4
        if length > 100:
            return 2
        return 0

    def _teacher_static(self, features: np.ndarray) -> int:
        return 2

    # ------------------------------------------------------------------
    # Vault (v2 API in executor)
    # ------------------------------------------------------------------
    async def _refresh_headers_from_vault(self) -> None:
        if not self.vault_client:
            return
        loop = asyncio.get_running_loop()
        try:
            secret = await loop.run_in_executor(
                None,
                lambda: self.vault_client.secrets.kv.v2.read_secret_version(
                    path=self.vault_secret_path, mount_point="secret"),
            )
            data = secret.get("data", {}).get("data", {}) or {}
            api_key = data.get("api_key")
            if api_key:
                self.headers = {**self.headers, "Authorization": f"Bearer {api_key}"}
        except Exception as e:
            logger.warning(f"Vault refresh failed: {e}")

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self.router:
            # Avoid recursion via primary == self
            for name, client in self.router.teachers:
                if client is not self:
                    with contextlib.suppress(Exception):
                        await client.close()
            self.router.teachers = [(n, c) for n, c in self.router.teachers
                                    if c is self or c is None]
        with contextlib.suppress(Exception):
            await self.carbon_manager.close()

    # ------------------------------------------------------------------
    # HTTP request
    # ------------------------------------------------------------------
    async def _do_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._get_session()
        if self.vault_client:
            await self._refresh_headers_from_vault()
        async with session.post(
            self.endpoint,
            json=payload,
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        ) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"LLM API returned {resp.status}",
                )
            body = await resp.json()
            if body is None:
                body = {}
            return body

    # ------------------------------------------------------------------
    # Reward and features
    # ------------------------------------------------------------------
    def _extract_features(self, prompt: str) -> np.ndarray:
        words = prompt.split()
        return np.array([
            len(prompt),
            len(words),
            float(np.mean([len(w) for w in words])) if words else 0.0,
            datetime.now().hour / 24.0,
        ])

    def _compute_reward(self, response: Optional[str]) -> float:
        if not response:
            return 0.0
        # Length term (saturating)
        length_score = min(1.0, len(response) / 200.0)
        # Diversity term (unique words / total words)
        tokens = response.lower().split()
        if tokens:
            diversity = len(set(tokens)) / len(tokens)
        else:
            diversity = 0.0
        # Key-phrase bonus
        bonus = 0.1 if any(k in response.lower()
                           for k in ("recommend", "optimize", "suggest")) else 0.0
        # Penalise obvious fallback boilerplate
        penalty = -0.1 if "no specific recommendation" in response.lower() else 0.0
        return float(max(0.0, min(1.0, 0.5 * length_score + 0.4 * diversity + bonus + penalty)))

    def _compute_quality_score(self, response: Optional[str]) -> float:
        return self._compute_reward(response)

    # ------------------------------------------------------------------
    # GA fitness
    # ------------------------------------------------------------------
    def _ga_fitness(self, arm: Dict[str, Any]) -> float:
        temp = float(arm.get("temp", 0.5))
        tokens = int(arm.get("max_tokens", 100))
        model = arm.get("model", "small")
        quality = 0.6 * (tokens / 500.0) + 0.4 * (1.0 if model == "large"
                                                 else 0.5 if model == "medium"
                                                 else 0.2)
        cost = tokens / 500.0 + (0.3 if model == "large"
                                 else 0.2 if model == "medium" else 0.1)
        return quality - 0.5 * cost

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------
    def _record_lineage(self, prompt: str, response: str,
                        params: Dict[str, Any]) -> None:
        self.lineage_records.append({
            "timestamp": datetime.utcnow().isoformat(),
            "prompt": prompt,
            "response": response,
            "params": params,
            "endpoint": self.endpoint,
            "model": self.model,
            "instance_id": self.node_id,
        })

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------
    async def generate_explanation(
        self,
        prompt: str,
        max_tokens: int = 150,
        temperature: float = 0.7,
        **kwargs: Any,
    ) -> str:
        call_start = time.time()

        # Chaos fault injection
        if self.chaos and self.chaos.maybe_fault():
            METRIC_FALLBACK.inc()
            return self.fallback_generator(prompt)

        # 1. Cache
        if self.cache_enabled and self.cache is not None:
            cached = await self.cache.get(prompt)
            if cached is not None:
                return cached

        # 2. Feature extraction
        features = self._extract_features(prompt)

        # 3. Arm selection
        params: Dict[str, Any] = {
            "max_tokens": max_tokens,
            "temperature": temperature,
            "model": self.model,
        }
        arm_idx = -1
        source = "default"
        if self.contextual_bandit_enabled and self.linucb is not None:
            if self.arm_distiller is not None:
                try:
                    arm_idx = int(self.arm_distiller.distill(features))
                except Exception:
                    arm_idx = self.linucb.select_arm(features)
                source = "distilled"
            elif self.rlhf is not None and random.random() < 0.1:
                sampled = self.rlhf.sample_action(features)
                arm_idx = int(sampled) if sampled is not None else 0
                source = "rlhf"
            else:
                arm_idx = int(self.linucb.select_arm(features))
                source = "linucb"
            if 0 <= arm_idx < len(self.arms):
                params.update(self.arms[arm_idx])
            else:
                arm_idx = 0
                params.update(self.arms[0])

        # 4. LIMIT graph clip
        if self.limit_graph is not None:
            try:
                limits = self.limit_graph.get_limits(features)
            except Exception:
                limits = {}
            if isinstance(limits, dict):
                if "max_tokens" in limits:
                    params["max_tokens"] = min(
                        int(params.get("max_tokens", 150)), int(limits["max_tokens"]))
                if "temperature" in limits:
                    params["temperature"] = min(
                        float(params.get("temperature", 0.7)),
                        float(limits["temperature"]))
                allowed_models = limits.get("model")
                if allowed_models in ("small", "medium", "large"):
                    params["model"] = allowed_models
            params["max_tokens"] = max(1, int(params.get("max_tokens", 1)))

        # 5. Carbon-aware adjustment (chaos spike + market)
        carbon_intensity = await self.carbon_manager.get_current_intensity()
        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
        carbon_price = await self.carbon_manager.get_current_price()
        if self.carbon_aware_enabled:
            # Temporarily overwrite intensity so scheduler sees the chaos-spiked value
            original_intensity = self.carbon_manager.current_intensity
            self.carbon_manager.current_intensity = carbon_intensity
            params = await self.carbon_scheduler.adjust_params(params)
            self.carbon_manager.current_intensity = original_intensity

        # 6. Precision switch
        if self.precision_adapter is not None:
            params, level = self.precision_adapter.adapt(
                params, carbon_intensity, 0.5, carbon_price)
            self.current_precision = level

        # 7. MOE teacher selection
        teacher_name = "primary"
        client = self
        if self.moe_enabled and self.router is not None:
            try:
                teacher_name, client = await self.router.select_teacher(prompt)
            except Exception as e:
                logger.warning(f"MOE selection failed: {e}")
                teacher_name, client = "primary", self

        # 8. Multi-agent vote nudge on the selected arm
        if self.coordinator is not None and self.roles is not None and arm_idx >= 0:
            bids: List[AgentBid] = []
            for role in AgentRole:
                idx = arm_idx
                if role == AgentRole.EXPLORER:
                    idx = random.randrange(len(self.arms))
                elif role == AgentRole.EXPLOITER:
                    idx = int(np.argmax([self._ga_fitness(a) for a in self.arms]))
                elif role == AgentRole.CARBON_BROKER:
                    # Prefer the smallest-token arm under carbon stress
                    idx = int(np.argmin([a.get("max_tokens", 150) for a in self.arms]))
                elif role == AgentRole.SAFETY_OFFICER:
                    idx = arm_idx
                bids.append(AgentBid(
                    agent_id=role.value, role=role, confidence=0.7,
                    proposed_action=idx, rationale=role.value,
                    carbon_score=1.0 / (1.0 + carbon_intensity / 1000.0),
                ))
            voted = self.coordinator.vote(bids, len(self.arms))
            if voted != arm_idx:
                arm_idx = voted
                params.update(self.arms[arm_idx])

        # 9. Chaos latency
        if self.chaos:
            delay_ms = self.chaos.maybe_latency()
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)

        # 10. Execute
        try:
            if client is self:
                result = await self._circuit_breaker.call(
                    self._do_request, {"prompt": prompt, **params})
                text = (result.get("text")
                        or result.get("generated_text")
                        or result.get("response")
                        or "")
            else:
                text = await client.generate_explanation(prompt, **params)
            success = bool(text)
            error = None
        except Exception as e:
            logger.warning(f"LLM generation failed: {e}")
            METRIC_FALLBACK.inc()
            text = self.fallback_generator(prompt)
            success = False
            error = str(e)

        latency_ms = (time.time() - call_start) * 1000.0

        # 11. Reward and learner updates
        reward = self._compute_reward(text) if success else 0.0
        if self.linucb is not None and arm_idx >= 0:
            self.linucb.update(arm_idx, features, reward)
        if self.moo is not None:
            # Real latency in seconds, saturating
            latency_s = max(0.001, latency_ms / 1000.0)
            outcome = [reward, 1.0 / (1.0 + latency_s), 1.0 / (1.0 + len(text) / 100.0)]
            self.moo.update_weights(outcome)
        if self.rlhf is not None and arm_idx >= 0:
            with contextlib.suppress(Exception):
                self.rlhf.update(features, arm_idx, reward)
        if self.router is not None and client is not self:
            await self.router.update(teacher_name, reward, prompt)
        elif self.router is not None:
            # Also record the primary teacher's outcome
            await self.router.update(teacher_name, reward, prompt)

        # 12. Causal counterfactuals
        state_vec = np.asarray(features, dtype=np.float64)
        if self.causal is not None:
            self.causal.update(CausalTransition(
                state=state_vec, action=arm_idx if arm_idx >= 0 else 0,
                reward=reward, next_state=state_vec))
            self.last_counterfactuals = self.causal.counterfactuals(
                state_vec, len(self.arms))

        # 13. Federated submit
        if self.federated is not None:
            self.federated.submit(FederatedUpdate(
                node_id=self.node_id,
                weights={"local": np.array([reward], dtype=np.float64)},
                n_samples=1, carbon_intensity=float(carbon_intensity)))
            self.federated.aggregate()

        # 14. Temporal monitor + safety shield
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.temporal and self.shield:
            self.temporal.observe({
                "latency_ms": latency_ms,
                "success": success,
                "carbon_intensity": carbon_intensity,
            })
            candidates = [
                {"value": latency_ms, "text_len": len(text), "prompt_len": len(prompt)}
            ]
            _, safety_ok, stl_verdict = self.shield.screen(
                candidates[0], candidates, score_key="value")

        # 15. Uncertainty + HITL
        uncertainty_score = 0.0
        human_approved = True
        if self.uncertainty and self.hitl:
            # Use arm distribution as a proxy for probs
            probs = None
            if self.linucb is not None:
                try:
                    scores = []
                    for a in range(len(self.arms)):
                        A_inv = np.linalg.inv(self.linucb.A[a])
                        theta = A_inv.dot(self.linucb.b[a])
                        scores.append(float(theta.dot(features)))
                    probs = np.exp(np.array(scores) - np.max(scores))
                    probs = probs / probs.sum()
                except Exception:
                    probs = None
            is_unc, unc = self.uncertainty.is_uncertain(probs)
            uncertainty_score = unc
            if is_unc or not safety_ok:
                req = HITLRequest(
                    request_id=uuid.uuid4().hex[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=arm_idx, candidates=list(self.arms),
                    uncertainty=unc, carbon_price=carbon_price)
                human_approved = await self.hitl.request(req)
                if not human_approved and success:
                    # Conservative fallback to shortest arm
                    smallest = min(self.arms, key=lambda a: a.get("max_tokens", 999))
                    text = await self._safe_fallback_regenerate(prompt, smallest)
            self.uncertainty.observe(reward)
            if self.active_learner:
                self.active_learner.maybe_store(
                    {"prompt": prompt, "arm_idx": arm_idx, "reward": reward},
                    uncertainty=unc, threshold=0.4)

        # 16. XAI
        if self.explainer is not None:
            chosen = {"prompt_len": float(features[0]),
                      "num_words": float(features[1]),
                      "avg_word_len": float(features[2]),
                      "hour_norm": float(features[3])}
            pareto = [{
                "prompt_len": float(features[0]),
                "num_words": float(features[1]),
                "avg_word_len": float(features[2]),
                "hour_norm": float(features[3]),
                "value": float(self._ga_fitness(a)),
            } for a in self.arms]
            explanation = self.explainer.explain(
                chosen_idx=arm_idx if arm_idx >= 0 else 0,
                chosen=chosen, pareto=pareto,
                counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved,
                carbon_price=carbon_price,
                confidence=float(np.max(probs)) if probs is not None else 0.5,
            )
            self.last_explanation = explanation

        # 17. GA evolution (periodic, uses real call count)
        if self.ga is not None:
            self._calls_since_ga += 1
            if self._calls_since_ga >= self.ga_evolve_every:
                self._calls_since_ga = 0
                best_arm = self.ga.evolve(self._ga_fitness, generations=3)
                if best_arm:
                    worst_idx = int(np.argmin([self._ga_fitness(a) for a in self.arms]))
                    self.arms[worst_idx] = best_arm
                    logger.info(f"GA evolved arm {worst_idx} -> {best_arm}")

        # 18. Self-healing
        if self.self_healing is not None:
            quality = self._compute_quality_score(text)
            await self.self_healing.record_quality(quality)
            anomaly, _score = await self.self_healing.detect_anomaly(quality)
            if anomaly:
                await self.self_healing.trigger_recovery()
                if success:
                    text = self.fallback_generator(prompt)

        # 19. Cache
        if self.cache_enabled and self.cache is not None and success:
            await self.cache.set(prompt, text)

        # 20. Lineage
        if self.lineage_enabled:
            self._record_lineage(prompt, text, params)

        return text

    async def _safe_fallback_regenerate(self, prompt: str,
                                        params: Dict[str, Any]) -> str:
        try:
            result = await self._circuit_breaker.call(
                self._do_request, {"prompt": prompt, **params})
            return (result.get("text")
                    or result.get("generated_text")
                    or result.get("response")
                    or self.fallback_generator(prompt))
        except Exception:
            return self.fallback_generator(prompt)

    # ------------------------------------------------------------------
    # Batch and stream
    # ------------------------------------------------------------------
    async def batch_generate_explanations(self, prompts: List[str],
                                          **kwargs: Any) -> List[str]:
        return [await self.generate_explanation(p, **kwargs) for p in prompts]

    async def stream_explanation(self, prompt: str,
                                 **kwargs: Any) -> AsyncIterable[str]:
        response = await self.generate_explanation(prompt, **kwargs)
        yield response

    async def __aenter__(self) -> "LLMClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        return self._circuit_breaker.get_status()

    def get_stats(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "node_id": self.node_id,
            "circuit_breaker": self.get_circuit_breaker_status(),
            "cache": {"enabled": self.cache_enabled,
                      "size": len(self.cache.cache) if self.cache else 0},
            "lineage": {"records": len(self.lineage_records)},
            "ten_enhancements": {
                "quantum_distillation": self.quantum_teacher is not None,
                "causal": self.causal is not None,
                "federated": self.federated is not None,
                "multi_agent": self.coordinator is not None,
                "temporal_logic": self.temporal is not None,
                "xai": self.explainer is not None,
                "precision_switching": self.precision_adapter is not None,
                "carbon_market": self.carbon_market is not None,
                "chaos": self.chaos is not None,
                "hitl": self.hitl is not None,
            },
            "current_precision": self.current_precision.value
                if self.current_precision else None,
            "carbon": {"intensity": self.carbon_manager.current_intensity,
                       "rec_kwh": self.recs.total_kwh() if self.recs else 0.0},
            "rlhf": {"enabled": self.rlhf is not None},
            "distillation": {"enabled": self.arm_distiller is not None},
            "limit_graph": {"enabled": self.limit_graph is not None},
            "last_explanation": self.last_explanation.to_dict()
                if self.last_explanation else None,
            "counterfactuals": self.last_counterfactuals,
            "federated_rounds": len(self.federated._updates) if self.federated else 0,
            "active_learning_size": len(self.active_learner) if self.active_learner else 0,
            "uncertainty_last": self.uncertainty.variance() if self.uncertainty else 0.0,
        }
        if self.moo is not None:
            stats["moo"] = {"weights": list(self.moo.weights)}
        if self.ga is not None:
            stats["ga"] = {
                "population_size": self.ga.pop_size,
                "arm_history": len(self.ga.arm_history),
            }
        if self.router is not None:
            stats["moe"] = self.router.get_stats()
        if self.self_healing is not None:
            stats["self_healing"] = self.self_healing.get_stats()
        if self.roles is not None:
            stats["roles"] = {r.value: w for r, w in self.roles.weights().items()}
        if self.temporal is not None:
            stats["stl_verdict"] = self.temporal.verify()
        if self.hitl is not None:
            stats["hitl_audit_size"] = len(self.hitl.audit)
        return stats


# ===========================================================================
# Local fallbacks for external enhancement modules
# ===========================================================================
class LimitGraph:
    def __init__(self, *a, **kw):
        self.limits: Dict[str, Any] = {}
        self._feedback: List[Any] = []

    def build_graph(self, nodes, edges): pass

    def get_limits(self, context):
        # Default policy: allow the largest tokens/temperature
        return {"max_tokens": 500, "temperature": 1.0}

    def update_from_feedback(self, feedback):
        self._feedback.append(feedback)


class RLHFOptimizer:
    def __init__(self, action_space, *a, **kw):
        self.actions = list(action_space)
        self.scores = {a: 0.0 for a in self.actions}

    def update(self, context, action, reward):
        if action not in self.scores:
            self.actions.append(action)
            self.scores[action] = 0.0
        self.scores[action] += 0.1 * (float(reward) - self.scores[action])

    def sample_action(self, context):
        if not self.scores:
            return None
        if random.random() < 0.1:
            return random.choice(self.actions)
        return max(self.scores, key=self.scores.get)


class MultiTeacherDistiller:
    def __init__(self, teachers, weights=None, *a, **kw):
        self.teachers = list(teachers)
        self.weights = list(weights) if weights else [1.0] * len(self.teachers)

    def _call(self, t, ctx):
        try:
            out = t(ctx)
            if asyncio.iscoroutine(out):
                out.close()
                return None
            return out
        except Exception:
            return None

    def distill(self, context):
        votes: Dict[Any, float] = {}
        for t, w in zip(self.teachers, self.weights):
            c = self._call(t, context)
            if c is not None:
                votes[c] = votes.get(c, 0.0) + float(w)
        return max(votes, key=votes.get) if votes else None


# ===========================================================================
# Backward-compatible aliases
# ===========================================================================
MOPDOptimizer = LinUCB
MultiTeacherRouter = MOERouter


# ===========================================================================
# Smoke test
# ===========================================================================
async def _smoke_test() -> None:
    async def fake_executor(policy, node, workload):  # unused
        return {}

    # A fake local endpoint so we don't hit a real server
    async def _fake_do_request(self, payload):
        return {"text": f"Recommend optimizing for prompt length {len(payload.get('prompt', ''))}"}

    LLMClient._do_request = _fake_do_request  # type: ignore

    client = LLMClient(endpoint="http://fake", enable_cache=True,
                       enable_chaos=False)
    for _ in range(3):
        text = await client.generate_explanation("How do we reduce helium usage?")
        print(f"→ {text[:80]}...")
    stats = client.get_stats()
    print(json.dumps({k: v for k, v in stats.items()
                      if k != "ten_enhancements"}, indent=2, default=str))
    print("ten_enhancements:", stats["ten_enhancements"])
    await client.close()


if __name__ == "__main__":
    asyncio.run(_smoke_test())
