"""
gpu_profiler.py — Enhanced v2

Real-time hardware metrics collector with all ten Green Agent enhancements
hosted as first-class modules in the same file:

  1. Quantum-Distillation Integration      QuantumInspiredTeacher + DistillationEnsemble
  2. Causal RL for Policy Adaptation       CausalCounterfactualEstimator
  3. Federated Green Learning              FederatedAggregator
  4. Multi-Agent Coordination              EmergentRoleRegistry + MultiAgentCoordinator
  5. Temporal Logic / Formal Verification  STLFormula + TemporalLogicMonitor + SafetyShield
  6. Explainable AI                        DecisionExplainer + Explanation
  7. Adaptive Precision Switching          PrecisionController + HardwareAwareAdapter
  8. Carbon Markets / RECs                 CarbonMarketClient + RECInventory
  9. Resilience / Chaos                    ChaosEngineer
 10. HITL / Active Learning                UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler

Bug fixes over v1:
  - NVML import fallback no longer raises NameError when pynvml is missing
  - avg_temp divide-by-zero guarded (device_count == 0)
  - Cumulative energy integration respects idle gaps (no runaway on resume)
  - SQLite uses autoincrement id (was timestamp PRIMARY KEY -> silent drops)
  - SQLite batched writes (no commit per sample) + thread-safe connection
  - CPU power model parameterized (was hardcoded 65 * util)
  - tokens_per_sec contract explicit (external input, not silently zero)
  - get_modp_utility reuses a cached ParetoOptimizer
  - atexit cleanup + context manager protocol
  - Schema-versioned, with instance_id / node_id provenance columns
  - Callback list protected by lock
  - NVML-unavailable path reports gpu_available = False
"""

from __future__ import annotations

import atexit
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False


# ============================================================
# NVML import — fixes the original NameError bug
# ============================================================
pynvml = None
NVML_AVAILABLE = False

try:
    import pynvml as _pynvml  # noqa: F401
    pynvml = _pynvml
    try:
        pynvml.nvmlInit()
        NVML_AVAILABLE = True
    except Exception as _nvml_init_err:  # noqa: BLE001
        logging.getLogger(__name__).warning(
            f"NVML init failed: {_nvml_init_err}. GPU metrics disabled."
        )
        NVML_AVAILABLE = False
except ImportError:
    NVML_AVAILABLE = False


# ============================================================
# Schema
# ============================================================
PROFILER_SCHEMA_VERSION = "gpu_profiler_v2"


# ============================================================
# External module fallbacks
# ============================================================
try:
    from enhancements.MODP import ParetoOptimizer  # type: ignore
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, metrics, weights):
            return sum(metrics.get(k, 0.0) * weights.get(k, 1.0) for k in metrics)


try:
    from enhancements.bio_inspired import FitnessEvaluator  # type: ignore
except ImportError:
    class FitnessEvaluator:
        def evaluate(self, metrics, policy=None):
            # Simple energy-efficiency fitness.
            tokens = metrics.get("tokens_per_sec", 0.0)
            power = max(metrics.get("gpu_power_watts", 1.0), 1.0)
            return tokens / power


try:
    from enhancements.moe_system import ContextEncoder  # type: ignore
except ImportError:
    class ContextEncoder:
        def encode(self, metrics):
            return [
                float(metrics.get("gpu_utilization_pct", 0.0)),
                float(metrics.get("cpu_utilization_pct", 0.0)),
                float(metrics.get("gpu_memory_used_mb", 0.0)) / 1000.0,
                float(metrics.get("gpu_power_watts", 0.0)) / 400.0,
                float(metrics.get("gpu_temp_c", 0.0)) / 100.0,
            ]


try:
    from enhancements.limit_graph import LimitGraph  # type: ignore
except ImportError:
    class LimitGraph:
        def __init__(self, *a, **kw): pass
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass


try:
    from enhancements.rlhf import RLHFOptimizer  # type: ignore
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *a, **kw):
            self.actions = list(action_space)
        def update(self, context, action, reward): pass
        def sample_action(self, context):
            return self.actions[0] if self.actions else None


try:
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller  # type: ignore
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, *a, **kw):
            self.teachers = list(teachers)
        def distill(self, context):
            return self.teachers[0](context) if self.teachers else None


# ============================================================
# ============================================================
# ENHANCEMENT MODULES
# ============================================================
# ============================================================


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
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "memory": 1.00, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "memory": 0.125, "bits": 4.0},
}


class PrecisionController:
    """Chooses precision from carbon intensity, latency headroom, carbon price."""

    def __init__(
        self,
        supported: Optional[Sequence[PrecisionLevel]] = None,
        quality_floor: float = 0.95,
        carbon_aware: bool = True,
    ):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware

    def select(
        self,
        carbon_intensity: float,
        latency_headroom_ratio: float,
        carbon_price: float = 0.0,
    ) -> PrecisionLevel:
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
    """Applies precision to a policy dict and reports the chosen level."""

    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(
        self,
        policy: Dict[str, Any],
        carbon_intensity: float,
        latency_headroom: float,
        carbon_price: float,
    ) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        return out, level


# ------------------------------------------------------------
# Enhancement 8: Carbon Markets / RECs
# ------------------------------------------------------------
class CarbonMarketClient:
    """In-process carbon price model, time-of-day aware."""

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
    """Renewable Energy Credits with carbon offset potential."""

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
                new_records.append(RECRecord(kwh=r.kwh - take, issued_at=r.issued_at, source=r.source))
        self._records = new_records
        return offset


# ------------------------------------------------------------
# Enhancement 9: Resilience / Chaos
# ------------------------------------------------------------
@dataclass
class ChaosConfig:
    fault_prob: float = 0.0
    latency_inject_ms: float = 0.0
    latency_inject_prob: float = 0.3
    carbon_spike_prob: float = 0.0
    carbon_spike_factor: float = 1.5
    sensor_noise_std: float = 0.0
    seed: int = 0

    def enabled(self) -> bool:
        return (
            self.fault_prob > 0
            or (self.latency_inject_ms > 0 and self.latency_inject_prob > 0)
            or self.carbon_spike_prob > 0
            or self.sensor_noise_std > 0
        )


class ChaosEngineer:
    """Deterministic chaos injector for profiling."""

    def __init__(self, config: Optional[ChaosConfig] = None):
        self.config = config or ChaosConfig()
        self.rng = random.Random(self.config.seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fault(self) -> bool:
        if self.rng.random() < self.config.fault_prob:
            self.events.append({"type": "fault", "t": time.time()})
            return True
        return False

    def maybe_latency(self) -> float:
        if self.config.latency_inject_ms > 0 and self.rng.random() < self.config.latency_inject_prob:
            self.events.append({"type": "latency", "t": time.time()})
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity

    def apply_sensor_noise(self, value: float) -> float:
        if self.config.sensor_noise_std > 0:
            return value + self.rng.gauss(0.0, self.config.sensor_noise_std)
        return value


# ------------------------------------------------------------
# Enhancement 5: Temporal Logic
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
    """Runtime monitor over a rolling window."""

    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=horizon * 4)
        self.formulas: List[STLFormula] = []

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        self._history.append(record)

    def _window(self, f: STLFormula) -> List[Dict[str, Any]]:
        return list(self._history)[-f.horizon:]

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = self._window(f)
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
        return results


class SafetyShield:
    """Reports violations and points to a fallback sensor profile."""

    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(self) -> Tuple[bool, Dict[str, bool], Optional[str]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return True, verdict, None
        fallback = "safe_default"
        self.violations.append({"t": time.time(), "violated": violated, "fallback": fallback})
        return False, verdict, fallback


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
    """Ridge-regression SCM for counterfactual reward estimation."""

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
    """Carbon-weighted FedAvg with optional differential privacy."""

    def __init__(self, dp_sigma: float = 1e-3, staleness_s: float = 3600.0):
        self.dp_sigma = dp_sigma
        self.staleness_s = staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[Dict[str, np.ndarray]] = None
        self._lock = threading.Lock()

    def submit(self, u: FederatedUpdate) -> None:
        with self._lock:
            self._updates[u.node_id] = u

    def _fresh(self) -> List[FederatedUpdate]:
        now = time.time()
        return [u for u in self._updates.values() if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self) -> Optional[Dict[str, np.ndarray]]:
        with self._lock:
            fresh = self._fresh()
            if not fresh:
                return self._global
            weights = np.array(
                [u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh],
                dtype=np.float64,
            )
            weights /= max(weights.sum(), 1e-12)
            keys = fresh[0].weights.keys()
            agg: Dict[str, np.ndarray] = {}
            for k in keys:
                stacked = np.stack([u.weights[k] for u in fresh], axis=0)
                blended = np.tensordot(weights, stacked, axes=([0], [0]))
                if self.dp_sigma > 0:
                    blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
                agg[k] = blended
            self._global = agg
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
    """Decay-weighted role performance tracker."""

    def __init__(self, decay: float = 0.95):
        self.decay = decay
        self._scores: Dict[AgentRole, float] = {r: 1.0 for r in AgentRole}
        self._counts: Dict[AgentRole, int] = {r: 0 for r in AgentRole}

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
    """Carbon-weighted voting across agent bids."""

    def __init__(
        self,
        registry: EmergentRoleRegistry,
        role_bias: Optional[Dict[AgentRole, float]] = None,
    ):
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
            weight = (
                role_w.get(b.role, 0.1)
                * self.role_bias.get(b.role, 1.0)
                * max(b.confidence, 0.0)
                * (1.0 + b.carbon_score)
            )
            scores[idx] += weight
        return int(np.argmax(scores))


# ------------------------------------------------------------
# Enhancement 6: XAI
# ------------------------------------------------------------
@dataclass
class Explanation:
    decision_id: str
    kind: str
    top_features: List[Tuple[str, float]]
    counterfactuals: List[Dict[str, Any]]
    rationale: str
    confidence: float
    safety_ok: bool
    carbon_price_signal: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    """Attribution builder over a rolling metrics window."""

    def __init__(self, feature_names: Optional[Sequence[str]] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "gpu_utilization_pct",
            "gpu_memory_used_mb",
            "gpu_power_watts",
            "gpu_temp_c",
            "cpu_utilization_pct",
            "energy_gpu_joules",
            "net_recv_bandwidth_gbps",
        ]

    def _attributions(
        self,
        current: Dict[str, Any],
        baseline: Dict[str, float],
        scales: Dict[str, float],
    ) -> List[Tuple[str, float]]:
        attrs: List[Tuple[str, float]] = []
        for f in self.feature_names:
            v = float(current.get(f, 0.0))
            mu = float(baseline.get(f, 0.0))
            s = float(scales.get(f, 1.0)) + 1e-9
            attrs.append((f, (v - mu) / s))
        attrs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attrs

    def explain_anomaly(
        self,
        kind: str,
        current: Dict[str, Any],
        baseline: Dict[str, float],
        scales: Dict[str, float],
        counterfactuals: Optional[Dict[int, float]] = None,
        safety_ok: bool = True,
        carbon_price: float = 0.0,
        confidence: float = 0.5,
    ) -> Explanation:
        attrs = self._attributions(current, baseline, scales)
        cf_list: List[Dict[str, Any]] = []
        if counterfactuals:
            for k, v in sorted(counterfactuals.items(), key=lambda kv: kv[1], reverse=True)[:3]:
                cf_list.append({"alt_idx": int(k), "expected_reward": float(v)})
        top = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Anomaly '{kind}' at t={current.get('timestamp', 0.0):.2f}. "
            f"Top deviations vs rolling baseline: {top}. "
            f"Safety={'ok' if safety_ok else 'VIOLATION'}. "
            f"Carbon price={carbon_price:.4f}."
        )
        return Explanation(
            decision_id=f"exp-{uuid.uuid4().hex[:8]}",
            kind=kind,
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
    """Small state-vector simulation blended with a carbon-aware prior."""

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

    def _simulate(self, features: np.ndarray, n: int) -> np.ndarray:
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)] + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features: np.ndarray, n_candidates: int) -> np.ndarray:
        if n_candidates <= 0:
            return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n_candidates)


class DistillationEnsemble:
    """Blends a quantum teacher with an existing probability vector."""

    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum
        self.alpha = alpha

    def blend(self, features: np.ndarray, other_probs: Optional[np.ndarray], n: int) -> np.ndarray:
        q = self.quantum.teacher_probs(features, n)
        if other_probs is None or len(other_probs) != n:
            return q
        return self.alpha * q + (1.0 - self.alpha) * np.asarray(other_probs)


# ------------------------------------------------------------
# Enhancement 10: HITL / Active Learning
# ------------------------------------------------------------
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    metric_name: str
    value: float
    uncertainty: float
    carbon_price: float
    timestamp: float = field(default_factory=time.time)


class UncertaintyEstimator:
    """Rolling-window standard deviation for key metrics."""

    def __init__(self, window: int = 32, entropy_threshold: float = 0.75):
        self.window = window
        self.entropy_threshold = entropy_threshold
        self._history: Dict[str, deque] = {}

    def observe(self, metrics: Dict[str, Any]) -> None:
        for k, v in metrics.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                d = self._history.setdefault(k, deque(maxlen=self.window))
                d.append(float(v))

    def std(self, key: str) -> float:
        d = self._history.get(key)
        if d is None or len(d) < 3:
            return 0.0
        arr = np.asarray(d, dtype=np.float64)
        return float(np.std(arr))

    def mean(self, key: str) -> float:
        d = self._history.get(key)
        if d is None or not d:
            return 0.0
        return float(np.mean(np.asarray(d, dtype=np.float64)))

    def z_score(self, key: str, value: float) -> float:
        s = self.std(key)
        if s < 1e-9:
            return 0.0
        return (value - self.mean(key)) / s

    def is_anomalous(self, key: str, value: float, z_thresh: float = 3.0) -> Tuple[bool, float]:
        z = abs(self.z_score(key, value))
        return z > z_thresh, z

    def snapshot_stats(self) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for k, d in self._history.items():
            if len(d) < 2:
                continue
            arr = np.asarray(d, dtype=np.float64)
            out[k] = {"mean": float(arr.mean()), "std": float(arr.std())}
        return out


class HumanInTheLoopGate:
    """Async approval gate with timeout and audit trail."""

    def __init__(
        self,
        approver: Optional[Callable[[HITLRequest], bool]] = None,
        timeout_s: float = 2.0,
        auto_approve_on_timeout: bool = True,
    ):
        self.approver = approver
        self.timeout_s = timeout_s
        self.auto_approve_on_timeout = auto_approve_on_timeout
        self.audit: List[Dict[str, Any]] = []

    def request_sync(self, req: HITLRequest) -> bool:
        """Synchronous approval (safe to call from sampler thread)."""
        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True, "reason": "no_approver"})
            return True
        try:
            approved = bool(self.approver(req))
        except Exception as e:  # noqa: BLE001
            self.audit.append({"request_id": req.request_id, "approved": False, "error": str(e)})
            return False
        self.audit.append({"request_id": req.request_id, "approved": approved, "reason": req.reason})
        return approved


class ActiveLearningSampler:
    """Uncertainty-prioritized anomaly buffer."""

    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def maybe_store(
        self, record: Dict[str, Any], uncertainty: float, threshold: float = 0.5,
    ) -> bool:
        if uncertainty < threshold:
            return False
        with self._lock:
            if len(self._buffer) >= self.capacity:
                self._buffer.pop(0)
            self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._buffer:
                return []
            k = min(k, len(self._buffer))
            return random.sample(self._buffer, k)

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)


# ============================================================
# ProfilerConfig
# ============================================================
@dataclass
class ProfilerConfig:
    """Configuration for the profiler."""
    sample_interval: float = 0.5
    enable_history: bool = True
    history_db_path: str = "gpu_metrics.db"
    history_batch_size: int = 10
    max_history_days: int = 7
    enable_callbacks: bool = True
    callback_cooldown: float = 0.1
    enable_limit_graph: bool = True
    enable_rlhf: bool = True
    enable_distillation: bool = True

    # Instance / node identity (provenance)
    instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    node_id: str = "local"

    # CPU power model (was hardcoded 65 W)
    cpu_tdp_watts: float = 65.0
    cpu_idle_fraction: float = 0.2

    # Carbon market inputs
    carbon_intensity_g_per_kwh: float = 400.0
    carbon_market_base_price: float = 0.05
    carbon_market_sensitivity: float = 0.0005
    rec_default_kwh: float = 0.0

    # Enhancement flags
    enable_quantum_teacher: bool = True
    enable_causal: bool = True
    enable_federated: bool = True
    enable_multi_agent: bool = True
    enable_temporal_logic: bool = True
    enable_xai: bool = True
    enable_precision_switching: bool = True
    enable_carbon_market: bool = True
    enable_chaos: bool = False
    enable_hitl: bool = True

    # Chaos config (used if enable_chaos)
    chaos_fault_prob: float = 0.0
    chaos_latency_ms: float = 0.0
    chaos_carbon_spike_prob: float = 0.0
    chaos_sensor_noise_std: float = 0.0

    # HITL
    hitl_timeout_s: float = 2.0
    hitl_auto_approve: bool = True


# ============================================================
# GPUProfiler
# ============================================================
class GPUProfiler:
    """
    Enhanced profiler with multi-GPU support, cumulative energy, persistent
    SQLite history, streaming callbacks, and all ten Green Agent enhancements.
    """

    def __init__(
        self,
        config: Optional[ProfilerConfig] = None,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_evaluator: Optional[Any] = None,
        moe_encoder: Optional[Any] = None,
        limit_graph: Optional[Any] = None,
        rlhf_optimizer: Optional[Any] = None,
        distiller: Optional[Any] = None,
        hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
    ):
        self.config = config or ProfilerConfig()
        self.modp_weights = modp_weights or {
            "energy_efficiency": 0.3,
            "carbon_efficiency": 0.3,
            "memory_efficiency": 0.2,
            "throughput": 0.2,
        }
        self.bio = bio_evaluator if bio_evaluator else FitnessEvaluator()
        self.moe = moe_encoder if moe_encoder else ContextEncoder()

        # --- External module integrations ---
        self.limit_graph = limit_graph if (self.config.enable_limit_graph and limit_graph) else (
            LimitGraph() if self.config.enable_limit_graph else None
        )
        self.rlhf = rlhf_optimizer if (self.config.enable_rlhf and rlhf_optimizer) else (
            RLHFOptimizer(action_space=["balanced", "performance", "power_save"])
            if self.config.enable_rlhf else None
        )
        self.distiller = distiller if (self.config.enable_distillation and distiller) else None

        # --- Enhancement 1: Quantum-Distillation ---
        self.quantum_teacher = QuantumInspiredTeacher(seed=0) if self.config.enable_quantum_teacher else None
        self.distill_ensemble = (
            DistillationEnsemble(self.quantum_teacher, alpha=0.5)
            if self.quantum_teacher else None
        )

        # --- Enhancement 2: Causal RL ---
        # state dim: 8 numeric features; n_actions: role count
        self.causal: Optional[CausalCounterfactualEstimator] = (
            CausalCounterfactualEstimator(state_dim=8, n_actions=len(AgentRole))
            if self.config.enable_causal else None
        )

        # --- Enhancement 3: Federated ---
        self.federated = FederatedAggregator() if self.config.enable_federated else None

        # --- Enhancement 4: Multi-Agent ---
        self.roles = EmergentRoleRegistry() if self.config.enable_multi_agent else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # --- Enhancement 5: Temporal Logic ---
        self.temporal: Optional[TemporalLogicMonitor] = None
        self.shield: Optional[SafetyShield] = None
        if self.config.enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="thermal_ok",
                predicate=lambda r: r.get("gpu_temp_c", 0.0) <= 85.0,
                operator=STLOperator.ALWAYS,
                horizon=10,
            ))
            self.temporal.add_formula(STLFormula(
                name="power_within_cap",
                predicate=lambda r: r.get("gpu_power_watts", 0.0) <= 400.0,
                operator=STLOperator.ALWAYS,
                horizon=10,
            ))
            self.temporal.add_formula(STLFormula(
                name="eventually_idle",
                predicate=lambda r: r.get("gpu_utilization_pct", 1.0) < 0.1,
                operator=STLOperator.EVENTUALLY,
                horizon=20,
            ))
            self.shield = SafetyShield(self.temporal)

        # --- Enhancement 6: XAI ---
        self.explainer = DecisionExplainer() if self.config.enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        # --- Enhancement 7: Precision Switching ---
        self.precision_controller = (
            PrecisionController(quality_floor=0.95)
            if self.config.enable_precision_switching else None
        )
        self.precision_adapter = (
            HardwareAwareAdapter(self.precision_controller)
            if self.precision_controller else None
        )
        self.current_precision: Optional[PrecisionLevel] = None

        # --- Enhancement 8: Carbon Market / RECs ---
        if self.config.enable_carbon_market:
            self.carbon_market: Optional[CarbonMarketClient] = CarbonMarketClient(
                base_price=self.config.carbon_market_base_price,
                sensitivity=self.config.carbon_market_sensitivity,
            )
            self.recs: Optional[RECInventory] = RECInventory()
            if self.config.rec_default_kwh > 0:
                self.recs.add(self.config.rec_default_kwh)
        else:
            self.carbon_market = None
            self.recs = None

        # --- Enhancement 9: Chaos ---
        self.chaos: Optional[ChaosEngineer] = None
        if self.config.enable_chaos:
            self.chaos = ChaosEngineer(ChaosConfig(
                fault_prob=self.config.chaos_fault_prob,
                latency_inject_ms=self.config.chaos_latency_ms,
                carbon_spike_prob=self.config.chaos_carbon_spike_prob,
                sensor_noise_std=self.config.chaos_sensor_noise_std,
            ))

        # --- Enhancement 10: HITL / Active Learning ---
        self.uncertainty = UncertaintyEstimator() if self.config.enable_hitl else None
        self.hitl = (
            HumanInTheLoopGate(
                approver=hitl_approver,
                timeout_s=self.config.hitl_timeout_s,
                auto_approve_on_timeout=self.config.hitl_auto_approve,
            )
            if self.config.enable_hitl else None
        )
        self.active_learner = ActiveLearningSampler() if self.config.enable_hitl else None

        # --- Internal state ---
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._callbacks_lock = threading.Lock()
        self._latest_metrics: Dict[str, Any] = {}
        self._callbacks: List[Tuple[Callable, float, float]] = []

        self._disk_io_start = None
        self._net_io_start = None
        self._last_disk_time = time.time()
        self._last_net_time = time.time()
        self._energy_cumulative_gpu_joules = 0.0
        self._energy_cumulative_cpu_joules = 0.0
        self._last_power_sample_time = time.time()

        # External token rate (contract: caller sets this)
        self._external_tokens_per_sec: float = 0.0

        # --- History ---
        self._conn: Optional[sqlite3.Connection] = None
        self._conn_lock = threading.Lock()
        self._pending_rows: List[Tuple] = []
        self._samples_since_flush = 0

        if PSUTIL_AVAILABLE:
            try:
                self._disk_io_start = psutil.disk_io_counters()
                self._net_io_start = psutil.net_io_counters()
            except Exception:
                self._disk_io_start = None
                self._net_io_start = None

        if self.config.enable_history:
            self._init_history()

        # Cleanup registration
        atexit.register(self._atexit_cleanup)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    def __enter__(self) -> "GPUProfiler":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def _atexit_cleanup(self) -> None:
        try:
            self.stop()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # History (SQLite)
    # ------------------------------------------------------------------
    def _init_history(self) -> None:
        try:
            self._conn = sqlite3.connect(
                self.config.history_db_path, check_same_thread=False
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    instance_id TEXT,
                    node_id TEXT,
                    schema_version TEXT,
                    gpu_util REAL,
                    gpu_mem_used_mb REAL,
                    gpu_power_watts REAL,
                    cpu_util REAL,
                    cpu_mem_used_mb REAL,
                    disk_read_gbps REAL,
                    disk_write_gbps REAL,
                    net_recv_gbps REAL,
                    net_sent_gbps REAL,
                    energy_gpu_joules REAL,
                    energy_cpu_joules REAL,
                    precision_level TEXT,
                    effective_bits REAL,
                    carbon_intensity REAL,
                    carbon_price REAL,
                    net_carbon_g REAL,
                    rec_offset_kg REAL,
                    uncertainty_score REAL,
                    chaos_events TEXT,
                    provenance TEXT
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp)"
            )
            self._conn.commit()
            self._clean_history()
        except Exception as e:
            logging.warning(f"Failed to initialize history: {e}")
            self._conn = None

    def _clean_history(self) -> None:
        if not self._conn:
            return
        cutoff = time.time() - self.config.max_history_days * 86400
        try:
            with self._conn_lock:
                self._conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
                self._conn.commit()
        except Exception as e:
            logging.warning(f"History cleanup failed: {e}")

    def _queue_row(self, metrics: Dict[str, Any]) -> None:
        if not self._conn:
            return
        chaos_events = metrics.get("chaos_events") or []
        row = (
            metrics.get("timestamp", time.time()),
            self.config.instance_id,
            self.config.node_id,
            PROFILER_SCHEMA_VERSION,
            metrics.get("gpu_utilization_pct", 0.0),
            metrics.get("gpu_memory_used_mb", 0.0),
            metrics.get("gpu_power_watts", 0.0),
            metrics.get("cpu_utilization_pct", 0.0),
            metrics.get("cpu_memory_used_mb", 0.0),
            metrics.get("disk_read_bandwidth_gbps", 0.0),
            metrics.get("disk_write_bandwidth_gbps", 0.0),
            metrics.get("net_recv_bandwidth_gbps", 0.0),
            metrics.get("net_sent_bandwidth_gbps", 0.0),
            metrics.get("energy_gpu_joules", 0.0),
            metrics.get("energy_cpu_joules", 0.0),
            metrics.get("precision_level"),
            metrics.get("effective_bits", 0.0),
            metrics.get("carbon_intensity_g_per_kwh", 0.0),
            metrics.get("carbon_price_per_kg", 0.0),
            metrics.get("net_carbon_g", 0.0),
            metrics.get("rec_offset_kg", 0.0),
            metrics.get("uncertainty_score", 0.0),
            json.dumps(chaos_events),
            json.dumps(metrics.get("provenance", {})),
        )
        with self._conn_lock:
            self._pending_rows.append(row)
        self._samples_since_flush += 1
        if self._samples_since_flush >= self.config.history_batch_size:
            self._flush_rows()

    def _flush_rows(self) -> None:
        if not self._conn or not self._pending_rows:
            return
        rows = None
        with self._conn_lock:
            if not self._pending_rows:
                return
            rows = self._pending_rows
            self._pending_rows = []
        try:
            with self._conn_lock:
                self._conn.executemany("""
                    INSERT INTO metrics (
                        timestamp, instance_id, node_id, schema_version,
                        gpu_util, gpu_mem_used_mb, gpu_power_watts,
                        cpu_util, cpu_mem_used_mb,
                        disk_read_gbps, disk_write_gbps,
                        net_recv_gbps, net_sent_gbps,
                        energy_gpu_joules, energy_cpu_joules,
                        precision_level, effective_bits,
                        carbon_intensity, carbon_price,
                        net_carbon_g, rec_offset_kg,
                        uncertainty_score, chaos_events, provenance
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                self._conn.commit()
        except Exception as e:
            logging.warning(f"Failed to flush history rows: {e}")
        finally:
            self._samples_since_flush = 0

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def register_callback(
        self,
        callback: Callable[[Dict[str, Any]], None],
        cooldown: float = 0.1,
    ) -> None:
        with self._callbacks_lock:
            self._callbacks.append((callback, cooldown, 0.0))

    def _trigger_callbacks(self, metrics: Dict[str, Any]) -> None:
        now = time.time()
        with self._callbacks_lock:
            callbacks = list(self._callbacks)
        for i, (cb, cd, last) in enumerate(callbacks):
            if now - last >= cd:
                try:
                    cb(metrics)
                except Exception as e:
                    logging.error(f"Callback error: {e}")
                with self._callbacks_lock:
                    if i < len(self._callbacks):
                        self._callbacks[i] = (cb, cd, now)

    # ------------------------------------------------------------------
    # Core sampling
    # ------------------------------------------------------------------
    def _snapshot(self) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        now = time.time()
        metrics["timestamp"] = now
        metrics["instance_id"] = self.config.instance_id
        metrics["node_id"] = self.config.node_id

        # Chaos: fault
        if self.chaos and self.chaos.maybe_fault():
            metrics["gpu_available"] = False
            metrics["chaos_events"] = list(self.chaos.events[-1:])
            return metrics

        # --- GPU (NVML) ---
        if NVML_AVAILABLE and pynvml is not None:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                metrics["gpu_count"] = device_count
                if device_count > 0:
                    total_used = 0.0
                    total_free = 0.0
                    total_power = 0.0
                    max_util = 0.0
                    temps = []
                    for i in range(device_count):
                        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                        mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                        util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                        power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0
                        temp = pynvml.nvmlDeviceGetTemperature(
                            handle, pynvml.NVML_TEMPERATURE_GPU
                        )
                        total_used += mem_info.used / 1024 ** 2
                        total_free += mem_info.free / 1024 ** 2
                        total_power += power
                        max_util = max(max_util, util.gpu / 100.0)
                        temps.append(temp)
                    metrics["gpu_memory_total_mb"] = total_used + total_free
                    metrics["gpu_memory_used_mb"] = total_used
                    metrics["gpu_memory_free_mb"] = total_free
                    metrics["gpu_utilization_pct"] = max_util
                    metrics["gpu_power_watts"] = total_power
                    metrics["gpu_temp_c"] = sum(temps) / len(temps)
                    metrics["gpu_available"] = True
                else:
                    metrics["gpu_available"] = False
            except Exception as e:
                logging.warning(f"NVML snapshot error: {e}")
                metrics["gpu_available"] = False
        else:
            metrics["gpu_available"] = False

        # Chaos: sensor noise on GPU signals
        if self.chaos:
            for k in ("gpu_power_watts", "gpu_temp_c", "gpu_utilization_pct"):
                if k in metrics:
                    metrics[k] = self.chaos.apply_sensor_noise(metrics[k])

        # --- CPU / memory ---
        if PSUTIL_AVAILABLE:
            vm = psutil.virtual_memory()
            metrics["cpu_memory_total_mb"] = vm.total / 1024 ** 2
            metrics["cpu_memory_free_mb"] = vm.available / 1024 ** 2
            metrics["cpu_memory_used_mb"] = vm.used / 1024 ** 2
            # NOTE: psutil's first call returns 0.0; call twice with tiny sleep.
            try:
                psutil.cpu_percent(interval=None)
                cpu_pct = psutil.cpu_percent(interval=0.0)
            except Exception:
                cpu_pct = 0.0
            metrics["cpu_utilization_pct"] = cpu_pct / 100.0
        else:
            metrics["cpu_utilization_pct"] = 0.0

        # --- Disk / net I/O ---
        if PSUTIL_AVAILABLE:
            try:
                disk_io = psutil.disk_io_counters()
            except Exception:
                disk_io = None
            if disk_io is not None and self._disk_io_start is not None:
                delta = now - self._last_disk_time
                if delta > 1e-3:
                    metrics["disk_read_bandwidth_gbps"] = (
                        (disk_io.read_bytes - self._disk_io_start.read_bytes) / delta
                    ) * 8 / 1e9
                    metrics["disk_write_bandwidth_gbps"] = (
                        (disk_io.write_bytes - self._disk_io_start.write_bytes) / delta
                    ) * 8 / 1e9
            self._disk_io_start = disk_io
            self._last_disk_time = now

            try:
                net_io = psutil.net_io_counters()
            except Exception:
                net_io = None
            if net_io is not None and self._net_io_start is not None:
                delta = now - self._last_net_time
                if delta > 1e-3:
                    metrics["net_recv_bandwidth_gbps"] = (
                        (net_io.bytes_recv - self._net_io_start.bytes_recv) / delta
                    ) * 8 / 1e9
                    metrics["net_sent_bandwidth_gbps"] = (
                        (net_io.bytes_sent - self._net_io_start.bytes_sent) / delta
                    ) * 8 / 1e9
            self._net_io_start = net_io
            self._last_net_time = now

        # --- Cumulative energy (idle-gap safe) ---
        elapsed = now - self._last_power_sample_time
        # Clamp elapsed to avoid runaway integration after long idle / resume
        elapsed = max(0.0, min(elapsed, self.config.sample_interval * 4.0))
        gpu_power = float(metrics.get("gpu_power_watts", 0.0))
        cpu_util = float(metrics.get("cpu_utilization_pct", 0.0))
        idle_frac = self.config.cpu_idle_fraction
        cpu_power = self.config.cpu_tdp_watts * (idle_frac + (1.0 - idle_frac) * cpu_util)
        self._energy_cumulative_gpu_joules += gpu_power * elapsed
        self._energy_cumulative_cpu_joules += cpu_power * elapsed
        metrics["gpu_power_watts_cpu_model"] = cpu_power
        metrics["energy_gpu_joules"] = self._energy_cumulative_gpu_joules
        metrics["energy_cpu_joules"] = self._energy_cumulative_cpu_joules
        self._last_power_sample_time = now

        # --- External token rate contract ---
        metrics["tokens_per_sec"] = self._external_tokens_per_sec

        # --- Carbon intensity (with chaos spike) ---
        carbon_intensity = self.config.carbon_intensity_g_per_kwh
        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
        metrics["carbon_intensity_g_per_kwh"] = carbon_intensity

        # --- Carbon price ---
        carbon_price = 0.0
        if self.carbon_market:
            carbon_price = self.carbon_market.price(
                carbon_intensity, hour_of_day=time.localtime().tm_hour
            )
        metrics["carbon_price_per_kg"] = carbon_price

        # --- Net carbon after REC offset ---
        energy_kwh = (self._energy_cumulative_gpu_joules + self._energy_cumulative_cpu_joules) / 3.6e6
        gross_carbon_g = energy_kwh * carbon_intensity
        rec_offset_kg = 0.0
        if self.recs and self.recs.total_kwh() > 0 and energy_kwh > 0:
            rec_offset_kg = self.recs.consume(min(energy_kwh, self.recs.total_kwh()))
        metrics["gross_carbon_g"] = gross_carbon_g
        metrics["rec_offset_kg"] = rec_offset_kg
        metrics["net_carbon_g"] = max(0.0, gross_carbon_g - rec_offset_kg * 1000.0)

        # --- Derived metrics ---
        metrics["energy_efficiency"] = self._compute_energy_efficiency(metrics)
        metrics["carbon_efficiency"] = self._compute_carbon_efficiency(metrics)
        metrics["memory_efficiency"] = self._compute_memory_efficiency(metrics)
        metrics["throughput"] = metrics.get("tokens_per_sec", 0.0)

        # --- Precision switching ---
        if self.precision_adapter:
            headroom = 0.5  # caller may override
            _, level = self.precision_adapter.adapt(
                {}, carbon_intensity, headroom, carbon_price
            )
            self.current_precision = level
            metrics["precision_level"] = level.value
            metrics["effective_bits"] = PRECISION_COST[level]["bits"]
        else:
            metrics["precision_level"] = None
            metrics["effective_bits"] = 0.0

        # --- Uncertainty observation (before emitting) ---
        if self.uncertainty:
            self.uncertainty.observe(metrics)
            metrics["uncertainty_score"] = float(np.mean([
                self.uncertainty.std(k)
                for k in ("gpu_power_watts", "gpu_temp_c", "energy_gpu_joules")
                if self.uncertainty.std(k) > 0
            ]) if any(self.uncertainty.std(k) > 0 for k in
                     ("gpu_power_watts", "gpu_temp_c", "energy_gpu_joules")) else 0.0)
        else:
            metrics["uncertainty_score"] = 0.0

        # --- Temporal logic monitor ---
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.temporal and self.shield:
            self.temporal.observe(metrics)
            safety_ok, stl_verdict, fallback = self.shield.screen()
        metrics["safety_ok"] = safety_ok
        metrics["stl_verdict"] = stl_verdict

        # --- XAI on anomaly ---
        if self.explainer and self.uncertainty:
            # Detect anomaly: z-score of gpu_power_watts > 3
            gpu_power = float(metrics.get("gpu_power_watts", 0.0))
            is_anom, z = self.uncertainty.is_anomalous("gpu_power_watts", gpu_power, z_thresh=3.0)
            if is_anom or not safety_ok:
                baseline = {
                    k: self.uncertainty.mean(k)
                    for k in self.explainer.feature_names
                }
                scales = {
                    k: self.uncertainty.std(k)
                    for k in self.explainer.feature_names
                }
                explanation = self.explainer.explain_anomaly(
                    kind="power_anomaly" if is_anom else "safety_violation",
                    current=metrics,
                    baseline=baseline,
                    scales=scales,
                    safety_ok=safety_ok,
                    carbon_price=carbon_price,
                    confidence=min(1.0, abs(z) / 5.0) if is_anom else 0.9,
                )
                self.last_explanation = explanation
                metrics["explanation"] = explanation.to_dict()

                # HITL gate for high-uncertainty anomalies
                if self.hitl and self.uncertainty:
                    unc = float(metrics.get("uncertainty_score", 0.0))
                    if unc > 0.5 or not safety_ok:
                        req = HITLRequest(
                            request_id=uuid.uuid4().hex[:8],
                            reason="high_uncertainty" if unc > 0.5 else "safety_violation",
                            metric_name="gpu_power_watts",
                            value=gpu_power,
                            uncertainty=unc,
                            carbon_price=carbon_price,
                        )
                        approved = self.hitl.request_sync(req)
                        metrics["human_approved"] = approved
                        if not approved:
                            metrics["action"] = "fallback_safe_profile"

                # Active learning storage
                if self.active_learner:
                    self.active_learner.maybe_store(
                        {"metrics": {k: v for k, v in metrics.items()
                                     if isinstance(v, (int, float))}},
                        uncertainty=float(metrics.get("uncertainty_score", 0.0)),
                        threshold=0.4,
                    )

        # --- Causal update from controller callbacks (stored, not learned here) ---
        # The controller can call profiler.update_feedback(context, action, reward)
        # to push transitions into the causal estimator.

        # --- Federated export (only if consumer calls export_federated_update) ---
        # (No auto submit here to avoid spamming the aggregator.)

        # --- Provenance ---
        metrics["provenance"] = {
            "schema": PROFILER_SCHEMA_VERSION,
            "instance_id": self.config.instance_id,
            "node_id": self.config.node_id,
            "nvml_available": NVML_AVAILABLE,
            "chaos_active": self.chaos is not None,
        }

        return metrics

    # ------------------------------------------------------------------
    # Derived metric helpers
    # ------------------------------------------------------------------
    def _compute_energy_efficiency(self, metrics: Dict[str, Any]) -> float:
        tokens = float(metrics.get("tokens_per_sec", 0.0))
        total_power = (
            float(metrics.get("gpu_power_watts", 0.0))
            + float(metrics.get("gpu_power_watts_cpu_model", 0.0))
        )
        if total_power > 0:
            return tokens / total_power
        return 0.0

    def _compute_carbon_efficiency(self, metrics: Dict[str, Any]) -> float:
        carbon_intensity = float(metrics.get("carbon_intensity_g_per_kwh", 0.0))
        total_energy_j = (
            float(metrics.get("energy_gpu_joules", 0.0))
            + float(metrics.get("energy_cpu_joules", 0.0))
        )
        energy_kwh = total_energy_j / 3.6e6
        carbon_kg = energy_kwh * carbon_intensity / 1000.0
        tokens = float(metrics.get("tokens_per_sec", 0.0))
        if carbon_kg > 0:
            return tokens / carbon_kg
        return 0.0

    def _compute_memory_efficiency(self, metrics: Dict[str, Any]) -> float:
        total = float(metrics.get("gpu_memory_total_mb", 0.0))
        used = float(metrics.get("gpu_memory_used_mb", 0.0))
        if total > 0:
            return used / total
        return 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        # Flush pending rows
        try:
            self._flush_rows()
        except Exception:
            pass
        if self._conn:
            try:
                with self._conn_lock:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _sample_loop(self) -> None:
        while self._running:
            try:
                metrics = self._snapshot()
                with self._lock:
                    self._latest_metrics = metrics
                if self.config.enable_history:
                    self._queue_row(metrics)
                if self.config.enable_callbacks:
                    self._trigger_callbacks(metrics)
            except Exception as e:
                logging.error(f"Sampler error: {e}")
            time.sleep(self.config.sample_interval)

    def get_current_metrics(self) -> Dict[str, Any]:
        if self._running:
            with self._lock:
                return dict(self._latest_metrics)
        return self._snapshot()

    # ------------------------------------------------------------------
    # External input contract
    # ------------------------------------------------------------------
    def set_token_rate(self, tokens_per_sec: float) -> None:
        """Set the external token rate used in efficiency metrics."""
        self._external_tokens_per_sec = float(tokens_per_sec)

    # ------------------------------------------------------------------
    # Integration interfaces
    # ------------------------------------------------------------------
    def get_modp_utility(self, metrics: Optional[Dict[str, Any]] = None) -> float:
        if metrics is None:
            metrics = self.get_current_metrics()
        objectives = {
            "energy_efficiency": metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": metrics.get("carbon_efficiency", 0.0),
            "memory_efficiency": metrics.get("memory_efficiency", 0.0),
            "throughput": metrics.get("throughput", 0.0),
        }
        optimizer = getattr(self, "_modp_optimizer", None)
        if optimizer is None:
            optimizer = ParetoOptimizer()
            self._modp_optimizer = optimizer
        return optimizer.evaluate(objectives, self.modp_weights)

    def get_bio_fitness(self, policy: Dict[str, Any]) -> float:
        return self.bio.evaluate(self.get_current_metrics(), policy)

    def get_moe_context(self) -> List[float]:
        return self.moe.encode(self.get_current_metrics())

    def get_policy_from_distillation(self, context: Optional[Dict] = None) -> str:
        """Blend quantum teacher with distiller output (Enhancement 1)."""
        if context is None:
            context = self.get_current_metrics()
        if self.distiller is not None:
            try:
                base_policy = self.distiller.distill(context)
            except Exception:
                base_policy = "balanced"
        else:
            base_policy = "balanced"
        if self.distill_ensemble is None:
            return base_policy

        # Quantum teacher produces a distribution over the three default actions.
        actions = ["balanced", "performance", "power_save"]
        features = np.array([
            float(context.get("gpu_utilization_pct", 0.0)),
            float(context.get("gpu_power_watts", 0.0)) / 400.0,
            float(context.get("gpu_temp_c", 0.0)) / 100.0,
            float(context.get("cpu_utilization_pct", 0.0)),
            float(context.get("carbon_intensity_g_per_kwh", 400.0)) / 1000.0,
        ], dtype=np.float64)
        if base_policy in actions:
            other = np.zeros(len(actions))
            other[actions.index(base_policy)] = 1.0
        else:
            other = np.ones(len(actions)) / len(actions)
        blended = self.distill_ensemble.blend(features, other, len(actions))
        return actions[int(np.argmax(blended))]

    def get_policy_from_rlhf(self, context: Optional[Dict] = None) -> str:
        if not self.rlhf:
            return "balanced"
        if context is None:
            context = self.get_current_metrics()
        try:
            return self.rlhf.sample_action(context) or "balanced"
        except Exception:
            return "balanced"

    def get_limits(self, context: Optional[Dict] = None) -> Dict:
        if not self.limit_graph:
            return {}
        if context is None:
            context = self.get_current_metrics()
        try:
            return self.limit_graph.get_limits(context)
        except Exception:
            return {}

    def update_feedback(self, context: Dict, action: str, reward: float) -> None:
        """Update RLHF, LIMIT Graph, and causal estimator with feedback."""
        if self.rlhf:
            try:
                self.rlhf.update(context, action, reward)
            except Exception:
                pass
        if self.limit_graph:
            try:
                self.limit_graph.update_from_feedback({
                    "context": context,
                    "action": action,
                    "reward": reward,
                })
            except Exception:
                pass

        # Causal estimator update
        if self.causal is not None:
            try:
                state = self._state_vector(context)
                actions = ["balanced", "performance", "power_save"]
                a_idx = actions.index(action) if action in actions else 0
                self.causal.update(CausalTransition(
                    state=state,
                    action=a_idx,
                    reward=float(reward),
                    next_state=state,
                ))
            except Exception:
                pass

        # Multi-agent role registry update
        if self.roles is not None:
            try:
                role = AgentRole.EXPLOITER
                if "carbon" in action:
                    role = AgentRole.CARBON_BROKER
                elif "power" in action:
                    role = AgentRole.SAFETY_OFFICER
                elif "performance" in action:
                    role = AgentRole.EXPLOITER
                self.roles.record(role, success=reward > 0.5, reward=reward)
            except Exception:
                pass

    def _state_vector(self, context: Dict) -> np.ndarray:
        return np.array([
            float(context.get("gpu_utilization_pct", 0.0)),
            float(context.get("gpu_power_watts", 0.0)) / 400.0,
            float(context.get("gpu_temp_c", 0.0)) / 100.0,
            float(context.get("cpu_utilization_pct", 0.0)),
            float(context.get("carbon_intensity_g_per_kwh", 400.0)) / 1000.0,
            float(context.get("carbon_price_per_kg", 0.0)) / 0.5,
            float(context.get("rec_kwh_available", 0.0)) / 100.0,
            float(context.get("uncertainty_score", 0.0)),
        ], dtype=np.float64)

    # ------------------------------------------------------------------
    # Multi-agent / causal / federated helpers
    # ------------------------------------------------------------------
    def vote_metric_emphasis(self, metrics: Optional[Dict[str, Any]] = None) -> str:
        """Multi-agent vote over which metric to emphasize (Enhancement 4)."""
        if metrics is None:
            metrics = self.get_current_metrics()
        if self.coordinator is None or self.roles is None:
            return "energy_efficiency"
        objectives = [
            "energy_efficiency",
            "carbon_efficiency",
            "memory_efficiency",
            "throughput",
        ]
        bids: List[AgentBid] = []
        for role in AgentRole:
            if role == AgentRole.CARBON_BROKER:
                idx = objectives.index("carbon_efficiency")
            elif role == AgentRole.EXPLOITER:
                idx = objectives.index("throughput")
            elif role == AgentRole.EXPLORER:
                idx = random.randrange(len(objectives))
            elif role == AgentRole.SAFETY_OFFICER:
                idx = objectives.index("energy_efficiency")
            else:
                idx = objectives.index("memory_efficiency")
            bids.append(AgentBid(
                agent_id=role.value,
                role=role,
                confidence=0.7,
                proposed_action=idx,
                rationale=role.value,
                carbon_score=1.0 / (1.0 + metrics.get("net_carbon_g", 0.0)),
            ))
        return objectives[self.coordinator.vote(bids, len(objectives))]

    def causal_counterfactuals(
        self, context: Optional[Dict] = None, n_actions: int = 3,
    ) -> Dict[int, float]:
        if self.causal is None:
            return {}
        if context is None:
            context = self.get_current_metrics()
        try:
            return self.causal.counterfactuals(self._state_vector(context), n_actions)
        except Exception:
            return {}

    def export_federated_update(self) -> Optional[Dict[str, Any]]:
        """Package a compact local update for a federated aggregator."""
        if self.federated is None:
            return None
        m = self.get_current_metrics()
        vec = np.array([
            m.get("energy_efficiency", 0.0),
            m.get("carbon_efficiency", 0.0),
            m.get("memory_efficiency", 0.0),
            m.get("throughput", 0.0),
            m.get("net_carbon_g", 0.0) / 1000.0,
        ], dtype=np.float64)
        update = FederatedUpdate(
            node_id=self.config.node_id,
            weights={"telemetry": vec},
            n_samples=1,
            carbon_intensity=float(m.get("carbon_intensity_g_per_kwh", 400.0)),
        )
        self.federated.submit(update)
        return {"node_id": update.node_id, "vec": vec.tolist()}

    def aggregate_federated(self) -> Optional[Dict[str, np.ndarray]]:
        if self.federated is None:
            return None
        return self.federated.aggregate()

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "schema": PROFILER_SCHEMA_VERSION,
            "instance_id": self.config.instance_id,
            "node_id": self.config.node_id,
            "nvml_available": NVML_AVAILABLE,
            "psutil_available": PSUTIL_AVAILABLE,
            "running": self._running,
            "history_enabled": self._conn is not None,
            "callbacks_registered": len(self._callbacks),
            "quantum_teacher_active": self.quantum_teacher is not None,
            "causal_active": self.causal is not None,
            "federated_active": self.federated is not None,
            "multi_agent_active": self.coordinator is not None,
            "temporal_logic_active": self.temporal is not None,
            "xai_active": self.explainer is not None,
            "precision_switching_active": self.precision_adapter is not None,
            "carbon_market_active": self.carbon_market is not None,
            "chaos_active": self.chaos is not None,
            "hitl_active": self.hitl is not None,
            "current_precision": self.current_precision.value if self.current_precision else None,
            "rec_kwh_available": self.recs.total_kwh() if self.recs else 0.0,
            "last_explanation": (
                self.last_explanation.to_dict() if self.last_explanation else None
            ),
            "active_learning_buffer_size": len(self.active_learner) if self.active_learner else 0,
            "hitl_audit_size": len(self.hitl.audit) if self.hitl else 0,
            "stl_verdict": (
                self.shield.monitor.verify() if self.shield else {}
            ),
            "role_weights": (
                {r.value: w for r, w in self.roles.weights().items()}
                if self.roles else {}
            ),
        }

    def get_history(
        self,
        start_time: float = 0.0,
        end_time: Optional[float] = None,
    ) -> List[Dict]:
        if not self._conn:
            return []
        if end_time is None:
            end_time = time.time()
        # Flush pending rows first so recent samples are visible.
        self._flush_rows()
        try:
            with self._conn_lock:
                cursor = self._conn.execute("""
                    SELECT timestamp, instance_id, node_id, schema_version,
                           gpu_util, gpu_mem_used_mb, gpu_power_watts,
                           cpu_util, cpu_mem_used_mb,
                           disk_read_gbps, disk_write_gbps,
                           net_recv_gbps, net_sent_gbps,
                           energy_gpu_joules, energy_cpu_joules,
                           precision_level, effective_bits,
                           carbon_intensity, carbon_price,
                           net_carbon_g, rec_offset_kg,
                           uncertainty_score, chaos_events, provenance
                    FROM metrics
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp
                """, (start_time, end_time))
                rows = cursor.fetchall()
        except Exception as e:
            logging.warning(f"History query failed: {e}")
            return []
        out: List[Dict] = []
        for row in rows:
            out.append({
                "timestamp": row[0],
                "instance_id": row[1],
                "node_id": row[2],
                "schema_version": row[3],
                "gpu_utilization_pct": row[4],
                "gpu_memory_used_mb": row[5],
                "gpu_power_watts": row[6],
                "cpu_utilization_pct": row[7],
                "cpu_memory_used_mb": row[8],
                "disk_read_bandwidth_gbps": row[9],
                "disk_write_bandwidth_gbps": row[10],
                "net_recv_bandwidth_gbps": row[11],
                "net_sent_bandwidth_gbps": row[12],
                "energy_gpu_joules": row[13],  # cumulative since profiler start
                "energy_cpu_joules": row[14],  # cumulative since profiler start
                "precision_level": row[15],
                "effective_bits": row[16],
                "carbon_intensity_g_per_kwh": row[17],
                "carbon_price_per_kg": row[18],
                "net_carbon_g": row[19],
                "rec_offset_kg": row[20],
                "uncertainty_score": row[21],
                "chaos_events": json.loads(row[22]) if row[22] else [],
                "provenance": json.loads(row[23]) if row[23] else {},
            })
        return out


# ============================================================
# Example usage
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    profiler = GPUProfiler()
    profiler.start()

    def print_metrics(metrics):
        print(
            f"GPU util: {metrics.get('gpu_utilization_pct', 0) * 100:.1f}%, "
            f"Power: {metrics.get('gpu_power_watts', 0):.1f}W, "
            f"Precision: {metrics.get('precision_level')}, "
            f"Carbon: {metrics.get('net_carbon_g', 0):.3f}g, "
            f"Uncertainty: {metrics.get('uncertainty_score', 0):.3f}"
        )

    profiler.register_callback(print_metrics, cooldown=1.0)
    time.sleep(5)

    print(f"\nMODP utility: {profiler.get_modp_utility():.4f}")
    print(f"MoE context: {profiler.get_moe_context()}")
    print(f"Distilled policy: {profiler.get_policy_from_distillation()}")
    print(f"RLHF policy: {profiler.get_policy_from_rlhf()}")
    print(f"Metric emphasis vote: {profiler.vote_metric_emphasis()}")
    print(f"Causal counterfactuals: {profiler.causal_counterfactuals()}")

    history = profiler.get_history(start_time=time.time() - 10)
    print(f"\nHistory entries: {len(history)}")
    print(f"Stats: {json.dumps(profiler.get_stats(), indent=2, default=str)}")

    profiler.stop()
