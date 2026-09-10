"""
FlexGen Controller — End-to-end closed-loop policy selection (enhanced v2).

Single-file integration of all ten Green Agent enhancements:
  1. Quantum-Distillation Integration
  2. Causal Reinforcement Learning for Policy Adaptation
  3. Federated Green Learning Across Deployments
  4. Advanced Multi-Agent Coordination with Emergent Role Specialisation
  5. Temporal Logic and Formal Verification for Safety-Critical Policies
  6. Explainable AI (XAI) for Every Decision
  7. Adaptive Precision Switching with Hardware-Aware Policies
  8. External Carbon Markets and Renewable Energy Credits
  9. Resilience Engineering and Chaos Testing as First-Class Citizens
 10. Human-in-the-Loop for Critical Decisions with Active Learning

Everything lives in this file. Each enhancement is a self-contained class
with a clean interface, and FlexGenController wires them into a single
closed loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Original imports (kept as-is)
# ---------------------------------------------------------------------------
from .flexgen_policy import FlexGenPolicy, MockFlexGenExecutor, generate_candidate_policies
from .flexgen_policy_selector import DistillationFlexGenSelector, FlexGenState
from .flexgen_cost_model import FlexGenCostModel
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..pareto_gating import ParetoGating
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger

try:
    from ..gpu_optimization.reward import compute_reward  # type: ignore
except ImportError:
    def compute_reward(metrics: Dict[str, Any], workload: WorkloadDescriptor) -> float:
        latency_score = 1.0 - min(1.0, metrics["latency_ms"] / max(workload.latency_target, 1.0))
        energy_score = 1.0 - min(1.0, metrics["energy_joules"] / 100.0)
        carbon_score = 1.0 - min(1.0, metrics["carbon_g"] / 10.0)
        success_bonus = 1.0 if metrics.get("success", False) else 0.0
        return 0.4 * success_bonus + 0.2 * latency_score + 0.2 * energy_score + 0.2 * carbon_score

try:
    from .bio_policy_search import BioPolicySearch  # type: ignore
except ImportError:
    BioPolicySearch = None

try:
    from ..modp.flexgen_modp_planner import FlexGenMODPPlanner  # type: ignore
except ImportError:
    FlexGenMODPPlanner = None

try:
    from .policy_drift_detector import PolicyDriftDetector  # type: ignore
except ImportError:
    PolicyDriftDetector = None

try:
    from .gpu_profiler import GPUProfiler  # type: ignore
except ImportError:
    GPUProfiler = None


# ===========================================================================
# Enhancement 1 — Quantum-Distillation Integration
# ===========================================================================
class QuantumDistillationTeacher:
    """
    Quantum-inspired teacher that produces soft policy targets.

    A lightweight quantum-circuit-style simulator is used to produce
    interference-weighted probabilities over candidate indices. In a real
    deployment this would delegate to a quantum simulator or QPU; here we
    implement a deterministic state-vector simulation for reproducibility.
    """

    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = n_qubits
        self.rng = np.random.default_rng(seed)
        self._circuit_params = self.rng.normal(size=(n_qubits, 3))

    def _apply_ry(self, state: np.ndarray, theta: float, qubit: int) -> np.ndarray:
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state)
        new = state.copy()
        step = 1 << qubit
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b
                new[i | step] = s * a + c * b
        return new

    def _simulate(self, feature_vec: np.ndarray, n_candidates: int) -> np.ndarray:
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._circuit_params[q, 0] * feature_vec[q % len(feature_vec)])
            state = self._apply_ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        # Reduce to candidate distribution.
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full):
            probs[i % n_candidates] += p
        probs /= max(probs.sum(), 1e-12)
        return probs

    def teacher_probs(
        self,
        state_features: np.ndarray,
        pareto_candidates: Sequence[Dict[str, Any]],
    ) -> np.ndarray:
        n = len(pareto_candidates)
        if n == 0:
            return np.zeros(0)
        raw = self._simulate(np.asarray(state_features, dtype=np.float64), n)
        # Blend with carbon-aware prior so distillation is sustainability-biased.
        carbon_prior = np.array(
            [1.0 / (1.0 + float(c.get("carbon_g", 1.0))) for c in pareto_candidates],
            dtype=np.float64,
        )
        carbon_prior /= max(carbon_prior.sum(), 1e-12)
        blended = 0.6 * raw + 0.4 * carbon_prior
        blended /= max(blended.sum(), 1e-12)
        return blended


# ===========================================================================
# Enhancement 2 — Causal Reinforcement Learning for Policy Adaptation
# ===========================================================================
@dataclass
class CausalTransition:
    state: np.ndarray
    action: int
    reward: float
    next_state: np.ndarray
    context: Dict[str, float] = field(default_factory=dict)


class CausalCounterfactualEstimator:
    """
    Maintains a linear structural causal model over (state, action, reward)
    and estimates counterfactual rewards for actions not taken.
    """

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

    def update(self, t: CausalTransition) -> None:
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x)
        self._b += t.reward * x
        self._n += 1

    def _theta(self) -> np.ndarray:
        return np.linalg.solve(self._A, self._b)

    def predict(self, s: np.ndarray, a: int) -> float:
        if self._n < 2:
            return 0.0
        return float(self._features(s, a) @ self._theta())

    def counterfactuals(self, s: np.ndarray, all_actions: Sequence[int]) -> Dict[int, float]:
        return {a: self.predict(s, a) for a in all_actions}

    def best_counterfactual(self, s: np.ndarray, all_actions: Sequence[int]) -> Tuple[int, float]:
        cf = self.counterfactuals(s, all_actions)
        if not cf:
            return -1, 0.0
        best_a = max(cf, key=cf.get)
        return best_a, cf[best_a]


# ===========================================================================
# Enhancement 3 — Federated Green Learning Across Deployments
# ===========================================================================
@dataclass
class FederatedUpdate:
    node_id: str
    weights: np.ndarray
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    """
    Carbon-weighted FedAvg aggregator. Deployments with lower carbon intensity
    and more samples contribute more to the global model.
    """

    def __init__(self, dp_sigma: float = 1e-3, max_staleness_s: float = 3600.0):
        self.dp_sigma = dp_sigma
        self.max_staleness_s = max_staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[np.ndarray] = None
        self._lock = threading.Lock()

    def submit(self, update: FederatedUpdate) -> None:
        with self._lock:
            self._updates[update.node_id] = update

    def _is_stale(self, u: FederatedUpdate, now: float) -> bool:
        return (now - u.timestamp) > self.max_staleness_s

    def aggregate(self) -> Optional[np.ndarray]:
        with self._lock:
            now = time.time()
            fresh = [u for u in self._updates.values() if not self._is_stale(u, now)]
            if not fresh:
                return self._global
            weights = np.array(
                [
                    u.n_samples * (1.0 / max(u.carbon_intensity, 1.0))
                    for u in fresh
                ],
                dtype=np.float64,
            )
            weights /= max(weights.sum(), 1e-12)
            stacked = np.stack([u.weights for u in fresh], axis=0)
            agg = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                agg = agg + np.random.normal(0.0, self.dp_sigma, size=agg.shape)
            self._global = agg
            return agg

    def global_weights(self) -> Optional[np.ndarray]:
        return self._global


# ===========================================================================
# Enhancement 4 — Advanced Multi-Agent Coordination
# ===========================================================================
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
    """
    Tracks recent contribution of each agent/role to successful outcomes,
    and lets roles emerge based on performance. Specialization is dynamic.
    """

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
    """
    Combines bids from specialized agents via carbon-weighted voting.
    """

    def __init__(self, registry: EmergentRoleRegistry, role_bias: Optional[Dict[AgentRole, float]] = None):
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


# ===========================================================================
# Enhancement 5 — Temporal Logic and Formal Verification
# ===========================================================================
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
    """
    Runtime monitor over a rolling window of the last `horizon` executions.
    Supports G (always), F (eventually), and U (until).
    """

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
            sat = [f.predicate(r) for r in window]
            if f.operator == STLOperator.ALWAYS:
                results[f.name] = all(sat)
            elif f.operator == STLOperator.EVENTUALLY:
                results[f.name] = any(sat)
            elif f.operator == STLOperator.UNTIL:
                # first occurrence of predicate must eventually become true
                results[f.name] = any(sat)
            else:
                results[f.name] = True
        return results


class SafetyShield:
    """
    Blocks actions that violate formal constraints. Falls back to a known-safe
    action from the Pareto set (lowest carbon + latency, or index 0).
    """

    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: List[Dict[str, Any]] = []

    def screen(
        self,
        selected_idx: int,
        pareto_candidates: Sequence[Dict[str, Any]],
    ) -> Tuple[int, bool, Dict[str, bool]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected_idx, True, verdict

        # Fall back to the lowest-carbon feasible candidate.
        safe_idx = min(
            range(len(pareto_candidates)),
            key=lambda i: (
                pareto_candidates[i].get("carbon_g", float("inf")),
                pareto_candidates[i].get("latency_ms", float("inf")),
            ),
        )
        self.violations.append(
            {"time": time.time(), "violated": violated, "replaced_with": safe_idx}
        )
        return safe_idx, False, verdict


# ===========================================================================
# Enhancement 6 — Explainable AI for Every Decision
# ===========================================================================
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
    """
    Produces human-readable rationales with feature attributions and
    counterfactual alternatives.
    """

    def __init__(self, feature_names: Optional[Sequence[str]] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "latency_ms", "energy_joules", "carbon_g", "quality_score"
        ]

    def _attributions(
        self, chosen: Dict[str, Any], pareto: Sequence[Dict[str, Any]]
    ) -> List[Tuple[str, float]]:
        if not pareto:
            return []
        arr = np.array(
            [[float(c.get(f, 0.0)) for f in self.feature_names] for c in pareto],
            dtype=np.float64,
        )
        chosen_vec = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
        mean = arr.mean(axis=0)
        std = arr.std(axis=0) + 1e-9
        z = (chosen_vec - mean) / std
        attributions = list(zip(self.feature_names, z.tolist()))
        attributions.sort(key=lambda kv: abs(kv[1]), reverse=True)
        return attributions

    def explain(
        self,
        chosen_idx: int,
        chosen: Dict[str, Any],
        pareto: Sequence[Dict[str, Any]],
        teacher_probs: Optional[np.ndarray],
        counterfactuals: Dict[int, float],
        safety_ok: bool,
        carbon_price: float,
    ) -> Explanation:
        attrs = self._attributions(chosen, pareto)
        cf_list: List[Dict[str, Any]] = []
        ranked = sorted(counterfactuals.items(), key=lambda kv: kv[1], reverse=True)
        for idx, score in ranked[:3]:
            if idx < 0 or idx >= len(pareto):
                continue
            cf_list.append(
                {
                    "alternative_idx": idx,
                    "expected_reward": float(score),
                    "carbon_g": float(pareto[idx].get("carbon_g", float("nan"))),
                    "latency_ms": float(pareto[idx].get("latency_ms", float("nan"))),
                }
            )
        conf = float(teacher_probs[chosen_idx]) if (
            teacher_probs is not None and 0 <= chosen_idx < len(teacher_probs)
        ) else 0.5
        top_feat_str = ", ".join(f"{name}={val:+.2f}" for name, val in attrs[:3])
        rationale = (
            f"Selected candidate #{chosen_idx} (confidence={conf:.2f}). "
            f"Top normalized feature deviations: {top_feat_str}. "
            f"Carbon price signal={carbon_price:.4f}. "
            f"Safety shield={'passed' if safety_ok else 'OVERRIDE'}. "
            f"Counterfactual alternatives: {len(cf_list)}."
        )
        return Explanation(
            decision_id=str(uuid.uuid4()),
            chosen_idx=chosen_idx,
            top_features=attrs[:5],
            counterfactuals=cf_list,
            rationale=rationale,
            confidence=conf,
            safety_ok=safety_ok,
            carbon_price_signal=carbon_price,
        )


# ===========================================================================
# Enhancement 7 — Adaptive Precision Switching
# ===========================================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"


_PRECISION_COST = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.0, "quality": 1.00},
    PrecisionLevel.BF16: {"speed": 1.6, "energy": 0.7, "quality": 0.995},
    PrecisionLevel.FP16: {"speed": 1.9, "energy": 0.6, "quality": 0.99},
    PrecisionLevel.INT8: {"speed": 2.8, "energy": 0.45, "quality": 0.97},
    PrecisionLevel.INT4: {"speed": 4.0, "energy": 0.35, "quality": 0.90},
}


class PrecisionController:
    """
    Chooses precision level as a function of carbon intensity, latency target,
    and quality floor. Bounded by hardware capability.
    """

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
        # Base: prefer aggressive precision under carbon stress or high price.
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        # Combined aggressiveness in [0,1].
        agg = 0.5 * stress + 0.3 * price_stress + 0.2 * (1.0 - headroom)
        # Climb from low to high aggressiveness, but keep quality floor.
        ordered = [p for p in PrecisionLevel if p in self.supported]
        ordered.sort(key=lambda p: _PRECISION_COST[p]["energy"])
        chosen = ordered[0]
        for p in ordered:
            if _PRECISION_COST[p]["quality"] >= self.quality_floor and agg >= 0.25 * ordered.index(p):
                chosen = p
        return chosen


class HardwareAwarePolicyAdapter:
    """
    Applies a chosen precision level and adjusts executor hints.
    """

    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(
        self,
        policy: FlexGenPolicy,
        carbon_intensity: float,
        latency_headroom_ratio: float,
        carbon_price: float,
    ) -> Tuple[FlexGenPolicy, PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom_ratio, carbon_price)
        if hasattr(policy, "precision"):
            try:
                policy.precision = level.value  # type: ignore[attr-defined]
            except Exception:
                pass
        return policy, level


# ===========================================================================
# Enhancement 8 — Carbon Markets and RECs
# ===========================================================================
class CarbonMarketClient:
    """
    Lightweight in-process carbon price model. In production this would query
    an external market API. Price is a function of grid intensity and time.
    """

    def __init__(self, base_price: float = 0.05, sensitivity: float = 0.0005):
        self.base_price = base_price
        self.sensitivity = sensitivity

    def price(self, carbon_intensity: float, hour_of_day: Optional[int] = None) -> float:
        tod_factor = 1.0
        if hour_of_day is not None:
            # Assume peak hours have higher carbon prices.
            tod_factor = 1.0 + 0.3 * math.sin((hour_of_day / 24.0) * 2 * math.pi)
        return self.base_price + self.sensitivity * carbon_intensity * tod_factor


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"


class RECInventory:
    """
    Tracks Renewable Energy Credits and their carbon offset potential.
    """

    def __init__(self, grid_kg_co2_per_kwh: float = 0.4):
        self.grid_factor = grid_kg_co2_per_kwh
        self._records: List[RECRecord] = []

    def add(self, kwh: float, source: str = "solar") -> None:
        self._records.append(RECRecord(kwh=kwh, issued_at=time.time(), source=source))

    def total_kwh(self) -> float:
        return sum(r.kwh for r in self._records)

    def offset_kg(self, kwh_used: float) -> float:
        available = min(kwh_used, self.total_kwh())
        return available * self.grid_factor

    def consume(self, kwh: float) -> float:
        """Consume RECs and return the offset carbon in kg."""
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


# ===========================================================================
# Enhancement 9 — Resilience Engineering and Chaos Testing
# ===========================================================================
class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Classic circuit breaker with exponential backoff for the executor call.
    """

    def __init__(self, failure_threshold: int = 3, recovery_time_s: float = 10.0):
        self.failure_threshold = failure_threshold
        self.recovery_time_s = recovery_time_s
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._opened_at = 0.0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN and time.time() - self._opened_at >= self.recovery_time_s:
            self._state = CircuitState.HALF_OPEN
        return self._state

    def allow(self) -> bool:
        return self.state != CircuitState.OPEN

    def record(self, success: bool) -> None:
        if success:
            self._failures = 0
            self._state = CircuitState.CLOSED
        else:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.time()


class ChaosEngineer:
    """
    Injects faults for chaos testing. All injections are opt-in and controlled
    by a probability schedule.
    """

    def __init__(
        self,
        executor_fault_prob: float = 0.0,
        latency_injection_ms: float = 0.0,
        carbon_spike_prob: float = 0.0,
        seed: int = 0,
    ):
        self.executor_fault_prob = executor_fault_prob
        self.latency_injection_ms = latency_injection_ms
        self.carbon_spike_prob = carbon_spike_prob
        self.rng = random.Random(seed)
        self.events: List[Dict[str, Any]] = []

    def maybe_fault(self) -> bool:
        if self.rng.random() < self.executor_fault_prob:
            self.events.append({"type": "executor_fault", "t": time.time()})
            return True
        return False

    def maybe_latency(self, metrics: Dict[str, Any]) -> None:
        if self.latency_injection_ms > 0 and self.rng.random() < 0.3:
            metrics["latency_ms"] = metrics.get("latency_ms", 0.0) + self.latency_injection_ms
            self.events.append({"type": "latency_injection", "t": time.time()})

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if self.rng.random() < self.carbon_spike_prob:
            spike = carbon_intensity * 1.5
            self.events.append({"type": "carbon_spike", "t": time.time(), "value": spike})
            return spike
        return carbon_intensity


# ===========================================================================
# Enhancement 10 — Human-in-the-Loop and Active Learning
# ===========================================================================
@dataclass
class HITLRequest:
    request_id: str
    reason: str
    chosen_idx: int
    candidates: List[Dict[str, Any]]
    uncertainty: float
    carbon_price: float


class UncertaintyEstimator:
    """
    Predictive uncertainty from entropy of teacher distribution and reward
    variance. High uncertainty triggers human-in-the-loop or active learning.
    """

    def __init__(self, entropy_threshold: float = 0.75, variance_threshold: float = 0.05):
        self.entropy_threshold = entropy_threshold
        self.variance_threshold = variance_threshold
        self._recent_rewards: deque = deque(maxlen=32)

    def observe(self, reward: float) -> None:
        self._recent_rewards.append(reward)

    def entropy(self, probs: Optional[np.ndarray]) -> float:
        if probs is None or probs.size == 0:
            return 1.0
        p = np.clip(probs, 1e-12, 1.0)
        h = -np.sum(p * np.log(p)) / math.log(len(p))
        return float(h)

    def reward_variance(self) -> float:
        if len(self._recent_rewards) < 3:
            return 0.0
        return float(np.var(np.asarray(self._recent_rewards)))

    def is_uncertain(self, probs: Optional[np.ndarray]) -> Tuple[bool, float]:
        h = self.entropy(probs)
        v = self.reward_variance()
        score = 0.6 * h + 0.4 * min(1.0, v / max(self.variance_threshold, 1e-9))
        return (h > self.entropy_threshold or v > self.variance_threshold), score


class HumanInTheLoopGate:
    """
    Async approval gate. If no human is configured, requests are auto-approved
    after a configurable timeout to avoid blocking the control loop.
    """

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

    async def request_approval(self, req: HITLRequest) -> bool:
        if self.approver is None:
            self.audit.append({"request_id": req.request_id, "approved": True, "reason": "no_approver"})
            return True
        loop = asyncio.get_running_loop()
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req), timeout=self.timeout_s
            )
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
        self.audit.append({"request_id": req.request_id, "approved": approved, "reason": req.reason})
        return bool(approved)


class ActiveLearningSampler:
    """
    Prioritizes uncertain transitions for later human/expert review and can
    feed them into a replay buffer for retraining.
    """

    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []

    def maybe_store(self, record: Dict[str, Any], uncertainty: float, threshold: float = 0.5) -> bool:
        if uncertainty < threshold:
            return False
        if len(self._buffer) >= self.capacity:
            self._buffer.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k: int = 8) -> List[Dict[str, Any]]:
        if not self._buffer:
            return []
        k = min(k, len(self._buffer))
        return random.sample(self._buffer, k)


# ===========================================================================
# Main Controller
# ===========================================================================
class FlexGenController:
    def __init__(
        self,
        node: NodeDescriptor,
        workload: WorkloadDescriptor,
        carbon_intensity: float,
        message_queue: Optional[AsyncMessageQueue] = None,
        use_real_executor: bool = False,
        executor: Optional[Any] = None,
        use_cost_model_prefilter: bool = False,
        cost_model: Optional[FlexGenCostModel] = None,
        use_bio_search: bool = False,
        bio_search_config: Optional[Dict] = None,
        modp_planner: Optional[FlexGenMODPPlanner] = None,
        drift_detector: Optional[PolicyDriftDetector] = None,
        gpu_profiler: Optional[GPUProfiler] = None,
        selector_persistence_path: Optional[str] = None,
        # --- Enhancement flags ---
        enable_quantum_teacher: bool = True,
        enable_causal_rl: bool = True,
        enable_federated: bool = True,
        enable_multi_agent: bool = True,
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_precision_switching: bool = True,
        enable_carbon_markets: bool = True,
        enable_resilience: bool = True,
        enable_hitl: bool = True,
        # --- Tunables ---
        state_dim: int = 12,
        n_actions: int = 20,
        quality_floor: float = 0.95,
        hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
        carbon_market_base_price: float = 0.05,
    ):
        # Core
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity
        self.message_queue = message_queue
        self.use_real_executor = use_real_executor
        self.executor = executor if executor else MockFlexGenExecutor(carbon_intensity_g_per_kwh=carbon_intensity)
        self.use_cost_model_prefilter = use_cost_model_prefilter
        self.cost_model = cost_model or FlexGenCostModel(carbon_intensity_g_per_kwh=carbon_intensity)
        self.use_bio_search = use_bio_search
        self.bio_search_config = bio_search_config or {}
        self.modp_planner = modp_planner
        self.drift_detector = drift_detector
        self.gpu_profiler = gpu_profiler

        if hasattr(self.executor, "carbon_intensity"):
            self.executor.carbon_intensity = node.metadata.get("region_carbon_intensity", carbon_intensity)

        self.selector = DistillationFlexGenSelector(
            n_candidates=20, persistence_path=selector_persistence_path
        )
        self.pareto = ParetoGating(
            objectives=[
                {"key": "latency_ms", "direction": "min"},
                {"key": "energy_joules", "direction": "min"},
                {"key": "carbon_g", "direction": "min"},
            ]
        )

        # ---------------- Enhancement modules ----------------
        self.quantum_teacher = QuantumDistillationTeacher() if enable_quantum_teacher else None
        self.causal_estimator = CausalCounterfactualEstimator(state_dim, n_actions) if enable_causal_rl else None
        self.federated = FederatedAggregator() if enable_federated else None

        self.role_registry = EmergentRoleRegistry() if enable_multi_agent else None
        self.multi_agent = MultiAgentCoordinator(self.role_registry) if enable_multi_agent and self.role_registry else None

        self.temporal_monitor = TemporalLogicMonitor(horizon=10) if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula(
                STLFormula(
                    name="latency_ok",
                    predicate=lambda r: r.get("latency_ms", 0.0) <= self.workload.latency_target * 1.5,
                    operator=STLOperator.ALWAYS,
                    horizon=10,
                )
            )
            self.temporal_monitor.add_formula(
                STLFormula(
                    name="eventually_success",
                    predicate=lambda r: r.get("success", False),
                    operator=STLOperator.EVENTUALLY,
                    horizon=5,
                )
            )
        self.safety_shield = SafetyShield(self.temporal_monitor) if self.temporal_monitor else None

        self.explainer = DecisionExplainer() if enable_xai else None
        self.precision_controller = PrecisionController(quality_floor=quality_floor) if enable_precision_switching else None
        self.precision_adapter = HardwareAwarePolicyAdapter(self.precision_controller) if self.precision_controller else None

        self.carbon_market = CarbonMarketClient(base_price=carbon_market_base_price) if enable_carbon_markets else None
        self.rec_inventory = RECInventory() if enable_carbon_markets else None

        self.circuit_breaker = CircuitBreaker() if enable_resilience else None
        self.chaos = ChaosEngineer() if enable_resilience else None

        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = HumanInTheLoopGate(approver=hitl_approver) if enable_hitl else None
        self.active_learner = ActiveLearningSampler() if enable_hitl else None

        # State cache
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self.last_candidates: Optional[List[Dict[str, Any]]] = None
        self.last_explanation: Optional[Explanation] = None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def _get_node_carbon_intensity(self) -> float:
        base = self.node.metadata.get("region_carbon_intensity", self.carbon_intensity)
        if self.chaos:
            base = self.chaos.maybe_carbon_spike(base)
        return base

    def _state_features(self, state: "FlexGenState") -> np.ndarray:
        """Compact numeric vector for causal/quantum modules."""
        return np.array(
            [
                float(getattr(state, "tokens", 0.0)),
                float(getattr(state, "latency_target", 0.0)),
                float(getattr(state, "gpu_memory_gb", 0.0)),
                float(getattr(state, "cpu_memory_gb", 0.0)),
                float(getattr(state, "disk_bandwidth_gbps", 0.0)),
                float(getattr(state, "carbon_intensity", 0.0)),
                float(getattr(state, "recent_success_rate", 0.0)),
                float(getattr(state, "avg_reward", 0.0)),
                0.0, 0.0, 0.0, 0.0,
            ],
            dtype=np.float64,
        )

    # ------------------------------------------------------------------
    # Candidate generation
    # ------------------------------------------------------------------
    async def _generate_candidates(self) -> List[FlexGenPolicy]:
        if self.use_bio_search and BioPolicySearch is not None:
            bio = BioPolicySearch(
                node=self.node,
                workload=self.workload,
                cost_model=self.cost_model,
                **self.bio_search_config,
            )
            loop = asyncio.get_running_loop()
            candidates = await loop.run_in_executor(None, bio.run)
            logger.info(f"Bio search produced {len(candidates)} candidates.")
            return candidates

        candidates = generate_candidate_policies(20)
        if self.use_cost_model_prefilter and self.cost_model:
            estimates = []
            for policy in candidates:
                est = self.cost_model.estimate(policy, self.node, self.workload)
                if est.peak_gpu_memory_gb <= self.node.metadata.get("gpu_memory_gb", 16):
                    estimates.append((policy, est))
            # Normalize before summing (fixes unit-mismatch bug in original).
            if estimates:
                lat = np.array([e.total_latency_ms for _, e in estimates])
                ene = np.array([e.total_energy_joules for _, e in estimates])
                lat_n = (lat - lat.min()) / (lat.ptp() + 1e-9)
                ene_n = (ene - ene.min()) / (ene.ptp() + 1e-9)
                scores = 0.5 * lat_n + 0.5 * ene_n
                order = np.argsort(scores)
                candidates = [estimates[i][0] for i in order[:10]]
        return candidates

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    async def _execute_policy(self, policy: FlexGenPolicy) -> Dict[str, Any]:
        # Circuit breaker check.
        if self.circuit_breaker and not self.circuit_breaker.allow():
            logger.warning("Circuit breaker OPEN — returning failure metrics.")
            return {
                "success": False,
                "latency_ms": float("inf"),
                "energy_joules": float("inf"),
                "carbon_g": float("inf"),
                "policy": policy.to_dict(),
                "error": "circuit_open",
            }

        # Chaos injection.
        if self.chaos and self.chaos.maybe_fault():
            if self.circuit_breaker:
                self.circuit_breaker.record(False)
            return {
                "success": False,
                "latency_ms": float("inf"),
                "energy_joules": float("inf"),
                "carbon_g": float("inf"),
                "policy": policy.to_dict(),
                "error": "chaos_fault",
            }

        try:
            if self.use_real_executor and callable(self.executor) and not isinstance(self.executor, MockFlexGenExecutor):
                gpu_before = self.gpu_profiler.get_gpu_metrics() if self.gpu_profiler else None
                result = self.executor(policy, self.node, self.workload)
                metrics = await result if asyncio.iscoroutine(result) else result
                if self.gpu_profiler:
                    gpu_after = self.gpu_profiler.get_gpu_metrics()
                    if "energy_joules" not in metrics and gpu_before and gpu_after:
                        avg_power = (gpu_before.get("gpu_power_watts", 65) + gpu_after.get("gpu_power_watts", 65)) / 2
                        latency_s = metrics.get("latency_ms", 0) / 1000.0
                        metrics["energy_joules"] = avg_power * latency_s
                        metrics["carbon_g"] = (metrics["energy_joules"] / 3.6e6) * self._get_node_carbon_intensity()
            else:
                metrics = self.executor.execute(policy, self.node, self.workload)

            if self.chaos:
                self.chaos.maybe_latency(metrics)

            if self.circuit_breaker:
                self.circuit_breaker.record(bool(metrics.get("success", False)))
            return metrics

        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Executor failure: {exc}")
            if self.circuit_breaker:
                self.circuit_breaker.record(False)
            return {
                "success": False,
                "latency_ms": float("inf"),
                "energy_joules": float("inf"),
                "carbon_g": float("inf"),
                "policy": policy.to_dict(),
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # Step
    # ------------------------------------------------------------------
    async def step(self) -> Dict:
        step_start = time.time()
        carbon_intensity = self._get_node_carbon_intensity()

        # 0. MODP temporal decision — now recorded, not short-circuited away.
        modp_info: Dict[str, Any] = {}
        if self.modp_planner:
            modp_action, delay, target_node = await self.modp_planner.plan(
                self.workload, self.node, None, queue_length=0, current_carbon=carbon_intensity
            )
            modp_info = {"action": modp_action, "delay": delay, "target_node": target_node}
            if modp_action == "defer":
                logger.info(f"MODP suggests deferring for {delay} hours.")
                await self._publish_feedback(
                    selected_policy=None,
                    chosen_idx=-1,
                    pareto=[],
                    teacher_probs=None,
                    explanation=None,
                    reward=0.0,
                    metrics={"latency_ms": 0.0, "energy_joules": 0.0, "carbon_g": 0.0, "success": True},
                    drift_detected=False,
                    extra={"modp": modp_info, "reason": "modp_deferral"},
                )
                return {"action": "defer", "delay_hours": delay, "reason": "modp_deferral"}
            elif modp_action == "move_node" and target_node:
                logger.info(f"MODP suggests moving to node {target_node} (single-node mode).")

        # 1. Candidates
        candidates = await self._generate_candidates()

        # 2. Carbon market / REC price signal
        carbon_price = 0.0
        rec_offset_kg = 0.0
        if self.carbon_market:
            carbon_price = self.carbon_market.price(carbon_intensity)

        # 3. Adaptive precision
        precision_level: Optional[PrecisionLevel] = None
        if self.precision_adapter:
            # Rough headroom from latency target vs first-candidate estimate.
            headroom = 0.5
            adapted: List[FlexGenPolicy] = []
            for p in candidates:
                p2, lvl = self.precision_adapter.adapt(p, carbon_intensity, headroom, carbon_price)
                adapted.append(p2)
                precision_level = lvl
            candidates = adapted

        # 4. Evaluate candidates
        metrics_list: List[Dict[str, Any]] = []
        for idx, policy in enumerate(candidates):
            metrics = await self._execute_policy(policy)
            metrics["policy_idx"] = idx
            metrics["policy"] = policy.to_dict()
            if precision_level:
                metrics["precision"] = precision_level.value
            metrics_list.append(metrics)

        # 5. Pareto filter
        feasible = [m for m in metrics_list if m.get("success", False)]
        if not feasible:
            logger.warning("No feasible policies found, falling back to all.")
            feasible = metrics_list
        pareto_candidates = self.pareto.filter(feasible) or feasible

        # 6. State
        state = FlexGenState(
            tokens=self.workload.tokens,
            latency_target=self.workload.latency_target,
            gpu_memory_gb=self.node.metadata.get("gpu_memory_gb", 16.0),
            cpu_memory_gb=self.node.metadata.get("cpu_memory_gb", 64.0),
            disk_bandwidth_gbps=self.node.metadata.get("disk_bandwidth_gbps", 2.0),
            carbon_intensity=carbon_intensity,
            recent_success_rate=0.8,
            avg_reward=0.6,
            policy_idx=0,
        )
        state_features = self._state_features(state)

        # 7. Teacher signal — quantum-enhanced or selector's own.
        action_idx, state_vec, teacher_probs = await self.selector.select_policy(
            pareto_candidates, state, exploration=True
        )
        if self.quantum_teacher:
            q_probs = self.quantum_teacher.teacher_probs(state_features, pareto_candidates)
            if q_probs.size == (teacher_probs.size if teacher_probs is not None else 0):
                # Blend: quantum teacher influences distillation target.
                teacher_probs = 0.5 * q_probs + 0.5 * np.asarray(teacher_probs)

        # 8. Multi-agent coordination vote (nudges action_idx).
        if self.multi_agent and self.role_registry and pareto_candidates:
            bids: List[AgentBid] = []
            base_conf = (
                float(teacher_probs[action_idx]) if teacher_probs is not None else 0.5
            )
            for role in AgentRole:
                bias_idx = action_idx
                if role == AgentRole.CARBON_BROKER:
                    bias_idx = int(np.argmin([c.get("carbon_g", 1e9) for c in pareto_candidates]))
                elif role == AgentRole.EXPLOITER:
                    bias_idx = int(np.argmin([c.get("latency_ms", 1e9) for c in pareto_candidates]))
                elif role == AgentRole.EXPLORER:
                    bias_idx = random.randrange(len(pareto_candidates))
                bids.append(
                    AgentBid(
                        agent_id=f"{role.value}-agent",
                        role=role,
                        confidence=base_conf,
                        proposed_action=bias_idx,
                        rationale=role.value,
                        carbon_score=1.0 / (1.0 + float(pareto_candidates[bias_idx].get("carbon_g", 1.0))),
                    )
                )
            action_idx = self.multi_agent.vote(bids, len(pareto_candidates))

        # 9. Causal counterfactuals
        counterfactuals: Dict[int, float] = {}
        if self.causal_estimator:
            counterfactuals = self.causal_estimator.counterfactuals(
                state_features, list(range(len(pareto_candidates)))
            )

        # 10. Safety shield (temporal logic).
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.safety_shield:
            action_idx, safety_ok, stl_verdict = self.safety_shield.screen(action_idx, pareto_candidates)

        # 11. HITL + active learning
        uncertainty_score = 0.0
        human_approved = True
        if self.hitl and self.uncertainty:
            is_unc, uncertainty_score = self.uncertainty.is_uncertain(teacher_probs)
            if is_unc or not safety_ok:
                req = HITLRequest(
                    request_id=str(uuid.uuid4()),
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=action_idx,
                    candidates=list(pareto_candidates),
                    uncertainty=uncertainty_score,
                    carbon_price=carbon_price,
                )
                human_approved = await self.hitl.request_approval(req)
                if not human_approved and pareto_candidates:
                    # Fall back to lowest-carbon candidate.
                    action_idx = int(
                        np.argmin([c.get("carbon_g", 1e9) for c in pareto_candidates])
                    )

        chosen_policy_metrics = pareto_candidates[action_idx]
        chosen_policy = FlexGenPolicy(**chosen_policy_metrics["policy"])
        metrics = chosen_policy_metrics

        # 12. REC offset
        if self.rec_inventory and metrics.get("energy_joules", 0.0) > 0:
            kwh = metrics["energy_joules"] / 3.6e6
            rec_offset_kg = self.rec_inventory.consume(kwh)
            # Net carbon after REC offset
            metrics["carbon_g"] = max(0.0, float(metrics.get("carbon_g", 0.0)) - rec_offset_kg)

        # 13. Reward — carbon-market-aware.
        base_reward = compute_reward(metrics, self.workload)
        carbon_penalty = carbon_price * float(metrics.get("carbon_g", 0.0)) / 1000.0
        reward = base_reward - carbon_penalty

        # 14. Update selector, causal model, federated aggregator.
        next_state_vec = state_vec
        await self.selector.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)

        if self.causal_estimator:
            self.causal_estimator.update(
                CausalTransition(
                    state=state_vec,
                    action=action_idx,
                    reward=reward,
                    next_state=next_state_vec,
                    context={"carbon_intensity": carbon_intensity, "carbon_price": carbon_price},
                )
            )

        if self.federated:
            w = np.asarray(state_vec, dtype=np.float64).copy()
            self.federated.submit(
                FederatedUpdate(
                    node_id=getattr(self.node, "id", "node"),
                    weights=w,
                    n_samples=1,
                    carbon_intensity=carbon_intensity,
                )
            )
            self.federated.aggregate()

        if self.role_registry:
            self.role_registry.record(
                self.role_registry.dominant_role(),
                success=bool(metrics.get("success", False)),
                reward=reward,
            )

        if self.uncertainty:
            self.uncertainty.observe(reward)
        if self.active_learner:
            self.active_learner.maybe_store(
                {
                    "state": state_vec.tolist(),
                    "action": action_idx,
                    "reward": reward,
                    "carbon_intensity": carbon_intensity,
                },
                uncertainty=uncertainty_score,
                threshold=0.4,
            )

        # 15. Temporal monitor observation.
        if self.temporal_monitor:
            self.temporal_monitor.observe(
                {
                    "latency_ms": metrics.get("latency_ms", 0.0),
                    "carbon_g": metrics.get("carbon_g", 0.0),
                    "success": metrics.get("success", False),
                    "t": step_start,
                }
            )

        # 16. Drift detection
        drift_detected = False
        if self.drift_detector:
            self.drift_detector.add_policy(chosen_policy.to_dict(), reward=reward)
            if self.drift_detector.detect_drift():
                logger.warning("Policy drift detected; consider rollback.")
                drift_detected = True

        # 17. XAI explanation
        explanation: Optional[Explanation] = None
        if self.explainer:
            explanation = self.explainer.explain(
                chosen_idx=action_idx,
                chosen=metrics,
                pareto=pareto_candidates,
                teacher_probs=teacher_probs,
                counterfactuals=counterfactuals,
                safety_ok=safety_ok and human_approved,
                carbon_price=carbon_price,
            )
            logger.info(f"XAI: {explanation.rationale}")
        self.last_explanation = explanation

        # 18. Publish feedback
        await self._publish_feedback(
            selected_policy=chosen_policy,
            chosen_idx=action_idx,
            pareto=pareto_candidates,
            teacher_probs=teacher_probs,
            explanation=explanation,
            reward=reward,
            metrics=metrics,
            drift_detected=drift_detected,
            extra={
                "modp": modp_info,
                "precision": precision_level.value if precision_level else None,
                "carbon_price": carbon_price,
                "rec_offset_kg": rec_offset_kg,
                "safety_ok": safety_ok,
                "stl_verdict": stl_verdict,
                "human_approved": human_approved,
                "uncertainty": uncertainty_score,
                "counterfactuals": counterfactuals,
                "federated_round_size": len(getattr(self.federated, "_updates", {}) or {}) if self.federated else 0,
            },
        )

        # Cache
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs
        self.last_candidates = pareto_candidates

        return {
            "chosen_policy": chosen_policy.to_dict(),
            "metrics": metrics,
            "reward": reward,
            "pareto_count": len(pareto_candidates),
            "drift_detected": drift_detected,
            "precision": precision_level.value if precision_level else None,
            "carbon_price": carbon_price,
            "rec_offset_kg": rec_offset_kg,
            "safety_ok": safety_ok,
            "stl_verdict": stl_verdict,
            "human_approved": human_approved,
            "uncertainty": uncertainty_score,
            "explanation": explanation.to_dict() if explanation else None,
            "counterfactuals": counterfactuals,
        }

    # ------------------------------------------------------------------
    # Feedback publisher
    # ------------------------------------------------------------------
    async def _publish_feedback(
        self,
        selected_policy: Optional[FlexGenPolicy],
        chosen_idx: int,
        pareto: Sequence[Dict[str, Any]],
        teacher_probs: Optional[np.ndarray],
        explanation: Optional[Explanation],
        reward: float,
        metrics: Dict[str, Any],
        drift_detected: bool,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.message_queue:
            return
        ctx: Dict[str, Any] = {
            "node_id": self.node.id,
            "carbon_intensity": self._get_node_carbon_intensity(),
            "pareto_size": len(pareto),
            "executor_type": "real" if self.use_real_executor else "mock",
            "drift_detected": drift_detected,
            "teacher_probs": teacher_probs.tolist() if teacher_probs is not None else [],
        }
        if extra:
            ctx.update({k: v for k, v in extra.items() if k != "counterfactuals"})
            ctx["counterfactual_keys"] = list((extra.get("counterfactuals") or {}).keys())
        if explanation:
            ctx["xai_rationale"] = explanation.rationale
            ctx["xai_confidence"] = explanation.confidence

        event = FeedbackEvent(
            source="flexgen_controller",
            feedback_type="routing",
            task_id=getattr(self.workload, "task_id", "unknown") or "unknown",
            context=ctx,
            action={
                "selected_action": selected_policy.to_dict() if selected_policy else None,
                "selected_rank": chosen_idx,
                "confidence_score": (
                    float(teacher_probs[chosen_idx])
                    if teacher_probs is not None and 0 <= chosen_idx < len(teacher_probs)
                    else 0.5
                ),
            },
            performance={
                "quality_score": metrics.get("quality_score", 0.9),
                "latency_ms": metrics.get("latency_ms", 0.0),
                "energy_joules": metrics.get("energy_joules", 0.0),
                "carbon_g": metrics.get("carbon_g", 0.0),
                "helium_cost": 0,
                "duration_ms": 0,
            },
            adaptive_cost_value=reward,
            tags=[
                "flexgen",
                "policy_selection",
                "carbon_aware",
                "executor_" + ("real" if self.use_real_executor else "mock"),
                "safety_" + ("ok" if (extra or {}).get("safety_ok", True) else "override"),
                "hitl_" + ("approved" if (extra or {}).get("human_approved", True) else "rejected"),
            ],
        )
        await self.message_queue.publish("policy_outcomes", event.to_json())

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def _reconstruct_policy(self, metrics: Dict) -> FlexGenPolicy:
        return FlexGenPolicy(**metrics["policy"])

    def _compute_reward(self, metrics: Dict, workload: WorkloadDescriptor) -> float:
        return compute_reward(metrics, workload)

    def export_federated_weights(self) -> Optional[np.ndarray]:
        return self.federated.global_weights() if self.federated else None

    def recent_explanations(self, k: int = 8) -> List[Dict[str, Any]]:
        if not self.explainer or not self.last_explanation:
            return []
        return [self.last_explanation.to_dict()]

    def active_learning_batch(self, k: int = 8) -> List[Dict[str, Any]]:
        return self.active_learner.sample(k) if self.active_learner else []
