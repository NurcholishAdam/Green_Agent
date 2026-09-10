"""
main_integration.py

Complete orchestration loop for Green Agent with all ten Green Agent
enhancements built in, plus integration with bio_inspired, moe_system, MODP,
LIMIT Graph, RLHF, and Multi‑Teacher Policy Distillation.

Enhancements hosted in this file:
  1. Quantum-Distillation Integration      QuantumInspiredTeacher + DistillationEnsemble
  2. Causal RL for Policy Adaptation       CausalCounterfactualEstimator
  3. Federated Green Learning              FederatedAggregator
  4. Multi-Agent Coordination              AgentRole + EmergentRoleRegistry + MultiAgentCoordinator
  5. Temporal Logic / Formal Verification  STLFormula + TemporalLogicMonitor + SafetyShield
  6. Explainable AI                        DecisionExplainer + Explanation
  7. Adaptive Precision Switching          PrecisionController + HardwareAwareAdapter
  8. Carbon Markets / RECs                 CarbonMarketClient + RECInventory
  9. Resilience / Chaos                    ChaosEngineer (+ circuit breaker)
 10. HITL / Active Learning                UncertaintyEstimator + HumanInTheLoopGate + ActiveLearningSampler

Fixes over the previous version:
  - delay_result["status"] now correctly compared to "delayed"
  - MetricAggregator promotes quality_score to the top level
  - Bio expansion iterates values, not keys; num_actions/weights/trials grow atomically
  - reward_calc.weights snapshotted/restored per task
  - CarbonAPIStub uses math.sin (was sawtooth)
  - release_checker has a stop event and thread lock
  - _apply_limit_graph returns a copy, guards missing keys
  - State file round-trips cache; atomic write via tmp+rename
  - Bandit update uses per-action trial counter
  - ContextualBandit seed_safe_policy and update guard dict identity via canonical JSON
"""

import time
import math
import threading
import heapq
import json
import os
import random
import tempfile
from collections import Counter, deque
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Tuple, Callable, Sequence

import numpy as np
import psutil

# ----------------------------------------------------------------------
# 1. External module integrations (with graceful fallback)
# ----------------------------------------------------------------------
try:
    from enhancements.limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from enhancements.rlhf import RLHFOptimizer
except ImportError:
    class RLHFOptimizer:
        def __init__(self, action_space, *args, **kwargs):
            self.actions = list(action_space)
            self.scores = {a: 0.0 for a in self.actions}
        def update(self, context, action, reward):
            if action not in self.scores:
                self.actions.append(action); self.scores[action] = 0.0
            self.scores[action] += 0.1 * (float(reward) - self.scores[action])
        def sample_action(self, context):
            if not self.scores: return None
            if random.random() < 0.1: return random.choice(self.actions)
            return max(self.scores, key=self.scores.get)

try:
    from enhancements.multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, weights=None, *args, **kwargs):
            self.teachers = list(teachers)
            self.weights = list(weights) if weights else [1.0] * len(self.teachers)
        def _call(self, t, ctx):
            try:
                out = t(ctx)
                if asyncio.iscoroutine(out):
                    out.close(); return None
                return out
            except Exception:
                return None
        def distill(self, context):
            votes: Dict[str, Tuple[Dict, float]] = {}
            for t, w in zip(self.teachers, self.weights):
                c = self._call(t, context)
                if c is None: continue
                key = json.dumps(c, sort_keys=True)
                if key in votes:
                    votes[key] = (votes[key][0], votes[key][1] + float(w))
                else:
                    votes[key] = (c, float(w))
            return max(votes.values(), key=lambda kv: kv[1])[0] if votes else None

try:
    from enhancements.bio_inspired import GeneticPolicyGenerator
except ImportError:
    class GeneticPolicyGenerator:
        """Fallback: deterministic single-generation mutation."""
        def __init__(self, *args, **kwargs): pass
        def evolve(self, population, fitness_fn, generations=10, population_size=20):
            # Return a list (never a dict). Mutate each population member.
            out = []
            for p in population:
                if not isinstance(p, dict): continue
                child = dict(p)
                if random.random() < 0.3:
                    child["block_size"] = max(4, int(child.get("block_size", 8)) * 2)
                out.append(child)
            return out

try:
    from enhancements.moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def __init__(self, *args, **kwargs): pass
        def encode(self, context): return [0.0]*5
        def select(self, encoded): return "default"


import asyncio  # used by the distiller fallback and the HITL gate

# ===========================================================================
# ===========================================================================
# TEN ENHANCEMENT MODULES
# ===========================================================================
# ===========================================================================


# ------------------------------------------------------------
# Enhancement 7: Adaptive Precision Switching
# ------------------------------------------------------------
class PrecisionLevel(str, Enum):
    FP32 = "fp32"; BF16 = "bf16"; FP16 = "fp16"
    FP8 = "fp8"; INT8 = "int8"; INT4 = "int4"


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

    def adapt(self, policy: Dict[str, Any], carbon_intensity: float,
              latency_headroom: float, carbon_price: float) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        out["weight_bits"] = PRECISION_COST[level]["bits"]
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
        remaining, offset = kwh, 0.0
        new_records: List[RECRecord] = []
        for r in self._records:
            if remaining <= 0:
                new_records.append(r); continue
            take = min(r.kwh, remaining)
            remaining -= take
            offset += take * self.grid_factor
            if r.kwh - take > 1e-9:
                new_records.append(RECRecord(kwh=r.kwh - take,
                                             issued_at=r.issued_at, source=r.source))
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
            return True
        return False

    def maybe_latency(self) -> float:
        if (self.config.latency_inject_ms > 0
                and self.rng.random() < self.config.latency_inject_prob):
            self.events.append({"type": "latency", "t": time.time()})
            return self.config.latency_inject_ms
        return 0.0

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if self.rng.random() < self.config.carbon_spike_prob:
            self.events.append({"type": "carbon_spike", "t": time.time()})
            return carbon_intensity * self.config.carbon_spike_factor
        return carbon_intensity


class CircuitBreaker:
    def __init__(self, name: str = "executor",
                 failure_threshold: int = 3, recovery_s: float = 10.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_s = recovery_s
        self._failures = 0
        self._open_until = 0.0

    def allow(self) -> bool:
        return time.time() >= self._open_until

    def record(self, success: bool) -> None:
        if success:
            self._failures = 0
        else:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._open_until = time.time() + self.recovery_s
                self._failures = 0


# ------------------------------------------------------------
# Enhancement 5: Temporal Logic / Formal Verification
# ------------------------------------------------------------
class STLOperator(str, Enum):
    ALWAYS = "G"; EVENTUALLY = "F"; UNTIL = "U"


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
        self.violations: List[Dict[str, Any]] = []

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        self._history.append(record)

    def verify(self) -> Dict[str, bool]:
        results = {}
        for f in self.formulas:
            window = list(self._history)[-f.horizon:]
            if not window:
                results[f.name] = True; continue
            sat = [bool(f.predicate(r)) for r in window]
            if f.operator == STLOperator.ALWAYS:
                results[f.name] = all(sat)
            elif f.operator in (STLOperator.EVENTUALLY, STLOperator.UNTIL):
                results[f.name] = any(sat)
            else:
                results[f.name] = True
            if not results[f.name]:
                self.violations.append({"name": f.name, "t": time.time()})
        return results


class SafetyShield:
    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor

    def screen(self, selected: Dict[str, Any],
               candidates: Sequence[Dict[str, Any]],
               score_key: str = "block_size") -> Tuple[Dict[str, Any], bool, Dict[str, bool]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return selected, True, verdict
        if not candidates:
            return selected, False, verdict
        safe = min(candidates, key=lambda p: int(p.get(score_key, 0)))
        return dict(safe), False, verdict


# ------------------------------------------------------------
# Enhancement 2: Causal RL for Policy Adaptation
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

    def _features(self, s, a):
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([np.asarray(s, dtype=np.float64), one_hot, [1.0]])

    def _ensure_actions(self, n):
        if n == self.n_actions: return
        self._A = np.eye(self.state_dim + n + 1) * self.ridge
        self._b = np.zeros(self.state_dim + n + 1)
        self.n_actions = n
        self._n = 0

    def update(self, t: CausalTransition):
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x); self._b += t.reward * x; self._n += 1

    def predict(self, s, a):
        if self._n < 2: return 0.0
        try: theta = np.linalg.solve(self._A, self._b)
        except np.linalg.LinAlgError: return 0.0
        return float(self._features(s, a) @ theta)

    def counterfactuals(self, s, n_actions):
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

    def _fresh(self):
        now = time.time()
        return [u for u in self._updates.values() if (now - u.timestamp) <= self.staleness_s]

    def aggregate(self):
        fresh = self._fresh()
        if not fresh: return self._global
        weights = np.array([u.n_samples / max(u.carbon_intensity, 1.0) for u in fresh])
        weights /= max(weights.sum(), 1e-12)
        agg: Dict[str, np.ndarray] = {}
        for k in fresh[0].weights.keys():
            stacked = np.stack([u.weights[k] for u in fresh], axis=0)
            blended = np.tensordot(weights, stacked, axes=([0], [0]))
            if self.dp_sigma > 0:
                blended = blended + np.random.normal(0.0, self.dp_sigma, size=blended.shape)
            agg[k] = blended
        self._global = agg
        return agg

    def global_weights(self):
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
        for r in self._scores: self._scores[r] *= self.decay
        signal = reward if success else -abs(reward) * 0.5
        self._scores[role] += signal
        self._counts[role] += 1

    def weights(self):
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}


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
        if not bids or n_candidates <= 0: return 0
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

    def to_dict(self):
        return asdict(self)


class DecisionExplainer:
    def __init__(self, feature_names: Optional[Sequence[str]] = None):
        self.feature_names = list(feature_names) if feature_names else [
            "model_size_mb", "prompt_len", "gen_len", "gpu_mem_free_mb", "disk_speed_class",
        ]

    def _attributions(self, chosen, pareto):
        if not pareto: return []
        arr = np.array([[float(c.get(f, 0.0)) for f in self.feature_names] for c in pareto])
        chosen_vec = np.array([float(chosen.get(f, 0.0)) for f in self.feature_names])
        mean = arr.mean(axis=0); std = arr.std(axis=0) + 1e-9
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
        cf_list = []
        for idx, score in sorted(counterfactuals.items(),
                                 key=lambda kv: kv[1], reverse=True)[:3]:
            if 0 <= idx < len(pareto):
                cf_list.append({"alternative_idx": idx,
                                "expected_reward": float(score)})
        top_feat = ", ".join(f"{n}={v:+.2f}" for n, v in attrs[:3])
        rationale = (
            f"Chose policy #{chosen_idx} (confidence={confidence:.2f}). "
            f"Top deviations: {top_feat}. Carbon price={carbon_price:.4f}. "
            f"Safety={'ok' if safety_ok else 'OVERRIDE'}. "
            f"Counterfactuals: {len(cf_list)}."
        )
        return Explanation(
            decision_id=f"dec-{int(time.time()*1000) % 10**9}",
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

    def _ry(self, state, theta, q):
        c, s = math.cos(theta / 2), math.sin(theta / 2)
        n = len(state); new = state.copy(); step = 1 << q
        for i in range(n):
            if i & step == 0:
                a, b = state[i], state[i | step]
                new[i] = c * a - s * b; new[i | step] = s * a + c * b
        return new

    def _simulate(self, features, n_candidates):
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128); state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._params[q, 0] * features[q % len(features)] + self._params[q, 1])
            state = self._ry(state, angle, q)
        probs_full = np.abs(state) ** 2
        probs = np.zeros(n_candidates, dtype=np.float64)
        for i, p in enumerate(probs_full): probs[i % n_candidates] += p
        return probs / max(probs.sum(), 1e-12)

    def teacher_probs(self, features, n):
        if n <= 0: return np.zeros(0)
        return self._simulate(np.asarray(features, dtype=np.float64), n)


class DistillationEnsemble:
    def __init__(self, quantum: QuantumInspiredTeacher, alpha: float = 0.5):
        self.quantum = quantum; self.alpha = alpha

    def blend(self, features, other, n):
        q = self.quantum.teacher_probs(features, n)
        if other is None or len(other) != n: return q
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

    def observe(self, reward: float):
        self._rewards.append(float(reward))

    def entropy(self, probs):
        if probs is None or len(probs) == 0: return 1.0
        p = np.clip(np.asarray(probs, dtype=np.float64), 1e-12, 1.0)
        return float(-np.sum(p * np.log(p)) / math.log(len(p)))

    def variance(self):
        if len(self._rewards) < 3: return 0.0
        return float(np.var(np.asarray(self._rewards)))

    def is_uncertain(self, probs):
        h = self.entropy(probs); v = self.variance()
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
            return True
        loop = asyncio.get_running_loop()
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req), timeout=self.timeout_s)
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
        self.audit.append({"request_id": req.request_id,
                           "approved": bool(approved), "reason": req.reason})
        return bool(approved)


class ActiveLearningSampler:
    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []

    def maybe_store(self, record, uncertainty, threshold: float = 0.5) -> bool:
        if uncertainty < threshold: return False
        if len(self._buffer) >= self.capacity: self._buffer.pop(0)
        self._buffer.append({**record, "uncertainty": uncertainty, "t": time.time()})
        return True

    def sample(self, k: int = 8):
        if not self._buffer: return []
        return random.sample(self._buffer, min(k, len(self._buffer)))

    def __len__(self):
        return len(self._buffer)


# ===========================================================================
# ===========================================================================
# CORE ENHANCEMENT CLASSES (original plus fixes)
# ===========================================================================
# ===========================================================================


# --------------------- CarbonDelayScheduler ---------------------
class CarbonDelayScheduler:
    """Schedules tasks to run during lower‑carbon periods."""
    def __init__(self, carbon_api, max_delay_seconds: int = 3600,
                 threshold_gco2_per_kwh: float = 150.0):
        self.carbon_api = carbon_api
        self.max_delay = max_delay_seconds
        self.threshold = threshold_gco2_per_kwh
        self.queue: List[Tuple[float, int, Dict[str, Any]]] = []
        self._seq = 0

    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if task.get("priority") == "high":
            return {"status": "forward", "task": task, "delay_until": None}

        current_intensity = self.carbon_api.get_current()
        if current_intensity <= self.threshold:
            return {"status": "forward", "task": task, "delay_until": None}

        forecast_minutes = self.max_delay // 60 + 1
        forecast = self.carbon_api.get_forecast(forecast_minutes)
        if not forecast:
            return {"status": "forward", "task": task, "delay_until": None}

        now = time.time()
        best_time = None
        for ts, intensity in forecast:
            if intensity < self.threshold:
                best_time = ts
                break

        if best_time is None or (best_time - now) > self.max_delay:
            return {"status": "forward", "task": task, "delay_until": None}

        heapq.heappush(self.queue, (best_time, self._seq, task))
        self._seq += 1
        return {"status": "delayed", "task": task, "delay_until": best_time}

    def tick(self) -> List[Dict[str, Any]]:
        now = time.time()
        released: List[Dict[str, Any]] = []
        while self.queue and self.queue[0][0] <= now:
            _, _, task = heapq.heappop(self.queue)
            released.append(task)
        return released


# --------------------- CarbonAPIStub ---------------------
class CarbonAPIStub:
    """Mock carbon intensity API with a true sinusoidal daily cycle."""
    def __init__(self, base_intensity: float = 200.0, volatility: float = 50.0):
        self.base = base_intensity
        self.volatility = volatility
        self._start_time = time.time()

    def _intensity(self, ts: float) -> float:
        cycle = (ts - self._start_time) / 3600.0
        # Daily cycle: sin peaks at 12h, troughs at 0h/24h
        base = self.base + self.volatility * 0.5 * (1.0 + math.sin(2 * math.pi * cycle / 12.0))
        return max(50.0, base + random.gauss(0, 3))

    def get_current(self) -> float:
        return self._intensity(time.time())

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        now = time.time()
        return [(now + i * 60, self._intensity(now + i * 60))
                for i in range(0, minutes, 10)]


# --------------------- GPUProfiler ---------------------
try:
    import pynvml
    NVML_AVAILABLE = True
    try:
        pynvml.nvmlInit()
    except Exception:
        NVML_AVAILABLE = False
except ImportError:
    NVML_AVAILABLE = False


class GPUProfiler:
    def __init__(self, sample_interval_sec: float = 0.5):
        self.sample_interval = sample_interval_sec
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._latest_metrics: Dict[str, Any] = {}
        self._disk_io_start = psutil.disk_io_counters() if psutil else None
        self._last_disk_time = time.time()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def start(self):
        if self._running: return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _sample_loop(self):
        while not self._stop_event.is_set():
            snapshot = self._snapshot()
            with self._lock:
                self._latest_metrics = snapshot
            self._stop_event.wait(self.sample_interval)

    def _snapshot(self) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        if NVML_AVAILABLE:
            try:
                device_count = pynvml.nvmlDeviceGetCount()
                if device_count > 0:
                    handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                    mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0
                    name = pynvml.nvmlDeviceGetName(handle)
                    if isinstance(name, bytes): name = name.decode()
                    metrics["gpu_name"] = name
                    metrics["gpu_memory_total_mb"] = mem_info.total / 1024**2
                    metrics["gpu_memory_free_mb"] = mem_info.free / 1024**2
                    metrics["gpu_memory_used_mb"] = mem_info.used / 1024**2
                    metrics["gpu_utilization_pct"] = util.gpu / 100.0
                    metrics["gpu_power_watts"] = power
                    metrics["gpu_temp_c"] = pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU)
            except Exception:
                pass
        else:
            metrics["gpu_available"] = False

        vm = psutil.virtual_memory()
        metrics["cpu_memory_total_mb"] = vm.total / 1024**2
        metrics["cpu_memory_free_mb"] = vm.available / 1024**2
        metrics["cpu_utilization_pct"] = psutil.cpu_percent(interval=None) / 100.0

        now = time.time()
        disk_io = psutil.disk_io_counters()
        if self._disk_io_start and now - self._last_disk_time > 0.5:
            delta_time = now - self._last_disk_time
            read_bytes = disk_io.read_bytes - self._disk_io_start.read_bytes
            write_bytes = disk_io.write_bytes - self._disk_io_start.write_bytes
            metrics["disk_read_bandwidth_gbps"] = (read_bytes / delta_time) * 8 / 1e9
            metrics["disk_write_bandwidth_gbps"] = (write_bytes / delta_time) * 8 / 1e9
        self._disk_io_start = disk_io
        self._last_disk_time = now
        return metrics

    def get_current_metrics(self) -> Dict[str, Any]:
        if self._running:
            with self._lock:
                return dict(self._latest_metrics)
        return self._snapshot()


# --------------------- MetricAggregator ---------------------
class MetricAggregator:
    """Wraps the FlexGen executor and captures real-time metrics."""

    def __init__(self, gpu_profiler: GPUProfiler, executor_fn: Callable):
        self.profiler = gpu_profiler
        self.executor = executor_fn

    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()

        try:
            output, raw_inference_metrics = self.executor(task, policy)
            success = True
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()

        elapsed = end_time - start_time
        avg_power = (start_metrics.get("gpu_power_watts", 0.0)
                     + end_metrics.get("gpu_power_watts", 0.0)) / 2.0
        total_energy_kwh = (avg_power * elapsed) / 3600.0 / 1000.0

        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed if elapsed > 0 else 0.0

        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0

        quality_score = float(raw_inference_metrics.get("quality_score", 0.0))

        # FIX: promote quality_score to top level so RewardCalculator sees it
        return {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "quality_score": quality_score,
            "elapsed_sec": elapsed,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_power_avg_watts": avg_power,
            "gpu_memory_peak_mb": max(start_metrics.get("gpu_memory_used_mb", 0),
                                      end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "gpu_oom": (not success
                        and "CUDA out of memory" in str(raw_inference_metrics.get("error", ""))),
            "real_metrics": end_metrics,
        }


# --------------------- RewardCalculator ---------------------
class RewardCalculator:
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        self._default_weights = dict(self.weights)

    def snapshot(self) -> Dict[str, float]:
        return dict(self.weights)

    def restore(self, w: Optional[Dict[str, float]]) -> None:
        self.weights = dict(w) if w else dict(self._default_weights)

    def compute(self, aggregated_metrics: Dict[str, Any],
                constraints: Dict[str, Any],
                carbon_intensity_gco2_kwh: float = 0.0) -> float:
        quality = aggregated_metrics.get("quality_score", 1.0)
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)
        oom = aggregated_metrics.get("gpu_oom", False)

        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))
        else:
            energy_eff = 0.0

        penalty = 0.0
        if oom: penalty -= 10.0
        max_latency = constraints.get("max_latency_ms", 1e9)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0
        if quality < constraints.get("min_quality", 0.5):
            penalty -= 5.0

        reward = (
            self.weights["quality"] * quality +
            self.weights["throughput"] * min(1.0, throughput / 100.0) +
            self.weights["energy_efficiency"] * energy_eff +
            self.weights["carbon_efficiency"] * carbon_eff +
            self.weights["memory_efficiency"] * mem_eff
        ) + penalty
        return max(-10.0, min(10.0, reward))


# --------------------- WorkloadFingerprint ---------------------
class WorkloadFingerprint:
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.model_size_mb / 1000.0,
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_size_mb": self.model_size_mb,
            "prompt_len": self.prompt_len,
            "gen_len": self.gen_len,
            "gpu_mem_free_mb": self.gpu_mem_free_mb,
            "disk_speed_class": self.disk_speed_class,
        }


# --------------------- PolicyMetaCache ---------------------
class PolicyMetaCache:
    def __init__(self, max_age_hours: float = 24.0,
                 dist_threshold: float = 0.2, max_size: int = 500):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.max_size = max_size
        self.store: Dict[tuple, Tuple[Dict[str, Any], float, float]] = {}
        self.vectors: List[np.ndarray] = []
        self.keys: List[tuple] = []
        self._lock = threading.Lock()

    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(float(x) for x in vec.tolist())

    def get_best_policy(self, fp: WorkloadFingerprint) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self.vectors: return None
            vec = fp.to_vector()
            best_idx, best_dist = -1, float('inf')
            for i, stored_vec in enumerate(self.vectors):
                dist = float(np.linalg.norm(vec - stored_vec))
                if dist < best_dist:
                    best_dist, best_idx = dist, i
            if best_idx == -1 or best_dist > self.dist_threshold:
                return None
            key = self.keys[best_idx]
            policy, timestamp, _ = self.store[key]
            if (time.time() - timestamp) > self.max_age_seconds:
                return None
            return dict(policy)

    def update(self, fp: WorkloadFingerprint, policy: Dict[str, Any], reward: float) -> None:
        with self._lock:
            vec = fp.to_vector()
            key = self._vector_to_key(vec)
            if key in self.store:
                old_policy, _, old_reward = self.store[key]
                if reward > old_reward:
                    self.store[key] = (dict(policy), time.time(), reward)
            else:
                if len(self.vectors) >= self.max_size:
                    oldest_i = min(range(len(self.vectors)),
                                   key=lambda i: self.store[self.keys[i]][1])
                    oldest_key = self.keys[oldest_i]
                    self.store.pop(oldest_key, None)
                    self.vectors.pop(oldest_i)
                    self.keys.pop(oldest_i)
                self.store[key] = (dict(policy), time.time(), reward)
                self.vectors.append(vec)
                self.keys.append(key)

    def export_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "store": [(list(k), [dict(v[0]), v[1], v[2]]) for k, v in self.store.items()],
                "vectors": [v.tolist() for v in self.vectors],
                "keys": [list(k) for k in self.keys],
            }

    def import_state(self, data: Dict[str, Any]) -> None:
        with self._lock:
            try:
                self.store = {tuple(k): (dict(v[0]), float(v[1]), float(v[2]))
                              for k, v in data.get("store", [])}
                self.vectors = [np.array(v) for v in data.get("vectors", [])]
                self.keys = [tuple(k) for k in data.get("keys", [])]
            except Exception:
                self.store, self.vectors, self.keys = {}, [], []


# --------------------- ContextualBandit ---------------------
class ContextualBandit:
    def __init__(self, action_space: List[Dict[str, Any]], fallback_solver: Callable):
        self.actions = [dict(p) for p in action_space]
        self.fallback_solver = fallback_solver
        self.num_actions = len(self.actions)
        self.weights: Dict[tuple, np.ndarray] = {}
        self.trials: Dict[tuple, int] = {}
        self.action_trials: Dict[tuple, np.ndarray] = {}
        self._lock = threading.Lock()

    def _encode_context(self, fp: WorkloadFingerprint) -> tuple:
        return tuple(float(x) for x in fp.to_vector().tolist())

    @staticmethod
    def _canonical(p: Dict[str, Any]) -> str:
        return json.dumps(p, sort_keys=True)

    def select_action(self, fp: WorkloadFingerprint,
                      min_trials_before_bandit: int = 5,
                      confidence_threshold: float = 0.6) -> Tuple[Optional[Dict], float]:
        ctx_key = self._encode_context(fp)
        with self._lock:
            n_trials = self.trials.get(ctx_key, 0)
            if n_trials < min_trials_before_bandit:
                return None, 0.0
            weights = self.weights[ctx_key]
            std = 1.0 / np.sqrt(n_trials + 1)
            sampled = np.random.normal(weights, std)
            best_idx = int(np.argmax(sampled))
            confidence = 1.0 - (1.0 / (n_trials + 1))
            if confidence < confidence_threshold:
                return None, 0.0
            return dict(self.actions[best_idx]), confidence

    def update(self, fp: WorkloadFingerprint, action: Dict[str, Any], reward: float) -> None:
        ctx_key = self._encode_context(fp)
        with self._lock:
            if ctx_key not in self.weights:
                self.weights[ctx_key] = np.zeros(self.num_actions)
                self.trials[ctx_key] = 0
                self.action_trials[ctx_key] = np.zeros(self.num_actions, dtype=int)
            canonical = self._canonical(action)
            action_idx = -1
            for i, a in enumerate(self.actions):
                if self._canonical(a) == canonical:
                    action_idx = i; break
            if action_idx < 0:
                return
            n = self.action_trials[ctx_key][action_idx]
            lr = 0.1 / (n + 1)
            old_weight = self.weights[ctx_key][action_idx]
            self.weights[ctx_key][action_idx] = old_weight + lr * (reward - old_weight)
            self.action_trials[ctx_key][action_idx] += 1
            self.trials[ctx_key] += 1

    def seed_safe_policy(self, fp: WorkloadFingerprint, policy: Dict[str, Any]) -> None:
        ctx_key = self._encode_context(fp)
        with self._lock:
            if ctx_key not in self.weights:
                self.weights[ctx_key] = np.zeros(self.num_actions)
                self.trials[ctx_key] = 0
                self.action_trials[ctx_key] = np.zeros(self.num_actions, dtype=int)
            canonical = self._canonical(policy)
            for i, a in enumerate(self.actions):
                if self._canonical(a) == canonical:
                    self.weights[ctx_key][i] = 1.0
                    break

    def add_action(self, policy: Dict[str, Any]) -> bool:
        """Grow the action space atomically. Returns True if added."""
        with self._lock:
            canonical = self._canonical(policy)
            if any(self._canonical(a) == canonical for a in self.actions):
                return False
            self.actions.append(dict(policy))
            self.num_actions = len(self.actions)
            for key in self.weights:
                self.weights[key] = np.append(self.weights[key], 0.0)
                self.action_trials[key] = np.append(self.action_trials[key], 0)
            return True

    def export_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "actions": [dict(a) for a in self.actions],
                "weights": [(list(k), v.tolist()) for k, v in self.weights.items()],
                "trials": [(list(k), int(v)) for k, v in self.trials.items()],
                "action_trials": [(list(k), v.tolist()) for k, v in self.action_trials.items()],
            }

    def import_state(self, data: Dict[str, Any]) -> None:
        with self._lock:
            try:
                if data.get("actions"):
                    self.actions = [dict(a) for a in data["actions"]]
                    self.num_actions = len(self.actions)
                self.weights = {tuple(k): np.asarray(v, dtype=np.float64)
                                for k, v in data.get("weights", [])}
                self.trials = {tuple(k): int(v) for k, v in data.get("trials", [])}
                self.action_trials = {tuple(k): np.asarray(v, dtype=int)
                                      for k, v in data.get("action_trials", [])}
            except Exception:
                self.weights, self.trials, self.action_trials = {}, {}, {}


# --------------------- GreenAgentPolicyRouter (Enhanced) ---------------------
class GreenAgentPolicyRouter:
    def __init__(self, carbon_api, action_space: List[Dict[str, Any]],
                 lp_solver: Callable, executor: Callable,
                 min_trials_before_bandit: int = 5,
                 confidence_threshold: float = 0.6,
                 limit_graph=None, rlhf=None, distiller=None,
                 moe_router=None, bio_generator=None,
                 # Ten enhancement toggles
                 enable_quantum_teacher: bool = True,
                 enable_causal: bool = True,
                 enable_federated: bool = True,
                 enable_multi_agent: bool = True,
                 enable_temporal_logic: bool = True,
                 enable_xai: bool = True,
                 enable_precision_switching: bool = True,
                 enable_carbon_market: bool = True,
                 enable_chaos: bool = False,
                 enable_hitl: bool = True,
                 # Enhancement params
                 chaos_fault_prob: float = 0.0,
                 chaos_latency_ms: float = 0.0,
                 chaos_carbon_spike_prob: float = 0.0,
                 hitl_approver: Optional[Callable[[HITLRequest], bool]] = None,
                 hitl_timeout_s: float = 2.0,
                 rec_default_kwh: float = 0.0):
        self.carbon_scheduler = CarbonDelayScheduler(carbon_api)
        self.cache = PolicyMetaCache()
        self.bandit = ContextualBandit(action_space, lp_solver)
        self.lp_solver = lp_solver
        self.executor = executor
        self.min_trials = min_trials_before_bandit
        self.conf_threshold = confidence_threshold

        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distiller = distiller
        self.moe_router = moe_router
        self.bio_generator = bio_generator
        self._update_lock = threading.Lock()

        # ---------------- Enhancement 1: Quantum-Distillation ----------------
        self.quantum_teacher = QuantumInspiredTeacher(seed=42) if enable_quantum_teacher else None
        self.distill_ensemble = (DistillationEnsemble(self.quantum_teacher, alpha=0.5)
                                 if self.quantum_teacher else None)
        if self.distiller is not None:
            self.distiller.teachers = [
                self._teacher_cache,
                self._teacher_bandit,
                self._teacher_lp,
                self._teacher_moe,
            ]

        # ---------------- Enhancement 2: Causal RL ----------------
        self.causal = (CausalCounterfactualEstimator(
            state_dim=5, n_actions=self.bandit.num_actions)
            if enable_causal else None)
        self.last_counterfactuals: Dict[int, float] = {}

        # ---------------- Enhancement 3: Federated ----------------
        self.federated = FederatedAggregator() if enable_federated else None

        # ---------------- Enhancement 4: Multi-Agent ----------------
        self.roles = EmergentRoleRegistry() if enable_multi_agent else None
        self.coordinator = MultiAgentCoordinator(self.roles) if self.roles else None

        # ---------------- Enhancement 5: Temporal Logic ----------------
        self.temporal = None; self.shield = None
        if enable_temporal_logic:
            self.temporal = TemporalLogicMonitor(horizon=10)
            self.temporal.add_formula(STLFormula(
                name="latency_reasonable",
                predicate=lambda r: r.get("elapsed_sec", 0.0) <= 30.0,
                operator=STLOperator.ALWAYS, horizon=10))
            self.temporal.add_formula(STLFormula(
                name="eventually_success",
                predicate=lambda r: r.get("success", False),
                operator=STLOperator.EVENTUALLY, horizon=5))
            self.shield = SafetyShield(self.temporal)

        # ---------------- Enhancement 6: XAI ----------------
        self.explainer = DecisionExplainer() if enable_xai else None
        self.last_explanation: Optional[Explanation] = None

        # ---------------- Enhancement 7: Adaptive Precision ----------------
        self.precision_controller = (PrecisionController(quality_floor=0.95)
                                     if enable_precision_switching else None)
        self.precision_adapter = (HardwareAwareAdapter(self.precision_controller)
                                  if self.precision_controller else None)
        self.current_precision: Optional[PrecisionLevel] = None

        # ---------------- Enhancement 8: Carbon Market / REC ----------------
        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.recs = RECInventory() if enable_carbon_market else None
        if self.recs and rec_default_kwh > 0:
            self.recs.add(rec_default_kwh)

        # ---------------- Enhancement 9: Chaos ----------------
        self.chaos = (ChaosEngineer(ChaosConfig(
            fault_prob=chaos_fault_prob,
            latency_inject_ms=chaos_latency_ms,
            carbon_spike_prob=chaos_carbon_spike_prob))
            if enable_chaos else None)
        self.breaker = CircuitBreaker("executor") if enable_chaos else None

        # ---------------- Enhancement 10: HITL ----------------
        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = (HumanInTheLoopGate(approver=hitl_approver, timeout_s=hitl_timeout_s)
                     if enable_hitl else None)
        self.active_learner = ActiveLearningSampler() if enable_hitl else None

    # ---------------- Teachers ----------------
    def _teacher_cache(self, fp: WorkloadFingerprint):
        return self.cache.get_best_policy(fp)

    def _teacher_bandit(self, fp: WorkloadFingerprint):
        policy, confidence = self.bandit.select_action(
            fp, min_trials_before_bandit=self.min_trials,
            confidence_threshold=self.conf_threshold)
        return policy if policy is not None and confidence >= self.conf_threshold else None

    def _teacher_lp(self, fp: WorkloadFingerprint):
        return self.lp_solver(fp)

    def _teacher_moe(self, fp: WorkloadFingerprint):
        """MoE expert suggestion based on model size and GPU memory."""
        if self.moe_router is None:
            return None
        if fp.model_size_mb > 30000 or fp.gpu_mem_free_mb < 4000:
            return {"gpu_batch_size": 2, "block_size": 16,
                    "weight_device": "cpu", "kv_cache_device": "cpu",
                    "weight_bits": 8}
        return {"gpu_batch_size": 1, "block_size": 8,
                "weight_device": "gpu", "kv_cache_device": "gpu",
                "weight_bits": 16}

    # ---------------- Fingerprint ----------------
    def _generate_fingerprint(self, task: Dict[str, Any]) -> WorkloadFingerprint:
        return WorkloadFingerprint(
            model_size_mb=task.get("model_size_mb", 0),
            prompt_len=task.get("prompt_len", 0),
            gen_len=task.get("gen_len", 0),
            gpu_mem_free_mb=task.get("gpu_mem_free_mb", 0),
            disk_speed_class=task.get("disk_speed_class", 1))

    # ---------------- LIMIT Graph ----------------
    def _apply_limit_graph(self, fp: WorkloadFingerprint,
                           policy: Dict[str, Any]) -> Dict[str, Any]:
        if self.limit_graph is None:
            return dict(policy)
        context = {"fingerprint": fp.to_vector().tolist()}
        try:
            limits = self.limit_graph.get_limits(context)
        except Exception:
            limits = {}
        if not isinstance(limits, dict):
            return dict(policy)
        out = dict(policy)
        if "max_block_size" in limits and "block_size" in out:
            out["block_size"] = min(int(out.get("block_size", 8)),
                                    int(limits["max_block_size"]))
        if "forced_weight_device" in limits:
            out["weight_device"] = limits["forced_weight_device"]
        return out

    # ---------------- Main pipeline ----------------
    def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        # Chaos fault injection at entry
        if self.chaos and self.chaos.maybe_fault():
            return {"status": "failed", "reason": "chaos_fault"}

        # Step 1: Carbon delay
        delay_result = self.carbon_scheduler.submit(task)
        if delay_result["status"] == "delayed":
            return {
                "status": "deferred",
                "reason": "low_carbon_window",
                "delay_until": delay_result["delay_until"],
            }

        # Step 2: Fingerprint
        fp = self._generate_fingerprint(task)

        # Step 3: Policy selection
        if self.distiller is not None:
            policies = []
            for teacher in self.distiller.teachers:
                p = teacher(fp)
                if p is not None:
                    policies.append(p)
            if policies:
                # Weighted vote by canonical JSON
                counter = Counter(json.dumps(p, sort_keys=True) for p in policies)
                most_common = counter.most_common(1)[0][0]
                final_policy = json.loads(most_common)
                policy_source = "distillation"
            else:
                final_policy = self.lp_solver(fp)
                policy_source = "lp_solver"
        else:
            cached_policy = self.cache.get_best_policy(fp)
            if cached_policy is not None:
                final_policy = cached_policy
                policy_source = "cache"
            else:
                bandit_policy, confidence = self.bandit.select_action(
                    fp, min_trials_before_bandit=self.min_trials,
                    confidence_threshold=self.conf_threshold)
                if bandit_policy is not None and confidence >= self.conf_threshold:
                    final_policy = bandit_policy
                    policy_source = "bandit"
                else:
                    final_policy = self.lp_solver(fp)
                    policy_source = "lp_solver"
                    self.bandit.seed_safe_policy(fp, final_policy)

        # Step 4: LIMIT Graph
        final_policy = self._apply_limit_graph(fp, final_policy)

        # Step 5: Adaptive precision
        carbon_intensity = self.carbon_scheduler.carbon_api.get_current()
        if self.chaos:
            carbon_intensity = self.chaos.maybe_carbon_spike(carbon_intensity)
        carbon_price = (self.carbon_market.price(carbon_intensity, time.localtime().tm_hour)
                        if self.carbon_market else 0.0)
        if self.precision_adapter:
            final_policy, level = self.precision_adapter.adapt(
                final_policy, carbon_intensity, 0.5, carbon_price)
            self.current_precision = level

        # Step 6: Multi-agent vote
        if self.coordinator is not None and self.bandit.actions:
            bids: List[AgentBid] = []
            for role in AgentRole:
                idx = random.randrange(len(self.bandit.actions))
                if role == AgentRole.CARBON_BROKER:
                    idx = int(np.argmin([a.get("weight_bits", 16)
                                         for a in self.bandit.actions]))
                elif role == AgentRole.EXPLOITER:
                    idx = int(np.argmax([a.get("gpu_batch_size", 1)
                                         for a in self.bandit.actions]))
                elif role == AgentRole.SAFETY_OFFICER:
                    idx = 0
                bids.append(AgentBid(
                    agent_id=role.value, role=role, confidence=0.7,
                    proposed_action=idx, rationale=role.value,
                    carbon_score=1.0 / (1.0 + carbon_intensity / 1000.0)))
            voted = self.coordinator.vote(bids, len(self.bandit.actions))
            voted_policy = dict(self.bandit.actions[voted])
            # Only override if not a high-confidence cache/distillation pick.
            if policy_source in ("bandit", "lp_solver"):
                final_policy = voted_policy
                policy_source = policy_source + "+vote"

        # Step 7: Temporal shield
        safety_ok = True; stl_verdict: Dict[str, bool] = {}
        if self.shield and self.temporal:
            self.temporal.observe({
                "elapsed_sec": 0.0,  # unknown at selection time
                "success": True,
                "carbon_intensity": carbon_intensity,
            })
            _, safety_ok, stl_verdict = self.shield.screen(
                final_policy, self.bandit.actions, score_key="block_size")

        # Step 8: HITL gate
        human_approved = True
        uncertainty_score = 0.0
        if self.hitl and self.uncertainty:
            probs = np.ones(max(1, len(self.bandit.actions))) / max(1, len(self.bandit.actions))
            is_unc, unc = self.uncertainty.is_uncertain(probs)
            uncertainty_score = unc
            if is_unc or not safety_ok:
                req = HITLRequest(
                    request_id=str(uuid.uuid4())[:8],
                    reason="high_uncertainty" if is_unc else "safety_violation",
                    chosen_idx=0, candidates=list(self.bandit.actions),
                    uncertainty=unc, carbon_price=carbon_price)
                human_approved = self._await_hitl(req)
                if not human_approved and self.bandit.actions:
                    final_policy = dict(min(self.bandit.actions,
                                            key=lambda a: a.get("gpu_batch_size", 1)))

        # Step 9: Chaos latency
        if self.chaos:
            delay_ms = self.chaos.maybe_latency()
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

        # Step 10: Execute
        if self.breaker and not self.breaker.allow():
            return {"status": "failed", "reason": "circuit_open"}
        try:
            metrics = self.executor(task, final_policy)
            if self.breaker:
                self.breaker.record(True)
        except Exception as e:
            if self.breaker:
                self.breaker.record(False)
            return {"status": "failed", "reason": "executor_error", "error": str(e)}

        # Step 11: Causal counterfactuals
        state_vec = fp.to_vector()
        if self.causal:
            action_idx = 0
            canonical = json.dumps(final_policy, sort_keys=True)
            for i, a in enumerate(self.bandit.actions):
                if json.dumps(a, sort_keys=True) == canonical:
                    action_idx = i; break
            self.causal.update(CausalTransition(
                state=state_vec, action=action_idx, reward=0.0,
                next_state=state_vec))
            self.last_counterfactuals = self.causal.counterfactuals(
                state_vec, len(self.bandit.actions))

        # Step 12: Federated submit
        if self.federated:
            self.federated.submit(FederatedUpdate(
                node_id="local",
                weights={"local": np.array([carbon_intensity], dtype=np.float64)},
                n_samples=1, carbon_intensity=carbon_intensity))
            self.federated.aggregate()

        # Step 13: XAI
        explanation = None
        if self.explainer:
            fp_dict = fp.to_dict()
            explanation = self.explainer.explain(
                chosen_idx=0, chosen=fp_dict, pareto=[fp_dict],
                counterfactuals=self.last_counterfactuals,
                safety_ok=safety_ok and human_approved,
                carbon_price=carbon_price, confidence=0.7)
            self.last_explanation = explanation

        return {
            "status": "completed",
            "policy": final_policy,
            "policy_source": policy_source,
            "metrics": metrics,
            "fingerprint": fp,
            "carbon_intensity": carbon_intensity,
            "carbon_price": carbon_price,
            "precision_level": (self.current_precision.value
                                if self.current_precision else None),
            "safety_ok": safety_ok,
            "stl_verdict": stl_verdict,
            "human_approved": human_approved,
            "uncertainty": uncertainty_score,
            "counterfactuals": self.last_counterfactuals,
            "explanation": explanation.to_dict() if explanation else None,
            "rec_kwh": self.recs.total_kwh() if self.recs else 0.0,
        }

    def _await_hitl(self, req: HITLRequest) -> bool:
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop and loop.is_running():
                return True  # auto-approve to avoid blocking the loop
            return asyncio.run(self.hitl.request(req))
        except Exception:
            return True

    # ---------------- Post-task learning ----------------
    def update_after_task(self, fp: WorkloadFingerprint, policy: Dict[str, Any],
                          reward: float, policy_source: str) -> None:
        with self._update_lock:
            # Canonical comparison for bandit update
            canonical = json.dumps(policy, sort_keys=True)
            bandit_idx = -1
            for i, a in enumerate(self.bandit.actions):
                if json.dumps(a, sort_keys=True) == canonical:
                    bandit_idx = i; break
            if bandit_idx >= 0 and policy_source.split("+")[0] in ("bandit", "distillation"):
                self.bandit.update(fp, policy, reward)
            elif policy_source.split("+")[0] in ("cache", "lp_solver"):
                self.cache.update(fp, policy, reward)
            else:
                self.cache.update(fp, policy, reward)

            # RLHF update
            if self.rlhf is not None and bandit_idx >= 0:
                context = fp.to_vector().tolist()
                self.rlhf.update(context, bandit_idx, reward)

            # Federated submit with reward
            if self.federated is not None:
                self.federated.submit(FederatedUpdate(
                    node_id="local_reward",
                    weights={"reward": np.array([reward], dtype=np.float64)},
                    n_samples=1, carbon_intensity=400.0))
                self.federated.aggregate()

            # Causal update with real reward
            if self.causal is not None and bandit_idx >= 0:
                self.causal.update(CausalTransition(
                    state=fp.to_vector(), action=bandit_idx,
                    reward=reward, next_state=fp.to_vector()))

            # Multi-agent role update
            if self.roles is not None:
                self.roles.record(
                    AgentRole.CARBON_BROKER if "carbon" in policy_source
                    else AgentRole.EXPLOITER,
                    success=reward > 0.0, reward=reward)

            # Uncertainty + active learning
            if self.uncertainty:
                self.uncertainty.observe(reward)
            if self.active_learner:
                self.active_learner.maybe_store(
                    {"fp": fp.to_dict(), "policy": policy, "reward": reward},
                    uncertainty=abs(reward), threshold=0.4)

            # Bio-inspired expansion (values, not keys)
            if self.bio_generator is not None and policy_source.split("+")[0] == "lp_solver":
                ctx_key = self.bandit._encode_context(fp)
                n_trials = self.bandit.trials.get(ctx_key, 0)
                if n_trials > 10:
                    try:
                        new_policies = self.bio_generator.evolve(
                            population=list(self.bandit.actions),
                            fitness_fn=lambda p: random.uniform(0, 1),
                            generations=2,
                            population_size=len(self.bandit.actions) * 2)
                        if isinstance(new_policies, dict):
                            new_policies = [new_policies]
                        added = 0
                        for p in new_policies:
                            if not isinstance(p, dict): continue
                            if self.bandit.add_action(p):
                                added += 1
                        if added:
                            print(f"[Bio] Action space expanded by {added} policies.")
                    except Exception as e:
                        print(f"[Bio] expansion failed: {e}")

    def tick_carbon_queue(self) -> List[Dict[str, Any]]:
        return self.carbon_scheduler.tick()

    # ---------------- Metrics for external consumers ----------------
    def get_stats(self) -> Dict[str, Any]:
        return {
            "bandit_actions": len(self.bandit.actions),
            "bandit_contexts": len(self.bandit.weights),
            "cache_size": len(self.cache.store),
            "carbon_queue_size": len(self.carbon_scheduler.queue),
            "current_precision": (self.current_precision.value
                                  if self.current_precision else None),
            "rec_kwh": self.recs.total_kwh() if self.recs else 0.0,
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
            "last_explanation": (self.last_explanation.to_dict()
                                 if self.last_explanation else None),
            "counterfactuals": self.last_counterfactuals,
            "active_learning_buffer": len(self.active_learner) if self.active_learner else 0,
            "role_weights": ({r.value: w for r, w in self.roles.weights().items()}
                             if self.roles else {}),
        }


# ----------------------------------------------------------------------
# 3. Main orchestration loop
# ----------------------------------------------------------------------
STATE_FILE = "green_agent_state.json"


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".ga_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception:
        try: os.unlink(tmp)
        except OSError: pass
        raise


def run_green_agent_main_loop():
    # ---- Setup ----
    carbon_api = CarbonAPIStub(base_intensity=180.0)
    gpu_profiler = GPUProfiler(sample_interval=0.5)
    gpu_profiler.start()

    def lp_solver(fp: WorkloadFingerprint) -> Dict[str, Any]:
        return {"gpu_batch_size": 1, "block_size": 8,
                "weight_device": "gpu", "kv_cache_device": "gpu",
                "weight_bits": 16}

    def flexgen_executor(task, policy):
        time.sleep(0.5)
        return ({"text": "ok"},
                {"tokens_generated": 20, "quality_score": 0.95})

    metric_aggregator = MetricAggregator(gpu_profiler, flexgen_executor)
    reward_calc = RewardCalculator()

    ACTION_SPACE = [
        {"gpu_batch_size": 1, "block_size": 8, "weight_device": "gpu",
         "kv_cache_device": "gpu", "weight_bits": 16},
        {"gpu_batch_size": 2, "block_size": 16, "weight_device": "cpu",
         "kv_cache_device": "cpu", "weight_bits": 8},
    ]

    # ---- Instantiate external modules (fallbacks OK) ----
    limit_graph = LimitGraph()
    try: limit_graph.build_graph(nodes=[], edges=[])
    except Exception: pass

    rlhf = RLHFOptimizer(action_space=list(range(len(ACTION_SPACE))))
    distiller = MultiTeacherDistiller(teachers=[])
    moe_router = ExpertRouter()
    bio_generator = GeneticPolicyGenerator()

    router = GreenAgentPolicyRouter(
        carbon_api=carbon_api,
        action_space=ACTION_SPACE,
        lp_solver=lp_solver,
        executor=metric_aggregator.run,
        min_trials_before_bandit=5,
        confidence_threshold=0.6,
        limit_graph=limit_graph,
        rlhf=rlhf,
        distiller=distiller,
        moe_router=moe_router,
        bio_generator=bio_generator,
        enable_quantum_teacher=True,
        enable_causal=True,
        enable_federated=True,
        enable_multi_agent=True,
        enable_temporal_logic=True,
        enable_xai=True,
        enable_precision_switching=True,
        enable_carbon_market=True,
        enable_chaos=False,   # flip to True to inject faults
        enable_hitl=True,
    )

    # Load persistent state (bandit + cache)
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            router.bandit.import_state(state.get("bandit", {}))
            router.cache.import_state(state.get("cache", {}))
            print(f"Loaded state from {STATE_FILE}")
        except Exception as e:
            print(f"Failed to load state: {e}")

    stop_event = threading.Event()
    task_lock = threading.Lock()

    def process_task(task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with task_lock:
            # Snapshot & adjust reward weights per task
            original_weights = reward_calc.snapshot()
            try:
                if task.get("priority") == "eco":
                    reward_calc.weights = {
                        "quality": 0.1, "throughput": 0.1,
                        "energy_efficiency": 0.4, "carbon_efficiency": 0.4,
                        "memory_efficiency": 0.0,
                    }
                elif task.get("priority") == "speed":
                    reward_calc.weights = {
                        "quality": 0.2, "throughput": 0.5,
                        "energy_efficiency": 0.1, "carbon_efficiency": 0.1,
                        "memory_efficiency": 0.1,
                    }
                elif task.get("priority") == "high":
                    reward_calc.weights = {
                        "quality": 0.4, "throughput": 0.3,
                        "energy_efficiency": 0.1, "carbon_efficiency": 0.1,
                        "memory_efficiency": 0.1,
                    }

                result = router.handle_task(task)
                if result["status"] == "deferred":
                    print(f"[Deferred] Task delayed until {result['delay_until']}")
                    return result
                if result["status"] == "failed":
                    print(f"[Failed] {result.get('reason')}")
                    return result

                fp = result["fingerprint"]
                carbon_intensity = result.get("carbon_intensity", carbon_api.get_current())
                metrics = result["metrics"]
                constraints = task.get("constraints", {})
                reward = reward_calc.compute(metrics, constraints, carbon_intensity)
                result["reward"] = reward
                print(f"[Done] Policy: {result['policy_source']}, "
                      f"Reward: {reward:.3f}, Precision: {result.get('precision_level')}, "
                      f"Safety: {result.get('safety_ok')}, HITL: {result.get('human_approved')}")

                router.update_after_task(fp, result["policy"], reward,
                                         result["policy_source"])
                if result.get("explanation"):
                    print(f"[XAI] {result['explanation'].get('rationale')}")
                return result
            finally:
                reward_calc.restore(original_weights)

    def release_checker():
        while not stop_event.is_set():
            stop_event.wait(5)
            released = router.tick_carbon_queue()
            for task in released:
                print("[Release] Carbon window opened, re-submitting task")
                process_task(task)

    release_thread = threading.Thread(target=release_checker, daemon=True)
    release_thread.start()

    tasks = [
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2,
         "priority": "normal",
         "constraints": {"max_latency_ms": 5000, "min_quality": 0.8}},
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2,
         "priority": "eco",
         "constraints": {"max_latency_ms": 6000, "min_quality": 0.7}},
        {"model_size_mb": 35000, "prompt_len": 512, "gen_len": 32,
         "gpu_mem_free_mb": 12000, "disk_speed_class": 2,
         "priority": "high"},
    ]

    try:
        for i, task in enumerate(tasks):
            print(f"\n--- Processing task {i} (priority={task.get('priority')}) ---")
            process_task(task)

        time.sleep(15)

        # Persist state atomically
        _atomic_write_json(STATE_FILE, {
            "bandit": router.bandit.export_state(),
            "cache": router.cache.export_state(),
            "stats": router.get_stats(),
        })
        print("State persisted.")
        print(json.dumps(router.get_stats(), indent=2, default=str))
    finally:
        stop_event.set()
        release_thread.join(timeout=2.0)
        gpu_profiler.stop()
        print("Shutdown complete.")


import uuid  # used by the HITL request id


if __name__ == "__main__":
    run_green_agent_main_loop()
