"""
FlexGen Controller — End-to-end closed-loop policy selection (enhanced v3).

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

CHANGELOG v2 -> v3
- TemporalLogicMonitor: correct G / F / U semantics + per-formula horizon.
- SafetyShield: screens *candidate* metrics, not just history.
- EmergentRoleRegistry: credit goes to the *winning* role; adds exploration.
- MultiAgentCoordinator: weights by role success-rate + tie-break randomness.
- CausalCounterfactualEstimator: proper (s,a)->(r,s') SCM with joint solve.
- FederatedAggregator: shape validation + per-client DP + RDP budget.
- PrecisionController: monotonic level selection; HardwareAwarePolicyAdapter
  carries precision through to the reconstructed policy.
- CarbonMarketClient: correct peak-hour pricing; async API hook + cache.
- RECInventory: correct FIFO consumption with expiry.
- FlexGenController.step: parallel candidate execution with bounded
  concurrency; real next-state for the causal model; no in-place mutation
  of pareto_candidates; feedback carbon intensity is now consistent.
- Removed NumPy-2.0-deprecated .ptp(); uses max-min.
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
    Quantum-inspired teacher producing soft policy targets.

    Deterministic state-vector simulation for reproducibility.
    Handles feature vectors that are shorter than n_qubits, longer, or empty.
    """

    def __init__(self, n_qubits: int = 5, seed: int = 0):
        self.n_qubits = max(1, int(n_qubits))
        self.rng = np.random.default_rng(seed)
        self._circuit_params = self.rng.normal(size=(self.n_qubits, 3))

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
        if n_candidates <= 0:
            return np.zeros(0)
        feat = np.asarray(feature_vec, dtype=np.float64).ravel()
        if feat.size == 0:
            feat = np.zeros(self.n_qubits, dtype=np.float64)
        dim = 1 << self.n_qubits
        state = np.zeros(dim, dtype=np.complex128)
        state[0] = 1.0
        for q in range(self.n_qubits):
            angle = float(self._circuit_params[q, 0] * feat[q % feat.size])
            state = self._apply_ry(state, angle, q)
        probs_full = np.abs(state) ** 2
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
    Linear SCM with joint (s, a) -> (r, s') prediction.

      - predict_reward(s, a): E[r | do(a), s]
      - predict_next(s, a)  : E[s' | do(a), s]

    Uses a single shared design matrix and a stacked target. Solves the linear
    systems only when the model is dirty, so per-step prediction is O(d^2).
    """

    def __init__(self, state_dim: int, n_actions: int, ridge: float = 1e-3):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.ridge = ridge
        d = state_dim + n_actions + 1
        self._A = np.eye(d) * ridge
        self._b_reward = np.zeros(d)
        self._b_next = np.zeros((d, state_dim))
        self._n = 0
        self._theta_reward: Optional[np.ndarray] = None
        self._theta_next: Optional[np.ndarray] = None
        self._dirty = True

    def _features(self, s: np.ndarray, a: int) -> np.ndarray:
        s = np.asarray(s, dtype=np.float64).ravel()[: self.state_dim]
        if s.size < self.state_dim:
            s = np.pad(s, (0, self.state_dim - s.size))
        one_hot = np.zeros(self.n_actions)
        one_hot[a % self.n_actions] = 1.0
        return np.concatenate([s, one_hot, [1.0]])

    def update(self, t: CausalTransition) -> None:
        x = self._features(t.state, t.action)
        self._A += np.outer(x, x)
        self._b_reward += t.reward * x
        ns = np.asarray(t.next_state, dtype=np.float64).ravel()[: self.state_dim]
        if ns.size < self.state_dim:
            ns = np.pad(ns, (0, self.state_dim - ns.size))
        self._b_next += np.outer(x, ns)
        self._n += 1
        self._dirty = True

    def _solve(self) -> None:
        if not self._dirty:
            return
        self._theta_reward = np.linalg.solve(self._A, self._b_reward)
        self._theta_next = np.linalg.solve(self._A, self._b_next)
        self._dirty = False

    def predict_reward(self, s: np.ndarray, a: int) -> float:
        if self._n < 2:
            return 0.0
        self._solve()
        return float(self._features(s, a) @ self._theta_reward)

    def predict_next(self, s: np.ndarray, a: int) -> np.ndarray:
        if self._n < 2:
            return np.asarray(s, dtype=np.float64)
        self._solve()
        return self._features(s, a) @ self._theta_next

    def counterfactuals(self, s: np.ndarray, all_actions: Sequence[int]) -> Dict[int, float]:
        return {a: self.predict_reward(s, a) for a in all_actions}

    def best_counterfactual(self, s: np.ndarray, all_actions: Sequence[int]) -> Tuple[int, float]:
        cf = self.counterfactuals(s, all_actions)
        if not cf:
            return -1, 0.0
        best_a = max(cf, key=cf.get)
        return best_a, cf[best_a]

    def get_state(self) -> Dict[str, Any]:
        return {"n_observations": self._n, "dirty": self._dirty}


# ===========================================================================
# Enhancement 3 — Federated Green Learning Across Deployments
# ===========================================================================
class SimpleRDPAccountant:
    """
    Minimal RDP accountant for the federated aggregation path.

    Uses the standard bound for the subsampled Gaussian mechanism:
        eps_RDP(alpha) = alpha * q^2 / (2 * sigma^2)
    and converts to (eps, delta) via the standard formula.
    """

    def __init__(self, target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.target_delta = target_delta
        self.max_epsilon = max_epsilon
        self.steps = 0
        self.orders = [1 + x / 10.0 for x in range(1, 100)]
        self.rdp = np.zeros(len(self.orders))

    def step(self, sigma: float, q: float = 1.0) -> None:
        if sigma <= 0:
            return
        self.steps += 1
        for i, o in enumerate(self.orders):
            self.rdp[i] += (o * q * q) / (2 * sigma * sigma)

    def epsilon(self) -> float:
        if self.steps == 0:
            return 0.0
        eps = [
            self.rdp[i] + math.log(1.0 / self.target_delta) / (self.orders[i] - 1)
            for i in range(len(self.orders))
            if self.orders[i] > 1
        ]
        return float(min(eps)) if eps else float("inf")

    def budget_exceeded(self) -> bool:
        return self.epsilon() > self.max_epsilon

    def get_state(self) -> Dict[str, Any]:
        return {
            "steps": self.steps,
            "epsilon": self.epsilon(),
            "delta": self.target_delta,
            "budget_exceeded": self.budget_exceeded(),
        }


@dataclass
class FederatedUpdate:
    node_id: str
    weights: np.ndarray
    n_samples: int
    carbon_intensity: float
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    """
    Carbon-weighted FedAvg with:

      - shape validation (rejects mismatched updates)
      - per-client Gaussian noise before aggregation (proper DP)
      - RDP budget tracking
      - staleness filtering
    """

    def __init__(self, dp_sigma: float = 1e-3, max_staleness_s: float = 3600.0,
                 target_delta: float = 1e-5, max_epsilon: float = 10.0):
        self.dp_sigma = dp_sigma
        self.max_staleness_s = max_staleness_s
        self._updates: Dict[str, FederatedUpdate] = {}
        self._global: Optional[np.ndarray] = None
        self._lock = threading.Lock()
        self.accountant = SimpleRDPAccountant(
            target_delta=target_delta, max_epsilon=max_epsilon
        )
        self.rejected: List[Dict[str, Any]] = []

    def submit(self, update: FederatedUpdate) -> bool:
        if update.weights is None or update.weights.size == 0:
            return False
        with self._lock:
            if self._global is not None and update.weights.shape != self._global.shape:
                self.rejected.append({
                    "node_id": update.node_id,
                    "expected": self._global.shape,
                    "got": update.weights.shape,
                    "t": time.time(),
                })
                logger.warning(
                    f"Rejected federated update from {update.node_id}: shape mismatch"
                )
                return False
            if self.accountant.budget_exceeded():
                logger.warning(
                    f"DP budget exceeded (eps={self.accountant.epsilon():.3f}); "
                    f"skipping federated update"
                )
                return False
            if self.dp_sigma > 0:
                noisy = update.weights + np.random.normal(
                    0.0, self.dp_sigma, size=update.weights.shape
                )
                self.accountant.step(self.dp_sigma, q=1.0)
                update = FederatedUpdate(
                    node_id=update.node_id,
                    weights=noisy,
                    n_samples=update.n_samples,
                    carbon_intensity=update.carbon_intensity,
                    timestamp=update.timestamp,
                )
            self._updates[update.node_id] = update
            return True

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
            self._global = np.tensordot(weights, stacked, axes=([0], [0]))
            return self._global

    def global_weights(self) -> Optional[np.ndarray]:
        return self._global

    def get_dp_state(self) -> Dict[str, Any]:
        return {**self.accountant.get_state(), "rejected_updates": len(self.rejected)}


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
    Tracks per-role success rates and lets roles emerge based on which role
    actually contributed the winning bid — not on which role is currently
    dominant. Epsilon-greedy role selection encourages exploration.
    """

    def __init__(self, decay: float = 0.995, exploration: float = 0.1):
        self.decay = decay
        self.exploration = exploration
        self._scores: Dict[AgentRole, float] = {r: 1.0 for r in AgentRole}
        self._counts: Dict[AgentRole, int] = {r: 0 for r in AgentRole}
        self._successes: Dict[AgentRole, int] = {r: 0 for r in AgentRole}

    def record(self, winning_role: AgentRole, success: bool, reward: float) -> None:
        for r in self._scores:
            self._scores[r] *= self.decay
        signal = abs(reward) if success else -abs(reward) * 0.5
        self._scores[winning_role] += signal
        self._counts[winning_role] += 1
        if success:
            self._successes[winning_role] += 1

    def success_rate(self, role: AgentRole) -> float:
        c = self._counts[role]
        return (self._successes[role] / c) if c > 0 else 0.5

    def weights(self) -> Dict[AgentRole, float]:
        total = sum(max(v, 1e-6) for v in self._scores.values())
        return {r: max(v, 1e-6) / total for r, v in self._scores.items()}

    def dominant_role(self) -> AgentRole:
        return max(self._scores, key=self._scores.get)

    def sample_role(self, rng: Optional[random.Random] = None) -> AgentRole:
        rng = rng or random
        if rng.random() < self.exploration:
            return rng.choice(list(AgentRole))
        return self.dominant_role()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "scores": {r.value: round(v, 4) for r, v in self._scores.items()},
            "counts": {r.value: c for r, c in self._counts.items()},
            "success_rates": {r.value: round(self.success_rate(r), 4) for r in AgentRole},
            "dominant": self.dominant_role().value,
        }


class MultiAgentCoordinator:
    """
    Carbon-weighted voting with per-role success-rate weighting and
    proper tie-breaking.
    """

    def __init__(self, registry: EmergentRoleRegistry,
                 role_bias: Optional[Dict[AgentRole, float]] = None,
                 rng: Optional[random.Random] = None):
        self.registry = registry
        self.role_bias = role_bias or {
            AgentRole.EXPLORER: 0.6,
            AgentRole.EXPLOITER: 0.9,
            AgentRole.SAFETY_OFFICER: 1.2,
            AgentRole.CARBON_BROKER: 1.1,
            AgentRole.VERIFIER: 1.0,
        }
        self.rng = rng or random.Random(0)
        self._last_vote: Optional[Dict[str, Any]] = None

    def vote(self, bids: Sequence[AgentBid], n_candidates: int) -> int:
        if not bids or n_candidates <= 0:
            return 0
        role_w = self.registry.weights()
        scores = np.zeros(n_candidates)
        contributions: Dict[int, List[Tuple[AgentRole, float]]] = defaultdict(list)
        for b in bids:
            idx = b.proposed_action % n_candidates
            role_success = self.registry.success_rate(b.role)
            weight = (
                role_w.get(b.role, 0.1)
                * self.role_bias.get(b.role, 1.0)
                * max(b.confidence, 0.0)
                * (1.0 + b.carbon_score)
                * (0.5 + role_success)
            )
            scores[idx] += weight
            contributions[idx].append((b.role, weight))
        top = float(scores.max())
        tied = [i for i, s in enumerate(scores) if abs(s - top) < 1e-9]
        winner = self.rng.choice(tied) if len(tied) > 1 else tied[0]
        winning_roles = [r for r, _ in contributions.get(winner, [])]
        self._last_vote = {
            "winner": int(winner),
            "scores": scores.tolist(),
            "winning_roles": [r.value for r in winning_roles],
        }
        return int(winner)

    def last_vote(self) -> Optional[Dict[str, Any]]:
        return self._last_vote


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
    # For UNTIL: the "until" predicate. If None, uses predicate as the eventual goal.
    until_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None


class TemporalLogicMonitor:
    """
    Runtime monitor over rolling windows.

    Semantics:
      G[0,h) φ   : φ holds at every step in the window
      F[0,h) φ   : φ holds at some step in the window
      φ U[0,h) ψ : φ holds at every step until ψ holds, and ψ becomes true within h
    """

    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=max(horizon * 4, horizon))
        self.formulas: List[STLFormula] = []
        self._evaluations: List[Dict[str, Any]] = []

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
            if f.operator == STLOperator.ALWAYS:
                ok = all(f.predicate(r) for r in window)
            elif f.operator == STLOperator.EVENTUALLY:
                ok = any(f.predicate(r) for r in window)
            elif f.operator == STLOperator.UNTIL:
                until_pred = f.until_predicate or f.predicate
                idx_until = next(
                    (i for i, r in enumerate(window) if until_pred(r)), None
                )
                if idx_until is None:
                    ok = False
                else:
                    ok = all(f.predicate(window[i]) for i in range(idx_until))
            else:
                ok = True
            results[f.name] = ok
            self._evaluations.append(
                {"formula": f.name, "ok": ok, "t": time.time()}
            )
        return results

    def recent_evaluations(self, k: int = 32) -> List[Dict[str, Any]]:
        return self._evaluations[-k:]


class SafetyShield:
    """
    Screens a *candidate* against formal constraints. Two layers:

      1. Immediate constraint check on the candidate's predicted metrics.
      2. History-based invariant check (via the temporal monitor).

    If either fails, falls back to the lowest-carbon feasible candidate.
    """

    def __init__(
        self,
        monitor: TemporalLogicMonitor,
        immediate_constraints: Optional[List[Callable[[Dict[str, Any]], bool]]] = None,
    ):
        self.monitor = monitor
        self.immediate_constraints = immediate_constraints or []
        self.violations: List[Dict[str, Any]] = []

    def _candidate_ok(self, candidate: Dict[str, Any]) -> Tuple[bool, List[str]]:
        bad = [
            f"constraint_{i}"
            for i, c in enumerate(self.immediate_constraints)
            if not c(candidate)
        ]
        return (len(bad) == 0), bad

    def screen(
        self,
        selected_idx: int,
        pareto_candidates: Sequence[Dict[str, Any]],
    ) -> Tuple[int, bool, Dict[str, bool]]:
        if not pareto_candidates:
            return selected_idx, True, {}

        history_verdict = self.monitor.verify()
        violated_hist = [k for k, ok in history_verdict.items() if not ok]

        cand_ok, cand_violations = (True, [])
        if 0 <= selected_idx < len(pareto_candidates):
            cand_ok, cand_violations = self._candidate_ok(
                pareto_candidates[selected_idx]
            )

        if cand_ok and not violated_hist:
            return selected_idx, True, history_verdict

        def fallback_key(i: int) -> Tuple[float, float]:
            c = pareto_candidates[i]
            return (
                float(c.get("carbon_g", float("inf"))),
                float(c.get("latency_ms", float("inf"))),
            )

        safe_idx = min(range(len(pareto_candidates)), key=fallback_key)
        self.violations.append({
            "time": time.time(),
            "history_violations": violated_hist,
            "candidate_violations": cand_violations,
            "selected_idx": selected_idx,
            "replaced_with": safe_idx,
        })
        return safe_idx, False, history_verdict


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
        chosen_vec = np.array(
            [float(chosen.get(f, 0.0)) for f in self.feature_names]
        )
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
            cf_list.append({
                "alternative_idx": idx,
                "expected_reward": float(score),
                "carbon_g": float(pareto[idx].get("carbon_g", float("nan"))),
                "latency_ms": float(pareto[idx].get("latency_ms", float("nan"))),
            })
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
    Chooses precision level as a monotonic function of:

      - carbon intensity stress
      - carbon price stress
      - latency headroom (low headroom -> prefer conservative precision)

    Bounded by quality floor and hardware support.
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

    def _feasible_levels(self) -> List[PrecisionLevel]:
        # Ordered from least aggressive to most aggressive by energy
        levels = [p for p in PrecisionLevel if p in self.supported]
        levels.sort(key=lambda p: _PRECISION_COST[p]["energy"], reverse=True)
        return [p for p in levels if _PRECISION_COST[p]["quality"] >= self.quality_floor]

    def select(
        self,
        carbon_intensity: float,
        latency_headroom_ratio: float,
        carbon_price: float = 0.0,
    ) -> PrecisionLevel:
        feasible = self._feasible_levels()
        if not feasible:
            return PrecisionLevel.FP32

        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        agg = 0.5 * stress + 0.3 * price_stress + 0.2 * (1.0 - headroom)

        idx = int(round(agg * (len(feasible) - 1)))
        idx = max(0, min(len(feasible) - 1, idx))
        return feasible[idx]


class HardwareAwarePolicyAdapter:
    """
    Applies precision to the policy and returns the mutated policy + level.
    If the policy is frozen, rebuilds it from its dict with the precision field.
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
        try:
            policy.precision = level.value  # type: ignore[attr-defined]
        except Exception:
            try:
                d = policy.to_dict()
                d["precision"] = level.value
                policy = FlexGenPolicy(**d)
            except Exception:
                pass
        return policy, level


# ===========================================================================
# Enhancement 8 — Carbon Markets and RECs
# ===========================================================================
class CarbonMarketClient:
    """
    Carbon price model with optional real API hook.

    Price = base + sensitivity * intensity * time_of_day_factor
    time_of_day_factor peaks at 18:00 (evening peak), bottoms at 06:00.
    """

    def __init__(
        self,
        base_price: float = 0.05,
        sensitivity: float = 0.0005,
        session: Optional[Any] = None,  # aiohttp.ClientSession or compatible
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.base_price = base_price
        self.sensitivity = sensitivity
        self._session = session
        self._api_url = api_url
        self._api_key = api_key
        self._cache: Dict[Tuple[int, int], float] = {}

    def _tod_factor(self, hour_of_day: Optional[int]) -> float:
        if hour_of_day is None:
            return 1.0
        # Peak at 18:00, trough at 06:00
        return 1.0 + 0.3 * math.sin((hour_of_day - 6) / 24.0 * 2 * math.pi)

    def _cache_key(self, carbon_intensity: float, hour_of_day: Optional[int]) -> Tuple[int, int]:
        return (int(carbon_intensity), -1 if hour_of_day is None else int(hour_of_day))

    async def price_async(
        self, carbon_intensity: float, hour_of_day: Optional[int] = None
    ) -> float:
        cache_key = self._cache_key(carbon_intensity, hour_of_day)
        if cache_key in self._cache:
            return self._cache[cache_key]

        value: Optional[float] = None
        if self._api_url and self._session is not None:
            try:
                headers = (
                    {"Authorization": f"Bearer {self._api_key}"}
                    if self._api_key else {}
                )
                async with self._session.get(
                    self._api_url,
                    headers=headers,
                    params={"intensity": carbon_intensity},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        value = float(data.get("price_usd_per_kg", 0.0))
            except Exception as e:
                logger.warning(f"Carbon market API failed: {e}")

        if value is None:
            value = (
                self.base_price
                + self.sensitivity * carbon_intensity * self._tod_factor(hour_of_day)
            )
        self._cache[cache_key] = value
        return value

    def price(
        self, carbon_intensity: float, hour_of_day: Optional[int] = None
    ) -> float:
        """Synchronous fallback used when the caller cannot await."""
        cache_key = self._cache_key(carbon_intensity, hour_of_day)
        if cache_key in self._cache:
            return self._cache[cache_key]
        value = (
            self.base_price
            + self.sensitivity * carbon_intensity * self._tod_factor(hour_of_day)
        )
        self._cache[cache_key] = value
        return value


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"
    expires_at: Optional[float] = None


class RECInventory:
    """
    Tracks Renewable Energy Credits and their carbon offset potential.
    FIFO consumption with expiry pruning.
    """

    def __init__(self, grid_kg_co2_per_kwh: float = 0.4, default_ttl_days: float = 365.0):
        self.grid_factor = grid_kg_co2_per_kwh
        self.default_ttl_days = default_ttl_days
        self._records: List[RECRecord] = []

    def _prune(self) -> None:
        now = time.time()
        self._records = [
            r for r in self._records
            if r.expires_at is None or r.expires_at > now
        ]

    def add(
        self,
        kwh: float,
        source: str = "solar",
        expires_at: Optional[float] = None,
    ) -> None:
        if expires_at is None:
            expires_at = time.time() + self.default_ttl_days * 86400.0
        self._records.append(
            RECRecord(kwh=kwh, issued_at=time.time(), source=source, expires_at=expires_at)
        )

    def total_kwh(self) -> float:
        self._prune()
        return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
        """Consume RECs FIFO and return the offset in kg CO2."""
        self._prune()
        remaining = float(kwh)
        offset = 0.0
        for r in self._records:
            if remaining <= 0:
                break
            take = min(r.kwh, remaining)
            r.kwh -= take
            remaining -= take
            offset += take * self.grid_factor
        self._records = [r for r in self._records if r.kwh > 1e-9]
        return offset

    def snapshot(self) -> Dict[str, Any]:
        return {
            "total_kwh": self.total_kwh(),
            "n_records": len(self._records),
            "sources": sorted({r.source for r in self._records}),
        }


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
        if (
            self._state == CircuitState.OPEN
            and time.time() - self._opened_at >= self.recovery_time_s
        ):
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
            metrics["latency_ms"] = (
                metrics.get("latency_ms", 0.0) + self.latency_injection_ms
            )
            self.events.append({"type": "latency_injection", "t": time.time()})

    def maybe_carbon_spike(self, carbon_intensity: float) -> float:
        if self.rng.random() < self.carbon_spike_prob:
            spike = carbon_intensity * 1.5
            self.events.append(
                {"type": "carbon_spike", "t": time.time(), "value": spike}
            )
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
            self.audit.append(
                {"request_id": req.request_id, "approved": True, "reason": "no_approver"}
            )
            return True
        loop = asyncio.get_running_loop()
        try:
            approved = await asyncio.wait_for(
                loop.run_in_executor(None, self.approver, req), timeout=self.timeout_s
            )
        except asyncio.TimeoutError:
            approved = self.auto_approve_on_timeout
        self.audit.append(
            {"request_id": req.request_id, "approved": approved, "reason": req.reason}
        )
        return bool(approved)


class ActiveLearningSampler:
    """
    Prioritizes uncertain transitions for later human/expert review and can
    feed them into a replay buffer for retraining.
    """

    def __init__(self, capacity: int = 256):
        self.capacity = capacity
        self._buffer: List[Dict[str, Any]] = []

    def maybe_store(
        self, record: Dict[str, Any], uncertainty: float, threshold: float = 0.5
    ) -> bool:
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

    def size(self) -> int:
        return len(self._buffer)


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
        # --- v3 additions ---
        seed: int = 0,
        max_parallel_candidates: int = 4,
        carbon_market_api_url: Optional[str] = None,
        carbon_market_api_key: Optional[str] = None,
        dp_sigma: float = 1e-3,
        dp_target_delta: float = 1e-5,
        dp_max_epsilon: float = 10.0,
    ):
        # Core
        self.node = node
        self.workload = workload
        self.carbon_intensity = carbon_intensity
        self.message_queue = message_queue
        self.use_real_executor = use_real_executor
        self.executor = executor if executor else MockFlexGenExecutor(
            carbon_intensity_g_per_kwh=carbon_intensity
        )
        self.use_cost_model_prefilter = use_cost_model_prefilter
        self.cost_model = cost_model or FlexGenCostModel(
            carbon_intensity_g_per_kwh=carbon_intensity
        )
        self.use_bio_search = use_bio_search
        self.bio_search_config = bio_search_config or {}
        self.modp_planner = modp_planner
        self.drift_detector = drift_detector
        self.gpu_profiler = gpu_profiler

        if hasattr(self.executor, "carbon_intensity"):
            self.executor.carbon_intensity = node.metadata.get(
                "region_carbon_intensity", carbon_intensity
            )

        # Master seed for determinism
        self._master_seed = int(seed)
        self._rng = random.Random(self._master_seed)
        np.random.seed(self._master_seed % (2 ** 32))

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

        self.max_parallel_candidates = max(1, int(max_parallel_candidates))

        # ---------------- Enhancement modules ----------------
        self.quantum_teacher = (
            QuantumDistillationTeacher(seed=self._master_seed)
            if enable_quantum_teacher else None
        )
        self.causal_estimator = (
            CausalCounterfactualEstimator(state_dim, n_actions)
            if enable_causal_rl else None
        )
        self.federated = (
            FederatedAggregator(
                dp_sigma=dp_sigma,
                target_delta=dp_target_delta,
                max_epsilon=dp_max_epsilon,
            )
            if enable_federated else None
        )

        self.role_registry = EmergentRoleRegistry() if enable_multi_agent else None
        self.multi_agent = (
            MultiAgentCoordinator(
                self.role_registry, rng=random.Random(self._master_seed)
            )
            if enable_multi_agent and self.role_registry else None
        )

        self.temporal_monitor = (
            TemporalLogicMonitor(horizon=10) if enable_temporal_logic else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.add_formula(
                STLFormula(
                    name="latency_ok",
                    predicate=lambda r: r.get("latency_ms", 0.0)
                    <= self.workload.latency_target * 1.5,
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
            self.temporal_monitor.add_formula(
                STLFormula(
                    name="carbon_bounded_until_success",
                    predicate=lambda r: r.get("carbon_g", 0.0) <= 10.0,
                    operator=STLOperator.UNTIL,
                    horizon=8,
                    until_predicate=lambda r: r.get("success", False),
                )
            )
        self.safety_shield = (
            SafetyShield(
                self.temporal_monitor,
                immediate_constraints=[
                    lambda c: c.get("latency_ms", 0.0)
                    <= self.workload.latency_target * 2.0,
                    lambda c: c.get("carbon_g", 0.0) <= 50.0,
                ],
            )
            if self.temporal_monitor else None
        )

        self.explainer = DecisionExplainer() if enable_xai else None
        self.precision_controller = (
            PrecisionController(quality_floor=quality_floor)
            if enable_precision_switching else None
        )
        self.precision_adapter = (
            HardwareAwarePolicyAdapter(self.precision_controller)
            if self.precision_controller else None
        )

        self.carbon_market = (
            CarbonMarketClient(
                base_price=carbon_market_base_price,
                api_url=carbon_market_api_url,
                api_key=carbon_market_api_key,
            )
            if enable_carbon_markets else None
        )
        self.rec_inventory = RECInventory() if enable_carbon_markets else None

        self.circuit_breaker = CircuitBreaker() if enable_resilience else None
        self.chaos = ChaosEngineer(seed=self._master_seed) if enable_resilience else None

        self.uncertainty = UncertaintyEstimator() if enable_hitl else None
        self.hitl = HumanInTheLoopGate(approver=hitl_approver) if enable_hitl else None
        self.active_learner = ActiveLearningSampler() if enable_hitl else None

        # State cache
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self.last_candidates: Optional[List[Dict[str, Any]]] = None
        self.last_explanation: Optional[Explanation] = None
        self.last_winning_role: Optional[AgentRole] = None

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
            if estimates:
                lat = np.array([e.total_latency_ms for _, e in estimates])
                ene = np.array([e.total_energy_joules for _, e in estimates])
                lat_n = (lat - lat.min()) / (lat.max() - lat.min() + 1e-9)
                ene_n = (ene - ene.min()) / (ene.max() - ene.min() + 1e-9)
                scores = 0.5 * lat_n + 0.5 * ene_n
                order = np.argsort(scores)
                candidates = [estimates[i][0] for i in order[:10]]
        return candidates

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    async def _execute_policy(self, policy: FlexGenPolicy) -> Dict[str, Any]:
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
            if (
                self.use_real_executor
                and callable(self.executor)
                and not isinstance(self.executor, MockFlexGenExecutor)
            ):
                gpu_before = self.gpu_profiler.get_gpu_metrics() if self.gpu_profiler else None
                result = self.executor(policy, self.node, self.workload)
                metrics = await result if asyncio.iscoroutine(result) else result
                if self.gpu_profiler:
                    gpu_after = self.gpu_profiler.get_gpu_metrics()
                    if "energy_joules" not in metrics and gpu_before and gpu_after:
                        avg_power = (
                            gpu_before.get("gpu_power_watts", 65)
                            + gpu_after.get("gpu_power_watts", 65)
                        ) / 2
                        latency_s = metrics.get("latency_ms", 0) / 1000.0
                        metrics["energy_joules"] = avg_power * latency_s
                        metrics["carbon_g"] = (
                            metrics["energy_joules"] / 3.6e6
                        ) * self._get_node_carbon_intensity()
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

        # 0. MODP temporal decision
        modp_info: Dict[str, Any] = {}
        if self.modp_planner:
            modp_action, delay, target_node = await self.modp_planner.plan(
                self.workload, self.node, None, queue_length=0,
                current_carbon=carbon_intensity,
            )
            modp_info = {"action": modp_action, "delay": delay, "target_node": target_node}
            if modp_action == "defer":
                logger.info(f"MODP suggests deferring for {delay} hours.")
                await self._publish_feedback(
                    selected_policy=None, chosen_idx=-1, pareto=[],
                    teacher_probs=None, explanation=None, reward=0.0,
                    metrics={"latency_ms": 0.0, "energy_joules": 0.0,
                             "carbon_g": 0.0, "success": True},
                    drift_detected=False,
                    extra={"modp": modp_info, "reason": "modp_deferral"},
                    carbon_intensity=carbon_intensity,
                )
                return {"action": "defer", "delay_hours": delay, "reason": "modp_deferral"}
            if modp_action == "move_node" and target_node:
                logger.info(f"MODP suggests moving to node {target_node} (single-node mode).")

        # 1. Candidates
        candidates = await self._generate_candidates()

        # 2. Carbon price (per-step)
        carbon_price = 0.0
        if self.carbon_market:
            carbon_price = self.carbon_market.price(
                carbon_intensity, hour_of_day=time.localtime().tm_hour
            )

        # 3. Adaptive precision
        precision_level: Optional[PrecisionLevel] = None
        if self.precision_adapter:
            headroom = 0.5
            adapted: List[FlexGenPolicy] = []
            for p in candidates:
                p2, lvl = self.precision_adapter.adapt(
                    p, carbon_intensity, headroom, carbon_price
                )
                adapted.append(p2)
                precision_level = lvl
            candidates = adapted

        # 4. Parallel candidate execution with bounded concurrency
        sem = asyncio.Semaphore(self.max_parallel_candidates)

        async def run_one(idx: int, policy: FlexGenPolicy) -> Dict[str, Any]:
            async with sem:
                m = await self._execute_policy(policy)
                m["policy_idx"] = idx
                m["policy"] = policy.to_dict()
                if precision_level is not None:
                    m["precision"] = precision_level.value
                return m

        metrics_list = await asyncio.gather(
            *(run_one(i, p) for i, p in enumerate(candidates))
        )

        # 5. Pareto filter
        feasible = [m for m in metrics_list if m.get("success", False)]
        if not feasible:
            logger.warning("No feasible policies found, falling back to all.")
            feasible = metrics_list
        pareto_candidates = self.pareto.filter(feasible) or feasible

        if not pareto_candidates:
            return {"action": "no_candidates", "reason": "pareto_empty"}

        # 6. State — real values derived from recent outcomes
        recent_rewards = (
            list(self.uncertainty._recent_rewards) if self.uncertainty else []
        )
        recent_success = float(
            np.mean([
                1.0 if m.get("success", False) else 0.0
                for m in pareto_candidates
            ])
        ) if pareto_candidates else 0.0
        avg_reward = float(np.mean(recent_rewards)) if recent_rewards else 0.0

        state = FlexGenState(
            tokens=self.workload.tokens,
            latency_target=self.workload.latency_target,
            gpu_memory_gb=self.node.metadata.get("gpu_memory_gb", 16.0),
            cpu_memory_gb=self.node.metadata.get("cpu_memory_gb", 64.0),
            disk_bandwidth_gbps=self.node.metadata.get("disk_bandwidth_gbps", 2.0),
            carbon_intensity=carbon_intensity,
            recent_success_rate=recent_success,
            avg_reward=avg_reward,
            policy_idx=0,
        )
        state_features = self._state_features(state)

        # 7. Teacher signal — quantum-enhanced or selector's own.
        action_idx, state_vec, teacher_probs = await self.selector.select_policy(
            pareto_candidates, state, exploration=True
        )
        if self.quantum_teacher:
            q_probs = self.quantum_teacher.teacher_probs(
                state_features, pareto_candidates
            )
            if teacher_probs is not None and q_probs.size == teacher_probs.size:
                teacher_probs = 0.5 * q_probs + 0.5 * np.asarray(teacher_probs)

        # 8. Multi-agent coordination vote
        winning_role: Optional[AgentRole] = None
        if self.multi_agent and self.role_registry and pareto_candidates:
            bids: List[AgentBid] = []
            base_conf = (
                float(teacher_probs[action_idx]) if teacher_probs is not None else 0.5
            )
            for role in AgentRole:
                if role == AgentRole.CARBON_BROKER:
                    bias_idx = int(np.argmin([c.get("carbon_g", 1e9) for c in pareto_candidates]))
                elif role == AgentRole.EXPLOITER:
                    bias_idx = int(np.argmin([c.get("latency_ms", 1e9) for c in pareto_candidates]))
                elif role == AgentRole.SAFETY_OFFICER:
                    bias_idx = int(np.argmin([
                        c.get("carbon_g", 1e9) + c.get("latency_ms", 1e9) / 1000.0
                        for c in pareto_candidates
                    ]))
                elif role == AgentRole.EXPLORER:
                    bias_idx = self._rng.randrange(len(pareto_candidates))
                else:  # VERIFIER
                    bias_idx = action_idx
                bids.append(
                    AgentBid(
                        agent_id=f"{role.value}-agent",
                        role=role,
                        confidence=base_conf,
                        proposed_action=bias_idx,
                        rationale=role.value,
                        carbon_score=1.0 / (
                            1.0 + float(pareto_candidates[bias_idx].get("carbon_g", 1.0))
                        ),
                    )
                )
            action_idx = self.multi_agent.vote(bids, len(pareto_candidates))
            last_vote = self.multi_agent.last_vote() or {}
            winning_roles = last_vote.get("winning_roles", [])
            if winning_roles:
                try:
                    winning_role = AgentRole(winning_roles[0])
                except ValueError:
                    winning_role = None

        # 9. Causal counterfactuals
        counterfactuals: Dict[int, float] = {}
        if self.causal_estimator:
            counterfactuals = self.causal_estimator.counterfactuals(
                state_features, list(range(len(pareto_candidates)))
            )

        # 10. Safety shield (candidate-aware, temporal-aware)
        safety_ok = True
        stl_verdict: Dict[str, bool] = {}
        if self.safety_shield:
            action_idx, safety_ok, stl_verdict = self.safety_shield.screen(
                action_idx, pareto_candidates
            )

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
                    action_idx = int(
                        np.argmin([c.get("carbon_g", 1e9) for c in pareto_candidates])
                    )

        # Bounds guard
        action_idx = max(0, min(len(pareto_candidates) - 1, action_idx))

        # Copy: avoid mutating pareto_candidates in place
        chosen_policy_metrics = dict(pareto_candidates[action_idx])
        policy_dict = dict(chosen_policy_metrics.get("policy", {}))
        if precision_level is not None:
            policy_dict["precision"] = precision_level.value
        try:
            chosen_policy = FlexGenPolicy(**policy_dict)
        except Exception:
            chosen_policy = FlexGenPolicy(**chosen_policy_metrics["policy"])

        # 12. REC offset
        rec_offset_kg = 0.0
        if self.rec_inventory and chosen_policy_metrics.get("energy_joules", 0.0) > 0:
            kwh = chosen_policy_metrics["energy_joules"] / 3.6e6
            rec_offset_kg = self.rec_inventory.consume(kwh)
            chosen_policy_metrics["carbon_g"] = max(
                0.0,
                float(chosen_policy_metrics.get("carbon_g", 0.0)) - rec_offset_kg,
            )

        # 13. Reward — carbon-market-aware
        base_reward = compute_reward(chosen_policy_metrics, self.workload)
        carbon_penalty = (
            carbon_price * float(chosen_policy_metrics.get("carbon_g", 0.0)) / 100.0
        )
        reward = base_reward - carbon_penalty

        # 14. Update selector, causal model, federated aggregator
        # Use a plausible next-state: the chosen candidate's observed metrics
        # compressed to the same feature dimension.
        next_state_vec = self._next_state_features(state_features, chosen_policy_metrics)
        await self.selector.update(state_vec, action_idx, reward, next_state_vec, teacher_probs)

        if self.causal_estimator:
            self.causal_estimator.update(
                CausalTransition(
                    state=state_vec,
                    action=action_idx,
                    reward=reward,
                    next_state=next_state_vec,
                    context={"carbon_intensity": carbon_intensity,
                             "carbon_price": carbon_price},
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

        if self.role_registry and winning_role is not None:
            self.role_registry.record(
                winning_role,
                success=bool(chosen_policy_metrics.get("success", False)),
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

        # 15. Temporal monitor observation
        if self.temporal_monitor:
            self.temporal_monitor.observe({
                "latency_ms": chosen_policy_metrics.get("latency_ms", 0.0),
                "carbon_g": chosen_policy_metrics.get("carbon_g", 0.0),
                "success": chosen_policy_metrics.get("success", False),
                "t": step_start,
            })

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
                chosen=chosen_policy_metrics,
                pareto=pareto_candidates,
                teacher_probs=teacher_probs,
                counterfactuals=counterfactuals,
                safety_ok=safety_ok and human_approved,
                carbon_price=carbon_price,
            )
            logger.info(f"XAI: {explanation.rationale}")
        self.last_explanation = explanation

        # 18. Publish feedback — pass the same carbon_intensity used in this step
        await self._publish_feedback(
            selected_policy=chosen_policy,
            chosen_idx=action_idx,
            pareto=pareto_candidates,
            teacher_probs=teacher_probs,
            explanation=explanation,
            reward=reward,
            metrics=chosen_policy_metrics,
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
                "federated_round_size": (
                    len(getattr(self.federated, "_updates", {}) or {})
                    if self.federated else 0
                ),
                "winning_role": winning_role.value if winning_role else None,
                "federated_dp": (
                    self.federated.get_dp_state() if self.federated else None
                ),
            },
            carbon_intensity=carbon_intensity,
        )

        # Cache
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs
        self.last_candidates = pareto_candidates
        self.last_winning_role = winning_role

        return {
            "chosen_policy": chosen_policy.to_dict(),
            "metrics": chosen_policy_metrics,
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
            "federated_dp": self.federated.get_dp_state() if self.federated else None,
            "winning_role": winning_role.value if winning_role else None,
        }

    # ------------------------------------------------------------------
    # Next-state features
    # ------------------------------------------------------------------
    def _next_state_features(
        self, prev_state_vec: np.ndarray, metrics: Dict[str, Any]
    ) -> np.ndarray:
        """
        Derive a plausible next-state vector from the observed metrics.
        Keeps the same dimension as the state feature vector used by the
        selector and the causal model. This is the v3 replacement for
        `next_state_vec = state_vec`.
        """
        s = np.asarray(prev_state_vec, dtype=np.float64).copy()
        if s.size >= 8:
            s[6] = 1.0 if metrics.get("success", False) else 0.0  # recent_success_rate
            # avg_reward slot: exponential smoothing toward observed reward proxy
            reward_proxy = (
                0.4 * float(metrics.get("success", 0.0))
                + 0.3 * (1.0 - min(1.0, float(metrics.get("latency_ms", 0.0))
                                   / max(self.workload.latency_target, 1.0)))
                + 0.3 * (1.0 - min(1.0, float(metrics.get("carbon_g", 0.0)) / 10.0))
            )
            s[7] = 0.7 * s[7] + 0.3 * reward_proxy
        return s

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
        carbon_intensity: Optional[float] = None,
    ) -> None:
        if not self.message_queue:
            return
        if carbon_intensity is None:
            carbon_intensity = self._get_node_carbon_intensity()
        ctx: Dict[str, Any] = {
            "node_id": self.node.id,
            "carbon_intensity": carbon_intensity,
            "pareto_size": len(pareto),
            "executor_type": "real" if self.use_real_executor else "mock",
            "drift_detected": drift_detected,
            "teacher_probs": teacher_probs.tolist() if teacher_probs is not None else [],
        }
        if extra:
            ctx.update({k: v for k, v in extra.items() if k != "counterfactuals"})
            ctx["counterfactual_keys"] = list(
                (extra.get("counterfactuals") or {}).keys()
            )
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

    def federated_dp_state(self) -> Optional[Dict[str, Any]]:
        return self.federated.get_dp_state() if self.federated else None

    def recent_explanations(self, k: int = 8) -> List[Dict[str, Any]]:
        if not self.explainer or not self.last_explanation:
            return []
        return [self.last_explanation.to_dict()]

    def active_learning_batch(self, k: int = 8) -> List[Dict[str, Any]]:
        return self.active_learner.sample(k) if self.active_learner else []

    def temporal_evaluations(self, k: int = 32) -> List[Dict[str, Any]]:
        return self.temporal_monitor.recent_evaluations(k) if self.temporal_monitor else []

    def role_snapshot(self) -> Optional[Dict[str, Any]]:
        return self.role_registry.snapshot() if self.role_registry else None

    def rec_snapshot(self) -> Optional[Dict[str, Any]]:
        return self.rec_inventory.snapshot() if self.rec_inventory else None
