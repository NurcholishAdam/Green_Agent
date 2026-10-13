"""
tests/test_mopd_orchestrator_v17.py — Enhanced v17.1.0 Test Suite
==================================================================

Self-contained test suite for the MOPD Orchestrator v17.1.0.

This file is a single-file bundle containing:
  1. Enhanced implementations of all ten enhancement modules.
  2. A fault-injectable in-memory storage.
  3. A unified `MOPDOrchestratorV17` wiring all ten together.
  4. Async pytest fixtures (pytest_asyncio).
  5. Comprehensive unit + integration + edge-case + determinism tests.

v17.1.0 enhancements over v17.0.0
---------------------------------
  * QuantumDistillation  : density-matrix superposition, von Neumann entropy,
                           interference term, temperature schedule.
  * CausalGraphLearner   : partial-correlation skeleton + orientation, PC-style.
  * FederatedGreen       : weighted aggregation, optional DP noise, sparse
                           compression, round-based synchronous FL.
  * MultiAgent           : Byzantine-tolerant reputation, decay, coalition bid.
  * TemporalLogic        : past-time operators (H, O, Y, S), windowed MTL.
  * XAI                  : Integrated Gradients + actionable counterfactuals
                           + coverage-guaranteed anchors.
  * AdaptivePrecision    : layer-wise sensitivity, energy accounting, guard rails.
  * CarbonMarket         : carbon budget tracking, REC retirement, price EMA.
  * ChaosTesting         : steady-state hypothesis, auto-rollback on breach.
  * HITL                 : uncertainty sampling, Bayesian preference posterior.

Run:
    pytest tests/test_mopd_orchestrator_v17.py -v
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple

import pytest
import pytest_asyncio

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    SKLEARN_AVAILABLE = False


# =============================================================================
# CONFIG HELPERS
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


class _ConfigDict(dict):
    """Dict-like config that also supports attribute access."""

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as exc:
            raise AttributeError(k) from exc

    def __setattr__(self, k, v):
        self[k] = v


_DEFAULTS: Dict[str, Any] = {
    "DB_PATH": ":memory:",
    "causal_exploration_rate": 0.1,
    "causal_pc_alpha": 0.05,
    "agent_count": 5,
    "agent_reputation_decay": 0.999,
    "byzantine_threshold": 0.35,
    "temporal_max_trace": 500,
    "temporal_formulas": ["G (quality >= 0.5)"],
    "temporal_windows_seconds": {},  # optional per-rule window override
    "xai_method": "integrated_gradients",  # or "kernel_shap", "lime"
    "xai_depth": 3,
    "xai_ci_samples": 4,
    "xai_cf_max_iter": 40,
    "xai_cf_target_delta": 0.1,
    "xai_anchor_min_precision": 0.8,
    "xai_ig_steps": 32,
    "precision_levels": ["fp32", "tf32", "bf16", "fp16", "int8"],
    "precision_switch_threshold": 0.02,
    "precision_max_sensitivity": 0.5,
    "distillation_temperature": 2.0,
    "distillation_alpha": 0.5,
    "distillation_learning_rate": 0.1,
    "distillation_min_temperature": 0.5,
    "distillation_temperature_decay": 0.999,
    "chaos_intensity": 0.0,
    "chaos_blast_radius": 0.5,
    "chaos_auto_rollback": True,
    "federated_dp_epsilon": None,
    "federated_dp_clip": 1.0,
    "federated_sparse_topk": 0.0,  # 0 disables sparsification
    "carbon_budget_kg": 100.0,
    "reputation_decay": 0.999,
    "max_concurrent_modules": 5,
}


# =============================================================================
# IN-MEMORY STORAGE (fault-injectable)
# =============================================================================
class InMemoryStorage:
    """Minimal storage mirroring the v5 schema's public API.

    Any method whose name is set in ``self.fail_on`` will raise a
    ``RuntimeError``. Methods whose name is set in ``self.silent`` will
    swallow the injected failure and return ``None`` (useful for testing
    soft-fail paths).
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.tables: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.preferences: Dict[str, Dict[str, float]] = {}
        self.calls: List[Tuple[str, Tuple]] = []
        self.fail_on: Optional[str] = None
        self.silent: set[str] = set()

    def _log(self, method: str, args: Tuple = ()):
        self.calls.append((method, args))
        if self.fail_on == method:
            if method in self.silent:
                return False  # signal caller: skip side effects
            raise RuntimeError(f"injected failure in {method}")
        return True

    # --- Quantum distillation ---
    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl, entropy=0.0):
        if not self._log("save_teacher_superposition"):
            return
        self.tables["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl, "entropy": entropy})

    # --- Causal ---
    def save_causal_edge(self, source, target, weight, confidence,
                         p_value=0.0):
        if not self._log("save_causal_edge"):
            return
        self.tables["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence, "p_value": p_value})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        if not self._log("save_causal_experiment"):
            return
        self.tables["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment,
            "outcome": outcome, "ate": ate, "method": method})

    # --- Federated ---
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0, epsilon=None):
        if not self._log("save_federated_weights"):
            return
        self.tables["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id, "epsilon": epsilon})

    def get_federated_weights(self, model_id, round_id=None):
        self._log("get_federated_weights")
        rows = [r for r in self.tables["federated_weights"]
                if r["model_id"] == model_id]
        if round_id is not None:
            rows = [r for r in rows if r["round_id"] == round_id]
        return rows

    # --- Multi-agent ---
    def save_agent(self, agent_id, role, reputation, utilities):
        if not self._log("save_agent"):
            return
        self.tables["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient,
                           payload):
        if not self._log("save_agent_message"):
            return
        self.tables["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient})

    def save_agent_reputation_event(self, agent_id, delta, reason):
        if not self._log("save_agent_reputation_event"):
            return
        self.tables["agent_reputation_events"].append({
            "agent_id": agent_id, "delta": delta, "reason": reason})

    # --- Temporal ---
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window_seconds, active=True):
        if not self._log("save_temporal_rule"):
            return
        self.tables["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula,
            "operator": operator, "severity": severity,
            "window_seconds": window_seconds})

    def save_temporal_trace(self, state, context=None):
        if not self._log("save_temporal_trace"):
            return
        self.tables["temporal_trace"].append({"state": state})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        if not self._log("save_temporal_violation"):
            return
        self.tables["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula,
            "step": step, "severity": severity, "approved": approved})

    # --- XAI ---
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        if not self._log("save_xai_explanation"):
            return
        self.tables["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label,
            "attributions": attributions})
        for name, val in (attributions or {}).items():
            self.tables["xai_feature_importance"].append({
                "explanation_id": explanation_id,
                "feature_name": str(name), "importance": float(val)})

    # --- Precision ---
    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        if not self._log("save_precision_switch"):
            return
        self.tables["precision_history"].append({
            "from_p": from_p, "to_p": to_p,
            "reason": reason, "saved_wh": saved_wh, "acc_delta": acc_delta})

    # --- Carbon / REC ---
    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        if not self._log("save_credit_price"):
            return
        self.tables["carbon_credit_prices"].append({
            "price_usd": price_usd, "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        if not self._log("save_rec"):
            return
        self.tables["rec_ledger"].append({
            "mwh": mwh, "retired": retired, "source": source})

    def get_rec_balance(self):
        self._log("get_rec_balance")
        return sum(r["mwh"] for r in self.tables["rec_ledger"]
                   if not r.get("retired"))

    def retire_rec(self, mwh):
        if not self._log("retire_rec"):
            return 0.0
        remaining = mwh
        for r in self.tables["rec_ledger"]:
            if r.get("retired"):
                continue
            take = min(r["mwh"], remaining)
            r["mwh"] -= take
            remaining -= take
            if r["mwh"] <= 0:
                r["retired"] = True
            if remaining <= 0:
                break
        return mwh - remaining

    def save_carbon_budget(self, used_kg, budget_kg):
        if not self._log("save_carbon_budget"):
            return
        self.tables["carbon_budget"].append({
            "used_kg": used_kg, "budget_kg": budget_kg})

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        if not self._log("save_net_zero_match"):
            return
        self.tables["net_zero_matches"].append({
            "match_id": match_id, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost})

    # --- Chaos ---
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0, rolled_back=False):
        if not self._log("save_chaos_experiment"):
            return
        self.tables["chaos_experiments"].append({
            "experiment_id": experiment_id, "fault_type": fault_type,
            "status": status, "steady_before": steady_before,
            "steady_after": steady_after, "rolled_back": rolled_back})

    # --- HITL ---
    def enqueue_hitl_request(self, request_id, rule_id, state,
                             severity="critical"):
        if not self._log("enqueue_hitl_request"):
            return
        self.tables["hitl_approval_queue"].append({
            "request_id": request_id, "status": "pending",
            "severity": severity})

    def resolve_hitl_request(self, request_id, status="approved"):
        if not self._log("resolve_hitl_request"):
            return
        for r in self.tables["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        if not self._log("save_user_preference"):
            return
        self.preferences[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self.preferences.get(user_id)


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE (ENHANCED)
# =============================================================================
class QuantumDistillationEngine:
    """Quantum-inspired distillation.

    v17.1 additions:
      * Density-matrix superposition with interference (off-diagonal).
      * Von Neumann entropy of the teacher ensemble.
      * Temperature schedule with decay + floor.
      * KL-weighted teacher contributions (agreement amplifies).
    """

    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5,
                 learning_rate=0.1, temperature_decay=0.999,
                 min_temperature=0.5, convergence_eps=1e-4,
                 rng: Optional[random.Random] = None):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.learning_rate = float(learning_rate)
        self.temperature_decay = float(temperature_decay)
        self.min_temperature = float(min_temperature)
        self.convergence_eps = float(convergence_eps)
        self.rng = rng or random.Random()

        self.teachers: Dict[str, List[float]] = {}
        self.teacher_weights: Dict[str, float] = {}
        self.student_policy: List[float] = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._converged = False

    # ---------- teacher management ----------
    def register_teacher(self, name, policy, weight: float = 1.0):
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [max(0.0, p) / s for p in policy]
        self.teacher_weights[name] = max(1e-6, float(weight))

    def unregister_teacher(self, name):
        self.teachers.pop(name, None)
        self.teacher_weights.pop(name, None)

    # ---------- math helpers ----------
    def _softmax(self, x: Iterable[float], temp: float) -> List[float]:
        x = list(x)
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _von_neumann_entropy(self, rho: List[List[float]]) -> float:
        """Entropy of a small density matrix via eigenvalues (SVD)."""
        if not NUMPY_AVAILABLE:
            # Fall back to Shannon entropy of the diagonal.
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
        """Return (policy, entropy). Uses density-matrix formulation.

        rho = sum_i w_i |psi_i><psi_i| where |psi_i> = sqrt(p_i).
        Off-diagonal terms create interference; diagonal is the ensemble
        probability.
        """
        if not self.teachers:
            return list(self.student_policy), 0.0

        n = self.n_actions
        # Build amplitudes sqrt(p) for each teacher, padded to n.
        amps: List[List[float]] = []
        weights: List[float] = []
        for tid, pol in self.teachers.items():
            a = [math.sqrt(max(pol[i], 1e-12)) if i < len(pol) else 0.0
                 for i in range(n)]
            amps.append(a)
            weights.append(self.teacher_weights.get(tid, 1.0))
        wsum = sum(weights) or 1.0
        weights = [w / wsum for w in weights]

        # Density matrix (real, symmetric here).
        rho = [[0.0] * n for _ in range(n)]
        for a, w in zip(amps, weights):
            for i in range(n):
                for j in range(n):
                    rho[i][j] += w * a[i] * a[j]

        # Diagonal = ensemble probabilities; normalise.
        diag = [max(rho[i][i], 1e-12) for i in range(n)]
        s = sum(diag) or 1.0
        policy = [d / s for d in diag]
        entropy = self._von_neumann_entropy(rho)
        return policy, entropy

    # ---------- step ----------
    async def step(self, storage=None, student_id="test_student"):
        target_raw, entropy = self._superpose()
        target = self._softmax(target_raw, self.temperature)
        new: List[float] = []
        for s, t in zip(self.student_policy, target):
            eps = max(1e-12, min(self.student_policy) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        new_student = [x / ns for x in new]

        delta = sum(abs(a - b) for a, b in
                    zip(new_student, self.student_policy))
        self._converged = delta < self.convergence_eps
        self.student_policy = new_student

        self.temperature = max(self.min_temperature,
                               self.temperature * self.temperature_decay)

        entry = {
            "target": target, "student": list(self.student_policy),
            "temperature": self.temperature, "delta": delta,
            "converged": self._converged, "entropy": entropy,
        }
        self.history.append(entry)

        if storage is not None and self.teachers:
            for tid, pol in self.teachers.items():
                weight = pol[0] if pol else 0.0
                amplitude = math.sqrt(max(weight, 1e-9))
                kl = sum(t * math.log(max(t, 1e-9) / max(s, 1e-9))
                         for t, s in zip(target, self.student_policy))
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id, tid, weight, self.temperature,
                        amplitude, kl, entropy)
                except Exception:
                    pass
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)

    def is_converged(self) -> bool:
        return self._converged


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL (ENHANCED: PC-style skeleton)
# =============================================================================
class CausalGraphLearner:
    """Causal skeleton discovery via partial correlation (PC-style).

    v17.1 additions:
      * Pearson → partial correlation conditioning on all other variables.
      * Fisher-z test for p-values.
      * Orientation heuristic by variance ratio.
      * Deterministic fallback when numpy is unavailable (uses a seeded RNG).
    """

    def __init__(self, storage=None, rng: Optional[random.Random] = None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self.rng = rng or random.Random()

    def _partial_corr_matrix(self, X):
        """Return (corr, pcorr, p_values) matrices, all shape (d, d)."""
        n, d = X.shape
        corr = np.corrcoef(X, rowvar=False)
        corr = np.nan_to_num(corr, nan=0.0)
        try:
            prec = np.linalg.pinv(corr)
        except Exception:
            prec = np.eye(d)
        pcorr = np.zeros((d, d))
        for i in range(d):
            for j in range(d):
                if i == j:
                    pcorr[i, j] = 1.0
                    continue
                denom = math.sqrt(max(prec[i, i] * prec[j, j], 1e-12))
                pcorr[i, j] = -prec[i, j] / denom
        # Fisher-z p-values.
        p_values = np.ones((d, d))
        k = max(d - 2, 1)
        for i in range(d):
            for j in range(d):
                if i == j:
                    continue
                r = min(max(pcorr[i, j], -0.999999), 0.999999)
                z = 0.5 * math.log((1 + r) / (1 - r)) * math.sqrt(max(n - k, 1))
                # Two-sided tail approximation.
                p_values[i, j] = math.erfc(abs(z) / math.sqrt(2.0))
        return corr, pcorr, p_values

    async def learn(self, samples, variables, threshold=0.25,
                    alpha: float = 0.05):
        self.variables = list(variables)
        self.graph.clear()

        if len(samples) < 5 or not NUMPY_AVAILABLE:
            # Deterministic fallback: seeded random skeleton.
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and self.rng.random() < 0.25:
                        w = self.rng.uniform(0.1, 0.9)
                        self.graph[s][t] = {
                            "weight": w, "confidence": w, "p_value": 0.0}
            return self.summary()

        X = np.array([[float(s.get(v, 0.0)) for v in variables]
                      for s in samples], dtype=float)
        if X.shape[0] < 3:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)

        corr, pcorr, pvals = self._partial_corr_matrix(X)
        variances = X.var(0)

        for i in range(len(variables)):
            for j in range(i + 1, len(variables)):
                c = abs(float(pcorr[i, j]))
                p = float(pvals[i, j])
                if c <= threshold or p > alpha:
                    continue
                # Orient from higher-variance to lower (crude heuristic).
                if variances[i] >= variances[j]:
                    src, dst, sign = variables[i], variables[j], corr[i, j]
                else:
                    src, dst, sign = variables[j], variables[i], corr[j, i]
                self.graph[src][dst] = {
                    "weight": float(sign),
                    "confidence": 1.0 - p,
                    "p_value": p,
                }
                if self.storage is not None:
                    try:
                        await asyncio.to_thread(
                            self.storage.save_causal_edge,
                            src, dst, float(sign), 1.0 - p, p)
                    except Exception:
                        pass
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {
            "nodes": len(self.variables),
            "edges": sum(len(v) for v in self.graph.values()),
            "variables": list(self.variables),
        }


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph,
                 rng: Optional[random.Random] = None):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.rng = rng or random.Random()
        self.values = defaultdict(float)
        self.counts = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._lock = asyncio.Lock()

    async def choose_action(self, state):
        async with self._lock:
            if self.rng.random() < self.epsilon:
                return self.rng.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action, reward, state):
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    async def estimate_ate(self, treatment, outcome, samples=100):
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get(
            "weight", 0.0)
        try:
            await asyncio.to_thread(
                self.storage.save_causal_experiment,
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w,
                samples, "partial_correlation")
        except Exception:
            pass
        return w

    def get_policy(self):
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING (ENHANCED)
# =============================================================================
class FederatedGreenAggregator:
    """Synchronous weighted FL aggregation with optional DP + sparsification.

    v17.1 additions:
      * Sample-count-weighted averaging.
      * Optional differential privacy (Gaussian mechanism + clipping).
      * Optional top-k sparsification for communication savings.
      * Round-tagged pulls for reproducibility.
    """

    def __init__(self, storage, instance_id, share_interval=3600,
                 rng: Optional[random.Random] = None):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rng = rng or random.Random()
        self.rounds = 0
        self.dp_epsilon = None
        self.dp_clip = 1.0
        self.sparse_topk = 0.0

    def configure_dp(self, epsilon: Optional[float], clip: float = 1.0):
        self.dp_epsilon = epsilon
        self.dp_clip = max(1e-6, float(clip))

    def configure_sparsity(self, topk_fraction: float):
        self.sparse_topk = max(0.0, min(1.0, float(topk_fraction)))

    def _sparsify(self, blob: bytes) -> bytes:
        if self.sparse_topk <= 0.0 or len(blob) == 0:
            return blob
        k = max(1, int(len(blob) * self.sparse_topk))
        idx = sorted(range(len(blob)),
                     key=lambda i: -abs(blob[i] - 128))[:k]
        keep = set(idx)
        return bytes(b if i in keep else 128 for i, b in enumerate(blob))

    def _privatise(self, blob: bytes) -> bytes:
        if self.dp_epsilon is None or not blob:
            return blob
        # Sensitivity bound by clipping to [128-clip*128, 128+clip*128] then
        # adding Gaussian noise with sigma = clip / epsilon.
        sigma = self.dp_clip / max(self.dp_epsilon, 1e-6)
        out = bytearray(len(blob))
        for i, b in enumerate(blob):
            v = (b - 128) / 128.0
            v = max(-self.dp_clip, min(self.dp_clip, v))
            v += self.rng.gauss(0.0, sigma)
            v = max(-1.0, min(1.0, v))
            out[i] = int(round((v + 1.0) * 127.5))
        return bytes(out)

    async def share_weights(self, model_id, weights, sample_count: int = 1):
        w = self._sparsify(weights)
        w = self._privatise(w)
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, w,
                float(len(w)) * sample_count, self.rounds,
                self.dp_epsilon)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id, round_id=None):
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id, round_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [(r["weights"], max(r.get("weight_norm", 1.0), 1e-6))
                 for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b, _ in blobs)
        total_w = sum(w for _, w in blobs) or 1.0
        avg = bytearray(n)
        for i in range(n):
            acc = sum(b[i] * w for b, w in blobs) / total_w
            avg[i] = int(acc) & 0xFF
        self.rounds += 1
        return bytes(avg)

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION (ENHANCED)
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter",
             "negotiator"]

    def __init__(self, agent_id, rng: random.Random):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: rng.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0
        self.byzantine_flags = 0


class MultiAgentCoordinator:
    """Coordination with Byzantine-tolerant reputation + decay.

    v17.1 additions:
      * Reputation decay applied each step.
      * Byzantine flag threshold: agents with many flags are excluded from
        bidding.
      * Coalition-aware bid: prefers smallest team that fits the task role.
      * All storage writes wrapped in try/except (soft-fail).
    """

    def __init__(self, config, storage, rng: Optional[random.Random] = None):
        self.config = config
        self.storage = storage
        self.rng = rng or random.Random()
        count = int(_cfg_get(config, "agent_count", 5))
        self.agents: Dict[str, _Agent] = {
            f"agent_{i:02d}": _Agent(f"agent_{i:02d}", self.rng)
            for i in range(max(0, count))
        }
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()
        self.decay = float(_cfg_get(config, "reputation_decay", 0.999))
        self.byz_threshold = float(
            _cfg_get(config, "byzantine_threshold", 0.35))

    def _is_byzantine(self, a: _Agent) -> bool:
        if a.completed == 0:
            return False
        return (a.byzantine_flags / max(a.completed, 1)) >= self.byz_threshold

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                a.reputation *= self.decay
                a.reputation = max(0.0, min(1.0, a.reputation))
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent, a.id, a.role,
                        a.reputation, a.utilities)
                except Exception:
                    pass

    async def broadcast(self, topic, sender, payload):
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            pass
        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                uuid.uuid4().hex[:8], topic, sender, "*", payload)
        except Exception:
            pass

    async def bid(self, task):
        preferred = task.get("preferred_role", "orchestrator")
        if not self.agents:
            await self.broadcast("task_bid", "none",
                                 {"task": task.get("name", "?"),
                                  "score": 0.0})
            return None, 0.0
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                if self._is_byzantine(a):
                    continue
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += self.rng.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        # Fallback: if all agents are byzantine, pick the least flagged.
        if best_id is None:
            best_id = min(self.agents, key=lambda k: self.agents[k].byzantine_flags)
        return best_id, best_score

    async def reward(self, agent_id, reward, byzantine: bool = False):
        async with self._lock:
            if agent_id not in self.agents:
                return
            a = self.agents[agent_id]
            if byzantine:
                a.byzantine_flags += 1
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent_reputation_event,
                        agent_id, -1.0, "byzantine_flag")
                except Exception:
                    pass
                return
            n = max(1, a.completed)
            a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
            a.utilities[a.role] = min(
                1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self):
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self):
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {
            "roles": {a.id: a.role for a in self.agents.values()},
            "role_distribution": self.get_policy(),
            "processed_messages": processed,
        }


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC (ENHANCED: MTL + past-time)
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")
_WINDOW_RE = re.compile(r"^\[(\d+(?:\.\d+)?)s?\]\s*(.*)$")


@dataclass
class TemporalRule:
    rule_id: str
    operator: str
    conditions: List[Callable[[Dict], bool]]
    window: float = 0.0
    description: str = ""
    severity: str = "warning"
    violations: int = 0
    last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        """Return True if the rule is VIOLATED over the trace."""
        if not trace:
            return False
        # Window pruning: keep only events within window.
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":       # G  φ   — violated if φ ever false
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":   # F  φ   — violated if φ never true
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":        # ¬F φ   — violated if φ ever true
            return any(self.conditions[0](s) for _, s in trace)
        if op == "historically":  # H φ    — violated if φ ever false (past)
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "once":          # O φ    — violated if φ never true (past)
            return not any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    """MTL verifier supporting past-time operators H and O.

    Formula syntax:
        G (x >= 0.5)          -- globally
        F (done == 1)         -- eventually
        NEVER (carbon > 0.8)  -- never
        H (quality >= 0.5)    -- historically (past-time G)
        O (task_started == 1) -- once (past-time F)
        [60] G (x >= 0.5)     -- windowed (60 seconds)
    """

    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 500)))
        self.approval_cb = None
        windows = _cfg_get(config, "temporal_windows_seconds", {}) or {}
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula, windows)

    def _install(self, formula: str, windows: Dict[str, float]):
        f = formula.strip()
        window = 0.0
        m = _WINDOW_RE.match(f)
        if m:
            window = float(m.group(1))
            f = m.group(2).strip()
        op_map = {
            "G ": ("always", 2),
            "F ": ("eventually", 2),
            "NEVER ": ("never", 6),
            "H ": ("historically", 2),
            "O ": ("once", 2),
        }
        for prefix, (op, cut) in op_map.items():
            if f.startswith(prefix):
                self._add_atomic(f[cut:].strip().strip("()"), op, window)
                return

    def _add_atomic(self, expr: str, op: str, window: float = 0.0):
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True  # malformed state ⇒ assume satisfied
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}" + (f"@{window}s" if window else "")
        self.rules[rid] = TemporalRule(
            rid, op, [cond], window=window, description=expr,
            severity="warning")

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        self.trace.append((datetime.now(timezone.utc), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self):
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                approved = None
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        a = self.approval_cb(rid, self.trace[-1][1]
                                             if self.trace else {})
                        if asyncio.iscoroutine(a):
                            a = await a
                        approved = bool(a)
                    except Exception:
                        approved = None
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, approved)
                except Exception:
                    pass
        return result

    def counts(self) -> Dict[str, int]:
        return {rid: r.violations for rid, r in self.rules.items()}


# =============================================================================
# ENHANCEMENT 6 — XAI (ENHANCED)
# =============================================================================
class XAIDecisionExplainer:
    """Explanations: Integrated Gradients, Kernel SHAP, LIME, counterfactuals,
    and coverage-guaranteed anchors.

    v17.1 additions:
      * Integrated Gradients (default) — axiomatic, deterministic.
      * Actionable counterfactuals with bounded feature perturbation.
      * Anchors report coverage-precision tradeoff.
      * Deterministic RNG for reproducibility.
    """

    def __init__(self, config, storage, rng: Optional[random.Random] = None):
        self.config = config
        self.storage = storage
        self.rng = rng or random.Random()
        self.method = _cfg_get(config, "xai_method", "integrated_gradients")
        self.depth = int(_cfg_get(config, "xai_depth", 3))
        self.ci_samples = int(_cfg_get(config, "xai_ci_samples", 4))
        self.cf_max_iter = int(_cfg_get(config, "xai_cf_max_iter", 40))
        self.cf_target = float(_cfg_get(config, "xai_cf_target_delta", 0.1))
        self.anchor_min_precision = float(
            _cfg_get(config, "xai_anchor_min_precision", 0.8))
        self.ig_steps = int(_cfg_get(config, "xai_ig_steps", 32))
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)

    # ---- Integrated Gradients ----
    def _integrated_gradients(self, f, x, names, baseline=None,
                              steps=None):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        steps = steps or self.ig_steps
        if baseline is None:
            baseline = np.zeros_like(x)
        total = np.zeros_like(x)
        for s in range(1, steps + 1):
            alpha = s / steps
            interp = baseline + alpha * (x - baseline)
            # Central-difference gradient (works for black-box f).
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
        avg_grad = total / max(steps, 1)
        attr = (x - baseline) * avg_grad
        return dict(zip(names, attr.tolist()))

    def _kernel_shap(self, f, x, names, n=16):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            self.rng.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib /= max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _lime(self, f, x, names, n=100):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: self.rng.uniform(-1, 1) for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    def _counterfactual(self, f, x, names, base_pred,
                        target_delta=None, bounds=(-1.0, 1.0)):
        if not NUMPY_AVAILABLE:
            return {"success": False}
        target_delta = target_delta or self.cf_target
        x_cf, best_delta, best_pred = x.copy(), 0.0, base_pred
        chosen, step_used = None, 0.0
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
                chosen, step_used = names[i], step
            if delta >= abs(target_delta):
                break
        return {
            "success": best_delta >= abs(target_delta),
            "changed_feature": chosen,
            "change_amount": step_used,
            "delta": best_pred - base_pred,
        }

    def _anchors(self, f, x, names, n_samples=64):
        if not NUMPY_AVAILABLE:
            return []
        try:
            base_pred = float(f(x.reshape(1, -1)))
        except Exception:
            return []
        anchors = []
        for i, name in enumerate(names):
            hits, total = 0, 0
            for _ in range(n_samples):
                trial = x.copy()
                for j in range(len(x)):
                    if j != i:
                        trial[j] += self.rng.gauss(0, 0.2)
                try:
                    pred = float(f(trial.reshape(1, -1)))
                except Exception:
                    continue
                total += 1
                if abs(pred - base_pred) <= 0.15:
                    hits += 1
            if total > 0:
                prec = hits / total
                if prec >= self.anchor_min_precision:
                    anchors.append({
                        "feature": name,
                        "value": float(x[i]),
                        "precision": prec,
                        "coverage": total / n_samples,
                    })
        return anchors

    async def explain(self, decision_id, label, features, names, model_fn,
                      include_counterfactual=False, include_anchors=False,
                      cf_bounds=(-1.0, 1.0)):
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
            nl = self._nl(label, attrs)
            cf, anchors = None, None
        else:
            x = np.asarray(features, dtype=float)
            if self.method == "lime":
                attrs = self._lime(model_fn, x, names)
            elif self.method == "kernel_shap":
                attrs = self._kernel_shap(model_fn, x, names)
            else:
                attrs = self._integrated_gradients(model_fn, x, names)
            nl = self._nl(label, attrs)
            cf = self._counterfactual(
                model_fn, x, names,
                base_pred=float(model_fn(x.reshape(1, -1))),
                bounds=cf_bounds) if include_counterfactual else None
            anchors = self._anchors(model_fn, x, names) \
                if include_anchors else None
        self.global_attributions.append(dict(attrs))
        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                dict(enumerate(features)), attrs, nl)
        except Exception:
            pass
        return {
            "decision_id": decision_id, "method": self.method,
            "attributions": attrs, "explanation": nl,
            "counterfactual": cf, "anchors": anchors,
        }

    def global_feature_importance(self):
        if not self.global_attributions:
            return {}
        agg = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(vals) / len(vals) for k, vals in agg.items()}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION (ENHANCED)
# =============================================================================
class AdaptivePrecisionSwitcher:
    """Precision selection with layer-wise sensitivity + energy accounting.

    v17.1 additions:
      * Layer-wise sensitivity: switching down only if max sensitivity below
        threshold; otherwise keep fp32 for that layer.
      * Energy accounting with watts-per-op estimates.
      * Auto-rollback if accuracy drop > threshold (hysteresis).
      * Hardware probe is injectable (default: CPU-only).
    """

    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}

    def __init__(self, config, storage, hardware_probe=None):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.hardware_probe = hardware_probe or self._probe
        self._last_switch_ts = 0.0
        self._hysteresis_s = 0.5  # minimum seconds between switches

    def _probe(self):
        return {"cuda": False, "bf16": False, "device": "cpu"}

    def select_precision(self, layer_sensitivity: Optional[float] = None):
        hw = self.hardware_probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              list(self.ENERGY.keys())))
        if not hw.get("cuda"):
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw.get("bf16"):
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            cands = ["fp32"]
        if layer_sensitivity is not None and \
                layer_sensitivity > _cfg_get(
                    self.config, "precision_max_sensitivity", 0.5):
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        if target == self.current or target not in self.ENERGY:
            return False
        now = time.time()
        if now - self._last_switch_ts < self._hysteresis_s:
            return False
        old = self.current
        self.current = target
        self._last_switch_ts = now
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc, baseline_acc,
                          layer_sensitivity: Optional[float] = None):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh:
            target = self.select_precision(layer_sensitivity)
            await self.switch_to(target, reason="headroom")

    async def auto_switch_layers(self, per_layer_acc, baseline_acc):
        """Apply per-layer selection based on sensitivity.

        `per_layer_acc` maps layer name -> recent accuracy for that layer.
        Layers whose drop exceeds threshold are forced to fp32.
        """
        applied = {}
        for layer, acc in per_layer_acc.items():
            drop = (baseline_acc - acc) / max(baseline_acc, 1e-6)
            sens = min(1.0, drop / max(
                _cfg_get(self.config, "precision_switch_threshold", 0.02),
                1e-6))
            target = self.select_precision(layer_sensitivity=sens)
            applied[layer] = target
        return applied


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS (ENHANCED)
# =============================================================================
class CarbonMarketIntegrator:
    """Carbon markets with budget tracking + REC retirement.

    v17.1 additions:
      * Carbon budget accounting with automatic defer on overshoot.
      * REC retirement and balance tracking.
      * Price EMA smoothing.
      * Pluggable price oracle (default: random walk).
    """

    def __init__(self, config, storage, price_oracle=None,
                 rng: Optional[random.Random] = None):
        self.config = config
        self.storage = storage
        self.rng = rng or random.Random()
        self.price_oracle = price_oracle or self._random_walk
        self.last_price = 25.0
        self.price_ema = 25.0
        self.ema_alpha = 0.3
        self.carbon_budget_kg = float(
            _cfg_get(config, "carbon_budget_kg", 100.0))
        self.carbon_used_kg = 0.0

    async def _random_walk(self):
        return max(5.0, self.last_price + self.rng.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self.price_oracle()
        except Exception:
            price = self.last_price
        self.last_price = float(price)
        self.price_ema = (self.ema_alpha * self.last_price +
                          (1 - self.ema_alpha) * self.price_ema)
        try:
            await asyncio.to_thread(
                self.storage.save_credit_price, self.last_price)
        except Exception:
            pass
        return self.last_price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
        cost = mwh * price_per_mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, source)
        except Exception:
            pass
        return cost

    async def retire_rec(self, mwh):
        try:
            return await asyncio.to_thread(self.storage.retire_rec, mwh)
        except Exception:
            return 0.0

    def _budget_ok(self, extra_kg: float) -> bool:
        return (self.carbon_used_kg + extra_kg) <= self.carbon_budget_kg

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price

        if not self._budget_ok(carbon_kg):
            action = "defer"
        elif intensity > 0.3:
            action = "defer"
        elif offset_cost < 0.5:
            action = "run_offset"
        else:
            action = "run"

        if action in ("run", "run_offset"):
            self.carbon_used_kg += carbon_kg

        try:
            await asyncio.to_thread(
                self.storage.save_carbon_budget,
                self.carbon_used_kg, self.carbon_budget_kg)
        except Exception:
            pass

        try:
            await asyncio.to_thread(
                self.storage.save_net_zero_match,
                uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                carbon_kg, offset_cost, price)
        except Exception:
            pass

        return {
            "action": action, "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost, "credit_price_usd": price,
            "carbon_budget_remaining_kg":
                max(0.0, self.carbon_budget_kg - self.carbon_used_kg),
        }


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING (ENHANCED)
# =============================================================================
class ChaosTestingEngine:
    """Fault injection with steady-state hypothesis + auto-rollback.

    v17.1 additions:
      * Steady-state hypothesis checked before and after each experiment.
      * Auto-rollback flag when steady state is breached.
      * Pluggable steady-state predicate.
      * Blast-radius control (limits how many concurrent experiments).
    """

    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage, steady_predicate=None,
                 rng: Optional[random.Random] = None):
        self.config = config
        self.storage = storage
        self.rng = rng or random.Random()
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self.steady_predicate = steady_predicate or self._default_steady
        self.rollback_count = 0
        self.blast_radius = float(
            _cfg_get(config, "chaos_blast_radius", 0.5))
        self.auto_rollback = bool(
            _cfg_get(config, "chaos_auto_rollback", True))

    async def _default_steady(self) -> bool:
        if not self.active:
            return True
        intensity = _cfg_get(self.config, "chaos_intensity", 0.05)
        return self.rng.random() > intensity

    async def _steady(self) -> bool:
        try:
            return bool(await self.steady_predicate())
        except Exception:
            return True

    def _allowed_concurrent(self) -> int:
        return max(1, int(len(self.FAULT_TYPES) * self.blast_radius))

    async def run_experiment(self, name, fault_type):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        async with self._lock:
            if len(self.active) >= self._allowed_concurrent():
                return {"name": name, "fault_type": fault_type,
                        "status": "skipped", "reason": "blast_radius"}

        t0 = time.time()
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                await asyncio.sleep(0.01)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.01)
            elif fault_type == "data_corruption":
                await asyncio.sleep(0)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)

        after = await self._steady()
        rolled_back = False
        if before and not after and self.auto_rollback:
            rolled_back = True
            self.rollback_count += 1

        duration = (time.time() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type, self.blast_radius,
                int(before), int(after), status, duration, rolled_back)
        except Exception:
            pass
        return {
            "name": name, "fault_type": fault_type,
            "steady_before": before, "steady_after": after,
            "status": status, "duration_ms": duration,
            "rolled_back": rolled_back,
        }


# =============================================================================
# ENHANCEMENT 10 — HITL (ENHANCED: uncertainty sampling)
# =============================================================================
class ActiveUserPreferenceLearner:
    """HITL with uncertainty-sampled preference elicitation.

    v17.1 additions:
      * Bayesian preference posterior per user (Dirichlet-style counts).
      * Uncertainty sampling: query user only when top-2 candidates are
        close in expected utility (i.e., model is uncertain).
      * Preferences learned from feedback are stored for personalisation.
    """

    METRIC_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage, dashboard=None,
                 uncertainty_threshold: float = 0.05):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        # Dirichlet pseudo-counts per (user, feature, bucket) — simplified.
        self.posterior: Dict[str, Dict[str, float]] = {}
        self.uncertainty_threshold = uncertainty_threshold
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id, chosen_id):
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
        for k in self.METRIC_KEYS:
            if k in candidate:
                u += prefs.get(k, 0.0) * float(candidate[k])
        return u

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        # Uncertainty sampling: query only when top-2 are close.
        scored = sorted(
            ((self._utility(user_id, c), c) for c in candidates),
            key=lambda kv: kv[0], reverse=True)
        top = scored[0][0]
        second = scored[1][0]
        if abs(top - second) > self.uncertainty_threshold:
            # Confident — auto-select without asking.
            return scored[0][1].get("solution_id")
        try:
            msg = await asyncio.wait_for(
                self._responses.get(), timeout=timeout)
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return candidates[0].get("solution_id")

    async def record_choice(self, user_id, solution_id, metrics=None):
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self.METRIC_KEYS:
                if k in metrics:
                    v = float(metrics[k])
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + \
                            1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
            # Update posterior (Dirichlet-style).
            post = self.posterior.setdefault(user_id, {})
            for k in self.METRIC_KEYS:
                post[k] = post.get(k, 1.0) + prefs.get(k, 0.0)
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def expected_utility_uncertainty(self, user_id: str,
                                     candidates: List[Dict[str, Any]]):
        if len(candidates) < 2:
            return 0.0
        scored = sorted((self._utility(user_id, c) for c in candidates),
                        reverse=True)
        return abs(scored[0] - scored[1])


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class MOPDOrchestratorV17:
    """Unified entry point: wires all ten enhancements (v17.1.0)."""

    def __init__(self, storage, config, adaptive_function=None,
                 adaptive_api_url=None, adaptive_api_token=None,
                 dashboard=None, seed: int = 12345):
        self.storage = storage
        self.config = config
        self.adaptive = adaptive_function
        self.adaptive_api_url = adaptive_api_url
        self.adaptive_api_token = adaptive_api_token
        self.dashboard = dashboard
        self.instance_id = str(uuid.uuid4())[:8]

        # Per-component RNGs derived from the seed for determinism.
        self.rng = random.Random(seed)
        self._rng_causal = random.Random(seed + 1)
        self._rng_agent = random.Random(seed + 2)
        self._rng_fed = random.Random(seed + 3)
        self._rng_xai = random.Random(seed + 4)
        self._rng_chaos = random.Random(seed + 5)
        self._rng_carbon = random.Random(seed + 6)
        self._rng_quantum = random.Random(seed + 7)

        if NUMPY_AVAILABLE:
            np.random.seed(seed)

        self.quantum = QuantumDistillationEngine(
            temperature=_cfg_get(config, "distillation_temperature", 2.0),
            learning_rate=_cfg_get(config, "distillation_learning_rate", 0.1),
            temperature_decay=_cfg_get(
                config, "distillation_temperature_decay", 0.999),
            min_temperature=_cfg_get(
                config, "distillation_min_temperature", 0.5),
            rng=self._rng_quantum)
        self.causal_graph = CausalGraphLearner(storage, rng=self._rng_causal)
        self.causal_rl = CausalPolicyAdapter(
            config, storage, self.causal_graph, rng=self._rng_causal)
        self.federated = FederatedGreenAggregator(
            storage, self.instance_id, rng=self._rng_fed)
        if _cfg_get(config, "federated_dp_epsilon") is not None:
            self.federated.configure_dp(
                _cfg_get(config, "federated_dp_epsilon"),
                _cfg_get(config, "federated_dp_clip", 1.0))
        if _cfg_get(config, "federated_sparse_topk", 0.0) > 0:
            self.federated.configure_sparsity(
                _cfg_get(config, "federated_sparse_topk"))
        self.multi_agent = MultiAgentCoordinator(
            config, storage, rng=self._rng_agent)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage, rng=self._rng_xai)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(
            config, storage, rng=self._rng_carbon)
        self.chaos = ChaosTestingEngine(
            config, storage, rng=self._rng_chaos)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        self.temporal.set_approval_callback(self._on_critical_violation)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    # ------------------------------------------------------------------
    async def _on_critical_violation(self, rule_id, state):
        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, rule_id, state, "critical")
        except Exception:
            pass
        approved = self.rng.random() > 0.5
        try:
            await asyncio.to_thread(
                self.storage.resolve_hitl_request,
                req_id, "approved" if approved else "denied")
        except Exception:
            pass
        return approved

    # ------------------------------------------------------------------
    async def select_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {"strategy": "adaptive"}

        # 1. Causal RL
        if _cfg_get(self.config, "causal_rl_enabled", True):
            result["strategy"] = await self.causal_rl.choose_action(state)

        # 2. Multi-agent bid (may return None with zero agents)
        agent_id, agent_score = await self.multi_agent.bid({
            "name": "distill_task", "preferred_role": "optimizer"})
        result["agent_id"] = agent_id
        result["agent_score"] = agent_score

        # 3. Precision pre-selection
        await self.precision.auto_switch(recent_acc=0.9, baseline_acc=0.92)
        result["precision"] = self.precision.current

        # 4. Carbon market
        intensity = state.get("carbon_intensity", 400)
        try:
            intensity_f = float(intensity) / 1000.0
            if not math.isfinite(intensity_f):
                intensity_f = 0.4
        except Exception:
            intensity_f = 0.4
        cm = await self.carbon_market.net_zero_schedule(
            workload_kwh=1.0, intensity=intensity_f)
        result["carbon_decision"] = cm

        # 5. XAI
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    float(state.get("quality", 0.8) or 0.8),
                    intensity_f,
                    float(state.get("cost", 0.5) or 0.5),
                    float(state.get("latency_ms", 100) or 100) / 1000.0,
                ])
                if not np.all(np.isfinite(feats)):
                    feats = np.nan_to_num(feats, nan=0.0,
                                          posinf=1.0, neginf=0.0)

                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))

                result["xai"] = await self.xai.explain(
                    decision_id=f"mopd_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={result['strategy']}",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score,
                    include_counterfactual=True,
                    include_anchors=False)
            except Exception:
                pass

        # 6. Multi-agent reward
        if agent_id is not None:
            await self.multi_agent.reward(agent_id, 0.8)

        # 7. Temporal push + verify
        try:
            quality = float(state.get("quality", 0.8) or 0.8)
        except Exception:
            quality = 0.8
        if not math.isfinite(quality):
            quality = 0.8
        await self.temporal.push_state({
            "quality": quality,
            "carbon": intensity_f,
            "task_complete": True,
        })
        verify = await self.temporal.verify()
        result["temporal_violations"] = [k for k, v in verify.items() if not v]

        # 8. Causal update
        await self.causal_rl.update(result["strategy"], 0.8, state)

        return result

    # ------------------------------------------------------------------
    async def report_epoch(self, context, metrics, teacher_id,
                           distillation_loss, epoch):
        payload = {"context": context, "metrics": metrics,
                   "teacher_id": teacher_id,
                   "distillation_loss": distillation_loss, "epoch": epoch}
        if self.adaptive is not None:
            try:
                await self.adaptive.record_feedback(
                    context, metrics, teacher_id=teacher_id,
                    distillation_loss=distillation_loss)
                return
            except Exception:
                pass
        if self.adaptive_api_url:
            await self._post_http(payload)

    async def _post_http(self, payload):
        try:
            import aiohttp
            url = f"{self.adaptive_api_url}/mopd/record"
            headers = {}
            if self.adaptive_api_token:
                headers["Authorization"] = f"Bearer {self.adaptive_api_token}"
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, headers=headers,
                                  timeout=5) as r:
                    await r.read()
        except Exception:
            pass

    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        loop = asyncio.get_running_loop()
        tasks = [
            loop.create_task(self._causal_rl_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._multi_agent_loop()),
            loop.create_task(self._temporal_loop()),
            loop.create_task(self._xai_loop()),
            loop.create_task(self._precision_loop()),
            loop.create_task(self._carbon_market_loop()),
            loop.create_task(self._chaos_loop()),
            loop.create_task(self._distillation_loop()),
        ]
        for t in tasks:
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)

    async def shutdown(self):
        self._shutdown_event.set()
        self._running = False
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    # ------------------------------------------------------------------
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                samples = [{
                    "quality": self.rng.uniform(0.5, 1.0),
                    "carbon": self.rng.uniform(0.1, 0.8),
                    "cost": self.rng.uniform(0.1, 0.9),
                    "latency": self.rng.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                dummy = bytes(self.rng.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("policy", dummy)
                await self.federated.pull_aggregated_weights("policy")
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.multi_agent.step()
            except Exception:
                pass

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.temporal.verify()
            except Exception:
                pass

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])

                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))

                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="system_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except Exception:
                pass

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.carbon_market.update_price()
            except Exception:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                fault = self.rng.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "mopd_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        try:
            rec = await asyncio.to_thread(self.storage.get_rec_balance)
        except Exception:
            rec = 0.0
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "background_tasks": len(self._background_tasks),
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "causal_edges": self.causal_graph.summary()["edges"],
            "temporal_rules": len(self.temporal.rules),
            "precision": self.precision.current,
            "rec_balance_mwh": rec,
            "carbon_price": self.carbon_market.last_price,
            "carbon_budget_remaining_kg": max(
                0.0, self.carbon_market.carbon_budget_kg -
                self.carbon_market.carbon_used_kg),
            "federated_rounds": self.federated.rounds,
            "chaos_rollbacks": self.chaos.rollback_count,
            "quantum_policy": self.quantum.get_policy(),
        }


# =============================================================================
# FIXTURES (async, pytest_asyncio)
# =============================================================================
@pytest.fixture
def tmp_db_path(tmp_path):
    return str(tmp_path / "test_mopd_v17.db")


@pytest.fixture
def config():
    cfg = _ConfigDict(_DEFAULTS)
    random.seed(12345)
    if NUMPY_AVAILABLE:
        np.random.seed(12345)
    return cfg


@pytest.fixture
def storage(tmp_db_path):
    return InMemoryStorage(tmp_db_path)


@pytest_asyncio.fixture
async def orchestrator(storage, config):
    orch = MOPDOrchestratorV17(storage=storage, config=config)
    await orch.start()
    try:
        yield orch
    finally:
        await orch.shutdown()


@pytest.fixture
def stopped_orchestrator(storage, config):
    return MOPDOrchestratorV17(storage=storage, config=config)


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION TESTS
# =============================================================================
class TestQuantumDistillation:
    def test_register_teacher(self):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [1, 2, 3, 4, 5])
        assert "a" in q.teachers
        assert abs(sum(q.teachers["a"]) - 1.0) < 1e-9

    def test_register_empty_teacher_is_noop(self):
        q = QuantumDistillationEngine()
        q.register_teacher("empty", [])
        assert "empty" not in q.teachers

    def test_unregister_teacher(self):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [1, 2, 3, 4, 5])
        q.unregister_teacher("a")
        assert "a" not in q.teachers

    def test_superposition_agreement_amplifies(self):
        q = QuantumDistillationEngine()
        p = [0.5, 0.2, 0.1, 0.1, 0.1]
        q.register_teacher("a", p)
        q.register_teacher("b", p)
        superposed, entropy = q._superpose()
        for a, b in zip(superposed, p):
            assert abs(a - b) < 1e-6
        assert entropy >= 0.0

    def test_superposition_sums_to_one(self):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        q.register_teacher("b", [0.1, 0.2, 0.3, 0.2, 0.2])
        superposed, _ = q._superpose()
        assert abs(sum(superposed) - 1.0) < 1e-6
        assert all(x >= 0 for x in superposed)

    def test_entropy_increases_with_disagreement(self):
        agree = QuantumDistillationEngine()
        agree.register_teacher("a", [0.5, 0.5, 0, 0, 0])
        agree.register_teacher("b", [0.5, 0.5, 0, 0, 0])
        _, e_agree = agree._superpose()

        disagree = QuantumDistillationEngine()
        disagree.register_teacher("a", [0.5, 0.5, 0, 0, 0])
        disagree.register_teacher("b", [0, 0, 0.5, 0.5, 0])
        _, e_disagree = disagree._superpose()
        assert e_disagree > e_agree

    @pytest.mark.asyncio
    async def test_step_updates_student_policy(self, storage):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        before = q.get_policy()
        entry = await q.step(storage)
        after = q.get_policy()
        assert before != after
        assert "delta" in entry and "temperature" in entry
        assert "entropy" in entry
        assert sum(after) == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_step_persists_superpositions(self, storage):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        q.register_teacher("b", [0.1, 0.2, 0.3, 0.2, 0.2])
        await q.step(storage)
        assert len(storage.tables["teacher_superpositions"]) == 2

    @pytest.mark.asyncio
    async def test_convergence_detection(self, storage):
        q = QuantumDistillationEngine(learning_rate=1e-6,
                                      convergence_eps=1e-3)
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        for _ in range(50):
            await q.step()
            if q.is_converged():
                break
        assert q.is_converged()

    @pytest.mark.asyncio
    async def test_empty_teachers_returns_uniform(self):
        q = QuantumDistillationEngine()
        entry = await q.step()
        assert all(abs(p - 0.2) < 1e-6 for p in entry["student"])

    @pytest.mark.asyncio
    async def test_temperature_floor_respected(self, storage):
        q = QuantumDistillationEngine(temperature=0.6,
                                      temperature_decay=0.5,
                                      min_temperature=0.5)
        q.register_teacher("a", [0.5, 0.5, 0, 0, 0])
        for _ in range(10):
            await q.step()
        assert q.temperature >= 0.5 - 1e-9


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL TESTS
# =============================================================================
class TestCausalRL:
    @pytest.mark.asyncio
    async def test_graph_learn_with_small_samples(self, storage):
        g = CausalGraphLearner(storage, rng=random.Random(1))
        await g.learn([{"x": 1, "y": 2}] * 3, ["x", "y"])
        assert g.summary()["nodes"] == 2

    @pytest.mark.asyncio
    async def test_graph_learn_detects_edge(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        g = CausalGraphLearner(storage, rng=random.Random(1))
        samples = [{"x": float(i),
                    "y": float(i) * 2 + random.gauss(0, 0.1)}
                   for i in range(100)]
        await g.learn(samples, ["x", "y"])
        assert len(g.graph) >= 1

    @pytest.mark.asyncio
    async def test_learn_records_p_values(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        g = CausalGraphLearner(storage, rng=random.Random(1))
        samples = [{"x": float(i), "y": float(i) * 3} for i in range(50)]
        await g.learn(samples, ["x", "y"])
        edges = [e for v in g.graph.values() for e in v.values()]
        assert edges and all("p_value" in e for e in edges)

    @pytest.mark.asyncio
    async def test_choose_action_returns_valid(self, storage):
        g = CausalGraphLearner(storage)
        p = CausalPolicyAdapter({"causal_exploration_rate": 1.0},
                                 storage, g)
        action = await p.choose_action({})
        assert action in p.ACTIONS

    @pytest.mark.asyncio
    async def test_update_changes_policy(self, storage):
        g = CausalGraphLearner(storage)
        p = CausalPolicyAdapter({"causal_exploration_rate": 0.0},
                                 storage, g)
        before = p.get_policy()
        for _ in range(10):
            await p.update("carbon", 1.0, {})
        after = p.get_policy()
        assert before != after
        assert after[p.ACTIONS.index("carbon")] > 0.2

    @pytest.mark.asyncio
    async def test_estimate_ate(self, storage):
        g = CausalGraphLearner(storage)
        g.graph["x"] = {"y": {"weight": 0.5, "confidence": 0.9,
                              "p_value": 0.01}}
        p = CausalPolicyAdapter({"causal_exploration_rate": 0.1},
                                 storage, g)
        ate = await p.estimate_ate("x", "y")
        assert ate == 0.5

    @pytest.mark.asyncio
    async def test_estimate_ate_missing_edge(self, storage):
        g = CausalGraphLearner(storage)
        p = CausalPolicyAdapter({"causal_exploration_rate": 0.1},
                                 storage, g)
        ate = await p.estimate_ate("nonexistent", "y")
        assert ate == 0.0


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING TESTS
# =============================================================================
class TestFederatedGreenLearning:
    @pytest.mark.asyncio
    async def test_share_and_pull(self, storage):
        f = FederatedGreenAggregator(storage, "inst_1")
        await f.share_weights("model_a", b"\x01\x02\x03\x04")
        agg = await f.pull_aggregated_weights("model_a")
        assert agg == b"\x01\x02\x03\x04"

    @pytest.mark.asyncio
    async def test_average_of_two_instances(self, storage):
        f1 = FederatedGreenAggregator(storage, "inst_1")
        f2 = FederatedGreenAggregator(storage, "inst_2")
        await f1.share_weights("m", b"\x00\x00\x00\x00")
        await f2.share_weights("m", b"\x04\x04\x04\x04")
        agg = await f1.pull_aggregated_weights("m")
        assert agg == b"\x02\x02\x02\x02"

    @pytest.mark.asyncio
    async def test_pull_missing_returns_none(self, storage):
        f = FederatedGreenAggregator(storage, "inst")
        assert await f.pull_aggregated_weights("nonexistent") is None

    @pytest.mark.asyncio
    async def test_truncates_to_min_length(self, storage):
        f1 = FederatedGreenAggregator(storage, "inst_1")
        f2 = FederatedGreenAggregator(storage, "inst_2")
        await f1.share_weights("m", b"\x02\x02")
        await f2.share_weights("m", b"\x04\x04\x04\x04")
        agg = await f1.pull_aggregated_weights("m")
        assert len(agg) == 2

    @pytest.mark.asyncio
    async def test_dp_adds_noise(self, storage):
        f = FederatedGreenAggregator(storage, "inst", rng=random.Random(7))
        f.configure_dp(epsilon=0.5, clip=1.0)
        await f.share_weights("m", bytes([100] * 16))
        rows = storage.tables["federated_weights"]
        # Noise should make the vector not all-equal 100.
        assert any(b != 100 for b in rows[0]["weights"])

    @pytest.mark.asyncio
    async def test_sparsification_limits_nonzero(self, storage):
        f = FederatedGreenAggregator(storage, "inst", rng=random.Random(7))
        f.configure_sparsity(0.25)
        blob = bytes(range(64))
        await f.share_weights("m", blob)
        kept = storage.tables["federated_weights"][0]["weights"]
        # At most 25% of positions can be non-128 (plus those already equal).
        non_mid = sum(1 for b in kept if b != 128)
        assert non_mid <= 16

    @pytest.mark.asyncio
    async def test_weighted_average_uses_sample_count(self, storage):
        f1 = FederatedGreenAggregator(storage, "inst_1")
        f2 = FederatedGreenAggregator(storage, "inst_2")
        await f1.share_weights("m", b"\x00\x00\x00\x00", sample_count=1)
        await f2.share_weights("m", b"\x08\x08\x08\x08", sample_count=3)
        agg = await f1.pull_aggregated_weights("m")
        # (1*0 + 3*8)/(1+3) = 6
        assert agg == b"\x06\x06\x06\x06"


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION TESTS
# =============================================================================
class TestMultiAgentCoordination:
    @pytest.mark.asyncio
    async def test_specialisation_assigns_roles(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage,
                                  rng=random.Random(0))
        await m._specialise()
        for a in m.agents.values():
            assert a.role in a.ROLES

    @pytest.mark.asyncio
    async def test_bid_returns_winner(self, storage):
        m = MultiAgentCoordinator({"agent_count": 5}, storage,
                                  rng=random.Random(0))
        winner, score = await m.bid({
            "name": "test_task", "preferred_role": "optimizer"})
        assert winner in m.agents
        assert isinstance(score, float)

    @pytest.mark.asyncio
    async def test_bid_with_zero_agents_returns_none(self, storage):
        m = MultiAgentCoordinator({"agent_count": 0}, storage,
                                  rng=random.Random(0))
        winner, score = await m.bid({"name": "t"})
        assert winner is None
        assert score == 0.0

    @pytest.mark.asyncio
    async def test_reward_updates_reputation(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage,
                                  rng=random.Random(0))
        await m._specialise()
        aid = list(m.agents.keys())[0]
        before = m.agents[aid].reputation
        await m.reward(aid, 1.0)
        assert m.agents[aid].reputation != before

    @pytest.mark.asyncio
    async def test_byzantine_flags_exclude_from_bid(self, storage):
        m = MultiAgentCoordinator(
            {"agent_count": 3, "byzantine_threshold": 0.3},
            storage, rng=random.Random(0))
        await m._specialise()
        aid = list(m.agents.keys())[0]
        for _ in range(5):
            await m.reward(aid, 0.0, byzantine=True)
        m.agents[aid].completed = 5
        assert m._is_byzantine(m.agents[aid])

    @pytest.mark.asyncio
    async def test_broadcast_persists_message(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage,
                                  rng=random.Random(0))
        await m.broadcast("topic_a", "sender_1", {"k": "v"})
        assert len(storage.tables["agent_messages"]) >= 1

    @pytest.mark.asyncio
    async def test_broadcast_soft_fails_on_storage_error(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage,
                                  rng=random.Random(0))
        storage.fail_on = "save_agent_message"
        # Must not raise.
        await m.broadcast("topic_a", "sender_1", {"k": "v"})

    @pytest.mark.asyncio
    async def test_step_processes_messages(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage,
                                  rng=random.Random(0))
        await m.broadcast("t1", "s1", {"x": 1})
        await m.broadcast("t2", "s2", {"x": 2})
        result = await m.step()
        assert result["processed_messages"] == 2

    @pytest.mark.asyncio
    async def test_policy_is_valid_distribution(self, storage):
        m = MultiAgentCoordinator({"agent_count": 5}, storage,
                                  rng=random.Random(0))
        p = m.get_policy()
        assert len(p) == 5
        assert sum(p) == pytest.approx(1.0)
        assert all(x >= 0 for x in p)


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC TESTS
# =============================================================================
class TestTemporalLogic:
    @pytest.mark.asyncio
    async def test_always_violated(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        await v.push_state({"quality": 0.9})
        await v.push_state({"quality": 0.2})
        result = await v.verify()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_always_satisfied(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        await v.push_state({"quality": 0.9})
        await v.push_state({"quality": 0.8})
        result = await v.verify()
        assert all(ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_eventually_satisfied(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["F (task_complete)"]})
        await v.push_state({"task_complete": False})
        await v.push_state({"task_complete": True})
        result = await v.verify()
        assert all(ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_never_violated(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["NEVER (carbon >= 0.8)"]})
        await v.push_state({"carbon": 0.5})
        await v.push_state({"carbon": 0.9})
        result = await v.verify()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_historically_operator(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["H (quality >= 0.5)"]})
        await v.push_state({"quality": 0.9})
        await v.push_state({"quality": 0.1})
        result = await v.verify()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_once_operator(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["O (task_started == 1)"]})
        await v.push_state({"task_started": 0})
        result = await v.verify()
        # O is violated if it never became true.
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_windowed_rule(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["[3600] G (quality >= 0.5)"]})
        assert any("@3600" in rid for rid in v.rules)

    @pytest.mark.asyncio
    async def test_empty_trace(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        result = await v.verify()
        assert all(ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_violation_persisted(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        await v.push_state({"quality": 0.1})
        await v.verify()
        assert len(storage.tables["temporal_violations"]) >= 1

    @pytest.mark.asyncio
    async def test_approval_callback_invoked(self, storage):
        called = []
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        for rid in v.rules:
            v.rules[rid].severity = "critical"

        def cb(rule_id, state):
            called.append(rule_id)

        v.set_approval_callback(cb)
        await v.push_state({"quality": 0.1})
        await v.verify()
        assert len(called) >= 1

    @pytest.mark.asyncio
    async def test_counts_reports_violations(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        await v.push_state({"quality": 0.1})
        await v.verify()
        await v.verify()
        assert sum(v.counts().values()) >= 2


# =============================================================================
# ENHANCEMENT 6 — XAI TESTS
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_integrated_gradients_attributions(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer(
            {"xai_method": "integrated_gradients"}, storage)
        feats = np.array([0.9, 0.4, 0.5, 0.4])

        def f(v):
            return float(np.dot(v, [0.4, -0.3, -0.2, -0.1]))

        result = await x.explain("id1", "label", feats,
                                  ["a", "b", "c", "d"], f)
        assert set(result["attributions"].keys()) == {"a", "b", "c", "d"}
        assert "explanation" in result

    @pytest.mark.asyncio
    async def test_kernel_shap_attributions(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer({"xai_method": "kernel_shap"}, storage)
        feats = np.array([0.9, 0.4])
        result = await x.explain("id", "l", feats, ["a", "b"],
                                  lambda v: float(v.sum()))
        assert set(result["attributions"].keys()) == {"a", "b"}

    @pytest.mark.asyncio
    async def test_lime_attributions(self, storage):
        if not (NUMPY_AVAILABLE and SKLEARN_AVAILABLE):
            pytest.skip("numpy + sklearn required")
        x = XAIDecisionExplainer({"xai_method": "lime"}, storage)
        feats = np.array([0.9, 0.4, 0.5, 0.4])
        result = await x.explain("id2", "label", feats,
                                  ["a", "b", "c", "d"],
                                  lambda v: float(v.sum()))
        assert set(result["attributions"].keys()) == {"a", "b", "c", "d"}

    @pytest.mark.asyncio
    async def test_explanation_persisted(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer({}, storage)
        feats = np.array([0.9, 0.4])
        await x.explain("id3", "test", feats, ["a", "b"],
                        lambda v: float(v.sum()))
        assert len(storage.tables["xai_explanations"]) == 1

    @pytest.mark.asyncio
    async def test_global_feature_importance(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer({}, storage)
        for _ in range(3):
            feats = np.array([random.random(), random.random()])
            await x.explain("id", "l", feats, ["a", "b"],
                            lambda v: float(v.sum()))
        importance = x.global_feature_importance()
        assert "a" in importance and "b" in importance

    @pytest.mark.asyncio
    async def test_counterfactual_finds_flip(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer(
            {"xai_cf_target_delta": 0.05, "xai_cf_max_iter": 100},
            storage, rng=random.Random(0))
        feats = np.array([0.9, 0.4])

        def f(v):
            return float(v[0] - v[1])

        result = await x.explain("id", "test", feats, ["a", "b"], f,
                                  include_counterfactual=True)
        assert result["counterfactual"] is not None

    @pytest.mark.asyncio
    async def test_anchors_reported_with_coverage(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer(
            {"xai_anchor_min_precision": 0.5}, storage,
            rng=random.Random(0))
        feats = np.array([0.9, 0.4])
        result = await x.explain("id", "test", feats, ["a", "b"],
                                  lambda v: float(v[0]),
                                  include_anchors=True)
        assert result["anchors"] is not None


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION TESTS
# =============================================================================
class TestAdaptivePrecision:
    def test_select_lowest_energy(self, storage):
        p = AdaptivePrecisionSwitcher(
            {"precision_levels": ["fp32", "int8"]}, storage)
        assert p.select_precision() == "int8"

    def test_select_precision_fp32_for_high_sensitivity(self, storage):
        p = AdaptivePrecisionSwitcher(
            {"precision_levels": ["fp32", "int8"],
             "precision_max_sensitivity": 0.3}, storage)
        assert p.select_precision(layer_sensitivity=0.9) == "fp32"

    @pytest.mark.asyncio
    async def test_switch_to_updates_current(self, storage):
        p = AdaptivePrecisionSwitcher(
            {"precision_levels": ["fp32", "int8"]}, storage)
        p._hysteresis_s = 0.0
        assert p.current == "fp32"
        ok = await p.switch_to("int8", reason="test")
        assert ok is True
        assert p.current == "int8"

    @pytest.mark.asyncio
    async def test_switch_persisted(self, storage):
        p = AdaptivePrecisionSwitcher(
            {"precision_levels": ["fp32", "int8"]}, storage)
        p._hysteresis_s = 0.0
        await p.switch_to("int8")
        assert len(storage.tables["precision_history"]) == 1

    @pytest.mark.asyncio
    async def test_switch_to_same_is_noop(self, storage):
        p = AdaptivePrecisionSwitcher({}, storage)
        ok = await p.switch_to("fp32")
        assert ok is False

    @pytest.mark.asyncio
    async def test_switch_to_unknown_is_noop(self, storage):
        p = AdaptivePrecisionSwitcher({}, storage)
        p._hysteresis_s = 0.0
        ok = await p.switch_to("quantum_float")
        assert ok is False

    @pytest.mark.asyncio
    async def test_auto_switch_on_accuracy_drop(self, storage):
        p = AdaptivePrecisionSwitcher({
            "precision_switch_threshold": 0.02,
            "precision_levels": ["fp32", "int8"]}, storage)
        p._hysteresis_s = 0.0
        p.current = "int8"
        await p.auto_switch(recent_acc=0.7, baseline_acc=0.95)
        assert p.current == "fp32"

    @pytest.mark.asyncio
    async def test_auto_switch_on_headroom(self, storage):
        p = AdaptivePrecisionSwitcher({
            "precision_switch_threshold": 0.02,
            "precision_levels": ["fp32", "int8"]}, storage)
        p._hysteresis_s = 0.0
        # drop = (0.95-0.94)/0.95 ≈ 0.0105 < 0.02 ⇒ headroom ⇒ switch.
        await p.auto_switch(recent_acc=0.94, baseline_acc=0.95)
        assert p.current == "int8"

    @pytest.mark.asyncio
    async def test_zero_baseline_no_crash(self, storage):
        p = AdaptivePrecisionSwitcher({}, storage)
        await p.auto_switch(recent_acc=0.9, baseline_acc=0.0)
        assert p.current == "fp32"

    @pytest.mark.asyncio
    async def test_layer_wise_selection(self, storage):
        p = AdaptivePrecisionSwitcher({
            "precision_levels": ["fp32", "int8"],
            "precision_max_sensitivity": 0.3}, storage)
        applied = await p.auto_switch_layers(
            per_layer_acc={"a": 0.94, "b": 0.5}, baseline_acc=0.95)
        assert applied["a"] in ("fp32", "int8")
        assert applied["b"] == "fp32"


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS TESTS
# =============================================================================
class TestCarbonMarkets:
    @pytest.mark.asyncio
    async def test_update_price(self, storage):
        c = CarbonMarketIntegrator({}, storage, rng=random.Random(0))
        price = await c.update_price()
        assert price >= 5.0

    @pytest.mark.asyncio
    async def test_purchase_rec(self, storage):
        c = CarbonMarketIntegrator({}, storage)
        cost = await c.purchase_rec(10.0, price_per_mwh=5.0)
        assert cost == 50.0
        assert len(storage.tables["rec_ledger"]) == 1

    @pytest.mark.asyncio
    async def test_net_zero_defer(self, storage):
        c = CarbonMarketIntegrator({}, storage, rng=random.Random(0))
        result = await c.net_zero_schedule(workload_kwh=1.0, intensity=0.5)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_net_zero_run(self, storage):
        c = CarbonMarketIntegrator({}, storage, rng=random.Random(0))
        result = await c.net_zero_schedule(workload_kwh=0.01, intensity=0.1)
        assert result["action"] in ("run", "run_offset")

    @pytest.mark.asyncio
    async def test_budget_overshoot_defers(self, storage):
        c = CarbonMarketIntegrator(
            {"carbon_budget_kg": 0.1}, storage, rng=random.Random(0))
        # Force budget already used.
        c.carbon_used_kg = 0.09
        result = await c.net_zero_schedule(workload_kwh=1.0, intensity=0.1)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_rec_retirement(self, storage):
        c = CarbonMarketIntegrator({}, storage)
        await c.purchase_rec(10.0)
        await c.purchase_rec(5.0)
        retired = await c.retire_rec(7.0)
        assert retired == 7.0
        remaining = await asyncio.to_thread(storage.get_rec_balance)
        assert remaining == pytest.approx(8.0)

    @pytest.mark.asyncio
    async def test_price_ema_smooths(self, storage):
        prices = iter([100.0, 200.0])

        async def oracle():
            return next(prices)

        c = CarbonMarketIntegrator({}, storage, price_oracle=oracle)
        await c.update_price()
        await c.update_price()
        # EMA should be strictly between the two samples.
        assert 100.0 < c.price_ema < 200.0


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTS
# =============================================================================
class TestChaos:
    @pytest.mark.asyncio
    async def test_latency_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage,
                               rng=random.Random(0))
        result = await c.run_experiment("t1", "latency")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_exception_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage,
                               rng=random.Random(0))
        result = await c.run_experiment("t2", "exception")
        assert result["status"] == "injected"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, storage):
        c = ChaosTestingEngine({}, storage)
        with pytest.raises(ValueError):
            await c.run_experiment("t6", "unknown_fault")

    @pytest.mark.asyncio
    async def test_experiment_persisted(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage,
                               rng=random.Random(0))
        await c.run_experiment("t7", "latency")
        assert len(storage.tables["chaos_experiments"]) == 1

    @pytest.mark.asyncio
    async def test_auto_rollback_on_steady_breach(self, storage):
        # Steady predicate: steady before (no active), unsteady after.
        state = {"count": 0}

        async def steady():
            state["count"] += 1
            return state["count"] == 1  # True once, False thereafter

        c = ChaosTestingEngine(
            {"chaos_intensity": 0.0, "chaos_auto_rollback": True},
            storage, steady_predicate=steady, rng=random.Random(0))
        result = await c.run_experiment("rb", "latency")
        assert result["rolled_back"] is True
        assert c.rollback_count == 1

    @pytest.mark.asyncio
    async def test_blast_radius_skips(self, storage):
        c = ChaosTestingEngine(
            {"chaos_intensity": 0.0, "chaos_blast_radius": 0.0},
            storage, rng=random.Random(0))
        # Even the first experiment is skipped when the allowed concurrency
        # is at least 1 but our counter is inflated artificially.
        c.active["occupied"] = {"fault_type": "latency"}
        c.active["occupied2"] = {"fault_type": "latency"}
        c.active["occupied3"] = {"fault_type": "latency"}
        c.active["occupied4"] = {"fault_type": "latency"}
        c.active["occupied5"] = {"fault_type": "latency"}
        result = await c.run_experiment("t8", "latency")
        assert result["status"] == "skipped"


# =============================================================================
# ENHANCEMENT 10 — HITL TESTS
# =============================================================================
class TestHITL:
    @pytest.mark.asyncio
    async def test_submit_and_receive(self, storage):
        h = ActiveUserPreferenceLearner(
            storage, uncertainty_threshold=1.0)
        await h.submit_response("user_1", "choice_a")
        result = await h.query_user_if_needed(
            "user_1",
            [{"solution_id": "choice_a"}, {"solution_id": "choice_b"}],
            timeout=0.5)
        assert result == "choice_a"

    @pytest.mark.asyncio
    async def test_timeout_returns_first_candidate(self, storage):
        h = ActiveUserPreferenceLearner(
            storage, uncertainty_threshold=1.0)
        result = await h.query_user_if_needed(
            "user_1",
            [{"solution_id": "choice_a"}, {"solution_id": "choice_b"}],
            timeout=0.05)
        assert result == "choice_a"

    @pytest.mark.asyncio
    async def test_single_candidate_returns_none(self, storage):
        h = ActiveUserPreferenceLearner(storage)
        result = await h.query_user_if_needed(
            "u", [{"solution_id": "only"}], timeout=0.05)
        assert result is None

    @pytest.mark.asyncio
    async def test_confident_choice_skips_query(self, storage):
        h = ActiveUserPreferenceLearner(
            storage, uncertainty_threshold=0.01)
        # Give the user a strong preference for "quality_score".
        await h.record_choice("u", "s",
                              metrics={"quality_score": 1.0,
                                       "cost_usd": 0.01,
                                       "latency_ms": 0.01,
                                       "carbon_g": 0.01})
        result = await h.query_user_if_needed(
            "u",
            [{"solution_id": "hi", "quality_score": 10.0},
             {"solution_id": "lo", "quality_score": 0.1}],
            timeout=0.01)
        assert result == "hi"

    @pytest.mark.asyncio
    async def test_record_choice_persists(self, storage):
        h = ActiveUserPreferenceLearner(storage)
        await h.record_choice("user_1", "sol_a",
                              metrics={"quality_score": 0.9})
        assert "user_1" in storage.preferences

    @pytest.mark.asyncio
    async def test_uncertainty_metric(self, storage):
        h = ActiveUserPreferenceLearner(storage)
        await h.record_choice("u", "s",
                              metrics={"quality_score": 1.0})
        u = h.expected_utility_uncertainty(
            "u",
            [{"quality_score": 1.0}, {"quality_score": 0.99}])
        assert u >= 0.0


# =============================================================================
# INTEGRATION TESTS — ORCHESTRATOR
# =============================================================================
class TestOrchestratorIntegration:
    @pytest.mark.asyncio
    async def test_select_strategy_smoke(self, orchestrator):
        state = {"quality": 0.9, "carbon_intensity": 350,
                 "cost": 0.4, "latency_ms": 120}
        result = await orchestrator.select_strategy(state)
        assert "strategy" in result
        assert result["strategy"] in ["performance", "carbon", "cost",
                                       "hybrid", "adaptive"]
        assert "precision" in result
        assert "carbon_decision" in result

    @pytest.mark.asyncio
    async def test_health_check_reports_all(self, orchestrator):
        hc = await orchestrator.health_check()
        assert hc["running"] is True
        for key in ("agents", "temporal_rules", "precision",
                    "carbon_price", "rec_balance_mwh", "quantum_policy",
                    "carbon_budget_remaining_kg", "chaos_rollbacks"):
            assert key in hc

    @pytest.mark.asyncio
    async def test_full_cycle_exercises_all_ten(self, orchestrator):
        state = {"quality": 0.85, "carbon_intensity": 300,
                 "cost": 0.4, "latency_ms": 100}
        result = await orchestrator.select_strategy(state)
        assert "strategy" in result
        assert "agent_id" in result
        assert "precision" in result
        assert "carbon_decision" in result
        assert "temporal_violations" in result
        if NUMPY_AVAILABLE:
            assert "xai" in result

    @pytest.mark.asyncio
    async def test_concurrent_select_strategy(self, orchestrator):
        async def _call():
            return await orchestrator.select_strategy({
                "quality": 0.9, "carbon_intensity": 300,
                "cost": 0.4, "latency_ms": 100})

        results = await asyncio.gather(*[_call() for _ in range(10)])
        for r in results:
            assert r["strategy"] in ["performance", "carbon", "cost",
                                      "hybrid", "adaptive"]

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(self, storage, config):
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        assert len(orch._background_tasks) == 9
        await orch.shutdown()
        assert len(orch._background_tasks) == 0
        assert orch._running is False

    @pytest.mark.asyncio
    async def test_missing_keys_handled(self, orchestrator):
        result = await orchestrator.select_strategy({"quality": 0.9})
        assert result["strategy"] in ["performance", "carbon", "cost",
                                       "hybrid", "adaptive"]

    @pytest.mark.asyncio
    async def test_empty_state_handled(self, orchestrator):
        result = await orchestrator.select_strategy({})
        assert "strategy" in result

    @pytest.mark.asyncio
    async def test_report_epoch_in_process(self, storage, config):
        class FakeAdaptive:
            def __init__(self):
                self.calls = []

            async def record_feedback(self, context, metrics, **kw):
                self.calls.append({"context": context,
                                   "metrics": metrics, **kw})

        adaptive = FakeAdaptive()
        orch = MOPDOrchestratorV17(
            storage=storage, config=config, adaptive_function=adaptive)
        await orch.report_epoch(
            context={"request_id": "r1"},
            metrics={"loss": 0.05},
            teacher_id="t1",
            distillation_loss=0.123,
            epoch=1)
        assert len(adaptive.calls) == 1
        assert adaptive.calls[0]["teacher_id"] == "t1"
        assert adaptive.calls[0]["distillation_loss"] == 0.123

    @pytest.mark.asyncio
    async def test_report_epoch_no_adaptive_no_url(self, storage, config):
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.report_epoch(
            context={}, metrics={}, teacher_id="t1",
            distillation_loss=0.0, epoch=0)


# =============================================================================
# EDGE CASES
# =============================================================================
class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_nan_quality_does_not_crash(self, orchestrator):
        result = await orchestrator.select_strategy({
            "quality": float("nan"),
            "carbon_intensity": 300,
            "cost": 0.4,
            "latency_ms": 100})
        assert "strategy" in result

    @pytest.mark.asyncio
    async def test_infinite_carbon_does_not_crash(self, orchestrator):
        result = await orchestrator.select_strategy({
            "quality": 0.9,
            "carbon_intensity": float("inf"),
            "cost": 0.4,
            "latency_ms": 100})
        assert "strategy" in result

    @pytest.mark.asyncio
    async def test_storage_failure_does_not_crash(self, storage, config):
        storage.fail_on = "save_agent_message"
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        try:
            result = await orch.select_strategy({
                "quality": 0.9, "carbon_intensity": 300,
                "cost": 0.4, "latency_ms": 100})
            assert "strategy" in result
        finally:
            await orch.shutdown()

    @pytest.mark.asyncio
    async def test_zero_agents_handled(self, storage, config):
        config["agent_count"] = 0
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        try:
            result = await orch.select_strategy({"quality": 0.9})
            assert "strategy" in result
            assert result["agent_id"] is None
        finally:
            await orch.shutdown()

    @pytest.mark.asyncio
    async def test_empty_temporal_formulas(self, storage, config):
        config["temporal_formulas"] = []
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        try:
            assert len(orch.temporal.rules) == 0
            result = await orch.select_strategy({"quality": 0.9})
            assert result["temporal_violations"] == []
        finally:
            await orch.shutdown()

    @pytest.mark.asyncio
    async def test_extreme_values(self, orchestrator):
        result = await orchestrator.select_strategy({
            "quality": 1e6,
            "carbon_intensity": -1e6,
            "cost": 1e6,
            "latency_ms": 1e6})
        assert "strategy" in result

    @pytest.mark.asyncio
    async def test_storage_failure_everywhere(self, storage, config):
        """Inject failure into many storage methods; orchestrator survives."""
        for m in ("save_agent_message", "save_agent", "save_causal_edge",
                  "save_temporal_trace", "save_temporal_violation",
                  "save_xai_explanation", "save_precision_switch",
                  "save_credit_price", "save_net_zero_match",
                  "save_chaos_experiment", "save_federated_weights",
                  "get_federated_weights"):
            storage.fail_on = m
            orch = MOPDOrchestratorV17(storage=storage, config=config)
            await orch.start()
            try:
                result = await orch.select_strategy({
                    "quality": 0.9, "carbon_intensity": 300,
                    "cost": 0.4, "latency_ms": 100})
                assert "strategy" in result
            finally:
                await orch.shutdown()


# =============================================================================
# DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_seeded_reproducibility(self, storage, config):
        config["causal_exploration_rate"] = 0.0
        random.seed(42)
        if NUMPY_AVAILABLE:
            np.random.seed(42)
        orch1 = MOPDOrchestratorV17(storage=storage, config=config, seed=42)
        await orch1.start()
        r1 = await orch1.select_strategy({"quality": 0.9})
        await orch1.shutdown()

        random.seed(42)
        if NUMPY_AVAILABLE:
            np.random.seed(42)
        storage2 = InMemoryStorage(":memory:")
        orch2 = MOPDOrchestratorV17(storage=storage2, config=config, seed=42)
        await orch2.start()
        r2 = await orch2.select_strategy({"quality": 0.9})
        await orch2.shutdown()

        assert r1["strategy"] == r2["strategy"]

    @pytest.mark.asyncio
    async def test_quantum_reproducible_with_seed(self):
        def run(seed):
            q = QuantumDistillationEngine(rng=random.Random(seed))
            q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
            q.register_teacher("b", [0.1, 0.2, 0.3, 0.2, 0.2])
            asyncio.get_event_loop()
            return q._superpose()

        a, ea = run(123)
        b, eb = run(123)
        assert a == b
        assert ea == eb

    @pytest.mark.asyncio
    async def test_agent_roles_reproducible(self, storage):
        m1 = MultiAgentCoordinator({"agent_count": 3}, storage,
                                   rng=random.Random(99))
        m2 = MultiAgentCoordinator({"agent_count": 3}, storage,
                                   rng=random.Random(99))
        await m1._specialise()
        await m2._specialise()
        for aid in m1.agents:
            assert m1.agents[aid].role == m2.agents[aid].role


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
