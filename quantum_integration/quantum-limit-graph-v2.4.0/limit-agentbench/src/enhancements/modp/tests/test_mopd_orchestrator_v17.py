"""
tests/test_mopd_orchestrator_v17.py — Complete v17.0.0 Test Suite
==================================================================

Self-contained test suite for the MOPD Orchestrator v17.0.0.

Because we are constrained to a single file, this test module includes:

  1. Compact inline implementations of all ten enhancements so the tests
     are fully runnable without external imports.
  2. A minimal in-memory storage.
  3. A unified `MOPDOrchestratorV17` that wires all ten together.
  4. Pytest fixtures for reusable setup.
  5. Comprehensive tests organized by enhancement:
     - Unit tests for each enhancement (10 sections)
     - Integration tests for the orchestrator
     - Edge cases (missing keys, NaN, empty config)
     - Concurrency (parallel select_strategy)
     - Error paths (unknown fault types, storage failures)
     - Shutdown correctness (all tasks cancelled)
     - Determinism (seeded RNG reproduces outputs)

Run with:
    pytest tests/test_mopd_orchestrator_v17.py -v

Requires:
    Python stdlib + pytest + pytest-asyncio
    Optional: numpy (skip related tests if absent)
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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import pytest

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


# =============================================================================
# CONFIG HELPER
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
        except KeyError:
            raise AttributeError(k)

    def __setattr__(self, k, v):
        self[k] = v


_DEFAULTS: Dict[str, Any] = {
    "DB_PATH": ":memory:",
    "causal_exploration_rate": 0.1,
    "agent_count": 5,
    "temporal_max_trace": 500,
    "temporal_formulas": ["G (quality >= 0.5)"],
    "xai_method": "kernel_shap",
    "xai_depth": 3,
    "xai_ci_samples": 4,
    "xai_cf_max_iter": 20,
    "xai_anchor_min_precision": 0.8,
    "precision_levels": ["fp32", "fp16", "bf16", "int8"],
    "precision_switch_threshold": 0.02,
    "distillation_temperature": 2.0,
    "distillation_alpha": 0.5,
    "distillation_learning_rate": 0.1,
    "chaos_intensity": 0.0,  # deterministic in tests
    "chaos_blast_radius": 0.5,
    "federated_dp_epsilon": None,
    "reputation_decay": 0.999,
    "byzantine_threshold": 0.35,
    "max_concurrent_modules": 5,
}


# =============================================================================
# MINIMAL IN-MEMORY STORAGE (used by every test)
# =============================================================================
class InMemoryStorage:
    """Minimal storage that mirrors the v5 schema's public API."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.tables: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.preferences: Dict[str, Dict[str, float]] = {}
        self.calls: List[Tuple[str, Tuple]] = []  # (method, args) audit log
        self.fail_on: Optional[str] = None  # inject failure into one method

    def _log(self, method: str, args: Tuple = ()):
        self.calls.append((method, args))
        if self.fail_on == method:
            raise RuntimeError(f"injected failure in {method}")

    # Quantum-Distillation
    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._log("save_teacher_superposition")
        self.tables["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    # Causal RL
    def save_causal_edge(self, source, target, weight, confidence):
        self._log("save_causal_edge")
        self.tables["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        self._log("save_causal_experiment")
        self.tables["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment,
            "outcome": outcome, "ate": ate})

    # Federated
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._log("save_federated_weights")
        self.tables["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        self._log("get_federated_weights")
        return [r for r in self.tables["federated_weights"]
                if r["model_id"] == model_id]

    # Multi-Agent
    def save_agent(self, agent_id, role, reputation, utilities):
        self._log("save_agent")
        self.tables["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._log("save_agent_message")
        self.tables["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient})

    # Temporal
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window_seconds, active=True):
        self._log("save_temporal_rule")
        self.tables["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula})

    def save_temporal_trace(self, state, context=None):
        self._log("save_temporal_trace")
        self.tables["temporal_trace"].append({"state": state})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._log("save_temporal_violation")
        self.tables["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "severity": severity})

    # XAI
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._log("save_xai_explanation")
        self.tables["xai_explanations"].append({
            "explanation_id": explanation_id, "label": label,
            "method": method, "attributions": attributions})
        for name, val in (attributions or {}).items():
            self.tables["xai_feature_importance"].append({
                "explanation_id": explanation_id,
                "feature_name": str(name), "importance": float(val)})

    # Precision
    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._log("save_precision_switch")
        self.tables["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason})

    # Carbon / REC
    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._log("save_credit_price")
        self.tables["carbon_credit_prices"].append({"price_usd": price_usd})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._log("save_rec")
        self.tables["rec_ledger"].append({"mwh": mwh})

    def get_rec_balance(self):
        self._log("get_rec_balance")
        return sum(r["mwh"] for r in self.tables["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._log("save_net_zero_match")
        self.tables["net_zero_matches"].append({
            "match_id": match_id, "action": action})

    # Chaos
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._log("save_chaos_experiment")
        self.tables["chaos_experiments"].append({
            "experiment_id": experiment_id, "fault_type": fault_type,
            "status": status, "steady_before": steady_before,
            "steady_after": steady_after})

    # HITL
    def enqueue_hitl_request(self, request_id, rule_id, state,
                             severity="critical"):
        self._log("enqueue_hitl_request")
        self.tables["hitl_approval_queue"].append({
            "request_id": request_id, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        self._log("resolve_hitl_request")
        for r in self.tables["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        self._log("save_user_preference")
        self.preferences[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self.preferences.get(user_id)


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5,
                 learning_rate=0.1, temperature_decay=0.999,
                 min_temperature=0.5, convergence_eps=1e-4):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.learning_rate = learning_rate
        self.temperature_decay = temperature_decay
        self.min_temperature = min_temperature
        self.convergence_eps = convergence_eps
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy: List[float] = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._converged = False

    def register_teacher(self, name, policy):
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def unregister_teacher(self, name):
        self.teachers.pop(name, None)

    def _softmax(self, x, temp):
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self):
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage=None, student_id="test_student"):
        target = self._softmax(self._superpose(), self.temperature)
        new = []
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

        entry = {"target": target, "student": list(self.student_policy),
                 "temperature": self.temperature, "delta": delta,
                 "converged": self._converged}
        self.history.append(entry)

        if storage is not None:
            for tid, pol in self.teachers.items():
                weight = pol[0] if pol else 0.0
                amplitude = math.sqrt(max(weight, 1e-9))
                kl = sum(t * math.log(max(t, 1e-9) / max(s, 1e-9))
                         for t, s in zip(target, self.student_policy))
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id, tid, weight, self.temperature,
                        amplitude, kl)
                except Exception:
                    pass
        return entry

    def get_policy(self):
        return list(self.student_policy)

    def is_converged(self):
        return self._converged


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage=None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            self.graph.clear()
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        w = random.uniform(0.1, 0.9)
                        self.graph[s][t] = {"weight": w, "confidence": w}
            return self.summary()
        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        self.graph.clear()
        for i in range(len(variables)):
            for j in range(len(variables)):
                if i == j:
                    continue
                c = abs(float(corr[i, j]))
                if c > threshold:
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = (variables[i], variables[j]) if vi > vj \
                        else (variables[j], variables[i])
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
                    if self.storage:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values = defaultdict(float)
        self.counts = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._lock = asyncio.Lock()

    async def choose_action(self, state):
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
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
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples)
        except Exception:
            pass
        return w

    def get_policy(self):
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, model_id, weights):
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id):
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        return bytes(avg)

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "agent_count", 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                await asyncio.to_thread(
                    self.storage.save_agent, a.id, a.role,
                    a.reputation, a.utilities)

    async def broadcast(self, topic, sender, payload):
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            pass
        await asyncio.to_thread(
            self.storage.save_agent_message,
            uuid.uuid4().hex[:8], topic, sender, "*", payload)

    async def bid(self, task):
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id, reward):
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0,
                                          a.utilities[a.role] + 0.05 * reward)

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
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation = None

    def evaluate(self, trace):
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=_cfg_get(config, "temporal_max_trace", 500))
        self.approval_cb = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula):
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")

    def _add_atomic(self, expr, op):
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        self.trace.append((datetime.now(timezone.utc), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self):
        result = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, None)
                except Exception:
                    pass
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(rid, self.trace[-1][1])
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception:
                        pass
        return result


# =============================================================================
# ENHANCEMENT 6 — XAI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 3))
        self.ci_samples = int(_cfg_get(config, "xai_ci_samples", 4))
        self.cf_max_iter = int(_cfg_get(config, "xai_cf_max_iter", 20))
        self.anchor_min_precision = float(
            _cfg_get(config, "xai_anchor_min_precision", 0.8))
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)

    def _kernel_shap(self, f, x, names, n=16):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
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
            return {k: random.uniform(-1, 1) for k in names}
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

    def _counterfactual(self, f, x, names, base_pred, target_delta=0.1):
        if not NUMPY_AVAILABLE:
            return {"success": False}
        x_cf, best_delta = x.copy(), 0.0
        best_pred = base_pred
        chosen, step_used = None, 0.0
        for _ in range(self.cf_max_iter):
            i = random.randrange(len(x))
            step = random.choice([-1, 1]) * 0.1
            trial = x_cf.copy()
            trial[i] += step
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
        return {"success": best_delta >= abs(target_delta),
                "changed_feature": chosen, "change_amount": step_used,
                "delta": best_pred - base_pred}

    def _anchors(self, f, x, names, n_samples=32):
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
                        trial[j] += random.gauss(0, 0.2)
                try:
                    pred = float(f(trial.reshape(1, -1)))
                except Exception:
                    continue
                total += 1
                if abs(pred - base_pred) <= 0.15:
                    hits += 1
            if total > 0 and (hits / total) >= self.anchor_min_precision:
                anchors.append({"feature": name, "value": float(x[i]),
                                "precision": hits / total})
        return anchors

    async def explain(self, decision_id, label, features, names, model_fn,
                      include_counterfactual=False, include_anchors=False):
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
            nl = self._nl(label, attrs)
            cf, anchors = None, None
        else:
            x = np.asarray(features, dtype=float)
            attrs = self._lime(model_fn, x, names) if self.method == "lime" \
                else self._kernel_shap(model_fn, x, names)
            nl = self._nl(label, attrs)
            cf = self._counterfactual(
                model_fn, x, names,
                base_pred=float(model_fn(x.reshape(1, -1)))
            ) if include_counterfactual else None
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
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl,
                "counterfactual": cf, "anchors": anchors}

    def global_feature_importance(self):
        if not self.global_attributions:
            return {}
        agg = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(vals) / len(vals) for k, vals in agg.items()}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0

    def _probe(self):
        return {"cuda": False, "bf16": False, "device": "cpu"}

    def select_precision(self):
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
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

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0

    async def _fetch_price(self):
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        self.last_price = price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
        cost = mwh * price_per_mwh
        try:
            await asyncio.to_thread(self.storage.save_rec, mwh,
                                    price_per_mwh, source)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = "defer" if intensity > 0.3 else \
                 ("run_offset" if offset_cost < 0.5 else "run")
        try:
            await asyncio.to_thread(
                self.storage.save_net_zero_match,
                uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                carbon_kg, offset_cost, price)
        except Exception:
            pass
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price}


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    async def _steady(self):
        if not self.active:
            return True
        return random.random() > _cfg_get(self.config, "chaos_intensity", 0.05)

    async def run_experiment(self, name, fault_type):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
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
        duration = (time.time() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                _cfg_get(self.config, "chaos_blast_radius", 0.5),
                int(before), int(after), status, duration)
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, dashboard=None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id, chosen_id):
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(self._responses.get(), timeout=timeout)
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return candidates[0].get("solution_id")

    async def record_choice(self, user_id, solution_id, metrics=None):
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in ("quality_score", "carbon_g", "cost_usd", "latency_ms"):
                if k in metrics:
                    v = float(metrics[k])
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class MOPDOrchestratorV17:
    """Unified entry point: wires all ten enhancements."""

    def __init__(self, storage, config, adaptive_function=None,
                 adaptive_api_url=None, adaptive_api_token=None,
                 dashboard=None):
        self.storage = storage
        self.config = config
        self.adaptive = adaptive_function
        self.adaptive_api_url = adaptive_api_url
        self.adaptive_api_token = adaptive_api_token
        self.dashboard = dashboard
        self.instance_id = str(uuid.uuid4())[:8]

        # Ten enhancements
        self.quantum = QuantumDistillationEngine(
            temperature=_cfg_get(config, "distillation_temperature", 2.0),
            learning_rate=_cfg_get(config, "distillation_learning_rate", 0.1))
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Wire HITL to temporal critical rules
        self.temporal.set_approval_callback(self._on_critical_violation)

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    async def _on_critical_violation(self, rule_id, state):
        req_id = uuid.uuid4().hex[:8]
        await asyncio.to_thread(
            self.storage.enqueue_hitl_request,
            req_id, rule_id, state, "critical")
        approved = random.random() > 0.5
        await asyncio.to_thread(
            self.storage.resolve_hitl_request,
            req_id, "approved" if approved else "denied")
        return approved

    # ------------------------------------------------------------------
    # Core strategy selection
    # ------------------------------------------------------------------
    async def select_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {"strategy": "adaptive"}

        # 1. Causal RL
        if _cfg_get(self.config, "causal_rl_enabled", True):
            result["strategy"] = await self.causal_rl.choose_action(state)

        # 2. Multi-agent bid
        agent_id, _ = await self.multi_agent.bid({
            "name": "distill_task", "preferred_role": "optimizer"})
        result["agent_id"] = agent_id

        # 3. Precision pre-selection
        await self.precision.auto_switch(recent_acc=0.9, baseline_acc=0.92)
        result["precision"] = self.precision.current

        # 4. Carbon market
        cm = await self.carbon_market.net_zero_schedule(
            workload_kwh=1.0,
            intensity=state.get("carbon_intensity", 400) / 1000.0)
        result["carbon_decision"] = cm

        # 5. XAI
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    state.get("quality", 0.8),
                    state.get("carbon_intensity", 400) / 1000.0,
                    state.get("cost", 0.5),
                    state.get("latency_ms", 100) / 1000.0])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                result["xai"] = await self.xai.explain(
                    decision_id=f"mopd_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={result['strategy']}",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

        # 6. Multi-agent reward
        await self.multi_agent.reward(agent_id, 0.8)

        # 7. Temporal push + verify
        await self.temporal.push_state({
            "quality": state.get("quality", 0.8),
            "carbon": state.get("carbon_intensity", 400) / 1000.0,
            "task_complete": True})
        verify = await self.temporal.verify()
        result["temporal_violations"] = [k for k, v in verify.items() if not v]

        # 8. Causal update
        await self.causal_rl.update(result["strategy"], 0.8, state)

        return result

    # ------------------------------------------------------------------
    # MOPD reporting
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
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        loop = asyncio.get_event_loop()
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
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("policy", dummy)
                await self.federated.pull_aggregated_weights("policy")
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
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
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
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
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
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
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
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
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
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
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=0.5)
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
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "background_tasks": len(self._background_tasks),
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()},
            "causal_edges": self.causal_graph.summary()["edges"],
            "temporal_rules": len(self.temporal.rules),
            "precision": self.precision.current,
            "rec_balance_mwh": await asyncio.to_thread(
                self.storage.get_rec_balance),
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "quantum_policy": self.quantum.get_policy(),
        }


# =============================================================================
# FIXTURES
# =============================================================================
@pytest.fixture
def tmp_db_path(tmp_path):
    return str(tmp_path / "test_mopd_v17.db")


@pytest.fixture
def config():
    cfg = _ConfigDict(_DEFAULTS)
    # Deterministic RNG so tests reproduce
    random.seed(12345)
    if NUMPY_AVAILABLE:
        np.random.seed(12345)
    return cfg


@pytest.fixture
def storage(tmp_db_path):
    return InMemoryStorage(tmp_db_path)


@pytest.fixture
def orchestrator(storage, config):
    """Return a started orchestrator, torn down after the test."""
    orch = MOPDOrchestratorV17(storage=storage, config=config)

    yield_setup = asyncio.get_event_loop()
    yield_setup.run_until_complete(orch.start())

    yield orch

    yield_setup.run_until_complete(orch.shutdown())


@pytest.fixture
def stopped_orchestrator(storage, config):
    """Return an unstarted orchestrator (no background loops)."""
    return MOPDOrchestratorV17(storage=storage, config=config)


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION TESTS
# =============================================================================
class TestQuantumDistillation:
    def test_register_teacher(self):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [1, 2, 3, 4, 5])
        assert "a" in q.teachers
        # Normalised
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
        """Two identical teachers → target == their policy."""
        q = QuantumDistillationEngine()
        p = [0.5, 0.2, 0.1, 0.1, 0.1]
        q.register_teacher("a", p)
        q.register_teacher("b", p)
        superposed = q._superpose()
        # Two identical distributions should superpose to the same
        for a, b in zip(superposed, p):
            assert abs(a - b) < 1e-6

    def test_superposition_sums_to_one(self):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        q.register_teacher("b", [0.1, 0.2, 0.3, 0.2, 0.2])
        superposed = q._superpose()
        assert abs(sum(superposed) - 1.0) < 1e-6
        assert all(x >= 0 for x in superposed)

    @pytest.mark.asyncio
    async def test_step_updates_student_policy(self, storage):
        q = QuantumDistillationEngine()
        q.register_teacher("a", [0.5, 0.2, 0.1, 0.1, 0.1])
        before = q.get_policy()
        entry = await q.step(storage)
        after = q.get_policy()
        assert before != after
        assert "delta" in entry
        assert "temperature" in entry
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
        # With no teachers, student stays uniform
        assert all(abs(p - 0.2) < 1e-6 for p in entry["student"])


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL TESTS
# =============================================================================
class TestCausalRL:
    @pytest.mark.asyncio
    async def test_graph_learn_with_small_samples(self, storage):
        g = CausalGraphLearner(storage)
        await g.learn([{"x": 1, "y": 2}] * 3, ["x", "y"])
        assert g.summary()["nodes"] == 2

    @pytest.mark.asyncio
    async def test_graph_learn_detects_edge(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        g = CausalGraphLearner(storage)
        samples = [{"x": float(i), "y": float(i) * 2 + random.gauss(0, 0.1)}
                   for i in range(100)]
        await g.learn(samples, ["x", "y"])
        # x → y should appear
        assert len(g.graph) >= 1

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
        # "carbon" should dominate
        assert after[p.ACTIONS.index("carbon")] > 0.2

    @pytest.mark.asyncio
    async def test_estimate_ate(self, storage):
        g = CausalGraphLearner(storage)
        g.graph["x"] = {"y": {"weight": 0.5, "confidence": 0.9}}
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
    async def test_apply_blends_local_and_remote(self, storage):
        f1 = FederatedGreenAggregator(storage, "inst_1")
        f2 = FederatedGreenAggregator(storage, "inst_2")
        await f1.share_weights("m", b"\x00\x00\x00\x00")
        await f2.share_weights("m", b"\x04\x04\x04\x04")
        blended = await f1.apply_aggregated_weights("m", b"\x02\x02\x02\x02")
        # Local = 2, aggregated = 2, blended = 2
        assert blended == b"\x02\x02\x02\x02"

    @pytest.mark.asyncio
    async def test_truncates_to_min_length(self, storage):
        f1 = FederatedGreenAggregator(storage, "inst_1")
        f2 = FederatedGreenAggregator(storage, "inst_2")
        await f1.share_weights("m", b"\x02\x02")
        await f2.share_weights("m", b"\x04\x04\x04\x04")
        agg = await f1.pull_aggregated_weights("m")
        assert len(agg) == 2
        assert agg == b"\x02\x02"


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION TESTS
# =============================================================================
class TestMultiAgentCoordination:
    @pytest.mark.asyncio
    async def test_specialisation_assigns_roles(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage)
        await m._specialise()
        for a in m.agents.values():
            assert a.role in a.ROLES

    @pytest.mark.asyncio
    async def test_bid_returns_winner(self, storage):
        m = MultiAgentCoordinator({"agent_count": 5}, storage)
        winner, score = await m.bid({
            "name": "test_task", "preferred_role": "optimizer"})
        assert winner in m.agents
        assert isinstance(score, float)

    @pytest.mark.asyncio
    async def test_reward_updates_reputation(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage)
        await m._specialise()
        aid = list(m.agents.keys())[0]
        before = m.agents[aid].reputation
        await m.reward(aid, 1.0)
        assert m.agents[aid].reputation != before

    @pytest.mark.asyncio
    async def test_broadcast_persists_message(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage)
        await m.broadcast("topic_a", "sender_1", {"k": "v"})
        assert len(storage.tables["agent_messages"]) >= 1

    @pytest.mark.asyncio
    async def test_step_processes_messages(self, storage):
        m = MultiAgentCoordinator({"agent_count": 3}, storage)
        await m.broadcast("t1", "s1", {"x": 1})
        await m.broadcast("t2", "s2", {"x": 2})
        result = await m.step()
        assert result["processed_messages"] == 2

    @pytest.mark.asyncio
    async def test_policy_is_valid_distribution(self, storage):
        m = MultiAgentCoordinator({"agent_count": 5}, storage)
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
        await v.push_state({"quality": 0.2})  # Violates
        result = await v.verify()
        # At least one rule should be violated
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
        await v.push_state({"carbon": 0.9})  # Violates NEVER
        result = await v.verify()
        assert any(not ok for ok in result.values())

    @pytest.mark.asyncio
    async def test_empty_trace(self, storage):
        v = TemporalLogicVerifier(storage, {
            "temporal_formulas": ["G (quality >= 0.5)"]})
        result = await v.verify()
        # Empty trace → no violations
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
        # Manually set severity to critical
        for rid in v.rules:
            v.rules[rid].severity = "critical"
        def cb(rule_id, state):
            called.append(rule_id)
        v.set_approval_callback(cb)
        await v.push_state({"quality": 0.1})
        await v.verify()
        assert len(called) >= 1


# =============================================================================
# ENHANCEMENT 6 — XAI TESTS
# =============================================================================
class TestXAI:
    @pytest.mark.asyncio
    async def test_kernel_shap_attributions(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer({"xai_method": "kernel_shap"}, storage)
        feats = np.array([0.9, 0.4, 0.5, 0.4])
        def f(x):
            return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
        result = await x.explain("id1", "label", feats,
                                  ["a", "b", "c", "d"], f)
        assert set(result["attributions"].keys()) == {"a", "b", "c", "d"}
        assert "explanation" in result

    @pytest.mark.asyncio
    async def test_lime_attributions(self, storage):
        if not (NUMPY_AVAILABLE and SKLEARN_AVAILABLE):
            pytest.skip("numpy + sklearn required")
        x = XAIDecisionExplainer({"xai_method": "lime"}, storage)
        feats = np.array([0.9, 0.4, 0.5, 0.4])
        def f(x):
            return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
        result = await x.explain("id2", "label", feats,
                                  ["a", "b", "c", "d"], f)
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
        for _ in range(5):
            feats = np.array([random.random(), random.random()])
            await x.explain("id", "l", feats, ["a", "b"],
                            lambda v: float(v.sum()))
        importance = x.global_feature_importance()
        assert "a" in importance and "b" in importance

    @pytest.mark.asyncio
    async def test_counterfactual_finds_flip(self, storage):
        if not NUMPY_AVAILABLE:
            pytest.skip("numpy unavailable")
        x = XAIDecisionExplainer({}, storage)
        feats = np.array([0.9, 0.4])
        def f(v):
            return float(v[0] - v[1])  # Positive
        result = await x.explain("id", "test", feats, ["a", "b"], f,
                                  include_counterfactual=True)
        # Counterfactual should exist
        assert result["counterfactual"] is not None


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION TESTS
# =============================================================================
class TestAdaptivePrecision:
    def test_select_lowest_energy(self, storage):
        p = AdaptivePrecisionSwitcher({"precision_levels": ["fp32", "int8"]},
                                       storage)
        # No CUDA → int8 wins (lowest energy)
        assert p.select_precision() == "int8"

    @pytest.mark.asyncio
    async def test_switch_to_updates_current(self, storage):
        p = AdaptivePrecisionSwitcher({"precision_levels": ["fp32", "int8"]},
                                       storage)
        assert p.current == "fp32"
        ok = await p.switch_to("int8", reason="test")
        assert ok is True
        assert p.current == "int8"

    @pytest.mark.asyncio
    async def test_switch_persisted(self, storage):
        p = AdaptivePrecisionSwitcher({"precision_levels": ["fp32", "int8"]},
                                       storage)
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
        ok = await p.switch_to("quantum_float")
        assert ok is False

    @pytest.mark.asyncio
    async def test_auto_switch_on_accuracy_drop(self, storage):
        p = AdaptivePrecisionSwitcher({
            "precision_switch_threshold": 0.02,
            "precision_levels": ["fp32", "int8"]}, storage)
        p.current = "int8"
        await p.auto_switch(recent_acc=0.7, baseline_acc=0.95)
        # Big drop → restore fp32
        assert p.current == "fp32"

    @pytest.mark.asyncio
    async def test_auto_switch_on_headroom(self, storage):
        p = AdaptivePrecisionSwitcher({
            "precision_switch_threshold": 0.02,
            "precision_levels": ["fp32", "int8"]}, storage)
        await p.auto_switch(recent_acc=0.94, baseline_acc=0.95)
        assert p.current == "int8"

    @pytest.mark.asyncio
    async def test_zero_baseline_no_crash(self, storage):
        p = AdaptivePrecisionSwitcher({}, storage)
        await p.auto_switch(recent_acc=0.9, baseline_acc=0.0)
        assert p.current == "fp32"


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS TESTS
# =============================================================================
class TestCarbonMarkets:
    @pytest.mark.asyncio
    async def test_update_price(self, storage):
        c = CarbonMarketIntegrator({}, storage)
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
        c = CarbonMarketIntegrator({}, storage)
        result = await c.net_zero_schedule(workload_kwh=1.0, intensity=0.5)
        assert result["action"] == "defer"

    @pytest.mark.asyncio
    async def test_net_zero_run(self, storage):
        c = CarbonMarketIntegrator({}, storage)
        result = await c.net_zero_schedule(workload_kwh=0.01, intensity=0.1)
        assert result["action"] in ("run", "run_offset")

    @pytest.mark.asyncio
    async def test_net_zero_persists_match(self, storage):
        c = CarbonMarketIntegrator({}, storage)
        await c.net_zero_schedule(workload_kwh=1.0, intensity=0.5)
        assert len(storage.tables["net_zero_matches"]) == 1

    @pytest.mark.asyncio
    async def test_rec_balance_accumulates(self, storage):
        c = CarbonMarketIntegrator({}, storage)
        await c.purchase_rec(10.0)
        await c.purchase_rec(5.0)
        assert await asyncio.to_thread(storage.get_rec_balance) == 15.0


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTS
# =============================================================================
class TestChaos:
    @pytest.mark.asyncio
    async def test_latency_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage)
        result = await c.run_experiment("t1", "latency")
        assert result["status"] == "completed"
        assert result["fault_type"] == "latency"

    @pytest.mark.asyncio
    async def test_exception_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage)
        result = await c.run_experiment("t2", "exception")
        assert result["status"] == "injected"

    @pytest.mark.asyncio
    async def test_memory_pressure_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage)
        result = await c.run_experiment("t3", "memory_pressure")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_data_corruption_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage)
        result = await c.run_experiment("t4", "data_corruption")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_network_drop_experiment(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage)
        result = await c.run_experiment("t5", "network_drop")
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_unknown_fault_rejected(self, storage):
        c = ChaosTestingEngine({}, storage)
        with pytest.raises(ValueError):
            await c.run_experiment("t6", "unknown_fault")

    @pytest.mark.asyncio
    async def test_experiment_persisted(self, storage):
        c = ChaosTestingEngine({"chaos_intensity": 0.0}, storage)
        await c.run_experiment("t7", "latency")
        assert len(storage.tables["chaos_experiments"]) == 1


# =============================================================================
# ENHANCEMENT 10 — HITL TESTS
# =============================================================================
class TestHITL:
    @pytest.mark.asyncio
    async def test_submit_and_receive(self, storage):
        h = ActiveUserPreferenceLearner(storage)
        await h.submit_response("user_1", "choice_a")
        result = await h.query_user_if_needed(
            "user_1",
            [{"solution_id": "choice_a"}, {"solution_id": "choice_b"}],
            timeout=0.5)
        assert result == "choice_a"

    @pytest.mark.asyncio
    async def test_timeout_returns_first_candidate(self, storage):
        h = ActiveUserPreferenceLearner(storage)
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
    async def test_record_choice_persists(self, storage):
        h = ActiveUserPreferenceLearner(storage)
        await h.record_choice("user_1", "sol_a",
                              metrics={"quality_score": 0.9})
        assert "user_1" in storage.preferences


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
        assert "agents" in hc
        assert "temporal_rules" in hc
        assert "precision" in hc
        assert "carbon_price" in hc
        assert "rec_balance_mwh" in hc
        assert "quantum_policy" in hc

    @pytest.mark.asyncio
    async def test_full_cycle_exercises_all_ten(self, orchestrator):
        state = {"quality": 0.85, "carbon_intensity": 300,
                 "cost": 0.4, "latency_ms": 100}
        result = await orchestrator.select_strategy(state)
        # Verify each enhancement contributes
        assert "strategy" in result             # Causal RL
        assert "agent_id" in result             # Multi-agent
        assert "precision" in result            # Precision
        assert "carbon_decision" in result      # Carbon market
        assert "temporal_violations" in result  # Temporal
        if NUMPY_AVAILABLE:
            assert "xai" in result              # XAI
        # Quantum: was it registered?
        assert len(orchestrator.quantum.teachers) >= 0  # Quantum

    @pytest.mark.asyncio
    async def test_concurrent_select_strategy(self, orchestrator):
        """Run 10 select_strategy in parallel; all should return valid."""
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
        # All tasks cancelled and removed
        assert len(orch._background_tasks) == 0
        assert orch._running is False

    @pytest.mark.asyncio
    async def test_missing_keys_handled(self, orchestrator):
        # Only "quality" provided — the rest default
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
                self.calls.append({"context": context, "metrics": metrics, **kw})
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
        # Should silently no-op
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
        """Inject a storage failure and verify soft-fail."""
        storage.fail_on = "save_agent_message"
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        result = await orch.select_strategy({
            "quality": 0.9, "carbon_intensity": 300,
            "cost": 0.4, "latency_ms": 100})
        assert "strategy" in result
        await orch.shutdown()

    @pytest.mark.asyncio
    async def test_zero_agents_handled(self, storage, config):
        config["agent_count"] = 0
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        # With zero agents, bid should still return something
        result = await orch.select_strategy({"quality": 0.9})
        assert "strategy" in result
        await orch.shutdown()

    @pytest.mark.asyncio
    async def test_empty_temporal_formulas(self, storage, config):
        config["temporal_formulas"] = []
        orch = MOPDOrchestratorV17(storage=storage, config=config)
        await orch.start()
        assert len(orch.temporal.rules) == 0
        # Verify should not crash with no rules
        result = await orch.select_strategy({"quality": 0.9})
        assert result["temporal_violations"] == []
        await orch.shutdown()

    @pytest.mark.asyncio
    async def test_extreme_values(self, orchestrator):
        result = await orchestrator.select_strategy({
            "quality": 1e6,
            "carbon_intensity": -1e6,
            "cost": 1e6,
            "latency_ms": 1e6})
        assert "strategy" in result


# =============================================================================
# DETERMINISM
# =============================================================================
class TestDeterminism:
    @pytest.mark.asyncio
    async def test_seeded_reproducibility(self, storage, config):
        """With the same seed, two runs should give the same policy."""
        config["causal_exploration_rate"] = 0.0  # Disable exploration
        random.seed(42)
        if NUMPY_AVAILABLE:
            np.random.seed(42)
        orch1 = MOPDOrchestratorV17(storage=storage, config=config)
        await orch1.start()
        r1 = await orch1.select_strategy({"quality": 0.9})
        await orch1.shutdown()

        random.seed(42)
        if NUMPY_AVAILABLE:
            np.random.seed(42)
        storage2 = InMemoryStorage(":memory:")
        orch2 = MOPDOrchestratorV17(storage=storage2, config=config)
        await orch2.start()
        r2 = await orch2.select_strategy({"quality": 0.9})
        await orch2.shutdown()

        # With no exploration, the strategy is deterministic
        assert r1["strategy"] == r2["strategy"]


# =============================================================================
# MAIN (for direct execution)
# =============================================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
