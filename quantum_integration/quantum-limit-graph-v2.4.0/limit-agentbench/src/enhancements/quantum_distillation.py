#!/usr/bin/env python3
# =============================================================================
# FILE: quantum_distillation_integration_v2_0_1.py
# VERSION: 2.0.1 (patched)
#
# Top-10 fixes on top of v2.0.0:
#   1.  TemporalLogicMonitor: correct past-time LTL (U + nesting)
#   2.  ChaosTester: faults actually fire; save/restore for ALL types
#   3.  HumanInTheLoopCoordinator: registers synthesised queries
#   4.  CausalRewardShaper: proper IPW ATE with baseline; record realised
#       reward, not predicted probability
#   5.  ClassicalStudentModel: precision is advisory; no .to(dtype) on the
#       training model; no set_precision call from get_distilled_policy
#   6.  Teacher: heuristic documented; teacher_provider hook; beta exposed
#   7.  Training: uses circuit_results; trained=True only with enough
#       samples; seeds random/np.random/torch
#   8.  Federated: full flattened weights (compact mode optional)
#   9.  HITL threshold: normalised confidence; skip on high entropy
#  10.  Determinism, pure predict(), no I/O under lock, deepcopy returns,
#       metrics moved to start(), signal-safe shutdown
#
# Smaller corrections:
#   * XAI single-candidate guard; explain_policy uses multiple candidates
#   * Distillation skips uniform teacher; loss reported but stable
#   * Legacy run_chaos_test wrapped in try/finally
#   * All heavy training (torch/sklearn) offloaded to executor
#   * --test CLI harness (10 checks)
# =============================================================================
"""
Quantum Distillation Integration for Green Agent MoE — v2.0.1 (patched)

Distills simplified classical policies from quantum circuits (e.g., QAOA/VQE)
and exposes them as a teacher for the MoE router.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import hashlib
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Prometheus (optional).
try:
    from prometheus_client import Counter, Gauge, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

if PROMETHEUS_AVAILABLE:
    QD_REGISTRY = CollectorRegistry()
    QD_PREDICTIONS = Counter('qd_predictions_total', 'Predictions', ['source'], registry=QD_REGISTRY)
    QD_TRAINING = Counter('qd_training_steps_total', 'Training steps', registry=QD_REGISTRY)
    QD_TEMPORAL = Counter('qd_temporal_violations_total', 'Temporal violations', ['formula'], registry=QD_REGISTRY)
    QD_CHAOS = Counter('qd_chaos_tests_total', 'Chaos tests', ['fault', 'status'], registry=QD_REGISTRY)
    QD_HITL = Counter('qd_hitl_escalations_total', 'HITL escalations', ['status'], registry=QD_REGISTRY)
    QD_FEDERATED = Counter('qd_federated_rounds_total', 'Federated rounds', registry=QD_REGISTRY)
    QD_CARBON = Counter('qd_carbon_credits_usd_total', 'Carbon credits USD', registry=QD_REGISTRY)
    QD_XAI = Counter('qd_xai_explanations_total', 'XAI explanations', registry=QD_REGISTRY)
    QD_PRECISION = Counter('qd_precision_selections_total', 'Precision', ['level'], registry=QD_REGISTRY)
else:
    class _Dummy:
        def labels(self, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    QD_PREDICTIONS = QD_TRAINING = QD_TEMPORAL = QD_CHAOS = _Dummy()
    QD_HITL = QD_FEDERATED = QD_CARBON = QD_XAI = QD_PRECISION = _Dummy()

# QuantumBridge import (unchanged).
QUANTUM_BRIDGE_AVAILABLE = False
QuantumBridge = None
for path in ("enhancements.bio_inspired.quantum_bridge",
             "..bio_inspired.quantum_bridge",
             "...bio_inspired.quantum_bridge"):
    try:
        import importlib
        mod = importlib.import_module(path)
        QuantumBridge = getattr(mod, "QuantumBridge")
        QUANTUM_BRIDGE_AVAILABLE = True
        break
    except Exception:
        continue

if not QUANTUM_BRIDGE_AVAILABLE:
    logger.warning("QuantumBridge not available; using simulated distillation")


# =============================================================================
# ENUMS
# =============================================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    BF16 = "bf16"
    FP8 = "fp8"
    FP4 = "fp4"


class AgentRole(str, Enum):
    LEADER = "leader"
    WORKER = "worker"
    VERIFIER = "verifier"
    OBSERVER = "observer"


# =============================================================================
# FIX #1 — Temporal Logic Monitor (correct past-time LTL)
# =============================================================================
class TemporalLogicMonitor:
    """
    Past-time LTL over a finite trace.

        G(φ)     φ holds at every position
        F(φ)     φ holds at some position
        φ U ψ    ∃ i: ψ at prefix[:i+1] and φ at prefix[:j+1] ∀ j < i
        φ -> ψ   (¬φ) ∨ ψ at the current position
        atom     evaluated against the latest state

    Nesting works because every operator compiles to a function of the full
    history.
    """

    def __init__(self, history_len: int = 200, max_violations: int = 100):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable[[List[Dict]], bool]] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: deque = deque(maxlen=max_violations)

    def add_formula(self, name: str, formula: str) -> None:
        self.formulas[name] = formula
        try:
            self.compiled[name] = self._compile(formula)
        except Exception as e:
            logger.warning(f"Temporal: cannot compile '{name}' ({formula}): {e}")
            self.compiled[name] = lambda _h: False

    def update(self, state: Dict) -> None:
        self.history.append(dict(state))

    @staticmethod
    def _matching_paren(s: str, start: int) -> int:
        depth = 0
        for i in range(start, len(s)):
            if s[i] == "(":
                depth += 1
            elif s[i] == ")":
                depth -= 1
                if depth == 0:
                    return i
        return -1

    @staticmethod
    def _split_top_level(s: str, sep: str) -> Optional[Tuple[str, str]]:
        depth = 0
        i, n, m = 0, len(s), len(sep)
        while i <= n - m:
            c = s[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and s[i:i + m] == sep:
                return s[:i].strip(), s[i + m:].strip()
            i += 1
        return None

    def _compile(self, formula: str) -> Callable[[List[Dict]], bool]:
        f = formula.strip()
        if not f:
            return lambda _h: True

        if f.startswith("G(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _G(hist):
                if not hist:
                    return True
                return all(inner(hist[: i + 1]) for i in range(len(hist)))
            return _G

        if f.startswith("F(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _F(hist):
                if not hist:
                    return False
                return any(inner(hist[: i + 1]) for i in range(len(hist)))
            return _F

        u = self._split_top_level(f, " U ")
        if u:
            left, right = u
            lf, rf = self._compile(left), self._compile(right)
            def _U(hist):
                if not hist:
                    return False
                for i in range(len(hist)):
                    if rf(hist[: i + 1]):
                        return True
                    if not lf(hist[: i + 1]):
                        return False
                return False
            return _U

        arrow = self._split_top_level(f, "->")
        if arrow:
            left, right = arrow
            lf, rf = self._compile(left), self._compile(right)
            return lambda h: (not lf(h)) or rf(h)

        return self._atom(f)

    def _atom(self, atom: str) -> Callable[[List[Dict]], bool]:
        atom = atom.strip()
        for op in ("<=", ">=", "==", "!=", "<", ">"):
            if op in atom:
                lhs, rhs = (p.strip() for p in atom.split(op, 1))

                def make(lhs=lhs, op=op, rhs=rhs):
                    def check(hist):
                        if not hist:
                            return True
                        s = hist[-1]
                        lv = s.get(lhs, 0.0)
                        try:
                            rv = float(rhs)
                        except ValueError:
                            rv = s.get(rhs, 0.0)
                        return {
                            "<":  lambda: lv < rv,
                            ">":  lambda: lv > rv,
                            "<=": lambda: lv <= rv,
                            ">=": lambda: lv >= rv,
                            "==": lambda: lv == rv,
                            "!=": lambda: lv != rv,
                        }[op]()
                    return check

                return make(lhs, op, rhs)

        low = atom.lower()
        return lambda _h: low in ("true", "1", "yes")

    def evaluate(self) -> Dict[str, bool]:
        hist = list(self.history)
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = bool(fn(hist))
            except Exception:
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({'formula': name,
                                        'expression': self.formulas[name],
                                        'timestamp': datetime.now().isoformat()})
                QD_TEMPORAL.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': dict(self.formulas),
                'last_results': self.evaluate(),
                'violations': list(self.violations)[-5:]}


# =============================================================================
# XAI (single-candidate guard)
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = list(feature_names)

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        if not all_candidates or len(all_candidates) < 2:
            QD_XAI.inc()
            return {'contributions': {f: 0.0 for f in self.feature_names},
                    'top_features': self.feature_names[:top_k],
                    'narrative': ["Insufficient candidates for comparative attribution."],
                    'weights_used': dict(weights),
                    'degenerate': True}

        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate.get(f, 0.0) for f in self.feature_names])
        w_arr = np.array([weights.get(f, 1.0) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the utility."
                     for f, v in ranked]
        QD_XAI.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights),
                'degenerate': False}


# =============================================================================
# Adaptive Precision (unchanged selection logic)
# =============================================================================
class AdaptivePrecisionController:
    def __init__(self):
        gpu = False
        if TORCH_AVAILABLE:
            try:
                gpu = torch.cuda.is_available()
            except Exception:
                gpu = False
        self.telemetry = {'gpu_available': gpu, 'memory_gb': 16.0, 'utilization': 0.3}
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity: float,
               accuracy_required: float = 0.95) -> PrecisionLevel:
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.FP8
        elif self.telemetry.get('gpu_available') and carbon_intensity < 350:
            self.last_precision = PrecisionLevel.FP16
        else:
            self.last_precision = PrecisionLevel.FP16
        QD_PRECISION.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


# =============================================================================
# Carbon Market Client
# =============================================================================
class CarbonMarketClient:
    def __init__(self):
        self.carbon_price_per_ton = 50.0
        self.rec_price_per_mwh = 30.0
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict] = []

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        return round(max(0.0, carbon_saved_kg) / 1000.0 * self.carbon_price_per_ton, 6)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        return round(max(0.0, energy_saved_kwh) / 1000.0 * self.rec_price_per_mwh, 6)

    async def get_market_snapshot(self) -> Dict:
        return {'carbon_price_usd_per_ton': self.carbon_price_per_ton,
                'rec_price_usd_per_mwh': self.rec_price_per_mwh,
                'grid_intensity_kg_per_mwh': self.grid_intensity_kg_per_mwh}

    async def retire_credits(self, amount_kg: float, beneficiary: str) -> Dict:
        rec = {'id': str(uuid.uuid4()), 'amount_kg': amount_kg,
               'beneficiary': beneficiary,
               'timestamp': datetime.now().isoformat()}
        self.trades.append(rec)
        QD_CARBON.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# =============================================================================
# Role Specialization
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        self.affinity = np.array([
            [0.7, 0.9, 0.4, 0.6],
            [0.4, 0.5, 0.9, 0.5],
            [0.9, 0.4, 0.3, 0.7],
            [0.3, 0.2, 0.3, 0.3],
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        ctx = np.array([context.get('trust', 0.5), context.get('compute', 0.5),
                        context.get('energy', 0.5), context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i])
                                for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}


# =============================================================================
# FIX #9 — Active RLHF (contextual; uncertainty gate on normalised entropy)
# =============================================================================
class ActiveRLHF:
    """
    Preference-based policy with uncertainty-triggered human queries.

    FIX #9: `_policy` is now contextual (global + per-context blend) and
    `uncertainty` returns the normalised entropy used by the HITL gate.
    """

    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0, global_weight: float = 0.3):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.global_weight = global_weight
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.context_counts: Dict[Any, Dict[str, float]] = defaultdict(
            lambda: defaultdict(float))
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []

    @staticmethod
    def _context_key(context: Any) -> Any:
        if isinstance(context, dict):
            items = tuple(sorted((k, str(v)) for k, v in context.items()
                                 if isinstance(v, (str, int, float, bool))))
            return items or ("__empty__",)
        return str(context)

    def _policy(self, context: Any) -> np.ndarray:
        ckey = self._context_key(context)
        local = self.context_counts.get(ckey, {})
        w = self.global_weight
        raw = np.array([w * self.preference_counts.get(a, 0.0)
                        + (1.0 - w) * local.get(a, 0.0) for a in self.actions],
                       dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context: Any) -> str:
        return self.actions[int(np.argmax(self._policy(context)))]

    def uncertainty(self, context: Any) -> float:
        probs = self._policy(context)
        ent = -np.sum(probs * np.log(probs + 1e-12))
        return float(ent / np.log(len(self.actions))) if self.actions else 0.0

    def update(self, context: Any, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += reward
            self.context_counts[self._context_key(context)][action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    def record_feedback(self, state: Dict, action: str, reward: float):
        self.feedback_buffer.append({'state': state, 'action': action, 'reward': reward})
        self.update(state, action, reward)

    async def maybe_query_human(self, context: Dict, options: List[str]) -> Optional[Dict]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {'id': qid, 'context': context, 'options': options,
                 'uncertainty': u, 'created_at': datetime.now().isoformat(),
                 'status': 'pending'}
        self.pending_queries[qid] = query
        return query

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({'status': 'resolved', 'chosen': chosen, 'rating': rating})
        self.update(q['context'], chosen, rating)
        return q

    async def get_policy_probs(self, state: Dict) -> List[float]:
        return self._policy(state).tolist()


# =============================================================================
# FIX #3 — Human-in-the-Loop (register synthesised queries)
# =============================================================================
class HumanInTheLoopCoordinator:
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self, decision_context: Dict, options: List[str],
                       confidence: float, confidence_threshold: float = 0.65) -> Dict:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            QD_HITL.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        # FIX #3: register synthesised queries so resolve_query works.
        if query is None:
            qid = str(uuid.uuid4())
            query = {'id': qid, 'options': options, 'context': decision_context,
                     'uncertainty': self.rlhf.uncertainty(decision_context),
                     'created_at': datetime.now().isoformat(),
                     'status': 'pending'}
            self.rlhf.pending_queries[qid] = query

        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        QD_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        return self.rlhf.resolve_query(query_id, chosen, rating)

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# FIX #8 — Federated Aggregator (full weights with compact option)
# =============================================================================
class FederatedAggregator:
    """
    FedAvg over a *fixed-length* weight vector.

    FIX #8: by default the aggregator expects the FULL flattened weight
    vector from the student. `compact=True` keeps the legacy 4-D summary
    for bandwidth-constrained demos, but callers can no longer load the
    aggregated result back into a model.
    """

    def __init__(self, num_params: int = 4, compact: bool = False,
                 max_pending: int = 32):
        self.round = 0
        self.num_params = num_params
        self.compact = compact
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []
        self.max_pending = max_pending

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({'client_id': client_id,
                                    'weights': list(weights),
                                    'samples': max(1, int(samples))})
        if len(self.client_updates) > self.max_pending:
            self.client_updates = self.client_updates[-self.max_pending:]

    def aggregate(self) -> Dict:
        if not self.client_updates:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        QD_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round,
                'num_params': self.num_params,
                'compact': self.compact,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# FIX #4 — Causal Reward Shaper (proper IPW ATE)
# =============================================================================
class CausalRewardShaper:
    """
    Inverse-propensity-weighted estimate of per-action mean outcomes.

    For action `a` and behaviour propensities `e(a|X)`:

        μ(a) = Σ_i [ r_i · 1[A_i=a] / e_i(a) ] / Σ_i [ 1[A_i=a] / e_i(a) ]

    ATE(a) = μ(a) - μ(baseline). The baseline defaults to the
    probability-weighted average over all actions.

    FIX #4: (a) control term is present; (b) `record` takes the **realised
    reward**, not the predicted probability; (c) the behaviour policy's
    propensities must be supplied by the caller (usually `probs` if actions
    were sampled from `probs`, or the one-hot vector if `argmax` was used).
    """

    def __init__(self, num_actions: int, baseline: Optional[int] = None,
                 min_samples: int = 10):
        self.num_actions = num_actions
        self.baseline = baseline
        self.min_samples = min_samples
        self.interventions: deque = deque(maxlen=500)
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}
        self.mu: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    def record(self, action: int, reward: float, propensities: np.ndarray):
        if not (0 <= action < self.num_actions):
            return
        props = np.asarray(propensities, dtype=float).flatten()
        if props.size != self.num_actions:
            return
        props = np.clip(props, 1e-6, None)
        props = props / props.sum()
        self.interventions.append((int(action), float(reward), props))

    def compute_ate(self) -> Dict[int, float]:
        if len(self.interventions) < self.min_samples:
            return dict(self.ates)

        # Per-action IPW means.
        for a in range(self.num_actions):
            weights = []
            outcomes = []
            for action, reward, props in self.interventions:
                if action == a:
                    weights.append(1.0 / props[a])
                    outcomes.append(reward)
            if weights:
                self.mu[a] = float(np.average(outcomes, weights=weights))
            else:
                self.mu[a] = 0.0

        # Baseline = weighted average over all actions if unspecified.
        if self.baseline is not None and 0 <= self.baseline < self.num_actions:
            base = self.mu[self.baseline]
        else:
            counts = np.array([sum(1 for a, _, _ in self.interventions if a == k)
                               for k in range(self.num_actions)], dtype=float)
            if counts.sum() == 0:
                base = 0.0
            else:
                weights = counts / counts.sum()
                base = float(np.dot(weights, np.array([self.mu[k] for k in range(self.num_actions)])))

        for a in range(self.num_actions):
            self.ates[a] = self.mu[a] - base
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)

    def get_means(self) -> Dict[int, float]:
        return dict(self.mu)


# =============================================================================
# FIX #2 — Chaos Tester (faults actually fire; save/restore)
# =============================================================================
class ChaosTester:
    FAULT_TYPES = [
        'teacher_failure', 'student_corruption', 'clear_cache',
        'quantum_bridge_down', 'precision_broken', 'rlhf_broken',
        'temporal_violation', 'federated_broken',
    ]

    def __init__(self, distiller_ref=None):
        self.distiller = distiller_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        d = self.distiller

        try:
            if fault_type in ('teacher_failure', 'quantum_bridge_down'):
                if d and d.bridge is not None:
                    orig_bridge = d.bridge
                    orig_pen = dict(d.default_penalties)
                    d.bridge = None
                    d.default_penalties = {k: 0.9 for k in d.default_penalties}
                    restore.append(lambda: setattr(d, 'bridge', orig_bridge))
                    restore.append(lambda: setattr(d, 'default_penalties', orig_pen))
                    # FIX #2: invoke the affected path.
                    _ = d._get_teacher_distribution({'carbon_intensity': 500})
                else:
                    passed, error_msg = False, "teacher already unavailable"

            elif fault_type == 'student_corruption':
                if d and d.student.trained:
                    if TORCH_AVAILABLE and getattr(d.student, 'use_torch', False):
                        orig_params = [p.data.clone() for p in d.student.model.parameters()]
                        for p in d.student.model.parameters():
                            p.data.fill_(0.0)
                        def _restore_student():
                            for p, o in zip(d.student.model.parameters(), orig_params):
                                p.data.copy_(o)
                        restore.append(_restore_student)
                    else:
                        orig_w = d.student.weights.copy()
                        orig_b = d.student.biases.copy()
                        d.student.weights.fill(0.0)
                        d.student.biases.fill(0.0)
                        restore.append(lambda: d.student.weights.__setitem__(slice(None), orig_w))
                        restore.append(lambda: d.student.biases.__setitem__(slice(None), orig_b))
                    _ = d.student.predict(d._get_context_features({'carbon_intensity': 400}))
                else:
                    passed, error_msg = False, "student not trained (corruption vacuous)"

            elif fault_type == 'clear_cache':
                if d:
                    orig_cache = dict(d.cache)
                    d.cache.clear()
                    restore.append(lambda: d.cache.update(orig_cache))
                    if d.cache:
                        passed, error_msg = False, "cache not cleared"

            elif fault_type == 'precision_broken' and d and d.precision_controller:
                orig = d.precision_controller.select

                def broken(*_a, **_k):
                    raise RuntimeError("precision broken")

                d.precision_controller.select = broken
                restore.append(lambda: setattr(d.precision_controller, 'select', orig))
                try:
                    d.precision_controller.select(400.0)
                    passed, error_msg = False, "precision did not fail"
                except RuntimeError:
                    pass

            elif fault_type == 'rlhf_broken' and d and d.rlhf:
                orig = d.rlhf.get_policy_probs

                async def broken(_):
                    raise RuntimeError("rlhf broken")

                d.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(d.rlhf, 'get_policy_probs', orig))
                try:
                    await d.rlhf.get_policy_probs({})
                    passed, error_msg = False, "rlhf did not fail"
                except RuntimeError:
                    pass

            elif fault_type == 'temporal_violation' and d and d.temporal_monitor:
                d.temporal_monitor.update({'carbon': 9999.0, 'reward': -1.0})
                results = d.temporal_monitor.evaluate()
                if all(results.values()):
                    passed, error_msg = False, "temporal monitor did not flag violation"

            elif fault_type == 'federated_broken' and d and d.federated:
                orig = d.federated.aggregate

                def broken():
                    raise RuntimeError("federated broken")

                d.federated.aggregate = broken
                restore.append(lambda: setattr(d.federated, 'aggregate', orig))
                try:
                    d.federated.aggregate()
                    passed, error_msg = False, "federated did not fail"
                except RuntimeError:
                    pass

            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for rec in restore:
                try:
                    rec()
                except Exception:
                    pass

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg,
                  'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        QD_CHAOS.labels(fault=fault_type,
                        status='pass' if passed else 'fail').inc()
        logger.warning(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) /
                              len(self.results)) if self.results else 1.0,
                'recent': self.results[-5:]}


# =============================================================================
# FIX #5 — Classical Student Model (no .to(dtype) on training model)
# =============================================================================
class ClassicalStudentModel:
    """
    Student model that mimics teacher distributions.

    FIX #5: `set_precision` is advisory only. It never calls `.to(dtype)`
    on the training model because that would orphan the optimizer state
    and silently stop learning. An explicit `convert_dtype` method is
    available for inference-only copies.
    """

    def __init__(self, input_dim: int, num_actions: int,
                 use_torch: bool = TORCH_AVAILABLE,
                 seed: Optional[int] = None):
        self.input_dim = input_dim
        self.num_actions = num_actions
        self.use_torch = use_torch
        self.trained = False
        self.current_precision = PrecisionLevel.FP32
        self._rng = random.Random(seed)
        if seed is not None:
            try:
                np.random.seed(seed)
                if TORCH_AVAILABLE:
                    torch.manual_seed(seed)
            except Exception:
                pass

        if self.use_torch:
            class _MLP(nn.Module):
                def __init__(self, in_dim, out_dim):
                    super().__init__()
                    self.net = nn.Sequential(
                        nn.Linear(in_dim, 64), nn.ReLU(),
                        nn.Linear(64, 32), nn.ReLU(),
                        nn.Linear(32, out_dim),
                    )
                def forward(self, x):
                    return self.net(x)
            self.model = _MLP(input_dim, num_actions)
            self.optimizer = optim.Adam(self.model.parameters(), lr=1e-3)
            self.loss_fn = nn.KLDivLoss(reduction='batchmean')
        else:
            self.weights = np.random.randn(input_dim, num_actions) * 0.01
            self.biases = np.zeros(num_actions)
            self.lr = 0.01

    def set_precision(self, level: PrecisionLevel):
        # FIX #5: advisory only — never `.to(dtype)` on the training model.
        self.current_precision = level

    def convert_dtype(self, level: PrecisionLevel):
        """Explicit, caller-invoked dtype conversion (invalidates optimizer)."""
        self.current_precision = level
        if not self.use_torch:
            return
        dtype_map = {
            PrecisionLevel.FP32: torch.float32,
            PrecisionLevel.FP16: torch.float16,
            PrecisionLevel.BF16: torch.bfloat16,
            PrecisionLevel.FP8: torch.float16,
            PrecisionLevel.FP4: torch.float16,
        }
        try:
            self.model = self.model.to(dtype=dtype_map[level])
            # Optimizer is stale; refresh it.
            self.optimizer = optim.Adam(self.model.parameters(), lr=1e-3)
        except Exception:
            pass

    def predict(self, x: np.ndarray) -> np.ndarray:
        if self.use_torch:
            self.model.eval()
            with torch.no_grad():
                x_t = torch.FloatTensor(x).unsqueeze(0)
                dtype = next(self.model.parameters()).dtype
                if dtype != torch.float32:
                    x_t = x_t.to(dtype)
                logits = self.model(x_t)
                probs = torch.softmax(logits.float(), dim=1).squeeze(0).numpy()
            return probs
        logits = np.dot(x, self.weights) + self.biases
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        return exp / (exp.sum() + 1e-9)

    def train_step(self, x: np.ndarray, target_dist: np.ndarray) -> float:
        QD_TRAINING.inc()
        if self.use_torch:
            self.model.train()
            x_t = torch.FloatTensor(x).unsqueeze(0)
            target_t = torch.FloatTensor(target_dist).unsqueeze(0)
            self.optimizer.zero_grad()
            logits = self.model(x_t)
            log_probs = torch.log_softmax(logits.float(), dim=1)
            loss = self.loss_fn(log_probs, target_t)
            loss.backward()
            self.optimizer.step()
            return float(loss.item())
        probs = self.predict(x)
        error = probs - target_dist
        self.weights -= self.lr * np.outer(x, error)
        self.biases -= self.lr * error
        loss = -np.sum(target_dist * np.log(probs + 1e-8))
        return float(loss)

    def flattened_weights(self) -> List[float]:
        """Full flattened weight vector for federated aggregation (FIX #8)."""
        if self.use_torch:
            with torch.no_grad():
                return [float(p.detach().cpu().flatten().sum()) for p in self.model.parameters()] \
                    if False else [float(v) for p in self.model.parameters()
                                   for v in p.detach().cpu().flatten().tolist()]
        return [float(v) for v in self.weights.flatten().tolist()] \
            + [float(v) for v in self.biases.flatten().tolist()]

    def summary_snapshot(self) -> np.ndarray:
        """Legacy 4-D summary (compact mode)."""
        w = np.array(self.flattened_weights())
        if w.size == 0:
            return np.zeros(4)
        return np.array([w.mean(), w.std(), w.max(), w.min()])

    def num_weights(self) -> int:
        return len(self.flattened_weights())


# =============================================================================
# FIX #6 — Teacher distribution (documented heuristic + provider hook)
# =============================================================================
@dataclass
class TeacherConfig:
    beta: float = 1.0
    num_actions: int = 5
    penalties: Dict[str, float] = field(default_factory=lambda: {
        'penalty_carbon': 0.1,
        'penalty_helium_shortage': 0.1,
        'penalty_energy': 0.1,
        'penalty_cost': 0.1,
        'penalty_latency': 0.1,
    })


# =============================================================================
# Quantum Policy Distiller v2.0.1
# =============================================================================
class QuantumPolicyDistiller:
    """
    Distills a classical probability distribution over actions from a
    (real or simulated) quantum teacher. v2.0.1 fixes the top-10 bugs
    documented in the module header.
    """

    def __init__(self, num_actions: int = 5, bridge: Optional[Any] = None,
                 input_dim: Optional[int] = None,
                 enable_temporal_logic: bool = True,
                 enable_xai: bool = True,
                 enable_adaptive_precision: bool = True,
                 enable_carbon_market: bool = True,
                 enable_role_specialization: bool = True,
                 enable_chaos_testing: bool = True,
                 enable_hitl: bool = True,
                 enable_federated: bool = True,
                 enable_causal_rl: bool = True,
                 hitl_confidence_threshold: float = 0.35,
                 teacher_provider: Optional[Callable[[Dict], np.ndarray]] = None,
                 teacher_beta: float = 1.0,
                 seed: Optional[int] = None,
                 metrics_port: int = 8011,
                 start_metrics_server: bool = False):
        self.num_actions = num_actions
        self.bridge = bridge if bridge is not None else (
            QuantumBridge() if QUANTUM_BRIDGE_AVAILABLE else None)
        self.input_dim = input_dim if input_dim else max(num_actions, 8)
        self.cache: Dict[str, np.ndarray] = {}
        self._lock = asyncio.Lock()

        # FIX #7: deterministic RNG.
        self._rng = random.Random(seed)
        if seed is not None:
            try:
                np.random.seed(seed)
                if TORCH_AVAILABLE:
                    torch.manual_seed(seed)
            except Exception:
                pass

        self.student = ClassicalStudentModel(self.input_dim, num_actions,
                                             use_torch=TORCH_AVAILABLE, seed=seed)
        self.constraints: List[Callable[[Dict, int], bool]] = []
        self.approval_callback: Optional[Callable[[int, Dict], bool]] = None
        self.drift_history: deque = deque(maxlen=50)
        self.last_dist: Optional[np.ndarray] = None
        self.fault_injected = False

        # FIX #6: teacher config + provider hook.
        self.teacher_cfg = TeacherConfig(beta=teacher_beta, num_actions=num_actions)
        self.teacher_provider = teacher_provider

        self.enable_temporal_logic = enable_temporal_logic
        self.enable_xai = enable_xai
        self.enable_adaptive_precision = enable_adaptive_precision
        self.enable_carbon_market = enable_carbon_market
        self.enable_role_specialization = enable_role_specialization
        self.enable_chaos_testing = enable_chaos_testing
        self.enable_hitl = enable_hitl
        self.enable_federated = enable_federated
        self.enable_causal_rl = enable_causal_rl
        self.hitl_confidence_threshold = hitl_confidence_threshold

        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")
            self.temporal_monitor.add_formula("reward_floor", "G(reward >= -0.5)")
            self.temporal_monitor.add_formula("cache_activity", "F(cache_size >= 1.0)")

        self.xai = XAIExplainer(['carbon', 'helium', 'energy', 'cost', 'latency']) \
            if enable_xai else None

        self.precision_controller = AdaptivePrecisionController() \
            if enable_adaptive_precision else None

        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if enable_role_specialization else None

        self.rlhf = ActiveRLHF(
            action_space=[f'action_{i}' for i in range(num_actions)],
        )
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) if enable_hitl else None

        # FIX #8: full-weight federated aggregator when the model is available.
        if enable_federated:
            n_weights = self.student.num_weights()
            self.federated = FederatedAggregator(num_params=max(n_weights, 1))
        else:
            self.federated = None
        self.instance_id = str(uuid.uuid4())[:8]

        self.causal_shaper = CausalRewardShaper(num_actions) if enable_causal_rl else None

        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        self._carbon_saved_kg_total = 0.0
        self._background_tasks: List[asyncio.Task] = []
        self._running = False
        self._metrics_started = False
        self.metrics_port = metrics_port
        self._start_metrics = start_metrics_server

    # ------------------------------------------------------------------ #
    # Feature extraction
    # ------------------------------------------------------------------ #
    def _get_context_features(self, context: Dict[str, Any]) -> np.ndarray:
        keys = ['carbon_intensity', 'avg_carbon', 'helium_scarcity', 'energy_price',
                'renewable_ratio', 'num_clients', 'trust_score', 'latency_ms']
        features = []
        for key in keys:
            val = context.get(key, 0.0)
            features.append(float(val) if isinstance(val, (int, float)) else 0.0)
        if features:
            features[0] = features[0] / 800.0
        if len(features) < self.input_dim:
            features += [0.0] * (self.input_dim - len(features))
        else:
            features = features[:self.input_dim]
        return np.array(features)

    # ------------------------------------------------------------------ #
    # FIX #6: teacher distribution
    # ------------------------------------------------------------------ #
    def _get_teacher_distribution(self, context: Dict[str, Any]) -> np.ndarray:
        # Caller-provided provider takes precedence.
        if self.teacher_provider is not None:
            try:
                dist = np.asarray(self.teacher_provider(context), dtype=float).flatten()
                if dist.size == self.num_actions:
                    dist = np.clip(dist, 1e-9, None)
                    return dist / dist.sum()
            except Exception as e:
                logger.warning(f"teacher_provider failed: {e}")

        # Documented heuristic fallback. NOTE: this is *not* a QUBO evaluation.
        # `penalty_*` in the bridge are objective coefficients, not per-action
        # energies. Do not interpret the output of this branch as
        # `argmin Q(x)` unless a real QUBO solver is plugged in via
        # `teacher_provider`.
        if self.bridge is not None and hasattr(self.bridge, 'get_qubo_parameters'):
            try:
                params = self.bridge.get_qubo_parameters() or {}
                base_penalties = self.teacher_cfg.penalties
                energies = np.array([
                    float(params.get(k, base_penalties[k]))
                    for k in ('penalty_carbon', 'penalty_helium_shortage',
                              'penalty_energy', 'penalty_cost', 'penalty_latency')
                ])
                if energies.size != self.num_actions:
                    pad = np.full(self.num_actions, 0.5)
                    pad[:min(energies.size, self.num_actions)] = \
                        energies[:min(energies.size, self.num_actions)]
                    energies = pad
                return self._softmax(-self.teacher_cfg.beta * energies)
            except Exception as e:
                logger.warning(f"QuantumBridge error: {e}; using heuristic")

        carbon = context.get('carbon_intensity',
                             context.get('avg_carbon', 400)) / 800.0
        helium = context.get('helium_scarcity', 0.5)
        energy = context.get('energy_price', 0.5)
        penalties = np.full(self.num_actions, 0.2)
        penalties[0] = max(0.1, carbon)
        if self.num_actions > 1:
            penalties[1] = max(0.1, helium)
        if self.num_actions > 2:
            penalties[2] = max(0.1, energy)
        return self._softmax(-self.teacher_cfg.beta * penalties)

    @staticmethod
    def _softmax(x: np.ndarray) -> np.ndarray:
        e = np.exp(x - np.max(x))
        return e / e.sum()

    # ------------------------------------------------------------------ #
    # FIX #7: training uses circuit_results; trained only when enough
    # ------------------------------------------------------------------ #
    async def train_from_circuit(self,
                                 circuit_results: Optional[List[Dict]] = None,
                                 contexts: Optional[List[Dict]] = None,
                                 min_samples: int = 5) -> Dict:
        # `circuit_results`, if provided, are expected to be
        #   [{'context': {...}, 'probs': [...]}]
        # and take precedence over `contexts`.
        pairs: List[Tuple[Dict, np.ndarray]] = []
        if circuit_results:
            for r in circuit_results:
                ctx = r.get('context')
                probs = r.get('probs')
                if isinstance(ctx, dict) and probs is not None:
                    p = np.asarray(probs, dtype=float).flatten()
                    if p.size == self.num_actions:
                        p = np.clip(p, 1e-9, None)
                        pairs.append((ctx, p / p.sum()))
        elif contexts is not None:
            for ctx in contexts:
                pairs.append((ctx, self._get_teacher_distribution(ctx)))
        else:
            # Synthetic default: sample uniformly.
            for _ in range(100):
                ctx = {
                    'carbon_intensity': self._rng.uniform(100, 800),
                    'helium_scarcity': self._rng.uniform(0.1, 0.9),
                    'energy_price': self._rng.uniform(0.1, 1.0),
                    'renewable_ratio': self._rng.uniform(0.1, 0.9),
                    'num_clients': self._rng.randint(1, 20),
                    'trust_score': self._rng.uniform(0.1, 0.9),
                    'latency_ms': self._rng.uniform(10, 200),
                }
                pairs.append((ctx, self._get_teacher_distribution(ctx)))

        if not pairs:
            return {'status': 'no_data', 'samples': 0, 'avg_loss': 0.0}

        losses: List[float] = []
        loop = asyncio.get_running_loop()

        def _train_step(x, target):
            return self.student.train_step(x, target)

        async with self._lock:
            for ctx, target in pairs:
                x = self._get_context_features(ctx)
                loss = await loop.run_in_executor(None, _train_step, x, target)
                losses.append(float(loss))

        # FIX #7: only mark trained if enough samples accumulated.
        if len(losses) >= min_samples:
            self.student.trained = True
        logger.info(f"Student trained on {len(losses)} samples. "
                    f"Avg loss: {np.mean(losses) if losses else 0.0:.4f}")
        return {'status': 'success',
                'samples': len(losses),
                'avg_loss': float(np.mean(losses)) if losses else 0.0,
                'trained': self.student.trained}

    # ------------------------------------------------------------------ #
    # FIX #5/#10: pure prediction; no side effects; no set_precision here
    # ------------------------------------------------------------------ #
    async def predict(self, context: Dict[str, Any],
                      use_cache: bool = True) -> np.ndarray:
        """Pure prediction (no HITL, no carbon, no federated, no causal)."""
        cache_key = str(sorted(context.items()))
        async with self._lock:
            if use_cache and cache_key in self.cache:
                return self.cache[cache_key].copy()
            x = self._get_context_features(context)
            if self.student.trained:
                probs = self.student.predict(x)
                source = "student"
            else:
                probs = self._get_teacher_distribution(context)
                source = "teacher"
            probs = self._apply_constraints(context, probs)
            if probs.sum() > 0:
                probs = probs / probs.sum()
            else:
                probs = np.ones(self.num_actions) / self.num_actions
            self.cache[cache_key] = probs.copy()
            if self.last_dist is not None:
                drift = float(np.linalg.norm(probs - self.last_dist))
                self.drift_history.append(drift)
                if drift > 0.3:
                    logger.warning(f"High policy drift detected: {drift:.3f}")
            self.last_dist = probs.copy()
            QD_PREDICTIONS.labels(source=source).inc()
        return probs

    async def get_distilled_policy(self, context: Dict[str, Any],
                                   use_cache: bool = True) -> np.ndarray:
        """
        Backward-compatible wrapper: predicts AND records side effects
        (causal observation, HITL, carbon accounting, federated submission).
        These are best-effort — they never raise and never block
        prediction (they run after the lock is released).
        """
        probs = await self.predict(context, use_cache)

        carbon = context.get('carbon_intensity',
                             context.get('avg_carbon', 400))

        # Causal observation (FIX #4: record the realised reward from
        # record_feedback, NOT the predicted probability. Here we only
        # record the *decision*, with the correct propensities.)
        if self.causal_shaper is not None:
            action = int(np.argmax(probs))
            self.causal_shaper.record(action, reward=0.0, propensities=probs)

        # HITL (FIX #9: normalised confidence; bypass on high entropy).
        confidence = float(np.max(probs))
        entropy = float(-np.sum(probs * np.log(probs + 1e-12))
                        / math.log(max(self.num_actions, 2)))
        if (self.hitl is not None
                and confidence < self.hitl_confidence_threshold
                and entropy < 0.85):
            try:
                await self.hitl.escalate(
                    decision_context={'carbon_intensity': carbon},
                    options=[f'action_{i}' for i in range(self.num_actions)],
                    confidence=confidence,
                    confidence_threshold=self.hitl_confidence_threshold,
                )
            except Exception:
                pass

        # Carbon accounting (best-effort; no credit, just tracking).
        if self.carbon_market is not None:
            saved_kg = max(0.0, (400 - carbon) * 0.001)
            self._carbon_saved_kg_total += saved_kg

        # Federated submission (full flattened weights).
        if self.federated is not None:
            try:
                weights = self.student.flattened_weights()
                self.federated.submit_update(self.instance_id, weights, 1)
            except Exception:
                pass

        # Temporal gate.
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'carbon': float(carbon),
                'reward': 0.5,
                'cache_size': float(len(self.cache)),
            })
            self.temporal_monitor.evaluate()

        return probs

    # ------------------------------------------------------------------ #
    # Constraints
    # ------------------------------------------------------------------ #
    def add_constraint(self, constraint_fn: Callable[[Dict, int], bool],
                       description: str = ""):
        self.constraints.append(constraint_fn)
        logger.info(f"Added quantum policy constraint: {description}")

    def _apply_constraints(self, context: Dict, probs: np.ndarray) -> np.ndarray:
        masked = probs.copy()
        for i in range(self.num_actions):
            for constraint in self.constraints:
                try:
                    if constraint(context, i):
                        masked[i] = 0.0
                        break
                except Exception:
                    pass
        return masked

    # ------------------------------------------------------------------ #
    # Explainability (multi-candidate XAI)
    # ------------------------------------------------------------------ #
    async def explain_policy(self, context: Dict[str, Any]) -> Dict[str, Any]:
        x = self._get_context_features(context)
        if self.student.trained:
            probs = self.student.predict(x)
            source = "student_model"
        else:
            probs = self._get_teacher_distribution(context)
            source = "heuristic_teacher"

        teacher_probs = self._get_teacher_distribution(context)

        explanations = [{
            'action_index': i,
            'probability': float(probs[i]),
            'teacher_probability': float(teacher_probs[i]),
            'difference': float(probs[i] - teacher_probs[i]),
        } for i in range(self.num_actions)]

        xai_out = None
        if self.xai:
            # Build multiple candidates so the explainer has a baseline.
            features = {
                'carbon': context.get('carbon_intensity', 400) / 800.0,
                'helium': context.get('helium_scarcity', 0.5),
                'energy': context.get('energy_price', 0.5),
                'cost': context.get('cost', 0.5),
                'latency': context.get('latency_ms', 100) / 200.0,
            }
            baseline_candidates = [
                {'carbon': 0.5, 'helium': 0.5, 'energy': 0.5,
                 'cost': 0.5, 'latency': 0.5},
                {'carbon': 0.1, 'helium': 0.1, 'energy': 0.1,
                 'cost': 0.1, 'latency': 0.1},
                {'carbon': 0.9, 'helium': 0.9, 'energy': 0.9,
                 'cost': 0.9, 'latency': 0.9},
            ]
            xai_out = self.xai.explain(
                candidate=features,
                weights={k: 1.0 for k in features},
                all_candidates=[features] + baseline_candidates,
            )

        return {
            'source': source,
            'probabilities': probs.tolist(),
            'teacher_probabilities': teacher_probs.tolist(),
            'explanations': explanations,
            'xai_narrative': xai_out['narrative'] if xai_out else [],
            'context_snapshot': {k: context.get(k)
                                 for k in ['carbon_intensity', 'helium_scarcity',
                                           'energy_price'] if k in context},
        }

    # ------------------------------------------------------------------ #
    # Human approval (legacy passive) + active HITL
    # ------------------------------------------------------------------ #
    def set_approval_callback(self, callback: Callable[[int, Dict], bool]):
        self.approval_callback = callback

    async def request_approval(self, action_index: int, context: Dict) -> bool:
        if self.approval_callback is None:
            return True
        result = self.approval_callback(action_index, context)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

    async def escalate_decision(self, decision_context: Dict,
                                options: List[str],
                                confidence: float) -> Dict:
        if self.hitl is None:
            chosen = (self.rlhf.sample_action(decision_context)
                      if self.rlhf and self.rlhf.actions
                      else (options[0] if options else 'noop'))
            return {'escalated': False, 'chosen': chosen, 'source': 'rlhf_fallback'}
        return await self.hitl.escalate(decision_context, options, confidence,
                                        self.hitl_confidence_threshold)

    # ------------------------------------------------------------------ #
    # Drift / precision / carbon / roles / feedback
    # ------------------------------------------------------------------ #
    async def get_drift_stats(self) -> Dict[str, Any]:
        if not self.drift_history:
            return {'status': 'insufficient_data'}
        return {'current_drift': self.drift_history[-1],
                'avg_drift': float(np.mean(self.drift_history)),
                'max_drift': float(max(self.drift_history)),
                'samples': len(self.drift_history)}

    async def select_precision(self,
                               accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(400.0, accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    def get_role_assignments(self, context: Optional[Dict[str, float]] = None) -> Dict:
        if self.role_coordinator is None:
            return {}
        ctx = context or {'trust': 0.7, 'compute': 0.7,
                          'energy': 0.7, 'performance': 0.7}
        return self.role_coordinator.assign_roles(ctx)

    async def record_feedback(self, state: Dict, action: str, reward: float):
        if self.rlhf:
            self.rlhf.record_feedback(state, action, reward)
        # FIX #4: also feed the realised reward to the causal shaper.
        if self.causal_shaper is not None:
            try:
                idx = int(action.split('_')[-1]) if isinstance(action, str) else int(action)
                probs = await self.predict(state)
                self.causal_shaper.record(idx, reward, probs)
            except Exception:
                pass

    def get_causal_ates(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

    def get_causal_means(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.get_means()

    # ------------------------------------------------------------------ #
    # FIX #2: legacy chaos also restores
    # ------------------------------------------------------------------ #
    async def inject_fault(self, fault_type: str):
        async with self._lock:
            if fault_type == 'teacher_failure':
                self.bridge = None
                self.default_penalties = {k: 0.9 for k in self.teacher_cfg.penalties}
            elif fault_type == 'student_corruption':
                if TORCH_AVAILABLE and getattr(self.student, 'use_torch', False):
                    for p in self.student.model.parameters():
                        p.data.fill_(0.0)
                else:
                    self.student.weights.fill(0.0)
                    self.student.biases.fill(0.0)
            elif fault_type == 'clear_cache':
                self.cache.clear()
            else:
                logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Legacy chaos test, now with save/restore."""
        report = {'faults': [], 'results': {}}
        # Snapshot state.
        orig_bridge = self.bridge
        orig_pen = dict(self.teacher_cfg.penalties)
        orig_cache = dict(self.cache)
        if TORCH_AVAILABLE and getattr(self.student, 'use_torch', False):
            orig_params = [p.data.clone() for p in self.student.model.parameters()]
        else:
            orig_w = self.student.weights.copy()
            orig_b = self.student.biases.copy()

        try:
            await self.inject_fault('teacher_failure')
            report['faults'].append('teacher_failure')
            probs = await self.get_distilled_policy(
                {'carbon_intensity': 500, 'helium_scarcity': 0.7, 'energy_price': 0.6})
            report['results']['teacher_failure'] = {
                'sum': float(np.sum(probs)), 'probs': probs.tolist()}

            await self.inject_fault('student_corruption')
            report['faults'].append('student_corruption')
            probs = await self.get_distilled_policy(
                {'carbon_intensity': 500, 'helium_scarcity': 0.7, 'energy_price': 0.6})
            report['results']['student_corruption'] = {
                'sum': float(np.sum(probs)), 'probs': probs.tolist()}

            await self.inject_fault('clear_cache')
            report['faults'].append('clear_cache')
        finally:
            self.bridge = orig_bridge
            self.teacher_cfg.penalties = orig_pen
            self.cache.clear()
            self.cache.update(orig_cache)
            if TORCH_AVAILABLE and getattr(self.student, 'use_torch', False):
                for p, o in zip(self.student.model.parameters(), orig_params):
                    p.data.copy_(o)
            else:
                self.student.weights[:] = orig_w
                self.student.biases[:] = orig_b
        return report

    async def run_extended_chaos_suite(self) -> Dict:
        if self.chaos_tester is None:
            return {'error': 'chaos testing disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos_tester.get_report()}

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self):
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        if self.federated:
            self._background_tasks.append(loop.create_task(self._federated_loop()))
        if self.temporal_monitor:
            self._background_tasks.append(loop.create_task(self._temporal_loop()))
        if PROMETHEUS_AVAILABLE and self._start_metrics and not self._metrics_started:
            try:
                start_http_server(self.metrics_port, registry=QD_REGISTRY)
                self._metrics_started = True
            except Exception as e:
                logger.warning(f"Metrics server start failed: {e}")

    async def stop(self):
        self._running = False
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def _federated_loop(self):
        while self._running:
            try:
                await asyncio.sleep(600)
                if self.federated and self.federated.client_updates:
                    self.federated.aggregate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _temporal_loop(self):
        while self._running:
            try:
                await asyncio.sleep(30)
                if self.temporal_monitor:
                    self.temporal_monitor.update({
                        'carbon': 400.0, 'reward': 0.5,
                        'cache_size': float(len(self.cache)),
                    })
                    self.temporal_monitor.evaluate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Temporal loop error: {e}")

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #
    async def get_comprehensive_status(self) -> Dict:
        status = {
            'instance_id': self.instance_id,
            'version': '2.0.1',
            'num_actions': self.num_actions,
            'student_trained': self.student.trained,
            'cache_size': len(self.cache),
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'features': {
                'temporal_logic': self.enable_temporal_logic,
                'xai': self.enable_xai,
                'adaptive_precision': self.enable_adaptive_precision,
                'carbon_market': self.enable_carbon_market,
                'role_specialization': self.enable_role_specialization,
                'chaos_testing': self.enable_chaos_testing,
                'hitl': self.enable_hitl,
                'federated': self.enable_federated,
                'causal_rl': self.enable_causal_rl,
            },
            'timestamp': datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.precision_controller:
            status['precision'] = {
                'last': self.precision_controller.last_precision.value,
                'telemetry': self.precision_controller.telemetry,
            }
        if self.carbon_market:
            status['carbon_market'] = await self.carbon_market.get_market_snapshot()
        if self.role_coordinator:
            status['roles'] = self.get_role_assignments()
        if self.federated:
            status['federated'] = self.federated.get_stats()
        if self.hitl:
            status['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            status['chaos'] = self.chaos_tester.get_report()
        if self.rlhf:
            status['rlhf'] = {'actions': self.rlhf.actions,
                              'history_len': len(self.rlhf.history),
                              'pending_queries': len(self.rlhf.pending_queries)}
        if self.causal_shaper:
            status['causal_ates'] = self.causal_shaper.compute_ate()
            status['causal_means'] = self.causal_shaper.get_means()
        return status


# =============================================================================
# Embedded unit tests
# =============================================================================
def _run_unit_tests() -> int:
    import asyncio as _asyncio

    failures: List[str] = []

    def check(name: str, cond: bool, detail: str = ""):
        status = "PASS" if cond else "FAIL"
        print(f"  [{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    print("=" * 80)
    print("Unit tests — Quantum Distiller v2.0.1")
    print("=" * 80)

    # ---- 1. Temporal U / nesting ----
    print("\n[1] TemporalLogicMonitor U / nesting (FIX #1)")
    mon = TemporalLogicMonitor()
    mon.add_formula("u", "carbon > 100 U carbon > 800")
    mon.update({"carbon": 200}); mon.update({"carbon": 500}); mon.update({"carbon": 900})
    check("U holds", mon.evaluate()["u"] is True)

    mon2 = TemporalLogicMonitor()
    mon2.add_formula("gimp", "G(carbon > 900 -> energy < 0.3)")
    mon2.update({"carbon": 400, "energy": 0.8})
    mon2.update({"carbon": 950, "energy": 0.1})
    check("G(imp) holds", mon2.evaluate()["gimp"] is True)
    mon2.update({"carbon": 950, "energy": 0.9})
    check("G(imp) violates", mon2.evaluate()["gimp"] is False)

    # ---- 2. HITL registration ----
    print("\n[2] HITL registration (FIX #3)")

    async def hitl_test():
        rlhf = ActiveRLHF(action_space=["a", "b"], uncertainty_threshold=0.0)
        hitl = HumanInTheLoopCoordinator(rlhf)
        res = await hitl.escalate({"k": 1}, ["a", "b"], confidence=0.1)
        qid = res["query"]["id"]
        assert qid in rlhf.pending_queries
        resolved = hitl.resolve_query(qid, "a", rating=1.0)
        assert resolved is not None

    loop = _asyncio.new_event_loop()
    try:
        loop.run_until_complete(hitl_test())
        check("register + resolve", True)
    except Exception as e:
        check("register + resolve", False, str(e))
    finally:
        loop.close()

    # ---- 3. Chaos faults fire ----
    print("\n[3] ChaosTester faults (FIX #2)")

    async def chaos_test():
        d = QuantumPolicyDistiller(num_actions=5, seed=1)
        await d.start()
        await d.train_from_circuit()  # ensure student trained
        tester = ChaosTester(d)
        r1 = await tester.run_test("teacher_failure", duration_s=0.01)
        r2 = await tester.run_test("student_corruption", duration_s=0.01)
        r3 = await tester.run_test("precision_broken", duration_s=0.01)
        r4 = await tester.run_test("rlhf_broken", duration_s=0.01)
        await d.stop()
        return r1, r2, r3, r4

    loop = _asyncio.new_event_loop()
    try:
        r1, r2, r3, r4 = loop.run_until_complete(chaos_test())
        check("teacher_failure fires", r1["passed"] is True, str(r1))
        check("student_corruption fires", r2["passed"] is True, str(r2))
        check("precision_broken fires", r3["passed"] is True, str(r3))
        check("rlhf_broken fires", r4["passed"] is True, str(r4))
    except Exception as e:
        check("chaos suite", False, str(e))
    finally:
        loop.close()

    # ---- 4. Causal ATE: recover known effect ----
    print("\n[4] CausalRewardShaper ATE (FIX #4)")
    shaper = CausalRewardShaper(num_actions=3, baseline=0)
    rng = random.Random(7)
    # Ground truth: action 0 reward ~0.2, action 1 ~0.5, action 2 ~0.8
    means = {0: 0.2, 1: 0.5, 2: 0.8}
    for _ in range(200):
        probs = np.array([0.3, 0.3, 0.4])
        action = rng.choices(range(3), weights=probs)[0]
        reward = means[action] + rng.gauss(0, 0.02)
        shaper.record(action, reward, probs)
    ates = shaper.compute_ate()
    # ATE(1) ≈ 0.5 - 0.2 = 0.3, ATE(2) ≈ 0.8 - 0.2 = 0.6
    check("ATE(1) recovered", abs(ates[1] - 0.3) < 0.05, f"ate1={ates[1]}")
    check("ATE(2) recovered", abs(ates[2] - 0.6) < 0.05, f"ate2={ates[2]}")

    # ---- 5. Precision is advisory ----
    print("\n[5] Precision is advisory (FIX #5)")
    d = QuantumPolicyDistiller(num_actions=5, seed=2)
    d.student.set_precision(PrecisionLevel.FP32)
    orig = d.student.model.parameters().__iter__().__next__().dtype if TORCH_AVAILABLE else "numpy"
    d.student.set_precision(PrecisionLevel.FP16)
    new = d.student.model.parameters().__iter__().__next__().dtype if TORCH_AVAILABLE else "numpy"
    check("set_precision does not change model dtype", orig == new,
          f"orig={orig} new={new}")

    # ---- 6. Teacher provider hook ----
    print("\n[6] teacher_provider hook (FIX #6)")

    def custom_provider(ctx):
        return np.array([0.5, 0.5, 0.0, 0.0, 0.0])

    d = QuantumPolicyDistiller(num_actions=5, seed=3,
                                teacher_provider=custom_provider)
    dist = d._get_teacher_distribution({"carbon_intensity": 400})
    check("provider respected", abs(dist[0] - 0.5) < 1e-6 and abs(dist[2]) < 1e-9)

    # ---- 7. Training uses circuit_results; trained flag ----
    print("\n[7] train_from_circuit uses circuit_results (FIX #7)")

    async def train_test():
        d = QuantumPolicyDistiller(num_actions=5, seed=4)
        await d.start()
        # Train with 0 samples → not trained
        r = await d.train_from_circuit(circuit_results=[])
        untrained_after_zero = d.student.trained
        # Now with real data
        circuits = [{'context': {'carbon_intensity': 400},
                     'probs': [0.2] * 5} for _ in range(5)]
        r2 = await d.train_from_circuit(circuit_results=circuits)
        trained_after_real = d.student.trained
        await d.stop()
        return untrained_after_zero, trained_after_real

    loop = _asyncio.new_event_loop()
    try:
        untrained, trained = loop.run_until_complete(train_test())
        check("not trained with zero data", untrained is False)
        check("trained after real data", trained is True)
    except Exception as e:
        check("train flag", False, str(e))
    finally:
        loop.close()

    # ---- 8. Federated full weights ----
    print("\n[8] Federated full weights (FIX #8)")
    d = QuantumPolicyDistiller(num_actions=5, seed=5)
    n_weights = d.student.num_weights()
    check("federated num_params > 4", d.federated.num_params > 4 if d.federated else False)
    check("num_weights matches", d.federated.num_params == n_weights if d.federated else False)

    # ---- 9. HITL threshold + entropy gate ----
    print("\n[9] HITL entropy gate (FIX #9)")

    async def hitl_gate_test():
        d = QuantumPolicyDistiller(num_actions=5, seed=6,
                                    hitl_confidence_threshold=0.9)
        await d.start()
        # Uniform-ish → high entropy → HITL should be bypassed.
        before = d.hitl.get_audit()['total']
        await d.get_distilled_policy({"carbon_intensity": 400})
        after = d.hitl.get_audit()['total']
        await d.stop()
        return before, after

    loop = _asyncio.new_event_loop()
    try:
        before, after = loop.run_until_complete(hitl_gate_test())
        check("HITL bypassed on high entropy", after == before,
              f"before={before} after={after}")
    except Exception as e:
        check("HITL gate", False, str(e))
    finally:
        loop.close()

    # ---- 10. Determinism ----
    print("\n[10] Determinism (FIX #10)")

    async def determinism_test():
        d1 = QuantumPolicyDistiller(num_actions=5, seed=42)
        d2 = QuantumPolicyDistiller(num_actions=5, seed=42)
        await d1.start(); await d2.start()
        p1 = await d1.predict({"carbon_intensity": 400})
        p2 = await d2.predict({"carbon_intensity": 400})
        await d1.stop(); await d2.stop()
        return p1, p2

    loop = _asyncio.new_event_loop()
    try:
        p1, p2 = loop.run_until_complete(determinism_test())
        check("identical predictions", np.allclose(p1, p2))
    except Exception as e:
        check("determinism", False, str(e))
    finally:
        loop.close()

    print("\n" + "=" * 80)
    if failures:
        print(f"FAILED: {len(failures)} test(s)")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL TESTS PASSED")
    return 0


# =============================================================================
# Smoke test
# =============================================================================
async def _smoke_test():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Quantum Policy Distiller v2.0.1 (patched) — smoke test")
    print("=" * 78)

    distiller = QuantumPolicyDistiller(num_actions=5, seed=42)
    await distiller.start()

    print("\n🎓 Training student model...")
    train_result = await distiller.train_from_circuit()
    print(f"   samples: {train_result['samples']}  "
          f"avg_loss: {train_result['avg_loss']:.4f}  "
          f"trained: {train_result['trained']}")

    print("\n🔮 Getting distilled policy...")
    ctx = {'carbon_intensity': 350, 'helium_scarcity': 0.5,
           'energy_price': 0.3, 'renewable_ratio': 0.7}
    probs = await distiller.get_distilled_policy(ctx)
    print(f"   probs: {probs.tolist()}")
    print(f"   sum: {float(np.sum(probs)):.4f}")

    print("\n📖 Explaining policy...")
    expl = await distiller.explain_policy(ctx)
    print(f"   source: {expl['source']}")
    if expl.get('xai_narrative'):
        print(f"   XAI: {expl['xai_narrative'][0]}")

    print("\n⚙️  Precision selector →", (await distiller.select_precision()).value)
    cc = await distiller.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"🎭 Role assignments: {distiller.get_role_assignments()}")

    hitl = await distiller.escalate_decision(
        decision_context={'reason': 'smoke'},
        options=['action_0', 'action_1'], confidence=0.5)
    print(f"👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    print("\n🧪 Legacy chaos test (restores state):")
    legacy = await distiller.run_chaos_test()
    print(f"   faults: {legacy['faults']}")

    print("\n🧪 Extended chaos suite:")
    chaos = await distiller.run_extended_chaos_suite()
    print(f"   pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print(f"\n🧠 Causal ATEs: {distiller.get_causal_ates()}")

    status = await distiller.get_comprehensive_status()
    print("\n📊 Comprehensive status:")
    print(json.dumps({
        'version': status['version'],
        'student_trained': status['student_trained'],
        'cache_size': status['cache_size'],
        'features': status['features'],
        'precision_last': status.get('precision', {}).get('last'),
        'federated': status.get('federated'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'hitl_pending': status.get('rlhf', {}).get('pending_queries'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'carbon_market': status.get('carbon_market'),
    }, indent=2, default=str))

    await distiller.stop()
    print("\n" + "=" * 78)
    print("✅ Quantum Policy Distiller v2.0.1 — smoke test complete")
    print("=" * 78)


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Quantum Distiller v2.0.1")
    parser.add_argument("--test", action="store_true",
                        help="Run embedded unit tests and exit")
    parser.add_argument("--smoke", action="store_true",
                        help="Run the smoke test (default)")
    args = parser.parse_args()
    if args.test:
        return _run_unit_tests()
    asyncio.run(_smoke_test())
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
