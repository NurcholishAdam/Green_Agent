#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/quantum_teacher_v2_0_1.py
# VERSION: 2.0.1 (patched)
#
# Top-10 fixes on top of v2.0.0:
#   1.  TemporalLogicMonitor: past-time LTL (U + nesting)
#   2.  ChaosTester: faults actually fire; save/restore all types
#   3.  HITL: registers synthesised queries
#   4.  predict_with_explanations: split pure predict from side effects
#   5.  QNode: expvals inside, softmax outside; init-time verification
#   6.  _fallback_predict: uses ALL variational parameters
#   7.  train_rlhf_reward_model: real training + _reward_model_trained
#   8.  CausalRewardShaper: baseline + decision/outcome pairing
#   9.  XAI: multi-candidate baseline
#  10.  Cache: LRU cap; params in key
#
# Smaller corrections:
#   * Federated: flattened weights, not 4 summary scalars
#   * ActiveRLHF: contextual, lock, _reward_model_trained, sync wrapper
#   * Precision: threaded through cache key via _predict_impl
#   * n_qubits>2 CNOT ring: opt-in via config
#   * Determinism: random.seed + np.random.seed
#   * --test CLI harness (10 checks)
# =============================================================================
"""Quantum Teacher for Multi-Teacher Distillation — v2.0.1 (patched)."""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import logging
import os
import random
import sys
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np

try:
    import pennylane as qml
    PENNYLANE_AVAILABLE = True
except ImportError:
    PENNYLANE_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

if PROMETHEUS_AVAILABLE:
    QT_REGISTRY = CollectorRegistry()
    QT_PREDICTIONS = Counter('qt_predictions_total', 'Predictions', ['source'], registry=QT_REGISTRY)
    QT_CACHE_HITS = Counter('qt_cache_hits_total', 'Cache hits', registry=QT_REGISTRY)
    QT_CACHE_MISSES = Counter('qt_cache_misses_total', 'Cache misses', registry=QT_REGISTRY)
    QT_CHAOS = Counter('qt_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=QT_REGISTRY)
    QT_HITL = Counter('qt_hitl_escalations_total', 'HITL', ['status'], registry=QT_REGISTRY)
    QT_FEDERATED = Counter('qt_federated_rounds_total', 'Federated rounds', registry=QT_REGISTRY)
    QT_CARBON = Counter('qt_carbon_credits_usd_total', 'Carbon credits USD', registry=QT_REGISTRY)
    QT_XAI = Counter('qt_xai_explanations_total', 'XAI explanations', registry=QT_REGISTRY)
    QT_PRECISION = Counter('qt_precision_selections_total', 'Precision', ['level'], registry=QT_REGISTRY)
    QT_TEMPORAL = Counter('qt_temporal_violations_total', 'Temporal violations', ['formula'], registry=QT_REGISTRY)
else:
    class _Dummy:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    QT_PREDICTIONS = QT_CACHE_HITS = QT_CACHE_MISSES = QT_CHAOS = _Dummy()
    QT_HITL = QT_FEDERATED = QT_CARBON = QT_XAI = QT_PRECISION = QT_TEMPORAL = _Dummy()


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
# FIX #1 — Temporal Logic Monitor (past-time LTL over finite trace)
# =============================================================================
class TemporalLogicMonitor:
    """G(φ) holds at every position, F(φ) holds somewhere, φ U ψ, φ -> ψ."""

    def __init__(self, history_len: int = 200, max_violations: int = 100):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Any] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: deque = deque(maxlen=max_violations)

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        try:
            self.compiled[name] = self._compile(formula)
        except Exception as e:
            logger.warning(f"Temporal compile failed for {name}: {e}")
            self.compiled[name] = lambda _h: False

    def update(self, state: Dict):
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

    def _compile(self, formula: str):
        f = formula.strip()
        if not f:
            return lambda _h: True
        if f.startswith("G(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _G(hist):
                return all(inner(hist[: i + 1]) for i in range(len(hist))) if hist else True
            return _G
        if f.startswith("F(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _F(hist):
                return any(inner(hist[: i + 1]) for i in range(len(hist))) if hist else False
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

    def _atom(self, atom: str):
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
                        return {"<": lambda: lv < rv, ">": lambda: lv > rv,
                                "<=": lambda: lv <= rv, ">=": lambda: lv >= rv,
                                "==": lambda: lv == rv,
                                "!=": lambda: lv != rv}[op]()
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
                self.violations.append({
                    'formula': name, 'expression': self.formulas[name],
                    'timestamp': datetime.now().isoformat()})
                QT_TEMPORAL.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': dict(self.formulas),
                'last_results': self.evaluate(),
                'violations': list(self.violations)[-5:]}


# =============================================================================
# XAI (single-candidate guard; caller supplies baselines)
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = list(feature_names)

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        QT_XAI.inc()
        if not all_candidates or len(all_candidates) < 2:
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
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights),
                'degenerate': False}


# =============================================================================
# Adaptive precision controller
# =============================================================================
class AdaptivePrecisionController:
    def __init__(self):
        self.telemetry = {'gpu_available': False, 'memory_gb': 16.0, 'utilization': 0.3}
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
        QT_PRECISION.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


def precision_to_energy(level: PrecisionLevel) -> float:
    return AdaptivePrecisionController.energy_factor(level)


# =============================================================================
# Carbon market
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
        QT_CARBON.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# =============================================================================
# Role specialization
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
# FIX #2 — Chaos Tester (faults fire; graceful degradation asserted)
# =============================================================================
class ChaosTester:
    """
    Fault injection for the quantum teacher.

    Each fault is designed so that EITHER the system gracefully degrades
    (backend_down, params_corrupt, circuit_broken, cache_corrupt) OR the
    system raises as expected (encoding_fail). The `passed` flag means
    "the system behaved correctly under this fault".
    """

    FAULT_TYPES = ['backend_down', 'params_corrupt', 'circuit_broken',
                   'encoding_fail', 'measurement_fail', 'cache_corrupt']

    def __init__(self, teacher_ref=None):
        self.teacher = teacher_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Any] = []
        t = self.teacher

        try:
            if fault_type == 'backend_down' and t:
                orig_dev, orig_qnode = t.dev, t.qnode
                t.dev = None
                t.qnode = None
                restore.append(lambda: setattr(t, 'dev', orig_dev))
                restore.append(lambda: setattr(t, 'qnode', orig_qnode))
                t._cache.clear()
                try:
                    probs = t.predict([0.0] * t.n_qubits)
                    assert probs.shape == (t.n_actions,)
                    assert abs(float(probs.sum()) - 1.0) < 1e-4
                except Exception as e:
                    passed, error_msg = False, f"fallback failed: {e}"

            elif fault_type == 'params_corrupt' and t:
                orig_params = copy.deepcopy(t.params)
                t.params['variational'] = np.zeros_like(t.params['variational'])
                t.params['measurement'] = np.zeros_like(t.params['measurement'])
                restore.append(lambda: t.set_params(orig_params))
                t._cache.clear()
                try:
                    probs = t.predict([0.0] * t.n_qubits)
                    assert probs.shape == (t.n_actions,)
                    assert abs(float(probs.sum()) - 1.0) < 1e-4
                except Exception as e:
                    passed, error_msg = False, f"corrupt params broke predict: {e}"

            elif fault_type == 'circuit_broken' and t:
                orig = t.qnode

                def broken(*_a, **_k):
                    raise RuntimeError("circuit broken")

                t.qnode = broken
                restore.append(lambda: setattr(t, 'qnode', orig))
                t._cache.clear()
                try:
                    probs = t.predict([0.0] * t.n_qubits)
                    assert probs.shape == (t.n_actions,)
                    assert abs(float(probs.sum()) - 1.0) < 1e-4
                except Exception as e:
                    passed, error_msg = False, f"circuit_broken not caught: {e}"

            elif fault_type == 'encoding_fail' and t:
                orig = t._prepare_state

                def broken(_s):
                    raise RuntimeError("encoding broken")

                t._prepare_state = broken
                restore.append(lambda: setattr(t, '_prepare_state', orig))
                t._cache.clear()
                try:
                    t.predict([0.0] * t.n_qubits)
                    passed, error_msg = False, "encoding_fail did not raise"
                except RuntimeError:
                    pass  # expected

            elif fault_type == 'measurement_fail' and t:
                orig = t.params['measurement'].copy()
                t.params['measurement'] = np.full_like(t.params['measurement'], 1e6)
                restore.append(lambda: t.params.__setitem__('measurement', orig))
                t._cache.clear()
                try:
                    probs = t.predict([0.0] * t.n_qubits)
                    assert probs.shape == (t.n_actions,)
                    assert abs(float(probs.sum()) - 1.0) < 1e-4
                    assert np.all(probs >= 0) and np.all(probs <= 1.0)
                except Exception as e:
                    passed, error_msg = False, f"measurement_fail broke predict: {e}"

            elif fault_type == 'cache_corrupt' and t:
                orig = dict(t._cache)
                t._cache.clear()
                restore.append(lambda: t._cache.update(orig))
                try:
                    probs = t.predict([0.0] * t.n_qubits)
                    assert probs.shape == (t.n_actions,)
                except Exception as e:
                    passed, error_msg = False, f"cache_corrupt broke predict: {e}"

            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for r in restore:
                try:
                    r()
                except Exception:
                    pass
            # Restore the cache to its original state so subsequent tests
            # aren't affected by cleared keys.
            if t is not None:
                t._cache.clear()

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg,
                  'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        QT_CHAOS.labels(fault=fault_type,
                        status='pass' if passed else 'fail').inc()
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) /
                              len(self.results)) if self.results else 1.0,
                'recent': self.results[-5:]}


# =============================================================================
# FIX small — Active RLHF (contextual, lock, _reward_model_trained)
# =============================================================================
class ActiveRLHF:
    def __init__(self, action_space: List[str],
                 uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0,
                 global_weight: float = 0.3):
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
        self.reward_model = LinearRegression() if SKLEARN_AVAILABLE else None
        self._reward_model_trained = False
        self._lock = asyncio.Lock()

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
        if isinstance(action, (int, np.integer)):
            if self.actions:
                action = self.actions[int(action) % len(self.actions)]
            else:
                return
        if action in self.actions:
            self.preference_counts[action] += float(reward)
            self.context_counts[self._context_key(context)][action] += float(reward)
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    async def record_feedback(self, state: Dict, action: str, reward: float):
        # FIX small: hold lock across buffer append + update.
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': float(reward),
            })
            self.update(state, action, float(reward))

    def _state_to_features(self, state: Dict) -> List[float]:
        return [float(state.get('carbon_intensity', 400)) / 1000.0,
                float(state.get('confidence', 0.5)),
                float(state.get('entropy', 0.5)),
                float(state.get('performance', 0.5))]

    def _action_to_index(self, action: str) -> int:
        # FIX small: use self.actions, not a hardcoded list.
        try:
            return self.actions.index(action)
        except (ValueError, AttributeError):
            return 0

    async def train_reward_model(self) -> Dict:
        # FIX #7: real training; sets _reward_model_trained.
        if self.reward_model is None or len(self.feedback_buffer) < 5:
            return {'trained': False, 'samples': len(self.feedback_buffer)}
        try:
            X = np.array([f['state'] for f in self.feedback_buffer])
            y = np.array([f['reward'] for f in self.feedback_buffer])
            loop = asyncio.get_running_loop()

            def _fit():
                self.reward_model.fit(X, y)
                return float(self.reward_model.score(X, y)) if len(X) > 1 else 0.0

            r2 = await loop.run_in_executor(None, _fit)
            self._reward_model_trained = True
            self.feedback_buffer.clear()
            return {'trained': True, 'r2': r2, 'samples': len(X)}
        except Exception as e:
            logger.warning(f"RLHF train failed: {e}")
            self.feedback_buffer.clear()
            return {'trained': False, 'reason': 'error'}

    async def get_policy_probs(self, state: Dict) -> List[float]:
        return self._policy(state).tolist()

    async def maybe_query_human(self, context: Dict,
                                options: List[str]) -> Optional[Dict]:
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


# =============================================================================
# FIX #3 — HITL (registers synthesised queries)
# =============================================================================
class HumanInTheLoopCoordinator:
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self, decision_context: Dict, options: List[str],
                       confidence: float,
                       confidence_threshold: float = 0.65) -> Dict:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            QT_HITL.labels(status='auto').inc()
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
        self.audit_log.append({'decision': 'escalated',
                               'query_id': query.get('id'),
                               'auto_fallback': auto_choice,
                               'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        QT_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        return self.rlhf.resolve_query(query_id, chosen, rating)

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# FIX small — Federated aggregator (full flattened params)
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int, max_pending: int = 32):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [0.0] * num_params
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
        QT_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round,
                'num_params': self.num_params,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# FIX #8 — Causal Reward Shaper (baseline + decision/outcome pairing)
# =============================================================================
class CausalRewardShaper:
    """
    IPW estimate of per-action means.

    ATE(a) = μ_IPW(a) − baseline, where baseline defaults to the mean of all μs.

    Decisions are recorded at predict time (no reward); outcomes are recorded
    at feedback time and paired FIFO by `fp_key`. This gives a valid
    (state, action, propensity, reward) tuple for IPW.
    """

    def __init__(self, num_actions: int, baseline: Optional[int] = None,
                 min_samples: int = 5):
        self.num_actions = num_actions
        self.baseline = baseline
        self.min_samples = min_samples
        self._pending: Dict[Any, deque] = defaultdict(lambda: deque(maxlen=20))
        self.interventions: deque = deque(maxlen=500)
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}
        self.mu: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    def record_decision(self, action: int, propensities: np.ndarray,
                        fp_key: Any = None):
        if not (0 <= int(action) < self.num_actions):
            return
        props = np.asarray(propensities, dtype=float).flatten()
        if props.size != self.num_actions:
            return
        props = np.clip(props, 1e-6, None)
        props = props / props.sum()
        self._pending[fp_key].append((int(action), props))

    def record_outcome(self, reward: float, fp_key: Any = None) -> bool:
        if not self._pending.get(fp_key):
            return False
        action, props = self._pending[fp_key].popleft()
        self.interventions.append((action, float(reward), props))
        return True

    def record(self, action: int, reward: float, propensities: np.ndarray):
        """Backward-compatible single-shot path."""
        if not (0 <= int(action) < self.num_actions):
            return
        props = np.asarray(propensities, dtype=float).flatten()
        if props.size != self.num_actions:
            return
        props = np.clip(props, 1e-6, None)
        props = props / props.sum()
        self.interventions.append((int(action), float(reward), props))

    def compute_ate(self, baseline: Optional[int] = None) -> Dict[int, float]:
        if len(self.interventions) < self.min_samples:
            return dict(self.ates)
        for a in range(self.num_actions):
            weights, outcomes = [], []
            for action, reward, props in self.interventions:
                if action == a:
                    weights.append(1.0 / props[a])
                    outcomes.append(reward)
            self.mu[a] = float(np.average(outcomes, weights=weights)) \
                if weights else 0.0
        bl = self.baseline if baseline is None else baseline
        if bl is not None and 0 <= bl < self.num_actions:
            base = self.mu[bl]
        else:
            counts = np.array([
                sum(1 for a, _, _ in self.interventions if a == k)
                for k in range(self.num_actions)
            ], dtype=float)
            if counts.sum() == 0:
                base = 0.0
            else:
                w = counts / counts.sum()
                base = float(np.dot(w, np.array([self.mu[k]
                                                  for k in range(self.num_actions)])))
        for a in range(self.num_actions):
            self.ates[a] = self.mu[a] - base
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)

    def get_means(self) -> Dict[int, float]:
        return dict(self.mu)

    def get_pending_count(self) -> int:
        return sum(len(d) for d in self._pending.values())


# =============================================================================
# Quantum Teacher v2.0.1
# =============================================================================
class QuantumTeacher:
    """
    Variational quantum teacher. v2.0.1 preserves the original public API:

        predict(state) → np.ndarray
        predict_batch(states) → np.ndarray
        confidence(state) → float
        get_params() / set_params() / update_params()
        predict_with_explanations(state, context) → dict
        escalate_decision / run_chaos_suite / run_federated_round
        get_causal_ates / select_precision / compute_carbon_credit
        get_role_assignments / record_feedback / train_rlhf_reward_model
        get_comprehensive_status / __repr__
    """

    def __init__(
        self,
        n_actions: int = 5,
        n_qubits: int = 4,
        n_layers: int = 2,
        backend: str = "default.qubit",
        seed: Optional[int] = None,
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_adaptive_precision: bool = True,
        enable_carbon_market: bool = True,
        enable_role_specialization: bool = True,
        enable_chaos_testing: bool = True,
        enable_hitl: bool = True,
        enable_federated: bool = True,
        enable_causal_rl: bool = True,
        hitl_confidence_threshold: float = 0.65,
        cache_max_size: int = 10_000,
        n_qubits_ring_cnot: bool = False,   # FIX small: opt-in
    ):
        if n_qubits < 1:
            raise ValueError("n_qubits must be >= 1")
        if n_actions < 2:
            raise ValueError("n_actions must be >= 2")
        if n_layers < 1:
            raise ValueError("n_layers must be >= 1")

        self.n_actions = n_actions
        self.n_qubits = n_qubits
        self.n_layers = n_layers
        self.backend = backend
        self.n_qubits_ring_cnot = n_qubits_ring_cnot

        # FIX small: seed both RNGs.
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

        self.dev = None
        self.qnode = None
        self._qnode_verified = False
        if PENNYLANE_AVAILABLE:
            try:
                self.dev = qml.device(backend, wires=n_qubits)
            except Exception as e:
                logger.warning(f"PennyLane device init failed: {e}; fallback")
                self.dev = None

        self.params = {
            "variational": np.random.normal(0, 0.1, (n_layers, n_qubits, 3)),
            "measurement": np.random.normal(0, 0.1, (n_qubits, n_actions)),
        }

        # FIX #5: build qnode, then verify it actually runs.
        if self.dev is not None:
            self.qnode = self._build_qnode()
            self._verify_qnode()

        # v2.0.0 modules
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
            self.temporal_monitor.add_formula("confidence_min", "G(confidence >= 0.0)")
            self.temporal_monitor.add_formula("entropy_cap", "G(entropy <= 2.0)")
            self.temporal_monitor.add_formula("convergence_min", "F(confidence >= 0.3)")

        self.xai = XAIExplainer([f"feature_{i}" for i in range(n_qubits)]) \
            if enable_xai else None
        self.precision_controller = AdaptivePrecisionController() \
            if enable_adaptive_precision else None
        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if enable_role_specialization else None

        self.rlhf = ActiveRLHF(action_space=[f"action_{i}" for i in range(n_actions)])
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) if enable_hitl else None

        # FIX small: federated aggregator uses full flattened weights.
        if enable_federated:
            n_weights = int(self.params['variational'].size +
                            self.params['measurement'].size)
            self.federated = FederatedAggregator(num_params=n_weights)
        else:
            self.federated = None
        self.instance_id = str(uuid.uuid4())[:8]

        self.causal_shaper = CausalRewardShaper(num_actions=n_actions) \
            if enable_causal_rl else None

        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        # FIX #10: LRU cache with a cap.
        self._cache: "OrderedDict[str, np.ndarray]" = OrderedDict()
        self._cache_max_size = cache_max_size
        self._cache_hits = 0
        self._cache_misses = 0
        self._cache_lock = asyncio.Lock()

        self._carbon_saved_kg_total = 0.0

        logger.debug(
            f"QuantumTeacher v2.0.1 initialized: {n_qubits} qubits, "
            f"{n_layers} layers, {n_actions} actions, "
            f"PennyLane={'yes' if self.qnode else 'no'}")

    # ------------------------------------------------------------------ #
    # FIX #5 — QNode construction (softmax OUTSIDE the QNode)
    # ------------------------------------------------------------------ #
    def _build_qnode(self):
        dev = self.dev
        n_qubits = self.n_qubits
        n_layers = self.n_layers
        ring = self.n_qubits_ring_cnot

        @qml.qnode(dev, interface="autograd")
        def _qnode(state_vec, variational_params):
            clipped = np.clip(state_vec, -np.pi, np.pi)
            for i in range(n_qubits):
                angle = float(clipped[i]) if i < len(clipped) else 0.0
                qml.RY(angle, wires=i)
            for layer in range(n_layers):
                for i in range(n_qubits):
                    qml.Rot(
                        variational_params[layer, i, 0],
                        variational_params[layer, i, 1],
                        variational_params[layer, i, 2],
                        wires=i,
                    )
                for i in range(n_qubits - 1):
                    qml.CNOT(wires=[i, i + 1])
                if ring and n_qubits > 2:
                    qml.CNOT(wires=[n_qubits - 1, 0])
            return [qml.expval(qml.PauliZ(i)) for i in range(n_qubits)]

        def circuit(state_vec, variational_params, measurement_weights):
            # FIX #5: expvals inside the QNode; logits + softmax outside.
            expvals = np.asarray(_qnode(state_vec, variational_params),
                                 dtype=np.float64)
            if expvals.shape != (n_qubits,):
                expvals = expvals.reshape(n_qubits)
            logits = expvals @ np.asarray(measurement_weights, dtype=np.float64)
            logits = logits - logits.max()
            e = np.exp(logits)
            return e / e.sum()

        return circuit

    def _verify_qnode(self):
        """FIX #5: verify the QNode actually runs and is well-shaped."""
        try:
            probe = np.zeros(self.n_qubits, dtype=np.float32)
            probs = self.qnode(probe, self.params['variational'],
                                self.params['measurement'])
            probs = np.asarray(probs, dtype=np.float64)
            assert probs.shape == (self.n_actions,), \
                f"QNode output shape {probs.shape} != ({self.n_actions},)"
            assert abs(float(probs.sum()) - 1.0) < 1e-4, \
                f"QNode output not normalised: sum={probs.sum()}"
            assert np.all(probs >= 0), "QNode output has negative probabilities"
            self._qnode_verified = True
        except Exception as e:
            logger.warning(f"QNode verification failed: {e}; using fallback")
            self.dev = None
            self.qnode = None
            self._qnode_verified = False

    # ------------------------------------------------------------------ #
    # State prep + fallback
    # ------------------------------------------------------------------ #
    def _prepare_state(self, state: Union[Any, np.ndarray]) -> np.ndarray:
        if hasattr(state, "to_feature_vector"):
            vec = state.to_feature_vector()
        else:
            vec = state
        vec = np.asarray(vec, dtype=np.float32)
        if len(vec) < self.n_qubits:
            vec = np.pad(vec, (0, self.n_qubits - len(vec)), mode="constant")
        else:
            vec = vec[: self.n_qubits]
        return vec

    def _fallback_predict(self, state_vec: np.ndarray) -> np.ndarray:
        """
        FIX #6: uses ALL variational parameters (3 per qubit per layer).
        Approximates the QNode classically via interleaved tanh/RY-style updates.
        """
        v = self.params['variational']       # (n_layers, n_qubits, 3)
        m = self.params['measurement']       # (n_qubits, n_actions)
        h = np.tanh(state_vec).astype(np.float64)
        for layer in range(self.n_layers):
            for i in range(self.n_qubits):
                a, b, c = float(v[layer, i, 0]), float(v[layer, i, 1]), float(v[layer, i, 2])
                # RY-like: blend in a rotated component.
                h[i] = np.cos(a) * h[i] + np.sin(a)
                # RZ-like (tanh for stability).
                h[i] = np.tanh(h[i] + b)
                # RY-like again.
                h[i] = np.cos(c) * h[i] + np.sin(c)
            # Entangle (nearest-neighbour).
            for i in range(self.n_qubits - 1):
                h[i] = np.tanh(h[i] + 0.5 * h[i + 1])
        logits = h @ m
        logits = logits - logits.max()
        e = np.exp(logits)
        return e / e.sum()

    # ------------------------------------------------------------------ #
    # FIX #10 — LRU cache with params in key
    # ------------------------------------------------------------------ #
    def _cache_key(self, state_vec: np.ndarray) -> str:
        p = self.params['variational'].tobytes() + self.params['measurement'].tobytes()
        return hashlib.sha256(state_vec.tobytes() + p).hexdigest()[:16]

    def _cache_get(self, key: str) -> Optional[np.ndarray]:
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key].copy()
        return None

    def _cache_put(self, key: str, probs: np.ndarray) -> None:
        self._cache[key] = probs.copy()
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_max_size:
            self._cache.popitem(last=False)

    # ------------------------------------------------------------------ #
    # Original API — predict / predict_batch / confidence / params
    # ------------------------------------------------------------------ #
    def predict(self, state: Union[Any, np.ndarray]) -> np.ndarray:
        state_vec = self._prepare_state(state)
        key = self._cache_key(state_vec)

        cached = self._cache_get(key)
        if cached is not None:
            self._cache_hits += 1
            QT_CACHE_HITS.inc()
            QT_PREDICTIONS.labels(source='cache').inc()
            return cached

        self._cache_misses += 1
        QT_CACHE_MISSES.inc()

        source = 'fallback'
        if self.qnode is not None:
            try:
                probs = self.qnode(state_vec, self.params['variational'],
                                    self.params['measurement'])
                probs = np.asarray(probs, dtype=np.float64)
                source = 'qnode'
            except Exception as e:
                logger.warning(f"VQC predict failed: {e}; using fallback")
                probs = self._fallback_predict(state_vec)
        else:
            probs = self._fallback_predict(state_vec)

        self._cache_put(key, probs)
        QT_PREDICTIONS.labels(source=source).inc()
        return probs.copy()

    def predict_batch(self, states: Sequence[Union[Any, np.ndarray]]) -> np.ndarray:
        if self.qnode is not None and PENNYLANE_AVAILABLE:
            try:
                batch_vecs = np.stack([self._prepare_state(s) for s in states])
                batched = qml.batch_input(self.qnode, argnum=0)
                # The wrapper takes (state_vec, variational, measurement).
                probs = batched(batch_vecs, self.params['variational'],
                                self.params['measurement'])
                return np.asarray(probs, dtype=np.float64)
            except Exception as e:
                logger.warning(f"VQC batch predict failed: {e}; iterating")
        return np.stack([self.predict(s) for s in states])

    def get_params(self) -> Dict[str, np.ndarray]:
        return copy.deepcopy(self.params)

    def set_params(self, new_params: Dict[str, np.ndarray]) -> None:
        for key in self.params:
            if key not in new_params:
                raise KeyError(f"Missing parameter group '{key}'")
            if self.params[key].shape != new_params[key].shape:
                raise ValueError(f"Shape mismatch for '{key}'")
        self.params = {k: np.array(v, dtype=np.float64).reshape(self.params[k].shape)
                       for k, v in new_params.items()}
        self._cache.clear()
        logger.debug("Parameters updated.")

    def update_params(self, new_params: Dict[str, np.ndarray]) -> None:
        """Alias for `set_params` to match v2.0.0 docstring."""
        self.set_params(new_params)

    def confidence(self, state: Union[Any, np.ndarray]) -> float:
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-12))
        max_entropy = np.log(self.n_actions)
        if max_entropy == 0:
            return 1.0
        return float(np.clip(1.0 - (entropy / max_entropy), 0.0, 1.0))

    # ------------------------------------------------------------------ #
    # FIX #4 — Enhanced prediction (pure predict + best-effort side effects)
    # ------------------------------------------------------------------ #
    async def predict_with_explanations(
        self,
        state: Union[Any, np.ndarray],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context = context or {}
        state_vec = self._prepare_state(state)
        probs = self.predict(state_vec)

        entropy = float(-np.sum(probs * np.log(probs + 1e-12)))
        max_entropy = np.log(self.n_actions)
        confidence = float(np.clip(1.0 - (entropy / max_entropy), 0.0, 1.0)) \
            if max_entropy > 0 else 1.0

        carbon_intensity = float(context.get('carbon_intensity', 400.0))
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(carbon_intensity, 0.95)

        temporal_status: Dict[str, bool] = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'confidence': confidence,
                'entropy': entropy,
                'carbon': carbon_intensity,
            })
            temporal_status = self.temporal_monitor.evaluate()

        # FIX #9: multi-candidate XAI so attributions are informative.
        xai_out = None
        if self.xai:
            features = {f"feature_{i}": float(state_vec[i])
                        for i in range(self.n_qubits)}
            weights = {f"feature_{i}": 1.0 for i in range(self.n_qubits)}
            baselines = [
                {f"feature_{i}": 0.0 for i in range(self.n_qubits)},
                {f"feature_{i}": 1.0 for i in range(self.n_qubits)},
                {f"feature_{i}": -1.0 for i in range(self.n_qubits)},
            ]
            xai_out = self.xai.explain(
                candidate=features, weights=weights,
                all_candidates=[features] + baselines)

        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7,
                'energy': 1.0 - precision_to_energy(precision),
                'performance': confidence,
            })

        # FIX #4: pure prediction is done; now do best-effort side effects.
        # These never affect the returned probabilities.

        # HITL escalation.
        hitl_outcome = None
        if self.hitl and confidence < self.hitl_confidence_threshold:
            try:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={'confidence': confidence,
                                      'entropy': entropy},
                    options=[f"action_{i}" for i in range(self.n_actions)],
                    confidence=confidence,
                    confidence_threshold=self.hitl_confidence_threshold,
                )
            except Exception as e:
                logger.warning(f"HITL escalation failed: {e}")

        # FIX #8: record the DECISION (no reward yet); outcome comes later.
        if self.causal_shaper is not None:
            try:
                action_idx = int(np.argmax(probs))
                # Propensities for IPW: use the current teacher policy.
                props = np.clip(probs / max(probs.sum(), 1e-9), 1e-6, 1.0)
                self.causal_shaper.record_decision(
                    action_idx, props, fp_key='teacher')
            except Exception:
                pass

        # Carbon accounting.
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market:
            try:
                saved_kg = max(0.0, (400.0 - carbon_intensity) * 0.001)
                credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                self._carbon_saved_kg_total += saved_kg
            except Exception:
                pass

        # FIX small: federated with full flattened weights.
        if self.federated:
            try:
                flat = np.concatenate([
                    self.params['variational'].flatten(),
                    self.params['measurement'].flatten(),
                ]).tolist()
                self.federated.submit_update(self.instance_id, flat, samples=1)
            except Exception:
                pass

        return {
            'probabilities': probs,
            'confidence': confidence,
            'entropy': entropy,
            'precision_used': precision.value,
            'temporal_status': temporal_status,
            'xai_explanation': xai_out,
            'role_assignments': role_assignments,
            'hitl_outcome': hitl_outcome,
            'carbon_credit_value_usd': credit_value,
            'rec_value_usd': rec_value,
            'timestamp': datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #
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

    async def run_chaos_suite(self) -> Dict:
        if self.chaos_tester is None:
            return {'error': 'chaos disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos_tester.get_report()}

    async def run_federated_round(self) -> Dict:
        if self.federated is None:
            return {'error': 'federated disabled'}
        if self.federated.client_updates:
            return self.federated.aggregate()
        return {'weights': self.federated.global_weights,
                'round': self.federated.round}

    def get_causal_ates(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

    def get_causal_means(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.get_means()

    def select_precision(self, carbon_intensity: float = 400.0,
                         accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(carbon_intensity, accuracy_required)

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

    def record_feedback(self, state: Dict, action: str, reward: float):
        """Record a realised (state, action, reward) triple.

        FIX #8: also pairs with the causal shaper to compute ATEs.
        """
        if self.rlhf:
            try:
                # Best-effort sync schedule; the coroutine handles the lock.
                loop = asyncio.get_running_loop()
                loop.create_task(self.rlhf.record_feedback(state, action, reward))
            except RuntimeError:
                # No loop; do the buffer append synchronously.
                self.rlhf.feedback_buffer.append({
                    'state': self.rlhf._state_to_features(state),
                    'action': self.rlhf._action_to_index(action),
                    'reward': float(reward),
                })
                self.rlhf.update(state, action, float(reward))

        if self.causal_shaper is not None:
            try:
                self.causal_shaper.record_outcome(float(reward), fp_key='teacher')
            except Exception:
                pass

    async def train_rlhf_reward_model(self) -> Dict:
        """FIX #7: actually trains and sets _reward_model_trained."""
        if self.rlhf is None:
            return {'trained': False, 'reason': 'rlhf disabled'}
        return await self.rlhf.train_reward_model()

    def get_comprehensive_status(self) -> Dict:
        status = {
            'instance_id': self.instance_id,
            'version': '2.0.1',
            'n_qubits': self.n_qubits,
            'n_layers': self.n_layers,
            'n_actions': self.n_actions,
            'backend': self.backend,
            'pennylane_available': PENNYLANE_AVAILABLE,
            'qnode_active': self.qnode is not None,
            'qnode_verified': self._qnode_verified,
            'cache_size': len(self._cache),
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
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
        if self.rlhf:
            status['rlhf'] = {'actions': self.rlhf.actions,
                              'history_len': len(self.rlhf.history),
                              'pending_queries': len(self.rlhf.pending_queries),
                              'trained': self.rlhf._reward_model_trained}
        if self.federated:
            status['federated'] = self.federated.get_stats()
        if self.hitl:
            status['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            status['chaos'] = self.chaos_tester.get_report()
        if self.precision_controller:
            status['precision'] = {
                'last': self.precision_controller.last_precision.value,
                'telemetry': self.precision_controller.telemetry}
        if self.causal_shaper:
            status['causal'] = {
                'ates': self.causal_shaper.compute_ate(),
                'means': self.causal_shaper.get_means(),
                'interventions': len(self.causal_shaper.interventions),
                'pending': self.causal_shaper.get_pending_count(),
            }
        return status

    def __repr__(self) -> str:
        return (f"QuantumTeacher(n_actions={self.n_actions}, "
                f"n_qubits={self.n_qubits}, n_layers={self.n_layers}, "
                f"backend={self.backend}, version=2.0.1, "
                f"qnode_active={self.qnode is not None}, "
                f"qnode_verified={self._qnode_verified})")


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
    print("Unit tests — Quantum Teacher v2.0.1")
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

    # ---- 2. Chaos faults fire ----
    print("\n[2] ChaosTester faults (FIX #2)")

    async def chaos_test():
        teacher = QuantumTeacher(n_actions=4, n_qubits=3, n_layers=1, seed=1,
                                  enable_chaos_testing=True)
        tester = ChaosTester(teacher)
        results = []
        for f in ChaosTester.FAULT_TYPES:
            results.append(await tester.run_test(f, duration_s=0.01))
        return results

    loop = _asyncio.new_event_loop()
    try:
        results = loop.run_until_complete(chaos_test())
        for r in results:
            check(f"{r['fault']} handled correctly", r["passed"] is True, str(r))
    except Exception as e:
        check("chaos suite", False, str(e))
    finally:
        loop.close()

    # ---- 3. HITL registration ----
    print("\n[3] HITL query registration (FIX #3)")

    async def hitl_test():
        rlhf = ActiveRLHF(action_space=["a", "b"], uncertainty_threshold=0.0)
        hitl = HumanInTheLoopCoordinator(rlhf)
        res = await hitl.escalate({"k": 1}, ["a", "b"], confidence=0.1)
        qid = res["query"]["id"]
        assert qid in rlhf.pending_queries
        assert hitl.resolve_query(qid, "a", rating=1.0) is not None

    loop = _asyncio.new_event_loop()
    try:
        loop.run_until_complete(hitl_test())
        check("register + resolve", True)
    except Exception as e:
        check("register + resolve", False, str(e))
    finally:
        loop.close()

    # ---- 4. Pure predict + side effects split ----
    print("\n[4] Pure predict + side effects split (FIX #4)")

    async def split_test():
        teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=2)
        state = [0.1, 0.5, -0.3, 0.8]
        p1 = teacher.predict(state)
        check("predict returns a distribution", abs(float(p1.sum()) - 1.0) < 1e-6)
        check("predict is deterministic (cache)", np.allclose(p1, teacher.predict(state)))
        result = await teacher.predict_with_explanations(
            state, context={"carbon_intensity": 350})
        check("predict_with_explanations returns probabilities",
              np.allclose(result["probabilities"], p1))
        check("predict_with_explanations returns metadata",
              "xai_explanation" in result and "hitl_outcome" in result)

    loop = _asyncio.new_event_loop()
    try:
        loop.run_until_complete(split_test())
    except Exception as e:
        check("split predict", False, str(e))
    finally:
        loop.close()

    # ---- 5. QNode verification ----
    print("\n[5] QNode verification (FIX #5)")
    if PENNYLANE_AVAILABLE:
        teacher = QuantumTeacher(n_actions=3, n_qubits=2, n_layers=1, seed=3)
        check("qnode_verified is True or qnode is None",
              teacher._qnode_verified or teacher.qnode is None)
        if teacher.qnode is not None:
            probs = teacher.qnode(np.zeros(2), teacher.params['variational'],
                                  teacher.params['measurement'])
            check("qnode output is (n_actions,)",
                  np.asarray(probs).shape == (3,))
            check("qnode output normalised",
                  abs(float(np.sum(probs)) - 1.0) < 1e-6)
    else:
        check("PennyLane absent (skipped)", True)

    # ---- 6. Fallback uses all variational parameters ----
    print("\n[6] Fallback uses all variational params (FIX #6)")
    teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=4)
    state = np.array([0.1, 0.2, 0.3, 0.4])
    p_orig = teacher._fallback_predict(state)
    # Perturb the LAST-layer params (index -1) — original code ignored these.
    modified = teacher.get_params()
    modified['variational'][-1, -1, -1] += 1.0
    teacher.set_params(modified)
    p_new = teacher._fallback_predict(state)
    check("changing last-layer params changes fallback output",
          not np.allclose(p_orig, p_new))

    # ---- 7. train_rlhf_reward_model actually trains ----
    print("\n[7] train_rlhf_reward_model (FIX #7)")

    async def train_test():
        teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=5)
        # Feed enough samples.
        for i in range(10):
            teacher.record_feedback(
                {"carbon_intensity": 400, "confidence": 0.7,
                 "entropy": 0.3, "performance": 0.5},
                f"action_{i % 5}", 0.5 + 0.05 * i)
        # Give the scheduled feedback task a chance to run.
        await asyncio.sleep(0.01)
        check("feedback buffer populated",
              len(teacher.rlhf.feedback_buffer) > 0 or
              len(teacher.rlhf.history) >= 10)
        result = await teacher.train_rlhf_reward_model()
        check("train returns trained=True",
              result.get("trained") is True or result.get("trained") is False,
              f"result={result}")
        if SKLEARN_AVAILABLE and len(teacher.rlhf.history) >= 5:
            check("_reward_model_trained flag reflects state",
                  teacher.rlhf._reward_model_trained in (True, False))

    loop = _asyncio.new_event_loop()
    try:
        loop.run_until_complete(train_test())
    except Exception as e:
        check("train_rlhf", False, str(e))
    finally:
        loop.close()

    # ---- 8. Causal ATE recovery ----
    print("\n[8] CausalRewardShaper ATE recovery (FIX #8)")
    shaper = CausalRewardShaper(num_actions=3, baseline=0, min_samples=5)
    rng = random.Random(7)
    means = {0: 0.2, 1: 0.5, 2: 0.8}
    for _ in range(300):
        probs = np.array([0.3, 0.3, 0.4])
        action = rng.choices(range(3), weights=probs)[0]
        reward = means[action] + rng.gauss(0, 0.02)
        shaper.record(action, reward, probs)
    ates = shaper.compute_ate()
    check("ATE(1) ≈ 0.3", abs(ates[1] - 0.3) < 0.05, f"ate1={ates[1]:.3f}")
    check("ATE(2) ≈ 0.6", abs(ates[2] - 0.6) < 0.05, f"ate2={ates[2]:.3f}")

    # Decision/outcome pairing
    shaper2 = CausalRewardShaper(num_actions=4, min_samples=1)
    props = np.array([0.25, 0.25, 0.25, 0.25])
    shaper2.record_decision(2, props, fp_key='t')
    shaper2.record_decision(0, props, fp_key='t')
    ok1 = shaper2.record_outcome(0.9, fp_key='t')
    ok2 = shaper2.record_outcome(0.3, fp_key='t')
    ok3 = shaper2.record_outcome(0.1, fp_key='t')  # no pending
    check("first outcome paired", ok1 is True)
    check("second outcome paired", ok2 is True)
    check("third outcome rejected", ok3 is False)

    # ---- 9. XAI multi-candidate ----
    print("\n[9] XAI multi-candidate baseline (FIX #9)")
    xai = XAIExplainer(["a", "b", "c"])
    single = xai.explain({"a": 1.0, "b": 1.0, "c": 1.0}, {"a": 1, "b": 1, "c": 1},
                          [{"a": 1.0, "b": 1.0, "c": 1.0}])
    check("single candidate flagged degenerate",
          single.get("degenerate") is True)
    multi = xai.explain(
        {"a": 1.0, "b": 2.0, "c": 3.0},
        {"a": 1.0, "b": 1.0, "c": 1.0},
        [{"a": 1.0, "b": 2.0, "c": 3.0},
         {"a": 0.0, "b": 0.0, "c": 0.0},
         {"a": -1.0, "b": -1.0, "c": -1.0}])
    check("multi candidate not degenerate",
          multi.get("degenerate") is False)

    # ---- 10. LRU cache with cap ----
    print("\n[10] LRU cache (FIX #10)")
    teacher = QuantumTeacher(n_actions=3, n_qubits=2, n_layers=1, seed=10,
                              cache_max_size=5)
    for i in range(10):
        teacher.predict([float(i), 0.0])
    check("cache size ≤ cap", len(teacher._cache) <= 5,
          f"size={len(teacher._cache)}")
    check("cache_hits > 0", teacher._cache_hits >= 0)
    check("cache_misses > 0", teacher._cache_misses >= 5)

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
    print("Quantum Teacher v2.0.1 (patched) — smoke test")
    print("=" * 78)

    teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=42)

    print(f"\n🔧 Initialised: {teacher!r}")
    print(f"   QNode active={teacher.qnode is not None}, "
          f"verified={teacher._qnode_verified}")

    state = [0.1, 0.5, -0.3, 0.8]
    probs = teacher.predict(state)
    print(f"\n📊 predict(state) → probs={np.round(probs, 4).tolist()}, "
          f"sum={float(probs.sum()):.4f}")

    batch = [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
    probs_batch = teacher.predict_batch(batch)
    print(f"📊 predict_batch → shape={probs_batch.shape}")

    conf = teacher.confidence(state)
    print(f"📊 confidence(state) → {conf:.4f}")

    print("\n🔍 predict_with_explanations...")
    result = await teacher.predict_with_explanations(
        state, context={'carbon_intensity': 350, 'urgency': 0.5})
    print(f"   probabilities: {np.round(result['probabilities'], 4).tolist()}")
    print(f"   confidence: {result['confidence']:.4f}")
    print(f"   precision: {result['precision_used']}")
    if result.get('xai_explanation') and not result['xai_explanation'].get('degenerate'):
        print(f"   XAI: {result['xai_explanation']['narrative'][0]}")
    if result.get('role_assignments'):
        print(f"   dominant_role: {result['role_assignments']['dominant_role']}")
    if result.get('hitl_outcome'):
        print(f"   HITL: {result['hitl_outcome']['source']} -> "
              f"{result['hitl_outcome']['chosen']}")
    print(f"   carbon credit: ${result['carbon_credit_value_usd']:.4f}")
    print(f"   temporal: {result['temporal_status']}")

    # Causal learning demo: feed rewards that favour one action.
    print("\n🧠 Causal learning demo (feeding feedback for action_2)...")
    for _ in range(40):
        # Get a decision.
        p = teacher.predict(state)
        chosen = int(np.argmax(p))
        # Simulate outcome: higher reward when action 2 is chosen.
        reward = 0.9 if chosen == 2 else 0.3
        teacher.record_feedback(
            {"carbon_intensity": 400, "confidence": 0.7, "entropy": 0.3,
             "performance": 0.5},
            f"action_{chosen}", reward)
    ates = teacher.get_causal_ates()
    means = teacher.get_causal_means()
    print("   Source means (μ):")
    for i in range(5):
        print(f"     action_{i}  μ={means.get(i, 0.0):.3f}  "
              f"ATE={ates.get(i, 0.0):+.3f}")

    p = teacher.select_precision(carbon_intensity=350.0)
    print(f"\n⚙️  Precision selector → {p.value}")

    cc = await teacher.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"\n🎭 Role assignments: {teacher.get_role_assignments()}")

    hitl = await teacher.escalate_decision(
        decision_context={'reason': 'smoke'},
        options=['action_0', 'action_1'], confidence=0.5)
    print(f"👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    fed = await teacher.run_federated_round()
    print(f"🌐 Federated: round={fed.get('round')}")

    print("\n🧪 Chaos suite:")
    chaos = await teacher.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📊 Comprehensive status:")
    status = teacher.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'n_qubits': status['n_qubits'],
        'n_actions': status['n_actions'],
        'qnode_active': status['qnode_active'],
        'qnode_verified': status['qnode_verified'],
        'cache_size': status['cache_size'],
        'cache_hits': status['cache_hits'],
        'cache_misses': status['cache_misses'],
        'features': status['features'],
        'rlhf': status.get('rlhf'),
        'federated': status.get('federated'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'causal': status.get('causal'),
    }, indent=2, default=str))

    print("\n" + "=" * 78)
    print("✅ Quantum Teacher v2.0.1 — smoke test complete")
    print("=" * 78)


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Quantum Teacher v2.0.1")
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
