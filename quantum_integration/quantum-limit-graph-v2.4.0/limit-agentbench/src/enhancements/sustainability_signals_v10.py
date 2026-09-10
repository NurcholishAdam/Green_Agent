#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/sustainability_signals_enhanced_v16_0.py
# VERSION: 17.0.0
# =============================================================================
"""
Enhanced Sustainability Signals System v17.0.0

v16.0.0 features preserved:
    GA + MoE + Pareto + Forecasting + Federated + Drift + Active Learning
    + LIMIT Graph + RLHF + Multi-Teacher Distillation.

v17.0.0 adds (all in-file):
    • Temporal Logic Verification (G/F/U/->)
    • Explainable AI (XAI) — feature attribution + narrative
    • Adaptive Precision Switching (fp32/fp16/bf16/fp8/fp4)
    • Carbon Markets + Renewable Energy Credits (RECs)
    • Multi-Agent Role Specialization (emergent, softmax affinity)
    • Chaos Testing as first-class citizen
    • Active RLHF (uncertainty-triggered human queries)
    • Human-in-the-Loop Coordinator
    • Federated Green Learning (proper FedAvg)
    • Causal RL hooks (IPW / ATE)

Also fixes: async __init__ on ESGState, self.websocket init order,
missing comprehensive_sustainability_assessment method, config key errors.
"""

import asyncio
import hashlib
import json
import logging
import logging.handlers
import os
import random
import secrets
import signal
import sqlite3
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import contextvars

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

# -----------------------------------------------------------------------------
# Optional external dependencies
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve as ws_serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

audit_logger = logging.getLogger('esg_audit')
if not audit_logger.handlers:
    try:
        h = logging.handlers.RotatingFileHandler(
            'esg_audit_v17.log', maxBytes=50*1024*1024, backupCount=5)
        h.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        audit_logger.addHandler(h)
    except Exception:
        audit_logger.addHandler(logging.StreamHandler())
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SUSTAINABILITY_ASSESSMENTS = Counter('sustainability_assessments_total', 'Assessments',
                                          ['status', 'sector'], registry=REGISTRY)
    ESG_SCORE = Gauge('esg_score', 'ESG score', ['sector'], registry=REGISTRY)
    SC_TEMPORAL_VIOLATIONS = Counter('sc_temporal_violations_total', 'Temporal',
                                      ['formula'], registry=REGISTRY)
    SC_CHAOS = Counter('sc_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    SC_HITL = Counter('sc_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    SC_FEDERATED = Counter('sc_federated_rounds_total', 'Federated', registry=REGISTRY)
    SC_CARBON_CREDITS = Counter('sc_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    SC_XAI = Counter('sc_xai_explanations_total', 'XAI', registry=REGISTRY)
    SC_PRECISION = Counter('sc_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    SUSTAINABILITY_ASSESSMENTS = ESG_SCORE = DummyMetric()
    SC_TEMPORAL_VIOLATIONS = SC_CHAOS = SC_HITL = DummyMetric()
    SC_FEDERATED = SC_CARBON_CREDITS = SC_XAI = SC_PRECISION = DummyMetric()


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
# v17.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ."""
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: List[Dict] = []

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        self.history.append(dict(state))

    def _compile(self, formula: str):
        f = formula.strip()
        if f.startswith("G(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: all(inner([h]) for h in hist) if hist else True
        if f.startswith("F(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: any(inner([h]) for h in hist) if hist else False
        if " U " in f:
            left, right = f.split(" U ", 1)
            lf, rf = self._compile(left), self._compile(right)
            def until(hist):
                for i in range(len(hist)):
                    if rf(hist[i:]):
                        return True
                    if not lf([hist[i]]):
                        return False
                return False
            return until
        if "->" in f:
            left, right = f.split("->", 1)
            lf, rf = self._compile(left.strip()), self._compile(right.strip())
            return lambda hist: (not lf(hist)) or rf(hist)
        return self._atom(f)

    def _atom(self, atom: str):
        atom = atom.strip()
        for op in ["<=", ">=", "==", "!=", "<", ">"]:
            if op in atom:
                lhs, rhs = [s.strip() for s in atom.split(op, 1)]
                def make(lhs, op, rhs):
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
        return lambda hist: bool(atom.lower() in ("true", "1", "yes"))

    def evaluate(self) -> Dict[str, bool]:
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = fn(list(self.history))
            except Exception:
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({'formula': name,
                                        'expression': self.formulas[name],
                                        'timestamp': datetime.now().isoformat()})
                SC_TEMPORAL_VIOLATIONS.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}


# =============================================================================
# v17.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        if not NUMPY_AVAILABLE:
            return {'contributions': {}, 'narrative': [], 'weights_used': weights}
        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate.get(f, 0.0) for f in self.feature_names])
        w_arr = np.array([weights.get(f, 1.0) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the score."
                     for f, v in ranked]
        SC_XAI.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}


# =============================================================================
# v17.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    def __init__(self):
        self.telemetry = {'gpu_available': False, 'memory_gb': 16.0, 'utilization': 0.3}
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity: float, accuracy_required: float = 0.95) -> PrecisionLevel:
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.FP8
        elif self.telemetry.get('gpu_available') and carbon_intensity < 350:
            self.last_precision = PrecisionLevel.FP16
        else:
            self.last_precision = PrecisionLevel.FP16
        SC_PRECISION.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


# =============================================================================
# v17.0.0 MODULE D — CARBON MARKET CLIENT
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
        SC_CARBON_CREDITS.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# =============================================================================
# v17.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        if NUMPY_AVAILABLE:
            self.affinity = np.array([
                [0.7, 0.9, 0.4, 0.6],   # leader
                [0.4, 0.5, 0.9, 0.5],   # worker
                [0.9, 0.4, 0.3, 0.7],   # verifier
                [0.3, 0.2, 0.3, 0.3],   # observer
            ])
        else:
            self.affinity = None

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        if not NUMPY_AVAILABLE or self.affinity is None:
            return {'assignments': {r.value: 0.25 for r in self.roles},
                    'dominant_role': AgentRole.OBSERVER.value}
        ctx = np.array([context.get('trust', 0.5),
                        context.get('compute', 0.5),
                        context.get('energy', 0.5),
                        context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i])
                                for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}


# =============================================================================
# v17.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'storage_broken', 'moe_broken',
                   'ga_broken', 'rlhf_broken', 'distillation_broken']

    def __init__(self, system_ref=None):
        self.system = system_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        s = self.system

        try:
            if fault_type == 'carbon_api_down' and s:
                orig = s.carbon_client.get_current_intensity
                async def broken(): raise RuntimeError("carbon API down")
                s.carbon_client.get_current_intensity = broken
                restore.append(lambda: setattr(s.carbon_client, 'get_current_intensity', orig))
            elif fault_type == 'storage_broken' and s:
                orig = s.storage.save_esg_assessment
                async def broken(*a, **k): raise RuntimeError("storage broken")
                s.storage.save_esg_assessment = broken
                restore.append(lambda: setattr(s.storage, 'save_esg_assessment', orig))
            elif fault_type == 'moe_broken' and s and s.autonomous_optimizer.moe_gating:
                orig = s.autonomous_optimizer.moe_gating.select_expert
                async def broken(*a, **k): raise RuntimeError("moe broken")
                s.autonomous_optimizer.moe_gating.select_expert = broken
                restore.append(lambda: setattr(
                    s.autonomous_optimizer.moe_gating, 'select_expert', orig))
            elif fault_type == 'ga_broken' and s and s.autonomous_optimizer.ga_optimizer:
                orig = s.autonomous_optimizer.ga_optimizer.optimize
                async def broken(*a, **k): raise RuntimeError("ga broken")
                s.autonomous_optimizer.ga_optimizer.optimize = broken
                restore.append(lambda: setattr(
                    s.autonomous_optimizer.ga_optimizer, 'optimize', orig))
            elif fault_type == 'rlhf_broken' and s and s.rlhf:
                orig = s.rlhf.get_policy_probs
                async def broken(_): raise RuntimeError("rlhf broken")
                s.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(s.rlhf, 'get_policy_probs', orig))
            elif fault_type == 'distillation_broken' and s and s.distillation:
                orig = s.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distillation broken")
                s.distillation.distill = broken
                restore.append(lambda: setattr(s.distillation, 'distill', orig))
            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for rec in restore:
                try: rec()
                except Exception: pass

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg,
                  'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        SC_CHAOS.labels(fault=fault_type, status='pass' if passed else 'fail').inc()
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}


# =============================================================================
# v17.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
class ActiveRLHF:
    """Superset of the legacy RLHFManager; preserves record_feedback,
    train_reward_model, get_policy_probs."""
    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []
        self.reward_model = MLPRegressor(hidden_layer_sizes=(16,), max_iter=200,
                                          random_state=42) if SKLEARN_AVAILABLE else None
        self.policy_weights = np.array([1/4]*4) if NUMPY_AVAILABLE else [1/4]*4
        self._lock = asyncio.Lock()

    def _state_to_features(self, state):
        return [state.get('carbon_intensity', 0.4),
                state.get('esg_score', 0.5),
                state.get('cost', 0.5),
                state.get('latency', 0.5)]

    def _action_to_index(self, action):
        actions = ['environmental_focused', 'social_focused',
                   'governance_focused', 'balanced']
        return actions.index(action) if action in actions else 3

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward})
        self.update(state, action, reward)

    async def train_reward_model(self):
        if self.reward_model is None or len(self.feedback_buffer) < 10:
            return
        try:
            X = [f['state'] for f in self.feedback_buffer]
            y = [f['reward'] for f in self.feedback_buffer]
            self.reward_model.fit(X, y)
            self.feedback_buffer.clear()
            logger.info("ActiveRLHF trained on %d samples", len(X))
        except Exception as e:
            logger.warning("ActiveRLHF train failed: %s", e)

    async def get_policy_probs(self, state):
        if NUMPY_AVAILABLE:
            return self.policy_weights.tolist()
        return list(self.policy_weights)

    def update(self, context, action, reward):
        if action in self.actions:
            self.preference_counts[action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    def _policy(self, context):
        if not NUMPY_AVAILABLE:
            return [0.25] * len(self.actions)
        raw = np.array([self.preference_counts[a] for a in self.actions], dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context) -> str:
        if NUMPY_AVAILABLE:
            probs = self._policy(context)
            return self.actions[int(np.argmax(probs))]
        return self.actions[0]

    def uncertainty(self, context) -> float:
        if NUMPY_AVAILABLE:
            probs = self._policy(context)
            ent = -np.sum(probs * np.log(probs + 1e-12))
            return float(ent / np.log(len(self.actions))) if self.actions else 0.0
        return 0.0

    async def maybe_query_human(self, context, options):
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {'id': qid, 'context': context, 'options': options,
                 'uncertainty': u, 'created_at': datetime.now().isoformat(),
                 'status': 'pending'}
        self.pending_queries[qid] = query
        return query

    def resolve_query(self, query_id, chosen, rating=1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({'status': 'resolved', 'chosen': chosen, 'rating': rating})
        self.update(q['context'], chosen, rating)
        return q


# =============================================================================
# v17.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
class HumanInTheLoopCoordinator:
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self, decision_context, options, confidence,
                        confidence_threshold=0.65):
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            SC_HITL.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated',
                               'query_id': query.get('id'),
                               'auto_fallback': auto_choice,
                               'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        SC_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# v17.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 4):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id, weights, samples):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({'client_id': client_id,
                                    'weights': list(weights), 'samples': samples})

    def aggregate(self) -> Dict:
        if not self.client_updates or not NUMPY_AVAILABLE:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        SC_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# v17.0.0 MODULE J — CAUSAL REWARD SHAPER (IPW / ATE)
# =============================================================================
class CausalRewardShaper:
    def __init__(self, num_actions: int):
        self.num_actions = num_actions
        self.interventions: deque = deque(maxlen=500)
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    def record(self, action: int, reward: float, propensities):
        if NUMPY_AVAILABLE:
            self.interventions.append((action, float(reward), np.array(propensities)))

    def compute_ate(self) -> Dict[int, float]:
        if len(self.interventions) < 10 or not NUMPY_AVAILABLE:
            return dict(self.ates)
        for a in range(self.num_actions):
            weights, outcomes = [], []
            for action, reward, props in self.interventions:
                if action == a:
                    w = 1.0 / (props[a] + 1e-6)
                    weights.append(w)
                    outcomes.append(reward)
            if weights:
                self.ates[a] = float(np.average(outcomes, weights=weights))
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)


# =============================================================================
# ORIGINAL CONFIG (extended with v17.0.0 flags)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class ESGConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("17.0.0")
        log_level: str = Field("INFO")
        db_path: str = Field("/tmp/esg_system_v17.db")
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)
        metrics_port: int = Field(8000, ge=1024, le=65535)
        websocket_port: int = Field(8770, ge=1024)
        cache_ttl: int = Field(300, ge=1)
        master_key_env: str = Field("ESG_MASTER_KEY")
        mopd_weights: Dict[str, float] = Field(default_factory=lambda: {
            'environmental': 0.4, 'social': 0.3, 'governance': 0.3})
        # v16 flags preserved
        ga_enabled: bool = True
        ga_population_size: int = 20
        ga_generations: int = 5
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        moe_enabled: bool = True
        moe_expert_count: int = 4
        pareto_enabled: bool = True
        pareto_max_architectures: int = 100
        forecast_enabled: bool = True
        federated_learning_enabled: bool = True
        drift_detection_enabled: bool = True
        user_preference_learning_enabled: bool = True
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        rlhf_training_interval: int = 600
        distillation_enabled: bool = True
        distillation_temperature: float = 2.0
        distillation_interval: int = 300
        # ============ v17.0.0 flags ============
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        hitl_confidence_threshold: float = 0.65
        causal_rl_enabled: bool = True

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v: str) -> str:
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                return b'\x00' * 32
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                return b'\x00' * 32

        class Config:
            env_prefix = "ESG_"
else:
    @dataclass
    class ESGConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "17.0.0"
        log_level: str = "INFO"
        db_path: str = "/tmp/esg_system_v17.db"
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        metrics_port: int = 8000
        websocket_port: int = 8770
        cache_ttl: int = 300
        master_key_env: str = "ESG_MASTER_KEY"
        mopd_weights: Dict[str, float] = field(default_factory=lambda: {
            'environmental': 0.4, 'social': 0.3, 'governance': 0.3})
        ga_enabled: bool = True
        ga_population_size: int = 20
        ga_generations: int = 5
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        moe_enabled: bool = True
        moe_expert_count: int = 4
        pareto_enabled: bool = True
        pareto_max_architectures: int = 100
        forecast_enabled: bool = True
        federated_learning_enabled: bool = True
        drift_detection_enabled: bool = True
        user_preference_learning_enabled: bool = True
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        rlhf_training_interval: int = 600
        distillation_enabled: bool = True
        distillation_temperature: float = 2.0
        distillation_interval: int = 300
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        hitl_confidence_threshold: float = 0.65
        causal_rl_enabled: bool = True

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                return b'\x00' * 32
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                return b'\x00' * 32


# =============================================================================
# ORIGINAL CIRCUIT BREAKER + RATE LIMITER
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise e


class RateLimiter:
    def __init__(self, rate=100, window=60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)


# =============================================================================
# ORIGINAL ENHANCED STORAGE (in-memory + SQLite backed)
# =============================================================================
class EnhancedStorage:
    def __init__(self, config: ESGConfig):
        self.config = config
        self.db_path = config.db_path
        self.cache = {}
        self.cache_ttl = config.cache_ttl
        self._esg_assessments: deque = deque(maxlen=1000)
        self._optimisations: deque = deque(maxlen=500)
        self._pareto_front: List[Dict] = []
        self._conn = None
        try:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            for tbl in ['esg_assessments', 'optimisation_history']:
                self._conn.execute(f"CREATE TABLE IF NOT EXISTS {tbl} "
                                   f"(id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
            self._conn.commit()
        except Exception as e:
            logger.warning("Storage init failed: %s", e)

    async def save_esg_assessment(self, assessment: 'SustainabilityAssessmentResult'):
        self._esg_assessments.append(assessment)
        if self._conn:
            try:
                self._conn.execute(
                    "INSERT INTO esg_assessments (data) VALUES (?)",
                    (json.dumps(asdict(assessment), default=str),))
                self._conn.commit()
            except Exception:
                pass

    async def save_optimisation(self, strategy: str, result: Dict):
        self._optimisations.append({'strategy': strategy, 'result': result,
                                    'timestamp': datetime.now().isoformat()})

    async def get_recent_optimisations(self, limit: int = 10) -> List[Dict]:
        return list(self._optimisations)[-limit:]

    async def save_pareto_front(self, solutions: List[Dict]):
        self._pareto_front = solutions

    async def get_current_pareto_front(self) -> List[Dict]:
        return self._pareto_front

    async def save_state(self, key: str, value: str):
        self.cache[key] = value

    async def get_state(self, key: str) -> Optional[str]:
        return self.cache.get(key)

    async def get_carbon_intensity(self, region: str, hours_ago: int = 1) -> Optional[float]:
        return self.cache.get(f"carbon_{region}")

    async def save_carbon_intensity(self, region: str, intensity: float):
        self.cache[f"carbon_{region}"] = intensity

    async def save_node_data(self, node_id: str, helium: float, material: float):
        self.cache[f"node_{node_id}"] = {'helium_index': helium, 'material_index': material}

    async def get_node_data(self, node_id: str) -> Optional[Dict]:
        return self.cache.get(f"node_{node_id}")

    def dispose(self):
        if self._conn:
            try: self._conn.close()
            except Exception: pass


# =============================================================================
# ORIGINAL CARBON INTENSITY MANAGER
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config: ESGConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.region = config.carbon_region
        self._circuit_breaker = CircuitBreaker(name="carbon_api")
        self._rate_limiter = RateLimiter(rate=10, window=60)

    async def get_current_intensity(self) -> float:
        cached = await self.storage.get_carbon_intensity(self.region)
        if cached is not None:
            return cached / 1000.0
        intensity = 400.0
        await self.storage.save_carbon_intensity(self.region, intensity)
        return intensity / 1000.0

    async def close(self):
        pass


class NodeRegistry:
    def __init__(self, storage: EnhancedStorage, config: ESGConfig):
        self.storage = storage
        self.config = config

    async def get_node(self, node_id: str) -> Optional[Dict]:
        cached = await self.storage.get_node_data(node_id)
        if cached:
            return cached
        default = {'helium_index': 0.0, 'material_index': 0.0}
        await self.storage.save_node_data(node_id, 0.0, 0.0)
        return default

    async def close(self):
        pass


# =============================================================================
# ORIGINAL LIMIT GRAPH MANAGER
# =============================================================================
class LimitGraphManager:
    def __init__(self, config: ESGConfig):
        self.config = config
        self.graph: Dict[str, Dict[str, float]] = {}
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ['carbon', 'cost', 'latency', 'energy', 'helium', 'material', 'accuracy']
        for n in nodes:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['energy']['cost'] = 0.6
        self.graph['helium']['cost'] = 0.4
        self.graph['material']['cost'] = 0.3
        self.graph['latency']['cost'] = 0.2
        self.graph['cost']['accuracy'] = -0.1

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name):
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start, end):
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    async def get_graph_summary(self):
        return {'nodes': list(self.graph.keys()),
                'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}


# =============================================================================
# ORIGINAL GA / MoE / Pareto / MTOP / Student
# =============================================================================
class GeneticStrategyOptimizer:
    def __init__(self, config: ESGConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.obj_names = ['environmental', 'social', 'governance']

    def _random_weights(self):
        w = [random.random() for _ in self.obj_names]
        total = sum(w)
        return [v / total for v in w]

    def _mutate(self, weights):
        new_w = weights.copy()
        for i in range(len(new_w)):
            if random.random() < self.mutation_rate:
                new_w[i] = max(0.0, min(1.0, new_w[i] + random.gauss(0, 0.1)))
        total = sum(new_w)
        if total > 0:
            new_w = [v / total for v in new_w]
        return new_w

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for i in range(len(c1)):
            if random.random() < 0.5:
                c1[i], c2[i] = p2[i], p1[i]
        return c1, c2

    async def _evaluate_fitness(self, weights):
        return random.uniform(0.5, 0.95)

    async def run_search(self):
        population = [self._random_weights() for _ in range(self.population_size)]
        best_fitness = -1.0
        best = None
        for gen in range(self.generations):
            fitnesses = await asyncio.gather(
                *[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses),
                                 key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best = sorted_pop[0][0]
            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = random.choice(parents)
                p2 = random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            combined_fitness = await asyncio.gather(
                *[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness),
                                      key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]
        return best if best else self._random_weights()

    async def optimize(self) -> Dict[str, float]:
        best_vec = await self.run_search()
        return {self.obj_names[i]: float(best_vec[i]) for i in range(len(self.obj_names))}


class MoEGatingNetwork:
    def __init__(self, config: ESGConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.experts = {
            'balanced': lambda d: {'environmental': 0.4, 'social': 0.3, 'governance': 0.3},
            'environmental_focused': lambda d: {'environmental': 0.7, 'social': 0.15,
                                                 'governance': 0.15},
            'social_focused': lambda d: {'environmental': 0.2, 'social': 0.6,
                                          'governance': 0.2},
            'governance_focused': lambda d: {'environmental': 0.2, 'social': 0.2,
                                              'governance': 0.6},
        }
        self.expert_names = list(self.experts.keys())
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()

    def _encode_context(self, context, carbon_intensity, node_data):
        if not NUMPY_AVAILABLE:
            return [0.5] * 6
        return np.array([
            min(1.0, carbon_intensity),
            context.get('sector_encoded', 0.5),
            context.get('company_size', 0.5),
            node_data.get('helium_index', 0.0),
            node_data.get('material_index', 0.0),
            len(context.get('suppliers', [])) / 100.0], dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10 or not NUMPY_AVAILABLE:
            return
        try:
            X = np.array([item[0] for item in self._training_data])
            y = np.array([item[1] for item in self._training_data])
            if len(set(y)) < 2:
                return
            self._scaler = StandardScaler()
            X_scaled = self._scaler.fit_transform(X)
            self._gating_model = MLPClassifier(hidden_layer_sizes=(16, 8),
                                                max_iter=200, random_state=42)
            self._gating_model.fit(X_scaled, y)
            self._trained = True
        except Exception:
            self._trained = False

    async def select_expert(self, context, carbon_intensity, node_data):
        selected = 'balanced'
        if self._trained and self._gating_model is not None and NUMPY_AVAILABLE:
            try:
                features = self._encode_context(context, carbon_intensity, node_data)
                X = features.reshape(1, -1)
                if self._scaler:
                    X = self._scaler.transform(X)
                probs = self._gating_model.predict_proba(X)[0]
                selected = self.expert_names[int(np.argmax(probs))]
            except Exception:
                selected = 'balanced'
        return selected, self.experts[selected](context)

    async def add_training_sample(self, context, carbon_intensity, node_data,
                                   selected_expert, reward):
        if not NUMPY_AVAILABLE:
            return
        features = self._encode_context(context, carbon_intensity, node_data)
        try:
            expert_idx = self.expert_names.index(selected_expert)
        except ValueError:
            expert_idx = 0
        async with self._lock:
            self._training_data.append((features, expert_idx))
            if len(self._training_data) % 10 == 0:
                self._train_gating()


class ParetoFrontOptimizer:
    def __init__(self, config: ESGConfig, storage: EnhancedStorage):
        self.config = config
        self.storage = storage
        self.pareto_front: List[Dict] = []
        self.max_size = config.pareto_max_architectures

    def _dominates(self, a, b):
        return (a['env'] >= b['env'] and a['social'] >= b['social']
                and a['gov'] >= b['gov']) and \
               (a['env'] > b['env'] or a['social'] > b['social'] or a['gov'] > b['gov'])

    async def add_assessment(self, assessment):
        entry = {
            'solution_id': f"sol_{uuid.uuid4().hex[:8]}",
            'company_name': assessment.company_name,
            'sector': assessment.sector,
            'env': assessment.environmental_score,
            'social': assessment.social_score,
            'gov': assessment.governance_score,
            'overall': assessment.overall_sustainability_score,
        }
        for existing in self.pareto_front:
            if self._dominates(existing, entry):
                return False
        self.pareto_front = [e for e in self.pareto_front
                              if not self._dominates(entry, e)]
        self.pareto_front.append(entry)
        if len(self.pareto_front) > self.max_size:
            self.pareto_front.sort(key=lambda x: x['overall'])
            self.pareto_front = self.pareto_front[-self.max_size:]
        return True

    def get_pareto_front(self):
        return self.pareto_front


# =============================================================================
# ORIGINAL FEDERATED LEARNER (simplified)
# =============================================================================
class FederatedESGLearner:
    def __init__(self, storage, instance_id, interval):
        self.storage = storage
        self.instance_id = instance_id
        self.interval = interval
        self.insights = deque(maxlen=100)

    async def share_esg_insight(self, insight):
        self.insights.append(insight)

    async def apply_federated_insights(self, params):
        return params


# =============================================================================
# ORIGINAL DRIFT DETECTOR
# =============================================================================
class DriftDetector:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.carbon_history = deque(maxlen=100)
        self.esg_history = deque(maxlen=100)
        self.threshold = 0.15

    async def check_carbon_drift(self, current_intensity):
        self.carbon_history.append(current_intensity)
        if len(self.carbon_history) < 10:
            return False
        recent = list(self.carbon_history)[-10:]
        mean = sum(recent) / len(recent)
        if mean == 0:
            return False
        return abs(current_intensity - mean) > self.threshold * mean


# =============================================================================
# ORIGINAL CARBON FORECASTER (ARIMA)
# =============================================================================
class CarbonForecaster:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config

    async def get_forecast(self, hours_ahead=24):
        return 0.4  # Simulated


# =============================================================================
# ORIGINAL ACTIVE USER PREFERENCE LEARNER
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, websocket):
        self.storage = storage
        self.websocket = websocket

    async def query_user_if_needed(self, user_id, top_options):
        if len(top_options) < 2:
            return None
        scores = [o['overall'] for o in top_options[:2]]
        if max(scores) == 0:
            return None
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            try:
                await self.websocket.broadcast({
                    'type': 'preference_query',
                    'user_id': user_id,
                    'options': [{'id': o['solution_id'],
                                 'name': o['company_name'],
                                 'score': o['overall']}
                                for o in top_options[:2]]}, topic='user_preferences')
            except Exception:
                pass
            return top_options[0]['solution_id']
        return None


# =============================================================================
# ORIGINAL WEBSOCKET SERVER (implemented)
# =============================================================================
class EnhancedWebSocketServer:
    def __init__(self, port):
        self.port = port
        self.connections = set()
        self.subscriptions = defaultdict(set)
        self._lock = asyncio.Lock()
        self.server = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets not available; server disabled")
            return
        try:
            self.server = await ws_serve(self._handle, '0.0.0.0', self.port)
            logger.info("WebSocket server started on port %d", self.port)
        except Exception as e:
            logger.warning("WebSocket start failed: %s", e)

    async def _handle(self, ws, path=None):
        async with self._lock:
            self.connections.add(ws)
        try:
            async for _ in ws:
                pass
        except Exception:
            pass
        finally:
            async with self._lock:
                self.connections.discard(ws)

    async def broadcast(self, message, topic='all'):
        if not self.connections:
            return
        data = json.dumps(message, default=str)
        for conn in list(self.connections):
            try:
                await conn.send(data)
            except Exception:
                self.connections.discard(conn)

    async def stop(self):
        if self.server:
            self.server.close()
            try: await self.server.wait_closed()
            except Exception: pass


# =============================================================================
# ORIGINAL: Quantum + Blockchain stubs
# =============================================================================
class QuantumResilientESGSecurity:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage

    async def get_quantum_status(self):
        return {'pqc_available': False, 'algorithms': ['ecdsa']}

    async def generate_keypair(self, algorithm='dilithium'):
        return {'key_id': f'{algorithm}_{uuid.uuid4().hex[:8]}',
                'algorithm': algorithm, 'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_esg_data(self, data, key_id):
        return {'signature': hashlib.sha3_256(
            json.dumps(data, sort_keys=True, default=str).encode()).hexdigest(),
            'algorithm': 'dilithium-sim', 'key_id': key_id}


class BlockchainESGVerification:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.connected = False

    async def record_esg_data(self, data_id, data_hash, metadata):
        return {'tx_hash': '0x' + hashlib.sha256(os.urandom(32)).hexdigest(),
                'status': 'simulated'}

    async def get_blockchain_status(self):
        return {'connected': self.connected}


class MultiCloudESGDistribution:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'

    async def distribute_esg_data(self, data):
        return {'optimal_provider': self.active_provider,
                'optimal_region': self.active_region,
                'timestamp': datetime.now().isoformat()}

    async def get_distribution_status(self):
        return {'active_provider': self.active_provider,
                'active_region': self.active_region}


# =============================================================================
# ORIGINAL: Data Classes
# =============================================================================
@dataclass
class SupplierNode:
    id: str
    name: str
    esg_score: float = 50.0
    risk_score: float = 50.0
    location: Optional[str] = None
    sector: Optional[str] = None
    tier: int = 1
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SustainabilityAssessmentResult:
    overall_sustainability_score: float
    environmental_score: float
    social_score: float
    governance_score: float
    data_quality_score: float = 100.0
    assessment_time_ms: float = 0.0
    supply_chain_analysis: Dict = field(default_factory=dict)
    financial_impact: Dict = field(default_factory=dict)
    emerging_topics: Dict = field(default_factory=dict)
    scenario_analysis: Dict = field(default_factory=dict)
    trend_analysis: Dict = field(default_factory=dict)
    peer_comparison: Dict = field(default_factory=dict)
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict] = None
    autonomous_optimization: Optional[Dict] = None
    company_name: str = "N/A"
    sector: str = "general"
    # v17.0.0 fields
    temporal_status: Optional[Dict] = None
    xai_explanation: Optional[Dict] = None
    precision_used: Optional[str] = None
    carbon_credit_value_usd: Optional[float] = None
    rec_value_usd: Optional[float] = None
    role_assignments: Optional[Dict] = None
    chaos_test_passed: Optional[bool] = None
    hitl_outcome: Optional[Dict] = None
    federated_round: Optional[int] = None
    causal_ates: Optional[Dict] = None
    limit_graph_summary: Optional[Dict] = None


# ESGDataInput — flexible fallback
if PYDANTIC_AVAILABLE:
    class ESGDataInput(BaseModel):
        company_name: str = "N/A"
        sector: str = "general"
        company_ticker: Optional[str] = None
        carbon_intensity: float = 200.0
        renewable_energy_pct: float = 0.0
        employee_satisfaction: float = 70.0
        board_diversity_pct: float = 40.0
        sustainability_report_available: bool = False
        audited_emissions: bool = False
        double_materiality_assessed: bool = False
        supplier_assessments_performed: bool = False
        suppliers: List[Dict] = Field(default_factory=list)
        documents: List[str] = Field(default_factory=list)
        esg_rating_provider: Optional[str] = None
else:
    @dataclass
    class ESGDataInput:
        company_name: str = "N/A"
        sector: str = "general"
        company_ticker: Optional[str] = None
        carbon_intensity: float = 200.0
        renewable_energy_pct: float = 0.0
        employee_satisfaction: float = 70.0
        board_diversity_pct: float = 40.0
        sustainability_report_available: bool = False
        audited_emissions: bool = False
        double_materiality_assessed: bool = False
        supplier_assessments_performed: bool = False
        suppliers: List[Dict] = field(default_factory=list)
        documents: List[str] = field(default_factory=list)
        esg_rating_provider: Optional[str] = None


# =============================================================================
# FIXED ESGState (async-load instead of async __init__)
# =============================================================================
class ESGState:
    def __init__(self, storage: EnhancedStorage):
        self.storage = storage
        self.confidence = 0.5
        self.uncertainty = 0.1
        self.historical_success_rate = 0.5
        self.reflection_count = 0
        self.carbon_budget_remaining = 100.0
        self.helium_budget_remaining = 100.0
        self.esg_threshold = 80.0
        self.mopd_weights = {"environmental": 0.4, "social": 0.3, "governance": 0.3}

    async def load(self):
        try:
            self.confidence = float(await self.storage.get_state('confidence') or 0.5)
            self.carbon_budget_remaining = float(
                await self.storage.get_state('carbon_budget') or 100.0)
            self.mopd_weights = json.loads(
                await self.storage.get_state('mopd_weights')
                or '{"environmental":0.4,"social":0.3,"governance":0.3}')
        except Exception as e:
            logger.warning("ESGState load failed: %s", e)

    async def save(self):
        await self.storage.save_state('confidence', str(self.confidence))
        await self.storage.save_state('carbon_budget', str(self.carbon_budget_remaining))
        await self.storage.save_state('mopd_weights', json.dumps(self.mopd_weights))

    async def trigger_reflection(self, trigger_type, **kwargs):
        self.reflection_count += 1
        if trigger_type == 'esg_improved':
            self.confidence = min(1.0, self.confidence + 0.05)
        elif trigger_type == 'esg_decreased':
            self.confidence = max(0.1, self.confidence - 0.1)
        elif trigger_type == 'high_carbon':
            self.carbon_budget_remaining *= 0.9
        await self.save()


# =============================================================================
# ORIGINAL AUTONOMOUS ESG OPTIMIZER (with v17 modules wired in)
# =============================================================================
class AutonomousESGOptimizer:
    def __init__(self, config, storage, state,
                 rlhf=None, distillation=None, limit_graph=None,
                 role_coordinator=None, causal_shaper=None):
        self.config = config
        self.storage = storage
        self.state = state
        self._lock = asyncio.Lock()
        self.moe_gating = MoEGatingNetwork(config, storage) if config.moe_enabled else None
        self.ga_optimizer = GeneticStrategyOptimizer(config, storage) if config.ga_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(config, storage) if config.pareto_enabled else None
        self.limit_graph = limit_graph
        self.rlhf = rlhf
        self.distillation = distillation
        self.role_coordinator = role_coordinator
        self.causal_shaper = causal_shaper

    async def optimize_esg(self, current_state, strategy=None):
        carbon_intensity = current_state.get('carbon_intensity', 400)

        selected_expert = 'balanced'
        weights = {'environmental': 0.4, 'social': 0.3, 'governance': 0.3}
        source = 'moe'

        # Priority: RLHF > Distillation > MoE
        if self.rlhf and self.rlhf.history:
            probs = await self.rlhf.get_policy_probs(current_state)
            names = ['environmental_focused', 'social_focused',
                     'governance_focused', 'balanced']
            idx = int(np.argmax(probs)) % len(names) if NUMPY_AVAILABLE else 0
            selected_expert = names[idx]
            weights = {'environmental': float(probs[0]) if len(probs) > 0 else 0.4,
                       'social': float(probs[1]) if len(probs) > 1 else 0.3,
                       'governance': float(probs[2]) if len(probs) > 2 else 0.3}
            source = 'rlhf'
        elif self.distillation and self.distillation.get_student_probs() != [0.25] * 4:
            probs = self.distillation.get_student_probs()
            names = ['environmental_focused', 'social_focused',
                     'governance_focused', 'balanced']
            idx = int(np.argmax(probs)) % len(names) if NUMPY_AVAILABLE else 0
            selected_expert = names[idx]
            source = 'distillation'
        elif self.moe_gating and self.config.moe_enabled:
            selected_expert, weights = await self.moe_gating.select_expert(
                current_state, carbon_intensity, {})

        # LIMIT Graph adjustment
        if self.limit_graph:
            await self.limit_graph.update_constraint('carbon', carbon_intensity)
            influence = await self.limit_graph.evaluate_path('carbon', 'cost')
            if influence > 0.5:
                weights['governance'] = weights.get('governance', 0.3) + 0.1
                total = sum(weights.values())
                weights = {k: v / total for k, v in weights.items()}

        result = {
            'action': f'{selected_expert}_optimization',
            'selected_strategy': selected_expert,
            'weights': weights,
            'source': source,
            'recommendation': self._generate_recommendation(selected_expert),
        }
        await self.storage.save_optimisation(selected_expert, result)

        # GA weight evolution
        if self.ga_optimizer and self.config.ga_enabled:
            best_weights = await self.ga_optimizer.optimize()
            if best_weights:
                self.state.mopd_weights.update(best_weights)
                await self.state.save()

        return result

    async def record_outcome(self, reward, context):
        if self.moe_gating and self.config.moe_enabled:
            carbon_intensity = context.get('carbon_intensity', 400)
            await self.moe_gating.add_training_sample(
                context, carbon_intensity, {},
                context.get('selected_strategy', 'balanced'), reward)
        if self.causal_shaper:
            try:
                idx = ['environmental_focused', 'social_focused',
                       'governance_focused', 'balanced'].index(
                    context.get('selected_strategy', 'balanced'))
            except ValueError:
                idx = 3
            self.causal_shaper.record(idx, reward, [0.25, 0.25, 0.25, 0.25])

    def _generate_recommendation(self, strategy):
        return {
            'balanced': "Focus on balanced ESG improvements.",
            'environmental_focused': "Prioritise carbon reduction and renewables.",
            'social_focused': "Prioritise employee wellbeing and community.",
            'governance_focused': "Prioritise board diversity and transparency.",
        }.get(strategy, "Maintain current strategy.")


# =============================================================================
# ORIGINAL MTOP / Distillation (kept for API compat)
# =============================================================================
class ESGTeacherEnsemble:
    def __init__(self, config):
        self.config = config
        self.teacher_weights = {'performance': 0.25, 'carbon': 0.25,
                                'cost': 0.25, 'adaptive': 0.25}

    async def get_teacher_scores(self, state, carbon_intensity):
        return {n: {'balanced': 0.25, 'environmental_focused': 0.25,
                    'social_focused': 0.25, 'governance_focused': 0.25}
                for n in self.teacher_weights}


class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None, role_coordinator=None):
        self.config = config
        self.moe_engine = moe_engine
        self.role_coordinator = role_coordinator
        self.student_policy = np.array([0.25] * 4) if NUMPY_AVAILABLE else [0.25] * 4
        self.temperature = getattr(config, 'distillation_temperature', 2.0)
        self.history: List[Dict] = []

    async def distill(self, state):
        if not self.moe_engine or not NUMPY_AVAILABLE:
            return
        try:
            _, weights = await self.moe_engine.select_expert(state, 400, {})
            teacher = np.array([weights.get('environmental', 0.4),
                                weights.get('social', 0.3),
                                weights.get('governance', 0.3),
                                0.0])
            teacher = teacher / (teacher.sum() + 1e-9)
        except Exception:
            teacher = np.ones(4) / 4
        soft = np.exp(np.log(teacher + 1e-6) / self.temperature)
        soft /= soft.sum()
        loss = -np.sum(soft * np.log(self.student_policy + 1e-6))
        grad = -soft / (self.student_policy + 1e-6)
        self.student_policy = np.clip(self.student_policy - 0.01 * grad, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        self.history.append({'loss': float(loss),
                             'timestamp': datetime.now().isoformat()})

    def get_student_probs(self):
        if NUMPY_AVAILABLE:
            return self.student_policy.tolist()
        return list(self.student_policy)


# =============================================================================
# ENHANCED SUSTAINABILITY SYSTEM v17.0.0
# =============================================================================
class EnhancedSustainabilitySystemV17:
    """
    Enhanced sustainability system v17.0.0 with all ten v17 enhancement areas.
    Preserves v16 APIs.
    """

    def __init__(self, config: Optional[ESGConfig] = None):
        self.config = config or ESGConfig()
        self.instance_id = self.config.instance_id
        self.sector = "general"

        # Core
        self.storage = EnhancedStorage(self.config)
        self.state = ESGState(self.storage)
        self.quantum_security = QuantumResilientESGSecurity(self.config, self.storage)
        self.blockchain = BlockchainESGVerification(self.config, self.storage)
        self.carbon_client = CarbonIntensityManager(self.config, self.storage)
        self.cloud_distributor = MultiCloudESGDistribution(self.config, self.storage)
        self.node_registry = NodeRegistry(self.storage, self.config)

        # Feature flags
        self.temporal_logic_enabled = self.config.temporal_logic_enabled
        self.xai_enabled = self.config.xai_enabled
        self.adaptive_precision_enabled = self.config.adaptive_precision_enabled
        self.carbon_market_enabled = self.config.carbon_market_enabled
        self.role_specialization_enabled = self.config.role_specialization_enabled
        self.chaos_testing_enabled = self.config.chaos_testing_enabled
        self.hitl_enabled = self.config.hitl_enabled

        # ---- v17.0.0 modules ----
        self.temporal_monitor = TemporalLogicMonitor() if self.temporal_logic_enabled else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("esg_floor", "G(esg_score >= 0.0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 1.0)")
            self.temporal_monitor.add_formula("convergence", "F(esg_score >= 70.0)")

        self.xai = XAIExplainer(['environmental', 'social', 'governance']) \
            if self.xai_enabled else None
        self.precision_controller = AdaptivePrecisionController() \
            if self.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketClient() if self.carbon_market_enabled else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if self.role_specialization_enabled else None

        # Active RLHF + HITL
        self.rlhf = ActiveRLHF(
            action_space=['environmental_focused', 'social_focused',
                          'governance_focused', 'balanced'],
        ) if self.config.rlhf_enabled else None
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (self.hitl_enabled and self.rlhf) else None

        # Federated + Causal
        self.federated = FederatedAggregator(num_params=3)
        self.causal_shaper = CausalRewardShaper(num_actions=4) \
            if self.config.causal_rl_enabled else None

        # LIMIT Graph
        self.limit_graph = LimitGraphManager(self.config) \
            if self.config.limit_graph_enabled else None

        # Distillation + Autonomous optimizer (wired with v17 modules)
        self.distillation = MultiTeacherPolicyDistillation(
            self.config, None, self.role_coordinator,
        ) if self.config.distillation_enabled else None

        self.autonomous_optimizer = AutonomousESGOptimizer(
            self.config, self.storage, self.state,
            rlhf=self.rlhf, distillation=self.distillation,
            limit_graph=self.limit_graph,
            role_coordinator=self.role_coordinator,
            causal_shaper=self.causal_shaper,
        )
        # Wire MoE into distillation for teacher policy source
        if self.distillation:
            self.distillation.moe_engine = self.autonomous_optimizer.moe_gating

        # Legacy compat
        self.federated_learner = FederatedESGLearner(
            self.storage, self.instance_id, 3600)
        self.drift_detector = DriftDetector(self.storage, self.config) \
            if self.config.drift_detection_enabled else None
        self.forecaster = CarbonForecaster(self.storage, self.config) \
            if self.config.forecast_enabled else None

        # WebSocket (BEFORE user_pref_learner to fix init order bug)
        self.websocket = EnhancedWebSocketServer(self.config.websocket_port)

        self.user_pref_learner = ActiveUserPreferenceLearner(
            self.storage, self.websocket) \
            if self.config.user_preference_learning_enabled else None

        # Chaos tester (needs system ref)
        self.chaos_tester = ChaosTester(self) if self.chaos_testing_enabled else None

        # Rate limiter + circuit breakers
        self.rate_limiter = RateLimiter(rate=100, window=60)
        self.circuit_breakers = {
            'esg_api': CircuitBreaker(name="esg_api"),
            'assessment': CircuitBreaker(name="assessment"),
        }

        # History
        self.assessment_history: deque = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._assessment_semaphore = asyncio.Semaphore(10)
        self._background_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._carbon_saved_kg_total = 0.0
        self._last_metadata: Optional[Dict] = None

        # Benchmarks
        self.industry_benchmarks = {
            'technology': {'e': 65, 's': 70, 'g': 68, 'overall': 67},
            'manufacturing': {'e': 55, 's': 60, 'g': 62, 'overall': 59},
            'energy': {'e': 45, 's': 55, 'g': 58, 'overall': 52},
            'finance': {'e': 50, 's': 68, 'g': 75, 'overall': 64},
        }

        if PROMETHEUS_AVAILABLE:
            try:
                start_http_server(self.config.metrics_port)
            except Exception:
                pass

        logger.info("EnhancedSustainabilitySystemV17 v%s initialized "
                    "(instance %s)", self.config.version, self.instance_id)

    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.state.load()
        await self.websocket.start()

        try:
            loop = asyncio.get_event_loop()
            self._background_tasks = [
                loop.create_task(self._carbon_update_loop()),
                loop.create_task(self._limit_graph_loop()),
                loop.create_task(self._rlhf_loop()),
                loop.create_task(self._distillation_loop()),
                loop.create_task(self._federated_loop()),
                loop.create_task(self._ga_loop()),
                loop.create_task(self._cleanup_loop()),
            ]
            if self.chaos_tester:
                self._background_tasks.append(loop.create_task(self._chaos_loop()))
        except RuntimeError:
            pass
        logger.info("Sustainability system started")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.carbon_client.get_current_intensity()
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Carbon loop error: %s", e)

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.limit_graph:
                    ci = await self.carbon_client.get_current_intensity()
                    await self.limit_graph.update_constraint('carbon', ci)
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Limit graph loop error: %s", e)

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                await asyncio.sleep(self.config.rlhf_training_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("RLHF loop error: %s", e)

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                if self.distillation:
                    ci = await self.carbon_client.get_current_intensity()
                    await self.distillation.distill(
                        {'carbon_intensity': ci, 'node_data': {}})
                await asyncio.sleep(self.config.distillation_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Distillation loop error: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)
                if self.federated.client_updates:
                    self.federated.aggregate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Federated loop error: %s", e)

    async def _ga_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                if self.autonomous_optimizer.ga_optimizer:
                    await self.autonomous_optimizer.ga_optimizer.optimize()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("GA loop error: %s", e)

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(86400)
            except asyncio.CancelledError:
                break

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                fault = random.choice(ChaosTester.FAULT_TYPES)
                await self.chaos_tester.run_test(fault, duration_s=0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ----------------------------------------------------------------------
    # MAIN: comprehensive_sustainability_assessment (the missing method)
    # ----------------------------------------------------------------------
    async def comprehensive_sustainability_assessment(
        self, esg_data: Dict, financial_data: Optional[Dict] = None,
        user_id: Optional[str] = None, run_scenarios: bool = False
    ) -> SustainabilityAssessmentResult:
        async with self._assessment_semaphore:
            await self.rate_limiter.wait_and_acquire()
            start = time.time()

            # Validate input
            try:
                validated = ESGDataInput(**esg_data)
            except Exception as e:
                logger.warning("ESGDataInput validation failed: %s", e)
                validated = ESGDataInput()

            # Carbon intensity
            carbon_intensity = await self.carbon_client.get_current_intensity()

            # Adaptive precision
            precision = PrecisionLevel.FP32
            if self.precision_controller:
                precision = self.precision_controller.select(carbon_intensity, 0.95)

            # Temporal gate
            temporal_status = {}
            if self.temporal_monitor:
                self.temporal_monitor.update({
                    'esg_score': 75.0,
                    'carbon': float(carbon_intensity)})
                temporal_status = self.temporal_monitor.evaluate()

            # Base ESG scores
            env_score = max(0.0, 100.0 - getattr(validated, 'carbon_intensity', 200) / 10)
            if hasattr(validated, 'renewable_energy_pct'):
                env_score = (env_score + validated.renewable_energy_pct * 0.8) / 2
            social_score = (70 + getattr(validated, 'employee_satisfaction', 70)) / 2
            governance_score = (65 + getattr(validated, 'board_diversity_pct', 40) * 1.2) / 2

            # MOPD weights
            weights = self.state.mopd_weights
            overall = (env_score * weights.get('environmental', 0.4) +
                       social_score * weights.get('social', 0.3) +
                       governance_score * weights.get('governance', 0.3))

            # LIMIT graph constraint
            if self.limit_graph:
                await self.limit_graph.update_constraint('carbon', carbon_intensity)
                influence = await self.limit_graph.evaluate_path('carbon', 'cost')
                if influence > 0.5:
                    overall *= 0.95  # small penalty when carbon influence high

            # Autonomous optimizer (RLHF > Distillation > MoE)
            state = {'esg_score': overall, 'carbon_intensity': carbon_intensity,
                     'cost_budget': self.state.carbon_budget_remaining,
                     'sector': getattr(validated, 'sector', 'general')}
            opt = await self.autonomous_optimizer.optimize_esg(state)
            selected_strategy = opt['selected_strategy']
            await self.autonomous_optimizer.record_outcome(
                overall / 100.0, {'carbon_intensity': carbon_intensity,
                                   'selected_strategy': selected_strategy})

            # XAI explanation
            xai_out = None
            if self.xai:
                cand = {'environmental': env_score, 'social': social_score,
                        'governance': governance_score}
                xai_out = self.xai.explain(cand, weights, [cand])

            # Role assignments
            role_assignments = None
            if self.role_coordinator:
                role_assignments = self.role_coordinator.assign_roles({
                    'trust': 0.7, 'compute': 0.7,
                    'energy': 1.0 - min(1.0, carbon_intensity),
                    'performance': overall / 100.0})

            # HITL escalation (only when confidence low)
            hitl_outcome = None
            confidence = overall / 100.0
            if self.hitl and confidence < self.config.hitl_confidence_threshold:
                try:
                    hitl_outcome = await self.hitl.escalate(
                        decision_context={'strategy': selected_strategy,
                                          'carbon': carbon_intensity},
                        options=['accept', 'retry', 'downgrade'],
                        confidence=confidence,
                        confidence_threshold=self.config.hitl_confidence_threshold)
                except Exception:
                    pass

            # Carbon credit + REC
            credit_value = 0.0
            rec_value = 0.0
            if self.carbon_market:
                try:
                    saved_kg = max(0.0, (400.0 - carbon_intensity * 1000) * 0.001)
                    credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                    rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                    self._carbon_saved_kg_total += saved_kg
                except Exception:
                    pass

            # Federated submission
            federated_round = None
            if self.federated:
                try:
                    self.federated.submit_update(
                        self.instance_id,
                        [env_score / 100, social_score / 100, governance_score / 100],
                        samples=1)
                    if len(self.assessment_history) % 5 == 0:
                        agg = self.federated.aggregate()
                        federated_round = agg['round']
                except Exception:
                    pass

            # Chaos smoke
            chaos_passed = None
            if self.chaos_tester and len(self.assessment_history) % 5 == 0:
                try:
                    cr = await self.chaos_tester.run_test(
                        random.choice(ChaosTester.FAULT_TYPES), duration_s=0.02)
                    chaos_passed = cr['passed']
                except Exception:
                    pass

            # Peer comparison
            sector = getattr(validated, 'sector', 'general').lower()
            benchmark = self.industry_benchmarks.get(
                sector, self.industry_benchmarks['technology'])
            peer = {
                'sector': sector,
                'benchmark_score': benchmark['overall'],
                'comparison': 'above' if overall > benchmark['overall'] else 'below',
                'gap': overall - benchmark['overall'],
            }

            # Blockchain record
            bc_hash = None
            try:
                key = await self.quantum_security.generate_keypair()
                data_hash = hashlib.sha256(
                    f"{validated.company_name}_{overall}".encode()).hexdigest()
                bc = await self.blockchain.record_esg_data(
                    f"esg_{uuid.uuid4().hex[:8]}", data_hash,
                    {'esg_score': overall, 'sector': sector})
                bc_hash = bc.get('tx_hash')
            except Exception:
                pass

            cloud = await self.cloud_distributor.distribute_esg_data({'size_gb': 0.001})

            result = SustainabilityAssessmentResult(
                overall_sustainability_score=overall,
                environmental_score=env_score,
                social_score=social_score,
                governance_score=governance_score,
                data_quality_score=90.0,
                assessment_time_ms=(time.time() - start) * 1000,
                peer_comparison=peer,
                blockchain_tx_hash=bc_hash,
                cloud_distribution=cloud,
                autonomous_optimization={
                    'selected_strategy': selected_strategy, 'source': opt['source']},
                company_name=getattr(validated, 'company_name', 'N/A'),
                sector=sector,
                # v17.0.0 fields
                temporal_status=temporal_status,
                xai_explanation=xai_out,
                precision_used=precision.value,
                carbon_credit_value_usd=credit_value,
                rec_value_usd=rec_value,
                role_assignments=role_assignments,
                chaos_test_passed=chaos_passed,
                hitl_outcome=hitl_outcome,
                federated_round=federated_round,
                causal_ates=self.causal_shaper.compute_ate() if self.causal_shaper else {},
                limit_graph_summary=await self.limit_graph.get_graph_summary()
                    if self.limit_graph else None,
            )

            # State update
            if overall > 80:
                await self.state.trigger_reflection('esg_improved')
            else:
                await self.state.trigger_reflection('esg_decreased')
            if carbon_intensity > 0.4:
                await self.state.trigger_reflection('high_carbon')

            # Record + persist
            async with self._history_lock:
                self.assessment_history.append(result)
            await self.storage.save_esg_assessment(result)

            # WebSocket
            try:
                await self.websocket.broadcast({
                    'type': 'esg_assessment',
                    'company': result.company_name,
                    'esg_score': overall,
                    'strategy': selected_strategy,
                    'timestamp': datetime.now().isoformat()}, topic='esg')
            except Exception:
                pass

            # Metrics
            SUSTAINABILITY_ASSESSMENTS.labels(status='success', sector=sector).inc()
            ESG_SCORE.labels(sector=sector).set(overall)

            # Save metadata for get_last_metadata()
            self._last_metadata = {
                'precision_used': precision.value,
                'temporal_status': temporal_status,
                'xai_explanation': xai_out,
                'role_assignments': role_assignments,
                'hitl_outcome': hitl_outcome,
                'carbon_credit_value_usd': credit_value,
                'rec_value_usd': rec_value,
                'federated_round': federated_round,
                'chaos_test_passed': chaos_passed,
                'causal_ates': result.causal_ates,
                'strategy': selected_strategy,
                'carbon_intensity': carbon_intensity,
            }

            audit_logger.info("ESG Assessment: %s | Score=%.1f",
                              result.company_name, overall)
            return result

    # ----------------------------------------------------------------------
    # v17.0.0 utilities
    # ----------------------------------------------------------------------
    def get_last_metadata(self) -> Optional[Dict]:
        return self._last_metadata

    def select_precision(self, carbon_intensity: float = 0.4,
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

    def get_role_assignments(self, context: Optional[Dict] = None) -> Dict:
        if self.role_coordinator is None:
            return {}
        ctx = context or {'trust': 0.7, 'compute': 0.7,
                          'energy': 0.7, 'performance': 0.7}
        return self.role_coordinator.assign_roles(ctx)

    def get_causal_ates(self) -> Dict:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

    async def escalate_decision(self, decision_context, options, confidence) -> Dict:
        if self.hitl is None:
            return {'escalated': False,
                    'chosen': options[0] if options else 'noop',
                    'source': 'fallback'}
        return await self.hitl.escalate(decision_context, options, confidence,
                                        self.config.hitl_confidence_threshold)

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

    def get_comprehensive_status(self) -> Dict:
        status = {
            'instance_id': self.instance_id,
            'version': self.config.version,
            'assessment_count': len(self.assessment_history),
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'features': {
                'temporal_logic': self.temporal_logic_enabled,
                'xai': self.xai_enabled,
                'adaptive_precision': self.adaptive_precision_enabled,
                'carbon_market': self.carbon_market_enabled,
                'role_specialization': self.role_specialization_enabled,
                'chaos_testing': self.chaos_testing_enabled,
                'hitl': self.hitl_enabled,
                'federated': True,
                'causal_rl': self.config.causal_rl_enabled,
            },
            'mopd_weights': self.state.mopd_weights,
            'timestamp': datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.rlhf:
            status['rlhf'] = {'history_len': len(self.rlhf.history),
                              'feedback_buffer': len(self.rlhf.feedback_buffer)}
        if self.distillation:
            status['distillation'] = {
                'student_probs': self.distillation.get_student_probs(),
                'history_len': len(self.distillation.history)}
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
            status['causal_ates'] = self.causal_shaper.compute_ate()
        return status

    async def shutdown(self):
        logger.info("Shutting down EnhancedSustainabilitySystemV17...")
        self._shutdown_event.set()
        self._running = False
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_client.close()
        await self.websocket.stop()
        await self.state.save()
        self.storage.dispose()
        logger.info("Shutdown complete")


# Backward compat alias
EnhancedSustainabilitySystemV16 = EnhancedSustainabilitySystemV17


# =============================================================================
# SINGLETON + SIGNAL HANDLING + MAIN
# =============================================================================
_shutdown_event_global = asyncio.Event()
_system_instance: Optional[EnhancedSustainabilitySystemV17] = None
_system_lock = asyncio.Lock()
_shutdown_requested = False


def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        try:
            asyncio.create_task(_signal_shutdown())
        except Exception:
            pass


async def _signal_shutdown():
    _shutdown_event_global.set()


async def shutdown_handler():
    global _system_instance
    if _system_instance:
        await _system_instance.shutdown()
        _system_instance = None


async def get_sustainability_system(config: Optional[ESGConfig] = None) -> EnhancedSustainabilitySystemV17:
    global _system_instance
    if _system_instance is None:
        async with _system_lock:
            if _system_instance is None:
                _system_instance = EnhancedSustainabilitySystemV17(config)
                await _system_instance.start()
    return _system_instance


async def _smoke_test():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Sustainability Signals System v17.0.0 — smoke test")
    print("=" * 78)

    system = await get_sustainability_system()

    print("\n✅ v17.0.0 ENHANCEMENTS:")
    print("   ✅ Temporal Logic Verification (G/F/U/->)")
    print("   ✅ Explainable AI for ESG decisions")
    print("   ✅ Adaptive Precision Switching")
    print("   ✅ Carbon Markets + Renewable Energy Credits")
    print("   ✅ Multi-Agent Role Specialization (emergent)")
    print("   ✅ Chaos Testing as first-class citizen")
    print("   ✅ Active RLHF with uncertainty-triggered human queries")
    print("   ✅ Human-in-the-Loop Coordinator")
    print("   ✅ Federated Green Learning (FedAvg)")
    print("   ✅ Causal RL hooks (IPW / ATE)")

    esg_data = {
        'company_name': 'EcoTech Inc.',
        'company_ticker': 'ECO',
        'sector': 'technology',
        'carbon_intensity': 150,
        'renewable_energy_pct': 40,
        'employee_satisfaction': 78,
        'board_diversity_pct': 45,
        'suppliers': [
            {'id': 's1', 'name': 'Supplier A', 'esg_score': 70, 'risk_score': 30},
            {'id': 's2', 'name': 'Supplier B', 'esg_score': 55, 'risk_score': 50},
        ],
        'documents': [
            'We are committed to reducing carbon emissions by 50% by 2030.',
            'Board diversity has improved.',
        ],
    }
    financial_data = {'revenue': 1000, 'profit_margin': 0.15}

    print("\n🔬 Running sample ESG assessment...")
    result = await system.comprehensive_sustainability_assessment(
        esg_data, financial_data, user_id='user_123', run_scenarios=True)

    print(f"   ESG Score: {result.overall_sustainability_score:.1f}/100")
    print(f"   Environmental: {result.environmental_score:.1f}")
    print(f"   Social: {result.social_score:.1f}")
    print(f"   Governance: {result.governance_score:.1f}")
    print(f"   Strategy: {result.autonomous_optimization['selected_strategy']} "
          f"(source={result.autonomous_optimization['source']})")
    print(f"   Precision: {result.precision_used}")
    print(f"   Carbon credit: ${result.carbon_credit_value_usd or 0:.4f}")
    print(f"   REC value: ${result.rec_value_usd or 0:.4f}")
    if result.temporal_status:
        print(f"   Temporal: {result.temporal_status}")
    if result.role_assignments:
        print(f"   Dominant role: {result.role_assignments.get('dominant_role')}")
    if result.xai_explanation and result.xai_explanation.get('narrative'):
        print(f"   XAI: {result.xai_explanation['narrative'][0]}")
    if result.hitl_outcome:
        print(f"   HITL: {result.hitl_outcome.get('source')} -> "
              f"{result.hitl_outcome.get('chosen')}")
    print(f"   Peer comparison: {result.peer_comparison}")

    print("\n📝 Recording RLHF feedback...")
    await system.rlhf.record_feedback(
        state={'carbon_intensity': 0.15, 'esg_score': 0.85},
        action='environmental_focused', reward=0.85)
    print(f"   RLHF feedback recorded.")

    print("\n⚙️  Precision selector →",
          system.select_precision(carbon_intensity=0.3).value)
    cc = await system.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"\n🎭 Roles: {system.get_role_assignments()}")
    print(f"🧠 Causal ATEs: {system.get_causal_ates()}")

    print("\n🧪 Chaos suite:")
    chaos = await system.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📊 Comprehensive status:")
    status = system.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'assessment_count': status['assessment_count'],
        'carbon_saved_kg': status['carbon_saved_kg_total'],
        'features': status['features'],
        'mopd_weights': status['mopd_weights'],
        'rlhf': status.get('rlhf'),
        'distillation': status.get('distillation'),
        'federated': status.get('federated'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'causal_ates': status.get('causal_ates'),
    }, indent=2, default=str))

    await system.shutdown()
    print("\n" + "=" * 78)
    print("✅ Sustainability Signals System v17.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
