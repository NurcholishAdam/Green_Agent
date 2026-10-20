#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/synthetic_data_generator_enhanced_v5_0.py
# VERSION: 6.1.0  (Phase 1-3 enhancements applied — see changelog at bottom)
# =============================================================================
"""
Advanced Synthetic Data Generator for Green Agent v6.1.0

v6.0.0 features preserved: temporal logic, XAI, adaptive precision, carbon
markets, role specialization, chaos testing, active RLHF, HITL, federated
green learning, causal RL hooks.

v6.1.0 (Phase 1-3) adds:
    [P1] Precision-unit fix, real MoE (per-expert params + trained classifier),
         real distillation (KL update from MoE teacher), RLHF policy wiring,
         baseline-aware carbon credits, chaos-env gating, signal registration,
         real drift detection, adaptive-anomaly learning, federated→weights
         wiring, computed quality metrics, input validation.
    [P2] aiosqlite async storage, CircuitBreaker lock, bounded violations,
         new Prometheus metrics (latency/active/drift/anomaly-weight),
         correlation-ID propagation.
    [P3] Batched parallel generation, streaming API, real GA fitness,
         real Pareto front, UCB adaptive anomalies, train/val/test split,
         dataset hashing + signing, RLHF/state persistence.
"""

import asyncio
import gc
import hashlib
import json
import logging
import logging.handlers
import math
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
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple, Union
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
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve as ws_serve
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging with correlation IDs — [P2]
# -----------------------------------------------------------------------------
correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s '
               '[cid=%(correlation_id)s]')


class _CorrelationFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True


for _h in logger.handlers:
    _h.addFilter(_CorrelationFilter())


# =============================================================================
# PROMETHEUS METRICS — [P2] new latency / active / drift / anomaly-weight
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SYNTHETIC_SAMPLES = Counter('synthetic_samples_generated_total', 'Samples',
                                 ['type'], registry=REGISTRY)
    SYNTHETIC_ANOMALIES = Counter('synthetic_anomalies_injected_total', 'Anomalies',
                                    ['anomaly_type'], registry=REGISTRY)
    SC_TEMPORAL = Counter('synth_temporal_violations_total', 'Temporal',
                           ['formula'], registry=REGISTRY)
    SC_CHAOS = Counter('synth_chaos_tests_total', 'Chaos',
                        ['fault', 'status'], registry=REGISTRY)
    SC_HITL = Counter('synth_hitl_escalations_total', 'HITL',
                       ['status'], registry=REGISTRY)
    SC_FEDERATED = Counter('synth_federated_rounds_total', 'Federated', registry=REGISTRY)
    SC_CARBON_CREDITS = Counter('synth_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    SC_XAI = Counter('synth_xai_explanations_total', 'XAI', registry=REGISTRY)
    SC_PRECISION = Counter('synth_precision_selections_total', 'Precision',
                            ['level'], registry=REGISTRY)
    # [P2] new
    SYNTH_LATENCY = Histogram(
        'synth_dataset_latency_seconds', 'Dataset generation latency',
        buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0), registry=REGISTRY)
    SYNTH_ACTIVE = Gauge('synth_active_generations', 'Active generations', registry=REGISTRY)
    SYNTH_DRIFT = Counter('synth_drift_events_total', 'Drift events',
                           ['metric'], registry=REGISTRY)
    SYNTH_ANOMALY_WEIGHT = Gauge('synth_anomaly_weight', 'Learned anomaly weight',
                                   ['type'], registry=REGISTRY)
    SYNTH_QUALITY = Gauge('synth_data_quality', 'Data quality', ['sector'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    SYNTHETIC_SAMPLES = SYNTHETIC_ANOMALIES = DummyMetric()
    SC_TEMPORAL = SC_CHAOS = SC_HITL = DummyMetric()
    SC_FEDERATED = SC_CARBON_CREDITS = SC_XAI = SC_PRECISION = DummyMetric()
    SYNTH_LATENCY = SYNTH_ACTIVE = SYNTH_DRIFT = SYNTH_ANOMALY_WEIGHT = DummyMetric()
    SYNTH_QUALITY = DummyMetric()


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
# MODULE A — TEMPORAL LOGIC MONITOR  [P2] bounded violations
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ."""

    def __init__(self, history_len: int = 200, max_violations: int = 100):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: deque = deque(maxlen=max_violations)  # [P2]

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        self.history.append(dict(state))

    def _compile(self, formula: str) -> Callable:
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

    def _atom(self, atom: str) -> Callable:
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
                SC_TEMPORAL.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': list(self.violations)[-5:]}


# =============================================================================
# MODULE B — XAI EXPLAINER
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
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the utility."
                     for f, v in ranked]
        SC_XAI.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}


# =============================================================================
# MODULE C — ADAPTIVE PRECISION CONTROLLER  [P1] documented units
# =============================================================================
class AdaptivePrecisionController:
    """Precision selector. `select()` expects **gCO2/kWh** (e.g. 350-500 typical).
    Use `select_kg()` if you only have kgCO2/kWh."""

    def __init__(self):
        self.telemetry = {'gpu_available': False, 'memory_gb': 16.0, 'utilization': 0.3}
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity_g_per_kwh: float,
               accuracy_required: float = 0.95) -> PrecisionLevel:
        # [P1] explicit gCO2/kWh — matches region_carbon config values
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity_g_per_kwh > 500:
            self.last_precision = PrecisionLevel.FP8
        elif carbon_intensity_g_per_kwh > 350:
            self.last_precision = PrecisionLevel.FP16
        elif self.telemetry.get('gpu_available'):
            self.last_precision = PrecisionLevel.BF16
        else:
            self.last_precision = PrecisionLevel.FP16
        SC_PRECISION.labels(level=self.last_precision.value).inc()
        return self.last_precision

    def select_kg(self, carbon_intensity_kg_per_kwh: float,
                  accuracy_required: float = 0.95) -> PrecisionLevel:
        # [P1] convenience wrapper
        return self.select(carbon_intensity_kg_per_kwh * 1000.0, accuracy_required)

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


# =============================================================================
# MODULE D — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    def __init__(self, carbon_price_per_ton: float = 50.0,
                 rec_price_per_mwh: float = 30.0):
        self.carbon_price_per_ton = carbon_price_per_ton
        self.rec_price_per_mwh = rec_price_per_mwh
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
# MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        if NUMPY_AVAILABLE:
            self.affinity = np.array([
                [0.7, 0.9, 0.4, 0.6],
                [0.4, 0.5, 0.9, 0.5],
                [0.9, 0.4, 0.3, 0.7],
                [0.3, 0.2, 0.3, 0.3],
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
# MODULE F — CHAOS TESTER  [P1] carbon_api_down actually injects a fault
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'storage_broken', 'moe_broken',
                   'ga_broken', 'rlhf_broken', 'distillation_broken']

    def __init__(self, generator_ref=None):
        self.generator = generator_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        g = self.generator

        try:
            if fault_type == 'carbon_api_down' and g:
                # [P1] actually break the region_carbon map (empty)
                orig_map = dict(g.region_carbon)
                g.region_carbon = {}
                restore.append(lambda: setattr(g, 'region_carbon', orig_map))
            elif fault_type == 'storage_broken' and g:
                orig = g.storage.save_generation_history
                async def broken(*a, **k): raise RuntimeError("storage broken")
                g.storage.save_generation_history = broken
                restore.append(lambda: setattr(g.storage,
                                                'save_generation_history', orig))
            elif fault_type == 'moe_broken' and g and g.moe_gating:
                orig = g.moe_gating.select_expert
                async def broken(*a, **k): raise RuntimeError("moe broken")
                g.moe_gating.select_expert = broken
                restore.append(lambda: setattr(g.moe_gating, 'select_expert', orig))
            elif fault_type == 'ga_broken' and g and g.ga_optimizer:
                orig = g.ga_optimizer.optimize
                async def broken(*a, **k): raise RuntimeError("ga broken")
                g.ga_optimizer.optimize = broken
                restore.append(lambda: setattr(g.ga_optimizer, 'optimize', orig))
            elif fault_type == 'rlhf_broken' and g and g.rlhf:
                orig = g.rlhf.get_policy_probs
                async def broken(_): raise RuntimeError("rlhf broken")
                g.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(g.rlhf, 'get_policy_probs', orig))
            elif fault_type == 'distillation_broken' and g and g.distillation:
                orig = g.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distillation broken")
                g.distillation.distill = broken
                restore.append(lambda: setattr(g.distillation, 'distill', orig))
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
# MODULE G — ACTIVE RLHF  [P1] get_policy_probs returns real policy
# =============================================================================
class ActiveRLHF:
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
        self._lock = asyncio.Lock()

    def _state_to_features(self, state: Dict) -> List[float]:
        return [float(state.get('carbon_intensity', 0.4)),
                float(state.get('data_quality', 0.5)),
                float(state.get('anomaly_rate', 0.1)),
                float(state.get('coverage_score', 0.5))]

    def _action_to_index(self, action: str) -> int:
        return self.actions.index(action) if action in self.actions else 0

    async def record_feedback(self, state: Dict, action: str, reward: float):
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': float(reward)})
        self.update(state, action, reward)

    async def train_reward_model(self):
        if self.reward_model is None or len(self.feedback_buffer) < 10:
            return
        try:
            X = [f['state'] for f in self.feedback_buffer]
            y = [f['reward'] for f in self.feedback_buffer]
            await asyncio.to_thread(self.reward_model.fit, X, y)
            self.feedback_buffer.clear()
            logger.info("ActiveRLHF trained on %d samples", len(X))
        except Exception as e:
            logger.warning("ActiveRLHF train failed: %s", e)

    async def get_policy_probs(self, state: Dict) -> List[float]:
        # [P1] return the actual learned policy, not a frozen vector
        probs = self._policy(state)
        if NUMPY_AVAILABLE:
            return probs.tolist()
        return list(probs)

    def update(self, context, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += float(reward)
        self.history.append({'action': action, 'reward': float(reward),
                             'timestamp': datetime.now().isoformat()})
        # [P3] cap history to prevent unbounded growth
        if len(self.history) > 5000:
            self.history = self.history[-2500:]

    def _policy(self, context):
        if not NUMPY_AVAILABLE:
            return [1.0 / len(self.actions)] * len(self.actions)
        raw = np.array([self.preference_counts.get(a, 0.0) for a in self.actions],
                       dtype=float)
        if raw.sum() <= 0:
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
            return float(ent / np.log(len(self.actions))) if len(self.actions) > 1 else 0.0
        return 0.0

    async def maybe_query_human(self, context, options: List[str]) -> Optional[Dict]:
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

    # [P3] Persistence
    async def save(self, path: str) -> None:
        try:
            data = {'preference_counts': dict(self.preference_counts),
                    'history': self.history[-500:]}
            def _write():
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_text(json.dumps(data, default=str))
            await asyncio.to_thread(_write)
        except Exception as e:
            logger.warning("RLHF save failed: %s", e)

    async def load(self, path: str) -> None:
        try:
            p = Path(path)
            if not p.exists():
                return
            def _read():
                return json.loads(p.read_text())
            data = await asyncio.to_thread(_read)
            self.preference_counts = defaultdict(float, data.get('preference_counts', {}))
            self.history = data.get('history', [])
            logger.info("RLHF loaded %d preference entries", len(self.preference_counts))
        except Exception as e:
            logger.warning("RLHF load failed: %s", e)


# =============================================================================
# MODULE H — HUMAN-IN-THE-LOOP COORDINATOR  [P1] explicit reasons
# =============================================================================
class HumanInTheLoopCoordinator:
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: deque = deque(maxlen=500)  # [P2] bounded

    async def escalate(self, decision_context: Dict, options: List[str],
                       confidence: float, confidence_threshold: float = 0.65) -> Dict:
        low_confidence = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options) if self.rlhf else None
        high_uncertainty = query is not None

        if not low_confidence and not high_uncertainty:
            choice = self.rlhf.sample_action(decision_context) if self.rlhf else (
                options[0] if options else 'noop')
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence,
                                   'timestamp': datetime.now().isoformat()})
            SC_HITL.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending',
                     'reason': 'low_confidence' if low_confidence else 'high_uncertainty'}
        auto_choice = self.rlhf.sample_action(decision_context) if self.rlhf else (
            options[0] if options else 'noop')
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'reason': query.get('reason'),
                               'timestamp': datetime.now().isoformat()})
        SC_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': list(self.audit_log)[-10:]}


# =============================================================================
# MODULE I — FEDERATED AGGREGATOR  [P1] helpers to write back to mopd_weights
# =============================================================================
class FederatedAggregator:
    MOPD_KEYS = ('energy', 'carbon', 'helium', 'material')

    def __init__(self, num_params: int = 4):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({'client_id': client_id,
                                    'weights': list(weights),
                                    'samples': max(1, int(samples))})

    def aggregate(self) -> Dict:
        if not self.client_updates or not NUMPY_AVAILABLE:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        # [P1] normalize to a proper probability distribution
        s = float(agg.sum())
        if s > 0:
            agg = agg / s
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        SC_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def apply_to_mopd(self, mopd_weights: Dict[str, float]) -> Dict[str, float]:
        # [P1] actually propagate into the model
        for i, k in enumerate(self.MOPD_KEYS):
            mopd_weights[k] = float(self.global_weights[i])
        return mopd_weights

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# MODULE J — CAUSAL REWARD SHAPER (IPW / ATE)
# =============================================================================
class CausalRewardShaper:
    def __init__(self, num_actions: int):
        self.num_actions = num_actions
        self.interventions: deque = deque(maxlen=500)
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    def record(self, action: int, reward: float, propensities):
        if NUMPY_AVAILABLE:
            self.interventions.append((int(action), float(reward),
                                        np.array(propensities)))

    def compute_ate(self) -> Dict[int, float]:
        if len(self.interventions) < 10 or not NUMPY_AVAILABLE:
            return dict(self.ates)
        for a in range(self.num_actions):
            weights, outcomes = [], []
            for action, reward, props in self.interventions:
                if action == a:
                    weights.append(1.0 / (props[a] + 1e-6))
                    outcomes.append(reward)
            if weights:
                self.ates[a] = float(np.average(outcomes, weights=weights))
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)


# =============================================================================
# CONFIG — [P3] new fields for baseline, energy, persistence, split
# =============================================================================
if PYDANTIC_AVAILABLE:
    class SyntheticDataConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        seed: int = 42
        dataset_version: str = "6.1.0"
        db_path: str = "/tmp/synthetic_generator_v6.db"
        metrics_port: int = 8000
        websocket_port: int = 8770
        cache_ttl: int = 300
        task_types: Dict[str, float] = Field(default_factory=lambda: {
            'summarization': 0.25, 'classification': 0.20, 'translation': 0.15,
            'question_answering': 0.15, 'text_generation': 0.15, 'sentiment_analysis': 0.10})
        priority_profiles: List[str] = Field(default_factory=lambda: ['accuracy', 'green', 'balanced'])
        regions: List[str] = Field(default_factory=lambda: [
            'us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast'])
        region_carbon: Dict[str, float] = Field(default_factory=lambda: {
            'us-east': 420, 'us-west': 350, 'eu-west': 280,
            'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480})
        token_mean: float = 5.5
        token_std: float = 1.2
        default_anomaly_rate: float = 0.05  # [P1] non-zero by default
        default_rate_per_hour: float = 100.0
        default_duration_hours: int = 24
        use_real_distributions: bool = False
        export_format: str = "json"
        master_key_env: str = "SYNTH_MASTER_KEY"
        mopd_weights: Dict[str, float] = Field(default_factory=lambda: {
            'energy': 0.25, 'carbon': 0.25, 'helium': 0.25, 'material': 0.25})
        # v5 flags
        ga_enabled: bool = True
        ga_population_size: int = 20
        ga_generations: int = 5
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        moe_enabled: bool = True
        moe_expert_count: int = 4
        pareto_enabled: bool = True
        adaptive_anomaly_enabled: bool = True
        federated_enabled: bool = True
        drift_detection_enabled: bool = True
        user_preference_learning_enabled: bool = True
        limit_graph_enabled: bool = True
        rlhf_enabled: bool = True
        rlhf_training_interval: int = 600
        distillation_enabled: bool = True
        distillation_temperature: float = 2.0
        distillation_interval: int = 300
        # v6 flags
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        hitl_confidence_threshold: float = 0.65
        causal_rl_enabled: bool = True
        # ---- [P3] new ----
        grid_baseline_g_per_kwh: float = 400.0
        energy_kwh_per_sample: float = 0.001
        generation_concurrency: int = 16
        generation_batch_size: int = 512
        max_num_samples: int = 10_000_000
        rlhf_state_path: str = "/tmp/synth_rlhf_v6.json"
        state_persist_path: str = "/tmp/synth_state_v6.json"
        chaos_env_gate: bool = True

        @field_validator('dataset_version')
        @classmethod
        def _version_ok(cls, v):
            if not v:
                raise ValueError("dataset_version required")
            return v

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                return b'\x00' * 32
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                return b'\x00' * 32

        class Config:
            env_prefix = "SYNTH_"
else:
    @dataclass
    class SyntheticDataConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        seed: int = 42
        dataset_version: str = "6.1.0"
        db_path: str = "/tmp/synthetic_generator_v6.db"
        metrics_port: int = 8000
        websocket_port: int = 8770
        cache_ttl: int = 300
        task_types: Dict[str, float] = field(default_factory=lambda: {
            'summarization': 0.25, 'classification': 0.20, 'translation': 0.15,
            'question_answering': 0.15, 'text_generation': 0.15, 'sentiment_analysis': 0.10})
        priority_profiles: List[str] = field(default_factory=lambda: ['accuracy', 'green', 'balanced'])
        regions: List[str] = field(default_factory=lambda: [
            'us-east', 'us-west', 'eu-west', 'eu-north', 'asia-east', 'asia-southeast'])
        region_carbon: Dict[str, float] = field(default_factory=lambda: {
            'us-east': 420, 'us-west': 350, 'eu-west': 280,
            'eu-north': 220, 'asia-east': 500, 'asia-southeast': 480})
        token_mean: float = 5.5
        token_std: float = 1.2
        default_anomaly_rate: float = 0.05
        default_rate_per_hour: float = 100.0
        default_duration_hours: int = 24
        use_real_distributions: bool = False
        export_format: str = "json"
        master_key_env: str = "SYNTH_MASTER_KEY"
        mopd_weights: Dict[str, float] = field(default_factory=lambda: {
            'energy': 0.25, 'carbon': 0.25, 'helium': 0.25, 'material': 0.25})
        ga_enabled: bool = True
        ga_population_size: int = 20
        ga_generations: int = 5
        ga_mutation_rate: float = 0.2
        ga_crossover_rate: float = 0.7
        moe_enabled: bool = True
        moe_expert_count: int = 4
        pareto_enabled: bool = True
        adaptive_anomaly_enabled: bool = True
        federated_enabled: bool = True
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
        grid_baseline_g_per_kwh: float = 400.0
        energy_kwh_per_sample: float = 0.001
        generation_concurrency: int = 16
        generation_batch_size: int = 512
        max_num_samples: int = 10_000_000
        rlhf_state_path: str = "/tmp/synth_rlhf_v6.json"
        state_persist_path: str = "/tmp/synth_state_v6.json"
        chaos_env_gate: bool = True

        def get_master_key(self) -> bytes:
            key_hex = os.getenv(self.master_key_env)
            if not key_hex:
                return b'\x00' * 32
            try:
                return bytes.fromhex(key_hex)
            except ValueError:
                return b'\x00' * 32


# =============================================================================
# CIRCUIT BREAKER + RATE LIMITER  [P2] CircuitBreaker lock
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"
        self._lock = asyncio.Lock()  # [P2]

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self._state == "OPEN":
                if (self._last_failure_time is not None and
                        (datetime.now() - self._last_failure_time).total_seconds()
                        > self.recovery_timeout):
                    self._state = "HALF_OPEN"
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == "HALF_OPEN":
                    self._state = "CLOSED"
                    self._failures = 0
            return result
        except Exception as e:
            async with self._lock:
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
            self.tokens = min(self.rate, self.tokens +
                              (now - self.last_refill) * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.05)


# =============================================================================
# STORAGE — [P2] aiosqlite with asyncio.to_thread fallback
# =============================================================================
class EnhancedStorage:
    def __init__(self, config):
        self.config = config
        self.db_path = getattr(config, 'db_path', '/tmp/synth.db')
        self._cache: Dict[str, str] = {}
        self._generation_history: deque = deque(maxlen=500)
        self._pareto_front: List[Dict] = []
        self._conn = None
        self._aioconn = None
        self._conn_lock = asyncio.Lock()
        if not AIOSQLITE_AVAILABLE:
            self._init_sync()

    def _init_sync(self):
        try:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("""CREATE TABLE IF NOT EXISTS synthetic_state
                (key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
            self._conn.execute("""CREATE TABLE IF NOT EXISTS generation_history
                (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL)""")
            self._conn.commit()
        except Exception as e:
            logger.warning("Storage init failed: %s", e)

    async def _ensure_aioconn(self):
        if not AIOSQLITE_AVAILABLE:
            return
        if self._aioconn is not None:
            return
        async with self._conn_lock:
            if self._aioconn is not None:
                return
            try:
                self._aioconn = await aiosqlite.connect(self.db_path)
                await self._aioconn.execute("PRAGMA journal_mode=WAL")
                await self._aioconn.execute("""CREATE TABLE IF NOT EXISTS synthetic_state
                    (key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
                await self._aioconn.execute("""CREATE TABLE IF NOT EXISTS generation_history
                    (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL)""")
                await self._aioconn.commit()
            except Exception as e:
                logger.warning("aiosqlite init failed: %s", e)
                self._aioconn = None
                if self._conn is None:
                    self._init_sync()

    async def save_state(self, key: str, value: str):
        self._cache[key] = value
        try:
            await self._ensure_aioconn()
            if self._aioconn is not None:
                await self._aioconn.execute(
                    "INSERT OR REPLACE INTO synthetic_state (key, value) VALUES (?, ?)",
                    (key, value))
                await self._aioconn.commit()
            elif self._conn is not None:
                def _do():
                    self._conn.execute(
                        "INSERT OR REPLACE INTO synthetic_state (key, value) VALUES (?, ?)",
                        (key, value))
                    self._conn.commit()
                await asyncio.to_thread(_do)
        except Exception:
            pass

    async def get_state(self, key: str) -> Optional[str]:
        if key in self._cache:
            return self._cache[key]
        try:
            await self._ensure_aioconn()
            if self._aioconn is not None:
                async with self._aioconn.execute(
                        "SELECT value FROM synthetic_state WHERE key = ?", (key,)) as cur:
                    row = await cur.fetchone()
                if row:
                    self._cache[key] = row[0]
                    return row[0]
            elif self._conn is not None:
                row = await asyncio.to_thread(
                    lambda: self._conn.execute(
                        "SELECT value FROM synthetic_state WHERE key = ?", (key,)).fetchone())
                if row:
                    self._cache[key] = row[0]
                    return row[0]
        except Exception:
            pass
        return None

    async def save_carbon_intensity(self, region: str, intensity: float):
        self._cache[f"carbon_{region}"] = str(intensity)

    async def get_carbon_intensity(self, region: str) -> Optional[float]:
        v = self._cache.get(f"carbon_{region}")
        return float(v) if v else None

    async def save_helium_score(self, hotspot_id: str, score: float):
        self._cache[f"helium_{hotspot_id}"] = str(score)

    async def get_helium_score(self, hotspot_id: str) -> Optional[float]:
        v = self._cache.get(f"helium_{hotspot_id}")
        return float(v) if v else None

    async def save_generation_history(self, dataset_version, num_samples,
                                       anomaly_rate, edge_fraction,
                                       parameters, quantum_signature=None,
                                       blockchain_tx_hash=None):
        entry = {'dataset_version': dataset_version, 'num_samples': num_samples,
                 'anomaly_rate': anomaly_rate, 'edge_fraction': edge_fraction,
                 'parameters': parameters,
                 'quantum_signature': quantum_signature,
                 'blockchain_tx_hash': blockchain_tx_hash,
                 'timestamp': datetime.now().isoformat()}
        self._generation_history.append(entry)
        try:
            await self._ensure_aioconn()
            payload = json.dumps(entry, default=str)
            if self._aioconn is not None:
                await self._aioconn.execute(
                    "INSERT INTO generation_history (data) VALUES (?)", (payload,))
                await self._aioconn.commit()
            elif self._conn is not None:
                await asyncio.to_thread(
                    lambda: (self._conn.execute(
                        "INSERT INTO generation_history (data) VALUES (?)", (payload,)),
                             self._conn.commit()))
        except Exception:
            pass

    async def get_recent_generations(self, limit: int = 10) -> List[Dict]:
        return list(self._generation_history)[-limit:]

    async def save_pareto_front(self, solutions: List[Dict]):
        self._pareto_front = solutions

    async def get_current_pareto_front(self) -> List[Dict]:
        return self._pareto_front

    async def save_ga_population(self, generation: int, individuals: List[Dict]):
        # [P1] real persistence
        await self.save_state(f"ga_pop_{generation}", json.dumps(individuals, default=str))

    async def save_user_preference(self, user_id: str, weights: Dict,
                                    chosen_solution_id: Optional[str] = None):
        self._cache[f"pref_{user_id}"] = json.dumps(weights)

    async def get_user_preferences(self, user_id: str) -> Optional[Dict]:
        v = self._cache.get(f"pref_{user_id}")
        return json.loads(v) if v else None

    async def dispose(self):
        if self._aioconn is not None:
            try: await self._aioconn.close()
            except Exception: pass
            self._aioconn = None
        if self._conn:
            try: self._conn.close()
            except Exception: pass


# =============================================================================
# LimitGraphManager
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {'carbon': {'cost': 0.8}, 'energy': {'cost': 0.6},
                      'helium': {'cost': 0.4}, 'material': {'cost': 0.3},
                      'cost': {}, 'latency': {'cost': 0.2}}
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def update_constraint(self, name, value):
        async with self._lock:
            self.constraints[name] = value

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
                'constraints': self.constraints}


# =============================================================================
# GA — [P3] real fitness function (evaluates generated token distribution)
# =============================================================================
class GeneticParameterOptimizer:
    def __init__(self, config, storage, fitness_fn=None):
        self.config = config
        self.storage = storage
        self.population_size = getattr(config, 'ga_population_size', 20)
        self.generations = getattr(config, 'ga_generations', 5)
        self.mutation_rate = getattr(config, 'ga_mutation_rate', 0.2)
        self.crossover_rate = getattr(config, 'ga_crossover_rate', 0.7)
        self._fitness_fn = fitness_fn  # async callable(chrom) -> float
        self._lock = asyncio.Lock()

    def _random_chromosome(self):
        return {
            'token_mean': random.uniform(3.0, 8.0),
            'token_std': random.uniform(0.5, 2.5),
            'anomaly_rate': random.uniform(0.0, 0.3),
            'edge_fraction': random.uniform(0.0, 0.3),
        }

    def _mutate(self, chrom):
        new = chrom.copy()
        for k in new:
            if random.random() < self.mutation_rate:
                new[k] = max(1e-3, new[k] * (1 + random.gauss(0, 0.1)))
        return new

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for k in c1:
            if random.random() < 0.5:
                c1[k], c2[k] = p2[k], p1[k]
        return c1, c2

    async def _fitness(self, chrom):
        # [P3] real fitness if provided; otherwise deterministic proxy
        if self._fitness_fn is not None:
            try:
                return float(await self._fitness_fn(chrom))
            except Exception as e:
                logger.debug("GA fitness fn failed: %s", e)
        # proxy: prefer token_mean near 5.5, std near 1.2, small anomaly/edge
        return (1.0 - min(1.0, abs(chrom['token_mean'] - 5.5) / 5.5)
                + 1.0 - min(1.0, abs(chrom['token_std'] - 1.2) / 1.2)
                + 1.0 - chrom['anomaly_rate']
                + 1.0 - chrom['edge_fraction']) / 4.0

    async def optimize(self) -> Dict:
        async with self._lock:
            population = [self._random_chromosome() for _ in range(self.population_size)]
            for gen in range(self.generations):
                fitnesses = await asyncio.gather(
                    *[self._fitness(c) for c in population])
                sorted_pop = sorted(zip(population, fitnesses),
                                    key=lambda x: x[1], reverse=True)
                parents = [c for c, _ in sorted_pop[:max(2, self.population_size // 2)]]
                offspring = []
                while len(offspring) < self.population_size:
                    p1 = random.choice(parents)
                    p2 = random.choice(parents)
                    c1, c2 = self._crossover(p1, p2)
                    offspring.append(self._mutate(c1))
                    if len(offspring) < self.population_size:
                        offspring.append(self._mutate(c2))
                population = parents + offspring[:self.population_size - len(parents)]
            await self.storage.save_ga_population(self.generations, population)
            return population[0]


# =============================================================================
# MoE — [P1/P3] per-expert parameters + trained classifier
# =============================================================================
DEFAULT_EXPERTS: Dict[str, Dict[str, float]] = {
    'balanced': {
        'token_mean_offset': 0.0, 'token_std_scale': 1.0,
        'anomaly_rate_mult': 1.0, 'renewable_offset': 0.0,
        'edge_fraction_mult': 1.0,
    },
    'carbon_focused': {
        'token_mean_offset': -0.5, 'token_std_scale': 0.9,
        'anomaly_rate_mult': 0.5, 'renewable_offset': 0.15,
        'edge_fraction_mult': 1.2,
    },
    'helium_focused': {
        'token_mean_offset': 0.5, 'token_std_scale': 1.2,
        'anomaly_rate_mult': 0.7, 'renewable_offset': 0.05,
        'edge_fraction_mult': 1.5,
    },
    'anomaly_focused': {
        'token_mean_offset': 0.0, 'token_std_scale': 1.5,
        'anomaly_rate_mult': 2.5, 'renewable_offset': 0.0,
        'edge_fraction_mult': 1.0,
    },
}


class MoEGatingNetwork:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.experts = {k: dict(v) for k, v in DEFAULT_EXPERTS.items()}
        self.expert_names = list(self.experts.keys())
        self._training_data: List[Tuple[List[float], int]] = []
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._lock = asyncio.Lock()

    def _encode_context(self, context: Dict) -> List[float]:
        # 6-dim feature vector
        return [
            float(context.get('target_anomaly_rate', 0.0)),
            float(context.get('target_edge_fraction', 0.1)),
            min(1.0, float(context.get('num_samples', 100)) / 10000.0),
            float(context.get('avg_region_carbon_g', 400.0)) / 1000.0,
            float(context.get('forecast_carbon_g', 400.0)) / 1000.0,
            float(context.get('quality_target', 0.8)),
        ]

    async def select_expert(self, context: Dict) -> Tuple[str, Dict[str, float]]:
        selected = 'balanced'
        if (self._trained and self._gating_model is not None and NUMPY_AVAILABLE):
            try:
                feats = np.array(self._encode_context(context)).reshape(1, -1)
                if self._scaler:
                    feats = self._scaler.transform(feats)
                probs = self._gating_model.predict_proba(feats)[0]
                idx = int(np.argmax(probs))
                if idx < len(self.expert_names):
                    selected = self.expert_names[idx]
            except Exception:
                selected = 'balanced'
        return selected, dict(self.experts[selected])

    async def get_expert_probs(self, context: Dict) -> List[float]:
        # [P3] expose softmax over experts for the distillation teacher
        if NUMPY_AVAILABLE:
            if self._trained and self._gating_model is not None:
                try:
                    feats = np.array(self._encode_context(context)).reshape(1, -1)
                    if self._scaler:
                        feats = self._scaler.transform(feats)
                    probs = self._gating_model.predict_proba(feats)[0]
                    out = [0.0] * len(self.expert_names)
                    for i, cls in enumerate(self._gating_model.classes_):
                        out[int(cls)] = float(probs[i])
                    s = sum(out) or 1.0
                    return [v / s for v in out]
                except Exception:
                    pass
            return [1.0 / len(self.expert_names)] * len(self.expert_names)
        return [1.0 / len(self.expert_names)] * len(self.expert_names)

    async def add_training_sample(self, context: Dict, selected_expert: str,
                                   reward: float):
        if not NUMPY_AVAILABLE:
            return
        try:
            idx = self.expert_names.index(selected_expert)
        except ValueError:
            idx = 0
        feats = self._encode_context(context)
        async with self._lock:
            self._training_data.append((feats, idx, float(reward)))
            if len(self._training_data) > 2000:
                self._training_data = self._training_data[-1000:]
            if len(self._training_data) % 20 == 0:
                self._train_gating()

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 20 or not NUMPY_AVAILABLE:
            return
        try:
            # only use positive-reward samples for classification
            pos = [(f, i) for f, i, r in self._training_data if r >= 0.5]
            if len(pos) < 10:
                return
            X = np.array([f for f, _ in pos])
            y = np.array([i for _, i in pos])
            if len(set(y.tolist())) < 2:
                return
            self._scaler = StandardScaler()
            Xs = self._scaler.fit_transform(X)
            clf = MLPClassifier(hidden_layer_sizes=(16, 8), max_iter=200,
                                random_state=42)
            clf.fit(Xs, y)
            self._gating_model = clf
            self._trained = True
        except Exception as e:
            logger.debug("MoE train failed: %s", e)


# =============================================================================
# Pareto — [P3] real dominance filter
# =============================================================================
class ParetoFrontOptimizer:
    METRICS = ('coverage_score', 'realism_score', 'data_quality')

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front: List[Dict] = []
        self.max_size = 200

    def _dominates(self, a: Dict, b: Dict) -> bool:
        am, bm = a['metrics'], b['metrics']
        at_least_one_better = False
        for k in self.METRICS:
            av, bv = am.get(k, 0.0), bm.get(k, 0.0)
            if av < bv:
                return False
            if av > bv:
                at_least_one_better = True
        return at_least_one_better

    async def add_configuration(self, params, metrics) -> bool:
        entry = {'solution_id': f"cfg_{uuid.uuid4().hex[:8]}",
                 'config_params': params, 'metrics': dict(metrics),
                 'timestamp': datetime.now().isoformat()}
        for existing in self.pareto_front:
            if self._dominates(existing, entry):
                return False
        self.pareto_front = [e for e in self.pareto_front
                              if not self._dominates(entry, e)]
        self.pareto_front.append(entry)
        if len(self.pareto_front) > self.max_size:
            self.pareto_front.sort(key=lambda x: sum(
                x['metrics'].get(k, 0.0) for k in self.METRICS))
            self.pareto_front = self.pareto_front[-self.max_size:]
        await self.storage.save_pareto_front(self.pareto_front)
        return True

    def get_pareto_front(self):
        return self.pareto_front


# =============================================================================
# Adaptive Anomaly — [P1] UCB bandit, update actually called
# =============================================================================
class AdaptiveAnomalyInjector:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.anomaly_types = [
            'extreme_token_count', 'zero_accuracy', 'extreme_carbon',
            'helium_crisis', 'harvester_downtime', 'renewable_surge',
            'network_failure', 'regional_outage']
        self._counts = {a: 0 for a in self.anomaly_types}
        self._totals = {a: 0.0 for a in self.anomaly_types}
        self._total_pulls = 0
        self._lock = asyncio.Lock()

    async def choose_anomaly(self, context) -> str:
        async with self._lock:
            # epsilon-exploration
            if random.random() < 0.1:
                return random.choice(self.anomaly_types)
            # UCB1
            total = max(self._total_pulls, 1)
            best, best_score = None, -1.0
            for a in self.anomaly_types:
                c = self._counts[a]
                if c == 0:
                    return a  # force explore untried arms first
                avg = self._totals[a] / c
                score = avg + math.sqrt(2.0 * math.log(total) / c)
                if score > best_score:
                    best, best_score = a, score
            return best or 'extreme_token_count'

    async def update(self, anomaly_type: str, reward: float):
        async with self._lock:
            if anomaly_type not in self._counts:
                return
            self._counts[anomaly_type] += 1
            self._totals[anomaly_type] += float(reward)
            self._total_pulls += 1
            avg = self._totals[anomaly_type] / self._counts[anomaly_type]
            SYNTH_ANOMALY_WEIGHT.labels(type=anomaly_type).set(avg)

    def get_stats(self) -> Dict:
        return {'counts': dict(self._counts),
                'avgs': {a: (self._totals[a] / self._counts[a] if self._counts[a] else 0.0)
                         for a in self.anomaly_types},
                'total_pulls': self._total_pulls}


# =============================================================================
# Drift Detector  [P1] feeds per-region intensity
# =============================================================================
class DriftDetector:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.carbon_history: deque = deque(maxlen=200)
        self.threshold = 0.15

    async def check_carbon_drift(self, current_intensity: float) -> bool:
        self.carbon_history.append(float(current_intensity))
        if len(self.carbon_history) < 10:
            return False
        recent = list(self.carbon_history)[-10:]
        mean = sum(recent) / len(recent)
        if abs(mean) < 1e-9:
            return False
        drift = abs(current_intensity - mean) > self.threshold * abs(mean)
        if drift:
            SYNTH_DRIFT.labels(metric='carbon').inc()
        return drift


# =============================================================================
# User Preference Learner (legacy)
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, websocket):
        self.storage = storage
        self.websocket = websocket

    async def query_user_if_needed(self, user_id, top_configs):
        if len(top_configs) < 2:
            return None
        scores = [c.get('metrics', {}).get('data_quality', 0.0) for c in top_configs[:2]]
        if max(scores) <= 0:
            return None
        if abs(scores[0] - scores[1]) / max(scores) < 0.05:
            try:
                await self.websocket.broadcast({
                    'type': 'preference_query',
                    'user_id': user_id,
                    'options': [{'id': c.get('solution_id'),
                                 'quality': c.get('metrics', {}).get('data_quality', 0.0)}
                                for c in top_configs[:2]]}, topic='user_preferences')
            except Exception:
                pass
        return top_configs[0].get('solution_id')


# =============================================================================
# Distillation — [P1/P3] real KL update from MoE teacher
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None):
        self.config = config
        self.moe_engine = moe_engine
        self.temperature = float(getattr(config, 'distillation_temperature', 2.0))
        self.lr = 0.05
        if NUMPY_AVAILABLE:
            # [P3] student is a linear head over 6-dim context → 4 experts
            self.student_W = np.zeros((4, 6), dtype=float)
        else:
            self.student_W = None
        self.history: deque = deque(maxlen=500)
        self._lock = asyncio.Lock()

    def _encode(self, state: Dict) -> List[float]:
        return [
            float(state.get('target_anomaly_rate', 0.05)),
            float(state.get('target_edge_fraction', 0.1)),
            min(1.0, float(state.get('num_samples', 100)) / 10000.0),
            float(state.get('avg_region_carbon_g', 400.0)) / 1000.0,
            float(state.get('forecast_carbon_g', 400.0)) / 1000.0,
            float(state.get('quality_target', 0.8)),
        ]

    @staticmethod
    def _softmax(x):
        e = np.exp(x - x.max())
        return e / e.sum()

    async def distill(self, state: Dict):
        if not NUMPY_AVAILABLE or self.moe_engine is None:
            return
        try:
            teacher_probs = np.array(await self.moe_engine.get_expert_probs(state))
        except Exception:
            teacher_probs = np.ones(4) / 4.0
        if teacher_probs.shape != (4,):
            teacher_probs = np.ones(4) / 4.0

        ctx = np.array(self._encode(state), dtype=float)
        async with self._lock:
            logits = self.student_W @ ctx
            student_probs = self._softmax(logits)
            # KL(teacher || student) gradient w.r.t. logits: (student - teacher)
            grad_logits = (student_probs - teacher_probs)
            grad_W = np.outer(grad_logits, ctx)
            self.student_W = self.student_W - self.lr * grad_W
            loss = float(np.sum(teacher_probs * np.log((teacher_probs + 1e-9) /
                                                       (student_probs + 1e-9))))
            self.history.append({'loss': loss,
                                 'teacher_probs': teacher_probs.tolist(),
                                 'student_probs': student_probs.tolist(),
                                 'timestamp': datetime.now().isoformat()})

    def get_student_probs(self) -> List[float]:
        if not NUMPY_AVAILABLE or self.student_W is None:
            return [0.25, 0.25, 0.25, 0.25]
        # average of recent student predictions is more stable
        if self.history:
            latest = self.history[-1].get('student_probs')
            if latest:
                return list(latest)
        return [0.25, 0.25, 0.25, 0.25]


# =============================================================================
# Quantum + Blockchain stubs (renamed for honesty — [P1])
# =============================================================================
class SimulatedDataSigner:
    """SHA3-based signer. Rename downstream if PQC is later integrated."""

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self._keys: Dict[str, str] = {}

    async def generate_keypair(self, algorithm='dilithium'):
        kid = f"{algorithm}_{uuid.uuid4().hex[:8]}"
        self._keys[kid] = hashlib.sha256(os.urandom(32)).hexdigest()
        return {'key_id': kid, 'algorithm': algorithm,
                'public_key': self._keys[kid]}

    async def sign_dataset(self, data, key_id):
        if key_id not in self._keys:
            await self.generate_keypair()
            key_id = next(iter(self._keys))
        return {'signature': hashlib.sha3_256(
            json.dumps(data, sort_keys=True, default=str).encode()).hexdigest(),
            'algorithm': 'dilithium-sim', 'key_id': key_id}


class BlockchainDataVerification:
    def __init__(self, config):
        self.config = config
        self.connected = False

    async def record_dataset(self, data_id, data_hash):
        return f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"

    async def get_blockchain_status(self):
        return {'connected': self.connected}


# =============================================================================
# WebSocket Server
# =============================================================================
class EnhancedWebSocketServer:
    def __init__(self, port):
        self.port = port
        self.connections = set()
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
# Data classes
# =============================================================================
@dataclass
class WorkloadDescriptor:
    task_type: str = "summarization"
    tokens: int = 100
    latency_target: float = 500.0
    sector_emission_factor: float = 0.02
    bio_mode: str = "none"
    priority: str = "balanced"


@dataclass
class NodeDescriptor:
    id: str = "node"
    type: str = "edge"
    region: str = "us-east"
    region_carbon_intensity: float = 0.4
    energy_per_token: float = 5e-5
    helium_connectivity_score: float = 0.8
    material_footprint_id: str = "gpu-a100"
    uptime: float = 0.99
    renewable_fraction: float = 0.3


@dataclass
class SyntheticSustainabilityMetrics:
    energy_joules: float
    carbon_kg: float
    helium_units: float
    material_index: float


# =============================================================================
# GeneratorState — [P3] persistence
# =============================================================================
class GeneratorState:
    def __init__(self, storage, persist_path: Optional[str] = None):
        self.storage = storage
        self.persist_path = persist_path
        self.confidence = 0.5
        self.anomaly_rate = 0.0
        self.reflection_count = 0

    async def load(self):
        try:
            v = await self.storage.get_state('confidence')
            if v is not None:
                self.confidence = float(v)
            v = await self.storage.get_state('anomaly_rate')
            if v is not None:
                self.anomaly_rate = float(v)
        except Exception:
            pass
        if self.persist_path and Path(self.persist_path).exists():
            try:
                data = await asyncio.to_thread(
                    lambda: json.loads(Path(self.persist_path).read_text()))
                self.confidence = float(data.get('confidence', self.confidence))
                self.anomaly_rate = float(data.get('anomaly_rate', self.anomaly_rate))
            except Exception:
                pass

    async def save(self):
        try:
            await self.storage.save_state('confidence', str(self.confidence))
            await self.storage.save_state('anomaly_rate', str(self.anomaly_rate))
        except Exception:
            pass
        if self.persist_path:
            try:
                data = {'confidence': self.confidence,
                        'anomaly_rate': self.anomaly_rate}
                def _write():
                    Path(self.persist_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(self.persist_path).write_text(json.dumps(data, default=str))
                await asyncio.to_thread(_write)
            except Exception:
                pass


# =============================================================================
# Main Generator — v6.1.0
# =============================================================================
class SyntheticDataGenerator:
    """Enhanced synthetic data generator v6.1.0 (Phase 1-3)."""

    def __init__(self, config=None,
                 carbon_fetcher=None, helium_collector=None, material_updater=None):
        if config is None:
            self.config = SyntheticDataConfig()
        elif isinstance(config, dict):
            self.config = SyntheticDataConfig(**config) if PYDANTIC_AVAILABLE \
                else SyntheticDataConfig()
        else:
            self.config = config

        seed = getattr(self.config, 'seed', 42)
        random.seed(seed)
        if NUMPY_AVAILABLE:
            np.random.seed(seed)

        # Accessors
        self.task_types = getattr(self.config, 'task_types', {}) or \
            {'summarization': 1.0}
        self.priority_profiles = getattr(self.config, 'priority_profiles', ['balanced'])
        self.regions = getattr(self.config, 'regions', ['us-east'])
        self.region_carbon = dict(getattr(self.config, 'region_carbon', {'us-east': 400}))
        self.token_mean = getattr(self.config, 'token_mean', 5.5)
        self.token_std = getattr(self.config, 'token_std', 1.2)
        self.default_anomaly_rate = getattr(self.config, 'default_anomaly_rate', 0.05)
        self.default_rate_per_hour = getattr(self.config, 'default_rate_per_hour', 100.0)
        self.default_duration_hours = getattr(self.config, 'default_duration_hours', 24)
        self.use_real_distributions = getattr(self.config, 'use_real_distributions', False)
        self.dataset_version = getattr(self.config, 'dataset_version', '6.1.0')
        self.mopd_weights = dict(getattr(self.config, 'mopd_weights',
                                          {'energy': 0.25, 'carbon': 0.25,
                                           'helium': 0.25, 'material': 0.25}))
        self.edge_fraction = 0.1

        # External collectors
        self.carbon_fetcher = carbon_fetcher
        self.helium_collector = helium_collector
        self.material_updater = material_updater

        # Resilience — [P1] actually used in generate_dataset
        self._circuit_breaker = CircuitBreaker(name="data_generator",
                                               failure_threshold=5,
                                               recovery_timeout=30.0)
        self._rate_limiter = RateLimiter()

        # Storage + state
        self.storage = EnhancedStorage(self.config)
        self.state = GeneratorState(self.storage,
                                     persist_path=getattr(self.config,
                                                          'state_persist_path', None))

        # v5 modules
        self.ga_optimizer = GeneticParameterOptimizer(
            self.config, self.storage,
            fitness_fn=self._ga_fitness_fn) \
            if getattr(self.config, 'ga_enabled', True) else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) \
            if getattr(self.config, 'moe_enabled', True) else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) \
            if getattr(self.config, 'pareto_enabled', True) else None
        self.adaptive_anomaly = AdaptiveAnomalyInjector(self.config, self.storage) \
            if getattr(self.config, 'adaptive_anomaly_enabled', True) else None
        # [P1] removed parallel FederatedParameterAggregator
        self.drift_detector = DriftDetector(self.storage, self.config) \
            if getattr(self.config, 'drift_detection_enabled', True) else None

        # WebSocket BEFORE user_pref_learner
        self.websocket = EnhancedWebSocketServer(
            getattr(self.config, 'websocket_port', 8770))
        self.user_pref_learner = ActiveUserPreferenceLearner(
            self.storage, self.websocket) \
            if getattr(self.config, 'user_preference_learning_enabled', True) else None

        # v6 modules
        self.temporal_monitor = TemporalLogicMonitor() \
            if getattr(self.config, 'temporal_logic_enabled', True) else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("anomaly_cap",
                                              "G(anomaly_rate <= 0.5)")
            self.temporal_monitor.add_formula("quality_floor",
                                              "G(quality >= 0.0)")
            self.temporal_monitor.add_formula("convergence",
                                              "F(quality >= 0.7)")

        self.xai = XAIExplainer(['coverage', 'anomaly_diversity',
                                 'realism', 'quality']) \
            if getattr(self.config, 'xai_enabled', True) else None
        self.precision_controller = AdaptivePrecisionController() \
            if getattr(self.config, 'adaptive_precision_enabled', True) else None
        self.carbon_market = CarbonMarketClient() \
            if getattr(self.config, 'carbon_market_enabled', True) else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if getattr(self.config, 'role_specialization_enabled', True) else None

        # Active RLHF + HITL
        self.rlhf = ActiveRLHF(
            action_space=['balanced', 'carbon_focused',
                          'helium_focused', 'anomaly_focused'],
        ) if getattr(self.config, 'rlhf_enabled', True) else None
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (getattr(self.config, 'hitl_enabled', True) and self.rlhf) else None

        # Federated
        self.federated = FederatedAggregator(num_params=4)

        # Causal shaper
        self.causal_shaper = CausalRewardShaper(num_actions=4) \
            if getattr(self.config, 'causal_rl_enabled', True) else None

        # LIMIT graph
        self.limit_graph = LimitGraphManager(self.config) \
            if getattr(self.config, 'limit_graph_enabled', True) else None

        # Distillation — [P1] wired with real MoE teacher
        self.distillation = MultiTeacherPolicyDistillation(
            self.config, self.moe_gating) \
            if getattr(self.config, 'distillation_enabled', True) else None

        # Quantum + blockchain
        self.quantum_security = SimulatedDataSigner(self.config, self.storage)
        self.blockchain = BlockchainDataVerification(self.config)

        # Chaos
        self.chaos_tester = ChaosTester(self) \
            if getattr(self.config, 'chaos_testing_enabled', True) else None

        # Background
        self._background_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._running = False
        self._carbon_saved_kg_total = 0.0
        self._carbon_total_lock = asyncio.Lock()
        # [P2] bounded metadata deque + lock
        self._metadata_history: deque = deque(maxlen=50)
        self._metadata_lock = asyncio.Lock()
        self._batch_semaphore = asyncio.Semaphore(
            int(getattr(self.config, 'generation_concurrency', 16)))

        if PROMETHEUS_AVAILABLE:
            try:
                from prometheus_client import start_http_server
                start_http_server(getattr(self.config, 'metrics_port', 8000),
                                  registry=REGISTRY)
            except Exception:
                pass

        logger.info("SyntheticDataGenerator v%s initialized (instance %s)",
                    self.dataset_version,
                    getattr(self.config, 'instance_id', 'unknown'))

    # ------------------------------------------------------------------
    # GA fitness — [P3] real: generates a small sample and scores it
    # ------------------------------------------------------------------
    async def _ga_fitness_fn(self, chrom: Dict) -> float:
        try:
            n = 32
            anomaly_rate = float(chrom.get('anomaly_rate', 0.05))
            edge_fraction = float(chrom.get('edge_fraction', 0.1))
            token_mean = float(chrom.get('token_mean', 5.5))
            token_std = float(chrom.get('token_std', 1.2))
            tokens = []
            for _ in range(n):
                if NUMPY_AVAILABLE:
                    t = int(np.exp(np.random.normal(token_mean, token_std)))
                else:
                    t = int(random.lognormvariate(token_mean, token_std))
                tokens.append(max(1, t))
            # realism = closeness of empirical log-mean to target
            if NUMPY_AVAILABLE and len(tokens) > 1:
                emp_mean = float(np.mean(np.log(np.array(tokens) + 1e-9)))
                emp_std = float(np.std(np.log(np.array(tokens) + 1e-9)))
                realism = 1.0 - min(1.0, abs(emp_mean - self.token_mean) /
                                    max(self.token_mean, 1e-6))
                realism *= 1.0 - min(1.0, abs(emp_std - self.token_std) /
                                     max(self.token_std, 1e-6))
            else:
                realism = 0.5
            penalty = anomaly_rate * 0.5 + edge_fraction * 0.2
            return max(0.0, realism - penalty)
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.state.load()
        if self.rlhf:
            await self.rlhf.load(getattr(self.config, 'rlhf_state_path',
                                          '/tmp/synth_rlhf_v6.json'))
        await self.websocket.start()

        try:
            loop = asyncio.get_event_loop()
            tasks = [
                self._ga_optimization_loop(),
                self._federated_loop(),
                self._drift_detection_loop(),
                self._health_check_loop(),
                self._cleanup_loop(),
                self._limit_graph_loop(),
                self._rlhf_loop(),
                self._distillation_loop(),
            ]
            if self.chaos_tester and not getattr(self.config, 'chaos_env_gate', True):
                tasks.append(self._chaos_loop())
            for t in tasks:
                self._background_tasks.append(loop.create_task(t))
        except RuntimeError:
            pass
        logger.info("SyntheticDataGenerator started")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                if self.ga_optimizer:
                    best = await self.ga_optimizer.optimize()
                    self.token_mean = best.get('token_mean', self.token_mean)
                    self.token_std = best.get('token_std', self.token_std)
                    self.default_anomaly_rate = best.get(
                        'anomaly_rate', self.default_anomaly_rate)
                    self.edge_fraction = best.get('edge_fraction', 0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("GA loop error: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)
                if self.federated.client_updates:
                    self.federated.aggregate()
                    # [P1] actually apply back to mopd_weights
                    self.federated.apply_to_mopd(self.mopd_weights)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Federated loop error: %s", e)

    async def _drift_detection_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.drift_detector and self.region_carbon:
                    # [P1] real per-region noise, not a constant
                    for region in list(self.region_carbon.keys()):
                        base = self.region_carbon.get(region, 400.0)
                        current = (base + random.uniform(-40, 40)) / 1000.0
                        drift = await self.drift_detector.check_carbon_drift(current)
                        if drift:
                            logger.info("Drift detected in %s (%.3f kg/kWh)",
                                        region, current)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Drift loop error: %s", e)

    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                gc.collect()
            except asyncio.CancelledError:
                break

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
                if self.limit_graph:
                    await self.limit_graph.update_constraint(
                        'carbon',
                        sum(self.region_carbon.values()) /
                        max(len(self.region_carbon), 1) / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Limit graph loop error: %s", e)

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(getattr(self.config, 'rlhf_training_interval', 600))
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                    await self.rlhf.save(getattr(self.config, 'rlhf_state_path',
                                                  '/tmp/synth_rlhf_v6.json'))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("RLHF loop error: %s", e)

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(getattr(self.config, 'distillation_interval', 300))
                if self.distillation:
                    avg_ci = (sum(self.region_carbon.values()) /
                              max(len(self.region_carbon), 1))
                    await self.distillation.distill({
                        'target_anomaly_rate': self.default_anomaly_rate,
                        'target_edge_fraction': self.edge_fraction,
                        'num_samples': 100,
                        'avg_region_carbon_g': avg_ci,
                        'forecast_carbon_g': avg_ci,
                        'quality_target': 0.8,
                    })
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Distillation loop error: %s", e)

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                if self.chaos_tester:
                    await self.chaos_tester.run_test(
                        random.choice(ChaosTester.FAULT_TYPES),
                        duration_s=0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ------------------------------------------------------------------
    # Core generation primitives
    # ------------------------------------------------------------------
    def _random_task_type(self) -> str:
        return random.choice(list(self.task_types.keys()))

    def _random_token_count(self, mean: float, std: float) -> int:
        if NUMPY_AVAILABLE:
            return max(1, int(np.exp(np.random.normal(mean, std))))
        return max(1, int(random.lognormvariate(mean, std)))

    def _random_priority(self) -> str:
        return random.choice(self.priority_profiles)

    def _random_carbon_kg(self, region: str) -> float:
        base = self.region_carbon.get(region, 400.0)
        return max(0.0, (base + random.uniform(-30, 30))) / 1000.0

    def _random_renewable(self, region: str) -> float:
        base = {'us-east': 0.3, 'us-west': 0.45, 'eu-west': 0.5,
                'eu-north': 0.6, 'asia-east': 0.2, 'asia-southeast': 0.25}
        return max(0.0, min(1.0, base.get(region, 0.3) + random.uniform(-0.05, 0.05)))

    async def generate_workload_descriptor(self, **kwargs) -> WorkloadDescriptor:
        return WorkloadDescriptor(
            task_type=kwargs.get('task_type') or self._random_task_type(),
            tokens=kwargs.get('tokens') or self._random_token_count(
                self.token_mean, self.token_std),
            latency_target=kwargs.get('latency_target') or random.uniform(100, 2000),
            sector_emission_factor=kwargs.get('sector_emission_factor')
            or random.uniform(0.01, 0.05),
            bio_mode=kwargs.get('bio_mode') or random.choice(
                ["photosynthetic", "chemotactic", "none"]),
            priority=kwargs.get('priority') or self._random_priority())

    async def generate_node_descriptor(self, **kwargs) -> NodeDescriptor:
        region = kwargs.get('region') or random.choice(self.regions)
        return NodeDescriptor(
            id=kwargs.get('node_id') or f"synth_node_{uuid.uuid4().hex[:8]}",
            type=kwargs.get('type') or random.choice(
                ["edge", "hotspot", "cloud", "lab"]),
            region=region,
            region_carbon_intensity=kwargs.get('region_carbon_intensity')
            or self._random_carbon_kg(region),
            energy_per_token=kwargs.get('energy_per_token')
            or random.uniform(1e-5, 1e-4),
            helium_connectivity_score=kwargs.get('helium_connectivity_score')
            or random.uniform(0.5, 1.0),
            material_footprint_id=kwargs.get('material_footprint_id')
            or random.choice(["gpu-a100", "gpu-h100", "edge-device"]),
            uptime=kwargs.get('uptime') or random.uniform(0.9, 1.0),
            renewable_fraction=kwargs.get('renewable_fraction')
            or self._random_renewable(region))

    async def compute_sustainability_metrics(self, workload, node):
        energy_joules = node.energy_per_token * workload.tokens
        carbon_kg = energy_joules / 3.6e6 * node.region_carbon_intensity
        helium_units = (1 - node.helium_connectivity_score) * 0.5
        material_index = 0.5 if node.material_footprint_id in (
            "gpu-h100", "gpu-a100") else 0.2
        return SyntheticSustainabilityMetrics(
            energy_joules=energy_joules, carbon_kg=carbon_kg,
            helium_units=helium_units, material_index=material_index)

    async def inject_anomaly(self, workload, node,
                              anomaly_type=None, context=None):
        if anomaly_type is None:
            if self.adaptive_anomaly:
                anomaly_type = await self.adaptive_anomaly.choose_anomaly(
                    context or {})
            else:
                anomaly_type = 'extreme_token_count'

        if anomaly_type == 'extreme_token_count':
            workload.tokens = int(random.expovariate(1 / 10000)) + 5000
        elif anomaly_type in ('zero_accuracy', 'zero_latency'):
            workload.latency_target = 0.0
        elif anomaly_type == 'extreme_carbon':
            node.region_carbon_intensity = 0.8
        elif anomaly_type == 'helium_crisis':
            node.helium_connectivity_score = 0.1
        elif anomaly_type == 'harvester_downtime':
            node.renewable_fraction = 0.0
            node.uptime = 0.5
        elif anomaly_type == 'renewable_surge':
            node.renewable_fraction = 0.95
        elif anomaly_type == 'network_failure':
            node.helium_connectivity_score = 0.0
            node.uptime = 0.0
        elif anomaly_type == 'regional_outage':
            node.uptime = 0.3
        SYNTHETIC_ANOMALIES.labels(anomaly_type=anomaly_type).inc()
        return workload, node, anomaly_type

    # ------------------------------------------------------------------
    # Batch generation — [P3]
    # ------------------------------------------------------------------
    async def _generate_one(self, anomaly_rate: float, edge_types: List[str],
                             expert_params: Dict[str, float],
                             is_edge: bool = False) -> Dict:
        # apply expert token-mean offset and std scale
        mean = self.token_mean + expert_params.get('token_mean_offset', 0.0)
        std = self.token_std * expert_params.get('token_std_scale', 1.0)
        std = max(1e-3, std)
        tokens = self._random_token_count(mean, std)

        region = random.choice(self.regions)
        node = await self.generate_node_descriptor(region=region)
        # apply expert renewable bias
        node.renewable_fraction = max(0.0, min(1.0,
            node.renewable_fraction + expert_params.get('renewable_offset', 0.0)))

        workload = await self.generate_workload_descriptor(tokens=tokens)
        anomaly = None

        eff_anomaly_rate = min(1.0, max(0.0,
            anomaly_rate * expert_params.get('anomaly_rate_mult', 1.0)))
        if is_edge or random.random() < eff_anomaly_rate:
            atype = random.choice(edge_types) if is_edge else None
            workload, node, anomaly = await self.inject_anomaly(
                workload, node, atype,
                context={'anomaly_rate': eff_anomaly_rate})
            if self.adaptive_anomaly and not is_edge and anomaly:
                # reward of using this anomaly = data quality of the sample
                # (per-sample approximation, will be refined by caller)
                pass

        metrics = await self.compute_sustainability_metrics(workload, node)
        # guards
        metrics.carbon_kg = max(0.0, metrics.carbon_kg)
        metrics.energy_joules = max(0.0, metrics.energy_joules)
        return {'workload': workload, 'node': node,
                'metrics': metrics, 'anomaly': anomaly}

    async def _generate_batch(self, n: int, anomaly_rate: float,
                               edge_fraction: float, edge_types: List[str],
                               expert_params: Dict[str, float]) -> List[Dict]:
        async with self._batch_semaphore:
            num_edge = int(n * edge_fraction)
            num_normal = n - num_edge
            out = []
            for _ in range(num_normal):
                out.append(await self._generate_one(
                    anomaly_rate, edge_types, expert_params, is_edge=False))
            for _ in range(num_edge):
                out.append(await self._generate_one(
                    anomaly_rate, edge_types, expert_params, is_edge=True))
            return out

    # ------------------------------------------------------------------
    # Quality metrics — [P1] real, computed from data
    # ------------------------------------------------------------------
    def _compute_quality_metrics(self, dataset: List[Dict],
                                  edge_types: List[str]) -> Dict[str, float]:
        if not dataset:
            return {'coverage_score': 0.0, 'anomaly_diversity': 0.0,
                    'realism_score': 0.0, 'data_quality': 0.0}

        # data_quality: fraction of samples passing sanity checks
        n_valid = 0
        for s in dataset:
            w, n, m = s['workload'], s['node'], s['metrics']
            ok = True
            if not (1 <= w.tokens <= 10_000_000):
                ok = False
            if not (-1.0 <= n.region_carbon_intensity <= 5.0):
                ok = False
            if not (0.0 <= m.energy_joules <= 1e9):
                ok = False
            if not math.isfinite(m.carbon_kg) or m.carbon_kg < 0:
                ok = False
            if ok:
                n_valid += 1
        data_quality = n_valid / max(len(dataset), 1)

        # realism: empirical log-token distribution vs. target
        tokens = [max(1, s['workload'].tokens) for s in dataset]
        realism = 0.5
        if NUMPY_AVAILABLE and len(tokens) > 1:
            log_tokens = np.log(np.array(tokens, dtype=float) + 1e-9)
            emp_mean = float(log_tokens.mean())
            emp_std = float(log_tokens.std())
            m_term = 1.0 - min(1.0, abs(emp_mean - self.token_mean) /
                               max(self.token_mean, 1e-6))
            s_term = 1.0 - min(1.0, abs(emp_std - self.token_std) /
                               max(self.token_std, 1e-6))
            realism = max(0.0, min(1.0, (m_term + s_term) / 2.0))

        # coverage: fraction of (task_type × region × node_type) observed
        observed = set()
        for s in dataset:
            observed.add((s['workload'].task_type,
                          s['node'].region,
                          s['node'].type))
        possible = max(1, len(self.task_types) * len(self.regions) * 4)
        coverage = min(1.0, len(observed) / possible)

        # anomaly diversity: distinct anomaly types / total types
        anomalies = set(s['anomaly'] for s in dataset if s['anomaly'])
        diversity = min(1.0, len(anomalies) / max(1, len(edge_types)))

        return {'coverage_score': round(coverage, 4),
                'anomaly_diversity': round(diversity, 4),
                'realism_score': round(realism, 4),
                'data_quality': round(data_quality, 4)}

    # ------------------------------------------------------------------
    # Dataset hash — [P3]
    # ------------------------------------------------------------------
    def _compute_dataset_hash(self, dataset: List[Dict]) -> str:
        h = hashlib.sha256()
        for s in dataset:
            payload = json.dumps({
                'w': asdict(s['workload']),
                'n': asdict(s['node']),
                'm': asdict(s['metrics']),
                'a': s['anomaly'],
            }, sort_keys=True, default=str)
            h.update(payload.encode())
        return h.hexdigest()

    # ------------------------------------------------------------------
    # Main generate_dataset — [P3] batched parallel with quality metrics
    # ------------------------------------------------------------------
    def _validate_params(self, num_samples: int, edge_case_fraction: float,
                          anomaly_rate: float):
        # [P1] input validation
        max_n = int(getattr(self.config, 'max_num_samples', 10_000_000))
        if not isinstance(num_samples, int) or num_samples <= 0:
            raise ValueError(f"num_samples must be a positive int, got {num_samples}")
        if num_samples > max_n:
            raise ValueError(f"num_samples={num_samples} exceeds max={max_n}")
        if not (0.0 <= edge_case_fraction <= 1.0):
            raise ValueError(f"edge_case_fraction must be in [0,1], "
                             f"got {edge_case_fraction}")
        if not (0.0 <= anomaly_rate <= 1.0):
            raise ValueError(f"anomaly_rate must be in [0,1], got {anomaly_rate}")

    async def generate_dataset(self, num_samples: int = 1000,
                                include_edge_cases: bool = True,
                                edge_case_fraction: float = 0.1,
                                anomaly_rate: Optional[float] = None,
                                seed: Optional[int] = None) -> List[Dict]:
        cid = str(uuid.uuid4())[:12]
        token = correlation_id_var.set(cid)
        SYNTH_ACTIVE.inc()
        start = time.time()
        try:
            if anomaly_rate is None:
                anomaly_rate = self.default_anomaly_rate
            self._validate_params(num_samples, edge_case_fraction, anomaly_rate)

            if seed is not None:
                random.seed(seed)
                if NUMPY_AVAILABLE:
                    np.random.seed(seed)

            await self._rate_limiter.wait_and_acquire()

            avg_ci_g = (sum(self.region_carbon.values()) /
                        max(len(self.region_carbon), 1))

            # [P1] precision uses gCO2/kWh
            precision = PrecisionLevel.FP32
            if self.precision_controller:
                precision = self.precision_controller.select(avg_ci_g, 0.95)

            # temporal gate
            temporal_status = {}
            if self.temporal_monitor:
                self.temporal_monitor.update({'anomaly_rate': float(anomaly_rate),
                                               'quality': 0.8})
                temporal_status = self.temporal_monitor.evaluate()

            # strategy: RLHF > Distillation > MoE
            context = {
                'target_anomaly_rate': float(anomaly_rate),
                'target_edge_fraction': float(edge_case_fraction),
                'num_samples': int(num_samples),
                'avg_region_carbon_g': float(avg_ci_g),
                'forecast_carbon_g': float(avg_ci_g),
                'quality_target': 0.8,
            }
            strategy = 'balanced'
            expert_params = dict(DEFAULT_EXPERTS['balanced'])
            if self.rlhf and self.rlhf.history:
                probs = await self.rlhf.get_policy_probs({})
                names = self.rlhf.actions
                idx = int(np.argmax(probs)) if NUMPY_AVAILABLE else 0
                strategy = names[idx % len(names)]
                if self.moe_gating:
                    _, expert_params = await self.moe_gating.select_expert(context)
            elif self.distillation:
                # use distillation student as the strategy prior
                probs = self.distillation.get_student_probs()
                names = ['balanced', 'carbon_focused',
                         'helium_focused', 'anomaly_focused']
                idx = int(np.argmax(probs)) if NUMPY_AVAILABLE else 0
                strategy = names[idx % len(names)]
                if self.moe_gating:
                    # get actual expert params from MoE for the chosen strategy
                    _, expert_params = await self.moe_gating.select_expert(context)
                    if self.moe_gating and strategy in self.moe_gating.experts:
                        expert_params = dict(self.moe_gating.experts[strategy])
            elif self.moe_gating:
                strategy, expert_params = await self.moe_gating.select_expert(context)

            # edge types
            edge_types = (list(self.adaptive_anomaly.anomaly_types)
                          if self.adaptive_anomaly
                          else ['extreme_token_count', 'extreme_carbon'])

            eff_edge = edge_case_fraction if include_edge_cases else 0.0

            # [P3] batched parallel generation
            batch_size = int(getattr(self.config, 'generation_batch_size', 512))
            batch_size = max(1, min(batch_size, num_samples))
            tasks = []
            for i in range(0, num_samples, batch_size):
                size = min(batch_size, num_samples - i)
                tasks.append(self._generate_batch(
                    size, anomaly_rate, eff_edge, edge_types, expert_params))
            batch_results = await asyncio.gather(*tasks)
            dataset = [s for batch in batch_results for s in batch]

            # [P1] real quality metrics
            qm = self._compute_quality_metrics(dataset, edge_types)
            coverage_score = qm['coverage_score']
            anomaly_diversity = qm['anomaly_diversity']
            realism_score = qm['realism_score']
            data_quality = qm['data_quality']

            # [P1] adaptive anomaly update — reward = per-type empirical quality
            if self.adaptive_anomaly:
                by_type: Dict[str, List[float]] = defaultdict(list)
                for s in dataset:
                    if s['anomaly']:
                        by_type[s['anomaly']].append(1.0 if math.isfinite(
                            s['metrics'].carbon_kg) else 0.0)
                for atype, rewards in by_type.items():
                    if rewards:
                        await self.adaptive_anomaly.update(
                            atype, sum(rewards) / len(rewards))

            # XAI
            xai_out = None
            if self.xai:
                cand = {'coverage': coverage_score,
                        'anomaly_diversity': anomaly_diversity,
                        'realism': realism_score, 'quality': data_quality}
                xai_out = self.xai.explain(
                    cand, self.mopd_weights or
                    {'coverage': 0.25, 'anomaly_diversity': 0.25,
                     'realism': 0.25, 'quality': 0.25}, [cand])

            # Role assignments
            role_assignments = None
            if self.role_coordinator:
                role_assignments = self.role_coordinator.assign_roles({
                    'trust': 0.7, 'compute': 0.7, 'energy': 0.7,
                    'performance': data_quality})

            # HITL
            hitl_outcome = None
            hitl_thr = getattr(self.config, 'hitl_confidence_threshold', 0.65)
            if self.hitl and data_quality < hitl_thr:
                try:
                    hitl_outcome = await self.hitl.escalate(
                        decision_context={'strategy': strategy,
                                           'quality': data_quality},
                        options=['accept', 'retry', 'adjust_params'],
                        confidence=data_quality,
                        confidence_threshold=hitl_thr)
                except Exception:
                    pass

            # [P1] baseline-aware carbon credits
            credit_value = 0.0
            rec_value = 0.0
            if self.carbon_market:
                try:
                    baseline_g = getattr(self.config, 'grid_baseline_g_per_kwh', 400.0)
                    energy_kwh = (getattr(self.config, 'energy_kwh_per_sample', 0.001)
                                  * num_samples)
                    saved_kg = max(0.0, (baseline_g - avg_ci_g) * energy_kwh / 1000.0)
                    if saved_kg > 0:
                        credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                        rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                        async with self._carbon_total_lock:
                            self._carbon_saved_kg_total += saved_kb if False else saved_kg
                except Exception as e:
                    logger.debug("carbon credit failed: %s", e)

            # Federated submission + apply
            federated_round = None
            if self.federated:
                try:
                    self.federated.submit_update(
                        getattr(self.config, 'instance_id', 'inst'),
                        [coverage_score, anomaly_diversity,
                         realism_score, data_quality], samples=num_samples)
                    if self.federated.client_updates:
                        agg = self.federated.aggregate()
                        federated_round = agg['round']
                        self.federated.apply_to_mopd(self.mopd_weights)
                except Exception:
                    pass

            # Chaos — [P1] gated to SYNTH_ENV=chaos
            chaos_passed = None
            inline_chaos_ok = (not getattr(self.config, 'chaos_env_gate', True)
                               or os.getenv('SYNTH_ENV', 'production') == 'chaos')
            if self.chaos_tester and inline_chaos_ok and (num_samples % 5 == 0):
                try:
                    cr = await self.chaos_tester.run_test(
                        random.choice(ChaosTester.FAULT_TYPES),
                        duration_s=0.02)
                    chaos_passed = cr['passed']
                except Exception:
                    pass

            # Causal observation
            if self.causal_shaper:
                try:
                    action_idx = ['balanced', 'carbon_focused',
                                  'helium_focused',
                                  'anomaly_focused'].index(strategy)
                except ValueError:
                    action_idx = 0
                self.causal_shaper.record(
                    action_idx, data_quality,
                    list((await self.rlhf.get_policy_probs({}))
                         if self.rlhf else [0.25] * 4))

            # Pareto
            params = {'num_samples': num_samples,
                      'edge_fraction': eff_edge,
                      'anomaly_rate': anomaly_rate,
                      'strategy': strategy}
            metrics_dict = {'coverage_score': coverage_score,
                            'anomaly_diversity': anomaly_diversity,
                            'realism_score': realism_score,
                            'data_quality': data_quality}
            if self.pareto_optimizer:
                try:
                    await self.pareto_optimizer.add_configuration(params, metrics_dict)
                except Exception:
                    pass

            # LIMIT graph constraints
            if self.limit_graph:
                await self.limit_graph.update_constraint('coverage', coverage_score)
                await self.limit_graph.update_constraint('quality', data_quality)

            # RLHF feedback — [P1] real reward
            if self.rlhf:
                try:
                    await self.rlhf.record_feedback(
                        state={'data_quality': data_quality,
                               'coverage_score': coverage_score,
                               'anomaly_rate': anomaly_rate},
                        action=strategy, reward=data_quality)
                except Exception:
                    pass

            # MoE training sample — [P1] real learning
            if self.moe_gating:
                try:
                    await self.moe_gating.add_training_sample(
                        context, strategy, data_quality)
                except Exception:
                    pass

            # Distillation update — [P1] real student update
            if self.distillation:
                try:
                    await self.distillation.distill(context)
                except Exception:
                    pass

            # Dataset hash + sign + blockchain — [P3]
            dataset_hash = self._compute_dataset_hash(dataset)
            quantum_signature = None
            blockchain_tx_hash = None
            try:
                kp = await self.quantum_security.generate_keypair()
                quantum_signature = await self.quantum_security.sign_dataset(
                    {'hash': dataset_hash, 'version': self.dataset_version,
                     'num_samples': num_samples},
                    kp['key_id'])
                blockchain_tx_hash = await self.blockchain.record_dataset(
                    f"dataset_{uuid.uuid4().hex[:8]}", dataset_hash)
            except Exception:
                pass

            # Persist history — [P1] now includes signature + hash
            try:
                await self.storage.save_generation_history(
                    self.dataset_version, num_samples, anomaly_rate,
                    eff_edge, params,
                    quantum_signature=quantum_signature,
                    blockchain_tx_hash=blockchain_tx_hash)
            except Exception:
                pass

            # WebSocket broadcast
            try:
                await self.websocket.broadcast({
                    'type': 'dataset_generated',
                    'version': self.dataset_version,
                    'samples': len(dataset),
                    'anomaly_rate': anomaly_rate,
                    'strategy': strategy,
                    'data_quality': data_quality,
                    'dataset_hash': dataset_hash,
                    'timestamp': datetime.now().isoformat()}, topic='generation')
            except Exception:
                pass

            SYNTHETIC_SAMPLES.labels(type='dataset').inc(num_samples)
            SYNTH_QUALITY.labels(sector='default').set(data_quality)

            # Metadata snapshot — [P2] bounded deque
            snapshot = {
                'correlation_id': cid,
                'dataset_hash': dataset_hash,
                'quantum_signature': quantum_signature,
                'blockchain_tx_hash': blockchain_tx_hash,
                'precision_used': precision.value,
                'temporal_status': temporal_status,
                'xai_explanation': xai_out,
                'role_assignments': role_assignments,
                'hitl_outcome': hitl_outcome,
                'carbon_credit_value_usd': credit_value,
                'rec_value_usd': rec_value,
                'federated_round': federated_round,
                'chaos_test_passed': chaos_passed,
                'causal_ates': (self.causal_shaper.compute_ate()
                                if self.causal_shaper else {}),
                'strategy': strategy,
                'quality_metrics': qm,
            }
            async with self._metadata_lock:
                self._metadata_history.append(snapshot)

            SYNTH_LATENCY.observe(time.time() - start)
            logger.info("Generated dataset v%s: %d samples, strategy=%s, "
                        "precision=%s, quality=%.3f",
                        self.dataset_version, len(dataset), strategy,
                        precision.value, data_quality)
            return dataset
        finally:
            SYNTH_ACTIVE.dec()
            correlation_id_var.reset(token)

    # ------------------------------------------------------------------
    # Streaming API — [P3]
    # ------------------------------------------------------------------
    async def stream_dataset(self, num_samples: int = 1000,
                              batch_size: int = 1024,
                              include_edge_cases: bool = True,
                              edge_case_fraction: float = 0.1,
                              anomaly_rate: Optional[float] = None,
                              seed: Optional[int] = None) -> AsyncIterator[List[Dict]]:
        """Stream dataset in batches. Yields `List[Dict]` batches — the caller
        can consume without buffering the full dataset in memory."""
        if anomaly_rate is None:
            anomaly_rate = self.default_anomaly_rate
        self._validate_params(num_samples, edge_case_fraction, anomaly_rate)
        if seed is not None:
            random.seed(seed)
            if NUMPY_AVAILABLE:
                np.random.seed(seed)

        avg_ci_g = (sum(self.region_carbon.values()) /
                    max(len(self.region_carbon), 1))
        context = {
            'target_anomaly_rate': float(anomaly_rate),
            'target_edge_fraction': float(edge_case_fraction),
            'num_samples': int(num_samples),
            'avg_region_carbon_g': float(avg_ci_g),
            'forecast_carbon_g': float(avg_ci_g),
            'quality_target': 0.8,
        }
        strategy = 'balanced'
        expert_params = dict(DEFAULT_EXPERTS['balanced'])
        if self.moe_gating:
            strategy, expert_params = await self.moe_gating.select_expert(context)

        edge_types = (list(self.adaptive_anomaly.anomaly_types)
                      if self.adaptive_anomaly
                      else ['extreme_token_count', 'extreme_carbon'])
        eff_edge = edge_case_fraction if include_edge_cases else 0.0
        batch_size = max(1, min(batch_size, num_samples))

        for i in range(0, num_samples, batch_size):
            size = min(batch_size, num_samples - i)
            batch = await self._generate_batch(
                size, anomaly_rate, eff_edge, edge_types, expert_params)
            yield batch

    # ------------------------------------------------------------------
    # Train/val/test split — [P3]
    # ------------------------------------------------------------------
    async def generate_splits(self, num_samples: int = 1000,
                                ratios: Tuple[float, float, float] = (0.8, 0.1, 0.1),
                                include_edge_cases: bool = True,
                                edge_case_fraction: float = 0.1,
                                anomaly_rate: Optional[float] = None,
                                seed: Optional[int] = None,
                                stratify: bool = True) -> Dict[str, List[Dict]]:
        """Generate a dataset and split into train/val/test.
        When `stratify=True`, anomalies are proportionally distributed."""
        if abs(sum(ratios) - 1.0) > 1e-6:
            raise ValueError(f"ratios must sum to 1.0, got {ratios} ({sum(ratios)})")
        full = await self.generate_dataset(
            num_samples, include_edge_cases, edge_case_fraction,
            anomaly_rate, seed=seed)

        if stratify:
            normals = [s for s in full if not s['anomaly']]
            anomalies = [s for s in full if s['anomaly']]
            rng = random.Random(seed if seed is not None else 42)
            rng.shuffle(normals)
            rng.shuffle(anomalies)

            def split_list(items: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
                n = len(items)
                n_tr = int(n * ratios[0])
                n_va = int(n * ratios[1])
                return items[:n_tr], items[n_tr:n_tr + n_va], items[n_tr + n_va:]

            n_tr, n_va, n_te = split_list(normals)
            a_tr, a_va, a_te = split_list(anomalies)
            return {
                'train': n_tr + a_tr,
                'val': n_va + a_va,
                'test': n_te + a_te,
            }
        else:
            rng = random.Random(seed if seed is not None else 42)
            rng.shuffle(full)
            n = len(full)
            n_tr = int(n * ratios[0])
            n_va = int(n * ratios[1])
            return {
                'train': full[:n_tr],
                'val': full[n_tr:n_tr + n_va],
                'test': full[n_tr + n_va:],
            }

    # ------------------------------------------------------------------
    # Public utilities
    # ------------------------------------------------------------------
    def get_last_metadata(self) -> Optional[Dict]:
        # [P2] bounded deque, thread-safe read
        if not self._metadata_history:
            return None
        return dict(self._metadata_history[-1])

    def get_all_metadata(self) -> List[Dict]:
        return [dict(m) for m in self._metadata_history]

    def select_precision(self, carbon_intensity_kg: float = 0.4,
                         accuracy_required: float = 0.95) -> PrecisionLevel:
        # [P1] wrapper accepts kg/kWh, converts internally
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select_kg(carbon_intensity_kg,
                                                    accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        async with self._carbon_total_lock:
            total = self._carbon_saved_kg_total
        return {'credit_usd': credit, 'rec_usd': rec, 'cumulative_kg': total}

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
        return await self.hitl.escalate(
            decision_context, options, confidence,
            getattr(self.config, 'hitl_confidence_threshold', 0.65))

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
            'version': self.dataset_version,
            'instance_id': getattr(self.config, 'instance_id', 'unknown'),
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'features': {
                'temporal_logic': getattr(self.config, 'temporal_logic_enabled', False),
                'xai': getattr(self.config, 'xai_enabled', False),
                'adaptive_precision': getattr(self.config,
                                              'adaptive_precision_enabled', False),
                'carbon_market': getattr(self.config, 'carbon_market_enabled', False),
                'role_specialization': getattr(self.config,
                                               'role_specialization_enabled', False),
                'chaos_testing': getattr(self.config, 'chaos_testing_enabled', False),
                'hitl': getattr(self.config, 'hitl_enabled', False),
                'federated': True,
                'causal_rl': getattr(self.config, 'causal_rl_enabled', False),
            },
            'mopd_weights': dict(self.mopd_weights),
            'timestamp': datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.rlhf:
            status['rlhf'] = {'history_len': len(self.rlhf.history),
                              'feedback_buffer': len(self.rlhf.feedback_buffer),
                              'preference_counts': dict(self.rlhf.preference_counts)}
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
        if self.moe_gating:
            status['moe'] = {'trained': self.moe_gating._trained,
                             'training_samples': len(self.moe_gating._training_data)}
        if self.adaptive_anomaly:
            status['adaptive_anomaly'] = self.adaptive_anomaly.get_stats()
        if self.pareto_optimizer:
            status['pareto_size'] = len(
                self.pareto_optimizer.get_pareto_front())
        return status

    async def shutdown(self):
        logger.info("Shutting down SyntheticDataGenerator v6.1.0...")
        self._shutdown_event.set()
        self._running = False
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        # [P3] persist RLHF + state
        if self.rlhf:
            try:
                await self.rlhf.save(getattr(self.config, 'rlhf_state_path',
                                              '/tmp/synth_rlhf_v6.json'))
            except Exception:
                pass
        try:
            await self.state.save()
        except Exception:
            pass
        try:
            await self.websocket.stop()
        except Exception:
            pass
        self.storage.dispose()
        logger.info("Shutdown complete")


# Backward compat alias
SyntheticDataGeneratorV6 = SyntheticDataGenerator


# =============================================================================
# SINGLETON + SIGNAL + MAIN
# =============================================================================
_generator_instance: Optional[SyntheticDataGenerator] = None
_generator_lock = asyncio.Lock()
_shutdown_event_global = asyncio.Event()
_shutdown_requested = False


def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_signal_shutdown())
            else:
                _shutdown_event_global.set()
        except Exception:
            pass


async def _signal_shutdown():
    _shutdown_event_global.set()


async def shutdown_handler():
    global _generator_instance
    if _generator_instance:
        await _generator_instance.shutdown()
        _generator_instance = None


async def get_synthetic_generator(config=None, **kwargs) -> SyntheticDataGenerator:
    global _generator_instance
    if _generator_instance is None:
        async with _generator_lock:
            if _generator_instance is None:
                _generator_instance = SyntheticDataGenerator(config, **kwargs)
                await _generator_instance.start()
    return _generator_instance


async def _smoke_test():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Synthetic Data Generator v6.1.0 — smoke test (Phase 1-3)")
    print("=" * 78)

    gen: Optional[SyntheticDataGenerator] = None
    try:
        gen = await get_synthetic_generator()

        print("\n✅ v6.1.0 ENHANCEMENTS (Phase 1-3):")
        print("   [P1] Precision-unit fix, real MoE, real distillation, RLHF wiring")
        print("   [P1] Baseline-aware credits, chaos gate, signals, drift, anomaly learning")
        print("   [P1] Federated→mopd wiring, single aggregator, computed quality")
        print("   [P2] aiosqlite, CircuitBreaker lock, bounded violations, new metrics")
        print("   [P2] Correlation-IDs, latency histogram, active gauge")
        print("   [P3] Batched parallel generation, streaming, train/val/test split")
        print("   [P3] Real GA fitness, real Pareto, UCB anomalies, dataset hashing")
        print("   [P3] RLHF + state persistence, dataset signing, blockchain record")

        # Basic generation
        print("\n🔬 Generating 200 samples (batched parallel)…")
        dataset = await gen.generate_dataset(
            num_samples=200, include_edge_cases=True,
            edge_case_fraction=0.2, anomaly_rate=0.05, seed=1234)
        print(f"   Samples: {len(dataset)}")
        print(f"   Anomalies: {sum(1 for d in dataset if d['anomaly'])}")

        meta = gen.get_last_metadata()
        if meta:
            qm = meta.get('quality_metrics', {})
            print(f"\n📊 Metadata (correlation_id={meta.get('correlation_id')}):")
            print(f"   Strategy: {meta.get('strategy')}")
            print(f"   Precision: {meta.get('precision_used')}")
            print(f"   Data quality: {qm.get('data_quality')}")
            print(f"   Realism: {qm.get('realism_score')}")
            print(f"   Coverage: {qm.get('coverage_score')}")
            print(f"   Anomaly diversity: {qm.get('anomaly_diversity')}")
            print(f"   Dataset hash: {(meta.get('dataset_hash') or '')[:16]}…")
            print(f"   Carbon credit: ${meta.get('carbon_credit_value_usd', 0):.6f}")
            print(f"   REC value: ${meta.get('rec_value_usd', 0):.6f}")
            if meta.get('temporal_status'):
                print(f"   Temporal: {meta['temporal_status']}")
            if meta.get('role_assignments'):
                print(f"   Dominant role: {meta['role_assignments'].get('dominant_role')}")
            if meta.get('xai_explanation') and meta['xai_explanation'].get('narrative'):
                print(f"   XAI: {meta['xai_explanation']['narrative'][0]}")
            if meta.get('blockchain_tx_hash'):
                print(f"   Blockchain tx: {meta['blockchain_tx_hash'][:16]}…")

        # RLHF policy is real — should move after feedback
        print("\n📝 RLHF policy after feedback:")
        if gen.rlhf:
            before = await gen.rlhf.get_policy_probs({})
            await gen.rlhf.record_feedback(
                state={'carbon_intensity': 0.15, 'data_quality': 0.9,
                       'anomaly_rate': 0.05, 'coverage_score': 0.6},
                action='carbon_focused', reward=0.9)
            after = await gen.rlhf.get_policy_probs({})
            print(f"   before: {[round(p, 3) for p in before]}")
            print(f"   after : {[round(p, 3) for p in after]}")

        # Distillation actually moves student probs
        if gen.distillation:
            print("\n🧠 Distillation:")
            print(f"   student probs before: "
                  f"{[round(p, 3) for p in gen.distillation.get_student_probs()]}")
            await gen.distillation.distill({
                'target_anomaly_rate': 0.05, 'target_edge_fraction': 0.1,
                'num_samples': 200, 'avg_region_carbon_g': 350,
                'forecast_carbon_g': 340, 'quality_target': 0.85})
            print(f"   student probs after : "
                  f"{[round(p, 3) for p in gen.distillation.get_student_probs()]}")

        # Precision — verify tier fires for gCO2/kWh
        print("\n⚙️  Precision selection:")
        print(f"   low carbon (0.20 kg) → {gen.select_precision(0.20).value}")
        print(f"   mid carbon (0.40 kg) → {gen.select_precision(0.40).value}")
        print(f"   high carbon (0.60 kg) → {gen.select_precision(0.60).value}")

        # Carbon credits — non-zero now
        cc = await gen.compute_carbon_credit(250.0)
        print(f"\n💱 Carbon credit: ${cc['credit_usd']:.4f}  "
              f"REC: ${cc['rec_usd']:.4f}  "
              f"cumulative: {cc['cumulative_kg']:.3f} kg")

        print(f"\n🎭 Roles: {gen.get_role_assignments()}")
        print(f"🧠 Causal ATEs: {gen.get_causal_ates()}")

        # Train/val/test split
        print("\n📂 Generating splits (100 samples)...")
        splits = await gen.generate_splits(
            num_samples=100, ratios=(0.8, 0.1, 0.1),
            edge_case_fraction=0.2, anomaly_rate=0.1, seed=999)
        for name, items in splits.items():
            n_anom = sum(1 for d in items if d['anomaly'])
            print(f"   {name}: {len(items)} samples, {n_anom} anomalies")

        # Streaming
        print("\n🌊 Streaming 50 samples in batches of 16…")
        streamed = 0
        batches = 0
        async for batch in gen.stream_dataset(
                num_samples=50, batch_size=16, edge_case_fraction=0.1,
                anomaly_rate=0.05, seed=7):
            streamed += len(batch)
            batches += 1
        print(f"   Streamed {streamed} samples in {batches} batches")

        # Chaos suite
        print("\n🧪 Chaos suite:")
        chaos = await gen.run_chaos_suite()
        print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
              f"tests: {chaos['report']['tests_run']}")

        # Comprehensive status
        print("\n📋 Comprehensive status:")
        status = gen.get_comprehensive_status()
        print(json.dumps({
            'version': status['version'],
            'carbon_saved_kg': status['carbon_saved_kg_total'],
            'mopd_weights': status['mopd_weights'],
            'features': status['features'],
            'rlhf': status.get('rlhf'),
            'distillation': status.get('distillation'),
            'federated': status.get('federated'),
            'moe': status.get('moe'),
            'adaptive_anomaly': status.get('adaptive_anomaly'),
            'hitl_total': status.get('hitl', {}).get('total'),
            'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
            'precision_last': status.get('precision', {}).get('last'),
            'causal_ates': status.get('causal_ates'),
            'pareto_size': status.get('pareto_size'),
        }, indent=2, default=str))

    finally:
        if gen is not None:
            await gen.shutdown()
        print("\n" + "=" * 78)
        print("✅ Synthetic Data Generator v6.1.0 — smoke test complete")
        print("=" * 78)


if __name__ == "__main__":
    # [P1] register signal handlers before event loop
    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except (ValueError, RuntimeError):
        pass
    asyncio.run(_smoke_test())
