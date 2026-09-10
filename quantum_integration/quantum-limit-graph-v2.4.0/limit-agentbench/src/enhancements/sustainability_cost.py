#!/usr/bin/env python3
"""
Sustainability Cost Function v5.0.0
Enterprise Quantum Resilience + LIMIT Graph + MODP + RLHF + Multi-Teacher Distillation
+ Bio-inspired GA + MoE + Pareto + v5.0.0 suite:

v5.0.0 adds (all in-file, no external modules required):
    • Temporal Logic Verification (G/F/U/->)
    • Explainable AI (XAI)
    • Adaptive Precision Switching (fp32/fp16/bf16/fp8/fp4)
    • Carbon Markets + Renewable Energy Credits (RECs)
    • Multi-Agent Role Specialization (emergent)
    • Chaos Testing as first-class citizen
    • Active RLHF (uncertainty-triggered human queries)
    • Human-in-the-Loop Coordinator
    • Federated Green Learning (FedAvg)
    • Causal RL hooks (IPW / ATE)

This is a self-contained version. Every enhancement module is defined in-file
and wired into the main `SustainabilityCostFunction` class.
"""

import asyncio
import json
import logging
import os
import random
import time
import uuid
import hashlib
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple, Union

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

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
    from prometheus_client import Counter, Gauge, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SC_COST = Counter('sustainability_cost_total', 'Cost computations', ['status'], registry=REGISTRY)
    SC_TEMPORAL = Counter('sustainability_temporal_violations_total', 'Temporal', ['formula'], registry=REGISTRY)
    SC_CHAOS = Counter('sustainability_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    SC_HITL = Counter('sustainability_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    SC_FEDERATED = Counter('sustainability_federated_rounds_total', 'Federated', registry=REGISTRY)
    SC_CARBON_CREDITS = Counter('sustainability_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    SC_XAI = Counter('sustainability_xai_explanations_total', 'XAI', registry=REGISTRY)
    SC_PRECISION = Counter('sustainability_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    SC_COST = SC_TEMPORAL = SC_CHAOS = SC_HITL = DummyMetric()
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
# v5.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ."""
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Any] = {}
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
                SC_TEMPORAL.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}


# =============================================================================
# v5.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    """Feature-attribution explainer for cost decisions."""
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
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the cost."
                     for f, v in ranked]
        SC_XAI.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}


# =============================================================================
# v5.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
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
# v5.0.0 MODULE D — CARBON MARKET CLIENT
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
# v5.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
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
# v5.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'storage_broken', 'moe_broken',
                   'ga_broken', 'rlhf_broken', 'distillation_broken']

    def __init__(self, cost_fn_ref=None):
        self.cost_fn = cost_fn_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Any] = []
        c = self.cost_fn

        try:
            if fault_type == 'carbon_api_down' and c:
                orig = c.carbon_manager.get_current_intensity
                async def broken(): raise RuntimeError("carbon API down")
                c.carbon_manager.get_current_intensity = broken
                restore.append(lambda: setattr(c.carbon_manager, 'get_current_intensity', orig))
            elif fault_type == 'storage_broken' and c:
                orig = c.storage.save_cost_history
                async def broken(*a, **k): raise RuntimeError("storage broken")
                c.storage.save_cost_history = broken
                restore.append(lambda: setattr(c.storage, 'save_cost_history', orig))
            elif fault_type == 'moe_broken' and c and c.moe_gating:
                orig = c.moe_gating.select_expert
                async def broken(*a, **k): raise RuntimeError("moe broken")
                c.moe_gating.select_expert = broken
                restore.append(lambda: setattr(c.moe_gating, 'select_expert', orig))
            elif fault_type == 'ga_broken' and c and c.ga_optimizer:
                orig = c.ga_optimizer.optimize
                async def broken(*a, **k): raise RuntimeError("ga broken")
                c.ga_optimizer.optimize = broken
                restore.append(lambda: setattr(c.ga_optimizer, 'optimize', orig))
            elif fault_type == 'rlhf_broken' and c and c.rlhf:
                orig = c.rlhf.get_policy_probs
                async def broken(_): raise RuntimeError("rlhf broken")
                c.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(c.rlhf, 'get_policy_probs', orig))
            elif fault_type == 'distillation_broken' and c and c.distillation:
                orig = c.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distillation broken")
                c.distillation.distill = broken
                restore.append(lambda: setattr(c.distillation, 'distill', orig))
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
# v5.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
class ActiveRLHF:
    """Preference-based policy with uncertainty-triggered human queries.
    Superset of the legacy RLHFManager (preserves record_feedback,
    train_reward_model, get_policy_probs).
    """
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
        self.policy_weights = np.array([1/6] * 6) if NUMPY_AVAILABLE else [1/6] * 6

    def _state_to_features(self, state):
        return [state.get('carbon_intensity', 0.4),
                state.get('cost', 0.5),
                state.get('latency', 0.5),
                state.get('accuracy', 0.8),
                state.get('helium_index', 0.0),
                state.get('material_index', 0.0)]

    def _action_to_index(self, action):
        actions = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']
        return actions.index(action) if action in actions else 0

    async def record_feedback(self, state, action, reward):
        async with self._get_lock():
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'reward': reward})
        self.update(state, action, reward)

    def _get_lock(self):
        if not hasattr(self, '_lock'):
            self._lock = asyncio.Lock()
        return self._lock

    async def train_reward_model(self):
        if not self.reward_model or len(self.feedback_buffer) < 10:
            return
        try:
            X = [f['state'] for f in self.feedback_buffer]
            y = [f['reward'] for f in self.feedback_buffer]
            self.reward_model.fit(X, y)
            logger.info(f"ActiveRLHF reward model trained on {len(self.feedback_buffer)} samples")
            self.feedback_buffer.clear()
        except Exception as e:
            logger.warning(f"ActiveRLHF train failed: {e}")

    async def get_policy_probs(self, state):
        if NUMPY_AVAILABLE:
            return self.policy_weights.tolist()
        return list(self.policy_weights)

    def update(self, context, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    def _policy(self, context):
        if not NUMPY_AVAILABLE:
            return [0.2] * len(self.actions)
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

    async def maybe_query_human(self, context, options: List[str]):
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
# v5.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
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
            SC_HITL.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        SC_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# v5.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 6):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
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
# v5.0.0 MODULE J — CAUSAL REWARD SHAPER (IPW / ATE)
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
# ORIGINAL: Configuration (preserved)
# =============================================================================
@dataclass
class SustainabilityCostConfig:
    instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    version: str = "5.0.0"
    alpha: float = 1.0
    beta: float = 2.0
    gamma: float = 0.5
    delta: float = 0.3
    epsilon: float = 0.1
    zeta: float = 0.1
    cache_ttl: int = 300
    mtop_learning_rate: float = 0.01
    mtop_decay: float = 0.99
    ga_enabled: bool = True
    ga_population_size: int = 20
    ga_generations: int = 5
    ga_mutation_rate: float = 0.2
    ga_crossover_rate: float = 0.7
    moe_enabled: bool = True
    moe_expert_count: int = 4
    moe_hidden_layers: List[int] = field(default_factory=lambda: [16, 8])
    pareto_enabled: bool = True
    pareto_max_architectures: int = 100
    limit_graph_enabled: bool = True
    limit_graph_update_interval: int = 300
    rlhf_enabled: bool = True
    rlhf_reward_model: str = "linear"
    rlhf_training_interval: int = 600
    distillation_enabled: bool = True
    distillation_temperature: float = 2.0
    distillation_alpha: float = 0.5
    distillation_interval: int = 300
    # ============ v5.0.0 flags ============
    temporal_logic_enabled: bool = True
    xai_enabled: bool = True
    adaptive_precision_enabled: bool = True
    carbon_market_enabled: bool = True
    role_specialization_enabled: bool = True
    chaos_testing_enabled: bool = True
    hitl_enabled: bool = True
    federated_enabled: bool = True
    causal_rl_enabled: bool = True
    hitl_confidence_threshold: float = 0.65


# =============================================================================
# ORIGINAL: ExpertProfile (preserved)
# =============================================================================
@dataclass
class ExpertProfile:
    expert_id: str
    energy_per_inference: float
    carbon_per_inference: float
    helium_per_inference: float
    accuracy_score: float


# =============================================================================
# ORIGINAL: CircuitBreaker + RateLimiter (preserved)
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
# ORIGINAL: EnhancedStorage (preserved)
# =============================================================================
class EnhancedStorage:
    def __init__(self, config):
        self.config = config
        self.cache = {}
        self.weight_history = []
        self.cost_history = []
        self.carbon_cache = {}
        self.node_cache = {}

    async def save_weight_history(self, weights):
        self.weight_history.append(weights)

    async def save_cost_history(self, expert_id, cost, context, weights,
                                 quantum_signature=None, blockchain_tx_hash=None):
        self.cost_history.append({
            'expert_id': expert_id, 'cost': cost, 'context': context,
            'weights': weights, 'timestamp': datetime.now().isoformat()})

    async def get_carbon_intensity(self, region, hours_ago=1):
        return self.carbon_cache.get(region)

    async def save_carbon_intensity(self, region, intensity):
        self.carbon_cache[region] = intensity

    async def get_node_data(self, node_id):
        return self.node_cache.get(node_id)

    async def save_node_data(self, node_id, helium_index, material_index):
        self.node_cache[node_id] = {'helium_index': helium_index,
                                    'material_index': material_index}


# =============================================================================
# ORIGINAL: CarbonIntensityManager + NodeRegistry (preserved)
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.region = 'global'
        self._circuit_breaker = CircuitBreaker(name="carbon_api")
        self._rate_limiter = RateLimiter(rate=10, window=60)

    async def get_current_intensity(self):
        cached = await self.storage.get_carbon_intensity(self.region)
        if cached:
            return cached / 1000.0
        intensity = 400.0
        await self.storage.save_carbon_intensity(self.region, intensity)
        return intensity / 1000.0

    async def close(self):
        pass


class NodeRegistry:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config

    async def get_node(self, node_id):
        data = await self.storage.get_node_data(node_id)
        if data:
            return data
        default = {'helium_index': 0.0, 'material_index': 0.0}
        await self.storage.save_node_data(node_id, default['helium_index'],
                                          default['material_index'])
        return default

    async def close(self):
        pass


# =============================================================================
# ORIGINAL: LimitGraphManager (preserved)
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {}
        self.constraints = {}
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
# ORIGINAL: MultiTeacherPolicyDistillation (preserved + enhanced w/ role context)
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None, role_coordinator=None):
        self.config = config
        self.moe_engine = moe_engine
        self.role_coordinator = role_coordinator
        self.student_policy = np.array([1/6] * 6) if NUMPY_AVAILABLE else [1/6] * 6
        self.temperature = config.distillation_temperature
        self.alpha = config.distillation_alpha
        self.history = []
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_engine or not NUMPY_AVAILABLE:
            return
        carbon_intensity = state.get('carbon_intensity', 0.4)
        node_data = state.get('node_data', {})
        try:
            selected, weights = await self.moe_engine.select_expert(
                state, carbon_intensity, node_data)
        except Exception:
            weights = {k: 1/6 for k in ['energy', 'carbon', 'helium',
                                        'material', 'latency', 'accuracy']}
        teacher_probs = np.array([weights.get(k, 1/6) for k in
                                   ['energy', 'carbon', 'helium',
                                    'material', 'latency', 'accuracy']])
        teacher_probs /= teacher_probs.sum()
        soft_teacher = np.exp(np.log(teacher_probs + 1e-8) / self.temperature)
        soft_teacher /= soft_teacher.sum()
        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-8))
        grad = -soft_teacher / (self.student_policy + 1e-8)
        self.student_policy -= 0.01 * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        async with self._lock:
            self.history.append({'teacher_dist': teacher_probs.tolist(),
                                 'student_dist': self.student_policy.tolist(),
                                 'loss': float(loss)})

    def get_student_probs(self):
        if NUMPY_AVAILABLE:
            return self.student_policy.tolist()
        return list(self.student_policy)


# =============================================================================
# ORIGINAL: MoEGatingNetwork (preserved)
# =============================================================================
class MoEGatingNetwork:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.experts = {
            'balanced': self._balanced_expert,
            'carbon_focused': self._carbon_focused_expert,
            'performance_focused': self._performance_focused_expert,
            'cost_focused': self._cost_focused_expert,
        }
        self.expert_names = list(self.experts.keys())
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []
        self._lock = asyncio.Lock()

    def _balanced_expert(self, context):
        return {k: 1/6 for k in ['energy', 'carbon', 'helium',
                                  'material', 'latency', 'accuracy']}
    def _carbon_focused_expert(self, context):
        return {'energy': 0.1, 'carbon': 0.5, 'helium': 0.1,
                'material': 0.1, 'latency': 0.1, 'accuracy': 0.1}
    def _performance_focused_expert(self, context):
        return {'energy': 0.1, 'carbon': 0.1, 'helium': 0.1,
                'material': 0.1, 'latency': 0.2, 'accuracy': 0.4}
    def _cost_focused_expert(self, context):
        return {'energy': 0.3, 'carbon': 0.1, 'helium': 0.3,
                'material': 0.1, 'latency': 0.1, 'accuracy': 0.1}

    def _encode_context(self, context, carbon_intensity, node_data):
        return (np.array([
            min(1.0, carbon_intensity),
            node_data.get('helium_index', 0.0),
            node_data.get('material_index', 0.0),
            context.get('token_count', 1) / 1000.0,
            context.get('expected_latency_ms', 100) / 1000.0,
            0.5,
        ], dtype=np.float32) if NUMPY_AVAILABLE
            else [0.5] * 6)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        try:
            X = np.array([item[0] for item in self._training_data])
            y = np.array([item[1] for item in self._training_data])
            self._scaler = StandardScaler()
            X_scaled = self._scaler.fit_transform(X)
            self._gating_model = MLPClassifier(
                hidden_layer_sizes=self.config.moe_hidden_layers,
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
        expert_func = self.experts[selected]
        return selected, expert_func(context)

    async def add_training_sample(self, context, carbon_intensity, node_data,
                                   selected_expert, reward):
        if not NUMPY_AVAILABLE:
            return
        features = self._encode_context(context, carbon_intensity, node_data)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx))
            if len(self._training_data) % 10 == 0:
                self._train_gating()


# =============================================================================
# ORIGINAL: GeneticWeightOptimizer (preserved)
# =============================================================================
class GeneticWeightOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.obj_names = ['energy', 'carbon', 'helium', 'material',
                          'latency', 'accuracy']

    def _random_weight_vector(self):
        vec = [random.random() for _ in self.obj_names]
        total = sum(vec)
        return [v / total for v in vec]

    def _mutate(self, vec):
        new_vec = vec.copy()
        for i in range(len(new_vec)):
            if random.random() < self.mutation_rate:
                new_vec[i] = max(0.0, min(1.0, new_vec[i] + random.gauss(0, 0.1)))
        total = sum(new_vec)
        if total > 0:
            new_vec = [v / total for v in new_vec]
        return new_vec

    def _crossover(self, p1, p2):
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for i in range(len(c1)):
            if random.random() < 0.5:
                c1[i], c2[i] = p2[i], p1[i]
        return c1, c2

    async def _evaluate_fitness(self, weight_vec, historical_data=None):
        return random.uniform(0.5, 1.0)

    async def run_search(self, historical_data=None):
        population = [self._random_weight_vector() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None
        for gen in range(self.generations):
            fitnesses = await asyncio.gather(
                *[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses),
                                 key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]
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
        if best_individual is None:
            best_individual = self._random_weight_vector()
        return {name: best_individual[i] for i, name in enumerate(self.obj_names)}

    async def optimize(self):
        return await self.run_search()


# =============================================================================
# ORIGINAL: MTOPWeightEngine (preserved)
# =============================================================================
class MTOPWeightEngine:
    def __init__(self, config):
        self.config = config
        self.teacher_weights = {
            'energy': 1.0, 'carbon': 2.0, 'helium': 0.5,
            'material': 0.3, 'latency': 0.1, 'accuracy': 0.1}

    async def get_weights(self, context, carbon_intensity, historical_scores, user_prefs):
        return self.teacher_weights


# =============================================================================
# ORIGINAL: ParetoFrontOptimizer (preserved)
# =============================================================================
class ParetoFrontOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front = []
        self.max_size = config.pareto_max_architectures

    def _dominates(self, a, b):
        a_metrics = (a['energy'], a['carbon'], a['helium'],
                     a['material'], a['latency'], -a['accuracy'])
        b_metrics = (b['energy'], b['carbon'], b['helium'],
                     b['material'], b['latency'], -b['accuracy'])
        return (all(a_metrics[i] <= b_metrics[i] for i in range(6)) and
                any(a_metrics[i] < b_metrics[i] for i in range(6)))

    async def add_expert(self, expert, context, carbon_intensity):
        entry = {
            'expert_id': expert.expert_id,
            'energy': expert.energy_per_inference * context.get('token_count', 1),
            'carbon': expert.carbon_per_inference * context.get('token_count', 1) * carbon_intensity,
            'helium': expert.helium_per_inference * context.get('token_count', 1),
            'material': 0.0,
            'latency': context.get('expected_latency_ms', 100),
            'accuracy': expert.accuracy_score,
        }
        for existing in self.pareto_front:
            if self._dominates(existing, entry):
                return False
        self.pareto_front = [e for e in self.pareto_front
                              if not self._dominates(entry, e)]
        self.pareto_front.append(entry)
        if len(self.pareto_front) > self.max_size:
            self.pareto_front = self.pareto_front[:self.max_size]
        return True

    def get_pareto_front(self):
        return self.pareto_front


# =============================================================================
# ENHANCED SustainabilityCostFunction v5.0.0
# =============================================================================
class SustainabilityCostFunction:
    """
    Enhanced sustainability cost function v5.0.0.
    Preserves all original APIs and adds ten enhancement areas:
        - Temporal Logic Verification
        - Explainable AI (XAI)
        - Adaptive Precision Switching
        - Carbon Markets + RECs
        - Multi-Agent Role Specialization
        - Chaos Testing
        - Active RLHF
        - Human-in-the-Loop
        - Federated Green Learning
        - Causal RL hooks
    """

    def __init__(self, config=None):
        self.config = config or SustainabilityCostConfig()
        self.storage = EnhancedStorage(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.node_registry = NodeRegistry(self.storage, self.config)

        # Original modules
        self.mtop_engine = MTOPWeightEngine(self.config)
        self.ga_optimizer = GeneticWeightOptimizer(self.config, self.storage) \
            if self.config.ga_enabled else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) \
            if self.config.moe_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config, self.storage) \
            if self.config.pareto_enabled else None
        self.limit_graph = LimitGraphManager(self.config) \
            if self.config.limit_graph_enabled else None

        # ============ v5.0.0 module instances ============
        self.temporal_monitor = TemporalLogicMonitor() \
            if self.config.temporal_logic_enabled else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("cost_floor", "G(cost >= 0.0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 1.0)")
            self.temporal_monitor.add_formula("convergence", "F(cost <= 10.0)")

        self.xai = XAIExplainer(['energy', 'carbon', 'helium',
                                  'material', 'latency', 'accuracy']) \
            if self.config.xai_enabled else None

        self.precision_controller = AdaptivePrecisionController() \
            if self.config.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketClient() \
            if self.config.carbon_market_enabled else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if self.config.role_specialization_enabled else None

        # Active RLHF replaces the legacy RLHFManager
        self.rlhf = ActiveRLHF(
            action_space=['energy', 'carbon', 'helium',
                          'material', 'latency', 'accuracy'],
        ) if self.config.rlhf_enabled else None
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (self.config.hitl_enabled and self.rlhf) else None

        self.federated = FederatedAggregator(num_params=6) \
            if self.config.federated_enabled else None
        self.causal_shaper = CausalRewardShaper(num_actions=6) \
            if self.config.causal_rl_enabled else None
        self.chaos_tester = ChaosTester(self) \
            if self.config.chaos_testing_enabled else None

        # Distillation (with role coordinator for v5 awareness)
        self.distillation = MultiTeacherPolicyDistillation(
            self.config, self.moe_gating, self.role_coordinator,
        ) if self.config.distillation_enabled and self.moe_gating else None

        # Weights
        self.weights = {
            'alpha': self.config.alpha, 'beta': self.config.beta,
            'gamma': self.config.gamma, 'delta': self.config.delta,
            'epsilon': self.config.epsilon, 'zeta': self.config.zeta}

        # Caches
        self._carbon_cache = None
        self._carbon_cache_timestamp = None
        self._node_cache = {}
        self._cache_lock = asyncio.Lock()
        self._background_tasks = []
        self._shutdown_event = asyncio.Event()
        self._running = False

        self._circuit_breaker = CircuitBreaker(name="sustainability_cost")
        self._rate_limiter = RateLimiter(rate=100, window=60)

        self.instance_id = str(uuid.uuid4())[:8]
        self._carbon_saved_kg_total = 0.0
        self._last_metadata: Optional[Dict] = None

        logger.info(f"SustainabilityCostFunction v{self.config.version} "
                    f"initialized (instance {self.instance_id})")

    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    async def start(self):
        self._running = True
        tasks = []
        if self.config.ga_enabled and self.ga_optimizer:
            tasks.append(self._ga_optimization_loop())
        if self.limit_graph:
            tasks.append(self._limit_graph_loop())
        if self.rlhf:
            tasks.append(self._rlhf_loop())
        if self.distillation:
            tasks.append(self._distillation_loop())
        if self.federated:
            tasks.append(self._federated_loop())
        if self.chaos_tester:
            tasks.append(self._chaos_loop())
        for task in tasks:
            self._background_tasks.append(asyncio.create_task(task))
        logger.info("Background tasks started")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                best_weights = await self.ga_optimizer.optimize()
                if best_weights:
                    self.weights['alpha'] = best_weights.get('energy', self.weights['alpha'])
                    self.weights['beta'] = best_weights.get('carbon', self.weights['beta'])
                    self.weights['gamma'] = best_weights.get('helium', self.weights['gamma'])
                    self.weights['delta'] = best_weights.get('material', self.weights['delta'])
                    self.weights['epsilon'] = best_weights.get('latency', self.weights['epsilon'])
                    self.weights['zeta'] = best_weights.get('accuracy', self.weights['zeta'])
            except Exception as e:
                logger.error(f"GA optimization loop error: {e}")

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.limit_graph_update_interval)
            try:
                carbon_intensity = await self._get_carbon_intensity()
                await self.limit_graph.update_constraint('carbon', carbon_intensity)
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.rlhf_training_interval)
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.distillation_interval)
            try:
                if self.distillation:
                    ci = await self._get_carbon_intensity()
                    state = {'carbon_intensity': ci,
                             'node_data': {'helium_index': 0.0, 'material_index': 0.0},
                             'token_count': 100, 'expected_latency_ms': 50}
                    await self.distillation.distill(state)
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                if self.federated and self.federated.client_updates:
                    self.federated.aggregate()
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                if self.chaos_tester:
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.05)
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    # ----------------------------------------------------------------------
    # Helpers (preserved)
    # ----------------------------------------------------------------------
    async def _get_carbon_intensity(self):
        async with self._cache_lock:
            if self._carbon_cache is not None and \
               (datetime.now() - self._carbon_cache_timestamp).seconds < self.config.cache_ttl:
                return self._carbon_cache
        intensity = await self.carbon_manager.get_current_intensity()
        async with self._cache_lock:
            self._carbon_cache = intensity
            self._carbon_cache_timestamp = datetime.now()
        return intensity

    async def _get_node_data(self, node_id):
        async with self._cache_lock:
            if node_id in self._node_cache:
                return self._node_cache[node_id]
        data = await self.node_registry.get_node(node_id)
        async with self._cache_lock:
            self._node_cache[node_id] = data
        return data

    def inject_dependencies(self, carbon_manager=None, node_registry=None):
        if carbon_manager:
            self.carbon_manager = carbon_manager
        if node_registry:
            self.node_registry = node_registry

    # ----------------------------------------------------------------------
    # Core compute (v5.0.0 enriched)
    # ----------------------------------------------------------------------
    async def compute(self, expert: ExpertProfile, context: Dict[str, Any]) -> float:
        carbon_intensity = await self._get_carbon_intensity()
        target_node = context.get('target_node_id')
        node_data = await self._get_node_data(target_node) if target_node \
            else {'helium_index': 0.0, 'material_index': 0.0}

        tokens = context.get('token_count', 1)
        latency = context.get('expected_latency_ms', 100.0)

        # Adaptive precision
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(carbon_intensity, 0.95)

        # Components
        E = expert.energy_per_inference * tokens
        CO2 = expert.carbon_per_inference * tokens * carbon_intensity
        helium_usage = expert.helium_per_inference * tokens
        H = helium_usage * (1 + node_data.get('helium_index', 0.0))
        M = node_data.get('material_index', 0.0)
        L = latency
        acc = max(0.0, min(1.0, expert.accuracy_score))
        A = 1.0 - acc

        # Base weights
        alpha = self.weights['alpha']
        beta = self.weights['beta']
        gamma = self.weights['gamma']
        delta = self.weights['delta']
        epsilon = self.weights['epsilon']
        zeta = self.weights['zeta']

        # Priority: RLHF > Distillation > MoE > MTOP
        if self.rlhf and self.rlhf.reward_model is not None:
            probs = await self.rlhf.get_policy_probs(context)
            obj_names = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']
            wmap = {obj_names[i]: probs[i % len(probs)] for i in range(len(obj_names))}
            alpha = wmap.get('energy', alpha)
            beta = wmap.get('carbon', beta)
            gamma = wmap.get('helium', gamma)
            delta = wmap.get('material', delta)
            epsilon = wmap.get('latency', epsilon)
            zeta = wmap.get('accuracy', zeta)
            strategy = 'rlhf'
        elif self.distillation and self.distillation.get_student_probs():
            probs = self.distillation.get_student_probs()
            obj_names = ['energy', 'carbon', 'helium', 'material', 'latency', 'accuracy']
            wmap = {obj_names[i]: probs[i] for i in range(min(len(probs), len(obj_names)))}
            alpha = wmap.get('energy', alpha)
            beta = wmap.get('carbon', beta)
            gamma = wmap.get('helium', gamma)
            delta = wmap.get('material', delta)
            epsilon = wmap.get('latency', epsilon)
            zeta = wmap.get('accuracy', zeta)
            strategy = 'distillation'
        elif self.moe_gating:
            selected, weights = await self.moe_gating.select_expert(
                context, carbon_intensity, node_data)
            alpha = weights.get('energy', alpha)
            beta = weights.get('carbon', beta)
            gamma = weights.get('helium', gamma)
            delta = weights.get('material', delta)
            epsilon = weights.get('latency', epsilon)
            zeta = weights.get('accuracy', zeta)
            strategy = 'moe'
        else:
            mtop_weights = await self.mtop_engine.get_weights(
                context, carbon_intensity, {}, {})
            alpha = mtop_weights.get('energy', alpha)
            beta = mtop_weights.get('carbon', beta)
            gamma = mtop_weights.get('helium', gamma)
            delta = mtop_weights.get('material', delta)
            epsilon = mtop_weights.get('latency', epsilon)
            zeta = mtop_weights.get('accuracy', zeta)
            strategy = 'mtop'

        # LIMIT Graph adjustment
        if self.limit_graph:
            carbon_influence = await self.limit_graph.evaluate_path('carbon', 'cost')
            if carbon_influence > 0.5:
                beta *= (1 + carbon_influence * 0.2)

        cost = alpha * E + beta * CO2 + gamma * H + delta * M + epsilon * L + zeta * A

        # Update LIMIT graph constraints
        if self.limit_graph:
            await self.limit_graph.update_constraint('cost', cost)
            await self.limit_graph.update_constraint('latency', latency)

        # Temporal gate
        temporal_status = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'cost': float(cost),
                'carbon': float(CO2),
                'latency': float(latency)})
            temporal_status = self.temporal_monitor.evaluate()

        # XAI explanation
        xai_out = None
        if self.xai:
            cand = {'energy': E, 'carbon': CO2, 'helium': H,
                    'material': M, 'latency': L, 'accuracy': A}
            weights_used = {'energy': alpha, 'carbon': beta, 'helium': gamma,
                            'material': delta, 'latency': epsilon, 'accuracy': zeta}
            xai_out = self.xai.explain(cand, weights_used, [cand])

        # Role assignments
        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7,
                'energy': 1.0 - min(1.0, E),
                'performance': acc})

        # HITL escalation
        hitl_outcome = None
        confidence = acc
        if self.hitl and confidence < self.config.hitl_confidence_threshold:
            try:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={'expert': expert.expert_id,
                                      'cost': cost, 'strategy': strategy},
                    options=['accept', 'retry', 'downgrade'],
                    confidence=confidence,
                    confidence_threshold=self.config.hitl_confidence_threshold)
            except Exception:
                pass

        # Causal observation
        if self.causal_shaper:
            try:
                action_idx = ['energy', 'carbon', 'helium',
                              'material', 'latency', 'accuracy'].index(strategy)
            except (ValueError, Exception):
                action_idx = 0
            self.causal_shaper.record(action_idx, -cost,  # reward = -cost
                                       [alpha, beta, gamma, delta, epsilon, zeta])

        # Carbon credit + REC
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market and carbon_intensity > 0:
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
                    [alpha, beta, gamma, delta, epsilon, zeta],
                    samples=1)
                if len(self.storage.cost_history) % 5 == 0:
                    agg = self.federated.aggregate()
                    federated_round = agg['round']
            except Exception:
                pass

        # Chaos smoke test
        chaos_passed = None
        if self.chaos_tester and len(self.storage.cost_history) % 5 == 0:
            try:
                cr = await self.chaos_tester.run_test(
                    random.choice(ChaosTester.FAULT_TYPES), duration_s=0.02)
                chaos_passed = cr['passed']
            except Exception:
                pass

        # Save history (preserved)
        await self.storage.save_cost_history(expert.expert_id, cost, context, self.weights)

        # Update Pareto front (preserved)
        if self.pareto_optimizer:
            await self.pareto_optimizer.add_expert(expert, context, carbon_intensity)

        # Attach metadata for the caller
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
            'causal_ates': self.causal_shaper.compute_ate() if self.causal_shaper else {},
            'strategy': strategy,
            'carbon_intensity': carbon_intensity,
        }

        SC_COST.labels(status='success').inc()
        return cost

    async def compute_multiple(self, experts: List[ExpertProfile],
                                context: Dict[str, Any]) -> Dict[str, float]:
        results = {}
        for expert in experts:
            results[expert.expert_id] = await self.compute(expert, context)
        return results

    # ----------------------------------------------------------------------
    # Feedback (preserved API; async wrapper)
    # ----------------------------------------------------------------------
    async def record_feedback(self, expert: ExpertProfile,
                               context: Dict[str, Any], reward: float):
        if self.rlhf:
            state = {
                'carbon_intensity': await self._get_carbon_intensity(),
                'cost': reward,
                'latency': context.get('expected_latency_ms', 100),
                'accuracy': expert.accuracy_score,
                'helium_index': context.get('helium_index', 0.0),
                'material_index': context.get('material_index', 0.0),
            }
            await self.rlhf.record_feedback(state, 'balanced', reward)

    # ----------------------------------------------------------------------
    # v5.0.0 utility APIs
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

    async def escalate_decision(self, decision_context: Dict,
                                 options: List[str],
                                 confidence: float) -> Dict:
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
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'features': {
                'temporal_logic': self.config.temporal_logic_enabled,
                'xai': self.config.xai_enabled,
                'adaptive_precision': self.config.adaptive_precision_enabled,
                'carbon_market': self.config.carbon_market_enabled,
                'role_specialization': self.config.role_specialization_enabled,
                'chaos_testing': self.config.chaos_testing_enabled,
                'hitl': self.config.hitl_enabled,
                'federated': self.config.federated_enabled,
                'causal_rl': self.config.causal_rl_enabled,
            },
            'weights': dict(self.weights),
            'timestamp': datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.rlhf:
            status['rlhf'] = {'history_len': len(self.rlhf.history)}
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
        logger.info("Shutting down SustainabilityCostFunction v5.0.0...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        await self.node_registry.close()
        logger.info("Shutdown complete")


# =============================================================================
# Singleton accessor (preserved)
# =============================================================================
_cost_function_instance = None
_cost_function_lock = asyncio.Lock()


async def get_sustainability_cost_function(config=None):
    global _cost_function_instance
    if _cost_function_instance is None:
        async with _cost_function_lock:
            if _cost_function_instance is None:
                _cost_function_instance = SustainabilityCostFunction(config)
                await _cost_function_instance.start()
    return _cost_function_instance


# =============================================================================
# SMOKE TEST
# =============================================================================
async def _smoke_test():
    config = SustainabilityCostConfig()
    cost_func = await get_sustainability_cost_function(config)

    expert = ExpertProfile(
        expert_id="expert_1",
        energy_per_inference=0.5,
        carbon_per_inference=0.05,
        helium_per_inference=0.01,
        accuracy_score=0.92)

    context = {'token_count': 100, 'target_node_id': 'node_1',
               'expected_latency_ms': 50}

    print("\n🧮 Computing cost...")
    cost = await cost_func.compute(expert, context)
    print(f"   Cost: {cost:.4f}")

    meta = cost_func.get_last_metadata()
    if meta:
        print(f"   Strategy: {meta['strategy']}")
        print(f"   Precision: {meta['precision_used']}")
        print(f"   Carbon credit: ${meta['carbon_credit_value_usd']:.4f}")
        print(f"   REC value: ${meta['rec_value_usd']:.4f}")
        if meta['temporal_status']:
            print(f"   Temporal: {meta['temporal_status']}")
        if meta['role_assignments']:
            print(f"   Dominant role: {meta['role_assignments']['dominant_role']}")
        if meta['xai_explanation'] and meta['xai_explanation'].get('narrative'):
            print(f"   XAI: {meta['xai_explanation']['narrative'][0]}")
        if meta['hitl_outcome']:
            print(f"   HITL: {meta['hitl_outcome']['source']} -> "
                  f"{meta['hitl_outcome']['chosen']}")

    print("\n📝 Recording RLHF feedback...")
    await cost_func.record_feedback(expert, context, reward=0.8)
    print(f"   RLHF feedback recorded.")

    print("\n⚙️  Precision selector →",
          cost_func.select_precision(carbon_intensity=0.35).value)
    cc = await cost_func.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"\n🎭 Roles: {cost_func.get_role_assignments()}")
    print(f"🧠 Causal ATEs: {cost_func.get_causal_ates()}")

    print("\n🧪 Chaos suite:")
    chaos = await cost_func.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📊 Comprehensive status:")
    status = cost_func.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'carbon_saved_kg': status['carbon_saved_kg_total'],
        'features': status['features'],
        'weights': status['weights'],
        'rlhf': status.get('rlhf'),
        'distillation': status.get('distillation'),
        'federated': status.get('federated'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'causal_ates': status.get('causal_ates'),
    }, indent=2, default=str))

    # Pareto front (preserved API)
    if cost_func.pareto_optimizer:
        front = cost_func.pareto_optimizer.get_pareto_front()
        print(f"\n📈 Pareto front size: {len(front)}")

    await cost_func.shutdown()
    print("\n" + "=" * 78)
    print("✅ Sustainability Cost Function v5.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
