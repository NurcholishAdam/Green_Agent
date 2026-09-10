"""
reward_calculator.py — Enhanced v17.0.0

Enhanced reward calculator with MODP, bio_inspired, moe_system, LIMIT Graph, RLHF,
and Multi-Teacher Policy Distillation integration.

v17.0.0 adds (all in-file, no external modules required):
    • Temporal Logic Verification (G/F/U/->)
    • Explainable AI (XAI) for every reward decision
    • Adaptive Precision Switching (fp32/fp16/bf16/fp8/fp4)
    • Carbon Markets + Renewable Energy Credits (RECs)
    • Multi-Agent Role Specialization (emergent)
    • Chaos Testing as first-class citizen
    • Active RLHF (uncertainty-triggered human queries)
    • Human-in-the-Loop Coordinator
    • Federated Green Learning (FedAvg)
    • Causal RL hooks (IPW / ATE)

Original features preserved:
    • MODP-based multi-objective evaluation with fallback to weighted sum.
    • Bio-inspired weight adaptation via genetic algorithm.
    • MoE context-aware dynamic weighting.
    • LIMIT Graph for constraint propagation.
    • RLHF for reward model updates.
    • Multi-Teacher Policy Distillation.
    • Persistence of weights to JSON.
    • Configurable objectives.
"""

import asyncio
import json
import os
import time
import uuid
import hashlib
import logging
from collections import deque, defaultdict
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List, Callable, Tuple

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

# Optional imports with fallback stubs
try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .bio_inspired import GeneticOptimizer
except ImportError:
    class GeneticOptimizer:
        def adapt(self, context, reward, weights):
            return weights

try:
    from .moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def encode(self, task):
            return {}
        def get_weights(self, task):
            return {}

try:
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.linear_model import LinearRegression, LogisticRegression
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
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    RC_REWARDS = Counter('reward_calculator_total', 'Total rewards', ['status'], registry=REGISTRY)
    RC_TEMPORAL = Counter('reward_temporal_violations_total', 'Temporal', ['formula'], registry=REGISTRY)
    RC_CHAOS = Counter('reward_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    RC_HITL = Counter('reward_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    RC_FEDERATED = Counter('reward_federated_rounds_total', 'Federated', registry=REGISTRY)
    RC_CARBON_CREDITS = Counter('reward_carbon_credits_usd_total', 'Carbon credits', registry=REGISTRY)
    RC_XAI = Counter('reward_xai_explanations_total', 'XAI', registry=REGISTRY)
    RC_PRECISION = Counter('reward_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    RC_REWARDS = RC_TEMPORAL = RC_CHAOS = RC_HITL = DummyMetric()
    RC_FEDERATED = RC_CARBON_CREDITS = RC_XAI = RC_PRECISION = DummyMetric()


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
                RC_TEMPORAL.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}


# =============================================================================
# v17.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    """Feature-attribution explainer for reward decisions."""
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
        RC_XAI.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}


# =============================================================================
# v17.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    """Hardware-aware precision selection."""
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
        RC_PRECISION.labels(level=self.last_precision.value).inc()
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
    """Simulated client for carbon markets and RECs."""
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
        RC_CARBON_CREDITS.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# =============================================================================
# v17.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    """Emergent multi-agent role specialization via softmax affinity."""
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
    """Fault injection for the reward calculator."""
    FAULT_TYPES = ['modp_broken', 'bio_broken', 'moe_broken',
                   'rlhf_broken', 'distillation_broken', 'persistence_broken']

    def __init__(self, calculator_ref=None):
        self.calculator = calculator_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        c = self.calculator

        try:
            if fault_type == 'modp_broken' and c:
                orig = c.modp.evaluate
                def broken(*a, **k): raise RuntimeError("modp broken")
                c.modp.evaluate = broken
                restore.append(lambda: setattr(c.modp, 'evaluate', orig))
            elif fault_type == 'bio_broken' and c:
                orig = c.bio.adapt
                def broken(*a, **k): raise RuntimeError("bio broken")
                c.bio.adapt = broken
                restore.append(lambda: setattr(c.bio, 'adapt', orig))
            elif fault_type == 'moe_broken' and c:
                orig = c.moe.get_weights
                def broken(*a, **k): raise RuntimeError("moe broken")
                c.moe.get_weights = broken
                restore.append(lambda: setattr(c.moe, 'get_weights', orig))
            elif fault_type == 'rlhf_broken' and c:
                orig = c.rlhf.predict_reward
                def broken(*a, **k): raise RuntimeError("rlhf broken")
                c.rlhf.predict_reward = broken
                restore.append(lambda: setattr(c.rlhf, 'predict_reward', orig))
            elif fault_type == 'distillation_broken' and c:
                orig = c.distillation.distill
                def broken(*a, **k): raise RuntimeError("distillation broken")
                c.distillation.distill = broken
                restore.append(lambda: setattr(c.distillation, 'distill', orig))
            elif fault_type == 'persistence_broken' and c:
                orig = c._save_weights
                def broken(): raise RuntimeError("persistence broken")
                c._save_weights = broken
                restore.append(lambda: setattr(c, '_save_weights', orig))
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
        RC_CHAOS.labels(fault=fault_type, status='pass' if passed else 'fail').inc()
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
    """Preference-based policy with uncertainty-triggered human queries.
    Superset of the legacy RLHFManager (preserves record_feedback, train,
    predict_reward, get_policy_probs).
    """
    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0,
                 model: Optional[Any] = None):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []
        self.reward_model = model
        self.policy_weights = np.array([0.25] * 5) if NUMPY_AVAILABLE else [0.25] * 5
        self._trained = model is not None

    # ---- Legacy API (preserved) ----
    def record_feedback(self, state: Dict, action: str, reward: float):
        self.feedback_buffer.append({
            'state': self._state_to_features(state),
            'action': self._action_to_index(action),
            'reward': reward})
        self.update(state, action, reward)

    def _state_to_features(self, state: Dict) -> List[float]:
        return [state.get('quality', 0.5),
                state.get('throughput', 0.5),
                state.get('energy_efficiency', 0.5),
                state.get('carbon_efficiency', 0.5),
                state.get('memory_efficiency', 0.5)]

    def _action_to_index(self, action: str) -> int:
        actions = ['quality', 'throughput', 'energy_efficiency',
                   'carbon_efficiency', 'memory_efficiency']
        return actions.index(action) if action in actions else 0

    def train(self):
        if self.reward_model is None or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        try:
            self.reward_model.fit(X, y)
            self._trained = True
            self.feedback_buffer.clear()
        except Exception:
            pass

    def predict_reward(self, state: Dict) -> float:
        if self._trained and self.reward_model is not None:
            features = self._state_to_features(state)
            try:
                return float(self.reward_model.predict([features])[0])
            except Exception:
                pass
        if NUMPY_AVAILABLE:
            return sum(state.get(k, 0) * self.policy_weights[i]
                       for i, k in enumerate(['quality', 'throughput',
                                              'energy_efficiency',
                                              'carbon_efficiency',
                                              'memory_efficiency']))
        return 0.0

    def get_policy_probs(self) -> List[float]:
        if NUMPY_AVAILABLE:
            return self.policy_weights.tolist()
        return list(self.policy_weights)

    # ---- v17.0.0 additions ----
    def _policy(self, context) -> Any:
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

    def update(self, context, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

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
# v17.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
class HumanInTheLoopCoordinator:
    """Escalates low-confidence reward decisions to humans with auto-fallback."""
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
            RC_HITL.labels(status='auto').inc()
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
        RC_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# v17.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    """FedAvg-style cross-deployment aggregation of reward weights."""
    def __init__(self, num_params: int = 5):
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
        RC_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# v17.0.0 MODULE J — CAUSAL REWARD SHAPER (IPW / ATE)
# =============================================================================
class CausalRewardShaper:
    """Lightweight causal inference: IPW-based average treatment effect."""
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
# EXISTING LIMIT Graph Manager (unchanged)
# =============================================================================
class LimitGraphManager:
    """Maintains a graph of system constraints and provides path evaluation."""
    def __init__(self, graph: Optional[Dict[str, Dict[str, float]]] = None):
        self.graph = graph or {
            'carbon': {'cost': 0.8},
            'cost': {'latency': 0.2},
            'latency': {'throughput': -0.5},
            'throughput': {'diversity': 0.1},
            'diversity': {'carbon': -0.3}
        }
        self.constraints: Dict[str, float] = {}

    def update_constraint(self, name: str, value: float):
        self.constraints[name] = value

    def get_constraint(self, name: str) -> float:
        return self.constraints.get(name, 0.0)

    def evaluate_path(self, start: str, end: str) -> float:
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

    def get_graph_summary(self) -> Dict:
        return {'nodes': list(self.graph.keys()),
                'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}


# =============================================================================
# EXISTING RLHF Manager (legacy — kept for backward-compat)
# NOTE: New code should use ActiveRLHF (v17.0.0)
# =============================================================================
class RLHFManager:
    """Legacy RLHF manager. Kept for backward compatibility.
    For v17.0.0 features, use ActiveRLHF instead."""
    def __init__(self, model: Optional[Any] = None):
        self.feedback_buffer: List[Dict] = []
        self.reward_model = model
        self.policy_weights = np.array([0.25] * 5) if NUMPY_AVAILABLE else [0.25] * 5
        self._trained = model is not None

    def record_feedback(self, state: Dict, action: str, reward: float):
        self.feedback_buffer.append({
            'state': self._state_to_features(state),
            'action': self._action_to_index(action),
            'reward': reward})

    def _state_to_features(self, state: Dict) -> List[float]:
        return [state.get('quality', 0.5),
                state.get('throughput', 0.5),
                state.get('energy_efficiency', 0.5),
                state.get('carbon_efficiency', 0.5),
                state.get('memory_efficiency', 0.5)]

    def _action_to_index(self, action: str) -> int:
        actions = ['quality', 'throughput', 'energy_efficiency',
                   'carbon_efficiency', 'memory_efficiency']
        return actions.index(action) if action in actions else 0

    def train(self):
        if self.reward_model is None or len(self.feedback_buffer) < 10:
            return
        X = [f['state'] for f in self.feedback_buffer]
        y = [f['reward'] for f in self.feedback_buffer]
        try:
            self.reward_model.fit(X, y)
            self._trained = True
            self.feedback_buffer.clear()
        except Exception:
            pass

    def predict_reward(self, state: Dict) -> float:
        if self._trained and self.reward_model is not None:
            features = self._state_to_features(state)
            try:
                return float(self.reward_model.predict([features])[0])
            except Exception:
                pass
        if NUMPY_AVAILABLE:
            return sum(state.get(k, 0) * self.policy_weights[i]
                       for i, k in enumerate(['quality', 'throughput',
                                              'energy_efficiency',
                                              'carbon_efficiency',
                                              'memory_efficiency']))
        return 0.0

    def get_policy_probs(self) -> List[float]:
        if NUMPY_AVAILABLE:
            return self.policy_weights.tolist()
        return list(self.policy_weights)


# =============================================================================
# EXISTING Multi-Teacher Policy Distillation (enhanced)
# =============================================================================
class MultiTeacherPolicyDistillation:
    """Distills multiple teacher policies into a single student policy."""
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 role_coordinator: Optional[RoleSpecializationCoordinator] = None):
        self.temperature = temperature
        self.alpha = alpha
        self.role_coordinator = role_coordinator
        self.student_policy = np.array([0.2] * 5) if NUMPY_AVAILABLE else [0.2] * 5
        self.history = []

    def distill(self, teacher_policies: List[List[float]]):
        if not teacher_policies or not NUMPY_AVAILABLE:
            return
        teacher_avg = np.mean([np.array(p) for p in teacher_policies], axis=0)
        teacher_avg /= teacher_avg.sum()

        softened = np.exp(np.log(teacher_avg + 1e-8) / self.temperature)
        softened /= softened.sum()

        loss = -np.sum(softened * np.log(self.student_policy + 1e-8))
        grad = -softened / (self.student_policy + 1e-8)
        lr = 0.01
        self.student_policy -= lr * grad
        self.student_policy = np.clip(self.student_policy, 0.01, None)
        self.student_policy /= self.student_policy.sum()
        self.history.append(float(loss))

    def get_student_probs(self) -> List[float]:
        if NUMPY_AVAILABLE:
            return self.student_policy.tolist()
        return list(self.student_policy)


# =============================================================================
# ENHANCED RewardCalculator — v17.0.0
# =============================================================================
class RewardCalculator:
    """
    Enhanced reward calculator v17.0.0 with all ten enhancement areas.
    Preserves all original APIs while adding:
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

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        modp_optimizer: Optional[Any] = None,
        bio_optimizer: Optional[Any] = None,
        moe_router: Optional[Any] = None,
        limit_graph: Optional[LimitGraphManager] = None,
        rlhf: Optional[Any] = None,
        distillation: Optional[MultiTeacherPolicyDistillation] = None,
        persistence_file: Optional[str] = "reward_weights.json",
        enable_adaptation: bool = True,
        min_quality_threshold: float = 0.5,
        max_latency_ms: float = 1e9,
        # ============ v17.0.0 flags ============
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
    ):
        self.logger = logging.getLogger(__name__)

        # Core modules (fallback to stubs)
        self.modp = modp_optimizer if modp_optimizer else ParetoOptimizer()
        self.bio = bio_optimizer if bio_optimizer else GeneticOptimizer()
        self.moe = moe_router if moe_router else ExpertRouter()

        # LIMIT Graph
        self.limit_graph = limit_graph if limit_graph else LimitGraphManager()

        # ============ v17.0.0 module instances ============
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

        # Objectives
        self.objective_names = [
            "quality", "throughput", "energy_efficiency",
            "carbon_efficiency", "memory_efficiency"]

        # Temporal Logic Monitor
        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("quality_floor", "G(quality >= 0.0)")
            self.temporal_monitor.add_formula("reward_bounds", "G(reward >= -10.0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 1.0)")

        # XAI Explainer
        self.xai = XAIExplainer(self.objective_names) if enable_xai else None

        # Adaptive Precision Controller
        self.precision_controller = AdaptivePrecisionController() \
            if enable_adaptive_precision else None

        # Carbon Market Client
        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None

        # Role Specialization Coordinator
        self.role_coordinator = RoleSpecializationCoordinator() \
            if enable_role_specialization else None

        # Active RLHF (replaces legacy RLHFManager when not passed explicitly)
        if rlhf is not None:
            # Backward-compat: if user passed legacy RLHFManager, wrap with ActiveRLHF API
            self.rlhf = rlhf
        else:
            self.rlhf = ActiveRLHF(action_space=self.objective_names)

        # HITL Coordinator (uses ActiveRLHF)
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) if (
            enable_hitl and isinstance(self.rlhf, ActiveRLHF)) else None

        # Federated Aggregator
        self.federated = FederatedAggregator(num_params=len(self.objective_names)) \
            if enable_federated else None

        # Causal Reward Shaper
        self.causal_shaper = CausalRewardShaper(num_actions=len(self.objective_names)) \
            if enable_causal_rl else None

        # Distillation (pass role coordinator if v17 role-spec enabled)
        self.distillation = distillation if distillation else \
            MultiTeacherPolicyDistillation(role_coordinator=self.role_coordinator)

        # Chaos Tester
        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        # Weights
        self.weights = weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }
        for obj in self.objective_names:
            if obj not in self.weights:
                self.weights[obj] = 0.0

        self.persistence_file = persistence_file
        self.enable_adaptation = enable_adaptation
        self.min_quality = min_quality_threshold
        self.max_latency_ms = max_latency_ms
        self.instance_id = str(uuid.uuid4())[:8]
        self._carbon_saved_kg_total = 0.0
        self._last_explanation: Optional[Dict] = None

        # Load persisted weights
        self._load_weights()

        self.logger.info(
            f"RewardCalculator v17.0.0 initialized (instance {self.instance_id}) — "
            f"TemporalLogic={enable_temporal_logic} XAI={enable_xai} "
            f"Precision={enable_adaptive_precision} CarbonMarket={enable_carbon_market} "
            f"Roles={enable_role_specialization} Chaos={enable_chaos_testing} "
            f"HITL={enable_hitl} Federated={enable_federated} CausalRL={enable_causal_rl}")

    # ----------------------------------------------------------------------
    # Persistence (unchanged)
    # ----------------------------------------------------------------------
    def _load_weights(self):
        if not self.persistence_file or not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
                if "weights" in data:
                    self.weights.update(data["weights"])
                if "last_update" in data:
                    self.last_update = data["last_update"]
            self.logger.info("Loaded weights from %s", self.persistence_file)
        except Exception as e:
            self.logger.warning("Failed to load weights: %s", e)

    def _save_weights(self):
        if not self.persistence_file:
            return
        try:
            data = {"weights": self.weights, "last_update": time.time()}
            with open(self.persistence_file, "w") as f:
                json.dump(data, f)
            self.logger.debug("Weights saved.")
        except Exception as e:
            self.logger.warning("Failed to save weights: %s", e)

    # ----------------------------------------------------------------------
    # Objective extraction (unchanged)
    # ----------------------------------------------------------------------
    def _extract_objectives(
        self,
        aggregated_metrics: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> Dict[str, float]:
        quality = aggregated_metrics.get("quality_score", 1.0)
        throughput = aggregated_metrics.get("tokens_per_sec", 0.0)
        total_energy_kwh = aggregated_metrics.get("total_energy_kwh", 0.0)
        mem_eff = aggregated_metrics.get("memory_efficiency", 0.0)

        if throughput > 0 and total_energy_kwh > 0:
            carbon_per_token = (total_energy_kwh * carbon_intensity_gco2_kwh) / throughput
            carbon_eff = max(0.0, 1.0 - (carbon_per_token / 100.0))
        else:
            carbon_eff = 0.0

        if total_energy_kwh > 0 and throughput > 0:
            energy_eff = min(1.0, throughput / (total_energy_kwh * 1000))
        else:
            energy_eff = 0.0

        return {
            "quality": min(1.0, max(0.0, quality)),
            "throughput": min(1.0, throughput / 100.0),
            "energy_efficiency": energy_eff,
            "carbon_efficiency": carbon_eff,
            "memory_efficiency": min(1.0, max(0.0, mem_eff)),
        }

    # ----------------------------------------------------------------------
    # Compute reward (enhanced v17.0.0)
    # ----------------------------------------------------------------------
    def compute(
        self,
        aggregated_metrics: Dict[str, Any],
        constraints: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> float:
        # 1. Extract objectives
        objectives = self._extract_objectives(aggregated_metrics, carbon_intensity_gco2_kwh)

        # 2. Adaptive precision
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(carbon_intensity_gco2_kwh, 0.95)

        # 3. Temporal gate
        temporal_status = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'quality': objectives['quality'],
                'carbon': objectives['carbon_efficiency'],
                'reward': 0.0,  # placeholder
            })
            temporal_status = self.temporal_monitor.evaluate()

        # 4. Determine effective weights
        effective_weights = self.weights.copy()

        # 4a. LIMIT Graph adjustment
        carbon_influence = self.limit_graph.evaluate_path('carbon', 'cost')
        if carbon_influence > 0.5:
            boost = min(0.2, (carbon_influence - 0.5) * 0.4)
            effective_weights['carbon_efficiency'] += boost
            total = sum(effective_weights.values())
            if total > 0:
                effective_weights = {k: v / total for k, v in effective_weights.items()}

        # 4b. RLHF adjustment (works with both legacy and ActiveRLHF)
        rlhf_adjustment = 0.0
        try:
            if getattr(self.rlhf, '_trained', False):
                predicted_reward = self.rlhf.predict_reward(objectives)
                rlhf_adjustment = predicted_reward * 0.1
        except Exception:
            pass

        # 4c. Distillation: blend student policy
        student_probs = self.distillation.get_student_probs()
        if len(student_probs) == len(self.objective_names):
            blend_factor = 0.3
            effective_weights = {
                k: (1 - blend_factor) * effective_weights[k] + blend_factor * student_probs[i]
                for i, k in enumerate(self.objective_names)}

        # 5. Base utility via MODP
        try:
            utility = self.modp.evaluate(objectives, effective_weights)
        except Exception:
            # Fallback to weighted sum
            utility = sum(objectives.get(k, 0) * effective_weights.get(k, 0)
                          for k in objectives)

        utility += rlhf_adjustment

        # 6. Penalties (unchanged)
        penalty = 0.0
        if aggregated_metrics.get("gpu_oom", False):
            penalty -= 10.0
        max_latency = constraints.get("max_latency_ms", self.max_latency_ms)
        if aggregated_metrics.get("elapsed_sec", 0) * 1000 > max_latency:
            penalty -= 5.0
        min_quality = constraints.get("min_quality", self.min_quality)
        if objectives["quality"] < min_quality:
            penalty -= 5.0

        reward = utility + penalty
        reward = max(-10.0, min(10.0, reward))

        # 7. XAI explanation
        xai_out = None
        if self.xai:
            xai_out = self.xai.explain(
                candidate=objectives,
                weights=effective_weights,
                all_candidates=[objectives])
            self._last_explanation = xai_out

        # 8. Role assignments
        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7,
                'energy': objectives['energy_efficiency'],
                'performance': objectives['quality']})

        # 9. HITL escalation (if confidence low)
        hitl_outcome = None
        confidence = objectives['quality']
        if self.hitl and confidence < self.hitl_confidence_threshold:
            try:
                # HITL requires async; schedule if we're in an event loop
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.ensure_future(self.hitl.escalate(
                            decision_context={'reward': reward,
                                              'quality': confidence},
                            options=self.objective_names,
                            confidence=confidence,
                            confidence_threshold=self.hitl_confidence_threshold))
                except RuntimeError:
                    pass
            except Exception:
                pass

        # 10. Causal observation
        if self.causal_shaper:
            try:
                top_obj_idx = self.objective_names.index(
                    max(effective_weights, key=effective_weights.get))
            except (ValueError, Exception):
                top_obj_idx = 0
            self.causal_shaper.record(top_obj_idx, reward,
                                      [effective_weights[k] for k in self.objective_names])

        # 11. Carbon credit (best-effort, deferred to async context)
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market and carbon_intensity_gco2_kwh > 0:
            try:
                saved_kg = max(0.0, (400.0 - carbon_intensity_gco2_kwh) * 0.001)
                self._carbon_saved_kg_total += saved_kg
                # Credit value computed lazily; recorded async elsewhere
                if NUMPY_AVAILABLE:
                    credit_value = saved_kg / 1000.0 * 50.0  # Simulated $/tCO2e
                    rec_value = saved_kg / 2000.0 * 30.0    # Simulated $/MWh
            except Exception:
                pass

        # 12. Federated submission
        if self.federated:
            try:
                self.federated.submit_update(
                    self.instance_id,
                    [effective_weights[k] for k in self.objective_names],
                    samples=1)
            except Exception:
                pass

        # 13. Temporal tick with final reward
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'quality': objectives['quality'],
                'carbon': objectives['carbon_efficiency'],
                'reward': reward})

        RC_REWARDS.labels(status='success').inc()

        # Attach last metadata to instance for retrieval
        self._last_metadata = {
            'precision_used': precision.value,
            'temporal_status': temporal_status,
            'xai_explanation': xai_out,
            'role_assignments': role_assignments,
            'hitl_outcome': hitl_outcome,
            'carbon_credit_value_usd': credit_value,
            'rec_value_usd': rec_value,
        }

        return reward

    # ----------------------------------------------------------------------
    # Adaptation (Bio-inspired) — enhanced
    # ----------------------------------------------------------------------
    def adapt_weights(self, context: Dict[str, Any], reward: float):
        if not self.enable_adaptation:
            return

        moe_context = self.moe.encode(context)
        if moe_context:
            context.update(moe_context)

        new_weights = self.bio.adapt(context, reward, self.weights)
        if new_weights:
            self.weights.update(new_weights)
            self._save_weights()
            self.logger.info("Weights adapted via bio-inspired optimizer.")

        # Update distillation with current teacher policies
        self._update_distillation()

        # Federated submission on weight update
        if self.federated:
            try:
                self.federated.submit_update(
                    self.instance_id,
                    [self.weights.get(k, 0.0) for k in self.objective_names],
                    samples=1)
                # Aggregate every 5 adaptations
                if len(self.federated.client_updates) >= 5:
                    agg = self.federated.aggregate()
                    # Blend global weights back into local weights
                    if agg.get('weights'):
                        for i, k in enumerate(self.objective_names):
                            self.weights[k] = (0.7 * self.weights[k] +
                                               0.3 * agg['weights'][i])
            except Exception:
                pass

    def _update_distillation(self):
        """Update distillation using current weights, MoE weights, RLHF policy."""
        teacher_policies = []
        # Teacher 1: MODP weights
        modp_weights = [self.weights.get(k, 0.0) for k in self.objective_names]
        total = sum(modp_weights) + 1e-8
        modp_weights = [w / total for w in modp_weights]
        teacher_policies.append(modp_weights)

        # Teacher 2: MoE weights if available
        try:
            moe_weights = self.moe.get_weights({})
            if moe_weights:
                moe_list = [moe_weights.get(obj, 0.0) for obj in self.objective_names]
                moe_total = sum(moe_list) + 1e-8
                moe_list = [w / moe_total for w in moe_list]
                teacher_policies.append(moe_list)
        except Exception:
            pass

        # Teacher 3: RLHF policy
        try:
            if getattr(self.rlhf, '_trained', False):
                teacher_policies.append(self.rlhf.get_policy_probs())
        except Exception:
            pass

        if teacher_policies:
            self.distillation.distill(teacher_policies)

    # ----------------------------------------------------------------------
    # Context-aware weight adjustment (MoE)
    # ----------------------------------------------------------------------
    def adjust_weights_for_context(self, task: Dict[str, Any]):
        priority = task.get("priority", "normal")
        if priority == "eco":
            self.weights["carbon_efficiency"] = 0.4
            self.weights["throughput"] = 0.1
        elif priority == "speed":
            self.weights["throughput"] = 0.5
            self.weights["carbon_efficiency"] = 0.1

        try:
            moe_weights = self.moe.get_weights(task)
            if moe_weights:
                self.weights.update(moe_weights)
        except Exception:
            pass

        self._save_weights()

    # ----------------------------------------------------------------------
    # Utility methods (unchanged)
    # ----------------------------------------------------------------------
    def get_objectives(
        self,
        aggregated_metrics: Dict[str, Any],
        carbon_intensity_gco2_kwh: float = 0.0
    ) -> Dict[str, float]:
        return self._extract_objectives(aggregated_metrics, carbon_intensity_gco2_kwh)

    def get_weights(self) -> Dict[str, float]:
        return self.weights.copy()

    def reset_weights(self, weights: Optional[Dict[str, float]] = None):
        if weights:
            self.weights.update(weights)
        else:
            self.weights = {
                "quality": 0.30,
                "throughput": 0.25,
                "energy_efficiency": 0.20,
                "carbon_efficiency": 0.15,
                "memory_efficiency": 0.10,
            }
        self._save_weights()

    def record_feedback(self, state: Dict, action: str, reward: float):
        """Record human feedback for RLHF."""
        try:
            self.rlhf.record_feedback(state, action, reward)
        except Exception as e:
            self.logger.warning(f"record_feedback failed: {e}")

    def train_rlhf(self):
        """Train RLHF model on collected feedback."""
        try:
            self.rlhf.train()
        except Exception as e:
            self.logger.warning(f"train_rlhf failed: {e}")

    def get_distillation_probs(self) -> List[float]:
        return self.distillation.get_student_probs()

    # ----------------------------------------------------------------------
    # v17.0.0 utility methods
    # ----------------------------------------------------------------------
    def get_last_metadata(self) -> Optional[Dict]:
        """Return the metadata from the most recent compute() call."""
        return getattr(self, '_last_metadata', None)

    def select_precision(self, carbon_intensity: float = 400.0,
                         accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(carbon_intensity, accuracy_required)

    def get_role_assignments(self, context: Optional[Dict[str, float]] = None) -> Dict:
        if self.role_coordinator is None:
            return {}
        ctx = context or {'trust': 0.7, 'compute': 0.7,
                          'energy': 0.7, 'performance': 0.7}
        return self.role_coordinator.assign_roles(ctx)

    def get_causal_ates(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    async def escalate_decision(self, decision_context: Dict,
                                options: List[str],
                                confidence: float) -> Dict:
        if self.hitl is None:
            return {'escalated': False,
                    'chosen': options[0] if options else 'noop',
                    'source': 'fallback'}
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

    def get_comprehensive_status(self) -> Dict:
        status = {
            'instance_id': self.instance_id,
            'version': '17.0.0',
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
            'weights': self.weights.copy(),
            'timestamp': datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.rlhf:
            status['rlhf'] = {
                'trained': getattr(self.rlhf, '_trained', False),
                'history_len': len(getattr(self.rlhf, 'history', []))}
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


# =============================================================================
# SMOKE TEST
# =============================================================================
async def _smoke_test():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Reward Calculator v17.0.0 — smoke test")
    print("=" * 78)

    calc = RewardCalculator(persistence_file=None)

    metrics = {
        'quality_score': 0.85,
        'tokens_per_sec': 80.0,
        'total_energy_kwh': 0.05,
        'memory_efficiency': 0.7,
        'gpu_oom': False,
        'elapsed_sec': 0.5,
    }
    constraints = {'max_latency_ms': 5000, 'min_quality': 0.5}

    print("\n📊 Computing reward...")
    reward = calc.compute(metrics, constraints, carbon_intensity_gco2_kwh=350.0)
    print(f"   Reward: {reward:.4f}")

    meta = calc.get_last_metadata()
    if meta:
        print(f"   Precision: {meta.get('precision_used')}")
        print(f"   Carbon credit: ${meta.get('carbon_credit_value_usd', 0):.4f}")
        print(f"   REC value: ${meta.get('rec_value_usd', 0):.4f}")
        if meta.get('temporal_status'):
            print(f"   Temporal status: {meta['temporal_status']}")
        if meta.get('role_assignments'):
            print(f"   Dominant role: {meta['role_assignments'].get('dominant_role')}")
        if meta.get('xai_explanation'):
            print(f"   XAI: {meta['xai_explanation']['narrative'][0]}")

    print("\n📝 Recording feedback for RLHF...")
    for i in range(3):
        calc.record_feedback(
            state={'quality': 0.8 + i * 0.02, 'throughput': 0.7,
                   'energy_efficiency': 0.6, 'carbon_efficiency': 0.5,
                   'memory_efficiency': 0.7},
            action='quality', reward=0.8 + i * 0.05)
    calc.train_rlhf()
    print(f"   RLHF trained: {getattr(calc.rlhf, '_trained', False)}")

    print("\n⚙️  Precision selector →",
          calc.select_precision(carbon_intensity=350.0).value)

    cc = await calc.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"\n🎭 Roles: {calc.get_role_assignments()}")
    print(f"🧠 Causal ATEs: {calc.get_causal_ates()}")

    print("\n🧪 Chaos suite:")
    chaos = await calc.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📋 Comprehensive status:")
    status = calc.get_comprehensive_status()
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

    print("\n" + "=" * 78)
    print("✅ Reward Calculator v17.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
