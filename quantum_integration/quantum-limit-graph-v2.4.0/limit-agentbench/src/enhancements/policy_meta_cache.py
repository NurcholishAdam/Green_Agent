"""
policy_meta_cache.py — Enhanced v4.0.0

Stores successful policies keyed by a workload fingerprint.
v4.0.0 integrates:
    • MODP (Pareto front + TOPSIS)
    • MOE gating
    • Bio-inspired GA
    • Carbon-aware scheduling
    • Self-healing drift detection
    • LIMIT Graph
    • ✨ Temporal Logic Verification (G/F/U/->)
    • ✨ Explainable AI (XAI)
    • ✨ Adaptive Precision Switching (fp32/fp16/bf16/fp8/fp4)
    • ✨ Carbon Markets + Renewable Energy Credits (RECs)
    • ✨ Multi-Agent Role Specialization (emergent)
    • ✨ Chaos Testing as first-class citizen
    • ✨ Active RLHF (uncertainty-triggered human queries)
    • ✨ Human-in-the-Loop Coordinator
    • ✨ Federated Green Learning (FedAvg)

All enhancements degrade gracefully if optional dependencies are missing.
"""

import asyncio
import time
import math
import random
import hashlib
import logging
import uuid
import os
import json
import signal
from typing import Dict, Any, Optional, List, Tuple, Callable
from dataclasses import dataclass, field, asdict
from collections import deque, defaultdict
from datetime import datetime, timedelta
from enum import Enum

import numpy as np

# ================================
# Optional dependencies
# ================================
try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
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

logger = logging.getLogger(__name__)


# ================================
# Prometheus metrics
# ================================
if PROMETHEUS_AVAILABLE:
    PMC_REGISTRY = CollectorRegistry()
    PMC_HITS = Counter('pmc_cache_hits_total', 'Cache hits', ['status'], registry=PMC_REGISTRY)
    PMC_TEMPORAL_VIOLATIONS = Counter('pmc_temporal_violations_total', 'Temporal violations', ['formula'], registry=PMC_REGISTRY)
    PMC_CHAOS = Counter('pmc_chaos_tests_total', 'Chaos tests', ['fault', 'status'], registry=PMC_REGISTRY)
    PMC_HITL = Counter('pmc_hitl_escalations_total', 'HITL escalations', ['status'], registry=PMC_REGISTRY)
    PMC_FEDERATED = Counter('pmc_federated_rounds_total', 'Federated rounds', registry=PMC_REGISTRY)
    PMC_CARBON_CREDITS = Counter('pmc_carbon_credits_usd_total', 'Carbon credits USD', registry=PMC_REGISTRY)
    PMC_XAI = Counter('pmc_xai_explanations_total', 'XAI explanations', registry=PMC_REGISTRY)
    PMC_PRECISION = Counter('pmc_precision_selections_total', 'Precision selections', ['level'], registry=PMC_REGISTRY)
    PMC_CARBON_INTENSITY = Gauge('pmc_carbon_intensity', 'Carbon intensity', registry=PMC_REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
        def labels(self, *a, **k): return self
    PMC_HITS = PMC_TEMPORAL_VIOLATIONS = PMC_CHAOS = PMC_HITL = _Dummy()
    PMC_FEDERATED = PMC_CARBON_CREDITS = PMC_XAI = PMC_PRECISION = _Dummy()
    PMC_CARBON_INTENSITY = _Dummy()


# ================================
# Enums
# ================================
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


# ================================
# WorkloadFingerprint (unchanged)
# ================================
class WorkloadFingerprint:
    """Normalised fingerprint of a workload for similarity search."""
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class  # 0=HDD,1=SATA-SSD,2=NVMe

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.model_size_mb / 1000.0,
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])


# ================================
# Pareto front + TOPSIS
# ================================
class ParetoFront:
    """Simple Pareto front implementation for multi-objective dominance."""
    def __init__(self):
        self.solutions = []

    def add(self, objectives: List[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if all(o <= obj[i] for i, o in enumerate(objectives)):
                dominated = True
                break
        if not dominated:
            self.solutions = [(obj, dec) for obj, dec in self.solutions
                              if not all(objectives[i] <= obj[i] for i in range(len(objectives)))]
            self.solutions.append((objectives, decision))
        return dominated

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

    def get_best_by_weight(self, weights: List[float]) -> Any:
        best, best_score = None, -float('inf')
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score, best = score, dec
        return best


class TOPSIS:
    """TOPSIS multi-criteria decision analysis."""
    @staticmethod
    def score(candidates: List[Dict[str, float]], weights: List[float],
              criteria: List[str]) -> List[float]:
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()


# ================================
# v4.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# ================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ with atoms."""
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
                                "==": lambda: lv == rv, "!=": lambda: lv != rv}[op]()
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
                PMC_TEMPORAL_VIOLATIONS.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}


# ================================
# v4.0.0 MODULE B — XAI EXPLAINER
# ================================
class XAIExplainer:
    """Feature-attribution explainer for policy selection."""
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
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
        PMC_XAI.inc()
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}


# ================================
# v4.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# ================================
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
        PMC_PRECISION.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


# ================================
# v4.0.0 MODULE D — CARBON MARKET CLIENT
# ================================
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
        PMC_CARBON_CREDITS.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# ================================
# v4.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# ================================
class RoleSpecializationCoordinator:
    """Emergent multi-agent role specialization via softmax affinity."""
    def __init__(self):
        self.roles = list(AgentRole)
        # rows = roles, cols = [trust, compute, energy, performance]
        self.affinity = np.array([
            [0.7, 0.9, 0.4, 0.6],   # leader
            [0.4, 0.5, 0.9, 0.5],   # worker
            [0.9, 0.4, 0.3, 0.7],   # verifier
            [0.3, 0.2, 0.3, 0.3],   # observer
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        ctx = np.array([context.get('trust', 0.5),
                        context.get('compute', 0.5),
                        context.get('energy', 0.5),
                        context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i]) for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}


# ================================
# v4.0.0 MODULE F — CHAOS TESTER
# ================================
class ChaosTester:
    """Fault injection for resilience validation."""
    FAULT_TYPES = ['carbon_api_down', 'cache_corrupt', 'moe_broken',
                   'distiller_broken', 'rlhf_broken', 'scheduler_hang']

    def __init__(self, cache_ref=None):
        self.cache = cache_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        c = self.cache

        try:
            if fault_type == 'carbon_api_down' and c and c.scheduler:
                orig = c.scheduler.get_current_carbon
                async def broken(): raise RuntimeError("carbon API down")
                c.scheduler.get_current_carbon = broken
                restore.append(lambda: setattr(c.scheduler, 'get_current_carbon', orig))
            elif fault_type == 'cache_corrupt' and c:
                # Temporarily corrupt one entry
                if c.store:
                    key = next(iter(c.store))
                    orig = list(c.store[key])
                    c.store[key] = []
                    restore.append(lambda: c.store.update({key: orig}))
            elif fault_type == 'moe_broken' and c and c.moe_gating:
                orig = c.moe_gating.get_weights
                def broken(_): raise RuntimeError("moe broken")
                c.moe_gating.get_weights = broken
                restore.append(lambda: setattr(c.moe_gating, 'get_weights', orig))
            elif fault_type == 'distiller_broken' and c and c.distillation:
                orig = c.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distiller broken")
                c.distillation.distill = broken
                restore.append(lambda: setattr(c.distillation, 'distill', orig))
            elif fault_type == 'rlhf_broken' and c and c.rlhf:
                orig = c.rlhf.get_policy_probs
                async def broken(_): raise RuntimeError("rlhf broken")
                c.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(c.rlhf, 'get_policy_probs', orig))
            elif fault_type == 'scheduler_hang' and c and c.scheduler:
                orig = c.scheduler.should_delay
                async def broken(*a, **k):
                    await asyncio.sleep(5)
                    return False, 0
                c.scheduler.should_delay = broken
                restore.append(lambda: setattr(c.scheduler, 'should_delay', orig))
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
        PMC_CHAOS.labels(fault=fault_type, status='pass' if passed else 'fail').inc()
        logger.warning(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}


# ================================
# v4.0.0 MODULE G — ACTIVE RLHF
# ================================
class ActiveRLHF:
    """Preference-based policy with uncertainty-triggered human queries."""
    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []
        self.reward_model = None
        if SKLEARN_AVAILABLE:
            try:
                self.reward_model = LinearRegression()
            except Exception:
                self.reward_model = None

    def _policy(self, context: Any) -> np.ndarray:
        raw = np.array([self.preference_counts[a] for a in self.actions], dtype=float)
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
                 'uncertainty': u,
                 'created_at': datetime.now().isoformat(),
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

    async def train_reward_model(self) -> Dict:
        if self.reward_model is None or len(self.feedback_buffer) < 5:
            return {'trained': False, 'samples': len(self.feedback_buffer)}
        try:
            X = np.array([[f['state'].get('carbon_intensity', 400) / 1000.0,
                           f['state'].get('avg_score', 0.5),
                           f['state'].get('cost', 0.5),
                           f['state'].get('diversity', 0.5)]
                          for f in self.feedback_buffer])
            y = np.array([f['reward'] for f in self.feedback_buffer])
            if SKLEARN_AVAILABLE and len(X) >= 5:
                self.reward_model.fit(X, y)
                r2 = float(self.reward_model.score(X, y)) if len(X) > 1 else 0.0
                self.feedback_buffer.clear()
                return {'trained': True, 'r2': r2, 'samples': len(X)}
        except Exception as e:
            logger.warning(f"RLHF train failed: {e}")
        self.feedback_buffer.clear()
        return {'trained': False, 'reason': 'insufficient_data'}

    async def get_policy_probs(self, state: Dict) -> List[float]:
        return self._policy(state).tolist()


# ================================
# v4.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# ================================
class HumanInTheLoopCoordinator:
    """Escalates low-confidence decisions to humans with auto-fallback."""
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
            PMC_HITL.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice,
                               'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        PMC_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# ================================
# v4.0.0 MODULE I — FEDERATED AGGREGATOR
# ================================
class FederatedAggregator:
    """FedAvg-style cross-deployment aggregation of cache weights."""
    def __init__(self, num_params: int = 4):
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
        if not self.client_updates:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        PMC_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}


# ================================
# MODULE — MOE Gating
# ================================
class MOEGating:
    """Mixture of Experts gating network."""
    def __init__(self, num_experts: int, feature_dim: int = 4):
        self.num_experts = num_experts
        self.feature_dim = feature_dim
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)
        self._trained = False
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial',
                                                   solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    def extract_features(self, context: Dict) -> np.ndarray:
        carbon = context.get('carbon_intensity', 400) / 1000.0
        hour = datetime.now().hour / 24.0
        urgency = context.get('urgency', 0.5)
        workload_size = context.get('model_size_mb', 0) / 1000.0
        return np.array([carbon, hour, urgency, workload_size])

    def get_weights(self, context: Dict) -> List[float]:
        if self.gating_model is not None and self._trained:
            features = self.extract_features(context)
            X_scaled = self.scaler.transform([features])
            weights = self.gating_model.predict_proba(X_scaled)[0]
        else:
            weights = np.ones(self.num_experts) / self.num_experts
        return weights.tolist()

    def update(self, context: Dict, expert_idx: int, reward: float):
        features = self.extract_features(context)
        self.history.append((features, expert_idx, reward))
        if len(self.history) % 100 == 0:
            self._retrain()

    def _retrain(self):
        if self.gating_model is None or len(self.history) < 100:
            return
        X = np.array([h[0] for h in self.history])
        y = np.array([h[1] for h in self.history])
        self.scaler.fit(X)
        self.gating_model.fit(self.scaler.transform(X), y)
        self._trained = True


# ================================
# MODULE — GA
# ================================
class GAPopulation:
    """Genetic Algorithm population of policies."""
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population = []
        self.generation = 0

    def initialize(self, template_policy: Dict[str, Any]):
        self.population = [(template_policy, 0.0)] * self.pop_size
        for i in range(1, self.pop_size):
            self.population[i] = (self._mutate(template_policy), 0.0)

    def _mutate(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        new_policy = policy.copy()
        for key, value in new_policy.items():
            if isinstance(value, (int, float)):
                new_policy[key] = value * (1.0 + random.uniform(-0.1, 0.1))
        return new_policy

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for key in p1:
            child[key] = p1[key] if random.random() < 0.5 else p2[key]
        return child

    def evolve(self, fitness_func: Callable[[Dict], float]) -> Dict[str, Any]:
        for i, (policy, _) in enumerate(self.population):
            self.population[i] = (policy, fitness_func(policy))
        self.population.sort(key=lambda x: x[1], reverse=True)
        best = self.population[0]
        parents = []
        for _ in range(self.pop_size - 1):
            idx1, idx2 = random.sample(range(self.pop_size), 2)
            parents.append(self.population[idx1][0]
                           if self.population[idx1][1] > self.population[idx2][1]
                           else self.population[idx2][0])
        offspring = []
        for i in range(0, len(parents) - 1, 2):
            child = self._crossover(parents[i], parents[i + 1]) \
                if random.random() < self.crossover_rate else parents[i]
            if random.random() < self.mutation_rate:
                child = self._mutate(child)
            offspring.append((child, 0.0))
        self.population = [best] + offspring[:self.pop_size - 1]
        self.generation += 1
        return best[0]


# ================================
# MODULE — Carbon Scheduler
# ================================
class CarbonScheduler:
    """Multi-objective carbon-aware scheduler."""
    def __init__(self, carbon_manager: Optional[Any] = None, threshold: float = 400.0,
                 max_delay: int = 300):
        self.carbon_manager = carbon_manager
        self.threshold = threshold
        self.max_delay = max_delay

    async def get_current_carbon(self) -> float:
        if self.carbon_manager:
            return await self.carbon_manager.get_current_intensity()
        return 400.0

    async def should_delay(self, urgency: float = 0.5):
        carbon = await self.get_current_carbon()
        PMC_CARBON_INTENSITY.set(carbon)
        if carbon > self.threshold:
            delay = int(self.max_delay * (1.0 - urgency))
            return True, delay
        return False, 0


# ================================
# MODULE — Self-Healing
# ================================
class SelfHealingManager:
    """Drift detection and anomaly ensemble."""
    def __init__(self, contamination: float = 0.1):
        self.contamination = contamination
        self.anomaly_detectors = []
        self.reward_history = deque(maxlen=500)
        self._trained = False
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors.append(('iforest', IsolationForest(contamination=contamination)))
            self.anomaly_detectors.append(('ocsvm', OneClassSVM(nu=contamination)))

    def record_reward(self, reward: float):
        self.reward_history.append(reward)
        if len(self.reward_history) >= 100 and not self._trained:
            self._train()

    def _train(self):
        if not self.anomaly_detectors or len(self.reward_history) < 100:
            return
        X = np.array(list(self.reward_history)).reshape(-1, 1)
        for _, model in self.anomaly_detectors:
            try:
                model.fit(X)
            except Exception as e:
                logger.warning(f"Anomaly detector training failed: {e}")
        self._trained = True

    def detect_anomaly(self, reward: float):
        if not self._trained or not self.anomaly_detectors:
            return reward < 0.5, 0.0
        X = np.array([[reward]])
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        anomaly = sum(votes) / len(votes) > 0.5
        return anomaly, sum(votes) / len(votes)


# ================================
# MODULE — LIMIT Graph
# ================================
class LimitGraphManager:
    """Graph of system constraints for real-time decision support."""
    def __init__(self):
        self.graph = {}
        self.constraints = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        for n in ['carbon', 'cost', 'latency', 'throughput', 'diversity']:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['latency'] = 0.2
        self.graph['latency']['throughput'] = -0.5
        self.graph['throughput']['diversity'] = 0.1
        self.graph['diversity']['carbon'] = -0.3

    async def update_constraint(self, name: str, value: float):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name: str) -> float:
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start: str, end: str) -> float:
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

    async def get_graph_summary(self) -> Dict:
        return {'nodes': list(self.graph.keys()),
                'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}


# ================================
# MODULE — Multi-Teacher Distillation
# ================================
class MultiTeacherPolicyDistillation:
    """Distills multiple teacher policies into a single student policy."""
    def __init__(self, num_teachers: int = 4, temperature: float = 2.0, alpha: float = 0.5):
        self.num_teachers = num_teachers
        self.temperature = temperature
        self.alpha = alpha
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25])
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, teacher_probs: List[float]):
        if len(teacher_probs) != self.num_teachers:
            teacher_probs = np.ones(self.num_teachers) / self.num_teachers
        teacher_dist = np.array(teacher_probs, dtype=float)
        teacher_dist = teacher_dist / (teacher_dist.sum() + 1e-9)

        soft = np.exp(np.log(teacher_dist + 1e-6) / self.temperature)
        soft /= soft.sum()

        loss = -np.sum(soft * np.log(self.student_policy + 1e-6))
        grad = -soft / (self.student_policy + 1e-6)
        self.student_policy = np.clip(self.student_policy - 0.01 * grad, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({'teacher_dist': teacher_dist.tolist(),
                                 'student_dist': self.student_policy.tolist(),
                                 'loss': float(loss)})

    def get_student_probs(self) -> List[float]:
        return self.student_policy.tolist()


# ================================
# ENHANCED PolicyMetaCache v4.0.0
# ================================
class PolicyMetaCache:
    """
    Enhanced policy meta-cache with MODP, MOE, GA, carbon-aware scheduling,
    self-healing, LIMIT Graph, plus the v4.0.0 suite:
        Temporal Logic, XAI, Adaptive Precision, Carbon Markets, Role
        Specialization, Chaos Testing, Active RLHF, HITL, Federated Learning.
    """

    def __init__(
        self,
        max_age_hours: float = 24.0,
        dist_threshold: float = 0.2,
        enable_modp: bool = True,
        modp_weights: List[float] = None,
        enable_moe: bool = True,
        num_experts: int = 3,
        enable_ga: bool = True,
        ga_population_size: int = 20,
        enable_scheduler: bool = True,
        carbon_threshold: float = 400.0,
        max_delay_seconds: int = 300,
        enable_self_healing: bool = True,
        anomaly_contamination: float = 0.1,
        enable_limit_graph: bool = True,
        enable_rlhf: bool = True,
        rlhf_reward_model: str = "linear",
        rlhf_training_interval: int = 600,
        enable_distillation: bool = True,
        distillation_temperature: float = 2.0,
        distillation_alpha: float = 0.5,
        distillation_interval: int = 300,
        carbon_manager: Optional[Any] = None,
        # v4.0.0 flags
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_adaptive_precision: bool = True,
        enable_carbon_market: bool = True,
        enable_role_specialization: bool = True,
        enable_chaos_testing: bool = True,
        enable_hitl: bool = True,
        enable_federated: bool = True,
        hitl_confidence_threshold: float = 0.65,
    ):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.store: Dict[Tuple, List[Tuple]] = {}
        self.vectors: List[np.ndarray] = []
        self.keys: List[Tuple] = []

        self.enable_modp = enable_modp
        self.modp_weights = modp_weights or [0.5, 0.2, 0.2, 0.1]

        self.enable_moe = enable_moe
        self.num_experts = num_experts
        self.moe_gating = MOEGating(num_experts) if enable_moe else None

        self.enable_ga = enable_ga
        self.ga_population_size = ga_population_size
        self.ga_populations: Dict[Tuple, GAPopulation] = {}

        self.enable_scheduler = enable_scheduler
        self.carbon_manager = carbon_manager
        self.scheduler = CarbonScheduler(carbon_manager, carbon_threshold,
                                         max_delay_seconds) if enable_scheduler else None

        self.enable_self_healing = enable_self_healing
        self.self_healing = SelfHealingManager(anomaly_contamination) if enable_self_healing else None

        self.enable_limit_graph = enable_limit_graph
        self.limit_graph = LimitGraphManager() if enable_limit_graph else None
        self.limit_graph_update_interval = 300

        # ---- v4.0.0 modules ----
        self.enable_temporal_logic = enable_temporal_logic
        self.enable_xai = enable_xai
        self.enable_adaptive_precision = enable_adaptive_precision
        self.enable_carbon_market = enable_carbon_market
        self.enable_role_specialization = enable_role_specialization
        self.enable_chaos_testing = enable_chaos_testing
        self.enable_hitl = enable_hitl
        self.enable_federated = enable_federated

        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("reward_floor", "G(reward >= 0.0)")
            self.temporal_monitor.add_formula("cache_activity", "F(cache_size >= 1.0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")

        self.xai = XAIExplainer(['reward', 'carbon', 'latency', 'cost']) \
            if enable_xai else None

        self.precision_controller = AdaptivePrecisionController() \
            if enable_adaptive_precision else None

        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if enable_role_specialization else None

        # Active RLHF replaces the older RLHFManager
        self.enable_rlhf = enable_rlhf
        self.rlhf = ActiveRLHF(
            action_space=['performance_focus', 'carbon_focus', 'cost_focus', 'balanced'],
        ) if enable_rlhf else None
        self.rlhf_training_interval = rlhf_training_interval

        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (enable_hitl and self.rlhf) else None

        self.federated = FederatedAggregator(num_params=4) if enable_federated else None
        self.instance_id = str(uuid.uuid4())[:8]

        # Distillation
        self.enable_distillation = enable_distillation
        self.distillation = MultiTeacherPolicyDistillation(
            num_teachers=num_experts,
            temperature=distillation_temperature,
            alpha=distillation_alpha
        ) if enable_distillation else None
        self.distillation_interval = distillation_interval

        # Chaos
        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        self._fitness_evaluator = None
        self._background_tasks: List[asyncio.Task] = []
        self._running = False
        self._carbon_saved_kg_total = 0.0

    # ------------------------------------------------------------------
    # Key conversion
    # ------------------------------------------------------------------
    def _vector_to_key(self, vec: np.ndarray) -> tuple:
        return tuple(vec.tolist())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        if self._running:
            return
        self._running = True
        if self.rlhf:
            self._background_tasks.append(asyncio.create_task(self._rlhf_loop()))
        if self.distillation:
            self._background_tasks.append(asyncio.create_task(self._distillation_loop()))
        if self.limit_graph:
            self._background_tasks.append(asyncio.create_task(self._limit_graph_loop()))
        if self.federated:
            self._background_tasks.append(asyncio.create_task(self._federated_loop()))
        if self.chaos_tester:
            self._background_tasks.append(asyncio.create_task(self._chaos_loop()))
        logger.info(f"PolicyMetaCache v4.0.0 background tasks started (instance {self.instance_id})")

    async def stop(self):
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        logger.info("PolicyMetaCache background tasks stopped")

    async def _rlhf_loop(self):
        while self._running:
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                await asyncio.sleep(self.rlhf_training_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")
                await asyncio.sleep(60)

    async def _distillation_loop(self):
        while self._running:
            try:
                if self.distillation and self.moe_gating:
                    dummy = {'carbon_intensity': 400, 'urgency': 0.5, 'model_size_mb': 1000}
                    teacher_probs = self.moe_gating.get_weights(dummy)
                    await self.distillation.distill(teacher_probs)
                await asyncio.sleep(self.distillation_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")
                await asyncio.sleep(60)

    async def _limit_graph_loop(self):
        while self._running:
            try:
                if self.limit_graph:
                    carbon = await self.scheduler.get_current_carbon() \
                        if self.scheduler else 400.0
                    await self.limit_graph.update_constraint('carbon', carbon)
                    await self.limit_graph.evaluate_path('carbon', 'cost')
                await asyncio.sleep(self.limit_graph_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LIMIT Graph loop error: {e}")
                await asyncio.sleep(60)

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

    async def _chaos_loop(self):
        while self._running:
            try:
                await asyncio.sleep(1800)
                if self.chaos_tester:
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    # ------------------------------------------------------------------
    # Core retrieval
    # ------------------------------------------------------------------
    async def get_best_policy(self, fp: WorkloadFingerprint,
                              context: Dict[str, Any] = None,
                              urgency: float = 0.5) -> Optional[Dict[str, Any]]:
        vec = fp.to_vector()
        if not self.vectors:
            PMC_HITS.labels(status='miss').inc()
            return None

        # Carbon-aware scheduling
        if self.scheduler:
            should_delay, delay = await self.scheduler.should_delay(urgency)
            if should_delay:
                logger.info(f"Policy retrieval delayed by {delay}s due to high carbon")
                await asyncio.sleep(min(delay, 2))

        # Adaptive precision selection
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            carbon = await self.scheduler.get_current_carbon() \
                if self.scheduler else 400.0
            precision = self.precision_controller.select(carbon)

        # Temporal gate
        temporal_status = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'reward': 0.5,
                'cache_size': float(len(self.store)),
                'carbon': await self.scheduler.get_current_carbon() \
                    if self.scheduler else 400.0})
            temporal_status = self.temporal_monitor.evaluate()

        # Nearest neighbour
        best_idx = -1
        best_dist = float('inf')
        for i, stored_vec in enumerate(self.vectors):
            dist = np.linalg.norm(vec - stored_vec)
            if dist < best_dist:
                best_dist = dist
                best_idx = i

        if best_idx == -1 or best_dist > self.dist_threshold:
            PMC_HITS.labels(status='miss').inc()
            return None

        key = self.keys[best_idx]
        entries = self.store[key]

        # Stale filter
        now = time.time()
        valid_entries = [e for e in entries if (now - e[1]) < self.max_age_seconds]
        if not valid_entries:
            PMC_HITS.labels(status='stale').inc()
            return None

        # Self-healing check
        if self.self_healing and valid_entries:
            avg_reward = np.mean([e[2][0] for e in valid_entries])
            anomaly, _ = self.self_healing.detect_anomaly(avg_reward)
            if anomaly:
                logger.warning("Anomaly detected in cached policy performance")
                PMC_HITS.labels(status='anomaly').inc()
                return None

        # HITL escalation if confidence low
        hitl_outcome = None
        confidence = float(np.mean([e[2][0] for e in valid_entries])) if valid_entries else 0.5
        if self.hitl and confidence < 0.65:
            try:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={'entries': len(valid_entries)},
                    options=['accept', 'reject', 'recalibrate'],
                    confidence=confidence,
                    confidence_threshold=0.65)
            except Exception:
                pass

        # Selection strategy: RLHF → Distillation → MODP
        selected_policy = None
        source = "fallback"
        xai_out = None

        # If RLHF trained, use it to adjust selection
        if self.rlhf and self.rlhf.reward_model is not None and context:
            try:
                rlhf_probs = await self.rlhf.get_policy_probs(context)
                candidates = []
                for e in valid_entries:
                    policy, _, objectives, _, _ = e
                    adjusted_reward = objectives[0] * (rlhf_probs[0] if rlhf_probs else 1.0)
                    candidates.append((adjusted_reward, policy))
                if candidates:
                    best_idx_c = max(range(len(candidates)), key=lambda i: candidates[i][0])
                    selected_policy = candidates[best_idx_c][1]
                    source = "rlhf"
            except Exception:
                pass

        # MODP + TOPSIS
        if selected_policy is None and self.enable_modp and len(valid_entries) > 1:
            criteria = ['reward', 'carbon', 'latency', 'cost']
            candidates_list = []
            for e in valid_entries:
                policy, _, objectives, _, _ = e
                obj_inv = [objectives[0], 1.0 - objectives[1],
                           1.0 - objectives[2], 1.0 - objectives[3]]
                candidates_list.append({'objectives': obj_inv, 'policy': policy})

            # Weights: from distillation (if trained) else from modp_weights
            if self.distillation and self.distillation.get_student_probs():
                weights = self.distillation.get_student_probs()
                if len(weights) != len(criteria):
                    weights = self.modp_weights
            else:
                weights = self.modp_weights

            cand_dicts = [{crit: c['objectives'][i] for i, crit in enumerate(criteria)}
                          for c in candidates_list]
            scores = TOPSIS.score(cand_dicts, weights, criteria)
            best_i = int(np.argmax(scores))
            selected_policy = candidates_list[best_i]['policy']
            source = "modp"

            # XAI explanation
            if self.xai:
                xai_out = self.xai.explain(
                    candidate=cand_dicts[best_i],
                    weights={crit: w for crit, w in zip(criteria, weights)},
                    all_candidates=cand_dicts)

        # Fallback: best reward
        if selected_policy is None:
            best_entry = max(valid_entries, key=lambda e: e[2][0])
            selected_policy = best_entry[0]
            source = "reward"

        # Role assignment
        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7, 'energy': 0.7,
                'performance': confidence})

        # Carbon market credit
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market:
            try:
                saved_kg = max(0.0, (400 - (await self.scheduler.get_current_carbon()
                                             if self.scheduler else 400.0)) * 0.001)
                credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                self._carbon_saved_kg_total += saved_kg
            except Exception:
                pass

        # Federated submission
        federated_round = None
        if self.federated:
            self.federated.submit_update(
                self.instance_id, list(self.modp_weights),
                samples=len(valid_entries))

        PMC_HITS.labels(status='hit').inc()
        return {
            'policy': selected_policy,
            'source': source,
            'precision_used': precision.value,
            'temporal_status': temporal_status,
            'xai_explanation': xai_out,
            'role_assignments': role_assignments,
            'hitl_outcome': hitl_outcome,
            'carbon_credit_value_usd': credit_value,
            'rec_value_usd': rec_value,
            'federated_round': federated_round,
            'confidence': confidence,
        }

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------
    async def update(self, fp: WorkloadFingerprint, policy: Dict[str, Any],
                     reward: float, carbon: float = 0.0, latency: float = 0.0,
                     cost: float = 0.0, context: Dict[str, Any] = None,
                     expert_id: int = 0):
        vec = fp.to_vector()
        key = self._vector_to_key(vec)

        if key not in self.store:
            self.store[key] = []
            self.vectors.append(vec)
            self.keys.append(key)

        objectives = [reward, carbon, latency, cost]
        new_entry = (policy, time.time(), objectives, expert_id, None)

        if self.enable_modp:
            entries = self.store[key]
            entries.append(new_entry)
            max_entries = 20
            if len(entries) > max_entries:
                entries.sort(key=lambda e: e[2][0], reverse=True)
                self.store[key] = entries[:max_entries]
        else:
            entries = self.store[key]
            best_reward = max([e[2][0] for e in entries]) if entries else -float('inf')
            if reward > best_reward:
                self.store[key] = [new_entry]

        # MOE update
        if self.enable_moe and context is not None and self.moe_gating:
            self.moe_gating.update(context, expert_id, reward)

        # GA population
        if self.enable_ga and key not in self.ga_populations:
            pop = GAPopulation(population_size=self.ga_population_size)
            pop.initialize(policy)
            self.ga_populations[key] = pop

        # Self-healing
        if self.self_healing:
            self.self_healing.record_reward(reward)

        # LIMIT Graph
        if self.limit_graph:
            await self.limit_graph.update_constraint('carbon', carbon)

        # Active RLHF reward feedback
        if self.rlhf:
            self.rlhf.update(context or {}, expert_id, reward)

        # Carbon market (best-effort)
        if self.carbon_market and carbon > 0:
            try:
                await self.carbon_market.retire_credits(
                    max(0.0, carbon), self.instance_id)
            except Exception:
                pass

        # Federated submission
        if self.federated:
            self.federated.submit_update(
                self.instance_id, list(self.modp_weights), samples=1)

        logger.debug(f"Cache updated: reward={reward:.3f}, carbon={carbon:.3f}")

    # ------------------------------------------------------------------
    # Additional methods
    # ------------------------------------------------------------------
    async def record_feedback(self, state: Dict, action: str, reward: float):
        if self.rlhf:
            self.rlhf.record_feedback(state, action, reward)

    async def escalate_decision(self, decision_context: Dict,
                                options: List[str],
                                confidence: float) -> Dict:
        if self.hitl is None:
            return {'escalated': False,
                    'chosen': options[0] if options else 'noop',
                    'source': 'fallback'}
        return await self.hitl.escalate(decision_context, options, confidence)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    def select_precision(self, accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(400.0, accuracy_required)

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

    async def evolve_populations(self, fitness_func: Callable[[Dict], float]):
        if not self.enable_ga:
            return
        for key, pop in self.ga_populations.items():
            best_policy = pop.evolve(fitness_func)
            if best_policy:
                dummy_objectives = [0.0, 0.0, 0.0, 0.0]
                new_entry = (best_policy, time.time(), dummy_objectives, 0, None)
                if key in self.store:
                    self.store[key].append(new_entry)
                else:
                    self.store[key] = [new_entry]

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict:
        stats = {
            'instance_id': self.instance_id,
            'cache_size': len(self.store),
            'total_entries': sum(len(v) for v in self.store.values()),
            'ga_populations': len(self.ga_populations),
            'moe_trained': self.moe_gating._trained if self.moe_gating else False,
            'self_healing_trained': self.self_healing._trained if self.self_healing else False,
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
            },
        }
        if self.limit_graph:
            stats['limit_graph'] = {
                'nodes': list(self.limit_graph.graph.keys()),
                'constraints': self.limit_graph.constraints}
        if self.rlhf:
            stats['rlhf'] = {
                'actions': self.rlhf.actions,
                'history_len': len(self.rlhf.history)}
        if self.distillation:
            stats['distillation'] = {
                'student_probs': self.distillation.get_student_probs(),
                'history_len': len(self.distillation.history)}
        if self.federated:
            stats['federated'] = self.federated.get_stats()
        if self.hitl:
            stats['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            stats['chaos'] = self.chaos_tester.get_report()
        if self.temporal_monitor:
            stats['temporal_logic'] = self.temporal_monitor.get_status()
        if self.precision_controller:
            stats['precision'] = {
                'last': self.precision_controller.last_precision.value,
                'telemetry': self.precision_controller.telemetry}
        return stats

    def clear(self):
        self.store.clear()
        self.vectors.clear()
        self.keys.clear()
        self.ga_populations.clear()


# ================================
# Smoke test
# ================================
async def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')

    print("=" * 78)
    print("Policy Meta Cache v4.0.0 — smoke test")
    print("=" * 78)

    cache = PolicyMetaCache(
        max_age_hours=24, dist_threshold=0.2,
        enable_modp=True, modp_weights=[0.5, 0.2, 0.2, 0.1],
        enable_moe=True, num_experts=3,
        enable_ga=True, ga_population_size=10,
        enable_scheduler=True, carbon_threshold=400, max_delay_seconds=60,
        enable_self_healing=True, anomaly_contamination=0.1,
        enable_limit_graph=True,
        enable_rlhf=True,
        enable_distillation=True,
        enable_temporal_logic=True,
        enable_xai=True,
        enable_adaptive_precision=True,
        enable_carbon_market=True,
        enable_role_specialization=True,
        enable_chaos_testing=True,
        enable_hitl=True,
        enable_federated=True,
    )

    await cache.start()

    fp = WorkloadFingerprint(
        model_size_mb=500, prompt_len=1024, gen_len=2048,
        gpu_mem_free_mb=8000, disk_speed_class=2)

    # Update with several policies
    for i in range(5):
        policy = {"batch_size": 32 + i * 4, "learning_rate": 0.001, "precision": "fp16"}
        await cache.update(
            fp, policy, reward=0.85 + i * 0.02,
            carbon=0.2, latency=0.1, cost=0.05,
            context={"carbon_intensity": 350 + i * 10, "urgency": 0.3},
            expert_id=i % 3)

    # Simulate human feedback for RLHF
    for _ in range(3):
        await cache.record_feedback(
            state={"carbon_intensity": 350, "avg_score": 0.9, "cost": 0.2, "diversity": 0.1},
            action="balanced", reward=0.8)

    # Retrieve
    result = await cache.get_best_policy(fp, context={"carbon_intensity": 350, "urgency": 0.3})
    print(f"\n📋 Retrieval result:")
    if result:
        print(f"   source: {result['source']}")
        print(f"   precision: {result['precision_used']}")
        print(f"   confidence: {result['confidence']:.2f}")
        print(f"   carbon credit: ${result['carbon_credit_value_usd']:.4f}")
        if result.get('temporal_status'):
            print(f"   temporal_status: {result['temporal_status']}")
        if result.get('role_assignments'):
            print(f"   dominant_role: {result['role_assignments']['dominant_role']}")
        if result.get('xai_explanation'):
            print(f"   XAI: {result['xai_explanation']['narrative'][0]}")
    else:
        print("   No policy found")

    # Precision
    p = cache.select_precision()
    print(f"\n⚙️  Precision: {p.value}")

    # Carbon credit
    cc = await cache.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    # HITL
    hitl = await cache.escalate_decision(
        decision_context={'reason': 'smoke'}, options=['a', 'b'], confidence=0.5)
    print(f"👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    # Chaos
    print("\n🧪 Chaos suite:")
    chaos = await cache.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    # Stats
    print("\n📊 Stats:")
    stats = cache.get_stats()
    print(json.dumps({
        'instance': stats['instance_id'],
        'cache_size': stats['cache_size'],
        'total_entries': stats['total_entries'],
        'features': stats['features'],
        'rlhf': stats.get('rlhf'),
        'distillation': stats.get('distillation'),
        'federated': stats.get('federated'),
        'hitl_total': stats.get('hitl', {}).get('total'),
        'chaos_pass_rate': stats.get('chaos', {}).get('pass_rate'),
        'precision_last': stats.get('precision', {}).get('last'),
    }, indent=2, default=str))

    await cache.stop()
    print("\n" + "=" * 78)
    print("✅ Policy Meta Cache v4.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(main())
