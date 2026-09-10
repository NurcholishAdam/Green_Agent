"""
metric_aggregator.py

Enhanced wrapper for the FlexGen executor — v7.0.0

Captures accurate energy, latency, and throughput metrics, and integrates with:
  - MODP                       (multi-objective reward)
  - Bio-inspired fitness       (genetic-style objective)
  - MoE context encoding       (expert routing context)
  - LIMIT Graph                (constraint enforcement)
  - RLHF                       (preference updates)
  - Multi-Teacher Distillation (policy refinement)
  - ✨ v7.0.0 NEW:
      • Temporal Logic Monitor (G/F/U/->)
      • XAI Explainer (feature attribution + narrative)
      • Adaptive Precision Controller (fp32/fp16/int8)
      • Carbon Market Client + RECs
      • Role Specialization Coordinator (emergent multi-agent roles)
      • Chaos Tester (fault injection, first-class citizen)
      • Active RLHF (uncertainty-triggered human queries)
      • Human-in-the-Loop Coordinator (confidence escalation)
      • Federated Aggregator (cross-deployment FedAvg)

All v7.0.0 modules are defined inside this file. No additional files required.
"""

import asyncio
import hashlib
import json
import random
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

from .gpu_profiler import GPUProfiler

# =============================================================================
# OPTIONAL IMPORTS (graceful degradation)
# =============================================================================
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from .MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def evaluate(self, objectives, weights):
            return sum(objectives.get(k, 0) * weights.get(k, 1) for k in objectives)

try:
    from .bio_inspired import FitnessEvaluator
except ImportError:
    class FitnessEvaluator:
        def evaluate(self, metrics, policy):
            return 0.0

try:
    from .moe_system import ContextEncoder
except ImportError:
    class ContextEncoder:
        def encode(self, metrics):
            return [metrics.get("gpu_utilization_pct", 0),
                    metrics.get("cpu_utilization_pct", 0),
                    metrics.get("gpu_memory_used_mb", 0) / 1000]

try:
    from .limit_graph import LimitGraph
except ImportError:
    class LimitGraph:
        def __init__(self, *args, **kwargs): self.limits = {}
        def build_graph(self, nodes, edges): pass
        def get_limits(self, context): return {}
        def update_from_feedback(self, feedback): pass

try:
    from .multi_teacher_policy_distillation import MultiTeacherDistiller
except ImportError:
    class MultiTeacherDistiller:
        def __init__(self, teachers, *args, **kwargs): self.teachers = teachers
        def distill(self, context):
            if not self.teachers:
                return None
            # Fallback: majority vote across dict-returning teachers
            results = []
            for t in self.teachers:
                try:
                    r = t(context)
                    if isinstance(r, dict):
                        results.append(r)
                except Exception:
                    pass
            if not results:
                return None
            merged = {}
            for r in results:
                merged.update(r)
            return merged


# =============================================================================
# ENUMS
# =============================================================================
class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"


class AgentRole(str, Enum):
    QUALITY_SPECIALIST = "quality_specialist"
    THROUGHPUT_SPECIALIST = "throughput_specialist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    MEMORY_SPECIALIST = "memory_specialist"


# =============================================================================
# v7.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """
    Lightweight LTL-style monitor. Operators: G(φ), F(φ), φ U ψ, φ -> ψ.
    Comparison atoms: <, >, <=, >=, ==, !=.
    """
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable[[List[Dict]], bool]] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: List[Dict] = []

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        self.history.append(dict(state))

    def _compile(self, formula: str) -> Callable[[List[Dict]], bool]:
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

    def _atom(self, atom: str) -> Callable[[List[Dict]], bool]:
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
        return lambda hist: bool(atom.lower() in ("true", "1", "yes"))

    def evaluate(self) -> Dict[str, bool]:
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = fn(list(self.history))
            except Exception as e:
                print(f"[TemporalLogic] eval '{name}' failed: {e}")
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({
                    'formula': name,
                    'expression': self.formulas[name],
                    'timestamp': datetime.now().isoformat(),
                })
        return results

    def get_status(self) -> Dict:
        return {
            'formulas': self.formulas,
            'last_results': self.evaluate(),
            'violations': self.violations[-5:],
        }


# =============================================================================
# v7.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    """
    Feature-attribution explainer for policy selection / MODP utilities.
    """
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self,
                candidate: Dict[str, float],
                weights: Dict[str, float],
                all_candidates: List[Dict[str, float]],
                top_k: int = 5) -> Dict:
        """
        Returns normalized contributions + narrative for one candidate
        relative to all candidates.
        """
        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate.get(f, 0.0) for f in self.feature_names])
        w_arr = np.array([weights.get(f, 1.0) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr

        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
        narrative = [
            f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the utility."
            for f, v in ranked
        ]
        return {
            'contributions': contrib,
            'top_features': [f for f, _ in ranked],
            'narrative': narrative,
            'weights_used': dict(weights),
        }


# =============================================================================
# v7.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    """
    Hardware-aware precision switching based on GPU availability, memory,
    and carbon intensity.
    """
    def __init__(self):
        self.telemetry = {
            'gpu_available': False,
            'memory_gb': 16.0,
            'utilization': 0.3,
        }
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity: float, accuracy_required: float = 0.95) -> PrecisionLevel:
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.INT8
        elif self.telemetry.get('gpu_available') and carbon_intensity < 350:
            self.last_precision = PrecisionLevel.FP16
        else:
            self.last_precision = PrecisionLevel.FP32
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0,
                PrecisionLevel.FP16: 0.6,
                PrecisionLevel.INT8: 0.3}[level]


# =============================================================================
# v7.0.0 MODULE D — CARBON MARKET CLIENT (Carbon credits + RECs)
# =============================================================================
class CarbonMarketClient:
    """
    Client for external carbon markets and Renewable Energy Credits.
    Simulated prices; swap for real API in production.
    """
    def __init__(self):
        self.carbon_price_per_ton = 50.0       # USD / tCO2e
        self.rec_price_per_mwh = 30.0          # USD / MWh
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict] = []

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        tons = max(0.0, carbon_saved_kg) / 1000.0
        return round(tons * self.carbon_price_per_ton, 6)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        mwh = max(0.0, energy_saved_kwh) / 1000.0
        return round(mwh * self.rec_price_per_mwh, 6)

    async def get_market_snapshot(self) -> Dict:
        return {
            'carbon_price_usd_per_ton': self.carbon_price_per_ton,
            'rec_price_usd_per_mwh': self.rec_price_per_mwh,
            'grid_intensity_kg_per_mwh': self.grid_intensity_kg_per_mwh,
        }

    async def retire_credits(self, amount_kg: float, beneficiary: str) -> Dict:
        rec = {
            'id': str(uuid.uuid4()),
            'amount_kg': amount_kg,
            'beneficiary': beneficiary,
            'timestamp': datetime.now().isoformat(),
        }
        self.trades.append(rec)
        return rec


# =============================================================================
# v7.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    """
    Emergent multi-agent role assignment via softmax over an affinity matrix.
    """
    def __init__(self):
        self.roles = list(AgentRole)
        # rows = roles, cols = [quality, throughput, energy, carbon, memory]
        self.affinity = np.array([
            [0.9, 0.2, 0.2, 0.2, 0.2],   # quality_specialist
            [0.2, 0.9, 0.2, 0.2, 0.2],   # throughput_specialist
            [0.2, 0.2, 0.9, 0.3, 0.2],   # energy_specialist
            [0.2, 0.2, 0.3, 0.9, 0.2],   # carbon_specialist
            [0.2, 0.2, 0.2, 0.2, 0.9],   # memory_specialist
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        ctx = np.array([
            context.get('quality', 0.5),
            context.get('throughput', 0.5),
            context.get('energy', 0.5),
            context.get('carbon', 0.5),
            context.get('memory', 0.5),
        ])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        assignments = {role.value: float(probs[i]) for i, role in enumerate(self.roles)}
        dominant = self.roles[int(np.argmax(probs))].value
        return {'assignments': assignments, 'dominant_role': dominant}


# =============================================================================
# v7.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    """
    Fault injection to validate resilience.
    Faults: profiler_outage, executor_hang, carbon_api_down, distiller_broken,
            rlhf_broken.
    """
    FAULT_TYPES = [
        'profiler_outage',
        'executor_hang',
        'carbon_api_down',
        'distiller_broken',
        'rlhf_broken',
    ]

    def __init__(self, aggregator_ref=None):
        self.aggregator = aggregator_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.2) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")

        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable[[], None]] = []
        agg = self.aggregator

        try:
            if fault_type == 'profiler_outage' and agg is not None:
                orig = agg.profiler.get_current_metrics
                agg.profiler.get_current_metrics = lambda: {}
                restore.append(lambda: setattr(agg.profiler, 'get_current_metrics', orig))

            elif fault_type == 'executor_hang' and agg is not None:
                orig = agg.executor
                async def hung(*a, **k):
                    await asyncio.sleep(10)
                    return None, {}
                agg.executor = hung
                restore.append(lambda: setattr(agg, 'executor', orig))

            elif fault_type == 'carbon_api_down' and agg is not None and agg.carbon_market:
                orig = agg.carbon_market.get_market_snapshot
                async def broken():
                    raise RuntimeError("carbon market API down")
                agg.carbon_market.get_market_snapshot = broken
                restore.append(lambda: setattr(agg.carbon_market, 'get_market_snapshot', orig))

            elif fault_type == 'distiller_broken' and agg is not None:
                if agg.distiller is not None:
                    orig = agg.distiller.distill
                    def broken(ctx):
                        raise RuntimeError("distiller broken")
                    agg.distiller.distill = broken
                    restore.append(lambda: setattr(agg.distiller, 'distill', orig))

            elif fault_type == 'rlhf_broken' and agg is not None and agg.rlhf:
                orig = agg.rlhf.update
                def broken(*a, **k):
                    raise RuntimeError("rlhf broken")
                agg.rlhf.update = broken
                restore.append(lambda: setattr(agg.rlhf, 'update', orig))

            await asyncio.sleep(duration_s)

        except Exception as e:
            passed = False
            error_msg = str(e)
        finally:
            for r in restore:
                try:
                    r()
                except Exception:
                    pass

        result = {
            'fault': fault_type,
            'duration_s': duration_s,
            'elapsed_s': time.time() - start,
            'passed': passed,
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
        }
        self.results.append(result)
        print(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {
            'tests_run': len(self.results),
            'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                         if self.results else 1.0,
            'recent': self.results[-5:],
        }


# =============================================================================
# v7.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
class ActiveRLHF:
    """
    Preference-based policy with uncertainty-triggered human queries.
    """
    def __init__(self,
                 action_space: List[str],
                 uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}

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
        self.preference_counts[action] += reward
        self.history.append({
            'action': action, 'reward': reward,
            'timestamp': datetime.now().isoformat(),
        })

    async def maybe_query_human(self, context: Dict, options: List[str]) -> Optional[Dict]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {
            'id': qid,
            'context': context,
            'options': options,
            'uncertainty': u,
            'created_at': datetime.now().isoformat(),
            'status': 'pending',
        }
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
# v7.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
class HumanInTheLoopCoordinator:
    """
    Escalates low-confidence decisions to humans with auto-fallback.
    """
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self,
                       decision_context: Dict,
                       options: List[str],
                       confidence: float,
                       confidence_threshold: float = 0.65) -> Dict:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}

        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({
            'decision': 'escalated',
            'query_id': query.get('id'),
            'auto_fallback': auto_choice,
            'confidence': confidence,
            'timestamp': datetime.now().isoformat(),
        })
        return {'escalated': True, 'query': query,
                'chosen': auto_choice, 'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# v7.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    """
    FedAvg-style cross-deployment learning.
    """
    def __init__(self, num_params: int = 5):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({
            'client_id': client_id,
            'weights': list(weights),
            'samples': samples,
        })

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
        print(f"[Federated] round {self.round}: {self.global_weights}")
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {
            'round': self.round,
            'global_weights': self.global_weights,
            'pending_updates': len(self.client_updates),
        }


# =============================================================================
# METRIC AGGREGATOR (v7.0.0)
# =============================================================================
class MetricAggregator:
    """
    Enhanced metric aggregator with MODP, bio, MoE, LIMIT Graph, RLHF,
    Multi-Teacher Distillation, and the v7.0.0 enhancement suite.
    """

    def __init__(
        self,
        gpu_profiler: GPUProfiler,
        executor_fn: Callable,
        modp_weights: Optional[Dict[str, float]] = None,
        bio_evaluator: Optional[Any] = None,
        moe_encoder: Optional[Any] = None,
        enable_callbacks: bool = True,
        limit_graph: Optional[Any] = None,
        rlhf_optimizer: Optional[Any] = None,
        distiller: Optional[Any] = None,
        # v7.0.0 feature flags
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_adaptive_precision: bool = True,
        enable_carbon_market: bool = True,
        enable_role_specialization: bool = True,
        enable_chaos_testing: bool = True,
        enable_hitl: bool = True,
        enable_federated: bool = True,
        hitl_confidence_threshold: float = 0.65,
        carbon_intensity_gco2_kwh: float = 200.0,
    ):
        # -- Core --
        self.profiler = gpu_profiler
        self.executor = executor_fn

        # -- MODP --
        self.modp = ParetoOptimizer()
        self.modp_weights = modp_weights or {
            "quality": 0.30,
            "throughput": 0.25,
            "energy_efficiency": 0.20,
            "carbon_efficiency": 0.15,
            "memory_efficiency": 0.10,
        }

        # -- Bio --
        self.bio = bio_evaluator if bio_evaluator else FitnessEvaluator()

        # -- MoE --
        self.moe = moe_encoder if moe_encoder else ContextEncoder()

        # -- LIMIT Graph --
        self.limit_graph = limit_graph if limit_graph else LimitGraph()

        # -- Distillation --
        self.distiller = distiller
        if self.distiller is None:
            teachers = [
                self._teacher_modp_policy,
                self._teacher_bio_policy,
                self._teacher_moe_policy,
            ]
            self.distiller = MultiTeacherDistiller(teachers)

        # ====================================================================
        # v7.0.0 MODULES
        # ====================================================================
        self.enable_temporal_logic = enable_temporal_logic
        self.enable_xai = enable_xai
        self.enable_adaptive_precision = enable_adaptive_precision
        self.enable_carbon_market = enable_carbon_market
        self.enable_role_specialization = enable_role_specialization
        self.enable_chaos_testing = enable_chaos_testing
        self.enable_hitl = enable_hitl
        self.enable_federated = enable_federated
        self.hitl_confidence_threshold = hitl_confidence_threshold
        self.default_carbon_intensity = carbon_intensity_gco2_kwh

        # Temporal logic monitor
        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon_kg <= 1.0)")
            self.temporal_monitor.add_formula("success_min", "F(success >= 0.5)")
            self.temporal_monitor.add_formula("no_oom", "G(gpu_oom <= 0.5)")

        # XAI
        self.xai = XAIExplainer(
            ['quality', 'throughput', 'energy_efficiency',
             'carbon_efficiency', 'memory_efficiency']
        ) if enable_xai else None

        # Adaptive precision
        self.precision_controller = AdaptivePrecisionController() if enable_adaptive_precision else None

        # Carbon market
        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None

        # Role specialization
        self.role_coordinator = RoleSpecializationCoordinator() if enable_role_specialization else None

        # Active RLHF (v7 upgrade over the stub)
        # If the user passed an explicit rlhf_optimizer we keep it;
        # otherwise we build our own ActiveRLHF.
        self.rlhf = rlhf_optimizer if rlhf_optimizer is not None else ActiveRLHF(
            action_space=[
                'quality', 'throughput', 'energy',
                'carbon', 'memory',
            ]
        )

        # HITL
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) if enable_hitl else None

        # Federated aggregator
        self.federated = FederatedAggregator(num_params=5) if enable_federated else None
        self.instance_id = str(uuid.uuid4())[:8]

        # Chaos tester
        self.chaos = ChaosTester(self) if enable_chaos_testing else None

        # Callbacks
        self._callbacks: List[Tuple[Callable, float, float]] = []
        self.enable_callbacks = enable_callbacks

        # Store last metrics
        self._last_metrics: Optional[Dict[str, Any]] = None

        # History for federated & self-assessment
        self._run_history: deque = deque(maxlen=200)

    # ------------------------------------------------------------------
    # Teacher functions for distillation
    # ------------------------------------------------------------------
    def _teacher_modp_policy(self, context: Dict) -> Dict:
        task = context.get('task', {})
        policy = context.get('policy', {}).copy()
        carbon = task.get('carbon_intensity_gco2_kwh', self.default_carbon_intensity)
        if carbon > 300:
            policy['max_tokens'] = min(policy.get('max_tokens', 150), 100)
        return policy

    def _teacher_bio_policy(self, context: Dict) -> Dict:
        task = context.get('task', {})
        policy = context.get('policy', {}).copy()
        if task.get('model_size_mb', 0) > 30000:
            policy['gpu_batch_size'] = 2
        return policy

    def _teacher_moe_policy(self, context: Dict) -> Dict:
        task = context.get('task', {})
        policy = context.get('policy', {}).copy()
        if task.get('prompt_len', 0) > 512:
            policy['block_size'] = min(policy.get('block_size', 8), 4)
        return policy

    # ------------------------------------------------------------------
    # LIMIT Graph enforcement
    # ------------------------------------------------------------------
    def _apply_limit_graph(self, task: Dict, policy: Dict) -> Dict:
        if self.limit_graph is None:
            return policy
        context = {
            'task': task,
            'policy': policy,
            'model_size_mb': task.get('model_size_mb', 0),
            'prompt_len': task.get('prompt_len', 0),
            'gpu_mem_free_mb': task.get('gpu_mem_free_mb', 0),
        }
        limits = self.limit_graph.get_limits(context)
        for key, val in limits.items():
            if key == 'max_gpu_batch_size' and 'gpu_batch_size' in policy:
                policy['gpu_batch_size'] = min(policy['gpu_batch_size'], val)
            elif key == 'max_block_size' and 'block_size' in policy:
                policy['block_size'] = min(policy['block_size'], val)
            elif key == 'max_max_tokens' and 'max_tokens' in policy:
                policy['max_tokens'] = min(policy['max_tokens'], val)
        return policy

    # ------------------------------------------------------------------
    # Distillation
    # ------------------------------------------------------------------
    def _select_policy_with_distillation(self, task: Dict, policy: Dict) -> Dict:
        if self.distiller is None or not getattr(self.distiller, 'teachers', None):
            return policy
        context = {'task': task, 'policy': policy}
        try:
            refined = self.distiller.distill(context)
        except Exception as e:
            print(f"[Distiller] error: {e}")
            return policy
        if refined is None:
            return policy
        merged = policy.copy()
        merged.update(refined)
        return merged

    # ------------------------------------------------------------------
    # Callback system
    # ------------------------------------------------------------------
    def register_callback(self, callback: Callable[[Dict[str, Any]], None], cooldown: float = 0.1):
        self._callbacks.append((callback, cooldown, 0.0))

    def _trigger_callbacks(self, metrics: Dict[str, Any]):
        now = time.time()
        for i, (cb, cd, last) in enumerate(self._callbacks):
            if now - last >= cd:
                try:
                    cb(metrics)
                except Exception as e:
                    print(f"Callback error: {e}")
                self._callbacks[i] = (cb, cd, now)

    # ------------------------------------------------------------------
    # Core run method
    # ------------------------------------------------------------------
    def run(self, task: Dict[str, Any], policy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute inference and capture real metrics.
        Returns aggregated metrics dict (v7.0.0 enriched).
        """
        # ---- 1. Distillation ----
        refined_policy = self._select_policy_with_distillation(task, policy)

        # ---- 2. LIMIT Graph ----
        refined_policy = self._apply_limit_graph(task, refined_policy)

        # ---- 3. Adaptive precision ----
        precision = PrecisionLevel.FP32
        if self.precision_controller is not None:
            ci = task.get('carbon_intensity_gco2_kwh', self.default_carbon_intensity)
            precision = self.precision_controller.select(float(ci), accuracy_required=0.95)
            # Inform the executor via policy (executors may honour this hint)
            refined_policy.setdefault('precision', precision.value)

        # ---- 4. Pre-execution ----
        start_metrics = self.profiler.get_current_metrics()
        start_time = time.time()

        # ---- 5. Execute ----
        success = True
        try:
            output, raw_inference_metrics = self.executor(task, refined_policy)
        except Exception as e:
            output = None
            raw_inference_metrics = {"error": str(e)}
            success = False

        # ---- 6. Post-execution ----
        end_metrics = self.profiler.get_current_metrics()
        end_time = time.time()

        # ---- 7. Derived metrics ----
        elapsed_sec = end_time - start_time
        tokens_generated = raw_inference_metrics.get("tokens_generated", 0)
        tokens_per_sec = tokens_generated / elapsed_sec if elapsed_sec > 0 else 0.0

        gpu_energy_joules = (end_metrics.get("energy_gpu_joules", 0.0)
                             - start_metrics.get("energy_gpu_joules", 0.0))
        cpu_energy_joules = (end_metrics.get("energy_cpu_joules", 0.0)
                             - start_metrics.get("energy_cpu_joules", 0.0))
        total_energy_kwh = (gpu_energy_joules + cpu_energy_joules) / 3600.0 / 1000.0

        gpu_total = end_metrics.get("gpu_memory_total_mb", 1.0)
        gpu_used = end_metrics.get("gpu_memory_used_mb", 0.0)
        memory_efficiency = gpu_used / gpu_total if gpu_total > 0 else 0.0

        avg_gpu_power = (start_metrics.get("gpu_power_watts", 0.0)
                         + end_metrics.get("gpu_power_watts", 0.0)) / 2.0

        carbon_intensity = float(task.get('carbon_intensity_gco2_kwh', self.default_carbon_intensity))
        carbon_kg = total_energy_kwh * carbon_intensity / 1000.0

        metrics: Dict[str, Any] = {
            "success": success,
            "output": output,
            "inference_metrics": raw_inference_metrics,
            "elapsed_sec": elapsed_sec,
            "tokens_per_sec": tokens_per_sec,
            "total_energy_kwh": total_energy_kwh,
            "gpu_energy_joules": gpu_energy_joules,
            "cpu_energy_joules": cpu_energy_joules,
            "gpu_power_avg_watts": avg_gpu_power,
            "gpu_memory_peak_mb": max(start_metrics.get("gpu_memory_used_mb", 0),
                                      end_metrics.get("gpu_memory_used_mb", 0)),
            "memory_efficiency": memory_efficiency,
            "carbon_kg": carbon_kg,
            "gpu_oom": (not success and
                        "CUDA out of memory" in str(raw_inference_metrics.get("error", ""))),
            "start_metrics": start_metrics,
            "end_metrics": end_metrics,
            "energy_efficiency": end_metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": end_metrics.get("carbon_efficiency", 0.0),
            "quality_score": raw_inference_metrics.get("quality_score", 1.0),
            "refined_policy": refined_policy,
            "precision_used": precision.value,
        }

        self._last_metrics = metrics
        self._run_history.append(metrics)

        # ---- 8. MODP utility (used for RLHF reward) ----
        utility = self.compute_modp_utility(metrics)

        # ---- 9. Temporal logic gate ----
        if self.temporal_monitor is not None:
            self.temporal_monitor.update({
                'carbon_kg': float(carbon_kg),
                'success': 1.0 if success else 0.0,
                'gpu_oom': 1.0 if metrics['gpu_oom'] else 0.0,
                'utility': float(utility),
            })
            temporal_status = self.temporal_monitor.evaluate()
            metrics['temporal_status'] = temporal_status
            metrics['temporal_ok'] = all(temporal_status.values()) if temporal_status else True
        else:
            metrics['temporal_status'] = {}
            metrics['temporal_ok'] = True

        # ---- 10. XAI explanation ----
        if self.xai is not None:
            objectives = {
                "quality": metrics.get("quality_score", 1.0),
                "throughput": metrics.get("tokens_per_sec", 0.0) / 100.0,
                "energy_efficiency": metrics.get("energy_efficiency", 0.0),
                "carbon_efficiency": metrics.get("carbon_efficiency", 0.0),
                "memory_efficiency": metrics.get("memory_efficiency", 0.0),
            }
            # Compare the current policy to the default policy as reference
            default_obj = {
                "quality": 1.0,
                "throughput": 0.5,
                "energy_efficiency": 0.5,
                "carbon_efficiency": 0.5,
                "memory_efficiency": 0.5,
            }
            metrics['xai_explanation'] = self.xai.explain(
                candidate=objectives,
                weights=self.modp_weights,
                all_candidates=[objectives, default_obj],
            )

        # ---- 11. Carbon credit + REC value ----
        if self.carbon_market is not None:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = None

            async def _compute_credit_values():
                credit = await self.carbon_market.get_carbon_credit_value(carbon_kg)
                rec = await self.carbon_market.get_rec_value(total_energy_kwh * 1000.0)
                return credit, rec

            if loop is not None and loop.is_running():
                # Running inside an event loop — schedule and attach future
                future = asyncio.ensure_future(_compute_credit_values())
                metrics['carbon_credit_future'] = future
            else:
                try:
                    credit, rec = asyncio.run(_compute_credit_values())
                except RuntimeError:
                    credit, rec = 0.0, 0.0
                metrics['carbon_credit_value_usd'] = credit
                metrics['rec_value_usd'] = rec

        # ---- 12. Role assignment ----
        if self.role_coordinator is not None:
            role_context = {
                'quality': metrics.get('quality_score', 0.5),
                'throughput': min(1.0, metrics.get('tokens_per_sec', 0.0) / 100.0),
                'energy': metrics.get('energy_efficiency', 0.5),
                'carbon': metrics.get('carbon_efficiency', 0.5),
                'memory': metrics.get('memory_efficiency', 0.5),
            }
            metrics['role_assignments'] = self.role_coordinator.assign_roles(role_context)

        # ---- 13. RLHF update ----
        if self.rlhf is not None:
            action_id = metrics['role_assignments']['dominant_role'] \
                if 'role_assignments' in metrics else 'throughput'
            try:
                self.rlhf.update(
                    {'task': task, 'metrics': {k: v for k, v in metrics.items()
                                               if k in ('quality_score', 'tokens_per_sec',
                                                        'carbon_kg', 'total_energy_kwh')}},
                    action_id,
                    utility if success else -1.0,
                )
            except Exception as e:
                print(f"[RLHF] update failed: {e}")

        # ---- 14. HITL escalation ----
        if self.hitl is not None:
            confidence = float(metrics.get('quality_score', 0.5))
            try:
                # Only escalate synchronously if the loop is not running
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    future = asyncio.ensure_future(self.hitl.escalate(
                        decision_context={'task': task, 'utility': utility},
                        options=['accept', 'retry', 'downgrade'],
                        confidence=confidence,
                        confidence_threshold=self.hitl_confidence_threshold,
                    ))
                    metrics['hitl_future'] = future
                else:
                    outcome = asyncio.run(self.hitl.escalate(
                        decision_context={'task': task, 'utility': utility},
                        options=['accept', 'retry', 'downgrade'],
                        confidence=confidence,
                        confidence_threshold=self.hitl_confidence_threshold,
                    ))
                    metrics['hitl_outcome'] = outcome
            except RuntimeError:
                pass

        # ---- 15. Federated submission ----
        if self.federated is not None:
            weights_vec = [
                float(self.modp_weights.get('quality', 0.3)),
                float(self.modp_weights.get('throughput', 0.25)),
                float(self.modp_weights.get('energy_efficiency', 0.2)),
                float(self.modp_weights.get('carbon_efficiency', 0.15)),
                float(self.modp_weights.get('memory_efficiency', 0.1)),
            ]
            self.federated.submit_update(
                client_id=self.instance_id,
                weights=weights_vec,
                samples=1,
            )
            if len(self._run_history) % 10 == 0:
                metrics['federated_round'] = self.federated.aggregate()['round']

        # ---- 16. LIMIT Graph feedback ----
        if self.limit_graph is not None:
            self.limit_graph.update_from_feedback({
                'task': task,
                'policy': refined_policy,
                'metrics': metrics,
                'success': success,
            })

        # ---- 17. Callbacks ----
        if self.enable_callbacks:
            self._trigger_callbacks(metrics)

        return metrics

    # ------------------------------------------------------------------
    # Integration interfaces
    # ------------------------------------------------------------------
    def compute_modp_utility(self, metrics: Optional[Dict[str, Any]] = None) -> float:
        if metrics is None:
            if self._last_metrics is None:
                raise ValueError("Must provide metrics or run the executor first.")
            metrics = self._last_metrics

        objectives = {
            "quality": metrics.get("quality_score", 1.0),
            "throughput": metrics.get("tokens_per_sec", 0.0) / 100.0,
            "energy_efficiency": metrics.get("energy_efficiency", 0.0),
            "carbon_efficiency": metrics.get("carbon_efficiency", 0.0),
            "memory_efficiency": metrics.get("memory_efficiency", 0.0),
        }
        # ParetoOptimizer.evaluate expects (objectives, weights)
        try:
            return float(self.modp.evaluate(objectives, self.modp_weights))
        except Exception:
            return float(sum(objectives.get(k, 0) * self.modp_weights.get(k, 1)
                             for k in objectives))

    def compute_bio_fitness(self, metrics: Dict[str, Any], policy: Dict[str, Any]) -> float:
        return self.bio.evaluate(metrics, policy)

    def get_moe_context(self, metrics: Dict[str, Any]) -> List[float]:
        return self.moe.encode(metrics)

    # ------------------------------------------------------------------
    # v7.0.0 Utilities
    # ------------------------------------------------------------------
    def get_last_run_metrics(self) -> Optional[Dict[str, Any]]:
        return self._last_metrics

    def get_current_policy(self) -> Optional[Dict[str, Any]]:
        if self._last_metrics:
            return self._last_metrics.get("refined_policy")
        return None

    def get_comprehensive_status(self) -> Dict:
        return {
            'instance_id': self.instance_id,
            'runs': len(self._run_history),
            'features': {
                'temporal_logic': self.temporal_monitor is not None,
                'xai': self.xai is not None,
                'adaptive_precision': self.precision_controller is not None,
                'carbon_market': self.carbon_market is not None,
                'role_specialization': self.role_coordinator is not None,
                'chaos_testing': self.chaos is not None,
                'hitl': self.hitl is not None,
                'federated': self.federated is not None,
            },
            'temporal_logic': self.temporal_monitor.get_status() if self.temporal_monitor else None,
            'precision_last': self.precision_controller.last_precision.value
                if self.precision_controller else None,
            'carbon_market': (self.carbon_market.__dict__
                              if self.carbon_market is not None else None),
            'role_assignments': (self.role_coordinator.assign_roles({
                'quality': 0.5, 'throughput': 0.5, 'energy': 0.5,
                'carbon': 0.5, 'memory': 0.5}) if self.role_coordinator else None),
            'chaos_report': self.chaos.get_report() if self.chaos else None,
            'hitl_audit': self.hitl.get_audit() if self.hitl else None,
            'federated': self.federated.get_stats() if self.federated else None,
        }

    async def run_chaos_suite(self) -> Dict:
        """Run the full chaos suite against this aggregator."""
        if self.chaos is None:
            return {'error': 'chaos testing disabled'}
        results = []
        for fault in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos.run_test(fault, duration_s=0.05))
            except Exception as e:
                results.append({'fault': fault, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos.get_report()}


# =============================================================================
# STANDALONE SMOKE TEST
# =============================================================================
async def _smoke_test():
    """Quick standalone self-test (no FlexGen required)."""
    class DummyProfiler:
        def get_current_metrics(self):
            return {
                "energy_gpu_joules": random.random() * 100,
                "energy_cpu_joules": random.random() * 50,
                "gpu_memory_total_mb": 16000.0,
                "gpu_memory_used_mb": random.random() * 8000,
                "gpu_power_watts": random.random() * 200,
                "energy_efficiency": random.random(),
                "carbon_efficiency": random.random(),
            }

    def dummy_executor(task, policy):
        return {"result": "ok"}, {"tokens_generated": 100, "quality_score": 0.85}

    agg = MetricAggregator(
        gpu_profiler=DummyProfiler(),
        executor_fn=dummy_executor,
        enable_callbacks=True,
    )

    metrics = agg.run(
        task={'model_size_mb': 15000, 'prompt_len': 256,
              'carbon_intensity_gco2_kwh': 250.0},
        policy={'max_tokens': 128, 'gpu_batch_size': 4, 'block_size': 8},
    )

    print("\n=== Smoke Test Result ===")
    print(json.dumps({
        'utility': agg.compute_modp_utility(metrics),
        'precision': metrics.get('precision_used'),
        'temporal_ok': metrics.get('temporal_ok'),
        'xai_top_features': metrics.get('xai_explanation', {}).get('top_features'),
        'role_dominant': metrics.get('role_assignments', {}).get('dominant_role'),
    }, indent=2, default=str))

    print("\n=== Comprehensive Status ===")
    print(json.dumps(agg.get_comprehensive_status(), indent=2, default=str))

    print("\n=== Chaos Suite ===")
    report = await agg.run_chaos_suite()
    print(json.dumps(report['report'], indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(_smoke_test())
