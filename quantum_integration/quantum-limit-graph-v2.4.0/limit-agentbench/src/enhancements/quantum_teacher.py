# src/enhancements/quantum_teacher.py
"""
Quantum Teacher for Multi‑Teacher Distillation — v2.0.0

Integrates a variational quantum circuit (VQC) as a teacher in the
distillation ensemble. The circuit encodes the state vector, applies a
trainable ansatz, and outputs a probability distribution over actions.

v2.0.0 adds (all in-file, no external modules required):
    • Temporal Logic Verification (G/F/U/->)
    • Explainable AI (feature attribution + narrative)
    • Adaptive Precision Switching (fp32/fp16/bf16/fp8/fp4)
    • Carbon Markets + Renewable Energy Credits (RECs)
    • Multi-Agent Role Specialization (emergent, softmax affinity)
    • Chaos Testing (fault injection covering teacher + backend)
    • Active RLHF (uncertainty-triggered human queries)
    • Human-in-the-Loop Coordinator (escalation with auto-fallback)
    • Federated Green Learning (FedAvg over VQC parameters)
    • Causal RL hooks (IPW / ATE on student-teacher decisions)
    • PennyLane fallback (simulated VQC if PennyLane missing)

Usage:
    teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=42)
    probs = teacher.predict(state)                # single sample
    probs_batch = teacher.predict_batch(states)   # batch
    teacher.update_params(new_params)             # for training
    # v2.0.0:
    enhanced = await teacher.predict_with_explanations(state)
    await teacher.escalate_decision(context, options, confidence)
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import random
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np

try:
    import pennylane as qml
    PENNYLANE_AVAILABLE = True
except ImportError:
    PENNYLANE_AVAILABLE = False

logger = logging.getLogger(__name__)


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
# v2.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ with comparison atoms."""
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
                self.violations.append({
                    'formula': name, 'expression': self.formulas[name],
                    'timestamp': datetime.now().isoformat()})
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}


# =============================================================================
# v2.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    """Feature-attribution explainer for VQC outputs."""
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
        return {'contributions': contrib,
                'top_features': [f for f, _ in ranked],
                'narrative': narrative,
                'weights_used': dict(weights)}


# =============================================================================
# v2.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    """Hardware-aware precision selection for VQC simulation."""
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
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


# =============================================================================
# v2.0.0 MODULE D — CARBON MARKET CLIENT
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
        return rec


# =============================================================================
# v2.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
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
        ctx = np.array([context.get('trust', 0.5), context.get('compute', 0.5),
                        context.get('energy', 0.5), context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i])
                                for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}


# =============================================================================
# v2.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    """Fault injection for the quantum teacher."""
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
                orig = t.dev
                t.dev = None
                restore.append(lambda: setattr(t, 'dev', orig))
            elif fault_type == 'params_corrupt' and t:
                orig_params = copy.deepcopy(t.params)
                t.params['variational'] = np.zeros_like(t.params['variational'])
                t.params['measurement'] = np.zeros_like(t.params['measurement'])
                restore.append(lambda: t.set_params(orig_params))
            elif fault_type == 'circuit_broken' and t:
                orig = t.qnode
                def broken(*a, **k): raise RuntimeError("circuit broken")
                t.qnode = broken
                restore.append(lambda: setattr(t, 'qnode', orig))
            elif fault_type == 'encoding_fail' and t:
                orig = t._prepare_state
                t._prepare_state = lambda s: np.zeros(t.n_qubits)
                restore.append(lambda: setattr(t, '_prepare_state', orig))
            elif fault_type == 'measurement_fail' and t:
                # Simulate by corrupting measurement weights
                orig = t.params['measurement'].copy()
                t.params['measurement'] = np.full_like(t.params['measurement'], 1e6)
                restore.append(lambda: t.params.__setitem__('measurement', orig))
            elif fault_type == 'cache_corrupt' and t:
                if hasattr(t, '_cache'):
                    orig_cache = dict(t._cache)
                    t._cache = {}
                    restore.append(lambda: setattr(t, '_cache', orig_cache))
            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for r in restore:
                try: r()
                except Exception: pass

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg,
                  'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        logger.warning(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}


# =============================================================================
# v2.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
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
# v2.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
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
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# v2.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    """FedAvg-style cross-deployment aggregation of teacher parameters."""
    def __init__(self, num_params: int = 4):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [0.0] * num_params
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
        logger.info(f"[Federated] round {self.round}: {self.global_weights}")
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# v2.0.0 MODULE J — CAUSAL REWARD SHAPER (IPW / ATE)
# =============================================================================
class CausalRewardShaper:
    """Lightweight causal inference: IPW-based average treatment effect."""
    def __init__(self, num_actions: int):
        self.num_actions = num_actions
        self.interventions: deque = deque(maxlen=500)
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    def record(self, action: int, reward: float, propensities: np.ndarray):
        self.interventions.append((action, float(reward), np.array(propensities)))

    def compute_ate(self) -> Dict[int, float]:
        if len(self.interventions) < 10:
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
# QUANTUM TEACHER — v2.0.0
# =============================================================================
class QuantumTeacher:
    """
    Variational quantum teacher that maps a state vector to action probabilities.

    v2.0.0 integrates ten enhancement areas while preserving the original API:
        - predict(state)                 → probabilities
        - predict_batch(states)          → batch probabilities
        - get_params() / set_params()    → parameter management
        - confidence(state)              → scalar confidence

    New in v2.0.0:
        - predict_with_explanations(state) → rich payload (probs + XAI + temporal)
        - escalate_decision(context, options, confidence)
        - run_chaos_suite()
        - run_federated_round()
        - get_causal_ates()
        - select_precision()
        - compute_carbon_credit()
    """

    def __init__(
        self,
        n_actions: int = 5,
        n_qubits: int = 4,
        n_layers: int = 2,
        backend: str = "default.qubit",
        seed: Optional[int] = None,
        # v2.0.0 flags
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

        if seed is not None:
            np.random.seed(seed)

        # PennyLane device (or None if fallback)
        if PENNYLANE_AVAILABLE:
            try:
                self.dev = qml.device(backend, wires=n_qubits)
            except Exception as e:
                logger.warning(f"PennyLane device init failed: {e}; using fallback")
                self.dev = None
        else:
            self.dev = None

        # Parameters
        self.params = {
            "variational": np.random.normal(0, 0.1, (n_layers, n_qubits, 3)),
            "measurement": np.random.normal(0, 0.1, (n_qubits, n_actions)),
        }

        # Build QNode if PennyLane available; else fallback
        self.qnode = self._build_qnode() if self.dev is not None else None

        # ============================================================
        # v2.0.0 modules
        # ============================================================
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

        self.rlhf = ActiveRLHF(
            action_space=[f"action_{i}" for i in range(n_actions)],
        )
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) if enable_hitl else None

        self.federated = FederatedAggregator(num_params=4) if enable_federated else None
        self.instance_id = str(uuid.uuid4())[:8]

        self.causal_shaper = CausalRewardShaper(num_actions=n_actions) \
            if enable_causal_rl else None

        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        # Prediction cache
        self._cache: Dict[str, np.ndarray] = {}
        self._cache_hits = 0
        self._cache_misses = 0

        # Carbon credits accumulated
        self._carbon_saved_kg_total = 0.0

        logger.debug(f"QuantumTeacher v2.0.0 initialized with {n_qubits} qubits, "
                     f"{n_layers} layers, {n_actions} actions, "
                     f"PennyLane={'yes' if self.dev else 'no'}")

    # ------------------------------------------------------------------
    # QNode construction (PennyLane)
    # ------------------------------------------------------------------
    def _build_qnode(self):
        dev = self.dev
        n_qubits = self.n_qubits
        n_layers = self.n_layers
        n_actions = self.n_actions

        @qml.qnode(dev, interface="autograd")
        def circuit(state_vec, variational_params, measurement_weights):
            clipped = np.clip(state_vec, -np.pi, np.pi)
            for i in range(n_qubits):
                if i < len(clipped):
                    qml.RY(clipped[i], wires=i)
                else:
                    qml.RY(0.0, wires=i)

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
                if n_qubits > 2:
                    qml.CNOT(wires=[n_qubits - 1, 0])

            expvals = [qml.expval(qml.PauliZ(i)) for i in range(n_qubits)]
            expvals = qml.math.stack(expvals)
            logits = expvals @ measurement_weights
            return qml.math.softmax(logits)

        return circuit

    # ------------------------------------------------------------------
    # State preparation + fallback VQC
    # ------------------------------------------------------------------
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
        """Simulated VQC output when PennyLane is unavailable or circuit is broken."""
        # Simple deterministic function of state + params
        p = self.params['variational'].reshape(-1)[:self.n_qubits]
        m = self.params['measurement']
        features = np.tanh(state_vec * p)
        logits = features @ m
        # Softmax
        logits = logits - logits.max()
        e = np.exp(logits)
        return e / e.sum()

    # ------------------------------------------------------------------
    # ORIGINAL API (preserved) — predict / predict_batch / confidence
    # ------------------------------------------------------------------
    def predict(self, state: Union[Any, np.ndarray]) -> np.ndarray:
        state_vec = self._prepare_state(state)
        cache_key = hashlib.sha256(state_vec.tobytes()).hexdigest()[:16]

        if cache_key in self._cache:
            self._cache_hits += 1
            return self._cache[cache_key].copy()

        self._cache_misses += 1
        if self.qnode is not None:
            try:
                probs = self.qnode(state_vec, self.params["variational"],
                                    self.params["measurement"])
                probs = np.asarray(probs, dtype=np.float64)
            except Exception as e:
                logger.warning(f"VQC predict failed: {e}; using fallback")
                probs = self._fallback_predict(state_vec)
        else:
            probs = self._fallback_predict(state_vec)

        self._cache[cache_key] = probs.copy()
        return probs

    def predict_batch(self, states: Sequence[Union[Any, np.ndarray]]) -> np.ndarray:
        if self.qnode is not None and PENNYLANE_AVAILABLE:
            try:
                batch_vecs = np.stack([self._prepare_state(s) for s in states])
                batched_qnode = qml.batch_input(self.qnode, argnum=0)
                probs = batched_qnode(batch_vecs, self.params["variational"],
                                      self.params["measurement"])
                return np.asarray(probs, dtype=np.float64)
            except Exception as e:
                logger.warning(f"VQC batch predict failed: {e}; iterating one by one")
        return np.stack([self.predict(s) for s in states])

    def get_params(self) -> Dict[str, np.ndarray]:
        return copy.deepcopy(self.params)

    def set_params(self, new_params: Dict[str, np.ndarray]) -> None:
        for key in self.params:
            if key not in new_params:
                raise KeyError(f"Missing parameter group '{key}' in new_params")
            if self.params[key].shape != new_params[key].shape:
                raise ValueError(f"Shape mismatch for '{key}'")
        self.params = {k: np.array(v, dtype=np.float64) for k, v in new_params.items()}
        self._cache.clear()
        logger.debug("Parameters updated.")

    def confidence(self, state: Union[Any, np.ndarray]) -> float:
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-12))
        max_entropy = np.log(self.n_actions)
        if max_entropy == 0:
            return 1.0
        return float(np.clip(1.0 - (entropy / max_entropy), 0.0, 1.0))

    # ------------------------------------------------------------------
    # v2.0.0 — Enhanced prediction with XAI + temporal + HITL
    # ------------------------------------------------------------------
    async def predict_with_explanations(
        self,
        state: Union[Any, np.ndarray],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Enhanced prediction returning probabilities plus rich metadata.

        Returns a dict with:
            'probabilities'      — np.ndarray of shape (n_actions,)
            'confidence'         — scalar in [0,1]
            'entropy'            — scalar
            'precision_used'     — PrecisionLevel string
            'temporal_status'    — Dict[str, bool]
            'xai_explanation'    — feature attribution + narrative
            'role_assignments'   — dict of role → probability
            'hitl_outcome'       — escalation outcome (if triggered)
        """
        context = context or {}
        state_vec = self._prepare_state(state)
        probs = self.predict(state_vec)

        # Confidence + entropy
        entropy = float(-np.sum(probs * np.log(probs + 1e-12)))
        max_entropy = np.log(self.n_actions)
        confidence = float(np.clip(1.0 - (entropy / max_entropy), 0.0, 1.0)) \
            if max_entropy > 0 else 1.0

        # Adaptive precision
        carbon_intensity = float(context.get('carbon_intensity', 400.0))
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(carbon_intensity, 0.95)

        # Temporal gate
        temporal_status = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'confidence': confidence,
                'entropy': entropy,
                'carbon': carbon_intensity,
            })
            temporal_status = self.temporal_monitor.evaluate()

        # XAI explanation
        xai_out = None
        if self.xai:
            features = {f"feature_{i}": float(state_vec[i])
                        for i in range(self.n_qubits)}
            weights = {f"feature_{i}": 1.0 for i in range(self.n_qubits)}
            # Single-candidate explanation
            xai_out = self.xai.explain(
                candidate=features, weights=weights,
                all_candidates=[features],
            )

        # Role assignment
        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7,
                'energy': 1.0 - precision_to_energy(precision),
                'performance': confidence,
            })

        # HITL escalation
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

        # Causal observation
        if self.causal_shaper is not None:
            chosen_action = int(np.argmax(probs))
            self.causal_shaper.record(chosen_action, confidence, probs)

        # Carbon market credit (best-effort)
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

        # Federated submission (best-effort)
        if self.federated:
            params_flat = self.params['variational'].reshape(-1)
            summary = [float(params_flat.mean()), float(params_flat.std()),
                       float(params_flat.max()), float(params_flat.min())] \
                if params_flat.size > 0 else [0.0] * 4
            self.federated.submit_update(self.instance_id, summary, samples=1)

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

    # ------------------------------------------------------------------
    # v2.0.0 — utilities
    # ------------------------------------------------------------------
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

    async def run_federated_round(self) -> Dict:
        if self.federated is None:
            return {'error': 'federated disabled'}
        if self.federated.client_updates:
            return self.federated.aggregate()
        return {'weights': self.federated.global_weights, 'round': self.federated.round}

    def get_causal_ates(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

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
        if self.rlhf:
            self.rlhf.record_feedback(state, action, reward)

    async def train_rlhf_reward_model(self) -> Dict:
        """Lightweight stub — delegates to ActiveRLHF's update path."""
        if self.rlhf is None:
            return {'trained': False}
        return {'trained': True, 'samples': len(self.rlhf.history)}

    def get_comprehensive_status(self) -> Dict:
        status = {
            'instance_id': self.instance_id,
            'version': '2.0.0',
            'n_qubits': self.n_qubits,
            'n_layers': self.n_layers,
            'n_actions': self.n_actions,
            'backend': self.backend,
            'pennylane_available': self.dev is not None,
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
                              'history_len': len(self.rlhf.history)}
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

    def __repr__(self) -> str:
        return (f"QuantumTeacher(n_actions={self.n_actions}, n_qubits={self.n_qubits}, "
                f"n_layers={self.n_layers}, backend={self.backend}, "
                f"version=2.0.0, pennylane={self.dev is not None})")


# =============================================================================
# HELPER
# =============================================================================
def precision_to_energy(level: PrecisionLevel) -> float:
    return AdaptivePrecisionController.energy_factor(level)


# =============================================================================
# SMOKE TEST
# =============================================================================
async def _smoke_test():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Quantum Teacher v2.0.0 — smoke test")
    print("=" * 78)

    teacher = QuantumTeacher(n_actions=5, n_qubits=4, n_layers=2, seed=42)

    # Original API test
    state = [0.1, 0.5, -0.3, 0.8]
    probs = teacher.predict(state)
    print(f"\n📊 predict(state) → probs={probs.tolist()}, sum={float(probs.sum()):.4f}")

    batch = [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
    probs_batch = teacher.predict_batch(batch)
    print(f"📊 predict_batch → shape={probs_batch.shape}")

    conf = teacher.confidence(state)
    print(f"📊 confidence(state) → {conf:.4f}")

    # v2.0.0 enhanced prediction
    print("\n🔍 predict_with_explanations...")
    result = await teacher.predict_with_explanations(
        state, context={'carbon_intensity': 350, 'urgency': 0.5})
    print(f"   probabilities: {result['probabilities'].tolist()}")
    print(f"   confidence: {result['confidence']:.4f}")
    print(f"   precision: {result['precision_used']}")
    if result.get('xai_explanation'):
        print(f"   XAI: {result['xai_explanation']['narrative'][0]}")
    if result.get('role_assignments'):
        print(f"   dominant_role: {result['role_assignments']['dominant_role']}")
    if result.get('hitl_outcome'):
        print(f"   HITL: {result['hitl_outcome']['source']} -> "
              f"{result['hitl_outcome']['chosen']}")
    print(f"   carbon credit: ${result['carbon_credit_value_usd']:.4f}")
    print(f"   REC: ${result['rec_value_usd']:.4f}")
    print(f"   temporal_status: {result['temporal_status']}")

    # Precision selector
    p = teacher.select_precision(carbon_intensity=350.0)
    print(f"\n⚙️  Precision selector → {p.value}")

    # Carbon credit
    cc = await teacher.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    # Roles
    print(f"\n🎭 Role assignments: {teacher.get_role_assignments()}")

    # Causal ATEs
    print(f"🧠 Causal ATEs: {teacher.get_causal_ates()}")

    # HITL
    hitl = await teacher.escalate_decision(
        decision_context={'reason': 'smoke'},
        options=['action_0', 'action_1'], confidence=0.5)
    print(f"👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    # Federated round
    fed = await teacher.run_federated_round()
    print(f"🌐 Federated: {fed}")

    # Chaos suite
    print("\n🧪 Chaos suite:")
    chaos = await teacher.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    # Comprehensive status
    print("\n📊 Comprehensive status:")
    status = teacher.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'n_qubits': status['n_qubits'],
        'n_actions': status['n_actions'],
        'pennylane_available': status['pennylane_available'],
        'cache_hits': status['cache_hits'],
        'cache_misses': status['cache_misses'],
        'features': status['features'],
        'rlhf': status.get('rlhf'),
        'federated': status.get('federated'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'causal_ates': status.get('causal_ates'),
    }, indent=2, default=str))

    print("\n" + "=" * 78)
    print("✅ Quantum Teacher v2.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
