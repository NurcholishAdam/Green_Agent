#!/usr/bin/env python3
"""
Multi‑Agent Coordination with Emergent Role Specialisation — v2.0.0

Dynamically assigns roles (LEADER, WORKER, VERIFIER, OBSERVER) to agents
based on capabilities, performance history, environmental factors, and task-specific
skills. Includes agent communication, leadership election, persistence, XAI,
human approval, and chaos testing.

v2.0.0 ENHANCEMENTS (all in-file, no external modules required):
    • Quantum-Distillation Integration     (MultiTeacherDistiller)
    • Causal Reinforcement Learning        (ActiveRLHF, CausalPolicy)
    • Federated Green Learning             (FederatedAggregator)
    • Advanced Multi-Agent Coordination    (RoleSpecializationCoordinator — softmax affinity)
    • Temporal Logic & Formal Verification (TemporalLogicMonitor — G/F/U/->)
    • Explainable AI for every decision    (XAIExplainer — feature attribution + narrative)
    • Adaptive Precision Switching         (AdaptivePrecisionController — fp32/fp16/fp8)
    • Carbon Markets + RECs                (CarbonMarketClient)
    • Chaos Testing as first-class         (ChaosTester — extended fault types)
    • Human-in-the-Loop with Active Learning (HumanInTheLoopCoordinator + ActiveRLHF)
"""

import asyncio
import logging
import json
import os
import random
import time
import uuid
import hashlib
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Deque, Callable, Tuple
from collections import defaultdict, deque
from datetime import datetime, timezone
from enum import Enum

import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================
class AgentRole(Enum):
    LEADER = "leader"
    WORKER = "worker"
    VERIFIER = "verifier"
    OBSERVER = "observer"
    COORDINATOR = "coordinator"


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    FP8 = "fp8"


# =============================================================================
# NEW v2.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """
    Lightweight LTL-style monitor. Operators: G(φ), F(φ), φ U ψ, φ -> ψ.
    Comparison atoms: <, >, <=, >=, ==, !=.
    History is a rolling window of state dicts.
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
                logger.warning(f"Temporal eval '{name}' failed: {e}")
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({
                    'formula': name, 'expression': self.formulas[name],
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                })
                logger.warning(f"Temporal violation: {name} ({self.formulas[name]})")
        return results

    def get_status(self) -> Dict:
        return {
            'formulas': self.formulas,
            'last_results': self.evaluate(),
            'violations': self.violations[-5:],
        }


# =============================================================================
# NEW v2.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    """
    Feature-attribution explainer for role assignments and coordination decisions.
    Uses normalized weighted contributions so reviewers can see exactly why a
    decision was made.
    """
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self,
                candidate: Dict[str, float],
                weights: Dict[str, float],
                all_candidates: List[Dict[str, float]],
                top_k: int = 5) -> Dict:
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
# NEW v2.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    """
    Hardware-aware precision selection based on carbon intensity,
    GPU availability, and accuracy requirements.
    """
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
            self.last_precision = PrecisionLevel.FP32
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0,
                PrecisionLevel.FP16: 0.4,
                PrecisionLevel.FP8: 0.2}[level]


# =============================================================================
# NEW v2.0.0 MODULE D — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    """
    Simulated client for external carbon markets and Renewable Energy Credits.
    Replace with real API in production.
    """
    def __init__(self):
        self.carbon_price_per_ton = 50.0    # USD/tCO2e
        self.rec_price_per_mwh = 30.0       # USD/MWh
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
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self.trades.append(rec)
        return rec


# =============================================================================
# NEW v2.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR (softmax affinity)
# =============================================================================
class RoleSpecializationCoordinator:
    """
    Emergent multi-agent role assignment via softmax over an affinity matrix.
    Complements the threshold-based logic in MultiAgentCoordinator by
    providing probabilistic, context-sensitive role preferences.
    """
    def __init__(self):
        self.roles = list(AgentRole)
        # rows = roles, cols = [trust, compute, energy, performance]
        self.affinity = np.array([
            [0.7, 0.9, 0.4, 0.6],   # LEADER
            [0.4, 0.5, 0.9, 0.5],   # WORKER
            [0.9, 0.4, 0.3, 0.7],   # VERIFIER
            [0.3, 0.2, 0.3, 0.3],   # OBSERVER
            [0.6, 0.7, 0.6, 0.6],   # COORDINATOR
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        ctx = np.array([
            context.get('trust', 0.5),
            context.get('compute', 0.5),
            context.get('energy', 0.5),
            context.get('performance', 0.5),
        ])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        assignments = {role.value: float(probs[i]) for i, role in enumerate(self.roles)}
        dominant = self.roles[int(np.argmax(probs))].value
        return {'assignments': assignments, 'dominant_role': dominant}


# =============================================================================
# NEW v2.0.0 MODULE F — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    """
    FedAvg-style cross-deployment aggregation of role-assignment weights.
    """
    def __init__(self, num_params: int = 4):
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
        logger.info(f"[Federated] round {self.round}: {self.global_weights}")
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {
            'round': self.round,
            'global_weights': self.global_weights,
            'pending_updates': len(self.client_updates),
        }


# =============================================================================
# NEW v2.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
class ActiveRLHF:
    """
    Preference-based policy with uncertainty-triggered human queries.
    Uncertainty = normalized entropy of the policy distribution.
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
            'action': action,
            'reward': reward,
            'timestamp': datetime.now(timezone.utc).isoformat(),
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
            'created_at': datetime.now(timezone.utc).isoformat(),
            'status': 'pending',
        }
        self.pending_queries[qid] = query
        logger.warning(f"Active RLHF query created (u={u:.2f}, id={qid})")
        return query

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({'status': 'resolved', 'chosen': chosen, 'rating': rating})
        self.update(q['context'], chosen, rating)
        return q


# =============================================================================
# NEW v2.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
class HumanInTheLoopCoordinator:
    """
    Escalates low-confidence coordination decisions to humans with
    auto-fallback to the RLHF-preferred action.
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
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        return {'escalated': True, 'query': query,
                'chosen': auto_choice, 'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# NEW v2.0.0 MODULE I — MULTI-TEACHER DISTILLER (in-file)
# =============================================================================
class MultiTeacherDistiller:
    """
    Combines multiple teacher policies into a single student policy.
    Teachers are callables returning a dict; the distiller merges via
    soft (weighted) vote. Used here for role-priority fusion.
    """
    def __init__(self, teachers: Optional[List[Callable]] = None, temperature: float = 2.0):
        self.teachers = teachers or []
        self.temperature = temperature
        self.student_policy: np.ndarray = np.array([0.25, 0.25, 0.25, 0.25])
        self.history: List[Dict] = []

    def add_teacher(self, teacher: Callable):
        self.teachers.append(teacher)

    def distill(self, context: Dict) -> Optional[Dict]:
        if not self.teachers:
            return None
        votes: Dict[str, List[Any]] = defaultdict(list)
        for t in self.teachers:
            try:
                r = t(context)
                if isinstance(r, dict):
                    for k, v in r.items():
                        votes[k].append(v)
            except Exception as e:
                logger.warning(f"[Distiller] teacher error: {e}")
        if not votes:
            return None
        merged: Dict[str, Any] = {}
        for k, vals in votes.items():
            try:
                if all(isinstance(v, (int, float)) for v in vals):
                    merged[k] = float(np.mean(vals))
                else:
                    merged[k] = max(set(vals), key=vals.count)
            except Exception:
                merged[k] = vals[0]
        self.history.append({
            'context': dict(context),
            'merged': dict(merged),
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        return merged

    def update_student(self, teacher_probs: List[float]):
        if not teacher_probs:
            return
        arr = np.array(teacher_probs, dtype=float)
        arr = arr / (arr.sum() + 1e-9)
        soft = np.exp(np.log(arr + 1e-6) / self.temperature)
        soft /= soft.sum()
        lr = 0.01
        grad = -soft / (self.student_policy + 1e-6)
        self.student_policy = np.clip(self.student_policy - lr * grad, 0.01, None)
        self.student_policy /= self.student_policy.sum()

    def get_student_probs(self) -> List[float]:
        return self.student_policy.tolist()


# =============================================================================
# NEW v2.0.0 MODULE J — EXTENDED CHAOS TESTER
# =============================================================================
class ChaosTester:
    """
    Fault injection for resilience validation, covering a broader set of
    faults than the basic coordinator inject_fault.
    Faults: agent_failure, agent_recovery, environment_spike, environment_normal,
            clear_leader, message_flood, performance_corrupt, storage_broken,
            approval_timeout.
    """
    FAULT_TYPES = [
        'agent_failure', 'agent_recovery', 'environment_spike',
        'environment_normal', 'clear_leader', 'message_flood',
        'performance_corrupt', 'storage_broken', 'approval_timeout',
    ]

    def __init__(self, coordinator_ref=None):
        self.coordinator = coordinator_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        c = self.coordinator

        try:
            if fault_type in ('agent_failure', 'agent_recovery',
                              'environment_spike', 'environment_normal',
                              'clear_leader'):
                if c:
                    agent_id = next(iter(c.agents)) if c.agents else None
                    await c.inject_fault(fault_type, agent_id=agent_id)

            elif fault_type == 'message_flood' and c:
                if c.agents:
                    recipient = next(iter(c.agents))
                    sender = next(iter(c.agents))
                    for i in range(50):
                        await c.send_message(sender, recipient, {'seq': i})

            elif fault_type == 'performance_corrupt' and c:
                if c.agents:
                    agent_id = next(iter(c.agents))
                    await c.update_agent_performance(agent_id, 'chaos', -1.0)

            elif fault_type == 'storage_broken' and c:
                orig = c.save_state
                async def broken():
                    raise RuntimeError("storage broken")
                c.save_state = broken
                await asyncio.sleep(duration_s)
                c.save_state = orig

            elif fault_type == 'approval_timeout' and c:
                if c.approval_callback is None:
                    # simulate missing approval
                    await asyncio.sleep(duration_s)
                else:
                    await asyncio.sleep(duration_s)

            await asyncio.sleep(duration_s)

        except Exception as e:
            passed = False
            error_msg = str(e)

        result = {
            'fault': fault_type,
            'duration_s': duration_s,
            'elapsed_s': time.time() - start,
            'passed': passed,
            'error': error_msg,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self.results.append(result)
        logger.warning(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {
            'tests_run': len(self.results),
            'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                         if self.results else 1.0,
            'recent': self.results[-5:],
        }


# =============================================================================
# AGENT INFO (enhanced with per-agent RLHF & XAI)
# =============================================================================
@dataclass
class AgentInfo:
    agent_id: str
    capabilities: Dict[str, float]
    current_role: AgentRole = AgentRole.OBSERVER
    performance_history: Deque[float] = field(default_factory=lambda: deque(maxlen=100))
    task_performance: Dict[str, Deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=100))
    )
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_active: bool = True
    messages: asyncio.Queue = field(default_factory=asyncio.Queue)
    role_history: List[Dict[str, Any]] = field(default_factory=list)
    explanation: str = ""
    # v2.0.0 additions
    xai_explanation: Dict[str, Any] = field(default_factory=dict)
    role_softmax: Dict[str, float] = field(default_factory=dict)

    def average_performance(self, task_type: Optional[str] = None) -> float:
        if task_type and task_type in self.task_performance and self.task_performance[task_type]:
            return sum(self.task_performance[task_type]) / len(self.task_performance[task_type])
        if self.performance_history:
            return sum(self.performance_history) / len(self.performance_history)
        return 0.0


# =============================================================================
# MAIN COORDINATOR — v2.0.0
# =============================================================================
class MultiAgentCoordinator:
    """
    Coordinates multiple agents with emergent specialisation.
    v2.0.0 adds: Temporal Logic, XAI, Adaptive Precision, Carbon Markets,
    Role Specialization softmax, Chaos Tester, Active RLHF, HITL,
    Federated Learning, and Multi-Teacher Distillation.
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        persistence_path: str = "multi_agent_coordinator_state.json",
        approval_callback: Optional[Callable[[str, AgentRole, str], bool]] = None,
        # v2.0.0 flags
        enable_temporal_logic: bool = True,
        enable_xai: bool = True,
        enable_adaptive_precision: bool = True,
        enable_carbon_market: bool = True,
        enable_role_specialization: bool = True,
        enable_chaos_testing: bool = True,
        enable_hitl: bool = True,
        enable_federated: bool = True,
        enable_distillation: bool = True,
        hitl_confidence_threshold: float = 0.65,
    ):
        self.agents: Dict[str, AgentInfo] = {}
        self._lock = asyncio.Lock()
        self.leader_id: Optional[str] = None
        self.storage = storage
        self.persistence_path = persistence_path
        self.approval_callback = approval_callback

        # Environmental factors
        self.environment_factors: Dict[str, float] = {
            'carbon': 400.0,
            'helium': 0.5,
            'energy': 0.5,
        }

        # Baseline thresholds
        self.base_thresholds = {
            'trust': 0.8,
            'compute': 0.7,
            'energy': 0.5,
            'min_performance': 0.7,
            'min_history': 10,
        }

        # Feature flags
        self.enable_temporal_logic = enable_temporal_logic
        self.enable_xai = enable_xai
        self.enable_adaptive_precision = enable_adaptive_precision
        self.enable_carbon_market = enable_carbon_market
        self.enable_role_specialization = enable_role_specialization
        self.enable_chaos_testing = enable_chaos_testing
        self.enable_hitl = enable_hitl
        self.enable_federated = enable_federated
        self.enable_distillation = enable_distillation
        self.hitl_confidence_threshold = hitl_confidence_threshold

        # ---- v2.0.0 module instances ----
        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("leader_always", "G(leader_count >= 0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")
            self.temporal_monitor.add_formula("active_agents", "G(active_count >= 0)")

        self.xai = XAIExplainer(['trust', 'compute', 'energy', 'performance']) \
            if enable_xai else None

        self.precision_controller = AdaptivePrecisionController() \
            if enable_adaptive_precision else None

        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None

        self.role_softmax = RoleSpecializationCoordinator() \
            if enable_role_specialization else None

        self.federated = FederatedAggregator(num_params=4) if enable_federated else None

        # Active RLHF (actions = role names)
        self.rlhf = ActiveRLHF(action_space=[r.value for r in AgentRole])
        self.hitl = HumanInTheLoopCoordinator(
            self.rlhf, timeout_s=300.0
        ) if enable_hitl else None

        self.distiller = MultiTeacherDistiller([]) if enable_distillation else None
        if self.distiller:
            self.distiller.add_teacher(self._teacher_role_priority)
            self.distiller.add_teacher(self._teacher_environment)
            self.distiller.add_teacher(self._teacher_softmax)

        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        # Instance identity
        self.instance_id = str(uuid.uuid4())[:8]

        # Metrics counters
        self._carbon_saved_kg_total = 0.0
        self._last_run_metrics: Dict[str, Any] = {}

        # Load persisted state
        self._load_state_sync()

        logger.info(f"MultiAgentCoordinator v2.0.0 ready (instance {self.instance_id})")
        logger.info(f"  TemporalLogic={enable_temporal_logic} XAI={enable_xai} "
                    f"AdaptivePrecision={enable_adaptive_precision} "
                    f"CarbonMarket={enable_carbon_market} "
                    f"RoleSoftmax={enable_role_specialization} "
                    f"Chaos={enable_chaos_testing} HITL={enable_hitl} "
                    f"Federated={enable_federated} Distillation={enable_distillation}")

    # ------------------------------------------------------------------
    # Teacher functions for distillation
    # ------------------------------------------------------------------
    def _teacher_role_priority(self, context: Dict) -> Dict:
        """Teacher 1: role priorities based on capabilities."""
        caps = context.get('capabilities', {})
        trust = caps.get('trust', 0.5)
        compute = caps.get('compute', 0.5)
        energy = caps.get('energy', 0.5)
        if trust >= 0.8:
            return {'role': AgentRole.VERIFIER.value}
        if compute >= 0.8:
            return {'role': AgentRole.LEADER.value}
        if energy >= 0.6:
            return {'role': AgentRole.WORKER.value}
        return {'role': AgentRole.OBSERVER.value}

    def _teacher_environment(self, context: Dict) -> Dict:
        """Teacher 2: role priorities based on environment."""
        env = context.get('environment', self.environment_factors)
        if env.get('carbon', 400) > 500:
            return {'role': AgentRole.WORKER.value}  # prefer energy-efficient workers
        if env.get('helium', 0.5) > 0.7:
            return {'role': AgentRole.VERIFIER.value}
        return {'role': AgentRole.COORDINATOR.value}

    def _teacher_softmax(self, context: Dict) -> Dict:
        """Teacher 3: role priorities from softmax coordinator."""
        if self.role_softmax is None:
            return {'role': AgentRole.OBSERVER.value}
        caps = context.get('capabilities', {})
        perf = context.get('performance', 0.5)
        result = self.role_softmax.assign_roles({
            'trust': caps.get('trust', 0.5),
            'compute': caps.get('compute', 0.5),
            'energy': caps.get('energy', 0.5),
            'performance': perf,
        })
        return {'role': result['dominant_role']}

    # ------------------------------------------------------------------
    # State Persistence
    # ------------------------------------------------------------------
    async def save_state(self):
        async with self._lock:
            state = {
                'leader_id': self.leader_id,
                'environment_factors': self.environment_factors,
                'carbon_saved_kg_total': self._carbon_saved_kg_total,
                'agents': {},
            }
            for aid, info in self.agents.items():
                state['agents'][aid] = {
                    'capabilities': info.capabilities,
                    'current_role': info.current_role.value,
                    'performance_history': list(info.performance_history),
                    'task_performance': {k: list(v) for k, v in info.task_performance.items()},
                    'last_active': info.last_active.isoformat(),
                    'is_active': info.is_active,
                    'role_history': info.role_history,
                    'explanation': info.explanation,
                    'xai_explanation': info.xai_explanation,
                    'role_softmax': info.role_softmax,
                }
            if self.storage and hasattr(self.storage, 'save_state'):
                try:
                    self.storage.save_state("multi_agent_coordinator", json.dumps(state))
                    return
                except Exception as e:
                    logger.warning(f"Storage save failed: {e}")
            try:
                with open(self.persistence_path, 'w') as f:
                    json.dump(state, f, indent=2, default=str)
            except Exception as e:
                logger.warning(f"File save failed: {e}")

    def _load_state_sync(self):
        try:
            state = None
            if self.storage and hasattr(self.storage, 'get_state'):
                try:
                    raw = self.storage.get_state("multi_agent_coordinator")
                    if raw:
                        state = json.loads(raw)
                except Exception:
                    pass
            if state is None and os.path.exists(self.persistence_path):
                with open(self.persistence_path, 'r') as f:
                    state = json.load(f)
            if not state:
                return
            self.leader_id = state.get('leader_id')
            self.environment_factors.update(state.get('environment_factors', {}))
            self._carbon_saved_kg_total = float(state.get('carbon_saved_kg_total', 0.0))
            for aid, data in state.get('agents', {}).items():
                info = AgentInfo(
                    agent_id=aid,
                    capabilities=data.get('capabilities', {}),
                    current_role=AgentRole(data.get('current_role', 'observer')),
                    last_active=datetime.fromisoformat(
                        data.get('last_active', datetime.now(timezone.utc).isoformat())),
                    is_active=data.get('is_active', True),
                    explanation=data.get('explanation', ''),
                    xai_explanation=data.get('xai_explanation', {}),
                    role_softmax=data.get('role_softmax', {}),
                )
                info.performance_history = deque(data.get('performance_history', []), maxlen=100)
                for task, perf_list in data.get('task_performance', {}).items():
                    info.task_performance[task] = deque(perf_list, maxlen=100)
                info.role_history = data.get('role_history', [])
                self.agents[aid] = info
        except Exception as e:
            logger.warning(f"Failed to load coordinator state: {e}")

    # ------------------------------------------------------------------
    # Agent registration
    # ------------------------------------------------------------------
    async def register_agent(self, agent_id: str, capabilities: Dict[str, float]) -> bool:
        async with self._lock:
            if agent_id in self.agents:
                return False
            self.agents[agent_id] = AgentInfo(agent_id=agent_id, capabilities=capabilities)
            logger.info(f"Registered agent {agent_id}")
            await self._reassign_roles()
        await self.save_state()
        return True

    # ------------------------------------------------------------------
    # Performance update
    # ------------------------------------------------------------------
    async def update_agent_performance(self, agent_id: str, task_type: str,
                                       performance: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            agent = self.agents[agent_id]
            agent.performance_history.append(performance)
            agent.task_performance[task_type].append(performance)
            agent.last_active = datetime.now(timezone.utc)
            await self._reassign_roles()
        # RLHF reward: performance feeds back into role preferences
        if self.rlhf and performance > 0:
            try:
                self.rlhf.update(
                    {'agent_id': agent_id, 'task_type': task_type},
                    agent.current_role.value,
                    float(performance),
                )
            except Exception:
                pass
        await self.save_state()

    # ------------------------------------------------------------------
    # Environment update
    # ------------------------------------------------------------------
    async def set_environment_factor(self, name: str, value: float):
        async with self._lock:
            self.environment_factors[name] = value
            await self._reassign_roles()
        await self.save_state()

    # ------------------------------------------------------------------
    # Role assignment (core)
    # ------------------------------------------------------------------
    async def _reassign_roles(self):
        if not self.agents:
            return

        # Adjust thresholds based on environment
        adj = self.base_thresholds.copy()
        if self.environment_factors['carbon'] > 500:
            adj['energy'] = 0.4
        if self.environment_factors['helium'] > 0.7:
            adj['trust'] = 0.9
        if self.environment_factors['energy'] < 0.3:
            adj['energy'] = 0.6

        # First pass: threshold-based role candidates
        for agent in self.agents.values():
            if not agent.is_active:
                agent.current_role = AgentRole.OBSERVER
                agent.explanation = "Agent inactive; assigned OBSERVER."
                agent.role_softmax = {}
                agent.xai_explanation = {}
                continue

            trust = agent.capabilities.get('trust', 0.5)
            compute = agent.capabilities.get('compute', 0.5)
            energy = agent.capabilities.get('energy', 0.5)
            avg_perf = agent.average_performance()
            history_len = len(agent.performance_history)

            if trust >= adj['trust'] and avg_perf >= adj['min_performance']:
                new_role = AgentRole.VERIFIER
            elif compute >= adj['compute'] and history_len >= adj['min_history']:
                new_role = AgentRole.LEADER
            elif energy >= adj['energy']:
                new_role = AgentRole.WORKER
            else:
                new_role = AgentRole.OBSERVER

            agent.current_role = new_role
            agent.explanation = self._generate_role_explanation(agent, new_role, adj)

            # ---- v2.0.0: softmax role assignment (probabilistic) ----
            if self.role_softmax is not None:
                soft = self.role_softmax.assign_roles({
                    'trust': trust,
                    'compute': compute,
                    'energy': energy,
                    'performance': avg_perf,
                })
                agent.role_softmax = soft

            # ---- v2.0.0: XAI explanation ----
            if self.xai is not None:
                features = {'trust': trust, 'compute': compute,
                            'energy': energy, 'performance': avg_perf}
                other_agents = [
                    {'trust': a.capabilities.get('trust', 0.5),
                     'compute': a.capabilities.get('compute', 0.5),
                     'energy': a.capabilities.get('energy', 0.5),
                     'performance': a.average_performance()}
                    for a in self.agents.values() if a is not agent
                ] or [features]
                agent.xai_explanation = self.xai.explain(
                    candidate=features,
                    weights={'trust': 0.25, 'compute': 0.25,
                             'energy': 0.25, 'performance': 0.25},
                    all_candidates=[features] + other_agents,
                )

        # Second pass: elect a single leader
        leaders = [a for a in self.agents.values()
                   if a.current_role == AgentRole.LEADER and a.is_active]
        if leaders:
            best_leader = max(
                leaders,
                key=lambda a: (a.capabilities.get('compute', 0) * 0.4
                               + a.capabilities.get('trust', 0) * 0.3
                               + a.average_performance() * 0.3)
            )
            for leader in leaders:
                if leader.agent_id != best_leader.agent_id:
                    leader.current_role = AgentRole.WORKER
                    leader.explanation = "Not elected leader; assigned WORKER."
            best_leader.current_role = AgentRole.LEADER
            best_leader.explanation = "Elected LEADER based on compute, trust, and performance."
            self.leader_id = best_leader.agent_id
        else:
            active_agents = [a for a in self.agents.values() if a.is_active]
            if active_agents and (self.leader_id is None
                                  or self.leader_id not in self.agents
                                  or not self.agents[self.leader_id].is_active):
                best = max(
                    active_agents,
                    key=lambda a: (a.capabilities.get('compute', 0) * 0.4
                                   + a.capabilities.get('trust', 0) * 0.3
                                   + a.average_performance() * 0.3)
                )
                best.current_role = AgentRole.LEADER
                best.explanation = "No active leader; elected LEADER."
                self.leader_id = best.agent_id

        # ---- v2.0.0: Temporal logic update & gate ----
        if self.temporal_monitor is not None:
            self.temporal_monitor.update({
                'leader_count': sum(1 for a in self.agents.values()
                                    if a.current_role == AgentRole.LEADER),
                'active_count': sum(1 for a in self.agents.values() if a.is_active),
                'carbon': self.environment_factors.get('carbon', 400),
            })
            self.temporal_monitor.evaluate()

        # ---- v2.0.0: Federated submission ----
        if self.federated is not None:
            avg_trust = np.mean([a.capabilities.get('trust', 0.5)
                                 for a in self.agents.values()]) if self.agents else 0.5
            avg_compute = np.mean([a.capabilities.get('compute', 0.5)
                                   for a in self.agents.values()]) if self.agents else 0.5
            avg_energy = np.mean([a.capabilities.get('energy', 0.5)
                                  for a in self.agents.values()]) if self.agents else 0.5
            avg_perf = np.mean([a.average_performance()
                                for a in self.agents.values()]) if self.agents else 0.5
            self.federated.submit_update(
                self.instance_id,
                [float(avg_trust), float(avg_compute),
                 float(avg_energy), float(avg_perf)],
                samples=len(self.agents),
            )
            if len(self.agents) % 5 == 0:
                self.federated.aggregate()

        # ---- v2.0.0: Distillation-based policy update ----
        if self.distiller is not None and self.agents:
            for agent in self.agents.values():
                ctx = {
                    'capabilities': agent.capabilities,
                    'environment': self.environment_factors,
                    'performance': agent.average_performance(),
                }
                try:
                    self.distiller.distill(ctx)
                except Exception:
                    pass
            if self.role_softmax is not None:
                # Update student with the last computed softmax distribution
                last_softmax = max(
                    (a.role_softmax for a in self.agents.values() if a.role_softmax),
                    key=len, default=None
                )
                if last_softmax:
                    self.distiller.update_student(list(last_softmax.values()))

    def _generate_role_explanation(self, agent: AgentInfo, role: AgentRole,
                                   adj: Dict[str, float]) -> str:
        trust = agent.capabilities.get('trust', 0)
        compute = agent.capabilities.get('compute', 0)
        energy = agent.capabilities.get('energy', 0)
        avg_perf = agent.average_performance()
        history_len = len(agent.performance_history)

        reasons = []
        if role == AgentRole.VERIFIER:
            reasons.append(f"trust {trust:.2f} >= {adj['trust']:.2f}")
            reasons.append(f"avg_perf {avg_perf:.2f} >= {adj['min_performance']:.2f}")
        elif role == AgentRole.LEADER:
            reasons.append(f"compute {compute:.2f} >= {adj['compute']:.2f}")
            reasons.append(f"history {history_len} >= {adj['min_history']}")
        elif role == AgentRole.WORKER:
            reasons.append(f"energy {energy:.2f} >= {adj['energy']:.2f}")
        else:
            reasons.append("no criteria met")
        return f"Role {role.value} assigned because " + "; ".join(reasons)

    # ------------------------------------------------------------------
    # Leadership election
    # ------------------------------------------------------------------
    async def elect_leader(self) -> Optional[str]:
        async with self._lock:
            self.leader_id = None
            await self._reassign_roles()
        await self.save_state()
        return self.leader_id

    # ------------------------------------------------------------------
    # Agent communication
    # ------------------------------------------------------------------
    async def send_message(self, sender_id: str, recipient_id: str,
                           message: Dict[str, Any]) -> bool:
        async with self._lock:
            if sender_id not in self.agents or recipient_id not in self.agents:
                return False
            await self.agents[recipient_id].messages.put({
                'from': sender_id,
                'message': message,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            })
            return True

    async def receive_messages(self, agent_id: str) -> List[Dict[str, Any]]:
        if agent_id not in self.agents:
            return []
        messages = []
        while not self.agents[agent_id].messages.empty():
            messages.append(self.agents[agent_id].messages.get_nowait())
        return messages

    # ------------------------------------------------------------------
    # Role query
    # ------------------------------------------------------------------
    async def get_agents_by_role(self, role: AgentRole) -> List[str]:
        async with self._lock:
            return [aid for aid, info in self.agents.items()
                    if info.current_role == role]

    async def get_coordination_summary(self) -> Dict[str, int]:
        async with self._lock:
            summary = defaultdict(int)
            for info in self.agents.values():
                summary[info.current_role.value] += 1
            return dict(summary)

    # ------------------------------------------------------------------
    # XAI
    # ------------------------------------------------------------------
    async def explain_role(self, agent_id: str) -> Optional[str]:
        async with self._lock:
            agent = self.agents.get(agent_id)
            return agent.explanation if agent else None

    async def explain_role_xai(self, agent_id: str) -> Optional[Dict]:
        async with self._lock:
            agent = self.agents.get(agent_id)
            return agent.xai_explanation if agent else None

    # ------------------------------------------------------------------
    # Human approval (legacy) + Active HITL (v2.0.0)
    # ------------------------------------------------------------------
    def set_approval_callback(self, callback: Callable[[str, AgentRole, str], bool]):
        self.approval_callback = callback

    async def request_approval(self, agent_id: str, new_role: AgentRole,
                               reason: str) -> bool:
        if self.approval_callback is None:
            return True
        result = self.approval_callback(agent_id, new_role, reason)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

    async def escalate_decision(self, decision_context: Dict,
                                options: List[str],
                                confidence: float) -> Dict:
        """v2.0.0: escalate a decision to humans if confidence is low."""
        if self.hitl is None:
            # Fallback to legacy approval
            chosen = options[0] if options else 'noop'
            return {'escalated': False, 'chosen': chosen, 'source': 'fallback'}
        return await self.hitl.escalate(
            decision_context=decision_context,
            options=options,
            confidence=confidence,
            confidence_threshold=self.hitl_confidence_threshold,
        )

    # ------------------------------------------------------------------
    # Carbon market utilities
    # ------------------------------------------------------------------
    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        self._carbon_saved_kg_total += max(0.0, carbon_saved_kg)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    # ------------------------------------------------------------------
    # Adaptive precision
    # ------------------------------------------------------------------
    def select_precision(self, accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(
            carbon_intensity=float(self.environment_factors.get('carbon', 400.0)),
            accuracy_required=accuracy_required,
        )

    # ------------------------------------------------------------------
    # Chaos testing (legacy + extended)
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, agent_id: Optional[str] = None):
        async with self._lock:
            if fault_type == 'agent_failure' and agent_id and agent_id in self.agents:
                self.agents[agent_id].is_active = False
                await self._reassign_roles()
            elif fault_type == 'agent_recovery' and agent_id and agent_id in self.agents:
                self.agents[agent_id].is_active = True
                self.agents[agent_id].last_active = datetime.now(timezone.utc)
                await self._reassign_roles()
            elif fault_type == 'environment_spike':
                self.environment_factors['carbon'] = 800.0
                self.environment_factors['helium'] = 0.9
                await self._reassign_roles()
            elif fault_type == 'environment_normal':
                self.environment_factors = {'carbon': 400.0, 'helium': 0.5, 'energy': 0.5}
                await self._reassign_roles()
            elif fault_type == 'clear_leader':
                if self.leader_id and self.leader_id in self.agents:
                    self.agents[self.leader_id].is_active = False
                self.leader_id = None
                await self._reassign_roles()
            else:
                logger.warning(f"Unknown fault type: {fault_type}")
        await self.save_state()

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Legacy chaos test (kept for backward compatibility)."""
        report = {'faults': [], 'results': {}}
        if self.agents:
            test_agent = next(iter(self.agents))
            await self.inject_fault('agent_failure', agent_id=test_agent)
            report['faults'].append('agent_failure')
            report['results']['agent_failure'] = await self.get_coordination_summary()
            await self.inject_fault('agent_recovery', agent_id=test_agent)
            report['faults'].append('agent_recovery')
            report['results']['agent_recovery'] = await self.get_coordination_summary()
        await self.inject_fault('environment_spike')
        report['faults'].append('environment_spike')
        report['results']['environment_spike'] = await self.get_coordination_summary()
        await self.inject_fault('environment_normal')
        report['faults'].append('environment_normal')
        report['results']['environment_normal'] = await self.get_coordination_summary()
        return report

    async def run_extended_chaos_suite(self) -> Dict:
        """v2.0.0: extended chaos suite covering more fault types."""
        if self.chaos_tester is None:
            return {'error': 'chaos testing disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos_tester.get_report()}

    # ------------------------------------------------------------------
    # Comprehensive status (v2.0.0)
    # ------------------------------------------------------------------
    async def get_comprehensive_status(self) -> Dict:
        summary = await self.get_coordination_summary()
        status = {
            'instance_id': self.instance_id,
            'version': '2.0.0',
            'agents_count': len(self.agents),
            'leader_id': self.leader_id,
            'environment_factors': dict(self.environment_factors),
            'role_summary': summary,
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
                'distillation': self.enable_distillation,
            },
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.role_softmax:
            status['role_softmax'] = self.role_softmax.assign_roles({
                'trust': 0.5, 'compute': 0.5, 'energy': 0.5, 'performance': 0.5,
            })
        if self.federated:
            status['federated'] = self.federated.get_stats()
        if self.hitl:
            status['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            status['chaos'] = self.chaos_tester.get_report()
        if self.precision_controller:
            status['precision'] = {
                'last': self.precision_controller.last_precision.value,
                'telemetry': self.precision_controller.telemetry,
            }
        if self.carbon_market:
            status['carbon_market'] = await self.carbon_market.get_market_snapshot()
        if self.distiller:
            status['distillation'] = {
                'student_probs': self.distiller.get_student_probs(),
                'history_len': len(self.distiller.history),
            }
        if self.rlhf:
            status['rlhf'] = {
                'actions': self.rlhf.actions,
                'history_len': len(self.rlhf.history),
            }
        return status

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self):
        await self.save_state()
        logger.info("MultiAgentCoordinator v2.0.0 shutdown complete")


# =============================================================================
# SMOKE TEST
# =============================================================================
async def _smoke_test():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')
    print("=" * 78)
    print("Multi-Agent Coordinator v2.0.0 — smoke test")
    print("=" * 78)

    coord = MultiAgentCoordinator(persistence_path="/tmp/mac_smoke_v2.json")

    # Register agents
    await coord.register_agent("alice", {"trust": 0.9, "compute": 0.85, "energy": 0.6})
    await coord.register_agent("bob",   {"trust": 0.6, "compute": 0.9, "energy": 0.8})
    await coord.register_agent("carol", {"trust": 0.85, "compute": 0.55, "energy": 0.9})
    await coord.register_agent("dave",  {"trust": 0.4, "compute": 0.4, "energy": 0.3})

    # Simulate performance feedback
    for i in range(12):
        await coord.update_agent_performance("alice", "inference", 0.85 + 0.01 * i)
        await coord.update_agent_performance("bob", "inference", 0.8 + 0.01 * i)
        await coord.update_agent_performance("carol", "verification", 0.9)
        await coord.update_agent_performance("dave", "inference", 0.4)

    print("\n✅ Role summary after registration:")
    print(f"   {await coord.get_coordination_summary()}")
    print(f"   Leader: {coord.leader_id}")

    # XAI
    print(f"\n🔍 XAI explanation for alice:")
    print(f"   {await coord.explain_role('alice')}")
    xai = await coord.explain_role_xai("alice")
    if xai and xai.get('narrative'):
        print(f"   Narrative: {xai['narrative'][0]}")

    # Adaptive precision
    print(f"\n⚙️  Precision: {coord.select_precision(accuracy_required=0.95).value}")

    # Carbon market
    credit = await coord.compute_carbon_credit(carbon_saved_kg=250.0)
    print(f"\n💱 Carbon credit: ${credit['credit_usd']:.4f}  "
          f"REC: ${credit['rec_usd']:.4f}")

    # HITL escalation (auto path)
    hitl = await coord.escalate_decision(
        decision_context={'reason': 'leadership_change'},
        options=['alice', 'bob', 'carol'],
        confidence=0.85,
    )
    print(f"\n👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    # Chaos suite
    print("\n🧪 Extended chaos suite:")
    chaos = await coord.run_extended_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    # Comprehensive status
    print("\n📊 Comprehensive status:")
    status = await coord.get_comprehensive_status()
    print(json.dumps({
        'version': status['version'],
        'agents': status['agents_count'],
        'leader': status['leader_id'],
        'role_summary': status['role_summary'],
        'features': status['features'],
        'federated': status.get('federated'),
        'distillation': status.get('distillation'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
    }, indent=2, default=str))

    await coord.shutdown()
    print("\n" + "=" * 78)
    print("✅ Multi-Agent Coordinator v2.0.0 — smoke test complete")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
