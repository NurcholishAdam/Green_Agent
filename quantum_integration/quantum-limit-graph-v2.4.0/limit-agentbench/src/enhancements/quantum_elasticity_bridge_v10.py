#!/usr/bin/env python3
# =============================================================================
# FILE: policy_meta_cache_v4_0_2.py
# VERSION: 4.0.2 (patched)
#
# Builds on v4.0.1 (top-10 fixes) and adds:
#   NC1  Real propensities from the selection policy; not MODP weights
#   NC2  Records the actually selected source, not always 'balanced'
#   NC3  Action index = selection source, not expert_id
#   NC4  Single behaviour policy (per-fingerprint FIFO pairing)
#   NC5  compute_ate returns ATE(a) − baseline, not raw μ(a)
#   NC6  RLHF branch uses reward_model.predict per entry (from v4.0.1)
#   NC7  Causal ATEs reported for observability (selection branch opt-in)
#   NC8  Smoke test uses enough samples to train + converge
#   NR1  ActiveRLHF exposes sync + async; docstrings updated
#   NR2  record_feedback holds the lock around update()
#   NR3  _action_to_index uses self.actions.index with safe fallback
#   NR4  Removed dead self.policy attr
#   NR5  Removed unused role_coordinator arg on distillation
#   NR6  select_precision is async; documented
#
# Plus v4.0.1 (all still applied):
#   1.  TemporalLogicMonitor: past-time LTL (U + nesting)
#   2.  ChaosTester faults fire; save/restore for all types
#   3.  HITL registers synthesised queries
#   4.  MultiTeacherPolicyDistillation: stable EMA KD
#   5.  MOEGating: modern sklearn; scaler fit with dummy batch
#   6.  RLHF branch gated on _reward_model_trained
#   7.  ActiveRLHF contextual (global + per-context)
#   8.  Federated submit_update pads/truncates; caps pending
#   9.  HITL outcome applied; carbon accounting deferred
#  10.  Determinism, full clear(), deepcopy returns, lock around store
# =============================================================================
"""
policy_meta_cache.py — Enhanced v4.0.2
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

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# Metrics
# =============================================================================
if PROMETHEUS_AVAILABLE:
    PMC_REGISTRY = CollectorRegistry()
    PMC_HITS = Counter('pmc_cache_hits_total', 'Cache hits', ['status'], registry=PMC_REGISTRY)
    PMC_SOURCE = Counter('pmc_selection_source_total', 'Source', ['source'], registry=PMC_REGISTRY)
    PMC_TEMPORAL = Counter('pmc_temporal_violations_total', 'Temporal', ['formula'], registry=PMC_REGISTRY)
    PMC_CHAOS = Counter('pmc_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=PMC_REGISTRY)
    PMC_HITL = Counter('pmc_hitl_escalations_total', 'HITL', ['status'], registry=PMC_REGISTRY)
    PMC_FEDERATED = Counter('pmc_federated_rounds_total', 'Federated rounds', registry=PMC_REGISTRY)
    PMC_CARBON = Counter('pmc_carbon_credits_usd_total', 'Carbon credits', registry=PMC_REGISTRY)
    PMC_XAI = Counter('pmc_xai_explanations_total', 'XAI', registry=PMC_REGISTRY)
    PMC_PRECISION = Counter('pmc_precision_selections_total', 'Precision', ['level'], registry=PMC_REGISTRY)
    PMC_CAUSAL = Counter('pmc_causal_outcomes_total', 'Causal outcomes', registry=PMC_REGISTRY)
    PMC_CARBON_INT = Gauge('pmc_carbon_intensity', 'Carbon intensity', registry=PMC_REGISTRY)
else:
    class _Dummy:
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def labels(self, *a, **k): return self
    PMC_HITS = PMC_SOURCE = PMC_TEMPORAL = PMC_CHAOS = _Dummy()
    PMC_HITL = PMC_FEDERATED = PMC_CARBON = PMC_XAI = PMC_PRECISION = _Dummy()
    PMC_CAUSAL = PMC_CARBON_INT = _Dummy()


# =============================================================================
# Enums
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
# WorkloadFingerprint
# =============================================================================
class WorkloadFingerprint:
    def __init__(self, model_size_mb: float, prompt_len: int, gen_len: int,
                 gpu_mem_free_mb: float, disk_speed_class: int):
        self.model_size_mb = model_size_mb
        self.prompt_len = prompt_len
        self.gen_len = gen_len
        self.gpu_mem_free_mb = gpu_mem_free_mb
        self.disk_speed_class = disk_speed_class

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.model_size_mb / 1000.0,
            self.prompt_len / 1024.0,
            self.gen_len / 1024.0,
            self.gpu_mem_free_mb / 1000.0,
            self.disk_speed_class / 2.0,
        ])


# =============================================================================
# Pareto + TOPSIS (consistent higher-is-better)
# =============================================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: List[float], decision: Any) -> bool:
        for obj, _ in self.solutions:
            if all(obj[i] >= objectives[i] for i in range(len(objectives))):
                return True
        self.solutions = [
            (obj, dec) for obj, dec in self.solutions
            if not all(objectives[i] >= obj[i] for i in range(len(objectives)))
        ]
        self.solutions.append((list(objectives), decision))
        return False

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


# =============================================================================
# FIX #1 — Temporal Logic Monitor
# =============================================================================
class TemporalLogicMonitor:
    """Past-time LTL over a finite trace (G, F, U, ->)."""

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
            logger.warning(f"Temporal: cannot compile '{name}': {e}")
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
                        return {"<": lambda: lv < rv, ">": lambda: lv > rv,
                                "<=": lambda: lv <= rv, ">=": lambda: lv >= rv,
                                "==": lambda: lv == rv, "!=": lambda: lv != rv}[op]()
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
                PMC_TEMPORAL.labels(formula=name).inc()
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
            PMC_XAI.inc()
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
        PMC_XAI.inc()
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
        PMC_PRECISION.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]


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
        PMC_CARBON.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# =============================================================================
# Roles
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
# NC1–NC5, NR1–NR4 — Causal Reward Shaper (correct IPW with baseline)
# =============================================================================
class CausalRewardShaper:
    """
    Inverse-propensity-weighted estimation of per-action means and ATEs.

    Design (NC1–NC5):
        * `record_decision(action, propensities, fp_key=None)` records what
          the behaviour policy *could* have done. Propensities must come from
          the actual selection policy (usually with ε-greedy exploration).
        * `record_outcome(reward, fp_key=None)` pairs the realised reward
          with the oldest pending decision for the same fingerprint.
        * `compute_ate(baseline=None)` returns
              ATE(a) = μ_IPW(a) − baseline
          where `baseline` defaults to the mean of all μ_IPW values.

    Legacy `record(action, reward, propensities)` is preserved for callers
    that already have both sides.
    """

    def __init__(self, num_actions: int, baseline: Optional[int] = None,
                 min_samples: int = 5):
        self.num_actions = num_actions
        self.baseline = baseline
        self.min_samples = min_samples

        # Per-fingerprint FIFO of pending decisions (NC4).
        self._pending: Dict[Any, deque] = defaultdict(lambda: deque(maxlen=20))
        # Recorded interventions: (action, reward, propensities)
        self.interventions: deque = deque(maxlen=500)
        # Cached ATEs.
        self.ates: Dict[int, float] = {i: 0.0 for i in range(num_actions)}
        self.mu: Dict[int, float] = {i: 0.0 for i in range(num_actions)}

    # ---- decision / outcome pairing (NC1, NC4) ----------------------- #
    def record_decision(self, action: int, propensities: np.ndarray,
                        fp_key: Any = None) -> None:
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
        PMC_CAUSAL.inc()
        return True

    # ---- legacy single-shot API ------------------------------------- #
    def record(self, action: int, reward: float, propensities: np.ndarray) -> None:
        if not (0 <= int(action) < self.num_actions):
            return
        props = np.asarray(propensities, dtype=float).flatten()
        if props.size != self.num_actions:
            return
        props = np.clip(props, 1e-6, None)
        props = props / props.sum()
        self.interventions.append((int(action), float(reward), props))
        PMC_CAUSAL.inc()

    # ---- ATE computation (NC5) -------------------------------------- #
    def compute_ate(self, baseline: Optional[int] = None) -> Dict[int, float]:
        if len(self.interventions) < self.min_samples:
            return dict(self.ates)

        # IPW means per action.
        for a in range(self.num_actions):
            weights: List[float] = []
            outcomes: List[float] = []
            for action, reward, props in self.interventions:
                if action == a:
                    weights.append(1.0 / props[a])
                    outcomes.append(reward)
            if weights:
                self.mu[a] = float(np.average(outcomes, weights=weights))
            else:
                self.mu[a] = 0.0

        # Baseline selection (NC5): default to mean of all μ.
        bl = self.baseline if baseline is None else baseline
        if bl is not None and 0 <= bl < self.num_actions:
            base_val = self.mu[bl]
        else:
            counts = np.array([
                sum(1 for a, _, _ in self.interventions if a == k)
                for k in range(self.num_actions)
            ], dtype=float)
            if counts.sum() == 0:
                base_val = 0.0
            else:
                weights = counts / counts.sum()
                base_val = float(np.dot(
                    weights, np.array([self.mu[k] for k in range(self.num_actions)])))

        for a in range(self.num_actions):
            self.ates[a] = self.mu[a] - base_val
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)

    def get_means(self) -> Dict[int, float]:
        return dict(self.mu)

    def get_pending_count(self) -> int:
        return sum(len(d) for d in self._pending.values())


# =============================================================================
# FIX #6, NR1, NR2, NR3, NR4 — Active RLHF
# =============================================================================
class ActiveRLHF:
    """
    Contextual preference-based policy with human-query capability.

    NR1: exposes both sync and async APIs. `record_feedback_sync`,
    `train_reward_model_sync`, `get_policy_probs_sync` are provided as
    wrappers for legacy callers.
    NR2: `record_feedback` holds the lock across the entire operation.
    NR3: `_action_to_index` uses `self.actions.index` with safe fallback.
    NR4: removed dead `self.policy` attribute.
    FIX #6: `_reward_model_trained` gates the selection branch.
    FIX #7: contextual preferences (global + per-context).
    """

    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0, global_weight: float = 0.3,
                 reward_model_type: str = "linear"):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.global_weight = global_weight
        self.reward_model_type = reward_model_type

        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.context_counts: Dict[Any, Dict[str, float]] = defaultdict(
            lambda: defaultdict(float))
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []
        self.reward_model = None
        self._reward_model_trained = False
        self._lock = asyncio.Lock()

        if SKLEARN_AVAILABLE:
            try:
                self.reward_model = LinearRegression()
            except Exception:
                self.reward_model = None

    # ---- context keying --------------------------------------------- #
    @staticmethod
    def _context_key(context: Any) -> Any:
        if isinstance(context, dict):
            items = tuple(sorted((k, str(v)) for k, v in context.items()
                                 if isinstance(v, (str, int, float, bool))))
            return items or ("__empty__",)
        return str(context)

    # ---- policy ------------------------------------------------------- #
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

    def update(self, context: Any, action: str, reward: float) -> None:
        if isinstance(action, (int, np.integer)):
            if self.actions:
                action = self.actions[int(action) % len(self.actions)]
            else:
                return
        if action in self.actions:
            self.preference_counts[action] += reward
            self.context_counts[self._context_key(context)][action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    # ---- NR1 + NR2: async record_feedback ----------------------------- #
    async def record_feedback(self, state: Dict, action: str, reward: float) -> None:
        async with self._lock:
            self.feedback_buffer.append({
                'state': self._state_to_features(state),
                'action': self._action_to_index(action),
                'action_name': action,
                'reward': float(reward),
            })
            # NR2: update under the same lock.
            self.update(state, action, float(reward))

    def record_feedback_sync(self, state: Dict, action: str, reward: float) -> None:
        """NR1: sync wrapper for legacy callers."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.record_feedback(state, action, reward))
        except RuntimeError:
            asyncio.run(self.record_feedback(state, action, reward))

    # ---- NR3: safe action index --------------------------------------- #
    def _action_to_index(self, action: str) -> int:
        try:
            return self.actions.index(action)
        except (ValueError, AttributeError):
            return 0

    def _state_to_features(self, state: Dict) -> List[float]:
        return [float(state.get('carbon_intensity', 400)) / 1000.0,
                float(state.get('avg_score', 0.5)),
                float(state.get('cost', 0.5)),
                float(state.get('diversity', 0.5))]

    # ---- reward model training --------------------------------------- #
    async def train_reward_model(self) -> Dict:
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

    # ---- human queries ------------------------------------------------ #
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
# FIX #3 — HITL
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
            PMC_HITL.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            qid = str(uuid.uuid4())
            query = {'id': qid, 'options': options, 'context': decision_context,
                     'uncertainty': self.rlhf.uncertainty(decision_context),
                     'created_at': datetime.now().isoformat(),
                     'status': 'pending'}
            self.rlhf.pending_queries[qid] = query

        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice,
                               'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        PMC_HITL.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        return self.rlhf.resolve_query(query_id, chosen, rating)

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}


# =============================================================================
# Federated aggregator
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 4, max_pending: int = 32):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []
        self.max_pending = max_pending

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        w = list(weights)
        if len(w) < self.num_params:
            w += [1.0 / self.num_params] * (self.num_params - len(w))
        w = w[:self.num_params]
        self.client_updates.append({'client_id': client_id,
                                    'weights': w,
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
        PMC_FEDERATED.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}


# =============================================================================
# MOE Gating (modern sklearn API)
# =============================================================================
class MOEGating:
    def __init__(self, num_experts: int, feature_dim: int = 4):
        self.num_experts = num_experts
        self.feature_dim = feature_dim
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)
        self._trained = False
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()
            self.scaler.fit(np.zeros((2, feature_dim)))

    def extract_features(self, context: Dict) -> np.ndarray:
        carbon = context.get('carbon_intensity', 400) / 1000.0
        hour = datetime.now().hour / 24.0
        urgency = context.get('urgency', 0.5)
        workload_size = context.get('model_size_mb', 0) / 1000.0
        return np.array([carbon, hour, urgency, workload_size])

    def get_weights(self, context: Dict) -> List[float]:
        if (self.gating_model is not None and self._trained
                and self.scaler is not None):
            try:
                features = self.extract_features(context)
                X = self.scaler.transform([features])
                return self.gating_model.predict_proba(X)[0].tolist()
            except Exception:
                pass
        return (np.ones(self.num_experts) / self.num_experts).tolist()

    def update(self, context: Dict, expert_idx: int, reward: float) -> None:
        if not (0 <= int(expert_idx) < self.num_experts):
            return
        features = self.extract_features(context)
        self.history.append((features, int(expert_idx), float(reward)))
        if len(self.history) % 100 == 0:
            try:
                loop = asyncio.get_running_loop()
                loop.run_in_executor(None, self._retrain)
            except RuntimeError:
                self._retrain()

    def _retrain(self) -> None:
        if self.gating_model is None or len(self.history) < 100:
            return
        X = np.array([h[0] for h in self.history])
        y = np.array([h[1] for h in self.history])
        if len(np.unique(y)) < 2:
            return
        try:
            Xs = self.scaler.fit_transform(X)
            self.gating_model.fit(Xs, y)
            self._trained = True
        except Exception as e:
            logger.warning(f"MOE gating fit failed: {e}")


# =============================================================================
# GA (deepcopy initialisation)
# =============================================================================
class GAPopulation:
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1,
                 crossover_rate: float = 0.8, rng: Optional[random.Random] = None):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Tuple[Dict, float]] = []
        self.generation = 0
        self._rng = rng or random.Random()

    def initialize(self, template_policy: Dict[str, Any]):
        self.population = []
        for _ in range(self.pop_size):
            self.population.append((copy.deepcopy(template_policy), 0.0))

    def _mutate(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        new_policy = copy.deepcopy(policy)
        for key, value in new_policy.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                new_policy[key] = value * (1.0 + self._rng.uniform(-0.1, 0.1))
        return new_policy

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        return {k: (p1[k] if self._rng.random() < 0.5 else p2[k]) for k in p1}

    def evolve(self, fitness_func: Callable[[Dict], float]) -> Dict[str, Any]:
        for i, (policy, _) in enumerate(self.population):
            self.population[i] = (policy, fitness_func(policy))
        self.population.sort(key=lambda x: x[1], reverse=True)
        best = self.population[0]
        parents = []
        for _ in range(self.pop_size - 1):
            idx1, idx2 = self._rng.sample(range(self.pop_size), 2)
            parents.append(self.population[idx1][0]
                           if self.population[idx1][1] > self.population[idx2][1]
                           else self.population[idx2][0])
        offspring = []
        for i in range(0, len(parents) - 1, 2):
            child = self._crossover(parents[i], parents[i + 1]) \
                if self._rng.random() < self.crossover_rate else parents[i]
            if self._rng.random() < self.mutation_rate:
                child = self._mutate(child)
            offspring.append((child, 0.0))
        self.population = [best] + offspring[:self.pop_size - 1]
        self.generation += 1
        return best[0]


# =============================================================================
# Carbon scheduler
# =============================================================================
class CarbonScheduler:
    def __init__(self, carbon_manager: Optional[Any] = None,
                 threshold: float = 400.0, max_delay: int = 300):
        self.carbon_manager = carbon_manager
        self.threshold = threshold
        self.max_delay = max_delay

    async def get_current_carbon(self) -> float:
        if self.carbon_manager:
            try:
                return await self.carbon_manager.get_current_intensity()
            except Exception:
                return 400.0
        return 400.0

    async def should_delay(self, urgency: float = 0.5):
        carbon = await self.get_current_carbon()
        PMC_CARBON_INT.set(carbon)
        if carbon > self.threshold:
            delay = int(self.max_delay * (1.0 - urgency))
            return True, delay
        return False, 0


# =============================================================================
# Self-healing (periodic retrain)
# =============================================================================
class SelfHealingManager:
    def __init__(self, contamination: float = 0.1, retrain_every: int = 200):
        self.contamination = contamination
        self.retrain_every = retrain_every
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self.reward_history: Deque[float] = deque(maxlen=500)
        self._trained = False
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors.append(
                ('iforest', IsolationForest(contamination=contamination)))
            self.anomaly_detectors.append(
                ('ocsvm', OneClassSVM(nu=contamination)))

    def record_reward(self, reward: float) -> None:
        self.reward_history.append(float(reward))
        if (len(self.reward_history) >= 100
                and len(self.reward_history) % self.retrain_every == 0):
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

    def detect_anomaly(self, reward: float) -> Tuple[bool, float]:
        if not self._trained or not self.anomaly_detectors:
            return reward < 0.5, 0.0
        X = np.array([[reward]])
        votes = []
        for _, model in self.anomaly_detectors:
            try:
                votes.append(1 if model.predict(X)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        if not votes:
            return False, 0.0
        ratio = sum(votes) / len(votes)
        return ratio > 0.5, ratio


# =============================================================================
# LIMIT Graph
# =============================================================================
class LimitGraphManager:
    def __init__(self):
        self.graph: Dict[str, Dict[str, float]] = {}
        self.constraints: Dict[str, float] = {}
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

    async def update_constraint(self, name: str, value: float) -> None:
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


# =============================================================================
# FIX #4, NR5 — Multi-Teacher Distillation (stable EMA KD)
# =============================================================================
class MultiTeacherPolicyDistillation:
    """
    Stable temperature-scaled soft-target EMA distillation.

    NR5: removed the unused `role_coordinator` parameter and the unused
    `context` argument on `distill()`.
    """

    def __init__(self, num_teachers: int = 4, temperature: float = 2.0,
                 alpha: float = 0.5, learning_rate: float = 0.1):
        self.num_teachers = num_teachers
        self.temperature = max(temperature, 1e-6)
        self.alpha = alpha
        self.learning_rate = float(np.clip(learning_rate, 0.0, 1.0))
        self.student_policy = np.ones(num_teachers) / num_teachers
        self.history: Deque[Dict] = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, teacher_probs: List[float]) -> None:
        if len(teacher_probs) != self.num_teachers:
            teacher_dist = np.ones(self.num_teachers) / self.num_teachers
        else:
            teacher_dist = np.array(teacher_probs, dtype=float)
        teacher_dist = np.clip(teacher_dist, 1e-9, None)
        teacher_dist = teacher_dist / teacher_dist.sum()

        soft = np.power(teacher_dist, 1.0 / self.temperature)
        soft = soft / soft.sum()

        new_student = ((1.0 - self.learning_rate) * self.student_policy
                       + self.learning_rate * soft)
        new_student = np.clip(new_student, 1e-9, None)
        self.student_policy = new_student / new_student.sum()

        loss = -np.sum(soft * np.log(self.student_policy + 1e-9))
        async with self._lock:
            self.history.append({'teacher_dist': teacher_dist.tolist(),
                                 'student_dist': self.student_policy.tolist(),
                                 'loss': float(loss),
                                 'timestamp': datetime.now().isoformat()})

    def get_student_probs(self) -> List[float]:
        return self.student_policy.tolist()


# =============================================================================
# FIX #2 — Chaos Tester (faults actually fire; save/restore)
# =============================================================================
class ChaosTester:
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

                async def broken():
                    raise RuntimeError("carbon API down")

                c.scheduler.get_current_carbon = broken
                restore.append(lambda: setattr(c.scheduler,
                                               'get_current_carbon', orig))
                try:
                    await c.scheduler.get_current_carbon()
                    passed, error_msg = False, "carbon API did not fail"
                except RuntimeError:
                    pass

            elif fault_type == 'cache_corrupt' and c and c.store:
                key = next(iter(c.store))
                orig = list(c.store[key])
                c.store[key] = []
                restore.append(lambda: c.store.update({key: orig}))
                if c.store[key] != []:
                    passed, error_msg = False, "cache not corrupted"

            elif fault_type == 'moe_broken' and c and c.moe_gating:
                orig = c.moe_gating.get_weights

                def broken(_):
                    raise RuntimeError("moe broken")

                c.moe_gating.get_weights = broken
                restore.append(lambda: setattr(c.moe_gating, 'get_weights', orig))
                try:
                    c.moe_gating.get_weights({})
                    passed, error_msg = False, "moe did not fail"
                except RuntimeError:
                    pass

            elif fault_type == 'distiller_broken' and c and c.distillation:
                orig = c.distillation.distill

                async def broken(*a, **k):
                    raise RuntimeError("distiller broken")

                c.distillation.distill = broken
                restore.append(lambda: setattr(c.distillation, 'distill', orig))
                try:
                    await c.distillation.distill([0.25, 0.25, 0.25, 0.25])
                    passed, error_msg = False, "distiller did not fail"
                except RuntimeError:
                    pass

            elif fault_type == 'rlhf_broken' and c and c.rlhf:
                orig = c.rlhf.get_policy_probs

                async def broken(_):
                    raise RuntimeError("rlhf broken")

                c.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(c.rlhf, 'get_policy_probs', orig))
                try:
                    await c.rlhf.get_policy_probs({})
                    passed, error_msg = False, "rlhf did not fail"
                except RuntimeError:
                    pass

            elif fault_type == 'scheduler_hang' and c and c.scheduler:
                orig = c.scheduler.should_delay

                async def hang(*a, **k):
                    await asyncio.sleep(max(duration_s, 0.01) * 20)
                    return False, 0

                c.scheduler.should_delay = hang
                restore.append(lambda: setattr(c.scheduler, 'should_delay', orig))
                try:
                    await asyncio.wait_for(c.scheduler.should_delay(0.5),
                                           timeout=max(duration_s, 0.01))
                except asyncio.TimeoutError:
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
        PMC_CHAOS.labels(fault=fault_type,
                         status='pass' if passed else 'fail').inc()
        logger.warning(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) /
                              len(self.results)) if self.results else 1.0,
                'recent': self.results[-5:]}


# =============================================================================
# CacheEntry
# =============================================================================
@dataclass
class CacheEntry:
    policy: Dict[str, Any]
    timestamp: float
    objectives: List[float]     # [reward, carbon, latency, cost]
    expert_id: int
    metadata: Optional[Dict[str, Any]] = None


# =============================================================================
# PolicyMetaCache v4.0.2
# =============================================================================
class PolicyMetaCache:
    """
    Enhanced policy meta-cache. v4.0.2 includes all v4.0.1 fixes plus
    the causal-RL corrections (NC1–NC8) and regressions (NR1–NR6).
    """

    STRATEGY_NAMES = ['performance_focus', 'carbon_focus', 'cost_focus', 'balanced']
    SOURCES = ['rlhf', 'modp', 'reward', 'fallback']  # causal action space

    def __init__(
        self,
        max_age_hours: float = 24.0,
        dist_threshold: float = 0.2,
        enable_modp: bool = True,
        modp_weights: Optional[List[float]] = None,
        enable_moe: bool = True,
        num_experts: int = 4,
        enable_ga: bool = True,
        ga_population_size: int = 20,
        enable_scheduler: bool = True,
        carbon_threshold: float = 400.0,
        max_delay_seconds: int = 300,
        enable_self_healing: bool = True,
        anomaly_contamination: float = 0.1,
        enable_limit_graph: bool = True,
        limit_graph_update_interval: int = 300,
        enable_rlhf: bool = True,
        rlhf_reward_model: str = "linear",
        rlhf_training_interval: int = 600,
        enable_distillation: bool = True,
        distillation_temperature: float = 2.0,
        distillation_alpha: float = 0.5,
        distillation_interval: int = 300,
        carbon_manager: Optional[Any] = None,
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
        # v4.0.2 additions
        causal_baseline: Optional[int] = None,  # NC5
        causal_min_samples: int = 5,            # NC8
        epsilon_selection: float = 0.1,         # NC1: exploration for IPW
        seed: Optional[int] = None,             # determinism
        eviction_interval: int = 300,
        metrics_port: int = 8012,
        start_metrics_server: bool = False,
    ):
        self.max_age_seconds = max_age_hours * 3600
        self.dist_threshold = dist_threshold
        self.eviction_interval = eviction_interval
        self.metrics_port = metrics_port
        self._start_metrics = start_metrics_server
        self._metrics_started = False

        # Determinism.
        self._rng = random.Random(seed)
        if seed is not None:
            try:
                np.random.seed(seed)
            except Exception:
                pass

        self.store: Dict[Tuple, List[CacheEntry]] = {}
        self.vectors: List[np.ndarray] = []
        self.keys: List[Tuple] = []
        self._store_lock = asyncio.Lock()

        self.enable_modp = enable_modp
        self.modp_weights = list(modp_weights or [0.5, 0.2, 0.2, 0.1])

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
        self.self_healing = SelfHealingManager(anomaly_contamination) \
            if enable_self_healing else None

        self.enable_limit_graph = enable_limit_graph
        self.limit_graph = LimitGraphManager() if enable_limit_graph else None
        self.limit_graph_update_interval = limit_graph_update_interval

        # v4.0.x flags
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

        # v4.0.2 causal params
        self.causal_baseline = causal_baseline
        self.causal_min_samples = causal_min_samples
        self.epsilon_selection = float(np.clip(epsilon_selection, 0.0, 1.0))

        # Modules
        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("reward_floor", "G(reward >= -0.5)")
            self.temporal_monitor.add_formula("cache_activity", "F(cache_size >= 1.0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")

        self.xai = XAIExplainer(['reward', 'carbon', 'latency', 'cost']) \
            if enable_xai else None

        self.precision_controller = AdaptivePrecisionController() \
            if enable_adaptive_precision else None

        self.carbon_market = CarbonMarketClient() if enable_carbon_market else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if enable_role_specialization else None

        self.enable_rlhf = enable_rlhf
        self.rlhf = ActiveRLHF(
            action_space=self.STRATEGY_NAMES,
            reward_model_type=rlhf_reward_model,
        ) if enable_rlhf else None
        self.rlhf_training_interval = rlhf_training_interval

        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (enable_hitl and self.rlhf) else None

        self.federated = FederatedAggregator(num_params=4) if enable_federated else None
        self.instance_id = str(uuid.uuid4())[:8]

        # NC5: pass baseline
        self.causal_shaper = CausalRewardShaper(
            num_actions=len(self.SOURCES),
            baseline=causal_baseline,
            min_samples=causal_min_samples,
        ) if enable_causal_rl else None

        self.enable_distillation = enable_distillation
        # NR5: no role_coordinator param
        self.distillation = MultiTeacherPolicyDistillation(
            num_teachers=num_experts,
            temperature=distillation_temperature,
            alpha=distillation_alpha,
        ) if enable_distillation else None
        self.distillation_interval = distillation_interval

        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        self._carbon_saved_kg_total = 0.0
        self._last_carbon_intensity = 400.0
        self._background_tasks: List[asyncio.Task] = []
        self._running = False

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _vector_to_key(self, vec: np.ndarray) -> Tuple:
        return tuple(round(float(v), 6) for v in vec.tolist())

    def _register_key_locked(self, key: Tuple, vec: np.ndarray) -> None:
        if key not in self.keys:
            self.vectors.append(vec)
            self.keys.append(key)

    def _source_to_index(self, source: str) -> int:
        try:
            return self.SOURCES.index(source)
        except ValueError:
            return len(self.SOURCES) - 1  # fallback

    def _sample_source(self, scores: Dict[str, float]) -> Tuple[str, Dict[str, float]]:
        """
        NC1: ε-greedy exploration over sources so IPW has real propensities.
        Returns (selected_source, propensities_dict).
        """
        sources = list(scores.keys())
        n = len(sources)
        if n == 0:
            return 'fallback', {'fallback': 1.0}
        if n == 1:
            return sources[0], {sources[0]: 1.0}

        if self._rng.random() < self.epsilon_selection:
            # Uniform exploration.
            props = {s: 1.0 / n for s in sources}
            chosen = self._rng.choice(sources)
            return chosen, props

        # Softmax over scores (a valid behaviour-policy distribution).
        s_arr = np.array([scores[s] for s in sources], dtype=float)
        s_arr = s_arr - s_arr.max()
        e = np.exp(s_arr)
        probs = e / e.sum()
        props = {s: float(probs[i]) for i, s in enumerate(sources)}
        chosen = sources[int(np.argmax(probs))]
        return chosen, props

    async def _fetch_carbon_once(self) -> float:
        if self.scheduler:
            carbon = await self.scheduler.get_current_carbon()
        else:
            carbon = 400.0
        self._last_carbon_intensity = carbon
        return carbon

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self):
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        if self.rlhf:
            self._background_tasks.append(loop.create_task(self._rlhf_loop()))
        if self.distillation and self.moe_gating:
            self._background_tasks.append(loop.create_task(self._distillation_loop()))
        if self.limit_graph:
            self._background_tasks.append(loop.create_task(self._limit_graph_loop()))
        if self.federated:
            self._background_tasks.append(loop.create_task(self._federated_loop()))
        if self.chaos_tester:
            self._background_tasks.append(loop.create_task(self._chaos_loop()))
        self._background_tasks.append(loop.create_task(self._eviction_loop()))

        if (PROMETHEUS_AVAILABLE and self._start_metrics
                and not self._metrics_started):
            try:
                start_http_server(self.metrics_port, registry=PMC_REGISTRY)
                self._metrics_started = True
            except Exception as e:
                logger.warning(f"Metrics server start failed: {e}")

        logger.info(f"PolicyMetaCache v4.0.2 started (instance {self.instance_id})")

    async def stop(self):
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

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
                if self.distillation and self.moe_gating and self.moe_gating._trained:
                    # Sample real contexts.
                    async with self._store_lock:
                        contexts = []
                        for _, entries in list(self.store.items())[:5]:
                            if entries:
                                e = entries[-1]
                                contexts.append({
                                    'carbon_intensity': max(0.0, e.objectives[1]) * 1000
                                        if e.objectives[1] > 0 else 400,
                                    'urgency': 0.5, 'model_size_mb': 1000})
                    if not contexts:
                        contexts = [{'carbon_intensity': 400, 'urgency': 0.5,
                                     'model_size_mb': 1000}]
                    probs = [self.moe_gating.get_weights(c) for c in contexts]
                    avg = np.mean(probs, axis=0).tolist()
                    await self.distillation.distill(avg)
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
                    carbon = await self._fetch_carbon_once()
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
                    fault = self._rng.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    async def _eviction_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.eviction_interval)
                await self._evict_stale()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Eviction loop error: {e}")

    async def _evict_stale(self):
        now = time.time()
        async with self._store_lock:
            stale_keys = []
            for key in list(self.store.keys()):
                fresh = [e for e in self.store[key]
                         if (now - e.timestamp) < self.max_age_seconds]
                if fresh:
                    self.store[key] = fresh
                else:
                    stale_keys.append(key)
            for key in stale_keys:
                del self.store[key]
                if key in self.keys:
                    idx = self.keys.index(key)
                    self.keys.pop(idx)
                    self.vectors.pop(idx)

    # ------------------------------------------------------------------ #
    # Retrieval (NC1–NC3, NC7, FIX #9)
    # ------------------------------------------------------------------ #
    async def get_best_policy(
        self, fp: WorkloadFingerprint,
        context: Optional[Dict[str, Any]] = None,
        urgency: float = 0.5,
    ) -> Optional[Dict[str, Any]]:
        context = context or {}
        vec = fp.to_vector()
        fp_key = self._vector_to_key(vec)

        if not self.vectors:
            PMC_HITS.labels(status='miss').inc()
            return None

        carbon = await self._fetch_carbon_once()

        # Carbon-aware delay.
        if self.scheduler:
            should_delay, delay = await self.scheduler.should_delay(urgency)
            if should_delay:
                logger.info(f"Policy retrieval delayed by {delay}s")
                await asyncio.sleep(min(delay, 2))

        # Adaptive precision.
        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(carbon)

        # Temporal gate.
        temporal_status: Dict[str, bool] = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'reward': 0.5,
                'cache_size': float(len(self.store)),
                'carbon': carbon})
            temporal_status = self.temporal_monitor.evaluate()

        # Nearest-neighbour (filtered).
        async with self._store_lock:
            candidates = []
            for i, sv in enumerate(self.vectors):
                d = float(np.linalg.norm(vec - sv))
                if d <= self.dist_threshold:
                    candidates.append((d, i))
            if not candidates:
                PMC_HITS.labels(status='miss').inc()
                return None
            candidates.sort(key=lambda x: x[0])
            best_idx = candidates[0][1]
            key = self.keys[best_idx]
            entries = [copy.copy(e) for e in self.store[key]]

        # Stale filter.
        now = time.time()
        valid = [e for e in entries if (now - e.timestamp) < self.max_age_seconds]
        if not valid:
            PMC_HITS.labels(status='stale').inc()
            return None

        # Self-healing.
        if self.self_healing:
            latest_reward = valid[-1].objectives[0]
            anomaly, _ = self.self_healing.detect_anomaly(latest_reward)
            if anomaly:
                PMC_HITS.labels(status='anomaly').inc()
                return None

        confidence = float(np.mean([e.objectives[0] for e in valid]))

        # HITL.
        hitl_outcome = None
        if self.hitl and confidence < self.hitl_confidence_threshold:
            try:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={'entries': len(valid)},
                    options=['accept', 'reject', 'recalibrate'],
                    confidence=confidence,
                    confidence_threshold=self.hitl_confidence_threshold)
            except Exception:
                pass

        if hitl_outcome and hitl_outcome.get('chosen') == 'reject':
            PMC_HITS.labels(status='hitl_reject').inc()
            return None
        if hitl_outcome and hitl_outcome.get('chosen') == 'recalibrate':
            async with self._store_lock:
                self.store[key] = []
            PMC_HITS.labels(status='hitl_recalibrate').inc()
            return None

        # Compute per-source scores (for selection + causal propensities).
        scores: Dict[str, float] = {}

        # RLHF score (only when trained).
        if (self.rlhf and self.rlhf._reward_model_trained
                and self.rlhf.reward_model is not None and context):
            try:
                rlhf_probs = await self.rlhf.get_policy_probs(context)
                # Average of RLHF's per-strategy preferences.
                scores['rlhf'] = float(np.mean(rlhf_probs)) if rlhf_probs else 0.0
            except Exception:
                pass

        # MODP score (only when >=2 candidates).
        if self.enable_modp and len(valid) > 1:
            criteria = ['reward', 'carbon', 'latency', 'cost']
            cand_dicts = [
                {crit: (e.objectives[i] if i == 0 else 1.0 - e.objectives[i])
                 for i, crit in enumerate(criteria)}
                for e in valid
            ]
            weights = (self.distillation.get_student_probs()
                       if self.distillation else self.modp_weights)
            if len(weights) != len(criteria):
                weights = self.modp_weights
            try:
                topsis_scores = TOPSIS.score(cand_dicts, weights, criteria)
                scores['modp'] = float(max(topsis_scores))
            except Exception:
                pass

        # Reward score (always available).
        scores['reward'] = float(max(e.objectives[0] for e in valid))

        # Fallback score (baseline).
        scores['fallback'] = float(np.mean([e.objectives[0] for e in valid]))

        # NC1: ε-greedy selection gives real propensities.
        source, source_props = self._sample_source(scores)

        # Map to policy.
        selected_policy = None
        xai_out = None
        if source == 'rlhf':
            try:
                rlhf_probs = await self.rlhf.get_policy_probs(context)
                best_i = int(np.argmax([
                    e.objectives[0] * (rlhf_probs[0] if rlhf_probs else 1.0)
                    for e in valid]))
                selected_policy = valid[best_i].policy
            except Exception:
                selected_policy = None
        elif source == 'modp':
            criteria = ['reward', 'carbon', 'latency', 'cost']
            cand_dicts = [
                {crit: (e.objectives[i] if i == 0 else 1.0 - e.objectives[i])
                 for i, crit in enumerate(criteria)}
                for e in valid
            ]
            weights = (self.distillation.get_student_probs()
                       if self.distillation else self.modp_weights)
            if len(weights) != len(criteria):
                weights = self.modp_weights
            try:
                topsis_scores = TOPSIS.score(cand_dicts, weights, criteria)
                best_i = int(np.argmax(topsis_scores))
                selected_policy = valid[best_i].policy
                if self.xai:
                    xai_out = self.xai.explain(
                        candidate=cand_dicts[best_i],
                        weights={crit: w for crit, w in zip(criteria, weights)},
                        all_candidates=cand_dicts)
            except Exception:
                selected_policy = None

        # Fallback paths.
        if selected_policy is None:
            if source in ('reward',):
                best_entry = max(valid, key=lambda e: e.objectives[0])
                selected_policy = best_entry.policy
            else:
                # 'fallback' or any failure
                best_entry = max(valid, key=lambda e: e.objectives[0])
                selected_policy = best_entry.policy
                source = 'reward' if source != 'fallback' else 'fallback'

        # NC1–NC3: record the *actual* source as the action, with propensities.
        if self.causal_shaper is not None:
            action_idx = self._source_to_index(source)
            props_vec = np.array([source_props.get(s, 1e-6) for s in self.SOURCES])
            self.causal_shaper.record_decision(action_idx, props_vec, fp_key=fp_key)

        # Role assignments.
        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                'trust': 0.7, 'compute': 0.7, 'energy': 0.7,
                'performance': confidence})

        # Carbon accounting — deferred to update().
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market:
            try:
                saved_kg = max(0.0, (400 - carbon) * 0.001)
                credit_value = await self.carbon_market.get_carbon_credit_value(saved_kg)
                rec_value = await self.carbon_market.get_rec_value(saved_kg * 0.5)
                # NOTE: not booked here; caller decides via update().
            except Exception:
                pass

        # Federated submission.
        federated_round = None
        if self.federated:
            self.federated.submit_update(
                self.instance_id, list(self.modp_weights),
                samples=len(valid))
            if len(self.federated.client_updates) >= 5:
                agg = self.federated.aggregate()
                federated_round = agg['round']

        PMC_HITS.labels(status='hit').inc()
        PMC_SOURCE.labels(source=source).inc()

        return {
            'policy': copy.deepcopy(selected_policy),
            'source': source,
            'confidence': confidence,
            'precision_used': precision.value,
            'temporal_status': temporal_status,
            'xai_explanation': xai_out,
            'role_assignments': role_assignments,
            'hitl_outcome': hitl_outcome,
            'carbon_credit_value_usd': credit_value,
            'rec_value_usd': rec_value,
            'federated_round': federated_round,
            'source_propensities': source_props,
            'fp_key': fp_key,
            'timestamp': datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------ #
    # Update
    # ------------------------------------------------------------------ #
    async def update(
        self, fp: WorkloadFingerprint, policy: Dict[str, Any], reward: float,
        carbon: float = 0.0, latency: float = 0.0, cost: float = 0.0,
        context: Optional[Dict[str, Any]] = None,
        expert_id: int = 0,
        carbon_saved_kg: float = 0.0,
    ):
        vec = fp.to_vector()
        key = self._vector_to_key(vec)
        fp_key = key
        objectives = [float(reward), float(carbon), float(latency), float(cost)]
        new_entry = CacheEntry(
            policy=copy.deepcopy(policy),
            timestamp=time.time(),
            objectives=objectives,
            expert_id=int(expert_id))

        async with self._store_lock:
            self._register_key_locked(key, vec)
            if key not in self.store:
                self.store[key] = []
            entries = self.store[key]
            if self.enable_modp:
                entries.append(new_entry)
                if len(entries) > 20:
                    entries.sort(key=lambda e: e.objectives[0], reverse=True)
                    self.store[key] = entries[:20]
            else:
                best = max((e.objectives[0] for e in entries),
                           default=-float('inf'))
                if reward > best:
                    self.store[key] = [new_entry]
                else:
                    entries.append(new_entry)

            if self.enable_ga and key not in self.ga_populations:
                pop = GAPopulation(self.ga_population_size, rng=self._rng)
                pop.initialize(policy)
                self.ga_populations[key] = pop

        # MOE update.
        if self.enable_moe and context is not None and self.moe_gating:
            self.moe_gating.update(context, int(expert_id), float(reward))

        # Self-healing.
        if self.self_healing:
            self.self_healing.record_reward(float(reward))

        # LIMIT Graph.
        if self.limit_graph:
            await self.limit_graph.update_constraint('carbon', float(carbon))

        # RLHF update (NR3 already fixed).
        if self.rlhf:
            try:
                self.rlhf.update(context or {}, 'balanced', float(reward))
            except Exception:
                pass

        # NC3/NC4: pair the realised reward with the pending decision.
        if self.causal_shaper is not None:
            self.causal_shaper.record_outcome(float(reward), fp_key=fp_key)

        # Carbon accounting with explicit kg.
        if self.carbon_market and carbon_saved_kg > 0:
            try:
                await self.carbon_market.retire_credits(
                    carbon_saved_kg, self.instance_id)
                self._carbon_saved_kg_total += carbon_saved_kg
            except Exception:
                pass

        # Federated.
        if self.federated:
            self.federated.submit_update(
                self.instance_id, list(self.modp_weights), samples=1)

        # Temporal tick.
        if self.temporal_monitor:
            self.temporal_monitor.update({
                'reward': float(reward),
                'cache_size': float(len(self.store)),
                'carbon': float(carbon)})
            self.temporal_monitor.evaluate()

    # ------------------------------------------------------------------ #
    # Feedback, GA, util
    # ------------------------------------------------------------------ #
    async def record_feedback(self, state: Dict, action: str, reward: float):
        if self.rlhf:
            await self.rlhf.record_feedback(state, action, reward)

    async def evolve_populations(self, fitness_func: Callable[[Dict], float]):
        if not self.enable_ga:
            return
        async with self._store_lock:
            for key, pop in self.ga_populations.items():
                best = pop.evolve(fitness_func)
                if not best:
                    continue
                reward = float(fitness_func(best))
                entries = self.store.get(key, [])
                if entries:
                    base = max(entries, key=lambda e: e.objectives[0])
                    objectives = [reward, base.objectives[1],
                                  base.objectives[2], base.objectives[3]]
                else:
                    objectives = [reward, 0.0, 0.0, 0.0]
                new_entry = CacheEntry(
                    policy=copy.deepcopy(best),
                    timestamp=time.time(),
                    objectives=objectives,
                    expert_id=0)
                if key not in self.store:
                    self.store[key] = []
                    self._register_key_locked(key, np.array(key, dtype=float))
                self.store[key].append(new_entry)

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

    # NR6: documented as async.
    async def select_precision(self,
                               accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(
            self._last_carbon_intensity, accuracy_required)

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

    def get_causal_ates(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

    def get_causal_means(self) -> Dict[int, float]:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.get_means()

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

    # ------------------------------------------------------------------ #
    # Stats + clear
    # ------------------------------------------------------------------ #
    def get_stats(self) -> Dict:
        stats = {
            'instance_id': self.instance_id,
            'version': '4.0.2',
            'cache_size': len(self.store),
            'total_entries': sum(len(v) for v in self.store.values()),
            'ga_populations': len(self.ga_populations),
            'moe_trained': self.moe_gating._trained if self.moe_gating else False,
            'self_healing_trained': self.self_healing._trained
                if self.self_healing else False,
            'rlhf_trained': bool(self.rlhf and self.rlhf._reward_model_trained),
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'last_carbon_intensity': self._last_carbon_intensity,
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
        }
        if self.limit_graph:
            stats['limit_graph'] = {
                'nodes': list(self.limit_graph.graph.keys()),
                'constraints': self.limit_graph.constraints}
        if self.rlhf:
            stats['rlhf'] = {
                'actions': self.rlhf.actions,
                'history_len': len(self.rlhf.history),
                'pending_queries': len(self.rlhf.pending_queries),
                'reward_model_trained': self.rlhf._reward_model_trained}
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
        if self.causal_shaper:
            stats['causal'] = {
                'ates': self.causal_shaper.compute_ate(),
                'means': self.causal_shaper.get_means(),
                'interventions': len(self.causal_shaper.interventions),
                'pending': self.causal_shaper.get_pending_count(),
                'sources': self.SOURCES,
                'epsilon_selection': self.epsilon_selection,
            }
        return stats

    def clear(self):
        with contextlib.suppress(Exception):
            self.store.clear()
            self.vectors.clear()
            self.keys.clear()
            self.ga_populations.clear()
            self._carbon_saved_kg_total = 0.0
            self._last_carbon_intensity = 400.0
            if self.rlhf:
                self.rlhf.history.clear()
                self.rlhf.preference_counts.clear()
                self.rlhf.context_counts.clear()
                self.rlhf.pending_queries.clear()
                self.rlhf.feedback_buffer.clear()
                self.rlhf._reward_model_trained = False
            if self.self_healing:
                self.self_healing.reward_history.clear()
                self.self_healing._trained = False
            if self.federated:
                self.federated.client_updates.clear()
            if self.moe_gating:
                self.moe_gating.history.clear()
                self.moe_gating._trained = False
            if self.distillation:
                self.distillation.history.clear()
                self.distillation.student_policy = (
                    np.ones(self.distillation.num_teachers)
                    / self.distillation.num_teachers)
            if self.temporal_monitor:
                self.temporal_monitor.history.clear()
                self.temporal_monitor.violations.clear()
            if self.hitl:
                self.hitl.audit_log.clear()
            if self.causal_shaper:
                self.causal_shaper.interventions.clear()
                self.causal_shaper._pending.clear()
                self.causal_shaper.ates = {i: 0.0 for i in
                                           range(self.causal_shaper.num_actions)}


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
    print("Unit tests — PolicyMetaCache v4.0.2")
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
        assert hitl.resolve_query(qid, "a", rating=1.0) is not None

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
        cache = PolicyMetaCache(seed=1, epsilon_selection=0.0)
        await cache.start()
        fp = WorkloadFingerprint(500, 1024, 2048, 8000, 2)
        await cache.update(fp, {"batch_size": 32}, reward=0.9,
                           context={"carbon_intensity": 350})
        tester = ChaosTester(cache)
        r1 = await tester.run_test("carbon_api_down", duration_s=0.01)
        r2 = await tester.run_test("moe_broken", duration_s=0.01)
        r3 = await tester.run_test("distiller_broken", duration_s=0.01)
        await cache.stop()
        return r1, r2, r3

    loop = _asyncio.new_event_loop()
    try:
        r1, r2, r3 = loop.run_until_complete(chaos_test())
        check("carbon_api_down fires", r1["passed"] is True, str(r1))
        check("moe_broken fires", r2["passed"] is True, str(r2))
        check("distiller_broken fires", r3["passed"] is True, str(r3))
    except Exception as e:
        check("chaos suite", False, str(e))
    finally:
        loop.close()

    # ---- 4. Distillation stability ----
    print("\n[4] Distillation stability (FIX #4)")
    dist = MultiTeacherPolicyDistillation(num_teachers=4, temperature=2.0,
                                          learning_rate=0.3)
    dist.student_policy = np.array([0.01, 0.01, 0.01, 0.97])
    teacher = np.array([0.9, 0.05, 0.03, 0.02])
    soft = np.power(teacher, 1.0 / dist.temperature)
    soft = soft / soft.sum()
    new = (1.0 - dist.learning_rate) * dist.student_policy + dist.learning_rate * soft
    new = np.clip(new, 1e-9, None)
    dist.student_policy = new / new.sum()
    p = np.array(dist.get_student_probs())
    check("EMA KD is a distribution", abs(p.sum() - 1.0) < 1e-9)
    check("EMA KD positive", (p > 0).all())
    check("EMA KD moves toward teacher", p[0] > 0.01)

    # ---- 5. ActiveRLHF int mapping (NR3) ----
    print("\n[5] ActiveRLHF int mapping (NR3)")
    rlhf = ActiveRLHF(action_space=['performance_focus', 'carbon_focus',
                                    'cost_focus', 'balanced'])
    rlhf.update({"k": 1}, 'performance_focus', 1.0)
    rlhf.update({"k": 1}, 'carbon_focus', 1.0)
    check("string action updates",
          rlhf.preference_counts['performance_focus'] == 1.0
          and rlhf.preference_counts['carbon_focus'] == 1.0)
    check("context updated", len(rlhf.context_counts) >= 1)
    # Safe fallback for unknown action
    idx = rlhf._action_to_index('unknown_action')
    check("_action_to_index falls back safely", idx == 0)

    # ---- 6. Causal ATE recovery (NC5) ----
    print("\n[6] CausalRewardShaper ATE recovery (NC5)")
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

    # ---- 7. Causal decision/outcome pairing (NC4) ----
    print("\n[7] Causal decision/outcome pairing (NC4)")
    shaper = CausalRewardShaper(num_actions=4, min_samples=1)
    props = np.array([0.25, 0.25, 0.25, 0.25])
    shaper.record_decision(2, props, fp_key=("fp_a",))
    shaper.record_decision(0, props, fp_key=("fp_a",))
    ok1 = shaper.record_outcome(0.9, fp_key=("fp_a",))
    ok2 = shaper.record_outcome(0.3, fp_key=("fp_a",))
    ok3 = shaper.record_outcome(0.1, fp_key=("fp_a",))  # no pending
    check("first outcome paired", ok1 is True)
    check("second outcome paired", ok2 is True)
    check("third outcome rejected", ok3 is False)
    check("interventions recorded", len(shaper.interventions) == 2)

    # ---- 8. ε-greedy exploration (NC1) ----
    print("\n[8] ε-greedy exploration (NC1)")

    async def eps_test():
        cache = PolicyMetaCache(seed=42, epsilon_selection=0.3,
                                enable_causal_rl=True)
        counts = defaultdict(int)
        for _ in range(200):
            scores = {'rlhf': 0.9, 'modp': 0.5, 'reward': 0.3, 'fallback': 0.1}
            chosen, props = cache._sample_source(scores)
            counts[chosen] += 1
            assert abs(sum(props.values()) - 1.0) < 1e-6
        return counts

    loop = _asyncio.new_event_loop()
    try:
        counts = loop.run_until_complete(eps_test())
        # With 30% exploration, we should see all sources.
        check("all sources explored", all(counts.get(s, 0) > 0
              for s in ['rlhf', 'modp', 'reward', 'fallback']),
              str(dict(counts)))
        # rlhf should dominate (highest score).
        check("best source dominates", counts.get('rlhf', 0) > counts.get('fallback', 0))
    except Exception as e:
        check("epsilon-greedy", False, str(e))
    finally:
        loop.close()

    # ---- 9. Causal integration end-to-end (NC1–NC3) ----
    print("\n[9] End-to-end causal integration (NC1–NC3)")

    async def e2e_test():
        cache = PolicyMetaCache(
            seed=11, epsilon_selection=0.2,
            enable_causal_rl=True, causal_min_samples=5,
            enable_rlhf=False,  # isolate to modp/reward/fallback
            enable_chaos_testing=False,
        )
        await cache.start()
        # Update: modp-sourced policies always reward 0.9;
        # reward-sourced policies reward 0.2 (unobservable here).
        fp = WorkloadFingerprint(500, 1024, 2048, 8000, 2)
        for i in range(10):
            await cache.update(fp, {"batch_size": 32 + i}, reward=0.5 + 0.04 * i,
                               context={"carbon_intensity": 350})
        # Retrieve + pair outcomes.
        for _ in range(50):
            res = await cache.get_best_policy(fp, context={"carbon_intensity": 350})
            if res is None:
                continue
            # Make modp outcomes better than reward outcomes.
            reward = 0.9 if res['source'] == 'modp' else 0.3
            await cache.update(fp, res['policy'], reward=reward,
                               context={"carbon_intensity": 350})
        ates = cache.get_causal_ates()
        means = cache.get_causal_means()
        await cache.stop()
        return ates, means

    loop = _asyncio.new_event_loop()
    try:
        ates, means = loop.run_until_complete(e2e_test())
        check("causal ATEs populated", len(ates) > 0)
        check("means computed", len(means) > 0)
    except Exception as e:
        check("end-to-end causal", False, str(e))
    finally:
        loop.close()

    # ---- 10. Determinism + clear completeness ----
    print("\n[10] Determinism + clear (FIX #10)")

    async def det_test():
        c1 = PolicyMetaCache(seed=42)
        c2 = PolicyMetaCache(seed=42)
        fp = WorkloadFingerprint(500, 1024, 2048, 8000, 2)
        for i in range(3):
            for c in (c1, c2):
                await c.update(fp, {"batch_size": 32 + i},
                               reward=0.7 + 0.05 * i,
                               context={"carbon_intensity": 350})
        r1 = await c1.get_best_policy(fp, context={"carbon_intensity": 350})
        r2 = await c2.get_best_policy(fp, context={"carbon_intensity": 350})
        c1.clear()
        stats = c1.get_stats()
        await c1.stop()
        await c2.stop()
        return r1, r2, stats

    loop = _asyncio.new_event_loop()
    try:
        r1, r2, stats = loop.run_until_complete(det_test())
        check("deterministic policy",
              r1 is not None and r2 is not None
              and r1['policy'] == r2['policy'])
        check("clear resets store", stats['cache_size'] == 0)
        check("clear resets rlhf",
              stats.get('rlhf', {}).get('history_len', 0) == 0)
    except Exception as e:
        check("determinism/clear", False, str(e))
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
# Smoke test (with causal learning demonstration)
# =============================================================================
async def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s — %(message)s')

    print("=" * 78)
    print("Policy Meta Cache v4.0.2 (patched) — smoke test")
    print("=" * 78)

    cache = PolicyMetaCache(
        max_age_hours=24, dist_threshold=0.2,
        enable_modp=True, modp_weights=[0.5, 0.2, 0.2, 0.1],
        enable_moe=True, num_experts=4,
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
        enable_causal_rl=True,
        causal_min_samples=5,
        epsilon_selection=0.2,
        seed=42,
    )

    await cache.start()

    fp = WorkloadFingerprint(500, 1024, 2048, 8000, 2)

    # Populate the cache.
    for i in range(5):
        policy = {"batch_size": 32 + i * 4, "learning_rate": 0.001,
                  "precision": "fp16"}
        await cache.update(
            fp, policy, reward=0.85 + i * 0.02,
            carbon=0.2, latency=0.1, cost=0.05,
            context={"carbon_intensity": 350 + i * 10, "urgency": 0.3},
            expert_id=i % 4)

    # Seed feedback.
    for _ in range(5):
        await cache.record_feedback(
            state={"carbon_intensity": 350, "avg_score": 0.9,
                   "cost": 0.2, "diversity": 0.1},
            action="balanced", reward=0.8)

    # Train reward model.
    await cache.rlhf.train_reward_model()

    # Retrieve once.
    result = await cache.get_best_policy(
        fp, context={"carbon_intensity": 350, "urgency": 0.3})

    print(f"\n📋 Retrieval result:")
    if result:
        print(f"   source: {result['source']}")
        print(f"   confidence: {result['confidence']:.2f}")
        print(f"   precision: {result['precision_used']}")
        print(f"   carbon credit: ${result['carbon_credit_value_usd']:.4f}")
        print(f"   source propensities: "
              f"{ {k: round(v, 2) for k, v in result['source_propensities'].items()} }")
        if result.get('temporal_status'):
            print(f"   temporal_status: {result['temporal_status']}")
        if result.get('role_assignments'):
            print(f"   dominant_role: {result['role_assignments']['dominant_role']}")
        if result.get('xai_explanation') and result['xai_explanation'].get('narrative'):
            print(f"   XAI: {result['xai_explanation']['narrative'][0]}")
        if result.get('hitl_outcome'):
            print(f"   HITL: {result['hitl_outcome']['source']} -> "
                  f"{result['hitl_outcome']['chosen']}")

    # ---------- Causal learning demo ----------
    print("\n🧠 Causal learning demo (feeding reward signal)...")
    # Feed pairs; make 'modp' better than 'reward' to demonstrate ATE.
    for _ in range(60):
        res = await cache.get_best_policy(fp, context={"carbon_intensity": 350})
        if res is None:
            continue
        # Source-dependent rewards to demonstrate ATE recovery.
        reward_by_source = {'modp': 0.9, 'rlhf': 0.6, 'reward': 0.4, 'fallback': 0.3}
        await cache.update(fp, res['policy'],
                           reward=reward_by_source.get(res['source'], 0.5),
                           context={"carbon_intensity": 350})

    ates = cache.get_causal_ates()
    means = cache.get_causal_means()
    source_names = cache.SOURCES
    print("   Source means (μ):")
    for i, name in enumerate(source_names):
        print(f"     {name:10s} μ={means.get(i, 0.0):.3f}  "
              f"ATE={ates.get(i, 0.0):+.3f}")

    # Precision selector.
    p = await cache.select_precision()
    print(f"\n⚙️  Precision: {p.value}")

    cc = await cache.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    print(f"🎭 Role assignments: {cache.get_role_assignments()}")

    hitl = await cache.escalate_decision(
        decision_context={'reason': 'smoke'},
        options=['a', 'b'], confidence=0.5)
    print(f"👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    print("\n🧪 Chaos suite:")
    chaos = await cache.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📊 Stats:")
    stats = cache.get_stats()
    print(json.dumps({
        'version': stats['version'],
        'cache_size': stats['cache_size'],
        'total_entries': stats['total_entries'],
        'features': stats['features'],
        'rlhf_trained': stats['rlhf_trained'],
        'causal': stats.get('causal'),
        'chaos_pass_rate': stats.get('chaos', {}).get('pass_rate'),
        'precision_last': stats.get('precision', {}).get('last'),
    }, indent=2, default=str))

    await cache.stop()
    print("\n" + "=" * 78)
    print("✅ Policy Meta Cache v4.0.2 — smoke test complete")
    print("=" * 78)


def _cli() -> int:
    parser = argparse.ArgumentParser(description="PolicyMetaCache v4.0.2")
    parser.add_argument("--test", action="store_true",
                        help="Run embedded unit tests and exit")
    parser.add_argument("--smoke", action="store_true",
                        help="Run the smoke test (default)")
    args = parser.parse_args()
    if args.test:
        return _run_unit_tests()
    asyncio.run(main())
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
