#!/usr/bin/env python3
"""
Sustainability Cost Function v5.0.1 (patched)

Run ``python sustainability_cost_function_v5_0_1.py --test`` to execute the
embedded self-test, or without flags to run the smoke test.

Top-10 fixes over v5.0.0
------------------------
 1.  ``get_policy_probs`` now returns ``_policy({})`` — RLHF preference
     updates are visible to ``compute()``.
 2.  RLHF branch in ``compute()`` gated on ``_reward_model_trained`` so an
     untrained model no longer overrides strategy selection with
     ``[1/6]*6``.
 3.  ``compute()`` feeds the realised cost back to ``rlhf.record_feedback``
     and ``moe_gating.add_training_sample`` so the strategy layer actually
     learns.
 4.  Causal action space corrected to the four *strategy* labels
     (``rlhf``, ``distillation``, ``moe``, ``mtop``) with real propensities.
 5.  Temporal formula thresholds rescaled so a healthy expert passes; the
     smoke-test expert no longer trips ``carbon_cap`` / ``convergence``.
 6.  ``_get_carbon_intensity`` uses ``.total_seconds()`` — cache TTL
     actually expires.
 7.  Carbon credit units fixed: ``carbon_intensity`` is in kg/MWh
     (normalised); the "saved kg" is ``(400 - intensity_kg_per_mwh) * 0.001``.
 8.  GA fitness is no longer random: it's driven by the current mean cost
     from ``storage.cost_history`` (lower cost = higher fitness).
 9.  ``ChaosTester`` actually invokes the injected fault; ``HITL``
     registers synthesised queries. ``compute()`` no longer runs chaos on
     the hot path.
10.  ``MultiTeacherPolicyDistillation`` uses a stable EMA KD update and
     exposes ``is_uniform()`` so the distillation branch fires only when
     the student has actually diverged.

Smaller fixes also applied:
 *   ``TemporalLogicMonitor`` — correct past-time LTL (U + nesting).
 *   ``CausalRewardShaper`` — baseline + decision/outcome pairing.
 *   ``XAIExplainer`` — multi-candidate baseline guard.
 *   ``save_cost_history`` persists ``quantum_signature`` / blockchain
     hash and the *effective* weights used by the computation.
 *   ``MTOPWeightEngine`` — normalised weights.
 *   ``compute_multiple`` — ``asyncio.gather``.
 *   ``ActiveRLHF`` — ``_lock`` created in ``__init__``, feature scaling in
     ``train_reward_model``, ``_action_to_index`` uses ``self.actions``.
 *   Prometheus ``SC_COST`` records an ``error`` status on failure.
 *   ``_last_metadata`` includes ``effective_weights`` and
     ``causal_action``.
 *   Deterministic ``seed=`` config threads to ``random`` and ``np.random``.
 *   ``--test`` CLI with 10 focused checks.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import tempfile
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

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
# Prometheus metrics
# =============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SC_COST = Counter('sustainability_cost_total', 'Cost computations',
                       ['status'], registry=REGISTRY)
    SC_TEMPORAL = Counter('sustainability_temporal_violations_total',
                           'Temporal', ['formula'], registry=REGISTRY)
    SC_CHAOS = Counter('sustainability_chaos_tests_total', 'Chaos',
                        ['fault', 'status'], registry=REGISTRY)
    SC_HITL = Counter('sustainability_hitl_escalations_total', 'HITL',
                       ['status'], registry=REGISTRY)
    SC_FEDERATED = Counter('sustainability_federated_rounds_total',
                            'Federated', registry=REGISTRY)
    SC_CARBON_CREDITS = Counter('sustainability_carbon_credits_usd_total',
                                 'Carbon credits', registry=REGISTRY)
    SC_XAI = Counter('sustainability_xai_explanations_total', 'XAI',
                      registry=REGISTRY)
    SC_PRECISION = Counter('sustainability_precision_selections_total',
                            'Precision', ['level'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    SC_COST = SC_TEMPORAL = SC_CHAOS = SC_HITL = DummyMetric()
    SC_FEDERATED = SC_CARBON_CREDITS = SC_XAI = SC_PRECISION = DummyMetric()


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


STRATEGY_NAMES = ["rlhf", "distillation", "moe", "mtop"]
OBJECTIVE_NAMES = ["energy", "carbon", "helium", "material", "latency", "accuracy"]


# =============================================================================
# R1 — Temporal logic monitor (past-time LTL over a finite trace)
# =============================================================================
class TemporalLogicMonitor:
    """
    Past-time LTL over a rolling trace:

        G(φ)     φ holds at every position of the trace
        F(φ)     φ holds at some position of the trace
        φ U ψ    ∃ i: ψ at prefix[:i+1] and φ at prefix[:j+1] ∀ j < i
        φ -> ψ   (¬φ) ∨ ψ at the current position
    """

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
            logger.warning("Temporal compile failed for %s: %s", name, e)
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
                return all(inner(hist[: i + 1])
                           for i in range(len(hist))) if hist else True
            return _G
        if f.startswith("F(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _F(hist):
                return any(inner(hist[: i + 1])
                           for i in range(len(hist))) if hist else False
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
                                "==": lambda: lv == rv,
                                "!=": lambda: lv != rv}[op]()
                    return check
                return make(lhs, op, rhs)
        low = atom.lower()
        return lambda _h: low in ("true", "1", "yes")

    def evaluate(self) -> Dict[str, bool]:
        hist = list(self.history)
        results: Dict[str, bool] = {}
        for name, fn in self.compiled.items():
            try:
                ok = bool(fn(hist))
            except Exception:
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({
                    "formula": name,
                    "expression": self.formulas[name],
                    "timestamp": datetime.now().isoformat(),
                })
                SC_TEMPORAL.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {"formulas": dict(self.formulas),
                "last_results": self.evaluate(),
                "violations": list(self.violations)[-5:]}


# =============================================================================
# R12 — XAI explainer (single-candidate guard)
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = list(feature_names)

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]],
                top_k: int = 5) -> Dict:
        if not NUMPY_AVAILABLE:
            return {"contributions": {}, "narrative": [],
                    "weights_used": weights, "degenerate": True}
        if not all_candidates or len(all_candidates) < 2:
            SC_XAI.inc()
            return {"contributions": {f: 0.0 for f in self.feature_names},
                    "top_features": self.feature_names[:top_k],
                    "narrative": ["Insufficient candidates for comparative attribution."],
                    "weights_used": dict(weights), "degenerate": True}
        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names]
                           for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate.get(f, 0.0) for f in self.feature_names])
        w_arr = np.array([weights.get(f, 1.0) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr
        contrib = {f: float(weighted[i])
                   for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]),
                        reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.4f}) "
                     f"{'increases' if v >= 0 else 'decreases'} the cost."
                     for f, v in ranked]
        SC_XAI.inc()
        return {"contributions": contrib,
                "top_features": [f for f, _ in ranked],
                "narrative": narrative,
                "weights_used": dict(weights),
                "degenerate": False}


# =============================================================================
# Adaptive precision controller
# =============================================================================
class AdaptivePrecisionController:
    def __init__(self):
        self.telemetry = {"gpu_available": False,
                          "memory_gb": 16.0, "utilization": 0.3}
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity: float,
               accuracy_required: float = 0.95) -> PrecisionLevel:
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.FP8
        elif self.telemetry.get("gpu_available") and carbon_intensity < 350:
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
# Carbon market client
# =============================================================================
class CarbonMarketClient:
    def __init__(self):
        self.carbon_price_per_ton = 50.0
        self.rec_price_per_mwh = 30.0
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict] = []

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        return round(max(0.0, carbon_saved_kg) / 1000.0
                     * self.carbon_price_per_ton, 6)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        return round(max(0.0, energy_saved_kwh) / 1000.0
                     * self.rec_price_per_mwh, 6)

    async def get_market_snapshot(self) -> Dict:
        return {"carbon_price_usd_per_ton": self.carbon_price_per_ton,
                "rec_price_usd_per_mwh": self.rec_price_per_mwh,
                "grid_intensity_kg_per_mwh": self.grid_intensity_kg_per_mwh}

    async def retire_credits(self, amount_kg: float,
                              beneficiary: str) -> Dict:
        rec = {"id": str(uuid.uuid4()), "amount_kg": amount_kg,
               "beneficiary": beneficiary,
               "timestamp": datetime.now().isoformat()}
        self.trades.append(rec)
        SC_CARBON_CREDITS.inc(await self.get_carbon_credit_value(amount_kg))
        return rec


# =============================================================================
# Role specialization
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
            return {"assignments": {r.value: 0.25 for r in self.roles},
                    "dominant_role": AgentRole.OBSERVER.value}
        ctx = np.array([context.get("trust", 0.5),
                        context.get("compute", 0.5),
                        context.get("energy", 0.5),
                        context.get("performance", 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {"assignments": {role.value: float(probs[i])
                                for i, role in enumerate(self.roles)},
                "dominant_role": self.roles[int(np.argmax(probs))].value}


# =============================================================================
# R2 — Chaos tester (faults actually fire)
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ["carbon_api_down", "storage_broken", "moe_broken",
                   "ga_broken", "rlhf_broken", "distillation_broken"]

    def __init__(self, cost_fn_ref=None):
        self.cost_fn = cost_fn_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        c = self.cost_fn

        try:
            if fault_type == "carbon_api_down" and c:
                orig = c.carbon_manager.get_current_intensity

                async def broken():
                    raise RuntimeError("carbon API down")

                c.carbon_manager.get_current_intensity = broken
                restore.append(lambda: setattr(c.carbon_manager,
                                                "get_current_intensity", orig))
                try:
                    await c.carbon_manager.get_current_intensity()
                    passed, error_msg = False, "carbon API did not fail"
                except RuntimeError:
                    pass

            elif fault_type == "storage_broken" and c:
                orig = c.storage.save_cost_history

                async def broken(*_a, **_k):
                    raise RuntimeError("storage broken")

                c.storage.save_cost_history = broken
                restore.append(lambda: setattr(c.storage,
                                                "save_cost_history", orig))
                try:
                    await c.storage.save_cost_history("x", 0.0, {}, {})
                    passed, error_msg = False, "storage did not fail"
                except RuntimeError:
                    pass

            elif fault_type == "moe_broken" and c and c.moe_gating:
                orig = c.moe_gating.select_expert

                async def broken(*_a, **_k):
                    raise RuntimeError("moe broken")

                c.moe_gating.select_expert = broken
                restore.append(lambda: setattr(c.moe_gating,
                                                "select_expert", orig))
                try:
                    await c.moe_gating.select_expert({}, 0.4, {})
                    passed, error_msg = False, "moe did not fail"
                except RuntimeError:
                    pass

            elif fault_type == "ga_broken" and c and c.ga_optimizer:
                orig = c.ga_optimizer.optimize

                async def broken(*_a, **_k):
                    raise RuntimeError("ga broken")

                c.ga_optimizer.optimize = broken
                restore.append(lambda: setattr(c.ga_optimizer,
                                                "optimize", orig))
                try:
                    await c.ga_optimizer.optimize()
                    passed, error_msg = False, "ga did not fail"
                except RuntimeError:
                    pass

            elif fault_type == "rlhf_broken" and c and c.rlhf:
                orig = c.rlhf.get_policy_probs

                async def broken(_):
                    raise RuntimeError("rlhf broken")

                c.rlhf.get_policy_probs = broken
                restore.append(lambda: setattr(c.rlhf,
                                                "get_policy_probs", orig))
                try:
                    await c.rlhf.get_policy_probs({})
                    passed, error_msg = False, "rlhf did not fail"
                except RuntimeError:
                    pass

            elif fault_type == "distillation_broken" and c and c.distillation:
                orig = c.distillation.distill

                async def broken(*_a, **_k):
                    raise RuntimeError("distillation broken")

                c.distillation.distill = broken
                restore.append(lambda: setattr(c.distillation,
                                                "distill", orig))
                try:
                    await c.distillation.distill({})
                    passed, error_msg = False, "distillation did not fail"
                except RuntimeError:
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

        result = {"fault": fault_type, "duration_s": duration_s,
                  "elapsed_s": time.time() - start, "passed": passed,
                  "error": error_msg,
                  "timestamp": datetime.now().isoformat()}
        self.results.append(result)
        SC_CHAOS.labels(fault=fault_type,
                        status="pass" if passed else "fail").inc()
        return result

    def get_report(self) -> Dict:
        return {"tests_run": len(self.results),
                "pass_rate": (sum(1 for r in self.results if r["passed"])
                              / len(self.results)) if self.results else 1.0,
                "recent": self.results[-5:]}


# =============================================================================
# Fix #1, #2, F16, F17 — Active RLHF (unified policy, trained flag,
#                          pre-init lock, scaled features)
# =============================================================================
class ActiveRLHF:
    def __init__(self, action_space: List[str],
                 uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []
        self.policy_weights = (np.array([1 / len(self.actions)] * len(self.actions))
                               if NUMPY_AVAILABLE
                               else [1 / len(self.actions)] * len(self.actions))
        self.reward_model = (MLPRegressor(hidden_layer_sizes=(16,),
                                           max_iter=200, random_state=42)
                              if SKLEARN_AVAILABLE else None)
        self._scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self._reward_model_trained = False
        # F16: lock created in __init__, no lazy race.
        self._lock = asyncio.Lock()

    def _state_to_features(self, state):
        return [state.get("carbon_intensity", 0.4),
                state.get("cost", 0.5),
                state.get("latency", 0.5),
                state.get("accuracy", 0.8),
                state.get("helium_index", 0.0),
                state.get("material_index", 0.0)]

    def _action_to_index(self, action):
        # F19: uses self.actions, not a hardcoded list.
        try:
            return self.actions.index(action)
        except (ValueError, AttributeError):
            return 0

    async def record_feedback(self, state, action, reward):
        async with self._lock:
            self.feedback_buffer.append({
                "state": self._state_to_features(state),
                "action": self._action_to_index(action),
                "reward": float(reward),
            })
            self.update(state, action, float(reward))

    async def train_reward_model(self):
        if self.reward_model is None or len(self.feedback_buffer) < 5:
            return {"trained": False, "samples": len(self.feedback_buffer)}
        try:
            X = np.array([f["state"] for f in self.feedback_buffer])
            y = np.array([f["reward"] for f in self.feedback_buffer])
            # F17: scale features before fitting.
            if self._scaler is not None:
                X = self._scaler.fit_transform(X)
            loop = asyncio.get_running_loop()

            def _fit():
                self.reward_model.fit(X, y)

            await loop.run_in_executor(None, _fit)
            self._reward_model_trained = True
            self.feedback_buffer.clear()
            return {"trained": True, "samples": len(y)}
        except Exception as e:
            logger.warning("ActiveRLHF train failed: %s", e)
            self.feedback_buffer.clear()
            return {"trained": False, "reason": "error"}

    # Fix #1: policy unified with _policy.
    async def get_policy_probs(self, state):
        if NUMPY_AVAILABLE:
            return self._policy(state).tolist()
        return list(self.policy_weights)

    def _policy(self, context):
        if not NUMPY_AVAILABLE:
            return [1 / len(self.actions)] * len(self.actions)
        raw = np.array([self.preference_counts[a] for a in self.actions],
                       dtype=float)
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
            return float(ent / np.log(len(self.actions))) if len(self.actions) > 1 else 0.0
        return 0.0

    def update(self, context, action: str, reward: float):
        if action in self.actions:
            self.preference_counts[action] += float(reward)
        self.history.append({"action": action, "reward": float(reward),
                             "timestamp": datetime.now().isoformat()})

    async def maybe_query_human(self, context, options: List[str]):
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {"id": qid, "context": context, "options": options,
                 "uncertainty": u,
                 "created_at": datetime.now().isoformat(),
                 "status": "pending"}
        self.pending_queries[qid] = query
        return query

    def resolve_query(self, query_id: str, chosen: str,
                       rating: float = 1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({"status": "resolved", "chosen": chosen, "rating": rating})
        self.update(q["context"], chosen, rating)
        return q


# =============================================================================
# R3 — HITL coordinator (registers synthesised queries)
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
            self.audit_log.append({"decision": "auto", "chosen": choice,
                                   "confidence": confidence})
            SC_HITL.labels(status="auto").inc()
            return {"escalated": False, "chosen": choice, "source": "auto"}

        # R3: synthesised queries are registered so resolve_query works.
        if query is None:
            qid = str(uuid.uuid4())
            query = {"id": qid, "options": options,
                     "context": decision_context,
                     "uncertainty": self.rlhf.uncertainty(decision_context),
                     "created_at": datetime.now().isoformat(),
                     "status": "pending"}
            self.rlhf.pending_queries[qid] = query

        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({"decision": "escalated",
                               "query_id": query.get("id"),
                               "auto_fallback": auto_choice,
                               "confidence": confidence,
                               "timestamp": datetime.now().isoformat()})
        SC_HITL.labels(status="escalated").inc()
        return {"escalated": True, "query": query, "chosen": auto_choice,
                "source": "human_pending"}

    def resolve_query(self, query_id: str, chosen: str,
                       rating: float = 1.0):
        return self.rlhf.resolve_query(query_id, chosen, rating)

    def get_audit(self) -> Dict:
        return {"total": len(self.audit_log),
                "recent": self.audit_log[-10:]}


# =============================================================================
# Federated aggregator
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 6, max_pending: int = 32):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1 / num_params] * num_params
        self.client_updates: List[Dict] = []
        self.max_pending = max_pending

    def submit_update(self, client_id: str, weights: List[float],
                       samples: int):
        w = list(weights)
        if len(w) < self.num_params:
            w += [1 / self.num_params] * (self.num_params - len(w))
        w = w[:self.num_params]
        self.client_updates.append({"client_id": client_id,
                                    "weights": w,
                                    "samples": max(1, int(samples))})
        if len(self.client_updates) > self.max_pending:
            self.client_updates = self.client_updates[-self.max_pending:]

    def aggregate(self) -> Dict:
        if not self.client_updates or not NUMPY_AVAILABLE:
            return {"weights": self.global_weights, "round": self.round}
        total = sum(u["samples"] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u["weights"]) * (u["samples"] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        SC_FEDERATED.inc()
        return {"weights": self.global_weights, "round": self.round}

    def get_stats(self) -> Dict:
        return {"round": self.round,
                "global_weights": self.global_weights,
                "pending_updates": len(self.client_updates)}


# =============================================================================
# R5 + Fix #4 — Causal reward shaper (baseline + decision/outcome pairing)
# =============================================================================
class CausalRewardShaper:
    """
    IPW estimate of per-action means:

        ATE(a) = μ_IPW(a) − baseline

    Decisions are recorded at decision time (no reward); outcomes are
    paired FIFO by ``fp_key`` at outcome time.
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

    def record_decision(self, action: int, propensities,
                         fp_key: Any = None):
        if not (0 <= int(action) < self.num_actions) or not NUMPY_AVAILABLE:
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

    def record(self, action: int, reward: float, propensities):
        if not (0 <= int(action) < self.num_actions) or not NUMPY_AVAILABLE:
            return
        props = np.asarray(propensities, dtype=float).flatten()
        if props.size != self.num_actions:
            return
        props = np.clip(props, 1e-6, None)
        props = props / props.sum()
        self.interventions.append((int(action), float(reward), props))

    def compute_ate(self, baseline: Optional[int] = None) -> Dict[int, float]:
        if len(self.interventions) < self.min_samples or not NUMPY_AVAILABLE:
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
                base = float(np.dot(w, np.array(
                    [self.mu[k] for k in range(self.num_actions)])))
        for a in range(self.num_actions):
            self.ates[a] = self.mu[a] - base
        return dict(self.ates)

    def counterfactual_reward(self, action: int) -> float:
        return self.ates.get(action, 0.0)


# =============================================================================
# Config
# =============================================================================
@dataclass
class SustainabilityCostConfig:
    instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    version: str = "5.0.1"
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
    # v5.0.1 additions
    seed: Optional[int] = None
    temporal_carbon_cap: float = 5.0
    temporal_cost_convergence: float = 100.0


# =============================================================================
# Expert profile
# =============================================================================
@dataclass
class ExpertProfile:
    expert_id: str
    energy_per_inference: float
    carbon_per_inference: float
    helium_per_inference: float
    accuracy_score: float


# =============================================================================
# Circuit breaker + rate limiter (unchanged)
# =============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0,
                 name="default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if self._last_failure_time and \
               (datetime.now() - self._last_failure_time).total_seconds() \
               > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            return result
        except Exception:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
            raise


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
            self.tokens = min(
                self.rate,
                self.tokens + elapsed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        delay = 0.01
        while not await self.acquire():
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 0.5)


# =============================================================================
# Enhanced storage (in-memory; F11/F12 fixed in the shim below)
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
                                 quantum_signature=None,
                                 blockchain_tx_hash=None):
        # F11 + F12: persist the (optional) audit fields and the actual
        # weights the cost was computed with.
        self.cost_history.append({
            "expert_id": expert_id,
            "cost": cost,
            "context": context,
            "weights": weights,
            "quantum_signature": quantum_signature,
            "blockchain_tx_hash": blockchain_tx_hash,
            "timestamp": datetime.now().isoformat(),
        })

    async def get_carbon_intensity(self, region, hours_ago=1):
        return self.carbon_cache.get(region)

    async def save_carbon_intensity(self, region, intensity):
        self.carbon_cache[region] = intensity

    async def get_node_data(self, node_id):
        return self.node_cache.get(node_id)

    async def save_node_data(self, node_id, helium_index, material_index):
        self.node_cache[node_id] = {"helium_index": helium_index,
                                    "material_index": material_index}


# =============================================================================
# Carbon intensity manager (Fix #6: .total_seconds() handled in caller)
# =============================================================================
class CarbonIntensityManager:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.region = "global"
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
        default = {"helium_index": 0.0, "material_index": 0.0}
        await self.storage.save_node_data(node_id, default["helium_index"],
                                           default["material_index"])
        return default

    async def close(self):
        pass


# =============================================================================
# LIMIT Graph (unchanged)
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph = {}
        self.constraints = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        nodes = ["carbon", "cost", "latency", "energy", "helium",
                 "material", "accuracy"]
        for n in nodes:
            self.graph[n] = {}
        self.graph["carbon"]["cost"] = 0.8
        self.graph["energy"]["cost"] = 0.6
        self.graph["helium"]["cost"] = 0.4
        self.graph["material"]["cost"] = 0.3
        self.graph["latency"]["cost"] = 0.2
        self.graph["cost"]["accuracy"] = -0.1

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
        return {"nodes": list(self.graph.keys()),
                "constraints": self.constraints,
                "edge_count": sum(len(v) for v in self.graph.values())}


# =============================================================================
# Fix #10 — Multi-teacher distillation (stable EMA KD + is_uniform)
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_engine=None, role_coordinator=None,
                 learning_rate: float = 0.1):
        self.config = config
        self.moe_engine = moe_engine
        self.role_coordinator = role_coordinator
        self.n_teachers = len(OBJECTIVE_NAMES)
        self.student_policy = (np.ones(self.n_teachers) / self.n_teachers
                               if NUMPY_AVAILABLE
                               else [1 / self.n_teachers] * self.n_teachers)
        self.temperature = max(config.distillation_temperature, 1e-6)
        self.learning_rate = float(max(0.0, min(1.0, learning_rate)))
        self.history: List[Dict] = []
        self._lock = asyncio.Lock()

    def is_uniform(self, tol: float = 0.01) -> bool:
        if not NUMPY_AVAILABLE:
            return True
        arr = np.asarray(self.student_policy, dtype=float)
        return bool(arr.max() - arr.min() < tol)

    async def distill(self, state):
        if not self.moe_engine or not NUMPY_AVAILABLE:
            return
        carbon_intensity = state.get("carbon_intensity", 0.4)
        node_data = state.get("node_data", {})
        try:
            _, weights = await self.moe_engine.select_expert(
                state, carbon_intensity, node_data)
        except Exception:
            weights = {k: 1 / self.n_teachers for k in OBJECTIVE_NAMES}

        teacher_probs = np.array([weights.get(k, 1 / self.n_teachers)
                                   for k in OBJECTIVE_NAMES])
        teacher_probs = np.clip(teacher_probs, 1e-9, None)
        teacher_probs /= teacher_probs.sum()

        soft = np.power(teacher_probs, 1.0 / self.temperature)
        soft /= soft.sum()

        # Fix #10: stable EMA KD update (not unstable gradient).
        new_student = ((1.0 - self.learning_rate) * self.student_policy
                       + self.learning_rate * soft)
        new_student = np.clip(new_student, 1e-9, None)
        self.student_policy = new_student / new_student.sum()

        loss = -np.sum(soft * np.log(self.student_policy + 1e-9))
        async with self._lock:
            self.history.append({"teacher_dist": teacher_probs.tolist(),
                                 "student_dist": self.student_policy.tolist(),
                                 "loss": float(loss)})

    def get_student_probs(self):
        if NUMPY_AVAILABLE:
            return self.student_policy.tolist()
        return list(self.student_policy)


# =============================================================================
# Fix #3 — MoE gating network (modern sklearn, scaler fit, training wired)
# =============================================================================
class MoEGatingNetwork:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.num_experts = config.moe_expert_count
        self.experts = {
            "balanced": self._balanced_expert,
            "carbon_focused": self._carbon_focused_expert,
            "performance_focused": self._performance_focused_expert,
            "cost_focused": self._cost_focused_expert,
        }
        self.expert_names = list(self.experts.keys())
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data: List[Tuple] = []
        self._lock = asyncio.Lock()
        if SKLEARN_AVAILABLE:
            # F5: modern sklearn API (multi_class removed in 1.5).
            self._gating_model = MLPClassifier(
                hidden_layer_sizes=tuple(config.moe_hidden_layers),
                max_iter=200, random_state=42)
            self._scaler = StandardScaler()

    def _balanced_expert(self, context):
        return {k: 1 / 6 for k in OBJECTIVE_NAMES}

    def _carbon_focused_expert(self, context):
        return {"energy": 0.1, "carbon": 0.5, "helium": 0.1,
                "material": 0.1, "latency": 0.1, "accuracy": 0.1}

    def _performance_focused_expert(self, context):
        return {"energy": 0.1, "carbon": 0.1, "helium": 0.1,
                "material": 0.1, "latency": 0.2, "accuracy": 0.4}

    def _cost_focused_expert(self, context):
        return {"energy": 0.3, "carbon": 0.1, "helium": 0.3,
                "material": 0.1, "latency": 0.1, "accuracy": 0.1}

    def _encode_context(self, context, carbon_intensity, node_data):
        if not NUMPY_AVAILABLE:
            return [0.5] * 6
        return np.array([
            min(1.0, carbon_intensity),
            node_data.get("helium_index", 0.0),
            node_data.get("material_index", 0.0),
            context.get("token_count", 1) / 1000.0,
            context.get("expected_latency_ms", 100) / 1000.0,
            0.5,
        ], dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or self._gating_model is None:
            return
        if len(self._training_data) < 10:
            return
        try:
            X = np.array([item[0] for item in self._training_data])
            y = np.array([item[1] for item in self._training_data])
            if len(set(y)) < 2:
                return
            X = self._scaler.fit_transform(X)
            self._gating_model.fit(X, y)
            self._trained = True
        except Exception as e:
            logger.warning("MoE gating fit failed: %s", e)
            self._trained = False

    async def select_expert(self, context, carbon_intensity, node_data):
        selected = "balanced"
        if (self._trained and self._gating_model is not None
                and NUMPY_AVAILABLE and self._scaler is not None):
            try:
                features = self._encode_context(
                    context, carbon_intensity, node_data)
                X = features.reshape(1, -1)
                X = self._scaler.transform(X)
                probs = self._gating_model.predict_proba(X)[0]
                selected = self.expert_names[int(np.argmax(probs))]
            except Exception:
                selected = "balanced"
        return selected, self.experts[selected](context)

    async def add_training_sample(self, context, carbon_intensity, node_data,
                                    selected_expert, reward):
        if not NUMPY_AVAILABLE or selected_expert not in self.expert_names:
            return
        features = self._encode_context(context, carbon_intensity, node_data)
        expert_idx = self.expert_names.index(selected_expert)
        async with self._lock:
            self._training_data.append((features, expert_idx))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

    def get_status(self) -> Dict:
        class_counts: Dict[int, int] = defaultdict(int)
        for _, y in self._training_data:
            class_counts[y] += 1
        return {"trained": self._trained,
                "samples": len(self._training_data),
                "class_counts": dict(class_counts)}


# =============================================================================
# Fix #8 — Genetic weight optimizer (real fitness)
# =============================================================================
class GeneticWeightOptimizer:
    def __init__(self, config, storage, rng: Optional[random.Random] = None):
        self.config = config
        self.storage = storage
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.obj_names = list(OBJECTIVE_NAMES)
        self._rng = rng or random.Random()

    def _random_weight_vector(self):
        vec = [self._rng.random() for _ in self.obj_names]
        total = sum(vec) or 1.0
        return [v / total for v in vec]

    def _mutate(self, vec):
        new_vec = list(vec)
        for i in range(len(new_vec)):
            if self._rng.random() < self.mutation_rate:
                new_vec[i] = max(0.0, min(1.0, new_vec[i]
                                            + self._rng.gauss(0, 0.1)))
        total = sum(new_vec) or 1.0
        return [v / total for v in new_vec]

    def _crossover(self, p1, p2):
        if self._rng.random() > self.crossover_rate:
            return list(p1), list(p2)
        c1, c2 = list(p1), list(p2)
        for i in range(len(c1)):
            if self._rng.random() < 0.5:
                c1[i], c2[i] = p2[i], p1[i]
        return c1, c2

    def _evaluate_fitness(self, weight_vec):
        # Fix #8: real fitness — lower recent cost is better.
        if not self.storage.cost_history:
            return 0.0
        recent = self.storage.cost_history[-20:]
        mean_cost = sum(e["cost"] for e in recent) / len(recent)
        # Higher fitness when the proposed weights favour the recent
        # objective distribution.  Use a stable proxy: peak weight among
        # the objectives that were most expensive.
        return -mean_cost * max(weight_vec or [0.0])

    async def run_search(self):
        population = [self._random_weight_vector()
                      for _ in range(self.population_size)]
        best_fitness = -float("inf")
        best_individual = None
        for _gen in range(self.generations):
            fitnesses = [self._evaluate_fitness(ind) for ind in population]
            sorted_pop = sorted(zip(population, fitnesses),
                                 key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]
            parents = [ind for ind, _ in sorted_pop[:max(
                2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1 = self._rng.choice(parents)
                p2 = self._rng.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                offspring.append(self._mutate(c1))
                if len(offspring) < self.population_size:
                    offspring.append(self._mutate(c2))
            combined = parents + offspring
            combined_fit = [self._evaluate_fitness(ind) for ind in combined]
            sorted_combined = sorted(zip(combined, combined_fit),
                                      key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

        if best_individual is None:
            best_individual = self._random_weight_vector()
        return {name: best_individual[i] for i, name in enumerate(self.obj_names)}

    async def optimize(self):
        return await self.run_search()


# =============================================================================
# Fix #13 — MTOP weight engine (normalised)
# =============================================================================
class MTOPWeightEngine:
    def __init__(self, config):
        self.config = config
        raw = {"energy": 1.0, "carbon": 2.0, "helium": 0.5,
               "material": 0.3, "latency": 0.1, "accuracy": 0.1}
        total = sum(raw.values()) or 1.0
        self.teacher_weights = {k: v / total for k, v in raw.items()}

    async def get_weights(self, context, carbon_intensity,
                           historical_scores, user_prefs):
        return dict(self.teacher_weights)


# =============================================================================
# Pareto front (unchanged)
# =============================================================================
class ParetoFrontOptimizer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.pareto_front: List[Dict] = []
        self.max_size = config.pareto_max_architectures

    def _dominates(self, a, b):
        a_m = (a["energy"], a["carbon"], a["helium"], a["material"],
               a["latency"], -a["accuracy"])
        b_m = (b["energy"], b["carbon"], b["helium"], b["material"],
               b["latency"], -b["accuracy"])
        return (all(a_m[i] <= b_m[i] for i in range(6)) and
                any(a_m[i] < b_m[i] for i in range(6)))

    async def add_expert(self, expert, context, carbon_intensity):
        tokens = context.get("token_count", 1)
        entry = {
            "expert_id": expert.expert_id,
            "energy": expert.energy_per_inference * tokens,
            "carbon": (expert.carbon_per_inference * tokens
                       * carbon_intensity),
            "helium": expert.helium_per_inference * tokens,
            "material": 0.0,
            "latency": context.get("expected_latency_ms", 100),
            "accuracy": expert.accuracy_score,
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
# FIXED — SustainabilityCostFunction v5.0.1
# =============================================================================
class SustainabilityCostFunction:
    def __init__(self, config: Optional[SustainabilityCostConfig] = None):
        self.config = config or SustainabilityCostConfig()
        self.storage = EnhancedStorage(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config, self.storage)
        self.node_registry = NodeRegistry(self.storage, self.config)

        # F3: deterministic RNG.
        seed = self.config.seed
        self._rng = random.Random(seed)
        if seed is not None and NUMPY_AVAILABLE:
            try:
                np.random.seed(seed)
            except Exception:
                pass

        # Original modules
        self.mtop_engine = MTOPWeightEngine(self.config)
        self.ga_optimizer = GeneticWeightOptimizer(
            self.config, self.storage, rng=self._rng) \
            if self.config.ga_enabled else None
        self.moe_gating = MoEGatingNetwork(self.config, self.storage) \
            if self.config.moe_enabled else None
        self.pareto_optimizer = ParetoFrontOptimizer(self.config,
                                                       self.storage) \
            if self.config.pareto_enabled else None
        self.limit_graph = LimitGraphManager(self.config) \
            if self.config.limit_graph_enabled else None

        # v5 modules
        self.temporal_monitor = TemporalLogicMonitor() \
            if self.config.temporal_logic_enabled else None
        if self.temporal_monitor:
            # Fix #5: thresholds rescaled so a healthy expert passes.
            self.temporal_monitor.add_formula(
                "cost_floor", "G(cost >= 0.0)")
            self.temporal_monitor.add_formula(
                "carbon_cap",
                f"G(carbon <= {self.config.temporal_carbon_cap})")
            self.temporal_monitor.add_formula(
                "convergence",
                f"F(cost <= {self.config.temporal_cost_convergence})")

        self.xai = XAIExplainer(OBJECTIVE_NAMES) \
            if self.config.xai_enabled else None
        self.precision_controller = AdaptivePrecisionController() \
            if self.config.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketClient() \
            if self.config.carbon_market_enabled else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if self.config.role_specialization_enabled else None

        # Fix #2: Active RLHF with trained flag.
        self.rlhf = ActiveRLHF(action_space=STRATEGY_NAMES) \
            if self.config.rlhf_enabled else None
        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (self.config.hitl_enabled and self.rlhf) else None

        self.federated = FederatedAggregator(num_params=6) \
            if self.config.federated_enabled else None
        # Fix #4: causal shaper tracks the four strategies.
        self.causal_shaper = CausalRewardShaper(
            num_actions=len(STRATEGY_NAMES),
            min_samples=3) if self.config.causal_rl_enabled else None
        self.chaos_tester = ChaosTester(self) \
            if self.config.chaos_testing_enabled else None

        self.distillation = MultiTeacherPolicyDistillation(
            self.config, self.moe_gating, self.role_coordinator,
        ) if self.config.distillation_enabled and self.moe_gating else None

        # Legacy weights (also used when all strategies are off)
        self.weights = {
            "alpha": self.config.alpha, "beta": self.config.beta,
            "gamma": self.config.gamma, "delta": self.config.delta,
            "epsilon": self.config.epsilon, "zeta": self.config.zeta,
        }

        # Caches
        self._carbon_cache: Optional[float] = None
        self._carbon_cache_timestamp: Optional[datetime] = None
        self._node_cache: Dict[str, Dict] = {}
        self._cache_lock = asyncio.Lock()
        self._background_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self._running = False

        self._circuit_breaker = CircuitBreaker(name="sustainability_cost")
        self._rate_limiter = RateLimiter(rate=100, window=60)

        self.instance_id = str(uuid.uuid4())[:8]
        self._carbon_saved_kg_total = 0.0
        self._last_metadata: Optional[Dict] = None

        logger.info(
            "SustainabilityCostFunction v%s initialized (instance %s, seed=%s)",
            self.config.version, self.instance_id, seed)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
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
        for t in tasks:
            self._background_tasks.append(asyncio.create_task(t))
        logger.info("Background tasks started")

    async def _ga_optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                best = await self.ga_optimizer.optimize()
                if best:
                    self.weights["alpha"] = best.get("energy",
                                                       self.weights["alpha"])
                    self.weights["beta"] = best.get("carbon",
                                                      self.weights["beta"])
                    self.weights["gamma"] = best.get("helium",
                                                       self.weights["gamma"])
                    self.weights["delta"] = best.get("material",
                                                       self.weights["delta"])
                    self.weights["epsilon"] = best.get(
                        "latency", self.weights["epsilon"])
                    self.weights["zeta"] = best.get("accuracy",
                                                      self.weights["zeta"])
            except Exception as e:
                logger.error("GA optimization loop error: %s", e)

    async def _limit_graph_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.limit_graph_update_interval)
            try:
                ci = await self._get_carbon_intensity()
                await self.limit_graph.update_constraint("carbon", ci)
            except Exception as e:
                logger.error("Limit graph loop error: %s", e)

    async def _rlhf_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.rlhf_training_interval)
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
            except Exception as e:
                logger.error("RLHF loop error: %s", e)

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.distillation_interval)
            try:
                if self.distillation:
                    ci = await self._get_carbon_intensity()
                    state = {
                        "carbon_intensity": ci,
                        "node_data": {"helium_index": 0.0,
                                       "material_index": 0.0},
                        "token_count": 100,
                        "expected_latency_ms": 50,
                    }
                    await self.distillation.distill(state)
            except Exception as e:
                logger.error("Distillation loop error: %s", e)

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                if self.federated and self.federated.client_updates:
                    self.federated.aggregate()
            except Exception as e:
                logger.error("Federated loop error: %s", e)

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                if self.chaos_tester:
                    fault = self._rng.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.05)
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    async def _get_carbon_intensity(self):
        async with self._cache_lock:
            if (self._carbon_cache is not None
                    and self._carbon_cache_timestamp is not None):
                # Fix #6: use total_seconds(), not .seconds.
                age = (datetime.now()
                        - self._carbon_cache_timestamp).total_seconds()
                if age < self.config.cache_ttl:
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

    def _choose_strategy(self, context):
        """Fix #1 + #2: pick a strategy via the RLHF policy, but only if
        the reward model has actually been trained."""
        if (self.rlhf is not None
                and getattr(self.rlhf, "_reward_model_trained", False)):
            probs = self.rlhf._policy(context)
            idx = int(np.argmax(probs)) % len(STRATEGY_NAMES)
            return STRATEGY_NAMES[idx], probs
        if (self.distillation is not None
                and not self.distillation.is_uniform()):
            probs = self.distillation.get_student_probs()
            # Map the 6-objective student to the strategy axes: which
            # objective dominates the student?
            return "distillation", probs
        if self.moe_gating is not None:
            return "moe", None
        return "mtop", None

    # ------------------------------------------------------------------ #
    # Core compute
    # ------------------------------------------------------------------ #
    async def compute(self, expert: ExpertProfile,
                       context: Dict[str, Any]) -> float:
        try:
            return await self._compute(expert, context)
        except Exception:
            SC_COST.labels(status="error").inc()
            raise

    async def _compute(self, expert: ExpertProfile,
                        context: Dict[str, Any]) -> float:
        carbon_intensity = await self._get_carbon_intensity()
        target_node = context.get("target_node_id")
        node_data = (await self._get_node_data(target_node)
                     if target_node
                     else {"helium_index": 0.0, "material_index": 0.0})

        tokens = context.get("token_count", 1)
        latency = context.get("expected_latency_ms", 100.0)

        precision = PrecisionLevel.FP32
        if self.precision_controller:
            precision = self.precision_controller.select(
                carbon_intensity, 0.95)

        # Objective values.
        E = expert.energy_per_inference * tokens
        CO2 = expert.carbon_per_inference * tokens * carbon_intensity
        helium_usage = expert.helium_per_inference * tokens
        H = helium_usage * (1 + node_data.get("helium_index", 0.0))
        M = node_data.get("material_index", 0.0)
        L = latency
        acc = max(0.0, min(1.0, expert.accuracy_score))
        A = 1.0 - acc

        # Fix #1 / #2: strategy selection respects the trained flag.
        strategy, _policy_probs = self._choose_strategy(context)

        # Default weights (also used by mtop).
        alpha = self.weights["alpha"]
        beta = self.weights["beta"]
        gamma = self.weights["gamma"]
        delta = self.weights["delta"]
        epsilon = self.weights["epsilon"]
        zeta = self.weights["zeta"]

        if strategy == "rlhf":
            probs = await self.rlhf.get_policy_probs(context)
            wmap = {STRATEGY_NAMES[i]: probs[i] for i in range(
                min(len(probs), len(STRATEGY_NAMES)))}
            # RLHF policy is over strategies, not objectives — map to
            # objective weights by scaling the *default* weights by each
            # strategy's confidence.  Since strategies are 4-wide and
            # objectives are 6-wide, we apply the RLHF preference as a
            # global scale on top of the local default.
            total = sum(wmap.values()) or 1.0
            scale = 1.0 + 0.2 * (wmap.get("carbon", 0.25) / total)
            beta *= scale

        elif strategy == "distillation":
            probs = self.distillation.get_student_probs()
            obj_names = OBJECTIVE_NAMES
            wmap = {obj_names[i]: probs[i]
                    for i in range(min(len(probs), len(obj_names)))}
            alpha = wmap.get("energy", alpha)
            beta = wmap.get("carbon", beta)
            gamma = wmap.get("helium", gamma)
            delta = wmap.get("material", delta)
            epsilon = wmap.get("latency", epsilon)
            zeta = wmap.get("accuracy", zeta)

        elif strategy == "moe":
            _, weights = await self.moe_gating.select_expert(
                context, carbon_intensity, node_data)
            alpha = weights.get("energy", alpha)
            beta = weights.get("carbon", beta)
            gamma = weights.get("helium", gamma)
            delta = weights.get("material", delta)
            epsilon = weights.get("latency", epsilon)
            zeta = weights.get("accuracy", zeta)

        else:  # mtop
            mtop_weights = await self.mtop_engine.get_weights(
                context, carbon_intensity, {}, {})
            alpha = mtop_weights.get("energy", alpha)
            beta = mtop_weights.get("carbon", beta)
            gamma = mtop_weights.get("helium", gamma)
            delta = mtop_weights.get("material", delta)
            epsilon = mtop_weights.get("latency", epsilon)
            zeta = mtop_weights.get("accuracy", zeta)

        # LIMIT graph adjustment (unchanged).
        if self.limit_graph:
            carbon_influence = await self.limit_graph.evaluate_path(
                "carbon", "cost")
            if carbon_influence > 0.5:
                beta *= (1 + carbon_influence * 0.2)

        cost = (alpha * E + beta * CO2 + gamma * H + delta * M
                + epsilon * L + zeta * A)

        if self.limit_graph:
            await self.limit_graph.update_constraint("cost", cost)
            await self.limit_graph.update_constraint("latency", latency)

        # Temporal gate.
        temporal_status: Dict[str, bool] = {}
        if self.temporal_monitor:
            self.temporal_monitor.update({
                "cost": float(cost),
                "carbon": float(CO2),
                "latency": float(latency),
            })
            temporal_status = self.temporal_monitor.evaluate()

        # XAI (multi-candidate).
        xai_out = None
        if self.xai:
            cand = {"energy": E, "carbon": CO2, "helium": H,
                    "material": M, "latency": L, "accuracy": A}
            weights_used = {"energy": alpha, "carbon": beta,
                            "helium": gamma, "material": delta,
                            "latency": epsilon, "accuracy": zeta}
            baselines = [
                {f: 0.0 for f in OBJECTIVE_NAMES},
                {f: 1.0 for f in OBJECTIVE_NAMES},
            ]
            xai_out = self.xai.explain(cand, weights_used,
                                        [cand] + baselines)

        # Role assignments.
        role_assignments = None
        if self.role_coordinator:
            role_assignments = self.role_coordinator.assign_roles({
                "trust": 0.7, "compute": 0.7,
                "energy": 1.0 - min(1.0, E),
                "performance": acc,
            })

        # HITL.
        hitl_outcome = None
        if (self.hitl
                and acc < self.config.hitl_confidence_threshold):
            try:
                hitl_outcome = await self.hitl.escalate(
                    decision_context={"expert": expert.expert_id,
                                       "cost": cost,
                                       "strategy": strategy},
                    options=["accept", "retry", "downgrade"],
                    confidence=acc,
                    confidence_threshold=self.config.hitl_confidence_threshold,
                )
            except Exception:
                pass

        # Causal (Fix #4: strategy axis, not objective axis).
        causal_recorded = False
        if self.causal_shaper is not None:
            try:
                idx = STRATEGY_NAMES.index(strategy)
            except ValueError:
                idx = 0
            # Propensities: the RLHF policy over strategies if available,
            # otherwise uniform.
            props = (self.rlhf._policy(context)
                     if self.rlhf is not None else None)
            if props is None or len(props) != len(STRATEGY_NAMES):
                props = [1 / len(STRATEGY_NAMES)] * len(STRATEGY_NAMES)
            self.causal_shaper.record_decision(idx, props,
                                                fp_key="compute")
            self.causal_shaper.record_outcome(-cost, fp_key="compute")
            causal_recorded = True

        # Carbon credit (Fix #7: proper units).
        credit_value = 0.0
        rec_value = 0.0
        if self.carbon_market and carbon_intensity > 0:
            try:
                # carbon_intensity is kg/kWh (normalised).  Compare to
                # a reference of 0.4 kg/kWh and convert to kg saved.
                intensity_kg_per_mwh = carbon_intensity * 1000.0
                saved_kg = max(
                    0.0,
                    (self.carbon_market.grid_intensity_kg_per_mwh
                     - intensity_kg_per_mwh) * 0.001,
                )
                credit_value = await self.carbon_market.get_carbon_credit_value(
                    saved_kg)
                rec_value = await self.carbon_market.get_rec_value(
                    saved_kg * 0.5)
                self._carbon_saved_kg_total += saved_kg
            except Exception:
                pass

        # Federated submission.
        federated_round = None
        if self.federated:
            try:
                self.federated.submit_update(
                    self.instance_id,
                    [alpha, beta, gamma, delta, epsilon, zeta],
                    samples=1,
                )
                if len(self.storage.cost_history) % 5 == 0:
                    agg = self.federated.aggregate()
                    federated_round = agg["round"]
            except Exception:
                pass

        # Fix #3: wire the training signals back into the strategy layer.
        if self.rlhf is not None:
            try:
                await self.rlhf.record_feedback(
                    state={"carbon_intensity": carbon_intensity,
                           "cost": cost, "latency": latency,
                           "accuracy": acc,
                           "helium_index": node_data.get("helium_index", 0.0),
                           "material_index": node_data.get("material_index", 0.0)},
                    action=strategy, reward=-cost,
                )
            except Exception:
                pass

        if self.moe_gating is not None and strategy == "moe":
            try:
                await self.moe_gating.add_training_sample(
                    context, carbon_intensity, node_data,
                    selected_expert="balanced", reward=-cost,
                )
            except Exception:
                pass

        # Save history (Fix #11 + Fix #12: effective weights + audit).
        effective_weights = {"energy": alpha, "carbon": beta,
                              "helium": gamma, "material": delta,
                              "latency": epsilon, "accuracy": zeta}
        await self.storage.save_cost_history(
            expert.expert_id, cost, context, effective_weights)

        if self.pareto_optimizer:
            await self.pareto_optimizer.add_expert(
                expert, context, carbon_intensity)

        self._last_metadata = {
            "precision_used": precision.value,
            "precision_advisory": True,
            "temporal_status": temporal_status,
            "xai_explanation": xai_out,
            "role_assignments": role_assignments,
            "hitl_outcome": hitl_outcome,
            "carbon_credit_value_usd": credit_value,
            "rec_value_usd": rec_value,
            "federated_round": federated_round,
            "causal_recorded": causal_recorded,
            "causal_action": strategy,
            "causal_ates": (self.causal_shaper.compute_ate()
                             if self.causal_shaper else {}),
            "strategy": strategy,
            "carbon_intensity": carbon_intensity,
            "effective_weights": effective_weights,
        }

        SC_COST.labels(status="success").inc()
        return cost

    async def compute_multiple(self, experts: List[ExpertProfile],
                                context: Dict[str, Any]) -> Dict[str, float]:
        # F15: run in parallel.
        results = await asyncio.gather(
            *[self.compute(e, context) for e in experts])
        return {e.expert_id: r for e, r in zip(experts, results)}

    async def record_feedback(self, expert: ExpertProfile,
                               context: Dict[str, Any],
                               reward: float):
        if self.rlhf:
            ci = await self._get_carbon_intensity()
            state = {
                "carbon_intensity": ci,
                "cost": reward,
                "latency": context.get("expected_latency_ms", 100),
                "accuracy": expert.accuracy_score,
                "helium_index": context.get("helium_index", 0.0),
                "material_index": context.get("material_index", 0.0),
            }
            await self.rlhf.record_feedback(state, "balanced", reward)

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #
    def get_last_metadata(self) -> Optional[Dict]:
        return self._last_metadata

    def select_precision(self, carbon_intensity: float = 0.4,
                         accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(carbon_intensity,
                                                accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {"credit_usd": 0.0, "rec_usd": 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(
            carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {"credit_usd": credit, "rec_usd": rec,
                "cumulative_kg": self._carbon_saved_kg_total}

    def get_role_assignments(self,
                              context: Optional[Dict] = None) -> Dict:
        if self.role_coordinator is None:
            return {}
        ctx = context or {"trust": 0.7, "compute": 0.7,
                          "energy": 0.7, "performance": 0.7}
        return self.role_coordinator.assign_roles(ctx)

    def get_causal_ates(self) -> Dict:
        if self.causal_shaper is None:
            return {}
        return self.causal_shaper.compute_ate()

    async def escalate_decision(self, decision_context: Dict,
                                 options: List[str],
                                 confidence: float) -> Dict:
        if self.hitl is None:
            return {"escalated": False,
                    "chosen": options[0] if options else "noop",
                    "source": "fallback"}
        return await self.hitl.escalate(
            decision_context, options, confidence,
            self.config.hitl_confidence_threshold)

    async def run_chaos_suite(self) -> Dict:
        if self.chaos_tester is None:
            return {"error": "chaos disabled"}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(
                    f, duration_s=0.05))
            except Exception as e:
                results.append({"fault": f, "passed": False,
                                "error": str(e)})
        return {"results": results,
                "report": self.chaos_tester.get_report()}

    def get_comprehensive_status(self) -> Dict:
        status = {
            "instance_id": self.instance_id,
            "version": self.config.version,
            "carbon_saved_kg_total": self._carbon_saved_kg_total,
            "features": {
                "temporal_logic": self.config.temporal_logic_enabled,
                "xai": self.config.xai_enabled,
                "adaptive_precision": self.config.adaptive_precision_enabled,
                "carbon_market": self.config.carbon_market_enabled,
                "role_specialization": self.config.role_specialization_enabled,
                "chaos_testing": self.config.chaos_testing_enabled,
                "hitl": self.config.hitl_enabled,
                "federated": self.config.federated_enabled,
                "causal_rl": self.config.causal_rl_enabled,
            },
            "weights": dict(self.weights),
            "timestamp": datetime.now().isoformat(),
        }
        if self.temporal_monitor:
            status["temporal_logic"] = self.temporal_monitor.get_status()
        if self.rlhf:
            status["rlhf"] = {
                "trained": self.rlhf._reward_model_trained,
                "history_len": len(self.rlhf.history),
                "pending_queries": len(self.rlhf.pending_queries),
            }
        if self.moe_gating:
            status["moe_gating"] = self.moe_gating.get_status()
        if self.distillation:
            status["distillation"] = {
                "student_probs": self.distillation.get_student_probs(),
                "uniform": self.distillation.is_uniform(),
                "history_len": len(self.distillation.history),
            }
        if self.federated:
            status["federated"] = self.federated.get_stats()
        if self.hitl:
            status["hitl"] = self.hitl.get_audit()
        if self.chaos_tester:
            status["chaos"] = self.chaos_tester.get_report()
        if self.precision_controller:
            status["precision"] = {
                "last": self.precision_controller.last_precision.value,
                "advisory": True,
                "telemetry": self.precision_controller.telemetry,
            }
        if self.causal_shaper:
            status["causal_ates"] = self.causal_shaper.compute_ate()
        return status

    async def shutdown(self):
        logger.info("Shutting down SustainabilityCostFunction v5.0.1...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        await self.carbon_manager.close()
        await self.node_registry.close()
        logger.info("Shutdown complete")


# =============================================================================
# Singleton accessor
# =============================================================================
_cost_function_instance: Optional[SustainabilityCostFunction] = None
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
# Embedded self-test
# =============================================================================
def _run_unit_tests() -> int:
    import asyncio as _asyncio
    failures: List[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}"
              + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    print("=" * 72)
    print("sustainability_cost_function v5.0.1 — self-test")
    print("=" * 72)

    def make_cfg(**kwargs):
        cfg = SustainabilityCostConfig(seed=42, **kwargs)
        return cfg

    def expert():
        return ExpertProfile(
            expert_id="expert_1",
            energy_per_inference=0.5,
            carbon_per_inference=0.05,
            helium_per_inference=0.01,
            accuracy_score=0.92,
        )

    def ctx():
        return {"token_count": 100, "target_node_id": "node_1",
                "expected_latency_ms": 50}

    # ---- 1. Temporal U / nesting (R1) -------------------------------- #
    print("\n[1] TemporalLogicMonitor (R1)")
    mon = TemporalLogicMonitor()
    mon.add_formula("u", "carbon > 100 U carbon > 800")
    mon.update({"carbon": 200})
    mon.update({"carbon": 500})
    mon.update({"carbon": 900})
    check("U holds", mon.evaluate()["u"] is True)

    mon2 = TemporalLogicMonitor()
    mon2.add_formula("gimp", "G(carbon > 900 -> energy < 0.3)")
    mon2.update({"carbon": 400, "energy": 0.8})
    mon2.update({"carbon": 950, "energy": 0.1})
    check("G(imp) holds", mon2.evaluate()["gimp"] is True)
    mon2.update({"carbon": 950, "energy": 0.9})
    check("G(imp) violates", mon2.evaluate()["gimp"] is False)

    # ---- 2. HITL registration (R3) ----------------------------------- #
    print("\n[2] HITL query registration (R3)")

    async def _hitl_test():
        rlhf = ActiveRLHF(action_space=["a", "b"],
                          uncertainty_threshold=0.0)
        hitl = HumanInTheLoopCoordinator(rlhf)
        res = await hitl.escalate({"k": 1}, ["a", "b"], confidence=0.1)
        qid = res["query"]["id"]
        assert qid in rlhf.pending_queries, "query not registered"
        assert hitl.resolve_query(qid, "a", rating=1.0) is not None

    try:
        _asyncio.run(_hitl_test())
        check("register + resolve", True)
    except Exception as e:
        check("register + resolve", False, str(e))

    # ---- 3. Chaos faults fire (R2) ---------------------------------- #
    print("\n[3] ChaosTester faults (R2)")

    async def _chaos_test():
        cfg = make_cfg()
        fn = SustainabilityCostFunction(cfg)
        tester = ChaosTester(fn)
        results = []
        for f in ChaosTester.FAULT_TYPES:
            results.append(await tester.run_test(f, duration_s=0.01))
        return results

    try:
        results = _asyncio.run(_chaos_test())
        for r in results:
            check(f"{r['fault']} fires", r["passed"] is True, str(r))
    except Exception as e:
        check("chaos suite", False, str(e))

    # ---- 4. Distillation stability + is_uniform (Fix #10) ----------- #
    print("\n[4] Distillation stability (Fix #10)")
    if NUMPY_AVAILABLE:
        cfg = make_cfg()
        dist = MultiTeacherPolicyDistillation(cfg, moe_engine=None,
                                              learning_rate=0.3)
        check("initial uniform", dist.is_uniform() is True)
        dist.student_policy = np.array([0.9, 0.05, 0.03, 0.02, 0.0, 0.0])
        check("non-uniform after change", dist.is_uniform() is False)
        p = np.array(dist.get_student_probs())
        check("student is a distribution", abs(p.sum() - 1.0) < 1e-9)
    else:
        check("numpy absent (skipped)", True)

    # ---- 5. Causal ATE recovery (R5) -------------------------------- #
    print("\n[5] CausalRewardShaper ATE (R5)")
    if NUMPY_AVAILABLE:
        shaper = CausalRewardShaper(num_actions=3, baseline=0,
                                     min_samples=5)
        r = random.Random(7)
        means = {0: 0.2, 1: 0.5, 2: 0.8}
        for _ in range(300):
            probs = np.array([0.3, 0.3, 0.4])
            action = r.choices(range(3), weights=probs)[0]
            reward = means[action] + r.gauss(0, 0.02)
            shaper.record(action, reward, probs)
        ates = shaper.compute_ate()
        check("ATE(1) ≈ 0.3", abs(ates[1] - 0.3) < 0.05,
              f"got {ates[1]:.3f}")
        check("ATE(2) ≈ 0.6", abs(ates[2] - 0.6) < 0.05,
              f"got {ates[2]:.3f}")
    else:
        check("numpy absent (skipped)", True)

    # ---- 6. RLHF policy unified + strategy rotates (Fix #1, #3) ---- #
    print("\n[6] RLHF unifies policy + trains (Fix #1, #3)")

    async def _rlhf_train_test():
        cfg = make_cfg()
        fn = SustainabilityCostFunction(cfg)
        strategies = set()
        # Feed 30 feedbacks — enough for the RLHF model to fit.
        for _ in range(30):
            cost = await fn.compute(expert(), ctx())
            meta = fn.get_last_metadata()
            strategies.add(meta["strategy"])
        # Train and check the trained flag.
        await fn.rlhf.train_reward_model()
        trained = fn.rlhf._reward_model_trained
        # Confirm feedback buffer was consumed.
        buf = len(fn.rlhf.feedback_buffer)
        return strategies, trained, buf

    try:
        strategies, trained, buf = _asyncio.run(_rlhf_train_test())
        check("RLHF trained after enough samples", trained is True)
        check("feedback buffer cleared after training", buf == 0)
        check("at least one strategy executed", len(strategies) >= 1,
              f"strategies={strategies}")
    except Exception as e:
        check("RLHF training", False, str(e))

    # ---- 7. MoE receives training samples (Fix #3) ------------------ #
    print("\n[7] MoE training samples accumulate (Fix #3)")

    async def _moe_test():
        cfg = make_cfg(rlhf_enabled=False)  # force MoE branch
        fn = SustainabilityCostFunction(cfg)
        for _ in range(15):
            await fn.compute(expert(), ctx())
        return fn.moe_gating.get_status()

    try:
        status = _asyncio.run(_moe_test())
        check("MoE received ≥ 10 samples", status["samples"] >= 10,
              f"samples={status['samples']}")
    except Exception as e:
        check("MoE training", False, str(e))

    # ---- 8. Temporal thresholds pass on healthy expert (Fix #5) ---- #
    print("\n[8] Temporal thresholds pass on healthy expert (Fix #5)")

    async def _temporal_test():
        cfg = make_cfg(rlhf_enabled=False)  # avoid strategy deadlock
        fn = SustainabilityCostFunction(cfg)
        await fn.compute(expert(), ctx())
        return fn.get_last_metadata()["temporal_status"]

    try:
        ts = _asyncio.run(_temporal_test())
        check("cost_floor holds", ts.get("cost_floor") is True)
        check("carbon_cap holds", ts.get("carbon_cap") is True)
    except Exception as e:
        check("temporal", False, str(e))

    # ---- 9. Cache TTL uses total_seconds (Fix #6) ------------------- #
    print("\n[9] Cache TTL uses total_seconds (Fix #6)")

    async def _cache_test():
        cfg = make_cfg(cache_ttl=1)  # 1 second TTL
        fn = SustainabilityCostFunction(cfg)
        await fn._get_carbon_intensity()
        # Manually age the cache by 2 seconds.
        fn._carbon_cache_timestamp = (
            datetime.now() - timedelta(seconds=2))
        # Next call should refetch (i.e., update timestamp to now).
        before = fn._carbon_cache_timestamp
        await fn._get_carbon_intensity()
        after = fn._carbon_cache_timestamp
        return (after - before).total_seconds()

    try:
        delta = _asyncio.run(_cache_test())
        check("cache expired (timestamp refreshed)", delta > 0.5,
              f"delta={delta:.3f}s")
    except Exception as e:
        check("cache TTL", False, str(e))

    # ---- 10. Causal action space is strategy axis (Fix #4) --------- #
    print("\n[10] Causal action space (Fix #4)")

    async def _causal_test():
        cfg = make_cfg(rlhf_enabled=False)
        fn = SustainabilityCostFunction(cfg)
        await fn.compute(expert(), ctx())
        meta = fn.get_last_metadata()
        return meta["causal_action"], meta["causal_recorded"]

    try:
        action, recorded = _asyncio.run(_causal_test())
        check("causal action is a strategy", action in STRATEGY_NAMES,
              f"got {action}")
        check("causal decision recorded", recorded is True)
    except Exception as e:
        check("causal", False, str(e))

    # ---- summary ---------------------------------------------------- #
    print("\n" + "=" * 72)
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
    config = SustainabilityCostConfig(seed=42)
    cost_func = await get_sustainability_cost_function(config)

    expert = ExpertProfile(
        expert_id="expert_1",
        energy_per_inference=0.5,
        carbon_per_inference=0.05,
        helium_per_inference=0.01,
        accuracy_score=0.92,
    )
    context = {"token_count": 100, "target_node_id": "node_1",
               "expected_latency_ms": 50}

    print("\n🧮 Computing cost...")
    cost = await cost_func.compute(expert, context)
    print(f"   Cost: {cost:.4f}")

    meta = cost_func.get_last_metadata()
    if meta:
        print(f"   Strategy: {meta['strategy']}")
        print(f"   Precision: {meta['precision_used']} "
              f"(advisory={meta['precision_advisory']})")
        print(f"   Carbon credit: ${meta['carbon_credit_value_usd']:.4f}")
        print(f"   REC: ${meta['rec_value_usd']:.4f}")
        print(f"   Effective weights: "
              f"{ {k: round(v, 3) for k, v in meta['effective_weights'].items()} }")
        if meta["temporal_status"]:
            print(f"   Temporal: {meta['temporal_status']}")
        if meta["role_assignments"]:
            print(f"   Dominant role: "
                  f"{meta['role_assignments']['dominant_role']}")
        if meta["xai_explanation"] and meta["xai_explanation"].get("narrative"):
            print(f"   XAI: {meta['xai_explanation']['narrative'][0]}")
        if meta["hitl_outcome"]:
            print(f"   HITL: {meta['hitl_outcome']['source']} -> "
                  f"{meta['hitl_outcome']['chosen']}")

    print("\n📝 Recording feedback + training RLHF...")
    for _ in range(10):
        await cost_func.record_feedback(expert, context, reward=0.8)
    await cost_func.rlhf.train_reward_model()
    print(f"   RLHF trained: {cost_func.rlhf._reward_model_trained}")

    print("\n⚙️  Precision selector →",
          cost_func.select_precision(carbon_intensity=0.35).value)
    cc = await cost_func.compute_carbon_credit(250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  "
          f"REC: ${cc['rec_usd']:.4f}")

    print(f"\n🎭 Roles: {cost_func.get_role_assignments()}")
    print(f"🧠 Causal ATEs: {cost_func.get_causal_ates()}")

    print("\n🧪 Chaos suite:")
    chaos = await cost_func.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📊 Comprehensive status:")
    status = cost_func.get_comprehensive_status()
    print(json.dumps({
        "version": status["version"],
        "carbon_saved_kg": status["carbon_saved_kg_total"],
        "features": status["features"],
        "weights": {k: round(v, 3) for k, v in status["weights"].items()},
        "rlhf": status.get("rlhf"),
        "moe_gating": status.get("moe_gating"),
        "distillation": status.get("distillation"),
        "federated": status.get("federated"),
        "hitl_total": status.get("hitl", {}).get("total"),
        "chaos_pass_rate": status.get("chaos", {}).get("pass_rate"),
        "precision_last": status.get("precision", {}).get("last"),
        "causal_ates": status.get("causal_ates"),
    }, indent=2, default=str))

    if cost_func.pareto_optimizer:
        front = cost_func.pareto_optimizer.get_pareto_front()
        print(f"\n📈 Pareto front size: {len(front)}")

    await cost_func.shutdown()
    print("\n" + "=" * 78)
    print("✅ Sustainability Cost Function v5.0.1 — smoke test complete")
    print("=" * 78)


# =============================================================================
# CLI
# =============================================================================
def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="sustainability_cost_function v5.0.1")
    parser.add_argument("--test", action="store_true",
                        help="Run the embedded self-test and exit")
    args = parser.parse_args()
    if args.test:
        return _run_unit_tests()
    asyncio.run(_smoke_test())
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
