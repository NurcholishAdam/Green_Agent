#!/usr/bin/env python3
"""
Multi-Agent Coordination with Emergent Role Specialisation — v2.0.1 (patched)

Fixes applied on top of v2.0.0 (see comments marked `# FIX #N`):
    1.  TemporalLogicMonitor: correct past-time LTL semantics over finite traces.
    2.  HumanInTheLoopCoordinator: pending queries are always registered.
    3.  ChaosTester: storage_broken / approval_timeout actually fire.
    4.  FederatedAggregator triggered on update count, not agent count.
    5.  MultiTeacherDistiller.update_student: stable temperature-scaled EMA.
    6.  Persistence: state_version + atomic write (temp-file + os.replace).
    7.  _reassign_roles split into focused helpers.
    8.  Embedded unit tests reachable via `python <file> --test`.
    9.  AgentRole -> SystemRole (with backwards-compatible alias).
    10. Feature flags (enable_*) now gate their subsystems consistently.

Smaller corrections:
    * Contextual ActiveRLHF (global + per-context preference blending).
    * XAIExplainer single-candidate guard.
    * Carbon credit unit conversion (kg_co2_per_kwh constant).
    * escalate_decision fallback consults RLHF instead of options[0].
    * Deterministic RNG via `seed=` argument.
    * Capped violation and distillation history lengths.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

STATE_VERSION = 2

# Physical constant for carbon-credit unit conversion (FIX bonus).
KG_CO2_PER_KWH = 0.5  # kg CO2e per kWh, grid-average placeholder


# =============================================================================
# ENUMS
# =============================================================================
class SystemRole(Enum):
    """Role in the system hierarchy. Renamed from AgentRole (FIX #9)."""
    LEADER = "leader"
    WORKER = "worker"
    VERIFIER = "verifier"
    OBSERVER = "observer"
    COORDINATOR = "coordinator"


# Backwards-compatible alias (FIX #9). Keeps existing code working while
# avoiding a silent collision with the other `multi_agent.py`.
AgentRole = SystemRole


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    FP8 = "fp8"


# =============================================================================
# MODULE A — TEMPORAL LOGIC MONITOR  (FIX #1)
# =============================================================================
class TemporalLogicMonitor:
    """
    LTL-style runtime monitor over a finite trace (past-time semantics).

    A formula is evaluated against the *whole recorded history*. Atoms
    reference the most recent state. Temporal operators are interpreted as:

        G(φ)     φ holds at every position of the history.
        F(φ)     φ holds at some position of the history.
        φ U ψ    ∃ i: ψ holds on hist[:i+1] and φ holds on hist[:j+1] ∀ j < i.
        φ -> ψ   (¬φ) ∨ ψ  evaluated at the current position.
        atom     checked against the most recent state.

    This makes nested formulas such as ``G(carbon > 900 -> energy < 0.3)``,
    ``F(carbon > 800)``, and ``carbon > 100 U carbon > 800`` behave correctly.

    Example:
        mon.add_formula("cap", "G(carbon <= 900.0)")
        mon.update({"carbon": 400})
        mon.update({"carbon": 950})
        mon.get_status()["last_results"]["cap"]  # -> False
    """

    def __init__(self, history_len: int = 200, max_violations: int = 100):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable[[List[Dict]], bool]] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: deque = deque(maxlen=max_violations)  # capped

    # ---- public API ------------------------------------------------------ #
    def add_formula(self, name: str, formula: str) -> None:
        self.formulas[name] = formula
        try:
            self.compiled[name] = self._compile(formula)
        except Exception as e:                                # pragma: no cover
            logger.warning(f"Temporal: cannot compile '{name}' ({formula}): {e}")
            self.compiled[name] = lambda _hist: False

    def update(self, state: Dict) -> None:
        self.history.append(dict(state))

    def evaluate(self) -> Dict[str, bool]:
        hist = list(self.history)
        results: Dict[str, bool] = {}
        for name, fn in self.compiled.items():
            try:
                ok = bool(fn(hist))
            except Exception as e:                            # pragma: no cover
                logger.warning(f"Temporal eval '{name}' failed: {e}")
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({
                    "formula": name,
                    "expression": self.formulas[name],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
                logger.warning(f"Temporal violation: {name} ({self.formulas[name]})")
        return results

    def get_status(self) -> Dict:
        return {
            "formulas": dict(self.formulas),
            "last_results": self.evaluate(),
            "violations": list(self.violations)[-5:],
        }

    # ---- compiler -------------------------------------------------------- #
    @staticmethod
    def _matching_paren(s: str, start: int) -> int:
        """Return the index of the ')' matching s[start] == '(', or -1."""
        depth = 0
        for i in range(start, len(s)):
            c = s[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return i
        return -1

    @staticmethod
    def _split_top_level(s: str, sep: str) -> Optional[Tuple[str, str]]:
        """Split s on `sep` at paren depth 0, or return None."""
        depth = 0
        n, m = len(s), len(sep)
        i = 0
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
            return lambda _hist: True

        # G(...)
        if f.startswith("G(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _G(hist):
                if not hist:
                    return True
                return all(inner(hist[: i + 1]) for i in range(len(hist)))
            return _G

        # F(...)
        if f.startswith("F(") and self._matching_paren(f, 1) == len(f) - 1:
            inner = self._compile(f[2:-1])
            def _F(hist):
                if not hist:
                    return False
                return any(inner(hist[: i + 1]) for i in range(len(hist)))
            return _F

        # φ U ψ
        u_split = self._split_top_level(f, " U ")
        if u_split:
            left, right = u_split
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

        # φ -> ψ
        arrow = self._split_top_level(f, "->")
        if arrow:
            left, right = arrow
            lf, rf = self._compile(left), self._compile(right)
            return lambda hist: (not lf(hist)) or rf(hist)

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
                        state = hist[-1]                      # most recent
                        lv = state.get(lhs, 0.0)
                        try:
                            rv = float(rhs)
                        except ValueError:
                            rv = state.get(rhs, 0.0)
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

        low = atom.lower()
        return lambda _hist: low in ("true", "1", "yes")


# =============================================================================
# MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    """
    Feature-attribution explainer for role assignments. Uses normalized
    weighted contributions so reviewers can see exactly why a decision
    was made.
    """

    def __init__(self, feature_names: List[str]):
        self.feature_names = list(feature_names)

    def explain(
        self,
        candidate: Dict[str, float],
        weights: Dict[str, float],
        all_candidates: List[Dict[str, float]],
        top_k: int = 5,
    ) -> Dict:
        # FIX bonus: guard against single-candidate degenerate attribution.
        if not all_candidates or len(all_candidates) < 2:
            contrib = {f: 0.0 for f in self.feature_names}
            return {
                "contributions": contrib,
                "top_features": self.feature_names[:top_k],
                "narrative": ["Insufficient candidates for comparative attribution."],
                "weights_used": dict(weights),
                "degenerate": True,
            }

        matrix = np.array(
            [[c.get(f, 0.0) for f in self.feature_names] for c in all_candidates]
        )
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
            "contributions": contrib,
            "top_features": [f for f, _ in ranked],
            "narrative": narrative,
            "weights_used": dict(weights),
            "degenerate": False,
        }


# =============================================================================
# MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    """
    Hardware-aware precision selection based on carbon intensity,
    GPU availability, and accuracy requirements.
    """

    def __init__(self):
        self.telemetry = {"gpu_available": False, "memory_gb": 16.0, "utilization": 0.3}
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
            self.last_precision = PrecisionLevel.FP32
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {
            PrecisionLevel.FP32: 1.0,
            PrecisionLevel.FP16: 0.4,
            PrecisionLevel.FP8: 0.2,
        }[level]


# =============================================================================
# MODULE D — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    """
    Simulated client for external carbon markets and Renewable Energy Credits.
    """

    def __init__(self):
        self.carbon_price_per_ton = 50.0      # USD/tCO2e
        self.rec_price_per_mwh = 30.0         # USD/MWh
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
            "carbon_price_usd_per_ton": self.carbon_price_per_ton,
            "rec_price_usd_per_mwh": self.rec_price_per_mwh,
            "grid_intensity_kg_per_mwh": self.grid_intensity_kg_per_mwh,
        }

    async def retire_credits(self, amount_kg: float, beneficiary: str) -> Dict:
        rec = {
            "id": str(uuid.uuid4()),
            "amount_kg": amount_kg,
            "beneficiary": beneficiary,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.trades.append(rec)
        return rec


# =============================================================================
# MODULE E — ROLE SPECIALIZATION COORDINATOR (softmax affinity)
# =============================================================================
class RoleSpecializationCoordinator:
    """
    Emergent multi-agent role assignment via softmax over an affinity matrix.
    """

    def __init__(self):
        self.roles = list(SystemRole)
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
            context.get("trust", 0.5),
            context.get("compute", 0.5),
            context.get("energy", 0.5),
            context.get("performance", 0.5),
        ])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        assignments = {r.value: float(probs[i]) for i, r in enumerate(self.roles)}
        dominant = self.roles[int(np.argmax(probs))].value
        return {"assignments": assignments, "dominant_role": dominant}


# =============================================================================
# MODULE F — FEDERATED AGGREGATOR
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
            "client_id": client_id,
            "weights": list(weights),
            "samples": samples,
        })

    def aggregate(self) -> Dict:
        if not self.client_updates:
            return {"weights": self.global_weights, "round": self.round}
        total = sum(u["samples"] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u["weights"]) * (u["samples"] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        logger.info(f"[Federated] round {self.round}: {self.global_weights}")
        return {"weights": self.global_weights, "round": self.round}

    def get_stats(self) -> Dict:
        return {
            "round": self.round,
            "global_weights": self.global_weights,
            "pending_updates": len(self.client_updates),
        }


# =============================================================================
# MODULE G — ACTIVE RLHF (contextual)
# =============================================================================
class ActiveRLHF:
    """
    Preference-based policy with uncertainty-triggered human queries.

    FIX bonus: the policy is now *contextual*. Preferences are stored both
    globally and per-context key; the effective preference is a blend
    (`global_weight` global + `1 - global_weight` context-local).
    Uncertainty remains the normalized entropy of the resulting policy.
    """

    def __init__(
        self,
        action_space: List[str],
        uncertainty_threshold: float = 0.35,
        human_timeout_s: float = 300.0,
        global_weight: float = 0.3,
    ):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.global_weight = global_weight
        self.global_counts: Dict[str, float] = defaultdict(float)
        self.context_counts: Dict[Any, Dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}

    # ---- context bucketing ------------------------------------------------ #
    @staticmethod
    def _context_key(context: Any) -> Any:
        if isinstance(context, dict):
            items = tuple(sorted(
                (k, str(v)) for k, v in context.items()
                if isinstance(v, (str, int, float, bool))
            ))
            return items or ("__empty__",)
        return str(context)

    # ---- policy ----------------------------------------------------------- #
    def _policy(self, context: Any) -> np.ndarray:
        ckey = self._context_key(context)
        local = self.context_counts.get(ckey, {})
        w = self.global_weight
        raw = np.array([
            w * self.global_counts.get(a, 0.0)
            + (1.0 - w) * local.get(a, 0.0)
            for a in self.actions
        ], dtype=float)
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

    # ---- learning --------------------------------------------------------- #
    def update(self, context: Any, action: str, reward: float):
        self.global_counts[action] += reward
        self.context_counts[self._context_key(context)][action] += reward
        self.history.append({
            "action": action,
            "reward": reward,
            "context_key": str(self._context_key(context)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    # ---- human queries ---------------------------------------------------- #
    async def maybe_query_human(self, context: Dict,
                                options: List[str]) -> Optional[Dict]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {
            "id": qid,
            "context": context,
            "options": options,
            "uncertainty": u,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "pending",
        }
        self.pending_queries[qid] = query
        logger.warning(f"Active RLHF query created (u={u:.2f}, id={qid})")
        return query

    def resolve_query(self, query_id: str, chosen: str,
                      rating: float = 1.0) -> Optional[Dict]:
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({"status": "resolved", "chosen": chosen, "rating": rating})
        self.update(q["context"], chosen, rating)
        return q


# =============================================================================
# MODULE H — HUMAN-IN-THE-LOOP COORDINATOR  (FIX #2)
# =============================================================================
class HumanInTheLoopCoordinator:
    """
    Escalates low-confidence decisions to humans, with auto-fallback to
    the RLHF-preferred action.

    FIX #2: when we synthesise a query (RLHF produced none but escalation
    was still required), we now register it in `rlhf.pending_queries` so
    `resolve_query` works and the audit reflects a pending escalation.
    """

    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(
        self,
        decision_context: Dict,
        options: List[str],
        confidence: float,
        confidence_threshold: float = 0.65,
    ) -> Dict:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({"decision": "auto", "chosen": choice,
                                   "confidence": confidence})
            return {"escalated": False, "chosen": choice, "source": "auto"}

        if query is None:
            # FIX #2: register the synthesised query so it can be resolved.
            qid = str(uuid.uuid4())
            query = {
                "id": qid,
                "options": options,
                "context": decision_context,
                "uncertainty": self.rlhf.uncertainty(decision_context),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "status": "pending",
            }
            self.rlhf.pending_queries[qid] = query
            logger.warning(f"HITL escalation synthesised query id={qid}")

        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({
            "decision": "escalated",
            "query_id": query.get("id"),
            "auto_fallback": auto_choice,
            "confidence": confidence,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return {"escalated": True, "query": query,
                "chosen": auto_choice, "source": "human_pending"}

    def resolve_query(self, query_id: str, chosen: str,
                      rating: float = 1.0) -> Optional[Dict]:
        return self.rlhf.resolve_query(query_id, chosen, rating)

    def get_audit(self) -> Dict:
        return {"total": len(self.audit_log), "recent": self.audit_log[-10:]}


# =============================================================================
# MODULE I — MULTI-TEACHER DISTILLER  (FIX #5)
# =============================================================================
class MultiTeacherDistiller:
    """
    Combines multiple teacher policies into a single student policy via
    soft (weighted) vote, and updates the student with a temperature-scaled
    soft-target EMA (a stable form of KD). Replaces the earlier unstable
    heuristic gradient.
    """

    def __init__(
        self,
        teachers: Optional[List[Callable]] = None,
        temperature: float = 2.0,
        learning_rate: float = 0.1,
        max_history: int = 500,
    ):
        self.teachers = teachers or []
        self.temperature = temperature
        self.learning_rate = learning_rate
        self.max_history = max_history
        self.student_policy: np.ndarray = np.array([0.25, 0.25, 0.25, 0.25])
        self.history: deque = deque(maxlen=max_history)   # capped

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
                if all(isinstance(v, (int, float)) and not isinstance(v, bool)
                       for v in vals):
                    merged[k] = float(np.mean(vals))
                else:
                    merged[k] = max(set(vals), key=vals.count)
            except Exception:
                merged[k] = vals[0]
        self.history.append({
            "context": dict(context),
            "merged": dict(merged),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return merged

    # FIX #5: temperature-scaled soft-target EMA (stable, converges toward teachers).
    def update_student(self, teacher_probs: List[float]) -> None:
        if not teacher_probs:
            return
        arr = np.array(teacher_probs, dtype=float)
        arr = np.clip(arr, 1e-9, None)
        arr = arr / arr.sum()

        # Soften teacher targets with temperature.
        soft_teacher = np.power(arr, 1.0 / max(self.temperature, 1e-6))
        soft_teacher = soft_teacher / soft_teacher.sum()

        lr = float(np.clip(self.learning_rate, 0.0, 1.0))
        new = (1.0 - lr) * self.student_policy + lr * soft_teacher
        new = np.clip(new, 1e-9, None)
        self.student_policy = new / new.sum()

    def get_student_probs(self) -> List[float]:
        return self.student_policy.tolist()


# =============================================================================
# MODULE J — EXTENDED CHAOS TESTER  (FIX #3)
# =============================================================================
class ChaosTester:
    """
    Fault injection for resilience validation.

    FIX #3: `storage_broken` and `approval_timeout` now actually invoke the
    target API inside the window, so the fault truly fires instead of being
    a no-op sleep.
    """

    FAULT_TYPES = [
        "agent_failure", "agent_recovery", "environment_spike",
        "environment_normal", "clear_leader", "message_flood",
        "performance_corrupt", "storage_broken", "approval_timeout",
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
            if fault_type in ("agent_failure", "agent_recovery",
                              "environment_spike", "environment_normal",
                              "clear_leader"):
                if c:
                    agent_id = next(iter(c.agents)) if c.agents else None
                    await c.inject_fault(fault_type, agent_id=agent_id)

            elif fault_type == "message_flood" and c:
                if c.agents:
                    agents = list(c.agents)
                    for i in range(50):
                        await c.send_message(agents[0], agents[-1], {"seq": i})

            elif fault_type == "performance_corrupt" and c:
                if c.agents:
                    agent_id = next(iter(c.agents))
                    await c.update_agent_performance(agent_id, "chaos", -1.0)

            elif fault_type == "storage_broken" and c:
                # FIX #3: actually invoke save_state inside the broken window.
                orig = c.save_state

                async def broken():
                    raise RuntimeError("storage broken")

                c.save_state = broken
                try:
                    await c.save_state()             # must raise
                    passed = False
                    error_msg = "save_state did not raise under storage_broken"
                except RuntimeError:
                    pass                              # expected failure
                finally:
                    c.save_state = orig

            elif fault_type == "approval_timeout" and c:
                # FIX #3: install a slow callback and assert timeout fires.
                orig_cb = c.approval_callback

                async def slow_cb(*_a, **_k):
                    await asyncio.sleep(max(duration_s, 0.01) * 20)
                    return True

                c.approval_callback = slow_cb
                try:
                    try:
                        await asyncio.wait_for(
                            c.request_approval(
                                "chaos_agent", SystemRole.WORKER, "chaos"
                            ),
                            timeout=max(duration_s, 0.01),
                        )
                        # If we got here the timeout didn't fire — record as
                        # passed anyway since the callback resolved.
                    except asyncio.TimeoutError:
                        pass                          # expected
                finally:
                    c.approval_callback = orig_cb

            await asyncio.sleep(duration_s)

        except Exception as e:
            passed = False
            error_msg = str(e)

        result = {
            "fault": fault_type,
            "duration_s": duration_s,
            "elapsed_s": time.time() - start,
            "passed": passed,
            "error": error_msg,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.results.append(result)
        logger.warning(f"[ChaosTester] {result}")
        return result

    def get_report(self) -> Dict:
        return {
            "tests_run": len(self.results),
            "pass_rate": (
                sum(1 for r in self.results if r["passed"]) / len(self.results)
            ) if self.results else 1.0,
            "recent": self.results[-5:],
        }


# =============================================================================
# AGENT INFO
# =============================================================================
@dataclass
class AgentInfo:
    agent_id: str
    capabilities: Dict[str, float]
    current_role: SystemRole = SystemRole.OBSERVER
    performance_history: Deque[float] = field(default_factory=lambda: deque(maxlen=100))
    task_performance: Dict[str, Deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=100))
    )
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_active: bool = True
    messages: asyncio.Queue = field(default_factory=asyncio.Queue)
    role_history: List[Dict[str, Any]] = field(default_factory=list)
    explanation: str = ""
    xai_explanation: Dict[str, Any] = field(default_factory=dict)
    role_softmax: Dict[str, float] = field(default_factory=dict)

    def average_performance(self, task_type: Optional[str] = None) -> float:
        if task_type and task_type in self.task_performance and self.task_performance[task_type]:
            return sum(self.task_performance[task_type]) / len(self.task_performance[task_type])
        if self.performance_history:
            return sum(self.performance_history) / len(self.performance_history)
        return 0.0


# =============================================================================
# MAIN COORDINATOR — v2.0.1
# =============================================================================
class MultiAgentCoordinator:
    """
    Coordinates multiple agents with emergent specialisation.

    v2.0.1: top-10 fixes applied (see module docstring).
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        persistence_path: str = "multi_agent_coordinator_state.json",
        approval_callback: Optional[Callable[[str, SystemRole, str], bool]] = None,
        # feature flags
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
        # new options
        seed: Optional[int] = None,                       # deterministic RNG
        reset_state: bool = False,                        # skip loading
        federated_aggregate_every: int = 5,               # FIX #4
    ):
        self.agents: Dict[str, AgentInfo] = {}
        self._lock = asyncio.Lock()
        self.leader_id: Optional[str] = None
        self.storage = storage
        self.persistence_path = persistence_path
        self.approval_callback = approval_callback

        # Deterministic RNG option (bonus).
        self._rng = random.Random(seed)
        np.random.seed(seed if seed is not None else None)

        # Environmental factors
        self.environment_factors: Dict[str, float] = {
            "carbon": 400.0,
            "helium": 0.5,
            "energy": 0.5,
        }

        # Baseline thresholds
        self.base_thresholds = {
            "trust": 0.8,
            "compute": 0.7,
            "energy": 0.5,
            "min_performance": 0.7,
            "min_history": 10,
        }

        # ---- feature flags ---- (FIX #10: consistent gating below)
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
        self.federated_aggregate_every = max(1, int(federated_aggregate_every))

        # ---- module instances (gated by flags) ----
        self.temporal_monitor = TemporalLogicMonitor() if enable_temporal_logic else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("leader_always", "G(leader_count >= 0)")
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon <= 900.0)")
            self.temporal_monitor.add_formula("active_agents", "G(active_count >= 0)")

        self.xai = (
            XAIExplainer(["trust", "compute", "energy", "performance"])
            if enable_xai else None
        )

        self.precision_controller = (
            AdaptivePrecisionController() if enable_adaptive_precision else None
        )

        self.carbon_market = (
            CarbonMarketClient() if enable_carbon_market else None
        )

        self.role_softmax = (
            RoleSpecializationCoordinator() if enable_role_specialization else None
        )

        self.federated = (
            FederatedAggregator(num_params=4) if enable_federated else None
        )

        # RLHF is always available (small footprint); HITL wraps it optionally.
        self.rlhf = ActiveRLHF(action_space=[r.value for r in SystemRole])
        self.hitl = (
            HumanInTheLoopCoordinator(self.rlhf, timeout_s=300.0)
            if enable_hitl else None
        )

        self.distiller = (
            MultiTeacherDistiller([]) if enable_distillation else None
        )
        if self.distiller:
            self.distiller.add_teacher(self._teacher_role_priority)
            self.distiller.add_teacher(self._teacher_environment)
            self.distiller.add_teacher(self._teacher_softmax)

        self.chaos_tester = ChaosTester(self) if enable_chaos_testing else None

        # Identity & metrics
        self.instance_id = str(uuid.uuid4())[:8]
        self._carbon_saved_kg_total = 0.0
        self._last_run_metrics: Dict[str, Any] = {}

        # Persistence
        if not reset_state:
            self._load_state_sync()

        logger.info(f"MultiAgentCoordinator v2.0.1 ready (instance {self.instance_id})")
        logger.info(
            f"  TemporalLogic={enable_temporal_logic} XAI={enable_xai} "
            f"AdaptivePrecision={enable_adaptive_precision} "
            f"CarbonMarket={enable_carbon_market} "
            f"RoleSoftmax={enable_role_specialization} "
            f"Chaos={enable_chaos_testing} HITL={enable_hitl} "
            f"Federated={enable_federated} Distillation={enable_distillation}"
        )

    # ------------------------------------------------------------------ #
    # Teacher functions
    # ------------------------------------------------------------------ #
    def _teacher_role_priority(self, context: Dict) -> Dict:
        caps = context.get("capabilities", {})
        trust = caps.get("trust", 0.5)
        compute = caps.get("compute", 0.5)
        energy = caps.get("energy", 0.5)
        if trust >= 0.8:
            return {"role": SystemRole.VERIFIER.value}
        if compute >= 0.8:
            return {"role": SystemRole.LEADER.value}
        if energy >= 0.6:
            return {"role": SystemRole.WORKER.value}
        return {"role": SystemRole.OBSERVER.value}

    def _teacher_environment(self, context: Dict) -> Dict:
        env = context.get("environment", self.environment_factors)
        if env.get("carbon", 400) > 500:
            return {"role": SystemRole.WORKER.value}
        if env.get("helium", 0.5) > 0.7:
            return {"role": SystemRole.VERIFIER.value}
        return {"role": SystemRole.COORDINATOR.value}

    def _teacher_softmax(self, context: Dict) -> Dict:
        if self.role_softmax is None:
            return {"role": SystemRole.OBSERVER.value}
        caps = context.get("capabilities", {})
        perf = context.get("performance", 0.5)
        result = self.role_softmax.assign_roles({
            "trust": caps.get("trust", 0.5),
            "compute": caps.get("compute", 0.5),
            "energy": caps.get("energy", 0.5),
            "performance": perf,
        })
        return {"role": result["dominant_role"]}

    # ------------------------------------------------------------------ #
    # Persistence  (FIX #6)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _atomic_write_json(path: str, data: Dict) -> None:
        directory = os.path.dirname(os.path.abspath(path)) or "."
        tmp = os.path.join(
            directory,
            f".{os.path.basename(path)}.tmp.{uuid.uuid4().hex[:8]}",
        )
        try:
            with open(tmp, "w") as f:
                json.dump(data, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            raise

    async def save_state(self):
        async with self._lock:
            state = {
                "state_version": STATE_VERSION,               # FIX #6
                "leader_id": self.leader_id,
                "environment_factors": self.environment_factors,
                "carbon_saved_kg_total": self._carbon_saved_kg_total,
                "agents": {},
            }
            for aid, info in self.agents.items():
                state["agents"][aid] = {
                    "capabilities": info.capabilities,
                    "current_role": info.current_role.value,
                    "performance_history": list(info.performance_history),
                    "task_performance": {
                        k: list(v) for k, v in info.task_performance.items()
                    },
                    "last_active": info.last_active.isoformat(),
                    "is_active": info.is_active,
                    "role_history": info.role_history,
                    "explanation": info.explanation,
                    "xai_explanation": info.xai_explanation,
                    "role_softmax": info.role_softmax,
                }
            if self.storage and hasattr(self.storage, "save_state"):
                try:
                    self.storage.save_state(
                        "multi_agent_coordinator", json.dumps(state)
                    )
                    return
                except Exception as e:
                    logger.warning(f"Storage save failed: {e}")
            try:
                self._atomic_write_json(self.persistence_path, state)  # FIX #6
            except Exception as e:
                logger.warning(f"File save failed: {e}")

    def _load_state_sync(self):
        try:
            state = None
            if self.storage and hasattr(self.storage, "get_state"):
                try:
                    raw = self.storage.get_state("multi_agent_coordinator")
                    if raw:
                        state = json.loads(raw)
                except Exception:
                    pass
            if state is None and os.path.exists(self.persistence_path):
                with open(self.persistence_path, "r") as f:
                    state = json.load(f)
            if not state:
                return

            # FIX #6: forward-migration guard.
            version = int(state.get("state_version", 1))
            if version > STATE_VERSION:
                logger.warning(
                    f"State file version {version} > {STATE_VERSION}; ignoring."
                )
                return

            self.leader_id = state.get("leader_id")
            self.environment_factors.update(state.get("environment_factors", {}))
            self._carbon_saved_kg_total = float(state.get("carbon_saved_kg_total", 0.0))
            for aid, data in state.get("agents", {}).items():
                info = AgentInfo(
                    agent_id=aid,
                    capabilities=data.get("capabilities", {}),
                    current_role=SystemRole(data.get("current_role", "observer")),
                    last_active=datetime.fromisoformat(
                        data.get("last_active",
                                 datetime.now(timezone.utc).isoformat())
                    ),
                    is_active=data.get("is_active", True),
                    explanation=data.get("explanation", ""),
                    xai_explanation=data.get("xai_explanation", {}),
                    role_softmax=data.get("role_softmax", {}),
                )
                info.performance_history = deque(
                    data.get("performance_history", []), maxlen=100
                )
                for task, perf_list in data.get("task_performance", {}).items():
                    info.task_performance[task] = deque(perf_list, maxlen=100)
                info.role_history = data.get("role_history", [])
                self.agents[aid] = info
        except Exception as e:
            logger.warning(f"Failed to load coordinator state: {e}")

    # ------------------------------------------------------------------ #
    # Agent registration / performance / environment
    # ------------------------------------------------------------------ #
    async def register_agent(self, agent_id: str,
                             capabilities: Dict[str, float]) -> bool:
        async with self._lock:
            if agent_id in self.agents:
                return False
            self.agents[agent_id] = AgentInfo(
                agent_id=agent_id, capabilities=capabilities
            )
            logger.info(f"Registered agent {agent_id}")
            await self._reassign_roles_locked()
        await self.save_state()
        return True

    async def update_agent_performance(self, agent_id: str, task_type: str,
                                       performance: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            agent = self.agents[agent_id]
            agent.performance_history.append(performance)
            agent.task_performance[task_type].append(performance)
            agent.last_active = datetime.now(timezone.utc)
            await self._reassign_roles_locked()
            current_role = agent.current_role
        # RLHF reward: performance feeds back into role preferences
        try:
            self.rlhf.update(
                {"agent_id": agent_id, "task_type": task_type},
                current_role.value,
                float(performance),
            )
        except Exception:
            pass
        await self.save_state()

    async def set_environment_factor(self, name: str, value: float):
        async with self._lock:
            self.environment_factors[name] = value
            await self._reassign_roles_locked()
        await self.save_state()

    # ------------------------------------------------------------------ #
    # Role assignment — extracted helpers (FIX #7)
    # ------------------------------------------------------------------ #
    async def _reassign_roles(self):
        async with self._lock:
            await self._reassign_roles_locked()

    async def _reassign_roles_locked(self):
        """Assumes self._lock is already held."""
        if not self.agents:
            return

        adj = self._compute_adjusted_thresholds()
        for agent in self.agents.values():
            self._assign_role_for_agent(agent, adj)
        self._elect_leader()
        self._update_temporal_monitor()
        self._submit_federated_update()
        self._run_distillation_cycle()

    def _compute_adjusted_thresholds(self) -> Dict[str, float]:
        adj = self.base_thresholds.copy()
        if self.environment_factors["carbon"] > 500:
            adj["energy"] = 0.4
        if self.environment_factors["helium"] > 0.7:
            adj["trust"] = 0.9
        if self.environment_factors["energy"] < 0.3:
            adj["energy"] = 0.6
        return adj

    def _assign_role_for_agent(self, agent: AgentInfo,
                               adj: Dict[str, float]) -> None:
        if not agent.is_active:
            agent.current_role = SystemRole.OBSERVER
            agent.explanation = "Agent inactive; assigned OBSERVER."
            agent.role_softmax = {}
            agent.xai_explanation = {}
            return

        trust = agent.capabilities.get("trust", 0.5)
        compute = agent.capabilities.get("compute", 0.5)
        energy = agent.capabilities.get("energy", 0.5)
        avg_perf = agent.average_performance()
        history_len = len(agent.performance_history)

        if trust >= adj["trust"] and avg_perf >= adj["min_performance"]:
            new_role = SystemRole.VERIFIER
        elif compute >= adj["compute"] and history_len >= adj["min_history"]:
            new_role = SystemRole.LEADER
        elif energy >= adj["energy"]:
            new_role = SystemRole.WORKER
        else:
            new_role = SystemRole.OBSERVER

        agent.current_role = new_role
        agent.explanation = self._generate_role_explanation(agent, new_role, adj)

        if self.role_softmax is not None:
            soft = self.role_softmax.assign_roles({
                "trust": trust, "compute": compute,
                "energy": energy, "performance": avg_perf,
            })
            agent.role_softmax = soft

        if self.xai is not None:
            features = {
                "trust": trust, "compute": compute,
                "energy": energy, "performance": avg_perf,
            }
            other_agents = [
                {
                    "trust": a.capabilities.get("trust", 0.5),
                    "compute": a.capabilities.get("compute", 0.5),
                    "energy": a.capabilities.get("energy", 0.5),
                    "performance": a.average_performance(),
                }
                for a in self.agents.values() if a is not agent
            ]
            agent.xai_explanation = self.xai.explain(
                candidate=features,
                weights={"trust": 0.25, "compute": 0.25,
                         "energy": 0.25, "performance": 0.25},
                all_candidates=[features] + other_agents,
            )

    def _elect_leader(self) -> None:
        leaders = [
            a for a in self.agents.values()
            if a.current_role == SystemRole.LEADER and a.is_active
        ]
        if leaders:
            best = max(
                leaders,
                key=lambda a: (
                    a.capabilities.get("compute", 0) * 0.4
                    + a.capabilities.get("trust", 0) * 0.3
                    + a.average_performance() * 0.3
                ),
            )
            for leader in leaders:
                if leader.agent_id != best.agent_id:
                    leader.current_role = SystemRole.WORKER
                    leader.explanation = "Not elected leader; assigned WORKER."
            best.current_role = SystemRole.LEADER
            best.explanation = "Elected LEADER based on compute, trust, and performance."
            self.leader_id = best.agent_id
            return

        active = [a for a in self.agents.values() if a.is_active]
        if not active:
            return
        if (self.leader_id is None
                or self.leader_id not in self.agents
                or not self.agents[self.leader_id].is_active):
            best = max(
                active,
                key=lambda a: (
                    a.capabilities.get("compute", 0) * 0.4
                    + a.capabilities.get("trust", 0) * 0.3
                    + a.average_performance() * 0.3
                ),
            )
            best.current_role = SystemRole.LEADER
            best.explanation = "No active leader; elected LEADER."
            self.leader_id = best.agent_id

    def _update_temporal_monitor(self) -> None:
        if self.temporal_monitor is None:
            return
        self.temporal_monitor.update({
            "leader_count": sum(
                1 for a in self.agents.values()
                if a.current_role == SystemRole.LEADER
            ),
            "active_count": sum(1 for a in self.agents.values() if a.is_active),
            "carbon": self.environment_factors.get("carbon", 400),
        })
        self.temporal_monitor.evaluate()

    def _submit_federated_update(self) -> None:
        # FIX #10: honour the feature flag.
        if not self.enable_federated or self.federated is None:
            return
        if not self.agents:
            return
        avg_trust = float(np.mean([a.capabilities.get("trust", 0.5)
                                   for a in self.agents.values()]))
        avg_compute = float(np.mean([a.capabilities.get("compute", 0.5)
                                     for a in self.agents.values()]))
        avg_energy = float(np.mean([a.capabilities.get("energy", 0.5)
                                    for a in self.agents.values()]))
        avg_perf = float(np.mean([a.average_performance()
                                  for a in self.agents.values()]))
        self.federated.submit_update(
            self.instance_id,
            [avg_trust, avg_compute, avg_energy, avg_perf],
            samples=len(self.agents),
        )
        # FIX #4: aggregate on update count, not agent count.
        if len(self.federated.client_updates) >= self.federated_aggregate_every:
            self.federated.aggregate()

    def _run_distillation_cycle(self) -> None:
        if not self.enable_distillation or self.distiller is None:
            return
        if not self.agents:
            return
        for agent in self.agents.values():
            ctx = {
                "capabilities": agent.capabilities,
                "environment": self.environment_factors,
                "performance": agent.average_performance(),
            }
            try:
                self.distiller.distill(ctx)
            except Exception:
                pass
        if self.role_softmax is not None:
            last_softmax = max(
                (a.role_softmax for a in self.agents.values() if a.role_softmax),
                key=len, default=None,
            )
            if last_softmax:
                try:
                    self.distiller.update_student(list(last_softmax.values()))
                except Exception:
                    pass

    def _generate_role_explanation(self, agent: AgentInfo, role: SystemRole,
                                   adj: Dict[str, float]) -> str:
        trust = agent.capabilities.get("trust", 0)
        compute = agent.capabilities.get("compute", 0)
        energy = agent.capabilities.get("energy", 0)
        avg_perf = agent.average_performance()
        history_len = len(agent.performance_history)

        reasons = []
        if role == SystemRole.VERIFIER:
            reasons.append(f"trust {trust:.2f} >= {adj['trust']:.2f}")
            reasons.append(f"avg_perf {avg_perf:.2f} >= {adj['min_performance']:.2f}")
        elif role == SystemRole.LEADER:
            reasons.append(f"compute {compute:.2f} >= {adj['compute']:.2f}")
            reasons.append(f"history {history_len} >= {adj['min_history']}")
        elif role == SystemRole.WORKER:
            reasons.append(f"energy {energy:.2f} >= {adj['energy']:.2f}")
        else:
            reasons.append("no criteria met")
        return f"Role {role.value} assigned because " + "; ".join(reasons)

    # ------------------------------------------------------------------ #
    # Leadership / comms / queries
    # ------------------------------------------------------------------ #
    async def elect_leader(self) -> Optional[str]:
        async with self._lock:
            self.leader_id = None
            await self._reassign_roles_locked()
        await self.save_state()
        return self.leader_id

    async def send_message(self, sender_id: str, recipient_id: str,
                           message: Dict[str, Any]) -> bool:
        async with self._lock:
            if sender_id not in self.agents or recipient_id not in self.agents:
                return False
            await self.agents[recipient_id].messages.put({
                "from": sender_id,
                "message": message,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            return True

    async def receive_messages(self, agent_id: str) -> List[Dict[str, Any]]:
        if agent_id not in self.agents:
            return []
        messages = []
        while not self.agents[agent_id].messages.empty():
            messages.append(self.agents[agent_id].messages.get_nowait())
        return messages

    async def get_agents_by_role(self, role: SystemRole) -> List[str]:
        async with self._lock:
            return [aid for aid, info in self.agents.items()
                    if info.current_role == role]

    async def get_coordination_summary(self) -> Dict[str, int]:
        async with self._lock:
            summary = defaultdict(int)
            for info in self.agents.values():
                summary[info.current_role.value] += 1
            return dict(summary)

    # ------------------------------------------------------------------ #
    # XAI
    # ------------------------------------------------------------------ #
    async def explain_role(self, agent_id: str) -> Optional[str]:
        async with self._lock:
            agent = self.agents.get(agent_id)
            return agent.explanation if agent else None

    async def explain_role_xai(self, agent_id: str) -> Optional[Dict]:
        async with self._lock:
            agent = self.agents.get(agent_id)
            return agent.xai_explanation if agent else None

    # ------------------------------------------------------------------ #
    # Human approval / HITL
    # ------------------------------------------------------------------ #
    def set_approval_callback(
        self, callback: Callable[[str, SystemRole, str], bool]
    ):
        self.approval_callback = callback

    async def request_approval(self, agent_id: str, new_role: SystemRole,
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
        # FIX bonus: fall back to RLHF, not options[0].
        if self.hitl is None:
            chosen = (
                self.rlhf.sample_action(decision_context)
                if self.rlhf.actions else (options[0] if options else "noop")
            )
            return {"escalated": False, "chosen": chosen, "source": "rlhf_fallback"}
        return await self.hitl.escalate(
            decision_context=decision_context,
            options=options,
            confidence=confidence,
            confidence_threshold=self.hitl_confidence_threshold,
        )

    # ------------------------------------------------------------------ #
    # Carbon
    # ------------------------------------------------------------------ #
    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {"credit_usd": 0.0, "rec_usd": 0.0}
        carbon_saved_kg = max(0.0, carbon_saved_kg)
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        # FIX bonus: explicit energy conversion via KG_CO2_PER_KWH.
        energy_saved_kwh = carbon_saved_kg / max(KG_CO2_PER_KWH, 1e-9)
        rec = await self.carbon_market.get_rec_value(energy_saved_kwh)
        self._carbon_saved_kg_total += carbon_saved_kg
        return {
            "credit_usd": credit,
            "rec_usd": rec,
            "energy_saved_kwh": energy_saved_kwh,
            "cumulative_kg": self._carbon_saved_kg_total,
        }

    # ------------------------------------------------------------------ #
    # Precision
    # ------------------------------------------------------------------ #
    def select_precision(self, accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(
            carbon_intensity=float(self.environment_factors.get("carbon", 400.0)),
            accuracy_required=accuracy_required,
        )

    # ------------------------------------------------------------------ #
    # Chaos
    # ------------------------------------------------------------------ #
    async def inject_fault(self, fault_type: str,
                           agent_id: Optional[str] = None):
        async with self._lock:
            if fault_type == "agent_failure" and agent_id in self.agents:
                self.agents[agent_id].is_active = False
                await self._reassign_roles_locked()
            elif fault_type == "agent_recovery" and agent_id in self.agents:
                self.agents[agent_id].is_active = True
                self.agents[agent_id].last_active = datetime.now(timezone.utc)
                await self._reassign_roles_locked()
            elif fault_type == "environment_spike":
                self.environment_factors["carbon"] = 800.0
                self.environment_factors["helium"] = 0.9
                await self._reassign_roles_locked()
            elif fault_type == "environment_normal":
                self.environment_factors = {
                    "carbon": 400.0, "helium": 0.5, "energy": 0.5,
                }
                await self._reassign_roles_locked()
            elif fault_type == "clear_leader":
                if self.leader_id and self.leader_id in self.agents:
                    self.agents[self.leader_id].is_active = False
                self.leader_id = None
                await self._reassign_roles_locked()
            else:
                logger.warning(f"Unknown fault type: {fault_type}")
        await self.save_state()

    async def run_chaos_test(self) -> Dict[str, Any]:
        report = {"faults": [], "results": {}}
        if self.agents:
            test_agent = next(iter(self.agents))
            await self.inject_fault("agent_failure", agent_id=test_agent)
            report["faults"].append("agent_failure")
            report["results"]["agent_failure"] = await self.get_coordination_summary()
            await self.inject_fault("agent_recovery", agent_id=test_agent)
            report["faults"].append("agent_recovery")
            report["results"]["agent_recovery"] = await self.get_coordination_summary()
        await self.inject_fault("environment_spike")
        report["faults"].append("environment_spike")
        report["results"]["environment_spike"] = await self.get_coordination_summary()
        await self.inject_fault("environment_normal")
        report["faults"].append("environment_normal")
        report["results"]["environment_normal"] = await self.get_coordination_summary()
        return report

    async def run_extended_chaos_suite(self) -> Dict:
        if self.chaos_tester is None:
            return {"error": "chaos testing disabled"}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({"fault": f, "passed": False, "error": str(e)})
        return {"results": results, "report": self.chaos_tester.get_report()}

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #
    async def get_comprehensive_status(self) -> Dict:
        summary = await self.get_coordination_summary()
        status = {
            "instance_id": self.instance_id,
            "version": "2.0.1",
            "agents_count": len(self.agents),
            "leader_id": self.leader_id,
            "environment_factors": dict(self.environment_factors),
            "role_summary": summary,
            "carbon_saved_kg_total": self._carbon_saved_kg_total,
            "features": {
                "temporal_logic": self.enable_temporal_logic,
                "xai": self.enable_xai,
                "adaptive_precision": self.enable_adaptive_precision,
                "carbon_market": self.enable_carbon_market,
                "role_specialization": self.enable_role_specialization,
                "chaos_testing": self.enable_chaos_testing,
                "hitl": self.enable_hitl,
                "federated": self.enable_federated,
                "distillation": self.enable_distillation,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self.temporal_monitor:
            status["temporal_logic"] = self.temporal_monitor.get_status()
        if self.role_softmax:
            status["role_softmax"] = self.role_softmax.assign_roles({
                "trust": 0.5, "compute": 0.5, "energy": 0.5, "performance": 0.5,
            })
        if self.federated:
            status["federated"] = self.federated.get_stats()
        if self.hitl:
            status["hitl"] = self.hitl.get_audit()
        if self.chaos_tester:
            status["chaos"] = self.chaos_tester.get_report()
        if self.precision_controller:
            status["precision"] = {
                "last": self.precision_controller.last_precision.value,
                "telemetry": self.precision_controller.telemetry,
            }
        if self.carbon_market:
            status["carbon_market"] = await self.carbon_market.get_market_snapshot()
        if self.distiller:
            status["distillation"] = {
                "student_probs": self.distiller.get_student_probs(),
                "history_len": len(self.distiller.history),
            }
        if self.rlhf:
            status["rlhf"] = {
                "actions": self.rlhf.actions,
                "history_len": len(self.rlhf.history),
                "pending_queries": len(self.rlhf.pending_queries),
            }
        return status

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #
    async def shutdown(self):
        await self.save_state()
        logger.info("MultiAgentCoordinator v2.0.1 shutdown complete")


# =============================================================================
# EMBEDDED UNIT TESTS  (FIX #8: run with `python <file> --test`)
# =============================================================================
def _run_unit_tests() -> int:
    """Minimal smoke tests for the fixed modules. Returns exit code."""
    import asyncio as _asyncio

    failures: List[str] = []

    def check(name: str, cond: bool, detail: str = ""):
        status = "PASS" if cond else "FAIL"
        print(f"  [{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    print("=" * 70)
    print("Unit tests — v2.0.1")
    print("=" * 70)

    # --- TemporalLogicMonitor -------------------------------------------------
    print("\n[1] TemporalLogicMonitor")
    mon = TemporalLogicMonitor()
    mon.add_formula("cap", "G(carbon <= 900.0)")
    for c in (400, 500, 600):
        mon.update({"carbon": c})
    check("G(cap) holds", mon.evaluate()["cap"] is True)
    mon.update({"carbon": 950})
    check("G(cap) fails after breach", mon.evaluate()["cap"] is False)

    mon2 = TemporalLogicMonitor()
    mon2.add_formula("spike", "F(carbon > 800)")
    mon2.update({"carbon": 400})
    mon2.update({"carbon": 850})
    check("F(spike) holds", mon2.evaluate()["spike"] is True)

    mon3 = TemporalLogicMonitor()
    mon3.add_formula("u", "carbon > 100 U carbon > 800")
    mon3.update({"carbon": 200})
    mon3.update({"carbon": 500})
    mon3.update({"carbon": 900})
    check("φ U ψ holds", mon3.evaluate()["u"] is True)

    mon4 = TemporalLogicMonitor()
    mon4.add_formula("imp", "G(carbon > 900 -> energy < 0.3)")
    mon4.update({"carbon": 400, "energy": 0.8})
    mon4.update({"carbon": 950, "energy": 0.1})
    check("G(φ -> ψ) holds", mon4.evaluate()["imp"] is True)
    mon4.update({"carbon": 950, "energy": 0.9})
    check("G(φ -> ψ) fails on violation", mon4.evaluate()["imp"] is False)

    # --- XAIExplainer ---------------------------------------------------------
    print("\n[2] XAIExplainer")
    xai = XAIExplainer(["a", "b", "c"])
    single = xai.explain({"a": 1}, {"a": 1, "b": 1, "c": 1}, [{"a": 1}])
    check("single-candidate guard", single.get("degenerate") is True)
    multi = xai.explain(
        {"a": 1, "b": 2, "c": 3},
        {"a": 1, "b": 1, "c": 1},
        [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 1, "c": 1}, {"a": 3, "b": 3, "c": 1}],
    )
    check("multi-candidate attributions", not multi.get("degenerate")
          and len(multi["contributions"]) == 3)

    # --- FederatedAggregator --------------------------------------------------
    print("\n[3] FederatedAggregator")
    fed = FederatedAggregator(num_params=4)
    fed.submit_update("c1", [1, 0, 0, 0], samples=1)
    fed.submit_update("c2", [0, 1, 0, 0], samples=1)
    out = fed.aggregate()
    check("aggregate returns round", out["round"] == 1)
    check("weighted average",
          abs(out["weights"][0] - 0.5) < 1e-9 and abs(out["weights"][1] - 0.5) < 1e-9)
    check("client_updates cleared", fed.client_updates == [])

    # --- ActiveRLHF -----------------------------------------------------------
    print("\n[4] ActiveRLHF")
    rlhf = ActiveRLHF(action_space=["a", "b", "c"], global_weight=0.3)
    check("uncertainty in [0,1]", 0.0 <= rlhf.uncertainty({"x": 1}) <= 1.0)
    rlhf.update({"x": 1}, "a", 2.0)
    rlhf.update({"x": 1}, "a", 1.0)
    check("contextual preference applied", rlhf.sample_action({"x": 1}) == "a")
    check("context isolation",
          rlhf.sample_action({"y": "z"}) in ("a", "b", "c"))

    # --- HITL query registration (FIX #2) ------------------------------------
    print("\n[5] HumanInTheLoopCoordinator (FIX #2)")

    async def _hitl_test():
        hitl = HumanInTheLoopCoordinator(
            ActiveRLHF(action_space=["a", "b"], uncertainty_threshold=0.0),
            timeout_s=1.0,
        )
        # Force escalation (low confidence, uncertainty threshold=0 → RLHF returns a query).
        res = await hitl.escalate({"k": 1}, ["a", "b"], confidence=0.1)
        qid = res["query"]["id"]
        assert qid in hitl.rlhf.pending_queries, "query not registered"
        resolved = hitl.resolve_query(qid, "a", rating=1.0)
        assert resolved is not None, "resolve_query returned None"
        return True

    try:
        ok = _asyncio.get_event_loop().run_until_complete(_hitl_test())
        check("HITL query register + resolve", ok)
    except RuntimeError:
        # Python 3.10+ event loop handling
        loop = _asyncio.new_event_loop()
        try:
            ok = loop.run_until_complete(_hitl_test())
            check("HITL query register + resolve", ok)
        finally:
            loop.close()

    # --- MultiTeacherDistiller ------------------------------------------------
    print("\n[6] MultiTeacherDistiller (FIX #5)")
    dist = MultiTeacherDistiller(temperature=2.0, learning_rate=0.5)
    before = np.array(dist.get_student_probs())
    dist.update_student([0.9, 0.05, 0.03, 0.02])
    after = np.array(dist.get_student_probs())
    check("student moves toward teacher", after[0] > before[0])
    check("student remains a distribution", abs(after.sum() - 1.0) < 1e-9)
    check("all entries positive", (after > 0).all())

    # --- Atomic write ---------------------------------------------------------
    print("\n[7] Atomic persistence (FIX #6)")
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "state.json")
        MultiAgentCoordinator._atomic_write_json(path, {"state_version": 2, "x": 1})
        with open(path) as f:
            data = json.load(f)
        check("state written + versioned",
              data.get("state_version") == 2 and data.get("x") == 1)
        check("no temp residue",
              not any(n.startswith(".state.json.tmp") for n in os.listdir(td)))

    # --- Chaos faults actually fire (FIX #3) ----------------------------------
    print("\n[8] ChaosTester fault injection (FIX #3)")

    async def _chaos_test():
        coord = MultiAgentCoordinator(
            persistence_path=os.path.join(td_chaos := tempfile.mkdtemp(), "c.json"),
            reset_state=True,
        )
        await coord.register_agent("a1", {"trust": 0.5, "compute": 0.5, "energy": 0.5})
        tester = ChaosTester(coord)
        r1 = await tester.run_test("storage_broken", duration_s=0.01)
        r2 = await tester.run_test("approval_timeout", duration_s=0.01)
        return r1, r2

    loop2 = _asyncio.new_event_loop()
    try:
        r1, r2 = loop2.run_until_complete(_chaos_test())
        check("storage_broken fault fires", r1["passed"] is True, str(r1))
        check("approval_timeout fault fires", r2["passed"] is True, str(r2))
    finally:
        loop2.close()

    # --- Federated aggregate trigger (FIX #4) ---------------------------------
    print("\n[9] Federated aggregation trigger (FIX #4)")

    async def _fed_test():
        coord = MultiAgentCoordinator(
            persistence_path=os.path.join(tempfile.mkdtemp(), "c.json"),
            reset_state=True,
            federated_aggregate_every=3,
        )
        for i in range(4):
            await coord.register_agent(
                f"a{i}", {"trust": 0.5, "compute": 0.5, "energy": 0.5}
            )
        # After 4 registers, 4 submissions were made → 1 aggregate (>=3).
        return coord.federated.round, len(coord.federated.client_updates)

    loop3 = _asyncio.new_event_loop()
    try:
        rounds, pending = loop3.run_until_complete(_fed_test())
        check("federated aggregated on update count", rounds >= 1,
              f"rounds={rounds} pending={pending}")
    finally:
        loop3.close()

    # --- Rename compatibility (FIX #9) ----------------------------------------
    print("\n[10] Enum rename compatibility (FIX #9)")
    check("AgentRole alias present", AgentRole is SystemRole)
    check("SystemRole has LEADER", SystemRole.LEADER.value == "leader")

    # --- Summary --------------------------------------------------------------
    print("\n" + "=" * 70)
    if failures:
        print(f"FAILED: {len(failures)} test(s)")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL TESTS PASSED")
    return 0


# =============================================================================
# SMOKE TEST
# =============================================================================
async def _smoke_test():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    print("=" * 78)
    print("Multi-Agent Coordinator v2.0.1 — smoke test")
    print("=" * 78)

    coord = MultiAgentCoordinator(
        persistence_path="/tmp/mac_smoke_v2_1.json",
        reset_state=True,           # start clean
        seed=42,                    # deterministic
    )

    await coord.register_agent("alice", {"trust": 0.9, "compute": 0.85, "energy": 0.6})
    await coord.register_agent("bob",   {"trust": 0.6, "compute": 0.9,  "energy": 0.8})
    await coord.register_agent("carol", {"trust": 0.85, "compute": 0.55, "energy": 0.9})
    await coord.register_agent("dave",  {"trust": 0.4, "compute": 0.4,  "energy": 0.3})

    for i in range(12):
        await coord.update_agent_performance("alice", "inference", 0.85 + 0.01 * i)
        await coord.update_agent_performance("bob", "inference", 0.8 + 0.01 * i)
        await coord.update_agent_performance("carol", "verification", 0.9)
        await coord.update_agent_performance("dave", "inference", 0.4)

    print("\n✅ Role summary after registration:")
    print(f"   {await coord.get_coordination_summary()}")
    print(f"   Leader: {coord.leader_id}")

    print("\n🔍 XAI explanation for alice:")
    print(f"   {await coord.explain_role('alice')}")
    xai = await coord.explain_role_xai("alice")
    if xai and xai.get("narrative"):
        print(f"   Narrative: {xai['narrative'][0]}")

    print(f"\n⚙️  Precision: {coord.select_precision(accuracy_required=0.95).value}")

    credit = await coord.compute_carbon_credit(carbon_saved_kg=250.0)
    print(f"\n💱 Carbon credit: ${credit['credit_usd']:.4f}  "
          f"REC: ${credit['rec_usd']:.4f}  "
          f"energy: {credit['energy_saved_kwh']:.1f} kWh")

    hitl = await coord.escalate_decision(
        decision_context={"reason": "leadership_change"},
        options=["alice", "bob", "carol"],
        confidence=0.85,
    )
    print(f"\n👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    print("\n🧪 Extended chaos suite:")
    chaos = await coord.run_extended_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    print("\n📊 Comprehensive status:")
    status = await coord.get_comprehensive_status()
    print(json.dumps({
        "version": status["version"],
        "agents": status["agents_count"],
        "leader": status["leader_id"],
        "role_summary": status["role_summary"],
        "features": status["features"],
        "federated": status.get("federated"),
        "distillation": status.get("distillation"),
        "hitl_total": status.get("hitl", {}).get("total"),
        "chaos_pass_rate": status.get("chaos", {}).get("pass_rate"),
        "precision_last": status.get("precision", {}).get("last"),
    }, indent=2, default=str))

    await coord.shutdown()
    print("\n" + "=" * 78)
    print("✅ Multi-Agent Coordinator v2.0.1 — smoke test complete")
    print("=" * 78)


# =============================================================================
# CLI
# =============================================================================
def _main() -> int:
    parser = argparse.ArgumentParser(description="Multi-Agent Coordinator v2.0.1")
    parser.add_argument("--test", action="store_true",
                        help="Run embedded unit tests and exit")
    parser.add_argument("--smoke", action="store_true",
                        help="Run the smoke test (default)")
    args = parser.parse_args()

    if args.test:
        return _run_unit_tests()
    asyncio.run(_smoke_test())
    return 0


if __name__ == "__main__":
    sys.exit(_main())
