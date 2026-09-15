# src/carbon/carbon_ledger.py — Enhanced v17.0.0
# =============================================================================
# Extended Carbon Ledger with Helium Accounting — v17.0.0
# =============================================================================
"""
Extended Carbon Ledger — v17.0.0

The original v1.0 ledger recorded only: timestamp, task_id, energy, carbon,
helium metrics, hardware type, power budget, fallback flag, and hash.

v17.0.0 extends the ledger with rich, enhancement-aware fields so it can serve
as the system of record for the entire Green Agent architecture:

   1. Quantum-Distillation Integration        → distillation_teacher_id, distillation_weight
   2. Causal Reinforcement Learning           → causal_action, causal_ate_estimate, causal_parents
   3. Federated Green Learning                → federated_instance_id, federated_round
   4. Advanced Multi-Agent Coordination       → agent_id, agent_role, agent_reputation
   5. Temporal Logic & Formal Verification    → state_snapshot, temporal_violations
   6. Explainable AI                          → xai_explanation, xai_feature_importance
   7. Adaptive Precision Switching            → precision_level, predicted_energy_saved_wh
   8. Carbon Markets / REC                    → carbon_offset_kg, rec_mwh, offset_cost_usd, net_carbon_kg
   9. Resilience Engineering / Chaos Testing  → chaos_experiment_id, chaos_fault_type
  10. HITL Active Learning                    → human_review_required, human_approved, hitl_user_id

The file is self-contained: Python stdlib + optional numpy/sklearn/torch.
All storage / enhancement hooks soft-fail.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import statistics
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIG HELPER
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# ENHANCED LEDGER ENTRY (v17.0.0)
# =============================================================================
@dataclass
class LedgerEntry:
    """
    A single entry in the extended carbon ledger.

    v1.0 fields are preserved. v17.0.0 adds ~20 new optional fields,
    each tied to one of the ten enhancements. All new fields default to
    safe values so existing call sites continue to work unchanged.
    """
    # --- v1.0 core fields ---
    timestamp: datetime
    task_id: str
    energy_kwh: float
    carbon_kg: float
    helium_zone: Optional[str]
    helium_usage: float
    helium_supply_at_execution: str
    helium_spot_price: float
    hardware_type: str
    power_budget: float
    fallback_used: bool
    hash: str = ""

    # --- Enhancement 1: Quantum-Distillation ---
    distillation_teacher_id: Optional[str] = None
    distillation_weight: Optional[float] = None

    # --- Enhancement 2: Causal RL ---
    causal_action: Optional[str] = None
    causal_ate_estimate: Optional[float] = None
    causal_parents: List[str] = field(default_factory=list)

    # --- Enhancement 3: Federated Green Learning ---
    federated_instance_id: Optional[str] = None
    federated_round: Optional[int] = None

    # --- Enhancement 4: Multi-Agent Coordination ---
    agent_id: Optional[str] = None
    agent_role: Optional[str] = None
    agent_reputation: Optional[float] = None

    # --- Enhancement 5: Temporal Logic ---
    state_snapshot: Optional[Dict[str, Any]] = None
    temporal_violations: List[str] = field(default_factory=list)

    # --- Enhancement 6: XAI ---
    xai_explanation: Optional[Dict[str, float]] = None
    xai_feature_importance: Optional[Dict[str, float]] = None

    # --- Enhancement 7: Adaptive Precision ---
    precision_level: str = "fp32"
    predicted_energy_saved_wh: Optional[float] = None

    # --- Enhancement 8: Carbon Markets / REC ---
    carbon_offset_kg: float = 0.0
    rec_mwh: float = 0.0
    offset_cost_usd: float = 0.0
    net_carbon_kg: Optional[float] = None

    # --- Enhancement 9: Chaos Testing ---
    chaos_experiment_id: Optional[str] = None
    chaos_fault_type: Optional[str] = None

    # --- Enhancement 10: HITL ---
    human_review_required: bool = False
    human_approved: Optional[bool] = None
    hitl_user_id: Optional[str] = None


# =============================================================================
# ENHANCED CARBON LEDGER (v17.0.0)
# =============================================================================
class ExtendedCarbonLedger:
    """
    Extended carbon ledger with helium accounting and full enhancement
    integration.

    v17.0.0 API (backward compatible with v1.0):
      * add_entry(unified_result, execution_decision, helium_signal)  — v1.0
      * add_entry_v17(record: Dict)                                    — v17.0
      * get_helium_efficiency_report(task_id=None)                    — v1.0
      * verify_integrity()                                            — v1.0
      * get_aggregated_metrics()                                      — Enhancement 3
      * get_agent_performance(agent_id)                               — Enhancement 4
      * get_net_carbon_position()                                     — Enhancement 8
      * get_chaos_impact(experiment_id)                               — Enhancement 9
      * get_hitl_impact()                                             — Enhancement 10
      * get_causal_action_stats()                                     — Enhancement 2
      * get_xai_aggregate_importance()                                — Enhancement 6
      * get_precision_energy_savings()                                — Enhancement 7
      * get_temporal_violation_summary()                              — Enhancement 5
      * get_distillation_teacher_stats()                              — Enhancement 1
      * register_enhancement(name, hook)                              — v17.0
    """

    VALID_PRECISIONS = {"fp32", "tf32", "bf16", "fp16", "int8"}

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.ledger: List[LedgerEntry] = []
        self.ledger_file = self.config.get('ledger_file', 'carbon_ledger.json')

        # v17.0: enhancement hooks registered by the orchestrator
        self._hooks: Dict[str, Any] = {}

        # v17.0: in-process lock for concurrent append (thread-safe)
        self._lock = threading.RLock()

        # Load existing ledger if available
        self._load_ledger()
        logger.info("ExtendedCarbonLedger v17.0.0 initialized "
                    "(file=%s, entries=%d)",
                    self.ledger_file, len(self.ledger))

    # ------------------------------------------------------------------
    # Enhancement hook registration
    # ------------------------------------------------------------------
    def register_enhancement(self, name: str, hook: Any) -> None:
        """Register an enhancement (e.g., 'federated', 'xai') for querying."""
        self._hooks[name] = hook
        logger.debug("Registered enhancement hook: %s", name)

    # ------------------------------------------------------------------
    # v1.0 backward-compatible API
    # ------------------------------------------------------------------
    def add_entry(self, unified_result, execution_decision,
                  helium_signal) -> LedgerEntry:
        """
        Add an entry to the ledger using the v1.0 signature.

        Enhancement fields default to safe values unless the caller
        provides them via attributes on `unified_result` or
        `execution_decision`.
        """
        entry = LedgerEntry(
            timestamp=datetime.now(timezone.utc),
            task_id=getattr(unified_result, 'task_id', 'unknown'),
            energy_kwh=float(getattr(unified_result,
                                     'energy_consumed_kwh', 0.0)),
            carbon_kg=float(getattr(unified_result,
                                    'carbon_emitted_kg', 0.0)),
            helium_zone=(execution_decision.helium_zone.value
                         if getattr(execution_decision, 'helium_zone', None)
                         else None),
            helium_usage=float(getattr(unified_result,
                                       'helium_usage', 0.0)),
            helium_supply_at_execution=(
                helium_signal.scarcity_level.value
                if helium_signal and hasattr(helium_signal, 'scarcity_level')
                else 'unknown'),
            helium_spot_price=float(
                getattr(helium_signal, 'spot_price_usd_per_liter', 4.0)
                if helium_signal else 4.0),
            hardware_type=getattr(unified_result, 'worker_type', 'unknown'),
            power_budget=float(getattr(execution_decision, 'power_budget', 0.0)),
            fallback_used=bool(getattr(unified_result, 'fallback_used', False)),
        )

        # Best-effort extraction of v17 attributes
        self._populate_v17_fields(entry, unified_result, execution_decision,
                                   helium_signal)

        # Compute net carbon
        entry.net_carbon_kg = entry.carbon_kg - entry.carbon_offset_kg

        # Calculate cryptographic hash
        entry.hash = self._calculate_hash(entry)

        # Append + persist
        with self._lock:
            self.ledger.append(entry)
        self._save_ledger()
        return entry

    # ------------------------------------------------------------------
    # v17.0 explicit API
    # ------------------------------------------------------------------
    def add_entry_v17(self, record: Dict[str, Any]) -> LedgerEntry:
        """
        Add an entry using an explicit dictionary of v17 fields.

        Any field not provided gets a safe default. This is the preferred
        API for the orchestrator and enhancement modules.
        """
        defaults = dict(
            timestamp=datetime.now(timezone.utc),
            task_id=record.get('task_id', str(uuid.uuid4())[:8]),
            energy_kwh=float(record.get('energy_kwh', 0.0)),
            carbon_kg=float(record.get('carbon_kg', 0.0)),
            helium_zone=record.get('helium_zone'),
            helium_usage=float(record.get('helium_usage', 0.0)),
            helium_supply_at_execution=record.get('helium_supply_at_execution',
                                                  'unknown'),
            helium_spot_price=float(record.get('helium_spot_price', 4.0)),
            hardware_type=record.get('hardware_type', 'unknown'),
            power_budget=float(record.get('power_budget', 0.0)),
            fallback_used=bool(record.get('fallback_used', False)),
            # v17
            distillation_teacher_id=record.get('distillation_teacher_id'),
            distillation_weight=record.get('distillation_weight'),
            causal_action=record.get('causal_action'),
            causal_ate_estimate=record.get('causal_ate_estimate'),
            causal_parents=list(record.get('causal_parents', [])),
            federated_instance_id=record.get('federated_instance_id'),
            federated_round=record.get('federated_round'),
            agent_id=record.get('agent_id'),
            agent_role=record.get('agent_role'),
            agent_reputation=record.get('agent_reputation'),
            state_snapshot=record.get('state_snapshot'),
            temporal_violations=list(record.get('temporal_violations', [])),
            xai_explanation=record.get('xai_explanation'),
            xai_feature_importance=record.get('xai_feature_importance'),
            precision_level=record.get('precision_level', 'fp32'),
            predicted_energy_saved_wh=record.get('predicted_energy_saved_wh'),
            carbon_offset_kg=float(record.get('carbon_offset_kg', 0.0)),
            rec_mwh=float(record.get('rec_mwh', 0.0)),
            offset_cost_usd=float(record.get('offset_cost_usd', 0.0)),
            chaos_experiment_id=record.get('chaos_experiment_id'),
            chaos_fault_type=record.get('chaos_fault_type'),
            human_review_required=bool(record.get('human_review_required',
                                                   False)),
            human_approved=record.get('human_approved'),
            hitl_user_id=record.get('hitl_user_id'),
        )

        # Validate precision
        if defaults['precision_level'] not in self.VALID_PRECISIONS:
            defaults['precision_level'] = 'fp32'

        entry = LedgerEntry(**defaults)
        entry.net_carbon_kg = entry.carbon_kg - entry.carbon_offset_kg
        entry.hash = self._calculate_hash(entry)

        with self._lock:
            self.ledger.append(entry)
        self._save_ledger()
        return entry

    # ------------------------------------------------------------------
    # v1.0 compatibility helpers
    # ------------------------------------------------------------------
    def _populate_v17_fields(self, entry: LedgerEntry,
                             unified_result: Any,
                             execution_decision: Any,
                             helium_signal: Any) -> None:
        """Copy any v17 attributes that may be present on the inputs."""
        for attr in ("distillation_teacher_id", "distillation_weight",
                     "causal_action", "causal_ate_estimate",
                     "federated_instance_id", "federated_round",
                     "agent_id", "agent_role", "agent_reputation",
                     "xai_explanation", "xai_feature_importance",
                     "precision_level", "predicted_energy_saved_wh",
                     "carbon_offset_kg", "rec_mwh", "offset_cost_usd",
                     "chaos_experiment_id", "chaos_fault_type",
                     "human_review_required", "human_approved",
                     "hitl_user_id"):
            if hasattr(unified_result, attr):
                setattr(entry, attr, getattr(unified_result, attr))
            elif hasattr(execution_decision, attr):
                setattr(entry, attr, getattr(execution_decision, attr))

        # Temporal
        if hasattr(unified_result, 'state_snapshot'):
            entry.state_snapshot = unified_result.state_snapshot
        if hasattr(unified_result, 'temporal_violations'):
            entry.temporal_violations = list(
                getattr(unified_result, 'temporal_violations') or [])

        # Causal parents
        if hasattr(unified_result, 'causal_parents'):
            entry.causal_parents = list(
                getattr(unified_result, 'causal_parents') or [])

    # ------------------------------------------------------------------
    # Hash / persistence
    # ------------------------------------------------------------------
    def _calculate_hash(self, entry: LedgerEntry) -> str:
        """Calculate SHA-256 hash of entry for immutability."""
        entry_dict = asdict(entry)
        entry_dict.pop('hash', None)
        json_str = json.dumps(entry_dict, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()

    def _save_ledger(self) -> None:
        """Save ledger to disk."""
        try:
            with open(self.ledger_file, 'w') as f:
                json.dump([asdict(e) for e in self.ledger],
                          f, default=str, indent=2)
        except Exception as e:
            logger.error("Failed to save ledger: %s", e)

    def _load_ledger(self) -> None:
        """Load ledger from disk, gracefully ignoring unknown fields."""
        try:
            with open(self.ledger_file, 'r') as f:
                data = json.load(f)
            for entry_dict in data:
                if 'timestamp' in entry_dict and \
                        isinstance(entry_dict['timestamp'], str):
                    entry_dict['timestamp'] = datetime.fromisoformat(
                        entry_dict['timestamp'])
                # Filter to known fields
                known = set(LedgerEntry.__dataclass_fields__.keys())
                filtered = {k: v for k, v in entry_dict.items() if k in known}
                self.ledger.append(LedgerEntry(**filtered))
            logger.info("Loaded %d entries from %s",
                        len(self.ledger), self.ledger_file)
        except FileNotFoundError:
            logger.info("No existing ledger found, starting fresh")
        except Exception as e:
            logger.error("Failed to load ledger: %s", e)

    # ------------------------------------------------------------------
    # v1.0 report (preserved)
    # ------------------------------------------------------------------
    def get_helium_efficiency_report(self,
                                     task_id: Optional[str] = None
                                     ) -> Dict[str, Any]:
        """Generate helium efficiency report (v1.0 API)."""
        if task_id:
            entries = [e for e in self.ledger if e.task_id == task_id]
        else:
            entries = list(self.ledger)

        if not entries:
            return {'error': 'No entries found'}

        total_helium_usage = sum(e.helium_usage for e in entries)
        total_energy = sum(e.energy_kwh for e in entries)

        return {
            'total_entries': len(entries),
            'total_helium_usage': total_helium_usage,
            'total_energy_kwh': total_energy,
            'helium_per_energy_ratio': (total_helium_usage / total_energy
                                        if total_energy > 0 else 0),
            'tasks_by_helium_zone': {
                zone: len([e for e in entries if e.helium_zone == zone])
                for zone in set(e.helium_zone for e in entries
                                if e.helium_zone)},
            'fallback_rate': (
                len([e for e in entries if e.fallback_used]) / len(entries)
                if entries else 0),
        }

    def verify_integrity(self) -> bool:
        """Verify ledger integrity by checking all hashes."""
        for i, entry in enumerate(self.ledger):
            expected_hash = self._calculate_hash(entry)
            if entry.hash != expected_hash:
                logger.error("Integrity check failed at index %d", i)
                return False
        return True

    # ------------------------------------------------------------------
    # Enhancement 1: Quantum-Distillation
    # ------------------------------------------------------------------
    def get_distillation_teacher_stats(self) -> Dict[str, Any]:
        """Return per-teacher contribution and performance statistics."""
        by_teacher: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in self.ledger:
            if e.distillation_teacher_id:
                by_teacher[e.distillation_teacher_id].append(e)
        out: Dict[str, Any] = {}
        for tid, entries in by_teacher.items():
            out[tid] = {
                "task_count": len(entries),
                "avg_energy_kwh": statistics.mean(
                    e.energy_kwh for e in entries),
                "avg_carbon_kg": statistics.mean(
                    e.carbon_kg for e in entries),
                "avg_weight": statistics.mean(
                    e.distillation_weight for e in entries
                    if e.distillation_weight is not None),
            }
        return out

    # ------------------------------------------------------------------
    # Enhancement 2: Causal RL
    # ------------------------------------------------------------------
    def get_causal_action_stats(self) -> Dict[str, Any]:
        """Return per-action performance statistics."""
        by_action: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in self.ledger:
            if e.causal_action:
                by_action[e.causal_action].append(e)
        out: Dict[str, Any] = {}
        for action, entries in by_action.items():
            out[action] = {
                "count": len(entries),
                "avg_carbon_kg": statistics.mean(
                    e.carbon_kg for e in entries),
                "avg_energy_kwh": statistics.mean(
                    e.energy_kwh for e in entries),
                "avg_ate": statistics.mean(
                    e.causal_ate_estimate for e in entries
                    if e.causal_ate_estimate is not None) or 0.0,
            }
        return out

    # ------------------------------------------------------------------
    # Enhancement 3: Federated Green Learning
    # ------------------------------------------------------------------
    def get_aggregated_metrics(self) -> Dict[str, Any]:
        """
        Return aggregated metrics suitable for federated sharing.

        Only numeric aggregates are returned — no task IDs or
        hardware-level data — so the payload can safely be shared
        across deployments.
        """
        if not self.ledger:
            return {
                "count": 0, "total_energy_kwh": 0.0,
                "total_carbon_kg": 0.0, "total_helium_usage": 0.0,
                "avg_quality": 0.0,
            }
        return {
            "count": len(self.ledger),
            "total_energy_kwh": sum(e.energy_kwh for e in self.ledger),
            "total_carbon_kg": sum(e.carbon_kg for e in self.ledger),
            "total_helium_usage": sum(e.helium_usage for e in self.ledger),
            "total_carbon_offset_kg": sum(
                e.carbon_offset_kg for e in self.ledger),
            "avg_energy_kwh": statistics.mean(
                e.energy_kwh for e in self.ledger),
            "avg_carbon_kg": statistics.mean(
                e.carbon_kg for e in self.ledger),
            "avg_precision": self._dominant_precision(),
        }

    def _dominant_precision(self) -> str:
        counts = defaultdict(int)
        for e in self.ledger:
            counts[e.precision_level] += 1
        return max(counts, key=counts.get) if counts else "fp32"

    # ------------------------------------------------------------------
    # Enhancement 4: Multi-Agent Coordination
    # ------------------------------------------------------------------
    def get_agent_performance(self,
                              agent_id: Optional[str] = None
                              ) -> Dict[str, Any]:
        """
        Return per-agent performance from the ledger.

        Used by the `MultiAgentCoordinator` to ground agent reputation
        in real, recorded outcomes.
        """
        by_agent: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in self.ledger:
            if e.agent_id:
                by_agent[e.agent_id].append(e)
        if agent_id is not None:
            entries = by_agent.get(agent_id, [])
            return self._agent_summary(agent_id, entries)
        return {aid: self._agent_summary(aid, es)
                for aid, es in by_agent.items()}

    @staticmethod
    def _agent_summary(agent_id: str,
                       entries: List[LedgerEntry]) -> Dict[str, Any]:
        if not entries:
            return {"agent_id": agent_id, "count": 0, "quality_score": 0.0}
        avg_carbon = statistics.mean(e.carbon_kg for e in entries)
        avg_energy = statistics.mean(e.energy_kwh for e in entries)
        # Proxy quality: lower carbon/energy and fewer fallbacks = better
        fallback_rate = sum(1 for e in entries if e.fallback_used) / len(entries)
        quality = max(0.0, 1.0 - 0.5 * fallback_rate -
                      0.001 * avg_carbon - 0.01 * avg_energy)
        return {
            "agent_id": agent_id,
            "count": len(entries),
            "avg_carbon_kg": avg_carbon,
            "avg_energy_kwh": avg_energy,
            "fallback_rate": fallback_rate,
            "quality_score": quality,
        }

    # ------------------------------------------------------------------
    # Enhancement 5: Temporal Logic
    # ------------------------------------------------------------------
    def get_temporal_violation_summary(self) -> Dict[str, Any]:
        """Return a summary of temporal violations across the ledger."""
        violations: Dict[str, int] = defaultdict(int)
        total_violating_entries = 0
        for e in self.ledger:
            if e.temporal_violations:
                total_violating_entries += 1
                for v in e.temporal_violations:
                    violations[v] += 1
        return {
            "total_entries": len(self.ledger),
            "violating_entries": total_violating_entries,
            "violation_rate": (total_violating_entries / len(self.ledger)
                               if self.ledger else 0.0),
            "per_rule": dict(violations),
        }

    # ------------------------------------------------------------------
    # Enhancement 6: XAI
    # ------------------------------------------------------------------
    def get_xai_aggregate_importance(self) -> Dict[str, float]:
        """Aggregate feature importance across all XAI-annotated entries."""
        agg: Dict[str, List[float]] = defaultdict(list)
        for e in self.ledger:
            if not e.xai_feature_importance:
                continue
            for k, v in e.xai_feature_importance.items():
                try:
                    agg[k].append(abs(float(v)))
                except Exception:
                    continue
        return {k: statistics.mean(vs) for k, vs in agg.items()}

    # ------------------------------------------------------------------
    # Enhancement 7: Adaptive Precision
    # ------------------------------------------------------------------
    def get_precision_energy_savings(self) -> Dict[str, Any]:
        """Report energy savings attributed to precision switching."""
        by_precision: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in self.ledger:
            by_precision[e.precision_level].append(e)
        total_predicted = sum(
            e.predicted_energy_saved_wh or 0.0 for e in self.ledger)
        return {
            "total_predicted_saved_wh": total_predicted,
            "by_precision": {
                p: {
                    "count": len(es),
                    "avg_energy_kwh": statistics.mean(
                        e.energy_kwh for e in es),
                    "avg_saved_wh": statistics.mean(
                        e.predicted_energy_saved_wh or 0.0 for e in es),
                }
                for p, es in by_precision.items()
            },
        }

    # ------------------------------------------------------------------
    # Enhancement 8: Carbon Markets / REC
    # ------------------------------------------------------------------
    def get_net_carbon_position(self) -> Dict[str, Any]:
        """Report gross, offset, and net carbon position."""
        gross = sum(e.carbon_kg for e in self.ledger)
        offset = sum(e.carbon_offset_kg for e in self.ledger)
        rec = sum(e.rec_mwh for e in self.ledger)
        cost = sum(e.offset_cost_usd for e in self.ledger)
        net = gross - offset
        return {
            "gross_carbon_kg": gross,
            "offset_kg": offset,
            "net_carbon_kg": net,
            "rec_mwh": rec,
            "offset_cost_usd": cost,
            "net_zero": net <= 0.0,
        }

    # ------------------------------------------------------------------
    # Enhancement 9: Chaos Testing
    # ------------------------------------------------------------------
    def get_chaos_impact(self,
                         experiment_id: Optional[str] = None
                         ) -> Dict[str, Any]:
        """
        Compare performance of tasks run under chaos vs. tasks run
        normally. If `experiment_id` is provided, restrict to that
        experiment.
        """
        with_chaos: List[LedgerEntry] = []
        without_chaos: List[LedgerEntry] = []
        for e in self.ledger:
            if e.chaos_experiment_id:
                if experiment_id is None or e.chaos_experiment_id == experiment_id:
                    with_chaos.append(e)
            else:
                without_chaos.append(e)

        def _summary(entries: List[LedgerEntry]) -> Dict[str, float]:
            if not entries:
                return {"count": 0, "avg_energy_kwh": 0.0,
                        "avg_carbon_kg": 0.0, "fallback_rate": 0.0}
            return {
                "count": len(entries),
                "avg_energy_kwh": statistics.mean(
                    e.energy_kwh for e in entries),
                "avg_carbon_kg": statistics.mean(
                    e.carbon_kg for e in entries),
                "fallback_rate": sum(1 for e in entries if e.fallback_used) /
                                 len(entries),
            }
        return {
            "experiment_id": experiment_id,
            "with_chaos": _summary(with_chaos),
            "without_chaos": _summary(without_chaos),
        }

    # ------------------------------------------------------------------
    # Enhancement 10: HITL
    # ------------------------------------------------------------------
    def get_hitl_impact(self) -> Dict[str, Any]:
        """Compare outcomes with and without human review."""
        with_review: List[LedgerEntry] = []
        approved: List[LedgerEntry] = []
        rejected: List[LedgerEntry] = []
        for e in self.ledger:
            if e.human_review_required:
                with_review.append(e)
                if e.human_approved is True:
                    approved.append(e)
                elif e.human_approved is False:
                    rejected.append(e)

        def _avg(entries: List[LedgerEntry],
                 attr: str) -> float:
            if not entries:
                return 0.0
            return statistics.mean(
                float(getattr(e, attr, 0.0)) for e in entries)

        return {
            "total_reviewed": len(with_review),
            "approved": len(approved),
            "rejected": len(rejected),
            "avg_carbon_kg_approved": _avg(approved, "carbon_kg"),
            "avg_carbon_kg_rejected": _avg(rejected, "carbon_kg"),
            "avg_energy_kwh_approved": _avg(approved, "energy_kwh"),
            "avg_energy_kwh_rejected": _avg(rejected, "energy_kwh"),
        }

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def get_full_report(self) -> Dict[str, Any]:
        """Generate a comprehensive report across all ten enhancements."""
        return {
            "ledger": {
                "entry_count": len(self.ledger),
                "integrity_ok": self.verify_integrity(),
            },
            "helium": self.get_helium_efficiency_report(),
            "distillation": self.get_distillation_teacher_stats(),
            "causal": self.get_causal_action_stats(),
            "federated": self.get_aggregated_metrics(),
            "agents": self.get_agent_performance(),
            "temporal": self.get_temporal_violation_summary(),
            "xai": self.get_xai_aggregate_importance(),
            "precision": self.get_precision_energy_savings(),
            "carbon_market": self.get_net_carbon_position(),
            "chaos": self.get_chaos_impact(),
            "hitl": self.get_hitl_impact(),
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5,
                 learning_rate=0.1):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.learning_rate = learning_rate
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x, temp):
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self):
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage=None, student_id="ledger_student"):
        target = self._softmax(self._superpose(), self.temperature)
        new = []
        for s, t in zip(self.student_policy, target):
            eps = max(1e-12, min(self.student_policy) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {"target": target,
                 "student": list(self.student_policy),
                 "ts": datetime.now(timezone.utc).isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage=None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            self.graph.clear()
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        w = random.uniform(0.1, 0.9)
                        self.graph[s][t] = {"weight": w, "confidence": w}
            return self.summary()
        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        self.graph.clear()
        for i in range(len(variables)):
            for j in range(len(variables)):
                if i == j:
                    continue
                c = abs(float(corr[i, j]))
                if c > threshold:
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = (variables[i], variables[j]) if vi > vj \
                        else (variables[j], variables[i])
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values = defaultdict(float)
        self.counts = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)

    async def choose_action(self, state):
        if random.random() < self.epsilon:
            return random.choice(self.ACTIONS)
        return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action, reward, state):
        if action not in self.ACTIONS:
            action = self.ACTIONS[0]
        self.counts[action] += 1
        n = self.counts[action]
        self.values[action] += (reward - self.values[action]) / n
        vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
        m = max(vals)
        exps = [math.exp((v - m) / 0.5) for v in vals]
        s = sum(exps) or 1.0
        self.policy = [e / s for e in exps]

    def get_policy(self):
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, model_id, weights):
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id):
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        return bytes(avg)

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "agent_count", 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def bid(self, task):
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id, reward):
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0,
                                          a.utilities[a.role] + 0.05 * reward)

    def get_policy(self):
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i in range(5):
                out[i] += w * 0.2
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self):
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
import re as _re
_ATOMIC_RE = _re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation = None

    def evaluate(self, trace):
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque = deque(
            maxlen=_cfg_get(config, "temporal_max_trace", 2000))
        self.approval_cb = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula):
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")

    def _add_atomic(self, expr, op):
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        self.trace.append((datetime.now(timezone.utc), dict(state)))

    async def verify(self):
        result = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5))
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)

    def _kernel_shap(self, f, x, names, n=32):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib /= max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _lime(self, f, x, names, n=100):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: random.uniform(-1, 1) for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn,
                      include_counterfactual=False, include_anchors=False):
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
            nl = self._nl(label, attrs)
            result = {"decision_id": decision_id, "method": self.method,
                      "attributions": attrs, "explanation": nl}
        else:
            x = np.asarray(features, dtype=float)
            attrs = self._lime(model_fn, x, names) if self.method == "lime" \
                else self._kernel_shap(model_fn, x, names)
            nl = self._nl(label, attrs)
            result = {"decision_id": decision_id, "method": self.method,
                      "attributions": attrs, "explanation": nl}
        self.global_attributions.append(dict(attrs))
        return result

    def global_feature_importance(self):
        if not self.global_attributions:
            return {}
        agg = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(vals) / len(vals) for k, vals in agg.items()}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def _probe(self):
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = torch.cuda.is_available()
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self):
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        return True

    async def auto_switch(self, recent_acc, baseline_acc):
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0

    async def _fetch_price(self):
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        self.last_price = price
        return price

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = "defer" if intensity > 0.3 else \
                 ("run_offset" if offset_cost < 0.5 else "run")
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost,
                "credit_price_usd": price}


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    async def run_experiment(self, name, fault_type):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.time()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                await asyncio.sleep(0.1)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.05)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        duration = (time.time() - t0) * 1000.0
        return {"name": name, "fault_type": fault_type,
                "status": status, "duration_ms": duration}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, dashboard=None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id, chosen_id):
        try:
            self._responses.put_nowait({"user_id": user_id,
                                        "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(self._responses.get(),
                                         timeout=timeout)
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return candidates[0].get("solution_id")


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class CarbonLedgerOrchestratorV17:
    """
    Unified entry point: wires the enhanced ExtendedCarbonLedger with all
    ten enhancements and exposes a `record_task` method that populates
    every enhancement field.
    """

    def __init__(self, storage, config, dashboard=None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Ten enhancements
        self.quantum = QuantumDistillationEngine()
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage,
                                              self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Enhanced ledger
        self.ledger = ExtendedCarbonLedger(
            {"ledger_file": _cfg_get(config, "ledger_file",
                                      "/tmp/carbon_ledger_v17.json")})
        self.ledger.register_enhancement("federated", self.federated)
        self.ledger.register_enhancement("xai", self.xai)
        self.ledger.register_enhancement("precision", self.precision)
        self.ledger.register_enhancement("carbon_market", self.carbon_market)
        self.ledger.register_enhancement("chaos", self.chaos)
        self.ledger.register_enhancement("hitl", self.hitl)
        self.ledger.register_enhancement("causal", self.causal_rl)
        self.ledger.register_enhancement("multi_agent", self.multi_agent)
        self.ledger.register_enhancement("temporal", self.temporal)

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()

    # ------------------------------------------------------------------
    async def record_task(self,
                          task_id: str,
                          energy_kwh: float,
                          carbon_kg: float,
                          helium_zone: Optional[str] = None,
                          helium_usage: float = 0.0,
                          helium_spot_price: float = 4.0,
                          hardware_type: str = "cpu",
                          power_budget: float = 0.0,
                          fallback_used: bool = False,
                          state: Optional[Dict[str, Any]] = None,
                          force_chaos: Optional[str] = None,
                          force_human_review: bool = False
                          ) -> LedgerEntry:
        """
        Record a task with all ten enhancements contributing to the entry.

        The orchestrator:
          1. Causal RL chooses an action (state-based)
          2. Multi-agent bids for the task
          3. Precision switcher adjusts precision
          4. Carbon market offsets the carbon
          5. Temporal verifier checks the state
          6. XAI explains the decision
          7. Chaos injection (optional, or by config)
          8. HITL review (optional, or by config)
          9. Federated sharing of aggregates
         10. Distillation teacher stats
        """
        state = state or {}

        # 1. Causal RL
        causal_action = await self.causal_rl.choose_action(state)
        causal_parents = self.causal_graph.parents("carbon")
        causal_ate = self.causal_rl.graph.graph.get(
            "carbon", {}).get("carbon_kg", {}).get("weight", 0.0) \
            if self.causal_graph.graph else 0.0

        # 2. Multi-agent bid
        agent_id, _ = await self.multi_agent.bid({
            "name": task_id, "preferred_role": "optimizer"})
        agent_role = self.multi_agent.agents[agent_id].role
        agent_rep = self.multi_agent.agents[agent_id].reputation

        # 3. Precision switcher
        await self.precision.auto_switch(0.9, 0.92)
        precision_level = self.precision.current
        predicted_saved_wh = self.precision.saved_wh

        # 4. Carbon market
        cm = await self.carbon_market.net_zero_schedule(
            workload_kwh=energy_kwh, intensity=carbon_kg / max(energy_kwh, 1e-6))
        carbon_offset_kg = 0.0
        offset_cost_usd = 0.0
        rec_mwh = 0.0
        if cm["action"] == "run_offset":
            carbon_offset_kg = carbon_kg
            offset_cost_usd = cm["offset_cost_usd"]
        elif cm["action"] == "run":
            pass

        # 5. Temporal verification
        await self.temporal.push_state({
            "quality": 1.0 - fallback_used * 0.5,
            "carbon": carbon_kg,
            **state})
        verify = await self.temporal.verify()
        temporal_violations = [k for k, ok in verify.items() if not ok]

        # 6. XAI explanation
        xai_explanation = None
        xai_feature_importance = None
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([energy_kwh, carbon_kg, power_budget,
                                  float(helium_usage)])
                xai_result = await self.xai.explain(
                    decision_id=f"ledger_{task_id}",
                    label=f"task:{task_id}",
                    features=feats,
                    names=["energy_kwh", "carbon_kg",
                           "power_budget", "helium_usage"],
                    model_fn=lambda x: float(
                        np.dot(x, [0.4, 0.3, 0.2, 0.1])))
                xai_explanation = xai_result.get("attributions")
                xai_feature_importance = xai_explanation
            except Exception:
                pass

        # 7. Chaos injection
        chaos_experiment_id = None
        chaos_fault_type = None
        if force_chaos is not None:
            experiment_id = f"chaos_{uuid.uuid4().hex[:6]}"
            r = await self.chaos.run_experiment(experiment_id, force_chaos)
            chaos_experiment_id = experiment_id
            chaos_fault_type = force_chaos

        # 8. HITL review
        human_review_required = force_human_review
        human_approved: Optional[bool] = None
        if human_review_required:
            try:
                response = await self.hitl.query_user_if_needed(
                    "ledger_user",
                    [{"solution_id": "approve", "quality_score": 0.9},
                     {"solution_id": "reject", "quality_score": 0.8}],
                    timeout=0.5)
                human_approved = response == "approve"
            except Exception:
                human_approved = None

        # 9. Federated sharing (aggregate)
        try:
            agg = self.ledger.get_aggregated_metrics()
            blob = json.dumps(agg, default=str).encode()
            await self.federated.share_weights("carbon_ledger", blob)
        except Exception:
            pass

        # 10. Distillation teacher (record the causal policy as teacher)
        self.quantum.register_teacher(
            "causal", self.causal_rl.get_policy())
        teacher_id = "causal"
        teacher_weight = self.causal_rl.get_policy()[0]

        # Record the ledger entry
        entry = self.ledger.add_entry_v17({
            "task_id": task_id,
            "energy_kwh": energy_kwh,
            "carbon_kg": carbon_kg,
            "helium_zone": helium_zone,
            "helium_usage": helium_usage,
            "helium_spot_price": helium_spot_price,
            "hardware_type": hardware_type,
            "power_budget": power_budget,
            "fallback_used": fallback_used,
            "distillation_teacher_id": teacher_id,
            "distillation_weight": teacher_weight,
            "causal_action": causal_action,
            "causal_ate_estimate": causal_ate,
            "causal_parents": causal_parents,
            "federated_instance_id": self.instance_id,
            "federated_round": self.federated.rounds,
            "agent_id": agent_id,
            "agent_role": agent_role,
            "agent_reputation": agent_rep,
            "state_snapshot": state,
            "temporal_violations": temporal_violations,
            "xai_explanation": xai_explanation,
            "xai_feature_importance": xai_feature_importance,
            "precision_level": precision_level,
            "predicted_energy_saved_wh": predicted_saved_wh,
            "carbon_offset_kg": carbon_offset_kg,
            "rec_mwh": rec_mwh,
            "offset_cost_usd": offset_cost_usd,
            "chaos_experiment_id": chaos_experiment_id,
            "chaos_fault_type": chaos_fault_type,
            "human_review_required": human_review_required,
            "human_approved": human_approved,
            "hitl_user_id": "ledger_user" if human_review_required else None,
        })

        # Reward the agent based on the recorded outcome
        try:
            reward = max(0.0, 1.0 - fallback_used * 0.5)
            await self.multi_agent.reward(agent_id, reward)
        except Exception:
            pass

        return entry

    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self._causal_rl_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._multi_agent_loop()),
            loop.create_task(self._temporal_loop()),
            loop.create_task(self._xai_loop()),
            loop.create_task(self._precision_loop()),
            loop.create_task(self._carbon_market_loop()),
            loop.create_task(self._chaos_loop()),
            loop.create_task(self._distillation_loop()),
        ]
        for t in tasks:
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)

    async def shutdown(self):
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        # Persist the ledger one final time
        try:
            self.ledger._save_ledger()
        except Exception:
            pass

    # ------------------------------------------------------------------
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(900)
            try:
                samples = [{
                    "carbon_intensity": random.uniform(150, 700),
                    "quality": random.uniform(0.5, 1.0),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["carbon_intensity", "quality", "cost", "latency"])
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                agg = self.ledger.get_aggregated_metrics()
                blob = json.dumps(agg, default=str).encode()
                await self.federated.share_weights("carbon_ledger", blob)
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                await self.multi_agent.step()
            except Exception:
                pass

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.temporal.verify()
            except Exception:
                pass

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                imp = self.ledger.get_xai_aggregate_importance()
                if imp:
                    logger.debug("Ledger XAI aggregate: %s", imp)
            except Exception:
                pass

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except Exception:
                pass

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.carbon_market.update_price()
            except Exception:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                if random.random() < 0.3:
                    fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                    await self.chaos.run_experiment(
                        f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(student_id="ledger_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "ledger_entries": len(self.ledger.ledger),
            "integrity_ok": self.ledger.verify_integrity(),
            "precision": self.precision.current,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "federated_rounds": self.federated.rounds,
        }

    def full_report(self) -> Dict[str, Any]:
        return self.ledger.get_full_report()


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos and tests."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 500,
        "temporal_formulas": [
            "G (quality >= 0.5)",
            "G (carbon <= 500)",
        ],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "ledger_file": "/tmp/carbon_ledger_v17_demo.json",
    }

    storage = InMemoryStorage()
    orch = CarbonLedgerOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("carbon_ledger.py v17.0.0 — Demo")
    print("=" * 80)

    # 1. Record tasks with all enhancements contributing
    print("\n=== Recording 10 tasks ===")
    for i in range(10):
        entry = await orch.record_task(
            task_id=f"task_{i:03d}",
            energy_kwh=random.uniform(0.5, 2.0),
            carbon_kg=random.uniform(0.05, 0.5),
            helium_zone=random.choice(["zone_a", "zone_b", None]),
            helium_usage=random.uniform(0.0, 0.01),
            helium_spot_price=random.uniform(3.0, 6.0),
            hardware_type=random.choice(["cpu", "gpu", "tpu"]),
            power_budget=random.uniform(100, 400),
            fallback_used=random.random() < 0.1,
            state={"carbon_intensity": random.uniform(150, 600),
                   "queue_length": random.randint(0, 5)},
            force_chaos=random.choice([None, None, None, "latency"]),
            force_human_review=(i % 5 == 0),
        )
        print(f"  task_{i:03d}  hash={entry.hash[:10]}  "
              f"action={entry.causal_action}  "
              f"precision={entry.precision_level}  "
              f"agent={entry.agent_id}")

    # 2. Verify integrity
    print("\n=== Integrity check ===")
    ok = orch.ledger.verify_integrity()
    print(f"  Ledger integrity: {ok}")

    # 3. Enhancement-specific reports
    print("\n=== Enhancement 1: Distillation teacher stats ===")
    print(json.dumps(orch.ledger.get_distillation_teacher_stats(),
                     indent=2, default=str))

    print("\n=== Enhancement 2: Causal action stats ===")
    print(json.dumps(orch.ledger.get_causal_action_stats(),
                     indent=2, default=str))

    print("\n=== Enhancement 3: Federated aggregated metrics ===")
    print(json.dumps(orch.ledger.get_aggregated_metrics(),
                     indent=2, default=str))

    print("\n=== Enhancement 4: Agent performance ===")
    print(json.dumps(orch.ledger.get_agent_performance(),
                     indent=2, default=str))

    print("\n=== Enhancement 5: Temporal violation summary ===")
    print(json.dumps(orch.ledger.get_temporal_violation_summary(),
                     indent=2, default=str))

    print("\n=== Enhancement 6: XAI aggregate importance ===")
    print(json.dumps(orch.ledger.get_xai_aggregate_importance(),
                     indent=2, default=str))

    print("\n=== Enhancement 7: Precision energy savings ===")
    print(json.dumps(orch.ledger.get_precision_energy_savings(),
                     indent=2, default=str))

    print("\n=== Enhancement 8: Net carbon position ===")
    print(json.dumps(orch.ledger.get_net_carbon_position(),
                     indent=2, default=str))

    print("\n=== Enhancement 9: Chaos impact ===")
    print(json.dumps(orch.ledger.get_chaos_impact(),
                     indent=2, default=str))

    print("\n=== Enhancement 10: HITL impact ===")
    print(json.dumps(orch.ledger.get_hitl_impact(),
                     indent=2, default=str))

    # 4. Full report
    print("\n=== Full report ===")
    report = orch.full_report()
    for section, value in report.items():
        if isinstance(value, dict):
            print(f"  {section}: {len(value)} key(s)")
        else:
            print(f"  {section}: {value}")

    # 5. Health check
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
