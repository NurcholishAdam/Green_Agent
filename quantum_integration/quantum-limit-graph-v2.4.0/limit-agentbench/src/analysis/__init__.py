# src/analysis/__init__.py

"""
Analysis and evaluation modules including Pareto optimization (Enhanced)
=========================================================================

The `analysis` folder is the Green Agent's **decision-intelligence and
evidence layer**. Its responsibility:

    Turn execution telemetry, policies, and outcomes into trustworthy
    recommendations, explanations, evaluation reports, and learning signals.

It does NOT implement federated training, policy execution, or quantum
execution — those live in `federation/`, `policy/`, and `quantum/`.
`analysis` evaluates, compares, audits, and explains their outcomes.

Directory layout (target)
-------------------------
    contracts/       shared schemas (DecisionRecord, EvaluationVerdict)
    telemetry/       energy_meter, carbon_estimator, metrics_collector, ...
    optimization/    pareto, dominance, complexity, green_score, ...
    governance/      decision_explainer, causal_policy_evaluator, ...
    distributed/     federated_analytics, multiagent_role_analyzer
    resilience/      chaos, resilience_analyzer
    adapters/        langchain, autogen, runtime_adapter
    reporting/       leaderboard, pareto_visualizer, reports

Shared contracts
----------------
    DecisionRecord      — the single record every layer writes
    EvaluationVerdict   — the single verdict every evaluator emits

Enhancement-layer analytics (inline, feature-toggleable)
-------------------------------------------------------
    1. Quantum-Distillation      QuantumTradeoffAnalyzer
    2. Causal RL                 CausalPolicyEvaluator
    3. Federated Green Learning  FederatedAnalytics
    4. Multi-Agent Coordination  MultiAgentRoleAnalyzer
    5. Temporal Logic            VerificationEvidenceStore
    6. Explainable AI            DecisionExplainer
    7. Adaptive Precision        PrecisionHardwareAnalyzer
    8. Carbon Markets            CarbonInstrumentsLedger
    9. Resilience & Chaos        ResilienceAnalyzer
   10. Human-in-the-Loop         HumanFeedbackAnalyzer

Convenience helpers
-------------------
    get_capabilities()           runtime introspection
    create_decision_record()     factory for DecisionRecord
    record_from_budget_result()  convert EnhancedBudgetResult → DecisionRecord
    DEFAULT_FEATURES             default enhancement toggles
    ENABLE_ALL_FEATURES          preset to enable every layer
    DISABLE_ALL_FEATURES         preset to disable every layer

Metadata
--------
    __version__, __author__, __license__
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"
__author__ = "Green Agent Team"
__license__ = "Apache-2.0"


# =============================================================================
# 1. SHARED CONTRACTS — the two records every layer writes
# =============================================================================

class SafetyVerdict(Enum):
    """Outcome of formal verification for a decision."""
    PASSED = "passed"
    FAILED = "failed"
    UNVERIFIED = "unverified"
    NOT_APPLICABLE = "n/a"


class QuantumStatus(Enum):
    """Status of a quantum-executed route."""
    CLASSICAL = "classical"
    SIMULATOR = "simulator"
    HARDWARE = "hardware"
    HYBRID = "hybrid"
    DISTILLED = "distilled"


class PrecisionLevel(Enum):
    """Precision levels tracked by the analysis layer."""
    FP32 = "fp32"
    FP16 = "fp16"
    BF16 = "bf16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


@dataclass
class DecisionRecord:
    """
    The single versioned record that every layer writes.

    Any agent, policy, quantum route, hardware route, or human review
    should produce exactly this shape so the analysis layer can compare
    across layers without translation.
    """
    run_id: str
    timestamp: datetime
    task_id: str
    selected_action: str
    alternatives: List[str] = field(default_factory=list)
    policy_version: str = ""
    model_or_agent: str = ""
    hardware_profile: str = ""
    precision: Optional[str] = None

    # --- Quality / performance ---
    quality_score: Optional[float] = None
    latency_ms: float = 0.0

    # --- Physical sustainability (NEVER summed with contractual) ---
    energy_kwh: float = 0.0
    carbon_operational_kg: float = 0.0
    helium_units: float = 0.0

    # --- Contractual instruments (kept separate from operational) ---
    carbon_contractual_kg: float = 0.0
    rec_mwh: float = 0.0
    instrument_provenance: Optional[Dict[str, Any]] = None

    # --- Uncertainty and explanation ---
    uncertainty: Optional[float] = None
    explanation: Dict[str, Any] = field(default_factory=dict)

    # --- Safety and HITL ---
    safety_verdict: str = SafetyVerdict.NOT_APPLICABLE.value
    safety_evidence_id: Optional[str] = None
    human_approval_required: bool = False
    human_approval_status: Optional[str] = None
    escalation_reason: Optional[str] = None

    # --- Quantum-specific (only set when quantum_status != CLASSICAL) ---
    quantum_status: str = QuantumStatus.CLASSICAL.value
    quantum_shots: Optional[int] = None
    quantum_queue_ms: Optional[float] = None
    quantum_error_rate: Optional[float] = None
    quantum_mitigation_applied: bool = False

    # --- Provenance ---
    provenance: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["timestamp"] = self.timestamp.isoformat()
        return out

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DecisionRecord":
        data = dict(data)
        if isinstance(data.get("timestamp"), str):
            try:
                data["timestamp"] = datetime.fromisoformat(data["timestamp"])
            except ValueError:
                data["timestamp"] = datetime.now()
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @property
    def carbon_total_with_contractual(self) -> float:
        """
        Sum of operational + contractual emissions.

        NOTE: This is provided for reporting completeness only. Reports
        should show both numbers separately and never imply that buying
        RECs cancels physical emissions.
        """
        return self.carbon_operational_kg + self.carbon_contractual_kg


@dataclass
class EvaluationVerdict:
    """
    The single verdict that every evaluator emits for a DecisionRecord.

    Downstream consumers (leaderboards, dashboards, HITL gates) read this
    instead of re-deriving pass/fail from raw records.
    """
    record_id: str
    passed_hard_constraints: bool
    hard_constraint_violations: List[str] = field(default_factory=list)
    pareto_rank: Optional[int] = None
    is_pareto_efficient: Optional[bool] = None
    causal_attribution: Optional[Dict[str, Any]] = None
    explanation_confidence: Optional[float] = None
    safety_verdict: str = SafetyVerdict.NOT_APPLICABLE.value
    precision_verdict: Optional[str] = None
    resilience_verdict: Optional[str] = None
    human_feedback_verdict: Optional[str] = None
    notes: List[str] = field(default_factory=list)
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


# =============================================================================
# 2. SUBMODULE LOADING
# =============================================================================

_KNOWN_SUBMODULES = (
    # Existing telemetry
    "energy_meter", "carbon_estimator", "metrics_collector",
    "execution_trace", "metric_provenance", "self_monitor", "streaming",
    # Existing optimization
    "pareto", "pareto_analyzer", "extended_pareto_analyzer",
    "dominance_checker", "pareto_visualizer", "green_score",
    "complexity_analyzer",
    # Existing overhead / reporting
    "framework_overhead", "overhead_analyzer", "leaderboard",
    # Existing resilience
    "chaos",
    # Existing adapters
    "autogen_graph", "autogen_runtime",
    "langchain_callbacks", "langchain_runtime",
    "runtime_adapter",
    # Existing misc
    "policy_loader",
)

_SUBMODULES: Dict[str, Any] = {}


def _load_submodule(name: str) -> Optional[Any]:
    for spec in (f".{name}", name, f"src.analysis.{name}"):
        try:
            if spec.startswith("."):
                mod = import_module(spec, package=__name__)
            else:
                mod = import_module(spec)
            return mod
        except ImportError:
            continue
        except Exception as e:
            logger.warning(f"analysis.{name} raised on import: {e}")
            return None
    logger.debug(f"Optional submodule analysis.{name} not available")
    return None


for _name in _KNOWN_SUBMODULES:
    _SUBMODULES[_name] = _load_submodule(_name)


def _expose(submodule_name: str, *symbols: str) -> List[str]:
    """Lift symbols from a submodule; missing ones become None."""
    mod = _SUBMODULES.get(submodule_name)
    exposed: List[str] = []
    for sym in symbols:
        if mod is not None and hasattr(mod, sym):
            globals()[sym] = getattr(mod, sym)
            exposed.append(sym)
        else:
            globals().setdefault(sym, None)
    return exposed


# =============================================================================
# 3. EXISTING MODULE RE-EXPORTS (best-effort; safe if symbols absent)
# =============================================================================

_EXPORTED_TELEMETRY = []
_EXPORTED_TELEMETRY += _expose("energy_meter", "EnergyMeter")
_EXPORTED_TELEMETRY += _expose("carbon_estimator", "CarbonEstimator")
_EXPORTED_TELEMETRY += _expose("metrics_collector", "MetricsCollector", "MetricsSnapshot")
_EXPORTED_TELEMETRY += _expose("execution_trace", "ExecutionTrace", "TraceStep")
_EXPORTED_TELEMETRY += _expose("metric_provenance", "MetricProvenance")
_EXPORTED_TELEMETRY += _expose("self_monitor", "SelfMonitor")
_EXPORTED_TELEMETRY += _expose("streaming", "StreamingMetrics")

_EXPORTED_OPTIMIZATION = []
_EXPORTED_OPTIMIZATION += _expose("pareto", "ParetoPoint")
_EXPORTED_OPTIMIZATION += _expose(
    "pareto_analyzer", "ParetoFrontierAnalyzer", "ParetoPoint"
)
_EXPORTED_OPTIMIZATION += _expose(
    "extended_pareto_analyzer", "ExtendedParetoAnalyzer"
)
_EXPORTED_OPTIMIZATION += _expose("dominance_checker", "DominanceChecker")
_EXPORTED_OPTIMIZATION += _expose("pareto_visualizer", "ParetoVisualizer")
_EXPORTED_OPTIMIZATION += _expose("green_score", "GreenScore")
_EXPORTED_OPTIMIZATION += _expose(
    "complexity_analyzer", "ComplexityAnalyzer", "TaskComplexity"
)

_EXPORTED_OVERHEAD = []
_EXPORTED_OVERHEAD += _expose("framework_overhead", "FrameworkOverhead")
_EXPORTED_OVERHEAD += _expose("overhead_analyzer", "OverheadAnalyzer")

_EXPORTED_REPORTING = []
_EXPORTED_REPORTING += _expose("leaderboard", "Leaderboard")

_EXPORTED_RESILIENCE = []
_EXPORTED_RESILIENCE += _expose("chaos", "ChaosEngineer", "ChaosExperiment")

_EXPORTED_ADAPTERS = []
_EXPORTED_ADAPTERS += _expose("runtime_adapter", "RuntimeAdapter")
_EXPORTED_ADAPTERS += _expose("langchain_runtime", "LangChainRuntime")
_EXPORTED_ADAPTERS += _expose("autogen_runtime", "AutoGenRuntime")

_EXPORTED_MISC = []
_EXPORTED_MISC += _expose("policy_loader", "PolicyLoader")


# =============================================================================
# 4. NEW CONTRACT / FEATURE ENUMS
# =============================================================================

class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class RoleKind(Enum):
    GENERALIST = "generalist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    HELIUM_SPECIALIST = "helium_specialist"
    LATENCY_SPECIALIST = "latency_specialist"
    COORDINATOR = "coordinator"
    DELEGATOR = "delegator"


# =============================================================================
# 5. ENHANCEMENT-LAYER ANALYTICS (inline implementations)
# =============================================================================

# --- 6. Explainable AI ---
@dataclass
class ExplanationRecord:
    """Structured explanation for a DecisionRecord."""
    headline: str
    rationale: List[str]
    alternatives_rejected: List[Dict[str, Any]]
    trade_offs: Dict[str, float]
    constraints_applied: List[str]
    confidence: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DecisionExplainer:
    """
    Produce a traceable 'why this action?' record.

    Reads a DecisionRecord and emits an ExplanationRecord with:
      - headline
      - rationale (list of reasons)
      - alternatives_rejected (with reasons)
      - trade_offs (metric deltas vs. alternatives)
      - constraints_applied (which hard constraints were checked)
      - confidence (calibrated score)
    """

    def __init__(self, base_confidence: float = 0.75) -> None:
        self.base_confidence = base_confidence

    def explain(self, record: DecisionRecord) -> ExplanationRecord:
        reasons: List[str] = []
        reasons.append(
            f"Selected '{record.selected_action}' for task '{record.task_id}'."
        )
        if record.alternatives:
            reasons.append(
                f"Considered {len(record.alternatives)} alternatives: "
                f"{', '.join(record.alternatives)}."
            )
        if record.quality_score is not None:
            reasons.append(f"Quality score: {record.quality_score:.3f}.")
        reasons.append(
            f"Energy: {record.energy_kwh:.5f} kWh, "
            f"operational carbon: {record.carbon_operational_kg:.5f} kgCO₂e."
        )
        if record.precision:
            reasons.append(f"Precision: {record.precision}.")
        if record.safety_verdict == SafetyVerdict.PASSED.value:
            reasons.append("Formal safety verification passed.")
        elif record.safety_verdict == SafetyVerdict.FAILED.value:
            reasons.append("Formal safety verification FAILED.")

        rejected: List[Dict[str, Any]] = []
        for alt in record.alternatives:
            rejected.append({
                "alternative": alt,
                "reason": "dominated or failed a hard constraint",
            })

        trade_offs = {
            "energy_kwh": record.energy_kwh,
            "carbon_operational_kg": record.carbon_operational_kg,
            "latency_ms": record.latency_ms,
        }

        confidence = self.base_confidence
        if record.uncertainty is not None:
            confidence = max(0.3, confidence - record.uncertainty)
        if record.safety_verdict == SafetyVerdict.FAILED.value:
            confidence *= 0.5

        return ExplanationRecord(
            headline=f"[{record.selected_action}] {record.task_id}",
            rationale=reasons,
            alternatives_rejected=rejected,
            trade_offs=trade_offs,
            constraints_applied=["safety", "quality_threshold", "budget"],
            confidence=confidence,
        )


# --- 5. Temporal Logic Verification Evidence ---
@dataclass
class VerificationEvidence:
    """Evidence record for a formal verification run."""
    evidence_id: str
    policy_version: str
    verdict: str
    properties_checked: List[str]
    properties_passed: List[str]
    properties_failed: List[str]
    counterexamples: List[Dict[str, Any]]
    coverage_pct: float
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


class VerificationEvidenceStore:
    """
    Store verification evidence; do NOT embed the verifier.

    The temporal logic monitor lives in `symbolic/`. This class stores its
    output alongside the DecisionRecords that reference it.
    """

    def __init__(self) -> None:
        self._evidence: Dict[str, VerificationEvidence] = {}
        self._by_policy: Dict[str, List[str]] = defaultdict(list)

    def store(self, evidence: VerificationEvidence) -> None:
        self._evidence[evidence.evidence_id] = evidence
        self._by_policy[evidence.policy_version].append(evidence.evidence_id)

    def get(self, evidence_id: str) -> Optional[VerificationEvidence]:
        return self._evidence.get(evidence_id)

    def for_policy(self, policy_version: str) -> List[VerificationEvidence]:
        return [
            self._evidence[eid]
            for eid in self._by_policy.get(policy_version, [])
            if eid in self._evidence
        ]

    def coverage_summary(self) -> Dict[str, Any]:
        total = len(self._evidence)
        if total == 0:
            return {"total": 0, "pass_rate": 0.0, "avg_coverage_pct": 0.0}
        passed = sum(
            1 for e in self._evidence.values()
            if e.verdict == SafetyVerdict.PASSED.value
        )
        avg_cov = sum(e.coverage_pct for e in self._evidence.values()) / total
        return {
            "total": total,
            "pass_rate": passed / total,
            "avg_coverage_pct": avg_cov,
        }


# --- 7. Adaptive Precision / Hardware ---
@dataclass
class PrecisionHardwareComparison:
    """Comparison of precision levels across quality/power/memory."""
    precision: str
    quality_score: float
    energy_kwh: float
    power_watts: float
    memory_mb: float
    thermal_c: Optional[float] = None
    device_capability: Optional[str] = None


class PrecisionHardwareAnalyzer:
    """
    Compare FP32/FP16/BF16/INT8/INT4 routes against quality, power,
    memory, thermal state, and device capability.
    """

    def __init__(self) -> None:
        self.comparisons: List[PrecisionHardwareComparison] = []

    def add(self, cmp: PrecisionHardwareComparison) -> None:
        self.comparisons.append(cmp)

    def pareto_frontier(self) -> List[PrecisionHardwareComparison]:
        """Return non-dominated comparisons (min energy, max quality)."""
        frontier: List[PrecisionHardwareComparison] = []
        for c in self.comparisons:
            dominated = False
            for other in self.comparisons:
                if other is c:
                    continue
                if (other.energy_kwh <= c.energy_kwh
                        and other.quality_score >= c.quality_score
                        and (other.energy_kwh < c.energy_kwh
                             or other.quality_score > c.quality_score)):
                    dominated = True
                    break
            if not dominated:
                frontier.append(c)
        return frontier

    def summary(self) -> Dict[str, Any]:
        if not self.comparisons:
            return {"count": 0}
        return {
            "count": len(self.comparisons),
            "frontier_size": len(self.pareto_frontier()),
            "quality_range": (
                min(c.quality_score for c in self.comparisons),
                max(c.quality_score for c in self.comparisons),
            ),
            "energy_range_kwh": (
                min(c.energy_kwh for c in self.comparisons),
                max(c.energy_kwh for c in self.comparisons),
            ),
        }


# --- 9. Resilience & Chaos ---
@dataclass
class ResilienceEvent:
    """A single fault-injection scenario outcome."""
    scenario: str
    injected_fault: str
    recovery_time_ms: float
    degraded_mode_entered: bool
    safety_violations: int
    carbon_cost_of_recovery_kg: float
    at: datetime = field(default_factory=datetime.now)


class ResilienceAnalyzer:
    """
    Aggregate chaos experiment outcomes. Extends `chaos.py` with analysis.
    """

    def __init__(self) -> None:
        self.events: List[ResilienceEvent] = []

    def record(self, event: ResilienceEvent) -> None:
        self.events.append(event)

    def summary(self) -> Dict[str, Any]:
        if not self.events:
            return {"scenarios": 0}
        return {
            "scenarios": len(self.events),
            "avg_recovery_ms": sum(
                e.recovery_time_ms for e in self.events
            ) / len(self.events),
            "max_recovery_ms": max(e.recovery_time_ms for e in self.events),
            "degraded_mode_rate": sum(
                1 for e in self.events if e.degraded_mode_entered
            ) / len(self.events),
            "total_safety_violations": sum(
                e.safety_violations for e in self.events
            ),
            "total_carbon_recovery_kg": sum(
                e.carbon_cost_of_recovery_kg for e in self.events
            ),
        }


# --- 10. HITL & Active Learning ---
@dataclass
class HumanFeedbackEvent:
    """A single human review or override event."""
    decision_id: str
    reviewer_id: str
    original_action: str
    final_action: str
    override_applied: bool
    escalation_reason: Optional[str]
    reviewer_confidence: float
    uncertainty_at_decision: Optional[float]
    at: datetime = field(default_factory=datetime.now)


class HumanFeedbackAnalyzer:
    """
    Track overrides, escalation reasons, reviewer agreement, decision impact,
    and uncertainty-driven labeling value.
    """

    def __init__(self) -> None:
        self.events: List[HumanFeedbackEvent] = []

    def record(self, event: HumanFeedbackEvent) -> None:
        self.events.append(event)

    def summary(self) -> Dict[str, Any]:
        if not self.events:
            return {"events": 0}
        overrides = [e for e in self.events if e.override_applied]
        return {
            "events": len(self.events),
            "override_rate": len(overrides) / len(self.events),
            "avg_reviewer_confidence": sum(
                e.reviewer_confidence for e in self.events
            ) / len(self.events),
            "escalation_reasons": self._reason_counts(overrides),
            "avg_uncertainty_at_override": self._avg_uncertainty(overrides),
        }

    @staticmethod
    def _reason_counts(overrides: List[HumanFeedbackEvent]) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for e in overrides:
            counts[e.escalation_reason or "unspecified"] += 1
        return dict(counts)

    @staticmethod
    def _avg_uncertainty(
        overrides: List[HumanFeedbackEvent],
    ) -> Optional[float]:
        vals = [
            e.uncertainty_at_decision for e in overrides
            if e.uncertainty_at_decision is not None
        ]
        return sum(vals) / len(vals) if vals else None


# --- 4. Multi-Agent Role Analytics ---
@dataclass
class AgentObservation:
    """Observation of a single agent's behaviour in a run."""
    agent_id: str
    task_id: str
    role: str
    delegated_to: List[str]
    coordination_calls: int
    latency_ms: float
    energy_kwh: float


class MultiAgentRoleAnalyzer:
    """
    Detect recurring roles, collaboration patterns, redundancy,
    delegation quality, and coordination overhead.
    """

    def __init__(self) -> None:
        self.observations: List[AgentObservation] = []

    def record(self, obs: AgentObservation) -> None:
        self.observations.append(obs)

    def role_distribution(self) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for o in self.observations:
            counts[o.role] += 1
        return dict(counts)

    def coordination_overhead(self) -> Dict[str, float]:
        if not self.observations:
            return {"avg_coordination_calls": 0.0, "overhead_energy_kwh": 0.0}
        return {
            "avg_coordination_calls": sum(
                o.coordination_calls for o in self.observations
            ) / len(self.observations),
            "overhead_energy_kwh": sum(
                o.energy_kwh for o in self.observations
            ),
        }

    def delegation_quality(self) -> Dict[str, Any]:
        delegators = [o for o in self.observations if o.delegated_to]
        if not delegators:
            return {"delegation_events": 0}
        return {
            "delegation_events": len(delegators),
            "avg_delegations_per_event": sum(
                len(o.delegated_to) for o in delegators
            ) / len(delegators),
            "redundant_delegations": sum(
                1 for o in delegators if len(set(o.delegated_to)) != len(o.delegated_to)
            ),
        }


# --- 2. Causal RL Policy Evaluation ---
@dataclass
class PolicyIntervention:
    """A single policy intervention and its outcome."""
    policy_version_before: str
    policy_version_after: str
    metric: str
    value_before: float
    value_after: float
    confounders: Dict[str, float] = field(default_factory=dict)


class CausalPolicyEvaluator:
    """
    Estimate whether a policy change CAUSED improvement, not just correlated.

    This is a lightweight scaffold. Real implementations should use
    doubly-robust estimation or instrumental variables.
    """

    def __init__(self) -> None:
        self.interventions: List[PolicyIntervention] = []

    def record(self, intervention: PolicyIntervention) -> None:
        self.interventions.append(intervention)

    def naive_effect(self, metric: str) -> Optional[float]:
        """Average naive delta for a metric (correlation, not causation)."""
        relevant = [i for i in self.interventions if i.metric == metric]
        if not relevant:
            return None
        return sum(i.value_after - i.value_before for i in relevant) / len(relevant)

    def confounder_adjusted_effect(self, metric: str) -> Optional[float]:
        """
        Placeholder for confounder-adjusted effect estimation.

        Returns the naive effect minus a weighted confounder penalty.
        Replace with a proper causal estimator when enough data exists.
        """
        relevant = [i for i in self.interventions if i.metric == metric]
        if not relevant:
            return None
        adjusted = []
        for i in relevant:
            penalty = sum(i.confounders.values()) * 0.1
            adjusted.append((i.value_after - i.value_before) - penalty)
        return sum(adjusted) / len(adjusted)


# --- 3. Federated Analytics ---
@dataclass
class FederatedSiteReport:
    """Anonymized site-level metrics for federated aggregation."""
    site_id: str
    sample_count: int
    mean_energy_kwh: float
    mean_carbon_kg: float
    mean_latency_ms: float
    fairness_index: float
    communication_cost_kb: float


class FederatedAnalytics:
    """
    Aggregate site-level metrics WITHOUT centralizing raw telemetry.

    Measures fairness, communication cost, and carbon impact per deployment.
    """

    def __init__(self) -> None:
        self.reports: List[FederatedSiteReport] = []

    def push(self, report: FederatedSiteReport) -> None:
        self.reports.append(report)

    def aggregate(self) -> Dict[str, Any]:
        if not self.reports:
            return {"sites": 0}
        total = sum(r.sample_count for r in self.reports) or 1
        return {
            "sites": len(self.reports),
            "total_samples": total,
            "weighted_energy_kwh": sum(
                r.mean_energy_kwh * r.sample_count for r in self.reports
            ) / total,
            "weighted_carbon_kg": sum(
                r.mean_carbon_kg * r.sample_count for r in self.reports
            ) / total,
            "min_fairness_index": min(
                r.fairness_index for r in self.reports
            ),
            "total_communication_kb": sum(
                r.communication_cost_kb for r in self.reports
            ),
        }


# --- 1. Quantum-Distillation Trade-off ---
@dataclass
class QuantumRouteComparison:
    """A single classical / quantum / distilled route comparison."""
    route_id: str
    quantum_status: str
    quality_score: float
    energy_kwh: float
    carbon_kg: float
    latency_ms: float
    error_rate: Optional[float]
    shots: Optional[int]
    queue_ms: Optional[float]


class QuantumTradeoffAnalyzer:
    """
    Compare classical, quantum, and distilled routes across solution quality,
    energy, carbon, latency, error rate, shots, and queue time.

    Records simulator-vs-hardware status explicitly; never claims quantum
    advantage from isolated simulator results.
    """

    def __init__(self) -> None:
        self.routes: List[QuantumRouteComparison] = []

    def add(self, route: QuantumRouteComparison) -> None:
        self.routes.append(route)

    def summary(self) -> Dict[str, Any]:
        if not self.routes:
            return {"routes": 0}
        return {
            "routes": len(self.routes),
            "hardware_routes": sum(
                1 for r in self.routes
                if r.quantum_status == QuantumStatus.HARDWARE.value
            ),
            "simulator_routes": sum(
                1 for r in self.routes
                if r.quantum_status == QuantumStatus.SIMULATOR.value
            ),
            "avg_error_rate": self._avg(
                [r.error_rate for r in self.routes if r.error_rate is not None]
            ),
            "avg_queue_ms": self._avg(
                [r.queue_ms for r in self.routes if r.queue_ms is not None]
            ),
            "quality_range": (
                min(r.quality_score for r in self.routes),
                max(r.quality_score for r in self.routes),
            ),
        }

    @staticmethod
    def _avg(vals: List[float]) -> Optional[float]:
        return sum(vals) / len(vals) if vals else None


# --- 8. Carbon Instruments Ledger ---
@dataclass
class CarbonInstrument:
    """A carbon certificate, REC, or offset."""
    instrument_id: str
    kind: str                 # "rec" | "offset" | "credit"
    quantity: float           # MWh or kgCO2e depending on kind
    unit: str
    vintage_year: Optional[int]
    matching_period: Optional[str]
    producer: Optional[str]
    provenance: Dict[str, Any] = field(default_factory=dict)


class CarbonInstrumentsLedger:
    """
    Track carbon certificates, RECs, and offsets SEPARATELY from physical
    operational emissions. Reports must never sum operational and
    contractual emissions into a single number.
    """

    def __init__(self) -> None:
        self.instruments: List[CarbonInstrument] = []

    def add(self, instrument: CarbonInstrument) -> None:
        self.instruments.append(instrument)

    def by_kind(self) -> Dict[str, List[CarbonInstrument]]:
        grouped: Dict[str, List[CarbonInstrument]] = defaultdict(list)
        for i in self.instruments:
            grouped[i.kind].append(i)
        return dict(grouped)

    def summary(self) -> Dict[str, Any]:
        return {
            "total_instruments": len(self.instruments),
            "by_kind": {
                kind: len(items)
                for kind, items in self.by_kind().items()
            },
            "total_rec_mwh": sum(
                i.quantity for i in self.instruments
                if i.kind == "rec" and i.unit == "MWh"
            ),
            "total_offset_kgco2e": sum(
                i.quantity for i in self.instruments
                if i.kind == "offset" and i.unit == "kgCO2e"
            ),
        }


# =============================================================================
# 6. FACTORY HELPERS
# =============================================================================

def create_decision_record(
    *,
    run_id: str,
    task_id: str,
    selected_action: str,
    alternatives: Optional[List[str]] = None,
    policy_version: str = "",
    model_or_agent: str = "",
    hardware_profile: str = "",
    precision: Optional[str] = None,
    quality_score: Optional[float] = None,
    latency_ms: float = 0.0,
    energy_kwh: float = 0.0,
    carbon_operational_kg: float = 0.0,
    helium_units: float = 0.0,
    carbon_contractual_kg: float = 0.0,
    rec_mwh: float = 0.0,
    uncertainty: Optional[float] = None,
    explanation: Optional[Dict[str, Any]] = None,
    safety_verdict: str = SafetyVerdict.NOT_APPLICABLE.value,
    provenance: Optional[Dict[str, Any]] = None,
) -> DecisionRecord:
    """Convenience factory for DecisionRecord with sensible defaults."""
    return DecisionRecord(
        run_id=run_id,
        timestamp=datetime.now(),
        task_id=task_id,
        selected_action=selected_action,
        alternatives=alternatives or [],
        policy_version=policy_version,
        model_or_agent=model_or_agent,
        hardware_profile=hardware_profile,
        precision=precision,
        quality_score=quality_score,
        latency_ms=latency_ms,
        energy_kwh=energy_kwh,
        carbon_operational_kg=carbon_operational_kg,
        helium_units=helium_units,
        carbon_contractual_kg=carbon_contractual_kg,
        rec_mwh=rec_mwh,
        uncertainty=uncertainty,
        explanation=explanation or {},
        safety_verdict=safety_verdict,
        provenance=provenance or {},
    )


def record_from_budget_result(
    *,
    run_id: str,
    task_id: str,
    selected_action: str,
    budget_result: Any,
    model_or_agent: str = "",
) -> Optional[DecisionRecord]:
    """
    Convert a constraints.EnhancedBudgetResult (from the `constraints`
    package) into a DecisionRecord.

    This bridges the constraints package and the analysis package so
    budget decisions can be evaluated alongside everything else.
    """
    if budget_result is None:
        return None
    passed = getattr(budget_result, "passed", None)
    severity = getattr(budget_result, "severity", None)
    violations = getattr(budget_result, "violations", []) or []

    explanation = getattr(budget_result, "explanation", None) or {}
    safety_verdict = (
        SafetyVerdict.PASSED.value if passed else SafetyVerdict.FAILED.value
    )
    return create_decision_record(
        run_id=run_id,
        task_id=task_id,
        selected_action=selected_action,
        model_or_agent=model_or_agent,
        safety_verdict=safety_verdict,
        explanation={
            "budget_severity": severity,
            "budget_violations": violations,
            "budget_explanation": explanation,
        },
    )


# =============================================================================
# 7. FEATURE TOGGLES
# =============================================================================

DEFAULT_FEATURES: Dict[str, bool] = {
    "contracts": True,
    "decision_explainer": True,
    "verification_evidence": True,
    "precision_hardware_analyzer": True,
    "resilience_analyzer": True,
    "multiagent_role_analyzer": True,
    "human_feedback_analyzer": True,
    "causal_policy_evaluator": False,  # needs historical data
    "federated_analytics": False,      # needs real deployments
    "quantum_tradeoff_analyzer": False,# needs real quantum data
    "carbon_instruments_ledger": False,# needs external integration
}

ENABLE_ALL_FEATURES: Dict[str, bool] = {k: True for k in DEFAULT_FEATURES}
DISABLE_ALL_FEATURES: Dict[str, bool] = {k: False for k in DEFAULT_FEATURES}


# =============================================================================
# 8. CAPABILITY INTROSPECTION
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """Return a summary of what the analysis package can do."""
    return {
        "version": __version__,
        "submodules_loaded": {
            name: (mod is not None)
            for name, mod in _SUBMODULES.items()
        },
        "exported": {
            "telemetry": list(_EXPORTED_TELEMETRY),
            "optimization": list(_EXPORTED_OPTIMIZATION),
            "overhead": list(_EXPORTED_OVERHEAD),
            "reporting": list(_EXPORTED_REPORTING),
            "resilience_existing": list(_EXPORTED_RESILIENCE),
            "adapters": list(_EXPORTED_ADAPTERS),
            "misc": list(_EXPORTED_MISC),
        },
        "new_capabilities": [
            "DecisionRecord", "EvaluationVerdict",
            "DecisionExplainer", "VerificationEvidenceStore",
            "PrecisionHardwareAnalyzer", "ResilienceAnalyzer",
            "MultiAgentRoleAnalyzer", "HumanFeedbackAnalyzer",
            "CausalPolicyEvaluator", "FederatedAnalytics",
            "QuantumTradeoffAnalyzer", "CarbonInstrumentsLedger",
        ],
        "features_available": list(DEFAULT_FEATURES),
    }


# =============================================================================
# 9. PUBLIC API SURFACE
# =============================================================================

__all__: List[str] = []

# --- Shared contracts ---
__all__ += ["DecisionRecord", "EvaluationVerdict"]
__all__ += ["SafetyVerdict", "QuantumStatus", "PrecisionLevel", "Severity", "RoleKind"]

# --- Enhancement-layer analytics ---
__all__ += [
    "DecisionExplainer", "ExplanationRecord",
    "VerificationEvidenceStore", "VerificationEvidence",
    "PrecisionHardwareAnalyzer", "PrecisionHardwareComparison",
    "ResilienceAnalyzer", "ResilienceEvent",
    "MultiAgentRoleAnalyzer", "AgentObservation",
    "HumanFeedbackAnalyzer", "HumanFeedbackEvent",
    "CausalPolicyEvaluator", "PolicyIntervention",
    "FederatedAnalytics", "FederatedSiteReport",
    "QuantumTradeoffAnalyzer", "QuantumRouteComparison",
    "CarbonInstrumentsLedger", "CarbonInstrument",
]

# --- Factories & introspection ---
__all__ += [
    "create_decision_record", "record_from_budget_result",
    "get_capabilities",
    "DEFAULT_FEATURES", "ENABLE_ALL_FEATURES", "DISABLE_ALL_FEATURES",
    "__version__", "__author__", "__license__",
]

# --- Existing module re-exports (only if resolved to non-None) ---
for _sym in (
    _EXPORTED_TELEMETRY + _EXPORTED_OPTIMIZATION + _EXPORTED_OVERHEAD
    + _EXPORTED_REPORTING + _EXPORTED_RESILIENCE + _EXPORTED_ADAPTERS
    + _EXPORTED_MISC
):
    if globals().get(_sym) is not None and _sym not in __all__:
        __all__.append(_sym)


# =============================================================================
# 10. LOAD DIAGNOSTICS
# =============================================================================

if logger.isEnabledFor(logging.DEBUG):
    _loaded = [n for n, m in _SUBMODULES.items() if m is not None]
    _missing = [n for n, m in _SUBMODULES.items() if m is None]
    logger.debug(
        f"analysis package ready: {len(__all__)} public symbols "
        f"(loaded={len(_loaded)} submodules, missing={_missing})"
    )
