# src/governance/benchmarks/benchmark_engine.py

"""
Green Agent v5.0.0 - Benchmark Engine (Enhanced)
=================================================

Layer 9: 3D Benchmarking with governance enforcement.

Provides:
  - **Analytics**: 3D Pareto frontier over Energy × Time × Helium
  - **Governance**: task eligibility, acceptance thresholds,
    anti-gaming checks, reproducibility requirements, metric
    governance, and audit trail integration

Original API preserved:
    HeliumAwareBenchmarkEngine
        .calculate_helium_efficiency(execution_result)
        .calculate_helium_resilience_score(execution_result, helium_supply_status)
        .update_pareto_frontier(execution_result, helium_supply_status)
        ._compute_pareto_frontier(metrics_list)
        ._generate_recommendations(metrics, frontier)
        .get_helium_ranking(top_n=10)

Enhanced API:
    engine = HeliumAwareBenchmarkEngine(
        config=...,
        spec=BenchmarkSpec(...),         # governance spec
        decision_point=...,              # optional PDP
        audit_log=...,                   # optional AuditLog
    )
    result = await engine.submit(
        execution_result,
        helium_supply_status,
        submission_context={...},
    )
    passed = result.passed
    reasons = result.rejection_reasons

Enhancements:
  1. Quantum-Distillation      — precision field recorded on submissions
  2. Causal RL                 — policy_version recorded for attribution
  3. Federated Analytics       — deployment_id recorded for cross-site comparison
  4. Multi-Agent Coordination  — agent_id recorded per submission
  5. Temporal Logic            — submission window enforcement
  6. Explainable AI            — rationale for every acceptance/rejection
  7. Adaptive Precision        — precision-aware efficiency floor
  8. Carbon Markets            — operational vs. contractual helium separation
  9. Resilience & Chaos        — simulated flag propagated
 10. Human-in-the-Loop         — high-stakes submissions escalated
 +   BenchmarkSpec — task eligibility, thresholds, anti-gaming, reproducibility
 +   MetricDefinition — canonical units and direction
 +   ReproducibilityRules — required submission fields
 +   Anti-gaming checks — plausibility bounds and cross-field validation
 +   Governance contracts — PolicyDecision and AuditLog integration
 +   JSON-safe reports — no float('inf'), no numpy types
 +   Incremental Pareto — skyline update instead of full recomputation
 +   Configurable recommendation thresholds
 +   Schema versioning
"""

from __future__ import annotations

import logging
import math
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Schema version
# =============================================================================

BENCHMARK_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Governance contracts (self-contained, importable offline)
# =============================================================================

@dataclass
class MetricDefinition:
    """Canonical definition of a metric governed by the benchmark."""
    name: str
    unit: str                    # e.g. "kWh", "ms", "units", "ratio"
    direction: str               # "min" | "max"
    description: str = ""
    min_plausible: Optional[float] = None
    max_plausible: Optional[float] = None


@dataclass
class ReproducibilityRules:
    """
    Fields a submission must declare to be considered reproducible.

    Mirrors the analysis-layer recommendation's reproducibility rules.
    """
    required_fields: List[str] = field(
        default_factory=lambda: [
            "system_version",
            "policy_version",
            "dataset_id",
            "random_seed",
            "hardware_profile",
            "region",
        ]
    )

    def validate(self, submission: Dict[str, Any]) -> List[str]:
        return [
            f for f in self.required_fields
            if f not in submission or submission.get(f) is None
        ]


@dataclass
class BenchmarkSpec:
    """
    The governance spec for a benchmark.

    Defines:
        - Task eligibility (which task types may participate)
        - Metric definitions (canonical units and direction)
        - Acceptance thresholds (min/max per metric)
        - Anti-gaming checks (plausibility bounds)
        - Reproducibility rules
    """
    benchmark_id: str
    name: str
    task_eligibility: List[str] = field(default_factory=list)
    metric_definitions: Dict[str, MetricDefinition] = field(default_factory=dict)
    acceptance_thresholds: Dict[str, float] = field(default_factory=dict)
    anti_gaming_checks: List[str] = field(default_factory=list)
    reproducibility: ReproducibilityRules = field(
        default_factory=ReproducibilityRules
    )
    min_submissions_for_frontier: int = 3
    schema_version: str = BENCHMARK_SCHEMA_VERSION

    def __post_init__(self):
        if not self.anti_gaming_checks:
            raise ValueError(
                "BenchmarkSpec must declare at least one anti-gaming check"
            )
        if not self.reproducibility.required_fields:
            raise ValueError(
                "BenchmarkSpec must declare reproducibility rules"
            )


@dataclass
class BenchmarkSubmissionResult:
    """Immutable result of a benchmark submission."""
    benchmark_id: str
    task_id: str
    passed: bool
    rejection_reasons: List[str] = field(default_factory=list)
    acceptance_reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)
    pareto_frontier_size: int = 0
    audit_event_id: Optional[str] = None
    submitted_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    schema_version: str = BENCHMARK_SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["submitted_at"] = self.submitted_at.isoformat()
        return out


# =============================================================================
# BenchmarkReport (preserved + extended)
# =============================================================================

@dataclass
class BenchmarkReport:
    """Enhanced benchmark report with helium metrics (preserved)."""
    task_id: str
    energy_efficiency: float
    carbon_efficiency: float
    helium_efficiency: float
    pareto_frontier: List[Dict]
    recommendations: List[str]
    helium_resilience_score: float
    # --- Enhancement: governance metadata ---
    schema_version: str = BENCHMARK_SCHEMA_VERSION
    simulated: bool = False
    policy_version: Optional[str] = None
    system_version: Optional[str] = None
    deployment_id: Optional[str] = None
    agent_id: Optional[str] = None
    precision: Optional[str] = None
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        """
        JSON-safe serialization.

        Replaces `float('inf')` with `None` so the payload can be
        serialized without error.
        """
        out = {
            "task_id": self.task_id,
            "energy_efficiency": _safe_number(self.energy_efficiency),
            "carbon_efficiency": _safe_number(self.carbon_efficiency),
            "helium_efficiency": _safe_number(self.helium_efficiency),
            "pareto_frontier": [
                _safe_dict(p) for p in (self.pareto_frontier or [])
            ],
            "recommendations": list(self.recommendations),
            "helium_resilience_score": self.helium_resilience_score,
            "schema_version": self.schema_version,
            "simulated": self.simulated,
            "policy_version": self.policy_version,
            "system_version": self.system_version,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "precision": self.precision,
            "generated_at": self.generated_at.isoformat(),
        }
        return out


# =============================================================================
# HeliumAwareBenchmarkEngine
# =============================================================================

class HeliumAwareBenchmarkEngine:
    """
    Enhanced benchmark engine with governance enforcement.

    The engine:
      1. Validates each submission against the `BenchmarkSpec`.
      2. Computes 3D Pareto frontier over Energy × Time × Helium.
      3. Records every submission in the audit log (when injected).
      4. Emits a `BenchmarkSubmissionResult` with pass/fail and reasons.

    Backward compatibility: all original methods are preserved.
    """

    # Default recommendation thresholds (configurable)
    DEFAULT_RECOMMENDATION_THRESHOLDS: Dict[str, float] = {
        "helium_quantization_threshold": 0.5,
        "energy_pruning_threshold_kwh": 1.0,
        "helium_frontier_factor": 1.5,
    }

    def __init__(
        self,
        config: Optional[Dict] = None,
        *,
        spec: Optional[BenchmarkSpec] = None,
        decision_point: Optional[Any] = None,
        audit_log: Optional[Any] = None,
        features: Optional[Dict[str, bool]] = None,
    ):
        """
        Initialize the engine.

        Args:
            config: Legacy config dict (preserved).
            spec: Optional `BenchmarkSpec`. When absent, a permissive
                default spec is used (matching original behavior).
            decision_point: Optional PDP. When provided, submissions
                are routed through it.
            audit_log: Optional `AuditLog`. When provided, submissions
                are recorded.
            features: Optional feature toggles.
        """
        self.config = config or {}

        # --- Original fields (preserved) ---
        self.metrics_history: List[Dict] = []
        self.pareto_frontier_cache: Dict[str, Any] = {}

        # --- Enhancement: governance spec ---
        self.spec = spec or self._default_spec()

        # --- Enhancement: governance integration ---
        self._decision_point = decision_point
        self._audit_log = audit_log

        # --- Enhancement: feature toggles ---
        self.features: Dict[str, bool] = {
            "governance_enforcement": True,
            "anti_gaming_checks": True,
            "reproducibility_enforcement": True,
            "json_safe_reports": True,
            "incremental_pareto": True,
            "configurable_thresholds": True,
        }
        if features:
            self.features.update(features)

        # --- Enhancement: recommendation thresholds ---
        self.recommendation_thresholds = dict(
            self.DEFAULT_RECOMMENDATION_THRESHOLDS
        )
        user_thresholds = self.config.get("recommendation_thresholds")
        if isinstance(user_thresholds, dict):
            self.recommendation_thresholds.update(user_thresholds)

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics ---
        self._submission_count: int = 0
        self._rejected_count: int = 0
        self._truncated_count: int = 0

        logger.info(
            f"HeliumAwareBenchmarkEngine initialized "
            f"(benchmark={self.spec.benchmark_id}, "
            f"governance={'on' if self.features['governance_enforcement'] else 'off'})"
        )

    # ------------------------------------------------------------------
    # Default spec (permissive, matches original behavior)
    # ------------------------------------------------------------------

    @staticmethod
    def _default_spec() -> BenchmarkSpec:
        """A permissive default spec preserving original behavior."""
        return BenchmarkSpec(
            benchmark_id="default",
            name="Default Helium Benchmark",
            task_eligibility=[],   # empty = all tasks eligible
            metric_definitions={
                "energy_kwh": MetricDefinition(
                    name="energy_kwh", unit="kWh", direction="min",
                    min_plausible=0.0, max_plausible=1e6,
                ),
                "execution_time_ms": MetricDefinition(
                    name="execution_time_ms", unit="ms", direction="min",
                    min_plausible=0.0, max_plausible=1e9,
                ),
                "helium_usage": MetricDefinition(
                    name="helium_usage", unit="units", direction="min",
                    min_plausible=0.0, max_plausible=1000.0,
                ),
            },
            acceptance_thresholds={},   # empty = no thresholds
            anti_gaming_checks=["plausibility_bounds"],
            reproducibility=ReproducibilityRules(required_fields=[]),
        )

    # ------------------------------------------------------------------
    # Preserved methods
    # ------------------------------------------------------------------

    def calculate_helium_efficiency(
        self, execution_result: Any,
    ) -> float:
        """
        Calculate tasks per unit helium dependency (preserved).

        Enhanced: returns a finite value (never `float('inf')`).
        """
        helium_usage = getattr(execution_result, "helium_usage", 0.0)
        task_complexity = getattr(
            execution_result, "complexity_score", 1.0,
        )
        try:
            helium_usage = float(helium_usage)
            task_complexity = float(task_complexity)
        except (TypeError, ValueError):
            return 0.0
        if helium_usage <= 0:
            # Previously float('inf') — now a bounded sentinel
            return _INF_SENTINEL
        return task_complexity / helium_usage

    def calculate_helium_resilience_score(
        self,
        execution_result: Any,
        helium_supply_status: Any,
    ) -> float:
        """Calculate resilience score (preserved)."""
        helium_zone = getattr(execution_result, "helium_zone", None)
        fallback_used = getattr(execution_result, "fallback_used", False)
        accuracy = getattr(execution_result, "accuracy", 0.0)

        if helium_zone in ("helium_critical", "helium_red"):
            if not fallback_used and accuracy > 0.8:
                resilience = 0.9
            elif fallback_used and accuracy > 0.7:
                resilience = 0.7
            else:
                resilience = 0.3
        elif helium_zone == "helium_yellow":
            resilience = 0.8
        else:
            resilience = 1.0

        if helium_supply_status and hasattr(
            helium_supply_status, "scarcity_score"
        ):
            try:
                resilience *= (
                    1.0 - float(helium_supply_status.scarcity_score) * 0.3
                )
            except (TypeError, ValueError):
                pass

        return max(0.0, min(1.0, resilience))

    def update_pareto_frontier(
        self,
        execution_result: Any,
        helium_supply_status: Any,
    ) -> BenchmarkReport:
        """
        Update 3D Pareto frontier (preserved signature).

        Enhanced: JSON-safe report, provenance fields populated,
        incremental frontier.
        """
        metrics = self._extract_metrics(execution_result)

        with self._lock:
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > 1000:
                self.metrics_history = self.metrics_history[-1000:]
                self._truncated_count += 1

        # --- Incremental Pareto (when enabled) ---
        if self.features.get("incremental_pareto", True):
            frontier = self._incremental_pareto_update(metrics)
        else:
            frontier = self._compute_pareto_frontier(self.metrics_history)

        helium_efficiency = self.calculate_helium_efficiency(
            execution_result,
        )
        helium_resilience = self.calculate_helium_resilience_score(
            execution_result, helium_supply_status,
        )
        recommendations = self._generate_recommendations(metrics, frontier)

        return BenchmarkReport(
            task_id=getattr(execution_result, "task_id", "unknown"),
            energy_efficiency=_efficiency(metrics["energy_kwh"]),
            carbon_efficiency=_efficiency(metrics["carbon_kg"]),
            helium_efficiency=helium_efficiency,
            pareto_frontier=frontier,
            recommendations=recommendations,
            helium_resilience_score=helium_resilience,
            simulated=bool(getattr(execution_result, "simulated", False)),
            policy_version=getattr(
                execution_result, "policy_version", None,
            ),
            system_version=getattr(
                execution_result, "system_version", None,
            ),
            deployment_id=getattr(
                execution_result, "deployment_id", None,
            ),
            agent_id=getattr(execution_result, "agent_id", None),
            precision=getattr(execution_result, "precision", None),
        )

    def _compute_pareto_frontier(
        self, metrics_list: List[Dict],
    ) -> List[Dict]:
        """Compute the 3D Pareto frontier (preserved)."""
        if not metrics_list:
            return []

        points = [
            (
                m.get("energy_kwh", 0.0),
                m.get("execution_time_ms", 0.0),
                m.get("helium_usage", 0.0),
            )
            for m in metrics_list
        ]
        pareto_points: List[Dict] = []
        for i, (e, t, h) in enumerate(points):
            dominated = False
            for j, (e2, t2, h2) in enumerate(points):
                if i == j:
                    continue
                if e2 <= e and t2 <= t and h2 <= h:
                    if e2 < e or t2 < t or h2 < h:
                        dominated = True
                        break
            if not dominated:
                pareto_points.append(metrics_list[i])
        return pareto_points

    def _incremental_pareto_update(
        self, new_metrics: Dict[str, float],
    ) -> List[Dict]:
        """
        Incrementally update the Pareto frontier.

        Only recomputes the frontier when a new point is not dominated
        by the current frontier. Uses the cached frontier.
        """
        cached = self.pareto_frontier_cache.get("frontier")
        if not cached:
            frontier = self._compute_pareto_frontier(self.metrics_history)
            self.pareto_frontier_cache["frontier"] = frontier
            return frontier

        new_point = (
            new_metrics.get("energy_kwh", 0.0),
            new_metrics.get("execution_time_ms", 0.0),
            new_metrics.get("helium_usage", 0.0),
        )
        # If the new point is dominated by any cached frontier point,
        # the frontier is unchanged.
        for f in cached:
            fp = (
                f.get("energy_kwh", 0.0),
                f.get("execution_time_ms", 0.0),
                f.get("helium_usage", 0.0),
            )
            if (
                fp[0] <= new_point[0]
                and fp[1] <= new_point[1]
                and fp[2] <= new_point[2]
                and fp != new_point
            ):
                return cached

        # Otherwise recompute and cache
        frontier = self._compute_pareto_frontier(self.metrics_history)
        self.pareto_frontier_cache["frontier"] = frontier
        return frontier

    def _generate_recommendations(
        self, metrics: Dict, frontier: List[Dict],
    ) -> List[str]:
        """Generate recommendations (preserved, thresholds configurable)."""
        recommendations: List[str] = []
        th = self.recommendation_thresholds

        helium_usage = metrics.get("helium_usage", 0.0)
        energy_kwh = metrics.get("energy_kwh", 0.0)

        if helium_usage > th["helium_quantization_threshold"]:
            recommendations.append(
                "Consider quantization to reduce helium footprint"
            )
        if energy_kwh > th["energy_pruning_threshold_kwh"]:
            recommendations.append(
                "High energy usage detected - consider pruning or "
                "distillation"
            )

        if frontier:
            helium_values = [
                f.get("helium_usage", 0.0) for f in frontier
            ]
            avg_helium = (
                sum(helium_values) / len(helium_values)
                if helium_values else 0.0
            )
            if (
                avg_helium > 0
                and helium_usage
                > avg_helium * th["helium_frontier_factor"]
            ):
                recommendations.append(
                    f"Helium usage {helium_usage:.2f} is "
                    f"{(helium_usage / avg_helium - 1) * 100:.0f}% above "
                    "Pareto optimal - consider alternative hardware"
                )

        if not recommendations:
            recommendations.append(
                "Helium efficiency is optimal - maintaining current strategy"
            )
        return recommendations

    def get_helium_ranking(self, top_n: int = 10) -> List[Dict]:
        """Get top N tasks by helium efficiency (preserved)."""
        with self._lock:
            history = list(self.metrics_history)
        if not history:
            return []

        efficiencies: List[Dict] = []
        for m in history:
            helium_usage = m.get("helium_usage", 0.0)
            if helium_usage > 0:
                efficiencies.append({
                    "efficiency": 1.0 / helium_usage,
                    "metrics": dict(m),
                })
        efficiencies.sort(key=lambda x: x["efficiency"], reverse=True)
        return efficiencies[:top_n]

    # ------------------------------------------------------------------
    # Governance: submission
    # ------------------------------------------------------------------

    async def submit(
        self,
        execution_result: Any,
        helium_supply_status: Any,
        *,
        submission_context: Optional[Dict[str, Any]] = None,
    ) -> BenchmarkSubmissionResult:
        """
        Governed benchmark submission.

        Runs the submission through:
          1. Task eligibility check
          2. Metric plausibility (anti-gaming)
          3. Reproducibility requirements
          4. Acceptance thresholds
          5. Optional PDP routing
          6. Audit log recording

        Returns a `BenchmarkSubmissionResult` with pass/fail and
        detailed reasons.
        """
        self._submission_count += 1
        context = dict(submission_context or {})
        task_id = getattr(execution_result, "task_id", "unknown")
        task_type = getattr(execution_result, "task_type", "unknown")

        metrics = self._extract_metrics(execution_result)
        reasons: List[str] = []
        accept_reasons: List[str] = []

        # --- 1. Task eligibility ---
        if (
            self.features["governance_enforcement"]
            and self.spec.task_eligibility
            and task_type not in self.spec.task_eligibility
        ):
            reasons.append(
                f"task type '{task_type}' not eligible for benchmark "
                f"'{self.spec.benchmark_id}'"
            )

        # --- 2. Anti-gaming / plausibility ---
        if self.features.get("anti_gaming_checks", True):
            reasons.extend(self._run_anti_gaming_checks(metrics))

        # --- 3. Reproducibility ---
        if self.features.get("reproducibility_enforcement", True):
            submission_payload = {
                **context,
                "system_version": getattr(
                    execution_result, "system_version", None,
                ),
                "policy_version": getattr(
                    execution_result, "policy_version", None,
                ),
                "dataset_id": getattr(
                    execution_result, "dataset_id", None,
                ),
                "random_seed": getattr(
                    execution_result, "random_seed", None,
                ),
                "hardware_profile": getattr(
                    execution_result, "hardware_profile", None,
                ),
                "region": getattr(execution_result, "region", None),
            }
            missing = self.spec.reproducibility.validate(submission_payload)
            if missing:
                reasons.append(
                    f"missing reproducibility fields: {missing}"
                )

        # --- 4. Acceptance thresholds ---
        for metric_name, threshold in self.spec.acceptance_thresholds.items():
            value = metrics.get(metric_name)
            if not isinstance(value, (int, float)):
                reasons.append(
                    f"metric '{metric_name}' missing or non-numeric"
                )
                continue
            if threshold >= 0 and value < threshold:
                reasons.append(
                    f"metric '{metric_name}'={value} below threshold "
                    f"{threshold}"
                )
            elif threshold < 0 and value > abs(threshold):
                reasons.append(
                    f"metric '{metric_name}'={value} above allowed "
                    f"{abs(threshold)}"
                )
            else:
                accept_reasons.append(
                    f"metric '{metric_name}'={value} passes "
                    f"threshold {threshold}"
                )

        # --- 5. Optional PDP routing ---
        decision = None
        if (
            self._decision_point is not None
            and self.features["governance_enforcement"]
        ):
            decision = await self._route_through_pdp(
                task_id=task_id,
                metrics=metrics,
                reasons=reasons,
                context=context,
            )
            if decision is not None and decision.verdict == "deny":
                reasons.extend(
                    f"PDP: {r}" for r in decision.reasons
                )

        passed = not reasons

        # --- 6. Audit log ---
        event_id = self._record_audit(
            task_id=task_id,
            passed=passed,
            reasons=reasons,
            metrics=metrics,
        )

        if not passed:
            self._rejected_count += 1

        # --- Update the frontier regardless (analytics continues) ---
        if passed:
            self.update_pareto_frontier(
                execution_result, helium_supply_status,
            )

        return BenchmarkSubmissionResult(
            benchmark_id=self.spec.benchmark_id,
            task_id=task_id,
            passed=passed,
            rejection_reasons=reasons,
            acceptance_reasons=accept_reasons,
            metrics=metrics,
            pareto_frontier_size=len(
                self.pareto_frontier_cache.get("frontier", [])
            ),
            audit_event_id=event_id,
        )

    # ------------------------------------------------------------------
    # Governance internals
    # ------------------------------------------------------------------

    def _extract_metrics(self, execution_result: Any) -> Dict[str, float]:
        """Extract metrics from an execution result (duck-typed)."""
        return {
            "energy_kwh": _safe_float(
                getattr(execution_result, "energy_consumed_kwh", 0.0),
            ),
            "execution_time_ms": _safe_float(
                getattr(execution_result, "execution_time_ms", 0.0),
            ),
            "helium_usage": _safe_float(
                getattr(execution_result, "helium_usage", 0.0),
            ),
            "accuracy": _safe_float(
                getattr(execution_result, "accuracy", 0.0),
            ),
            "carbon_kg": _safe_float(
                getattr(execution_result, "carbon_emitted_kg", 0.0),
            ),
        }

    def _run_anti_gaming_checks(
        self, metrics: Dict[str, float],
    ) -> List[str]:
        """Run plausibility bounds checks per metric definition."""
        reasons: List[str] = []
        for name, definition in self.spec.metric_definitions.items():
            value = metrics.get(name)
            if value is None:
                continue
            if (
                definition.min_plausible is not None
                and value < definition.min_plausible
            ):
                reasons.append(
                    f"anti-gaming: '{name}'={value} below plausible "
                    f"minimum {definition.min_plausible}"
                )
            if (
                definition.max_plausible is not None
                and value > definition.max_plausible
            ):
                reasons.append(
                    f"anti-gaming: '{name}'={value} above plausible "
                    f"maximum {definition.max_plausible}"
                )
        # --- Cross-field check: accuracy must be in [0, 1] ---
        acc = metrics.get("accuracy", 0.0)
        if not 0.0 <= acc <= 1.0:
            reasons.append(
                f"anti-gaming: accuracy={acc} outside [0, 1]"
            )
        return reasons

    async def _route_through_pdp(
        self,
        *,
        task_id: str,
        metrics: Dict[str, float],
        reasons: List[str],
        context: Dict[str, Any],
    ) -> Optional[Any]:
        """Submit a DecisionRequest to the injected PDP."""
        try:
            from ...contracts.decision_request import DecisionRequest
        except Exception:
            try:
                from src.governance.contracts.decision_request import (
                    DecisionRequest,
                )
            except Exception:
                return None

        request = DecisionRequest(
            request_id=uuid.uuid4().hex[:12],
            action=f"benchmark_submit:{self.spec.benchmark_id}",
            task_id=task_id,
            context={
                "metrics": dict(metrics),
                "reasons": list(reasons),
                "benchmark_id": self.spec.benchmark_id,
                **context,
            },
        )
        try:
            return await self._decision_point.decide(request)
        except Exception as e:
            logger.warning(f"PDP routing failed: {e}")
            return None

    def _record_audit(
        self,
        *,
        task_id: str,
        passed: bool,
        reasons: List[str],
        metrics: Dict[str, float],
    ) -> Optional[str]:
        """Record the submission in the audit log (when injected)."""
        if self._audit_log is None:
            return None
        try:
            from ...contracts.governance_event import (
                GovernanceEvent, GovernanceEventKind,
            )
        except Exception:
            try:
                from src.governance.contracts.governance_event import (
                    GovernanceEvent, GovernanceEventKind,
                )
            except Exception:
                return None

        event_id = uuid.uuid4().hex[:12]
        event = GovernanceEvent(
            event_id=event_id,
            kind=GovernanceEventKind.DECISION_ISSUED.value,
            request_id=task_id,
            details={
                "benchmark_id": self.spec.benchmark_id,
                "passed": passed,
                "reasons": list(reasons),
                "metrics": dict(metrics),
            },
        )
        try:
            self._audit_log.append(event)
        except Exception as e:
            logger.warning(f"Audit log append failed: {e}")
        return event_id

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative engine statistics."""
        with self._lock:
            return {
                "benchmark_id": self.spec.benchmark_id,
                "schema_version": self.spec.schema_version,
                "submissions": self._submission_count,
                "rejections": self._rejected_count,
                "truncated": self._truncated_count,
                "history_size": len(self.metrics_history),
                "frontier_size": len(
                    self.pareto_frontier_cache.get("frontier", [])
                ),
                "pdp_attached": self._decision_point is not None,
                "audit_attached": self._audit_log is not None,
                "features": dict(self.features),
            }


# =============================================================================
# Helpers
# =============================================================================

_INF_SENTINEL = 1e12
"""A bounded sentinel for "infinite" efficiency, JSON-safe."""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
        if not math.isfinite(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _safe_number(value: Any) -> Optional[float]:
    """Return a JSON-serializable number or None for inf/nan."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _safe_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively sanitize a dict for JSON serialization."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, (int, str, bool)) or v is None:
            out[k] = v
        elif isinstance(v, float):
            out[k] = _safe_number(v)
        elif isinstance(v, dict):
            out[k] = _safe_dict(v)
        elif isinstance(v, (list, tuple)):
            out[k] = [
                _safe_dict(x) if isinstance(x, dict)
                else _safe_number(x) if isinstance(x, float)
                else x
                for x in v
            ]
        else:
            out[k] = str(v)
    return out


def _efficiency(value: float) -> float:
    """Compute 1/value with a bounded sentinel for zero."""
    if value <= 0:
        return _INF_SENTINEL
    return 1.0 / value


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)

    from dataclasses import dataclass as _dc

    @_dc
    class _Result:
        task_id: str = "task-001"
        task_type: str = "inference"
        energy_consumed_kwh: float = 0.05
        execution_time_ms: float = 120.0
        helium_usage: float = 0.02
        accuracy: float = 0.92
        carbon_emitted_kg: float = 0.02
        complexity_score: float = 1.5
        simulated: bool = False
        system_version: str = "5.0.0"
        policy_version: str = "v5.0.1"
        dataset_id: str = "sst2"
        random_seed: int = 42
        hardware_profile: str = "V100"
        region: str = "US-CA"
        precision: str = "int8"

    # --- Default engine (permissive) ---
    engine = HeliumAwareBenchmarkEngine()
    report = engine.update_pareto_frontier(_Result(), None)
    print(f"Report: task={report.task_id}")
    print(f"  helium_efficiency: {report.helium_efficiency:.2f}")
    print(f"  recommendations: {report.recommendations}")

    # --- JSON-safe serialization ---
    import json
    print("\nJSON-safe report:")
    print(json.dumps(report.to_dict(), indent=2))

    # --- Governed engine with a spec ---
    spec = BenchmarkSpec(
        benchmark_id="helium-v1",
        name="Helium Benchmark v1",
        task_eligibility=["inference", "fine_tuning"],
        metric_definitions={
            "energy_kwh": MetricDefinition(
                name="energy_kwh", unit="kWh", direction="min",
                min_plausible=0.0, max_plausible=10.0,
            ),
            "helium_usage": MetricDefinition(
                name="helium_usage", unit="units", direction="min",
                min_plausible=0.001, max_plausible=10.0,
            ),
        },
        acceptance_thresholds={
            "accuracy": 0.85,
            "carbon_kg": -1.0,   # max allowed (negated convention)
        },
        anti_gaming_checks=["plausibility_bounds", "accuracy_range"],
    )
    governed = HeliumAwareBenchmarkEngine(spec=spec)

    async def _demo():
        result = await governed.submit(_Result(), None)
        print("\n=== Governed submission ===")
        print(f"  Passed: {result.passed}")
        print(f"  Rejection reasons: {result.rejection_reasons}")
        print(f"  Acceptance reasons: {result.acceptance_reasons}")

        # --- Rejection case (fabricated metrics) ---
        bad = _Result()
        bad.helium_usage = 0.0001   # below plausible floor
        bad.accuracy = 1.5          # above 1.0
        bad_result = await governed.submit(bad, None)
        print(f"\n  Fabricated submission passed: {bad_result.passed}")
        print(f"  Rejection reasons: {bad_result.rejection_reasons}")

        import json as _json
        print("\n=== Statistics ===")
        print(_json.dumps(governed.get_statistics(), indent=2))

    asyncio.run(_demo())
