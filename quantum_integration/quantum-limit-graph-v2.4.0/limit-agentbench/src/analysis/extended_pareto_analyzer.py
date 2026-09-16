# src/analysis/optimization/extended_pareto_analyzer.py

"""
Extended Pareto Analysis with Memory, Quantum Circuit Depth,
and Inference Variance (Enhanced)
====================================================================

Adds three critical dimensions to Green_Agent's multi-objective optimization:

1. Memory Footprint (MB)  - Hard constraint for edge deployment
2. Quantum Circuit Depth  - Structural cost/fragility measure
3. Inference Variance (σ) - Stability/predictability measure

Enhanced with the analysis-layer recommendation:
  - Provenance, uncertainty, simulated flags
  - Structured XAI for recommendations
  - NaN-safe dominance with configurable tolerance
  - DecisionRecord emission (shared contract)
  - Federated aggregation of frontier statistics
  - HITL review for recommendations
  - Operational vs. contractual carbon separation
  - Statistics
  - No numpy dependency
"""

from __future__ import annotations

import logging
import math
import statistics
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class Severity(Enum):
    """Severity levels aligned with the analysis layer."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class Feasibility(Enum):
    """Feasibility classification for a point."""
    FEASIBLE = "feasible"
    BORDERLINE = "borderline"      # within 5% of a constraint
    INFEASIBLE = "infeasible"


class DimensionDirection(Enum):
    """Direction of an optimization objective."""
    MAXIMIZE = "maximize"
    MINIMIZE = "minimize"


# =============================================================================
# Core dimensions metadata (single source of truth)
# =============================================================================

_CORE_DIMENSIONS = (
    "accuracy", "energy_kwh", "carbon_co2e_kg", "latency_ms",
    "memory_mb", "circuit_depth", "variance_score",
)

_MAXIMIZE_DIMS = frozenset({"accuracy"})
_MINIMIZE_DIMS = frozenset(_CORE_DIMENSIONS) - _MAXIMIZE_DIMS


# =============================================================================
# Enhancement 6: XAI — Structured frontier explanation
# =============================================================================

@dataclass
class FrontierExplanation:
    """Structured rationale for a frontier recommendation."""
    headline: str
    rationale: List[str]
    contributing_factors: Dict[str, float]
    counterfactual: str = ""
    confidence: float = 0.9

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class FrontierExplainer:
    @staticmethod
    def explain_point(
        point: "ExtendedParetoPoint",
        frontier: List["ExtendedParetoPoint"],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> FrontierExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Agent '{point.agent_id}' selected from a "
            f"{len(frontier)}-agent frontier."
        )
        reasons.append(
            f"Accuracy={point.accuracy:.3f}, energy={point.energy_kwh:.5f} kWh, "
            f"carbon={point.carbon_co2e_kg:.6f} kgCO₂e."
        )
        if point.memory_mb:
            reasons.append(f"Memory footprint: {point.memory_mb:.1f} MB.")
        if point.circuit_depth:
            reasons.append(f"Quantum circuit depth: {point.circuit_depth}.")
        if point.variance_score:
            reasons.append(
                f"Inference variance: {point.variance_score:.3f} "
                f"({'stable' if point.variance_score < 0.2 else 'noisy'})."
            )
        if point.simulated:
            reasons.append(
                "WARNING: this point includes simulated measurements."
            )
        if not point.provenance_ok:
            reasons.append(
                "WARNING: point has missing or invalid provenance."
            )

        counterfactual = (
            f"Without the frontier, the naive best-accuracy agent would be "
            f"'{max(frontier, key=lambda p: p.accuracy).agent_id}' — "
            f"which may not be feasible under deployment constraints."
        ) if frontier else ""

        return FrontierExplanation(
            headline=f"Recommended: {point.agent_id}",
            rationale=reasons,
            contributing_factors={
                "accuracy": point.accuracy,
                "energy_kwh": point.energy_kwh,
                "carbon_co2e_kg": point.carbon_co2e_kg,
                "latency_ms": point.latency_ms,
                "memory_mb": point.memory_mb,
                "circuit_depth": float(point.circuit_depth),
                "variance_score": point.variance_score,
            },
            counterfactual=counterfactual,
        )


# =============================================================================
# Federated aggregation
# =============================================================================

@dataclass
class FederatedFrontierProfile:
    deployment_id: str
    dimension: str
    mean_value: float
    std_value: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedFrontierAggregator:
    """Aggregate anonymized frontier statistics across deployments."""
    def __init__(self) -> None:
        self.profiles: List[FederatedFrontierProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, p: FederatedFrontierProfile) -> None:
        self.profiles.append(p)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedFrontierProfile]] = defaultdict(list)
        for p in self.profiles:
            grouped[p.dimension].append(p)
        result: Dict[str, Dict[str, float]] = {}
        for dim, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[dim] = {
                "mean_value": sum(
                    p.mean_value * p.sample_count for p in profiles
                ) / total_w,
                "std_value": sum(
                    p.std_value * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "total_frontiers": _STATS["frontiers"],
        "total_points_analyzed": _STATS["points"],
        "total_recommendations": _STATS["recommendations"],
        "total_dominance_checks": _STATS["dominance_checks"],
        "invalid_dominance_checks": _STATS["invalid_dominance"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# ExtendedParetoPoint — enhanced, backward compatible
# =============================================================================

@dataclass
class ExtendedParetoPoint:
    """
    Extended Pareto point with 7 dimensions.

    Original fields preserved verbatim. New optional fields default to
    safe values so existing construction sites continue to work.
    """
    # --- Original fields ---
    agent_id: str
    accuracy: float
    energy_kwh: float
    carbon_co2e_kg: float
    latency_ms: float
    memory_mb: float
    circuit_depth: int = 0
    variance_score: float = 0.0
    metadata: Dict = field(default_factory=dict)

    # --- Enhancement: provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    trace_id: Optional[str] = None
    source: str = "unknown"
    simulated: bool = False

    # --- Enhancement: uncertainty ---
    accuracy_std: float = 0.0
    energy_std: float = 0.0
    carbon_std: float = 0.0
    latency_std: float = 0.0
    memory_std: float = 0.0

    # --- Enhancement: carbon separation (proposal requirement) ---
    carbon_contractual_kg: float = 0.0
    rec_mwh: float = 0.0

    # --- Enhancement: temporal verification ---
    provenance_ok: bool = True
    provenance_violations: List[str] = field(default_factory=list)

    # --- Enhancement: precision context ---
    precision: Optional[str] = None

    # --- Enhancement: timestamp ---
    at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        """Validate all dimensions (original + enhanced)."""
        # --- Original validation (preserved verbatim) ---
        if not 0 <= self.accuracy <= 1:
            logger.warning(
                f"Accuracy {self.accuracy} outside [0, 1] for {self.agent_id}"
            )
        for metric, value in [
            ('energy_kwh', self.energy_kwh),
            ('carbon_co2e_kg', self.carbon_co2e_kg),
            ('latency_ms', self.latency_ms),
            ('memory_mb', self.memory_mb),
            ('circuit_depth', self.circuit_depth),
            ('variance_score', self.variance_score),
        ]:
            if value < 0:
                raise ValueError(f"{metric} cannot be negative: {value}")

        # --- Enhancement: provenance verification ---
        violations: List[str] = []
        if self.run_id is None and self.task_id is None:
            violations.append("no_run_or_task_id")
        for metric_name, value in [
            ("accuracy", self.accuracy),
            ("energy_kwh", self.energy_kwh),
            ("carbon_co2e_kg", self.carbon_co2e_kg),
            ("latency_ms", self.latency_ms),
            ("memory_mb", self.memory_mb),
        ]:
            v = getattr(self, metric_name)
            try:
                f = float(v)
            except (TypeError, ValueError):
                violations.append(f"{metric_name}_non_numeric")
                continue
            if not math.isfinite(f):
                violations.append(f"{metric_name}_not_finite")
        self.provenance_ok = len(violations) == 0
        self.provenance_violations = violations

    def dominates(
        self,
        other: "ExtendedParetoPoint",
        dimensions: Optional[List[str]] = None,
        *,
        tolerance: float = 0.0,
    ) -> bool:
        """
        Check if this point Pareto-dominates another in extended space.

        Backward-compatible: same signature plus optional `tolerance`.
        NaN-safe: any non-finite value makes the comparison invalid
        (returns False) instead of silently corrupting the verdict.
        """
        if dimensions is None:
            dimensions = list(_CORE_DIMENSIONS)

        _STATS["dominance_checks"] += 1
        better_on_at_least_one = False

        for dim in dimensions:
            self_val = getattr(self, dim, None)
            other_val = getattr(other, dim, None)

            # --- NaN/missing safety ---
            if self_val is None or other_val is None:
                _STATS["invalid_dominance"] += 1
                return False
            try:
                sv = float(self_val)
                ov = float(other_val)
            except (TypeError, ValueError):
                _STATS["invalid_dominance"] += 1
                return False
            if not math.isfinite(sv) or not math.isfinite(ov):
                _STATS["invalid_dominance"] += 1
                return False

            diff = sv - ov
            within_tolerance = abs(diff) <= tolerance

            if within_tolerance:
                continue

            # --- Maximization objective ---
            if dim == 'accuracy':
                if sv < ov:
                    return False
                if sv > ov:
                    better_on_at_least_one = True
            # --- Minimization objective ---
            else:
                if sv > ov:
                    return False
                if sv < ov:
                    better_on_at_least_one = True

        return better_on_at_least_one

    def to_dict(self) -> Dict:
        """Serialize to dictionary (original keys preserved)."""
        out = {
            'agent_id': self.agent_id,
            'accuracy': self.accuracy,
            'energy_kwh': self.energy_kwh,
            'carbon_co2e_kg': self.carbon_co2e_kg,
            'latency_ms': self.latency_ms,
            'memory_mb': self.memory_mb,
            'circuit_depth': self.circuit_depth,
            'variance_score': self.variance_score,
            'metadata': self.metadata,
        }
        # Enhancement additions (purely additive)
        out.update({
            'run_id': self.run_id,
            'task_id': self.task_id,
            'source': self.source,
            'simulated': self.simulated,
            'carbon_contractual_kg': self.carbon_contractual_kg,
            'rec_mwh': self.rec_mwh,
            'precision': self.precision,
            'provenance_ok': self.provenance_ok,
            'provenance_violations': list(self.provenance_violations),
            'at': self.at.isoformat(),
            'accuracy_std': self.accuracy_std,
            'energy_std': self.energy_std,
        })
        return out

    @classmethod
    def from_dict(cls, data: Dict) -> "ExtendedParetoPoint":
        """Deserialize from dictionary (backward compatible)."""
        return cls(
            agent_id=data['agent_id'],
            accuracy=data['accuracy'],
            energy_kwh=data['energy_kwh'],
            carbon_co2e_kg=data['carbon_co2e_kg'],
            latency_ms=data['latency_ms'],
            memory_mb=data['memory_mb'],
            circuit_depth=data.get('circuit_depth', 0),
            variance_score=data.get('variance_score', 0.0),
            metadata=data.get('metadata', {}),
            run_id=data.get('run_id'),
            task_id=data.get('task_id'),
            source=data.get('source', 'unknown'),
            simulated=bool(data.get('simulated', False)),
            carbon_contractual_kg=data.get('carbon_contractual_kg', 0.0),
            rec_mwh=data.get('rec_mwh', 0.0),
            precision=data.get('precision'),
        )

    def to_decision_record(
        self,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord for this point (shared contract).

        Returns None if the contract isn't importable.
        """
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return None

        return DecisionRecord(
            run_id=self.run_id or "",
            timestamp=self.at,
            task_id=self.task_id or "",
            selected_action=f"pareto_point({self.agent_id})",
            policy_version=policy_version,
            model_or_agent=self.agent_id,
            precision=self.precision,
            quality_score=self.accuracy,
            latency_ms=self.latency_ms,
            energy_kwh=self.energy_kwh,
            carbon_operational_kg=self.carbon_co2e_kg,
            carbon_contractual_kg=self.carbon_contractual_kg,
            rec_mwh=self.rec_mwh,
            explanation={
                "frontier_dimensions": list(_CORE_DIMENSIONS),
                "memory_mb": self.memory_mb,
                "circuit_depth": self.circuit_depth,
                "variance_score": self.variance_score,
            },
            provenance={
                "source": self.source,
                "simulated": self.simulated,
                "provenance_ok": self.provenance_ok,
            },
        )


# =============================================================================
# ExtendedParetoAnalyzer — enhanced, backward compatible
# =============================================================================

class ExtendedParetoAnalyzer:
    """
    Extended Pareto frontier analysis with 7 dimensions.

    Backward-compatible: same constructor, same methods, same return shapes.
    """

    def __init__(
        self,
        dimensions: Optional[List[str]] = None,
        *,
        deployment_id: str = "local",
        dominance_tolerance: float = 0.0,
    ):
        # --- Original state ---
        self.dimensions = dimensions or list(_CORE_DIMENSIONS)

        # --- Enhancement state ---
        self.deployment_id = deployment_id
        self.dominance_tolerance = float(dominance_tolerance)
        self.federated = FederatedFrontierAggregator()
        self._hitl_callback: Optional[
            Callable[[ExtendedParetoPoint, FrontierExplanation], bool]
        ] = None

        logger.info(
            f"Enhanced ExtendedParetoAnalyzer initialized with "
            f"{len(self.dimensions)}D (deployment={deployment_id})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API
    # ------------------------------------------------------------------

    def compute_frontier(
        self, agents: List[ExtendedParetoPoint],
    ) -> List[ExtendedParetoPoint]:
        """Compute the Pareto frontier in the configured dimension space."""
        if not agents:
            logger.warning("Empty agent list")
            return []

        _STATS["frontiers"] += 1
        _STATS["points"] += len(agents)

        frontier: List[ExtendedParetoPoint] = []
        for agent in agents:
            is_dominated = False
            for other in agents:
                if other.agent_id != agent.agent_id:
                    if other.dominates(
                        agent, self.dimensions,
                        tolerance=self.dominance_tolerance,
                    ):
                        is_dominated = True
                        logger.debug(
                            f"{agent.agent_id} dominated by {other.agent_id}"
                        )
                        break
            if not is_dominated:
                frontier.append(agent)

        logger.info(f"Frontier: {len(frontier)} / {len(agents)} agents")
        return frontier

    def project_2d(
        self,
        agents: List[ExtendedParetoPoint],
        x_dim: str,
        y_dim: str,
    ) -> Tuple[List[ExtendedParetoPoint], List[ExtendedParetoPoint]]:
        """Project frontier onto 2D plane (unchanged semantics)."""
        temp_analyzer = ExtendedParetoAnalyzer(dimensions=[x_dim, y_dim])
        frontier_2d = temp_analyzer.compute_frontier(agents)
        frontier_ids = {a.agent_id for a in frontier_2d}
        dominated = [a for a in agents if a.agent_id not in frontier_ids]
        logger.info(
            f"2D projection ({x_dim} vs {y_dim}): "
            f"{len(frontier_2d)} frontier, {len(dominated)} dominated"
        )
        return frontier_2d, dominated

    def analyze_memory_constraint(
        self,
        agents: List[ExtendedParetoPoint],
        max_memory_mb: float,
    ) -> Dict:
        """Analyze agents under memory constraint (unchanged semantics)."""
        feasible = [a for a in agents if a.memory_mb <= max_memory_mb]
        infeasible = [a for a in agents if a.memory_mb > max_memory_mb]
        frontier_feasible = self.compute_frontier(feasible) if feasible else []
        memory_efficiency = {
            a.agent_id: a.accuracy / a.memory_mb if a.memory_mb > 0 else 0.0
            for a in agents
        }
        logger.info(
            f"Memory constraint {max_memory_mb} MB: "
            f"{len(feasible)} feasible, {len(infeasible)} infeasible"
        )
        return {
            'max_memory_mb': max_memory_mb,
            'feasible_count': len(feasible),
            'infeasible_count': len(infeasible),
            'feasible': feasible,
            'infeasible': infeasible,
            'frontier_feasible': frontier_feasible,
            'memory_efficiency': memory_efficiency,
            'best_memory_efficient': (
                max(memory_efficiency.items(), key=lambda x: x[1])[0]
                if memory_efficiency else None
            ),
        }

    def analyze_circuit_depth_scalability(
        self, agents: List[ExtendedParetoPoint],
    ) -> Dict:
        """Analyze quantum circuit depth (unchanged semantics)."""
        quantum_agents = [a for a in agents if a.circuit_depth > 0]
        if not quantum_agents:
            logger.warning("No quantum agents found (circuit_depth=0)")
            return {'quantum_agents': 0}

        depths = [a.circuit_depth for a in quantum_agents]
        accuracies = [a.accuracy for a in quantum_agents]
        energies = [a.energy_kwh for a in quantum_agents]

        # --- Stdlib correlations (no numpy) ---
        def _corr(xs: List[float], ys: List[float]) -> float:
            if len(xs) < 2:
                return 0.0
            mx = statistics.fmean(xs)
            my = statistics.fmean(ys)
            num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
            dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
            dy = math.sqrt(sum((y - my) ** 2 for y in ys))
            denom = dx * dy
            return num / denom if denom > 0 else 0.0

        acc_depth_corr = _corr(depths, accuracies)
        energy_depth_corr = _corr(depths, energies)

        fragility_scores = {
            a.agent_id: (
                a.circuit_depth / a.accuracy if a.accuracy > 0 else float('inf')
            )
            for a in quantum_agents
        }
        median_depth = statistics.median(depths)
        shallow_agents = [
            a for a in quantum_agents if a.circuit_depth < median_depth
        ]
        logger.info(
            f"Circuit depth analysis: {len(quantum_agents)} quantum agents, "
            f"median depth={median_depth:.0f}"
        )
        return {
            'quantum_agents_count': len(quantum_agents),
            'depth_stats': {
                'mean': statistics.fmean(depths),
                'median': median_depth,
                'min': min(depths),
                'max': max(depths),
                'std': (
                    statistics.pstdev(depths) if len(depths) > 1 else 0.0
                ),
            },
            'correlations': {
                'accuracy_vs_depth': acc_depth_corr,
                'energy_vs_depth': energy_depth_corr,
            },
            'shallow_circuit_agents': [a.agent_id for a in shallow_agents],
            'fragility_scores': fragility_scores,
            'most_fragile': max(
                fragility_scores.items(), key=lambda x: x[1]
            )[0] if fragility_scores else None,
            'most_robust': min(
                fragility_scores.items(), key=lambda x: x[1]
            )[0] if fragility_scores else None,
        }

    def analyze_variance_stability(
        self,
        agents: List[ExtendedParetoPoint],
        stability_threshold: float = 0.2,
    ) -> Dict:
        """Analyze inference variance (unchanged semantics)."""
        stable = [
            a for a in agents if a.variance_score < stability_threshold
        ]
        unstable = [
            a for a in agents if a.variance_score >= stability_threshold
        ]
        variance_cost = {}
        for agent in agents:
            p95_energy_estimate = agent.energy_kwh * (
                1 + 2 * agent.variance_score
            )
            variance_cost[agent.agent_id] = (
                p95_energy_estimate - agent.energy_kwh
            )
        stability_ranking = sorted(agents, key=lambda a: a.variance_score)
        logger.info(
            f"Variance analysis: {len(stable)} stable, {len(unstable)} unstable "
            f"(threshold={stability_threshold})"
        )
        mean_var = (
            statistics.fmean([a.variance_score for a in agents])
            if agents else 0.0
        )
        return {
            'stability_threshold': stability_threshold,
            'stable_count': len(stable),
            'unstable_count': len(unstable),
            'stable': stable,
            'unstable': unstable,
            'variance_cost': variance_cost,
            'stability_ranking': [a.agent_id for a in stability_ranking],
            'most_stable': (
                stability_ranking[0].agent_id if stability_ranking else None
            ),
            'least_stable': (
                stability_ranking[-1].agent_id if stability_ranking else None
            ),
            'mean_variance': mean_var,
        }

    def comprehensive_analysis(
        self,
        agents: List[ExtendedParetoPoint],
        constraints: Optional[Dict] = None,
    ) -> Dict:
        """
        Comprehensive 7D analysis (unchanged semantics).

        Enhanced: adds XAI rationale, provenance summary, federated
        contribution, HITL review, and DecisionRecords.
        """
        if constraints is None:
            constraints = {}

        frontier_7d = self.compute_frontier(agents)
        memory_analysis = self.analyze_memory_constraint(
            agents, constraints.get('max_memory_mb', 1024),
        )
        circuit_analysis = self.analyze_circuit_depth_scalability(agents)
        variance_analysis = self.analyze_variance_stability(
            agents, constraints.get('max_variance', 0.2),
        )

        # --- Original fully-compliant logic (preserved) ---
        fully_compliant = []
        for agent in agents:
            compliant = True
            if agent.memory_mb > constraints.get('max_memory_mb', float('inf')):
                compliant = False
            if agent.circuit_depth > constraints.get('max_circuit_depth', float('inf')):
                compliant = False
            if agent.variance_score > constraints.get('max_variance', float('inf')):
                compliant = False
            if compliant:
                fully_compliant.append(agent)

        frontier_compliant = (
            self.compute_frontier(fully_compliant)
            if fully_compliant else []
        )

        recommendation = (
            frontier_compliant[0] if frontier_compliant else None
        )

        # --- Enhancement: XAI explanation ---
        explanation: Optional[FrontierExplanation] = None
        if recommendation is not None:
            explanation = FrontierExplainer.explain_point(
                recommendation, frontier_compliant, constraints,
            )
            _STATS["recommendations"] += 1

        # --- Enhancement: HITL review ---
        hitl_required = False
        hitl_approved: Optional[bool] = None
        if (
            recommendation is not None
            and self._hitl_callback is not None
            and explanation is not None
        ):
            try:
                hitl_required = True
                hitl_approved = self._hitl_callback(
                    recommendation, explanation,
                )
            except Exception as e:
                logger.warning(f"HITL callback failed: {e}")

        # --- Enhancement: federated contribution ---
        if frontier_compliant:
            for dim in _CORE_DIMENSIONS:
                values = [getattr(a, dim, 0.0) for a in frontier_compliant]
                try:
                    nums = [float(v) for v in values]
                except (TypeError, ValueError):
                    continue
                if not nums:
                    continue
                self.federated.push(FederatedFrontierProfile(
                    deployment_id=self.deployment_id,
                    dimension=dim,
                    mean_value=statistics.fmean(nums),
                    std_value=(
                        statistics.pstdev(nums) if len(nums) > 1 else 0.0
                    ),
                    sample_count=len(nums),
                ))

        # --- Enhancement: provenance summary ---
        provenance_summary = {
            "total_agents": len(agents),
            "agents_with_valid_provenance": sum(
                1 for a in agents if a.provenance_ok
            ),
            "simulated_agents": sum(1 for a in agents if a.simulated),
        }

        # --- Build result (original keys preserved, additive keys added) ---
        result: Dict[str, Any] = {
            'total_agents': len(agents),
            'frontier_7d': frontier_7d,
            'frontier_7d_count': len(frontier_7d),
            'memory_analysis': memory_analysis,
            'circuit_analysis': circuit_analysis,
            'variance_analysis': variance_analysis,
            'constraints': constraints,
            'fully_compliant_count': len(fully_compliant),
            'frontier_compliant': frontier_compliant,
            'frontier_compliant_count': len(frontier_compliant),
            'recommendation': (
                recommendation.agent_id if recommendation else None
            ),
        }
        # --- Enhancement additions ---
        result.update({
            'recommendation_point': recommendation,
            'explanation': explanation.to_dict() if explanation else None,
            'hitl_required': hitl_required,
            'hitl_approved': hitl_approved,
            'provenance_summary': provenance_summary,
            'federated_aggregate': self.federated.aggregate(),
            'dimensions': list(self.dimensions),
            'dominance_tolerance': self.dominance_tolerance,
        })
        return result

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def analyze_detailed(
        self,
        agents: List[ExtendedParetoPoint],
        constraints: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Same as comprehensive_analysis but fully JSON-safe.

        Replaces each ExtendedParetoPoint with its to_dict() form.
        """
        report = self.comprehensive_analysis(agents, constraints)

        def _serialize(v: Any) -> Any:
            if isinstance(v, ExtendedParetoPoint):
                return v.to_dict()
            if isinstance(v, list):
                return [_serialize(x) for x in v]
            if isinstance(v, dict):
                return {k: _serialize(x) for k, x in v.items()}
            if isinstance(v, datetime):
                return v.isoformat()
            return v

        return _serialize(report)

    def set_hitl_callback(
        self,
        callback: Callable[[ExtendedParetoPoint, FrontierExplanation], bool],
    ) -> None:
        """Register a HITL callback for frontier recommendations."""
        self._hitl_callback = callback

    def contribute_federated(self) -> None:
        """Explicitly flush federated contributions."""
        logger.debug(
            f"Federated profiles: {len(self.federated.profiles)}"
        )

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate()

    def to_decision_records(
        self,
        agents: List[ExtendedParetoPoint],
        *,
        policy_version: str = "",
    ) -> List[Any]:
        """Emit a DecisionRecord for every agent."""
        records = []
        for a in agents:
            r = a.to_decision_record(policy_version=policy_version)
            if r is not None:
                records.append(r)
        return records

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative analyzer statistics."""
        stats = get_statistics()
        stats.update({
            "deployment_id": self.deployment_id,
            "dimensions": list(self.dimensions),
            "dominance_tolerance": self.dominance_tolerance,
            "federated_profiles_pushed": len(self.federated.profiles),
        })
        return stats
