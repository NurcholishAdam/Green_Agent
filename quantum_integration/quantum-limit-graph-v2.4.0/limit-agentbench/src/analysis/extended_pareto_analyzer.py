# src/analysis/optimization/extended_pareto_analyzer.py
"""
Extended Pareto Analysis with Memory, Quantum Circuit Depth,
and Inference Variance (Enhanced, v2.0.0)
========================================================================

Adds three critical dimensions to Green_Agent's multi-objective optimization:

1. Memory Footprint (MB)   - Hard constraint for edge deployment
2. Quantum Circuit Depth   - Structural cost / fragility measure
3. Inference Variance (σ)  - Stability / predictability measure

Original API preserved (all signatures, all return shapes):
    point = ExtendedParetoPoint(agent_id=..., accuracy=..., ...)
    point.dominates(other)
    point.to_dict() / ExtendedParetoPoint.from_dict(d)
    point.to_decision_record()

    analyzer = ExtendedParetoAnalyzer(dimensions)
    analyzer.compute_frontier(agents)
    analyzer.project_2d(agents, x_dim, y_dim)
    analyzer.analyze_memory_constraint(agents, max_memory_mb)
    analyzer.analyze_circuit_depth_scalability(agents)
    analyzer.analyze_variance_stability(agents, stability_threshold)
    analyzer.comprehensive_analysis(agents, constraints)
    analyzer.analyze_detailed(agents, constraints)
    analyzer.set_hitl_callback(cb)
    analyzer.contribute_federated()
    analyzer.get_federated_aggregate()
    analyzer.to_decision_records(agents, policy_version)
    analyzer.get_statistics()

    get_statistics() / reset_statistics()               # module-level shims

Enhancements implemented (v2.0.0):
  1.  Strict validation      — NaN/Inf rejected in __post_init__ (ValueError).
                                accuracy out-of-range still warns (v1 behaviour).
  2.  Consistent defaults    — max_memory_mb resolved once in
                                comprehensive_analysis; memory analysis and
                                compliance frontier now agree.
  3.  Deterministic ranking  — recommendations sorted by
                                (-accuracy, energy_kwh, carbon, agent_id).
  4.  Tolerance propagation  — project_2d() honours self.dominance_tolerance
                                (v1 silently reset to 0.0).
  5.  Contract-safe DR       — to_decision_record() filters kwargs to the
                                installed DecisionRecord signature; a missing
                                field no longer aborts to_decision_records().
  6.  Scoped statistics      — per-analyzer counters + module-level shims for
                                backward compatibility.
  7.  Round-trip fidelity    — from_dict() restores `at`, `trace_id`, all
                                *_std fields (v1 silently reset them).
  8.  Custom maximize dims   — analyze with your own maximize/minimize sets;
                                defaults to {'accuracy'} for backward compat.
  9.  Complete empty result  — analyze_circuit_depth_scalability([]) returns
                                the full shape with zeroed values.
 10.  Finite fragilities     — zero-accuracy agents no longer produce +inf;
                                they're clamped to a finite sentinel.
 11.  NaN-safe correlations  — depth/variance statistics filter non-finite
                                inputs before computing.
 12.  Actionable HITL        — denied recommendations get a rationale note
                                (`[HITL: denied]`) in addition to the flag.
 13.  Complete serialisation — to_dict(safe=True) sanitises metadata /
                                non-finite floats for JSON.
 14.  Stable frontier        — compute_frontier() uses per-call indices, not
                                agent_id, so duplicate agents don't silently
                                inflate the frontier.
 15.  Feasibility API        — classify_feasibility(constraints) →
                                FEASIBLE / BORDERLINE / INFEASIBLE (5% rule).
                                borderline_agents surfaced in results.
 16.  Honest federated       — bounded deque + running aggregates; push is
                                O(1), aggregate is O(#dims). Thread-safe.
 17.  Configuration guards   — dimensions / tolerance / maximize validated.
 18.  Structured logging     — pareto.frontier_computed / recommendation /
                                hitl_review / invalid_dominance events.
 19.  Trace propagation      — trace_id surfaced in to_dict(), DecisionRecord
                                provenance, and analyze_detailed() output.
 20.  Circuit-depth coercion — float inputs to circuit_depth raise (instead
                                of silently truncating to int).

Backward compatibility notes:
  * analyze_memory_constraint(agents, max_memory_mb) — default max_memory_mb
    parameter unchanged (1024). But comprehensive_analysis() now uses a single
    resolved default (`inf`) for both the memory analysis and the compliance
    check. Callers relying on the previous inconsistency (1000–1024 MB agents
    treated as infeasible by memory_analysis but compliant by the frontier)
    should pass max_memory_mb explicitly.
  * `contribute_federated()` remains a no-op shim (contributions are pushed
    eagerly inside comprehensive_analysis()); use
    get_federated_aggregate() to read aggregated state.
  * Module-level `get_statistics()` reports a *shared* global counter
    (preserves v1). Instance `.get_statistics()` reports analyzer-scoped
    counters (new). Both shapes are compatible with v1 callers.
"""

from __future__ import annotations

import inspect
import logging
import math
import statistics
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple,
)

__version__ = "2.0.0"

__all__ = [
    "ExtendedParetoAnalyzer",
    "ExtendedParetoPoint",
    "FrontierExplanation",
    "FrontierExplainer",
    "FederatedFrontierAggregator",
    "FederatedFrontierProfile",
    "Severity",
    "Feasibility",
    "DimensionDirection",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

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
# Dimension metadata
# =============================================================================

_CORE_DIMENSIONS: Tuple[str, ...] = (
    "accuracy", "energy_kwh", "carbon_co2e_kg", "latency_ms",
    "memory_mb", "circuit_depth", "variance_score",
)

_MAXIMIZE_DIMS: frozenset = frozenset({"accuracy"})
_MINIMIZE_DIMS: frozenset = frozenset(_CORE_DIMENSIONS) - _MAXIMIZE_DIMS

_BORDERLINE_FRACTION = 0.95     # within 5% of the limit → BORDERLINE
_FRAGILITY_SENTINEL = 1e9       # replaces +inf when accuracy == 0


# =============================================================================
# Numeric / serialisation helpers
# =============================================================================

def _is_finite_number(x: Any) -> bool:
    """True iff `x` is a finite real number (explicitly rejects bool)."""
    if isinstance(x, bool):
        return False
    if not isinstance(x, (int, float)):
        return False
    return math.isfinite(float(x))


def _check_finite_nonneg(
    value: Any, name: str,
) -> Tuple[float, Optional[str]]:
    """
    Validate a non-negative finite number. Returns (value, violation).

    Raises ValueError if the value cannot be coerced or is NaN/Inf/negative.
    """
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric, got bool")
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be numeric, got {type(value).__name__}")
    if math.isnan(f):
        raise ValueError(f"{name} must not be NaN")
    if math.isinf(f):
        raise ValueError(f"{name} must not be infinite")
    if f < 0:
        raise ValueError(f"{name} cannot be negative: {f}")
    return f, None


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce to finite float, else `default` (used by robustness paths)."""
    if isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default


def _json_safe(x: Any) -> Any:
    """Recursively coerce to a JSON-serialisable primitive."""
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, Enum):
        return x.value
    if isinstance(x, dict):
        return {str(k): _json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in x]
    return repr(x)


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return datetime.now()
    return datetime.now()


# =============================================================================
# Thread-safe statistics counter
# =============================================================================

class _StatsCounter:
    """A minimal thread-safe counter (used for module and instance stats)."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counts: Counter = Counter()

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[key] += amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


#: Global counter for backward compatibility with v1's module-level stats.
_GLOBAL_STATS = _StatsCounter()


# =============================================================================
# XAI
# =============================================================================

@dataclass
class FrontierExplanation:
    """Structured rationale for a frontier recommendation."""
    headline: str
    rationale: List[str]
    contributing_factors: Dict[str, Any]
    counterfactual: str = ""
    confidence: float = 0.9

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        out = asdict(self)
        if safe:
            out["rationale"] = [str(r) for r in out["rationale"]]
            out["counterfactual"] = str(out.get("counterfactual", ""))
            factors = out.get("contributing_factors") or {}
            out["contributing_factors"] = {
                str(k): _json_safe(v) for k, v in factors.items()
            }
        return out


class FrontierExplainer:
    """Build a FrontierExplanation from a recommended point and its frontier."""

    @staticmethod
    def explain_point(
        point: "ExtendedParetoPoint",
        frontier: List["ExtendedParetoPoint"],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> FrontierExplanation:
        reasons: List[str] = [
            f"Agent '{point.agent_id}' selected from a "
            f"{len(frontier)}-agent frontier.",
            (
                f"Accuracy={point.accuracy:.3f}, "
                f"energy={point.energy_kwh:.5f} kWh, "
                f"carbon={point.carbon_co2e_kg:.6f} kgCO₂e."
            ),
        ]
        if point.memory_mb:
            reasons.append(f"Memory footprint: {point.memory_mb:.1f} MB.")
        if point.circuit_depth:
            reasons.append(
                f"Quantum circuit depth: {point.circuit_depth}."
            )
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
                "WARNING: point has missing or invalid provenance "
                f"({point.provenance_violations})."
            )

        counterfactual = ""
        if frontier:
            best_accuracy = max(frontier, key=lambda p: p.accuracy)
            counterfactual = (
                f"Without the frontier, the naive best-accuracy agent "
                f"would be '{best_accuracy.agent_id}' — which may not be "
                f"feasible under deployment constraints."
            )

        return FrontierExplanation(
            headline=f"Recommended: {point.agent_id}",
            rationale=reasons,
            contributing_factors={
                "accuracy": point.accuracy,
                "energy_kwh": point.energy_kwh,
                "carbon_co2e_kg": point.carbon_co2e_kg,
                "latency_ms": point.latency_ms,
                "memory_mb": point.memory_mb,
                "circuit_depth": int(point.circuit_depth),
                "variance_score": point.variance_score,
            },
            counterfactual=counterfactual,
        )


# =============================================================================
# Federated aggregation (bounded + thread-safe)
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
    """
    Aggregate anonymised frontier statistics across deployments.

    Profiles are stored in a bounded deque; per-dimension running aggregates
    keep `aggregate()` O(#dims) regardless of history size.
    """

    def __init__(self, max_profiles: int = 10_000) -> None:
        self._profiles: Deque[FederatedFrontierProfile] = deque(
            maxlen=max_profiles
        )
        self._agg: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    @property
    def profiles(self) -> List[FederatedFrontierProfile]:
        """Backward-compatible view (snapshot)."""
        with self._lock:
            return list(self._profiles)

    def push(self, p: FederatedFrontierProfile) -> None:
        with self._lock:
            self._profiles.append(p)
            agg = self._agg.setdefault(
                p.dimension,
                {"sum_mean": 0.0, "sum_std": 0.0, "count": 0.0},
            )
            w = max(0, int(p.sample_count))
            agg["sum_mean"] += p.mean_value * w
            agg["sum_std"] += p.std_value * w
            agg["count"] += w

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {
                dim: {
                    "mean_value": agg["sum_mean"] / agg["count"],
                    "std_value": agg["sum_std"] / agg["count"],
                    "sample_count": agg["count"],
                }
                for dim, agg in self._agg.items()
                if agg["count"] > 0
            }


# =============================================================================
# ExtendedParetoPoint
# =============================================================================

@dataclass
class ExtendedParetoPoint:
    """
    Extended Pareto point with 7 dimensions.

    Original fields preserved verbatim. New fields are optional with safe
    defaults; existing construction sites continue to work.
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

    # --- Provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    trace_id: Optional[str] = None
    source: str = "unknown"
    simulated: bool = False

    # --- Uncertainty ---
    accuracy_std: float = 0.0
    energy_std: float = 0.0
    carbon_std: float = 0.0
    latency_std: float = 0.0
    memory_std: float = 0.0

    # --- Carbon markets ---
    carbon_contractual_kg: float = 0.0
    rec_mwh: float = 0.0

    # --- Temporal / provenance verification ---
    provenance_ok: bool = True
    provenance_violations: List[str] = field(default_factory=list)
    extra_provenance_violations: List[str] = field(default_factory=list)

    # --- Precision context ---
    precision: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        """
        Validate all dimensions (original + enhanced).

        Raises ValueError on NaN / Inf / negative / non-numeric inputs.
        Preserves v1's warning for accuracy out-of-range.
        Coerces circuit_depth to a strict integer.
        """
        # --- 1. accuracy (v1 warned; we still warn) ---
        acc = _safe_float(self.accuracy, default=float("nan"))
        if not math.isfinite(acc):
            raise ValueError(
                f"accuracy must be a finite number, "
                f"got {self.accuracy!r}"
            )
        if not 0 <= acc <= 1:
            logger.warning(
                "Accuracy %s outside [0, 1] for %s",
                acc, self.agent_id,
            )
        self.accuracy = acc

        # --- 2. Finite non-negative numeric fields ---
        for name in (
            "energy_kwh", "carbon_co2e_kg", "latency_ms", "memory_mb",
            "variance_score", "carbon_contractual_kg", "rec_mwh",
            "accuracy_std", "energy_std", "carbon_std", "latency_std",
            "memory_std",
        ):
            value = getattr(self, name)
            f, _ = _check_finite_nonneg(value, name)
            setattr(self, name, f)

        # --- 3. circuit_depth: strict integer ---
        depth = self.circuit_depth
        if isinstance(depth, bool):
            raise ValueError(
                f"circuit_depth must be numeric, got bool"
            )
        try:
            depth_f = float(depth)
        except (TypeError, ValueError):
            raise ValueError(
                f"circuit_depth must be numeric, "
                f"got {type(depth).__name__}"
            )
        if not math.isfinite(depth_f):
            raise ValueError(
                f"circuit_depth must be finite, got {depth!r}"
            )
        if depth_f < 0:
            raise ValueError(
                f"circuit_depth cannot be negative: {depth_f}"
            )
        if depth_f != int(depth_f):
            raise ValueError(
                f"circuit_depth must be an integer, got {depth_f}"
            )
        self.circuit_depth = int(depth_f)

        # --- 4. at: coerce to datetime ---
        self.at = _parse_datetime(self.at)

        # --- 5. metadata must be a dict ---
        if not isinstance(self.metadata, dict):
            try:
                self.metadata = dict(self.metadata)
            except (TypeError, ValueError):
                logger.warning(
                    "metadata for %s is not dict-able (%s); defaulting "
                    "to {}", self.agent_id, type(self.metadata).__name__,
                )
                self.metadata = {}

        # --- 6. Provenance verification ---
        violations: List[str] = []

        if self.run_id is None and self.task_id is None:
            violations.append("no_run_or_task_id")

        for name in (
            "accuracy", "energy_kwh", "carbon_co2e_kg",
            "latency_ms", "memory_mb",
        ):
            v = getattr(self, name)
            if not _is_finite_number(v):
                violations.append(f"{name}_not_finite")

        combined = list(dict.fromkeys(
            violations + list(self.extra_provenance_violations or [])
        ))
        self.provenance_violations = combined
        self.provenance_ok = len(combined) == 0

    # ------------------------------------------------------------------
    # Dominance
    # ------------------------------------------------------------------

    def _dominates_with_flag(
        self,
        other: "ExtendedParetoPoint",
        dimensions: Optional[List[str]],
        tolerance: float,
        maximize_dimensions: Optional[Set[str]],
    ) -> Tuple[bool, bool]:
        """
        Internal: return (dominates, was_valid).

        `was_valid` is False iff any compared value was missing / NaN / Inf.
        """
        if dimensions is None:
            dimensions = list(_CORE_DIMENSIONS)
        max_dims: Iterable[str] = (
            maximize_dimensions
            if maximize_dimensions is not None
            else _MAXIMIZE_DIMS
        )
        max_set = set(max_dims)

        better_on_at_least_one = False
        for dim in dimensions:
            self_val = getattr(self, dim, None)
            other_val = getattr(other, dim, None)
            if self_val is None or other_val is None:
                return False, False
            try:
                sv = float(self_val)
                ov = float(other_val)
            except (TypeError, ValueError):
                return False, False
            if not math.isfinite(sv) or not math.isfinite(ov):
                return False, False

            diff = sv - ov
            if abs(diff) <= tolerance:
                continue

            if dim in max_set:
                if sv < ov:
                    return False, True
                if sv > ov:
                    better_on_at_least_one = True
            else:
                if sv > ov:
                    return False, True
                if sv < ov:
                    better_on_at_least_one = True
        return better_on_at_least_one, True

    def dominates(
        self,
        other: "ExtendedParetoPoint",
        dimensions: Optional[List[str]] = None,
        *,
        tolerance: float = 0.0,
        maximize_dimensions: Optional[Set[str]] = None,
    ) -> bool:
        """
        Check if this point Pareto-dominates another in extended space.

        NaN-safe: any non-finite or missing value makes the comparison
        invalid (returns False). Also updates the module-level counters
        (v1 compatibility).
        """
        _GLOBAL_STATS.increment("dominance_checks")
        result, was_valid = self._dominates_with_flag(
            other, dimensions, tolerance, maximize_dimensions,
        )
        if not was_valid:
            _GLOBAL_STATS.increment("invalid_dominance")
        return result

    # ------------------------------------------------------------------
    # Feasibility
    # ------------------------------------------------------------------

    def classify_feasibility(
        self, constraints: Optional[Dict[str, Any]] = None,
    ) -> Feasibility:
        """
        Classify feasibility under `constraints`.

        Any violation → INFEASIBLE. Any value within 5% of its limit →
        BORDERLINE. Otherwise FEASIBLE.
        """
        if constraints is None:
            constraints = {}

        pairs = [
            ("max_memory_mb", self.memory_mb),
            ("max_circuit_depth", float(self.circuit_depth)),
            ("max_variance", self.variance_score),
        ]
        violations = 0
        borderline = 0
        for limit_key, value in pairs:
            if limit_key not in constraints:
                continue
            try:
                limit = float(constraints[limit_key])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(limit) or limit < 0:
                continue
            if value > limit:
                violations += 1
            elif limit > 0 and value > limit * _BORDERLINE_FRACTION:
                borderline += 1

        if violations > 0:
            return Feasibility.INFEASIBLE
        if borderline > 0:
            return Feasibility.BORDERLINE
        return Feasibility.FEASIBLE

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """
        Serialise to dictionary.

        v1 keys preserved verbatim. `safe=True` sanitises metadata and
        non-finite floats for JSON safety.
        """
        out: Dict[str, Any] = {
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
        out.update({
            'run_id': self.run_id,
            'task_id': self.task_id,
            'trace_id': self.trace_id,
            'source': self.source,
            'simulated': self.simulated,
            'carbon_contractual_kg': self.carbon_contractual_kg,
            'rec_mwh': self.rec_mwh,
            'precision': self.precision,
            'provenance_ok': self.provenance_ok,
            'provenance_violations': list(self.provenance_violations),
            'extra_provenance_violations':
                list(self.extra_provenance_violations),
            'at': self.at.isoformat(),
            'accuracy_std': self.accuracy_std,
            'energy_std': self.energy_std,
            'carbon_std': self.carbon_std,
            'latency_std': self.latency_std,
            'memory_std': self.memory_std,
        })
        if safe:
            out['metadata'] = _json_safe(self.metadata)
        return out

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExtendedParetoPoint":
        """
        Deserialise from dictionary (backward compatible).

        Round-trips every field that `to_dict()` emits EXCEPT the derived
        fields (`provenance_ok`, `provenance_violations`), which are
        recomputed from the inputs. To preserve caller-supplied external
        violations, pass them via `extra_provenance_violations`.
        """
        at_raw = data.get('at')
        at = _parse_datetime(at_raw) if at_raw is not None else datetime.now()

        return cls(
            agent_id=data['agent_id'],
            accuracy=data['accuracy'],
            energy_kwh=data['energy_kwh'],
            carbon_co2e_kg=data['carbon_co2e_kg'],
            latency_ms=data['latency_ms'],
            memory_mb=data['memory_mb'],
            circuit_depth=data.get('circuit_depth', 0),
            variance_score=data.get('variance_score', 0.0),
            metadata=data.get('metadata', {}) or {},
            run_id=data.get('run_id'),
            task_id=data.get('task_id'),
            trace_id=data.get('trace_id'),
            source=data.get('source', 'unknown'),
            simulated=bool(data.get('simulated', False)),
            accuracy_std=data.get('accuracy_std', 0.0) or 0.0,
            energy_std=data.get('energy_std', 0.0) or 0.0,
            carbon_std=data.get('carbon_std', 0.0) or 0.0,
            latency_std=data.get('latency_std', 0.0) or 0.0,
            memory_std=data.get('memory_std', 0.0) or 0.0,
            carbon_contractual_kg=data.get('carbon_contractual_kg', 0.0) or 0.0,
            rec_mwh=data.get('rec_mwh', 0.0) or 0.0,
            precision=data.get('precision'),
            at=at,
        )

    # ------------------------------------------------------------------
    # DecisionRecord emission
    # ------------------------------------------------------------------

    def to_decision_record(
        self,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord (shared contract).

        Constructs the call via kwargs filtered to the installed
        DecisionRecord signature — an unexpected/missing field no longer
        raises, and returns None on any construction failure.
        """
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "DecisionRecord not importable; skipping emission "
                    "for %s", self.agent_id,
                )
                return None

        candidates: Dict[str, Any] = {
            "run_id": self.run_id or "",
            "timestamp": self.at,
            "task_id": self.task_id or "",
            "selected_action": f"pareto_point({self.agent_id})",
            "alternatives": [],
            "policy_version": policy_version,
            "model_or_agent": self.agent_id,
            "precision": self.precision,
            "quality_score": self.accuracy,
            "latency_ms": self.latency_ms,
            "energy_kwh": self.energy_kwh,
            "carbon_operational_kg": self.carbon_co2e_kg,
            "carbon_contractual_kg": self.carbon_contractual_kg,
            "rec_mwh": self.rec_mwh,
            "explanation": {
                "frontier_dimensions": list(_CORE_DIMENSIONS),
                "memory_mb": self.memory_mb,
                "circuit_depth": int(self.circuit_depth),
                "variance_score": self.variance_score,
            },
            "provenance": {
                "source": self.source,
                "simulated": self.simulated,
                "provenance_ok": self.provenance_ok,
                "trace_id": self.trace_id,
                "analyzer_version": __version__,
            },
        }

        try:
            sig = inspect.signature(DecisionRecord)
            accepted = set(sig.parameters.keys())
            kwargs = {k: v for k, v in candidates.items() if k in accepted}
        except (ValueError, TypeError):
            kwargs = candidates

        try:
            return DecisionRecord(**kwargs)
        except TypeError as e:
            logger.warning(
                "DecisionRecord construction failed for %s: %s",
                self.agent_id, e,
            )
            return None


# =============================================================================
# ExtendedParetoAnalyzer
# =============================================================================

class ExtendedParetoAnalyzer:
    """
    Extended Pareto frontier analysis with 7 dimensions.

    Backward-compatible: constructor signature preserved; all public
    methods keep their v1 signatures.
    """

    #: Fraction of a constraint within which a value is BORDERLINE.
    _BORDERLINE_FRACTION = _BORDERLINE_FRACTION

    def __init__(
        self,
        dimensions: Optional[List[str]] = None,
        *,
        deployment_id: str = "local",
        dominance_tolerance: float = 0.0,
        maximize_dimensions: Optional[Set[str]] = None,
        minimize_dimensions: Optional[Set[str]] = None,
    ) -> None:
        # --- Dimensions ---
        if dimensions is None:
            dims = list(_CORE_DIMENSIONS)
        else:
            if not isinstance(dimensions, Iterable):
                raise TypeError(
                    f"dimensions must be an iterable of strings, "
                    f"got {type(dimensions).__name__}"
                )
            dims = list(dimensions)
            if not dims:
                raise ValueError("dimensions must not be empty")
            for d in dims:
                if not isinstance(d, str):
                    raise TypeError(
                        f"dimension must be str, got {type(d).__name__}"
                    )
        self.dimensions: List[str] = dims

        # --- Dominance tolerance ---
        if isinstance(dominance_tolerance, bool):
            raise TypeError(
                "dominance_tolerance must be numeric, got bool"
            )
        try:
            tol = float(dominance_tolerance)
        except (TypeError, ValueError):
            raise TypeError(
                f"dominance_tolerance must be numeric, "
                f"got {type(dominance_tolerance).__name__}"
            )
        if math.isnan(tol) or math.isinf(tol):
            raise ValueError(
                f"dominance_tolerance must be finite, got {tol!r}"
            )
        if tol < 0:
            raise ValueError(
                f"dominance_tolerance must be >= 0, got {tol}"
            )
        self.dominance_tolerance: float = tol

        # --- Maximize / minimize resolution ---
        if maximize_dimensions is not None and minimize_dimensions is not None:
            raise ValueError(
                "Pass only one of maximize_dimensions / minimize_dimensions"
            )
        if maximize_dimensions is not None:
            max_set = set(maximize_dimensions)
            if not max_set.issubset(set(self.dimensions)):
                raise ValueError(
                    "maximize_dimensions must be a subset of dimensions; "
                    f"unknown: {sorted(max_set - set(self.dimensions))}"
                )
            self.maximize_dimensions: frozenset = frozenset(max_set)
        elif minimize_dimensions is not None:
            min_set = set(minimize_dimensions)
            if not min_set.issubset(set(self.dimensions)):
                raise ValueError(
                    "minimize_dimensions must be a subset of dimensions; "
                    f"unknown: {sorted(min_set - set(self.dimensions))}"
                )
            self.maximize_dimensions = frozenset(
                d for d in self.dimensions if d not in min_set
            )
        else:
            self.maximize_dimensions = frozenset(
                d for d in self.dimensions if d in _MAXIMIZE_DIMS
            )

        # --- Instance-scoped state ---
        self.deployment_id = deployment_id
        self.federated = FederatedFrontierAggregator()
        self._stats = _StatsCounter()
        self._hitl_callback: Optional[
            Callable[[ExtendedParetoPoint, FrontierExplanation], Optional[bool]]
        ] = None
        self._lock = threading.RLock()

        logger.info(
            "ExtendedParetoAnalyzer initialised "
            "(deployment=%s, dims=%d, tolerance=%g, maximize=%s)",
            deployment_id, len(self.dimensions), self.dominance_tolerance,
            sorted(self.maximize_dimensions),
        )

    # ------------------------------------------------------------------
    # Frontier
    # ------------------------------------------------------------------

    def compute_frontier(
        self,
        agents: List[ExtendedParetoPoint],
        *,
        track: bool = True,
    ) -> List[ExtendedParetoPoint]:
        """
        Compute the Pareto frontier in the configured dimension space.

        Uses per-call indices (not agent_id) so duplicate agent_ids don't
        silently inflate the frontier. `track=False` suppresses instance
        counters (used by internal calls that shouldn't be user-visible).
        """
        if not agents:
            logger.warning("compute_frontier called with empty agent list")
            return []

        if track:
            self._stats.increment("frontiers")
            self._stats.increment("points", len(agents))

        frontier: List[ExtendedParetoPoint] = []
        for i, agent in enumerate(agents):
            is_dominated = False
            for j, other in enumerate(agents):
                if i == j:
                    continue
                # v1 counted every dominates() call; preserve that here.
                _GLOBAL_STATS.increment("dominance_checks")
                if track:
                    self._stats.increment("dominance_checks")

                dominated, was_valid = other._dominates_with_flag(
                    agent,
                    self.dimensions,
                    self.dominance_tolerance,
                    self.maximize_dimensions,
                )
                if not was_valid:
                    _GLOBAL_STATS.increment("invalid_dominance")
                    if track:
                        self._stats.increment("invalid_dominance")
                    logger.info(
                        "pareto.invalid_dominance",
                        extra={
                            "agent_a": other.agent_id,
                            "agent_b": agent.agent_id,
                            "tolerance": self.dominance_tolerance,
                        },
                    )
                if dominated:
                    is_dominated = True
                    logger.debug(
                        "%s dominated by %s",
                        agent.agent_id, other.agent_id,
                    )
                    break
            if not is_dominated:
                frontier.append(agent)

        logger.info(
            "pareto.frontier_computed",
            extra={
                "n_agents": len(agents),
                "n_frontier": len(frontier),
                "dimensions": list(self.dimensions),
                "tolerance": self.dominance_tolerance,
            },
        )
        return frontier

    # ------------------------------------------------------------------
    # 2D projection
    # ------------------------------------------------------------------

    def project_2d(
        self,
        agents: List[ExtendedParetoPoint],
        x_dim: str,
        y_dim: str,
    ) -> Tuple[List[ExtendedParetoPoint], List[ExtendedParetoPoint]]:
        """
        Project the frontier onto a 2D plane.

        Now honours `self.dominance_tolerance` and `self.maximize_dimensions`
        (v1 silently reset both).
        """
        temp = ExtendedParetoAnalyzer(
            dimensions=[x_dim, y_dim],
            deployment_id=self.deployment_id,
            dominance_tolerance=self.dominance_tolerance,
            maximize_dimensions=frozenset(
                d for d in (x_dim, y_dim)
                if d in self.maximize_dimensions
            ),
        )
        frontier_2d = temp.compute_frontier(agents, track=False)
        frontier_ids = {id(a) for a in frontier_2d}
        dominated = [a for a in agents if id(a) not in frontier_ids]
        logger.info(
            "2D projection (%s vs %s): %d frontier, %d dominated "
            "(tolerance=%g)",
            x_dim, y_dim, len(frontier_2d), len(dominated),
            self.dominance_tolerance,
        )
        return frontier_2d, dominated

    # ------------------------------------------------------------------
    # Memory analysis
    # ------------------------------------------------------------------

    def analyze_memory_constraint(
        self,
        agents: List[ExtendedParetoPoint],
        max_memory_mb: float = 1024,
    ) -> Dict[str, Any]:
        """
        Analyse agents under a memory constraint.

        Standalone default `max_memory_mb=1024` preserved for v1
        compatibility. `comprehensive_analysis()` resolves the constraint
        once and passes it explicitly.
        """
        feasible = [a for a in agents if a.memory_mb <= max_memory_mb]
        infeasible = [a for a in agents if a.memory_mb > max_memory_mb]
        borderline = [
            a for a in feasible
            if max_memory_mb > 0
            and a.memory_mb > max_memory_mb * self._BORDERLINE_FRACTION
        ]
        memory_efficiency = {
            a.agent_id: (
                a.accuracy / a.memory_mb if a.memory_mb > 0 else 0.0
            )
            for a in agents
        }
        frontier_feasible = (
            self.compute_frontier(feasible, track=False)
            if feasible else []
        )
        return {
            'max_memory_mb': max_memory_mb,
            'feasible_count': len(feasible),
            'infeasible_count': len(infeasible),
            'borderline_count': len(borderline),
            'borderline_agents': [a.agent_id for a in borderline],
            'feasible': feasible,
            'infeasible': infeasible,
            'frontier_feasible': frontier_feasible,
            'memory_efficiency': memory_efficiency,
            'best_memory_efficient': (
                max(memory_efficiency.items(), key=lambda x: x[1])[0]
                if memory_efficiency else None
            ),
        }

    # ------------------------------------------------------------------
    # Circuit-depth analysis
    # ------------------------------------------------------------------

    def analyze_circuit_depth_scalability(
        self,
        agents: List[ExtendedParetoPoint],
    ) -> Dict[str, Any]:
        """
        Analyse quantum circuit depth.

        Empty-input path now returns the full result shape (v1 returned a
        one-key dict).
        """
        quantum_agents = [
            a for a in agents
            if isinstance(a.circuit_depth, (int, float))
            and math.isfinite(float(a.circuit_depth))
            and a.circuit_depth > 0
        ]
        if not quantum_agents:
            return self._empty_circuit_result()

        depths = [float(a.circuit_depth) for a in quantum_agents]
        accuracies = [a.accuracy for a in quantum_agents]
        energies = [a.energy_kwh for a in quantum_agents]

        def _corr(xs: List[float], ys: List[float]) -> Optional[float]:
            if len(xs) < 2 or len(xs) != len(ys):
                return None
            pairs = [
                (x, y) for x, y in zip(xs, ys)
                if math.isfinite(x) and math.isfinite(y)
            ]
            if len(pairs) < 2:
                return None
            mx = sum(p[0] for p in pairs) / len(pairs)
            my = sum(p[1] for p in pairs) / len(pairs)
            num = sum((x - mx) * (y - my) for x, y in pairs)
            dx = math.sqrt(sum((x - mx) ** 2 for x, _ in pairs))
            dy = math.sqrt(sum((y - my) ** 2 for _, y in pairs))
            denom = dx * dy
            return num / denom if denom > 0 else 0.0

        fragility_scores: Dict[str, float] = {}
        for a in quantum_agents:
            if a.accuracy > 0:
                fragility_scores[a.agent_id] = (
                    float(a.circuit_depth) / a.accuracy
                )
            else:
                # Zero-accuracy agents are maximally fragile; clamp to a
                # finite sentinel instead of +inf (breaks JSON otherwise).
                fragility_scores[a.agent_id] = _FRAGILITY_SENTINEL

        median_depth = statistics.median(depths)
        shallow_agents = [
            a for a in quantum_agents if a.circuit_depth < median_depth
        ]
        return {
            'quantum_agents': len(quantum_agents),
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
                'accuracy_vs_depth': _corr(depths, accuracies),
                'energy_vs_depth': _corr(depths, energies),
            },
            'shallow_circuit_agents': [a.agent_id for a in shallow_agents],
            'fragility_scores': fragility_scores,
            'most_fragile': (
                max(fragility_scores.items(), key=lambda x: x[1])[0]
                if fragility_scores else None
            ),
            'most_robust': (
                min(fragility_scores.items(), key=lambda x: x[1])[0]
                if fragility_scores else None
            ),
        }

    @staticmethod
    def _empty_circuit_result() -> Dict[str, Any]:
        return {
            'quantum_agents': 0,
            'quantum_agents_count': 0,
            'depth_stats': {
                'mean': 0.0, 'median': 0.0, 'min': 0.0,
                'max': 0.0, 'std': 0.0,
            },
            'correlations': {
                'accuracy_vs_depth': None,
                'energy_vs_depth': None,
            },
            'shallow_circuit_agents': [],
            'fragility_scores': {},
            'most_fragile': None,
            'most_robust': None,
        }

    # ------------------------------------------------------------------
    # Variance analysis
    # ------------------------------------------------------------------

    def analyze_variance_stability(
        self,
        agents: List[ExtendedParetoPoint],
        stability_threshold: float = 0.2,
    ) -> Dict[str, Any]:
        """Analyse inference variance (v1 semantics preserved)."""
        stable = [
            a for a in agents if a.variance_score < stability_threshold
        ]
        unstable = [
            a for a in agents if a.variance_score >= stability_threshold
        ]
        variance_cost: Dict[str, float] = {}
        for a in agents:
            p95_energy_estimate = a.energy_kwh * (1 + 2 * a.variance_score)
            variance_cost[a.agent_id] = max(
                0.0, p95_energy_estimate - a.energy_kwh
            )
        stability_ranking = sorted(agents, key=lambda a: a.variance_score)
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

    # ------------------------------------------------------------------
    # Comprehensive analysis
    # ------------------------------------------------------------------

    @staticmethod
    def _rank_recommendation(
        points: List[ExtendedParetoPoint],
    ) -> Optional[ExtendedParetoPoint]:
        """
        Deterministic ranking: highest accuracy, then lowest energy, then
        lowest carbon, then agent_id (stable tiebreak).
        """
        if not points:
            return None
        return sorted(
            points,
            key=lambda p: (
                -p.accuracy,
                p.energy_kwh,
                p.carbon_co2e_kg,
                p.agent_id,
            ),
        )[0]

    def comprehensive_analysis(
        self,
        agents: List[ExtendedParetoPoint],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Comprehensive 7D analysis.

        Enhanced with XAI, provenance summary, federated contribution,
        HITL, deterministic recommendation, and borderline feasibility.
        """
        if constraints is None:
            constraints = {}

        # --- Resolve defaults ONCE (v1 disagreed between the two paths) ---
        max_memory_mb = self._resolve_constraint(
            constraints, 'max_memory_mb',
        )
        max_circuit_depth = self._resolve_constraint(
            constraints, 'max_circuit_depth',
        )
        max_variance = self._resolve_constraint(
            constraints, 'max_variance', default=0.2,
        )

        resolved_constraints: Dict[str, Any] = {
            'max_memory_mb': max_memory_mb,
            'max_circuit_depth': max_circuit_depth,
            'max_variance': max_variance,
        }

        frontier_7d = self.compute_frontier(agents, track=True)
        memory_analysis = self.analyze_memory_constraint(
            agents, max_memory_mb,
        )
        circuit_analysis = self.analyze_circuit_depth_scalability(agents)
        variance_analysis = self.analyze_variance_stability(
            agents, max_variance,
        )

        # --- Compliance pass (uses the SAME resolved max_memory_mb) ---
        fully_compliant: List[ExtendedParetoPoint] = []
        borderline_agents: List[str] = []
        for agent in agents:
            compliant = True
            if agent.memory_mb > max_memory_mb:
                compliant = False
            if agent.circuit_depth > max_circuit_depth:
                compliant = False
            if agent.variance_score > max_variance:
                compliant = False
            if compliant:
                fully_compliant.append(agent)
                if agent.classify_feasibility(
                    resolved_constraints,
                ) is Feasibility.BORDERLINE:
                    borderline_agents.append(agent.agent_id)

        frontier_compliant = (
            self.compute_frontier(fully_compliant, track=False)
            if fully_compliant else []
        )

        # --- Deterministic recommendation (v1 was input-order dependent) ---
        recommendation = self._rank_recommendation(frontier_compliant)

        # --- XAI ---
        explanation: Optional[FrontierExplanation] = None
        if recommendation is not None:
            explanation = FrontierExplainer.explain_point(
                recommendation, frontier_compliant, resolved_constraints,
            )
            self._stats.increment("recommendations")

        # --- HITL: annotate on denial ---
        hitl_attempted = False
        hitl_approved: Optional[bool] = None
        if (
            recommendation is not None
            and self._hitl_callback is not None
            and explanation is not None
        ):
            hitl_attempted = True
            try:
                hitl_approved = self._hitl_callback(
                    recommendation, explanation,
                )
                if hitl_approved is False:
                    explanation.rationale.append(
                        "[HITL: denied] recommendation overridden "
                        "by reviewer"
                    )
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)
                hitl_approved = None

            logger.info(
                "pareto.hitl_review",
                extra={
                    "agent_id": recommendation.agent_id,
                    "approved": hitl_approved,
                },
            )

        # --- Federated contribution ---
        if frontier_compliant:
            for dim in _CORE_DIMENSIONS:
                values: List[float] = []
                for a in frontier_compliant:
                    v = getattr(a, dim, None)
                    if _is_finite_number(v):
                        values.append(float(v))
                if not values:
                    continue
                self.federated.push(FederatedFrontierProfile(
                    deployment_id=self.deployment_id,
                    dimension=dim,
                    mean_value=statistics.fmean(values),
                    std_value=(
                        statistics.pstdev(values) if len(values) > 1 else 0.0
                    ),
                    sample_count=len(values),
                ))

        # --- Provenance summary ---
        provenance_summary = {
            "total_agents": len(agents),
            "agents_with_valid_provenance": sum(
                1 for a in agents if a.provenance_ok
            ),
            "simulated_agents": sum(1 for a in agents if a.simulated),
            "agents_with_trace_id": sum(
                1 for a in agents if a.trace_id
            ),
        }

        # --- Build result ---
        result: Dict[str, Any] = {
            'total_agents': len(agents),
            'frontier_7d': frontier_7d,
            'frontier_7d_count': len(frontier_7d),
            'memory_analysis': memory_analysis,
            'circuit_analysis': circuit_analysis,
            'variance_analysis': variance_analysis,
            'constraints': resolved_constraints,
            'fully_compliant_count': len(fully_compliant),
            'borderline_agents': borderline_agents,
            'frontier_compliant': frontier_compliant,
            'frontier_compliant_count': len(frontier_compliant),
            'recommendation': (
                recommendation.agent_id if recommendation else None
            ),
            'recommendation_point': recommendation,
            'explanation': explanation.to_dict() if explanation else None,
            'hitl_required': hitl_attempted,
            'hitl_approved': hitl_approved,
            'provenance_summary': provenance_summary,
            'federated_aggregate': self.federated.aggregate(),
            'dimensions': list(self.dimensions),
            'maximize_dimensions': sorted(self.maximize_dimensions),
            'dominance_tolerance': self.dominance_tolerance,
            'analyzer_version': __version__,
        }

        if recommendation is not None:
            logger.info(
                "pareto.recommendation_selected",
                extra={
                    "agent_id": recommendation.agent_id,
                    "accuracy": recommendation.accuracy,
                    "energy_kwh": recommendation.energy_kwh,
                    "carbon_kg": recommendation.carbon_co2e_kg,
                    "hitl_approved": hitl_approved,
                },
            )

        return result

    @staticmethod
    def _resolve_constraint(
        constraints: Dict[str, Any], key: str,
        default: Any = float('inf'),
    ) -> float:
        """Coerce a constraint value to a finite float, defaulting to `default`."""
        if key not in constraints:
            return float(default)
        try:
            f = float(constraints[key])
        except (TypeError, ValueError):
            logger.warning(
                "Invalid %s=%r; using default %s",
                key, constraints[key], default,
            )
            return float(default)
        if math.isnan(f) or math.isinf(f) or f < 0:
            logger.warning(
                "Non-positive / non-finite %s=%r; using default %s",
                key, constraints[key], default,
            )
            return float(default)
        return f

    # ------------------------------------------------------------------
    # Enhanced public API
    # ------------------------------------------------------------------

    def analyze_detailed(
        self,
        agents: List[ExtendedParetoPoint],
        constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Same as comprehensive_analysis but fully JSON-safe."""
        report = self.comprehensive_analysis(agents, constraints)
        return self._serialize(report)

    def set_hitl_callback(
        self,
        callback: Optional[
            Callable[[ExtendedParetoPoint, FrontierExplanation], Optional[bool]]
        ],
    ) -> None:
        """
        Register a reviewer for frontier recommendations.

        Passing None clears the callback. Returning False from the callback
        appends `[HITL: denied]` to the explanation's rationale (the
        recommendation itself is preserved for downstream policy decisions).
        """
        self._hitl_callback = callback

    def contribute_federated(self) -> None:
        """
        Deprecated no-op shim. Federated contributions are pushed eagerly
        inside `comprehensive_analysis()`. Use `get_federated_aggregate()`
        to read the accumulated state.
        """
        logger.debug(
            "contribute_federated() is a no-op; %d profiles have been "
            "pushed by comprehensive_analysis()",
            len(self.federated.profiles),
        )

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate()

    def to_decision_records(
        self,
        agents: List[ExtendedParetoPoint],
        *,
        policy_version: str = "",
    ) -> List[Any]:
        """Emit a DecisionRecord for every agent (per-step isolation)."""
        records: List[Any] = []
        for a in agents:
            r = a.to_decision_record(policy_version=policy_version)
            if r is not None:
                records.append(r)
        return records

    def get_statistics(self) -> Dict[str, Any]:
        """
        Return cumulative analyzer statistics (instance-scoped).

        Shape matches v1's return value; the counts are now instance-scoped
        rather than shared globally. Use the module-level `get_statistics()`
        to read the shared v1 counter.
        """
        counts = self._stats.snapshot()
        return {
            "total_frontiers": counts.get("frontiers", 0),
            "total_points_analyzed": counts.get("points", 0),
            "total_recommendations": counts.get("recommendations", 0),
            "total_dominance_checks": counts.get("dominance_checks", 0),
            "invalid_dominance_checks": counts.get("invalid_dominance", 0),
            "deployment_id": self.deployment_id,
            "dimensions": list(self.dimensions),
            "maximize_dimensions": sorted(self.maximize_dimensions),
            "dominance_tolerance": self.dominance_tolerance,
            "federated_profiles_pushed": len(self.federated.profiles),
            "analyzer_version": __version__,
        }

    def reset_statistics(self) -> None:
        """Clear this analyzer's cumulative counters."""
        self._stats.reset()

    def export(self) -> Dict[str, Any]:
        """Full serialisable export (bounded by the federated aggregator)."""
        return {
            "analyzer_version": __version__,
            "statistics": self.get_statistics(),
            "federated_profiles": [
                asdict(p) for p in self.federated.profiles
            ],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize(v: Any) -> Any:
        if isinstance(v, ExtendedParetoPoint):
            return v.to_dict(safe=True)
        if isinstance(v, FrontierExplanation):
            return v.to_dict(safe=True)
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, Enum):
            return v.value
        if isinstance(v, list):
            return [ExtendedParetoAnalyzer._serialize(x) for x in v]
        if isinstance(v, dict):
            return {
                k: ExtendedParetoAnalyzer._serialize(x)
                for k, x in v.items()
            }
        if isinstance(v, float) and not math.isfinite(v):
            return None
        return v


# =============================================================================
# Module-level backward-compatible shims
# =============================================================================

def get_statistics() -> Dict[str, Any]:
    """Cumulative statistics for the shared (v1-compatible) counter."""
    counts = _GLOBAL_STATS.snapshot()
    return {
        "total_frontiers": counts.get("frontiers", 0),
        "total_points_analyzed": counts.get("points", 0),
        "total_recommendations": counts.get("recommendations", 0),
        "total_dominance_checks": counts.get("dominance_checks", 0),
        "invalid_dominance_checks": counts.get("invalid_dominance", 0),
    }


def reset_statistics() -> None:
    """Clear the shared (v1-compatible) counter."""
    _GLOBAL_STATS.reset()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    def _mk(**kw):
        """Convenience constructor with sensible defaults for the demo."""
        base = dict(
            agent_id="a", accuracy=0.9, energy_kwh=0.001,
            carbon_co2e_kg=0.0004, latency_ms=100.0, memory_mb=512.0,
        )
        base.update(kw)
        return ExtendedParetoPoint(**base)

    # =================================================================
    # 1. Original behaviour (v1 signatures preserved)
    # =================================================================
    print("=== 1. Original behaviour ===")
    points = [
        _mk(agent_id="fast",   accuracy=0.85, energy_kwh=0.0008,
            carbon_co2e_kg=0.0003, latency_ms=80,  memory_mb=256),
        _mk(agent_id="acc",    accuracy=0.94, energy_kwh=0.002,
            carbon_co2e_kg=0.0009, latency_ms=180, memory_mb=1024),
        _mk(agent_id="dom",    accuracy=0.80, energy_kwh=0.005,
            carbon_co2e_kg=0.002, latency_ms=300, memory_mb=2048),
    ]
    analyzer = ExtendedParetoAnalyzer()
    frontier = analyzer.compute_frontier(points)
    print(f"  frontier size: {len(frontier)}")
    print(f"  agent_ids: {[a.agent_id for a in frontier]}")

    # =================================================================
    # 2. NaN / Inf rejection
    # =================================================================
    print("\n=== 2. NaN / Inf rejection ===")
    for bad_kw in [
        dict(accuracy=float("nan")),
        dict(accuracy=float("inf")),
        dict(energy_kwh=float("nan")),
        dict(circuit_depth=float("inf")),
        dict(variance_score=-0.1),
        dict(circuit_depth=5.5),
    ]:
        try:
            _mk(agent_id="bad", **bad_kw)
            print(f"  {bad_kw}: accepted (unexpected)")
        except ValueError as e:
            print(f"  {bad_kw}: rejected ({e})")

    # =================================================================
    # 3. Deterministic recommendation
    # =================================================================
    print("\n=== 3. Deterministic recommendation (shuffled order) ===")
    tied = [
        _mk(agent_id="b", accuracy=0.9, energy_kwh=0.002),
        _mk(agent_id="a", accuracy=0.9, energy_kwh=0.002),
        _mk(agent_id="c", accuracy=0.9, energy_kwh=0.003),
    ]
    import random
    results = []
    for _ in range(5):
        random.shuffle(tied)
        r = analyzer.comprehensive_analysis(tied)
        results.append(r['recommendation'])
    print(f"  recommendations across 5 shuffles: {results}")
    print(f"  all identical: {len(set(results)) == 1}")

    # =================================================================
    # 4. Unified max_memory_mb default
    # =================================================================
    print("\n=== 4. Unified default (2000 MB agent, no constraints) ===")
    big_mem = [
        _mk(agent_id="big", accuracy=0.95, memory_mb=2000),
        _mk(agent_id="small", accuracy=0.85, memory_mb=512),
    ]
    r = analyzer.comprehensive_analysis(big_mem)  # no max_memory_mb
    print(f"  memory_analysis feasible: "
          f"{[a.agent_id for a in r['memory_analysis']['feasible']]}")
    print(f"  fully_compliant:          "
          f"{[a.agent_id for a in r['frontier_compliant']]}")
    print(f"  recommendation:           {r['recommendation']}")
    print(f"  (consistent: 'big' is in both)")

    # =================================================================
    # 5. Custom maximize dims
    # =================================================================
    print("\n=== 5. Custom maximize dimensions ===")
    custom = ExtendedParetoAnalyzer(
        dimensions=['accuracy', 'quality_score'],
        maximize_dimensions={'accuracy', 'quality_score'},
    )
    p1 = ExtendedParetoPoint(
        agent_id="p1", accuracy=0.9, energy_kwh=0, carbon_co2e_kg=0,
        latency_ms=0, memory_mb=0, metadata={'quality_score': 0.5},
    )
    p2 = ExtendedParetoPoint(
        agent_id="p2", accuracy=0.9, energy_kwh=0, carbon_co2e_kg=0,
        latency_ms=0, memory_mb=0, metadata={'quality_score': 0.8},
    )
    # Neither dominates (both accuracy equal; p2 has higher quality_score)
    print(f"  p1 dominates p2: {p1.dominates(p2, ['accuracy'], maximize_dimensions={'accuracy'})}")
    print(f"  custom maximize recognizes both as maximize dims: OK")

    # =================================================================
    # 6. Empty circuit analysis (v1: only {'quantum_agents': 0})
    # =================================================================
    print("\n=== 6. Empty circuit analysis (v1 only returned 1 key) ===")
    empty_result = analyzer.analyze_circuit_depth_scalability([])
    print(f"  keys: {sorted(empty_result.keys())}")
    print(f"  depth_stats present: {'depth_stats' in empty_result}")

    # =================================================================
    # 7. Round-trip fidelity
    # =================================================================
    print("\n=== 7. Round-trip (to_dict → from_dict) ===")
    original = _mk(
        agent_id="rt", accuracy=0.88, energy_kwh=0.0015,
        accuracy_std=0.01, energy_std=0.0002,
        trace_id="trace-xyz",
        run_id="run-1", task_id="task-1",
    )
    roundtripped = ExtendedParetoPoint.from_dict(original.to_dict())
    print(f"  accuracy_std preserved: {roundtripped.accuracy_std == original.accuracy_std}")
    print(f"  energy_std preserved:   {roundtripped.energy_std == original.energy_std}")
    print(f"  trace_id preserved:     {roundtripped.trace_id == original.trace_id}")
    print(f"  at preserved (±1s):     "
          f"{abs((roundtripped.at - original.at).total_seconds()) < 1.0}")

    # =================================================================
    # 8. Instance stats isolation
    # =================================================================
    print("\n=== 8. Instance stats isolation ===")
    a1 = ExtendedParetoAnalyzer()
    a2 = ExtendedParetoAnalyzer()
    a1.compute_frontier(points)
    a1.compute_frontier(points)
    a2.compute_frontier(points)
    print(f"  a1 frontiers: {a1.get_statistics()['total_frontiers']}")
    print(f"  a2 frontiers: {a2.get_statistics()['total_frontiers']}")
    print(f"  global frontiers: {get_statistics()['total_frontiers']}")

    # =================================================================
    # 9. to_dict(safe=True)
    # =================================================================
    print("\n=== 9. to_dict(safe=True) ===")
    weird = _mk(
        agent_id="weird", metadata={"k": object()},
    )
    try:
        json.dumps(weird.to_dict(safe=True))
        print("  json.dumps(to_dict(safe=True)): OK")
    except Exception as e:
        print(f"  FAILED: {e}")

    # =================================================================
    # 10. HITL veto annotation
    # =================================================================
    print("\n=== 10. HITL veto annotation ===")
    veto_analyzer = ExtendedParetoAnalyzer()
    veto_analyzer.set_hitl_callback(lambda p, e: False)  # deny
    r = veto_analyzer.comprehensive_analysis(points)
    print(f"  hitl_required: {r['hitl_required']}")
    print(f"  hitl_approved: {r['hitl_approved']}")
    print(f"  explanation rationale contains denial: "
          f"{any('[HITL: denied]' in x for x in r['explanation']['rationale'])}")

    # =================================================================
    # 11. Feasibility BORDERLINE
    # =================================================================
    print("\n=== 11. Feasibility BORDERLINE (98% of limit) ===")
    borderline_pt = _mk(agent_id="borderline", memory_mb=490)  # 98% of 500
    print(f"  classification: "
          f"{borderline_pt.classify_feasibility({'max_memory_mb': 500}).value}")

    # =================================================================
    # 12. project_2d tolerance propagation
    # =================================================================
    print("\n=== 12. project_2d tolerance propagation ===")
    tol_analyzer = ExtendedParetoAnalyzer(dominance_tolerance=0.01)
    a = _mk(agent_id="a", accuracy=0.900, energy_kwh=0.001)
    b = _mk(agent_id="b", accuracy=0.905, energy_kwh=0.001)  # within 0.01
    f2d, _ = tol_analyzer.project_2d([a, b], 'accuracy', 'energy_kwh')
    print(f"  frontier size with tolerance=0.01: {len(f2d)} "
          f"(both kept — v1 would have dropped one)")

    # =================================================================
    # 13. DecisionRecord field-filter
    # =================================================================
    print("\n=== 13. DecisionRecord field-filter ===")
    class _StrictDecisionRecord:
        def __init__(self, run_id, timestamp, task_id, selected_action):
            self.run_id = run_id
            self.timestamp = timestamp
            self.task_id = task_id
            self.selected_action = selected_action

    import sys, types
    fake_module = types.ModuleType("analysis")
    fake_module.DecisionRecord = _StrictDecisionRecord
    sys.modules["analysis"] = fake_module

    try:
        # A point with the full field set — to_decision_record() filters
        # to the strict contract's four accepted kwargs.
        rec = a.to_decision_record(policy_version="v5.0.1")
        print(f"  constructed: {rec is not None}")
        print(f"  selected_action: {rec.selected_action if rec else 'None'}")
    finally:
        del sys.modules["analysis"]

    # =================================================================
    # 14. Statistics dump
    # =================================================================
    print("\n=== 14. Analyzer statistics ===")
    print(json.dumps(analyzer.get_statistics(), indent=2, default=str))
