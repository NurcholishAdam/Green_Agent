# src/governance/benchmarks/benchmark_engine.py

"""
Green Agent v5.0.0 - Benchmark Engine v3.0.0 (Enhanced)
========================================================

Layer 9: 3D Benchmarking with governance enforcement.

Provides:
  - **Analytics**: 3D Pareto frontier over Energy × Time × Helium
  - **Governance**: task eligibility, acceptance thresholds,
    anti-gaming checks, reproducibility requirements, metric
    governance, and audit trail integration

v3.0.0 changes vs. v2.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``_efficiency(0.0)`` returns ``None`` (not ``1e12``). The report
        carries ``has_efficiency_data: bool``; a zero metric is now
        distinguishable from a real, extremely-efficient one.
  * 2.2 ``calculate_helium_resilience_score`` normalizes the
        ``helium_zone`` value to its lowercase string before comparison.
        A ``HeliumZone.HELIUM_RED`` enum previously fell through to
        ``resilience=1.0`` (the *best* score for a critical task).
  * 2.3 ``_route_through_pdp`` cannot be overridden by caller input:
        ``submission_context`` can no longer replace the engine's own
        ``metrics`` / ``reasons`` / ``benchmark_id`` in the PDP request.
        Conflict keys are ignored with a WARNING.
  * 2.4 ``_submission_count`` / ``_rejected_count`` are now incremented
        under ``self._lock`` — no more under-reporting on concurrent
        ``submit()`` calls.
  * 2.5 ``submit(None, None)`` is rejected with
        ``"execution_result must not be None"``. Previously it passed
        every check and poisoned the Pareto frontier with ``0,0,0``.
  * 2.6 ``pareto_frontier_size`` is now reported correctly even when
        ``incremental_pareto=False`` — the engine caches
        ``_last_frontier`` regardless of the toggle.
  * 2.7 ``_extract_metrics`` reads both ``energy_consumed_kwh`` and
        ``energy_consumed`` (and their ``_ms`` / ``_kg`` aliases), so
        the engine sees the same values as ``CarbonLedger`` and
        ``AgentNode``.

**P1 robustness**
  * 3.1 ``get_helium_ranking`` includes ``task_id``.
  * 3.2 ``_record_audit`` returns ``None`` on failure (was returning a
        phantom event id).
  * 3.3 ``__post_init__`` validation on ``BenchmarkSubmissionResult``.
  * 3.4 ``__post_init__`` validation on ``MetricDefinition``.
  * 3.5 ``__post_init__`` validation on ``BenchmarkSpec``.
  * 3.6 ``anti_gaming_checks`` drives a real registry
        (``plausibility_bounds``, ``accuracy_range``,
        ``carbon_energy_ratio``).
  * 3.7 ``BenchmarkSpec.from_legacy_dict``.
  * 3.8 ``metrics_hook(event, fields)`` on every state transition.
  * 3.9 PDP failure is surfaced (or fails-closed, configurable).
  * 3.10 ``_incremental_pareto_update`` is truly incremental — skylines
         are updated in O(frontier_size) per submission.
  * 3.12 ``BenchmarkSubmissionResult.simulated`` and ``.source``.
  * 3.13 ``_safe_float`` rejects ``bool``.
  * 3.18 ``BenchmarkReport.helium_resilience_score`` sanitized in
         ``to_dict``.
  * 3.19 Recommendation for empty frontier is not "optimal".
  * 3.20 Pickling, ``__repr__``, context managers, ``__version__``.

Backward compatible: the public API is preserved. New fields are
additive; ``BenchmarkReport.energy_efficiency`` (etc.) are now
``Optional[float]`` — ``None`` when the metric is undefined. Callers
who relied on the old ``1e12`` sentinel should check ``has_efficiency_data``.
"""

from __future__ import annotations

import logging
import math
import threading
import uuid
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime, timezone
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "3.0.0"
BENCHMARK_SCHEMA_VERSION = "5.0.0"


# =============================================================================
# Errors
# =============================================================================

class BenchmarkError(Exception):
    """Base class for every error raised by the benchmark engine."""


class BenchmarkValidationError(BenchmarkError):
    """Raised for invalid benchmark inputs."""


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Parse a numeric field. Rejects bool / NaN / Inf (P1 3.13)."""
    if isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(f):
        return default
    return f


def _safe_number(value: Any) -> Optional[float]:
    """Return a JSON-safe number, or ``None`` for non-finite inputs."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _safe_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
    """Recursively sanitize a mapping for JSON serialization (P1 3.11)."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        out[str(k)] = _safe_value(v)
    return out


def _safe_value(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return _safe_number(v)
    if isinstance(v, Mapping):
        return _safe_dict(v)
    if isinstance(v, (list, tuple)):
        return [_safe_value(x) for x in v]
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def _efficiency(value: Optional[float]) -> Optional[float]:
    """
    Return ``1 / value``, or ``None`` when undefined (P0 2.1).

    ``None`` is used instead of ``1e12`` so downstream consumers can
    distinguish "0 kWh measured" from "0 kWh unknown".
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f <= 0:
        return None
    return 1.0 / f


def _as_aware_utc(dt: datetime) -> datetime:
    """Normalize a datetime to tz-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_zone(value: Any) -> Optional[str]:
    """Normalize a helium-zone value (enum or string) to lowercase (P0 2.2)."""
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value).strip().lower()
    return str(value).strip().lower()


# =============================================================================
# Anti-gaming check registry (P1 3.6)
# =============================================================================

_ANTI_GAMING_CHECKS: Dict[str, Callable] = {}


def _register_check(name: str):
    def deco(fn: Callable) -> Callable:
        _ANTI_GAMING_CHECKS[name] = fn
        return fn
    return deco


@_register_check("plausibility_bounds")
def _check_plausibility(engine: Any, metrics: Mapping[str, float]) -> List[str]:
    reasons: List[str] = []
    for name, definition in engine.spec.metric_definitions.items():
        value = metrics.get(name)
        if value is None:
            continue
        if (definition.min_plausible is not None
                and value < definition.min_plausible):
            reasons.append(
                f"anti-gaming: {name!r}={value} below plausible "
                f"minimum {definition.min_plausible}")
        if (definition.max_plausible is not None
                and value > definition.max_plausible):
            reasons.append(
                f"anti-gaming: {name!r}={value} above plausible "
                f"maximum {definition.max_plausible}")
    return reasons


@_register_check("accuracy_range")
def _check_accuracy_range(engine: Any, metrics: Mapping[str, float]) -> List[str]:
    acc = metrics.get("accuracy")
    if acc is None:
        return []
    if not 0.0 <= acc <= 1.0:
        return [f"anti-gaming: accuracy={acc} outside [0, 1]"]
    return []


@_register_check("carbon_energy_ratio")
def _check_carbon_energy_ratio(engine: Any, metrics: Mapping[str, float]) -> List[str]:
    energy = metrics.get("energy_kwh", 0.0)
    carbon = metrics.get("carbon_kg", 0.0)
    if energy > 0 and carbon > 0:
        intensity = carbon / energy * 1000  # gCO2 / kWh
        if intensity > 2000:
            return [f"anti-gaming: carbon intensity {intensity:.0f} "
                    f"gCO2/kWh exceeds 2000"]
    return []


# =============================================================================
# Governance contracts
# =============================================================================

@dataclass
class MetricDefinition:
    """Canonical definition of a metric governed by the benchmark."""
    name: str
    unit: str
    direction: str                    # "min" | "max"
    description: str = ""
    min_plausible: Optional[float] = None
    max_plausible: Optional[float] = None

    def __post_init__(self) -> None:  # P1 3.4
        if not isinstance(self.name, str) or not self.name:
            raise BenchmarkValidationError(
                "MetricDefinition.name must be a non-empty string")
        if not isinstance(self.unit, str) or not self.unit:
            raise BenchmarkValidationError(
                "MetricDefinition.unit must be a non-empty string")
        if self.direction not in ("min", "max"):
            raise BenchmarkValidationError(
                f"MetricDefinition.direction must be 'min' or 'max', "
                f"got {self.direction!r}")
        for name in ("min_plausible", "max_plausible"):
            v = getattr(self, name)
            if v is None:
                continue
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise BenchmarkValidationError(
                    f"MetricDefinition.{name} must be numeric or None")
            if math.isnan(v) or math.isinf(v):
                raise BenchmarkValidationError(
                    f"MetricDefinition.{name} must be finite")
        if (self.min_plausible is not None
                and self.max_plausible is not None
                and self.min_plausible > self.max_plausible):
            raise BenchmarkValidationError(
                "MetricDefinition.min_plausible > max_plausible")


@dataclass
class ReproducibilityRules:
    """Fields a submission must declare to be reproducible."""
    required_fields: List[str] = field(
        default_factory=lambda: [
            "system_version", "policy_version", "dataset_id",
            "random_seed", "hardware_profile", "region",
        ])

    def validate(self, submission: Mapping[str, Any]) -> List[str]:
        return [
            f for f in self.required_fields
            if f not in submission or submission.get(f) is None
        ]


@dataclass
class BenchmarkSpec:
    """The governance spec for a benchmark."""
    benchmark_id: str
    name: str
    task_eligibility: List[str] = field(default_factory=list)
    metric_definitions: Dict[str, MetricDefinition] = field(default_factory=dict)
    acceptance_thresholds: Dict[str, float] = field(default_factory=dict)
    anti_gaming_checks: List[str] = field(default_factory=list)
    reproducibility: ReproducibilityRules = field(
        default_factory=ReproducibilityRules)
    min_submissions_for_frontier: int = 3
    schema_version: str = BENCHMARK_SCHEMA_VERSION

    def __post_init__(self) -> None:  # P1 3.5
        if not isinstance(self.benchmark_id, str) or not self.benchmark_id:
            raise BenchmarkValidationError(
                "BenchmarkSpec.benchmark_id must be a non-empty string")
        if not isinstance(self.name, str) or not self.name:
            raise BenchmarkValidationError(
                "BenchmarkSpec.name must be a non-empty string")
        if not self.anti_gaming_checks:
            raise BenchmarkValidationError(
                "BenchmarkSpec must declare at least one anti-gaming check")
        if not self.reproducibility.required_fields:
            raise BenchmarkValidationError(
                "BenchmarkSpec must declare reproducibility rules")
        if isinstance(self.min_submissions_for_frontier, bool) or \
                not isinstance(self.min_submissions_for_frontier, int) or \
                self.min_submissions_for_frontier < 0:
            raise BenchmarkValidationError(
                "BenchmarkSpec.min_submissions_for_frontier must be "
                "a non-negative int")
        # Warn on unknown check names — don't reject (backward compat).
        for check_name in self.anti_gaming_checks:
            if check_name not in _ANTI_GAMING_CHECKS:
                logger.warning(
                    "BenchmarkSpec references unknown anti-gaming "
                    "check %r; will be skipped at runtime. "
                    "Known: %s",
                    check_name, sorted(_ANTI_GAMING_CHECKS))

    @classmethod
    def from_legacy_dict(cls, cfg: Optional[Mapping[str, Any]]) -> "BenchmarkSpec":
        """Build from a flat mapping; unknown keys ignored (P1 3.7)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        kwargs: Dict[str, Any] = {}
        for k, v in cfg.items():
            if k not in valid:
                continue
            if k == "anti_gaming_checks" and isinstance(v, list):
                kwargs[k] = list(v)
            elif k == "task_eligibility" and isinstance(v, list):
                kwargs[k] = list(v)
            elif k == "acceptance_thresholds" and isinstance(v, Mapping):
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


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
        default_factory=lambda: datetime.now(timezone.utc))
    schema_version: str = BENCHMARK_SCHEMA_VERSION
    # v3.0.0 additions
    simulated: bool = False
    source: Optional[str] = None
    pdp_verdict: Optional[str] = None

    def __post_init__(self) -> None:  # P1 3.3
        if not isinstance(self.benchmark_id, str) or not self.benchmark_id:
            raise BenchmarkValidationError(
                "BenchmarkSubmissionResult.benchmark_id must be "
                "a non-empty string")
        if not isinstance(self.task_id, str) or not self.task_id:
            raise BenchmarkValidationError(
                "BenchmarkSubmissionResult.task_id must be "
                "a non-empty string")
        if not isinstance(self.passed, bool):
            raise BenchmarkValidationError(
                "BenchmarkSubmissionResult.passed must be a bool")
        if not isinstance(self.metrics, dict):
            raise BenchmarkValidationError(
                "BenchmarkSubmissionResult.metrics must be a dict")
        for k, v in self.metrics.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise BenchmarkValidationError(
                    f"metrics[{k!r}] must be numeric")
            if math.isnan(v) or math.isinf(v):
                raise BenchmarkValidationError(
                    f"metrics[{k!r}] must be finite")
        # Normalize timestamp to UTC
        self.submitted_at = _as_aware_utc(self.submitted_at)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["submitted_at"] = self.submitted_at.isoformat()
        return out


# =============================================================================
# BenchmarkReport (preserved + extended)
# =============================================================================

@dataclass
class BenchmarkReport:
    """
    Enhanced benchmark report with helium metrics.

    ``energy_efficiency``, ``carbon_efficiency``, and
    ``helium_efficiency`` are ``Optional[float]`` — ``None`` when the
    underlying metric is undefined (zero or missing). Use
    ``has_efficiency_data`` to check.
    """
    task_id: str
    energy_efficiency: Optional[float]
    carbon_efficiency: Optional[float]
    helium_efficiency: Optional[float]
    pareto_frontier: List[Dict]
    recommendations: List[str]
    helium_resilience_score: float
    schema_version: str = BENCHMARK_SCHEMA_VERSION
    simulated: bool = False
    policy_version: Optional[str] = None
    system_version: Optional[str] = None
    deployment_id: Optional[str] = None
    agent_id: Optional[str] = None
    precision: Optional[str] = None
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))

    @property
    def has_efficiency_data(self) -> bool:
        """True iff all three efficiency fields are non-None (P0 2.1)."""
        return (
            self.energy_efficiency is not None
            and self.carbon_efficiency is not None
            and self.helium_efficiency is not None
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "energy_efficiency": _safe_number(self.energy_efficiency),
            "carbon_efficiency": _safe_number(self.carbon_efficiency),
            "helium_efficiency": _safe_number(self.helium_efficiency),
            "has_efficiency_data": self.has_efficiency_data,
            "pareto_frontier": [
                _safe_dict(p) for p in (self.pareto_frontier or [])
            ],
            "recommendations": list(self.recommendations),
            "helium_resilience_score": _safe_number(
                self.helium_resilience_score),
            "schema_version": self.schema_version,
            "simulated": self.simulated,
            "policy_version": self.policy_version,
            "system_version": self.system_version,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "precision": self.precision,
            "generated_at": _as_aware_utc(self.generated_at).isoformat(),
        }


# =============================================================================
# HeliumAwareBenchmarkEngine
# =============================================================================

class HeliumAwareBenchmarkEngine:
    """
    3D benchmark engine with governance enforcement.

    See the module docstring for the design principles.
    """

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
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ):
        self.config = config or {}

        # --- Original fields (preserved) ---
        self.metrics_history: List[Dict] = []
        self.pareto_frontier_cache: Dict[str, Any] = {}
        self._last_frontier: List[Dict] = []   # P0 2.6

        # --- Governance spec ---
        self.spec = spec or self._default_spec()

        # --- Governance integration ---
        self._decision_point = decision_point
        self._audit_log = audit_log

        if metrics_hook is not None and not callable(metrics_hook):
            raise BenchmarkValidationError("metrics_hook must be callable")
        self._metrics_hook = metrics_hook

        # --- Feature toggles ---
        self.features: Dict[str, bool] = {
            "governance_enforcement": True,
            "anti_gaming_checks": True,
            "reproducibility_enforcement": True,
            "json_safe_reports": True,
            "incremental_pareto": True,
            "configurable_thresholds": True,
            "reject_all_zero_metrics": False,   # opt-in safety net
            "fail_closed_on_pdp_error": False,  # opt-in strictness
        }
        if features:
            self.features.update(features)

        # --- Recommendation thresholds ---
        self.recommendation_thresholds = dict(
            self.DEFAULT_RECOMMENDATION_THRESHOLDS)
        user_thresholds = self.config.get("recommendation_thresholds")
        if isinstance(user_thresholds, dict):
            self.recommendation_thresholds.update(user_thresholds)

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Statistics (incremented under the lock) ---
        self._submission_count: int = 0
        self._rejected_count: int = 0
        self._truncated_count: int = 0

        logger.info(
            "HeliumAwareBenchmarkEngine v%s initialized "
            "(benchmark=%s, governance=%s)",
            __version__,
            self.spec.benchmark_id,
            "on" if self.features["governance_enforcement"] else "off")

    # ------------------------------------------------------------------
    # Default spec
    # ------------------------------------------------------------------

    @staticmethod
    def _default_spec() -> BenchmarkSpec:
        """A permissive default spec preserving original behavior."""
        return BenchmarkSpec(
            benchmark_id="default",
            name="Default Helium Benchmark",
            task_eligibility=[],
            metric_definitions={
                "energy_kwh": MetricDefinition(
                    name="energy_kwh", unit="kWh", direction="min",
                    min_plausible=0.0, max_plausible=1e6),
                "execution_time_ms": MetricDefinition(
                    name="execution_time_ms", unit="ms", direction="min",
                    min_plausible=0.0, max_plausible=1e9),
                "helium_usage": MetricDefinition(
                    name="helium_usage", unit="units", direction="min",
                    min_plausible=0.0, max_plausible=1000.0),
            },
            acceptance_thresholds={},
            anti_gaming_checks=["plausibility_bounds"],
            reproducibility=ReproducibilityRules(required_fields=[]),
        )

    # ------------------------------------------------------------------
    # Metrics hook (P1 3.8)
    # ------------------------------------------------------------------

    def _emit_metric(self, event: str, fields_: Mapping[str, Any]) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ------------------------------------------------------------------
    # Preserved methods
    # ------------------------------------------------------------------

    def calculate_helium_efficiency(self, execution_result: Any) -> Optional[float]:
        """Tasks per unit helium. Returns ``None`` for undefined (P0 2.1)."""
        helium_usage = _safe_float(
            getattr(execution_result, "helium_usage", 0.0), default=0.0)
        task_complexity = _safe_float(
            getattr(execution_result, "complexity_score", 1.0), default=1.0)
        if helium_usage <= 0:
            return None
        return task_complexity / helium_usage

    def calculate_helium_resilience_score(
        self, execution_result: Any, helium_supply_status: Any,
    ) -> float:
        """Resilience score in [0, 1] (P0 2.2: enum-safe)."""
        helium_zone = _normalize_zone(
            getattr(execution_result, "helium_zone", None))
        fallback_used = bool(
            getattr(execution_result, "fallback_used", False))
        accuracy = _safe_float(
            getattr(execution_result, "accuracy", 0.0), default=0.0)

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

        if helium_supply_status is not None and hasattr(
            helium_supply_status, "scarcity_score"
        ):
            scarcity = _safe_float(
                helium_supply_status.scarcity_score, default=0.0)
            resilience *= (1.0 - scarcity * 0.3)

        return max(0.0, min(1.0, resilience))

    def update_pareto_frontier(
        self, execution_result: Any, helium_supply_status: Any,
    ) -> BenchmarkReport:
        """Update 3D Pareto frontier. Returns a JSON-safe report."""
        metrics = self._extract_metrics(execution_result)

        with self._lock:
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > 1000:
                self.metrics_history = self.metrics_history[-1000:]
                self._truncated_count += 1

        if self.features.get("incremental_pareto", True):
            frontier = self._incremental_pareto_update(metrics)
        else:
            frontier = self._compute_pareto_frontier(self.metrics_history)
            self.pareto_frontier_cache["frontier"] = frontier
        self._last_frontier = frontier

        helium_efficiency = self.calculate_helium_efficiency(
            execution_result)
        helium_resilience = self.calculate_helium_resilience_score(
            execution_result, helium_supply_status)
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
                execution_result, "policy_version", None),
            system_version=getattr(
                execution_result, "system_version", None),
            deployment_id=getattr(
                execution_result, "deployment_id", None),
            agent_id=getattr(execution_result, "agent_id", None),
            precision=getattr(execution_result, "precision", None),
        )

    def _compute_pareto_frontier(
        self, metrics_list: List[Dict],
    ) -> List[Dict]:
        """Compute the 3D Pareto frontier (full recomputation)."""
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
        Truly incremental Pareto update (P1 3.10).

        Cost is O(frontier_size) per call — not O(N²).
        """
        cached = self.pareto_frontier_cache.get("frontier")
        if not cached:
            frontier = self._compute_pareto_frontier(self.metrics_history)
            self.pareto_frontier_cache["frontier"] = frontier
            return frontier

        new_point = (
            _safe_float(new_metrics.get("energy_kwh", 0.0)),
            _safe_float(new_metrics.get("execution_time_ms", 0.0)),
            _safe_float(new_metrics.get("helium_usage", 0.0)),
        )

        # Step 1: If dominated by a frontier point (or duplicate), no change.
        for f in cached:
            fp = (
                _safe_float(f.get("energy_kwh", 0.0)),
                _safe_float(f.get("execution_time_ms", 0.0)),
                _safe_float(f.get("helium_usage", 0.0)),
            )
            if fp == new_point:
                return cached
            if fp[0] <= new_point[0] and fp[1] <= new_point[1] \
                    and fp[2] <= new_point[2]:
                return cached

        # Step 2: Remove frontier points dominated by the new point.
        new_frontier: List[Dict] = []
        for f in cached:
            fp = (
                _safe_float(f.get("energy_kwh", 0.0)),
                _safe_float(f.get("execution_time_ms", 0.0)),
                _safe_float(f.get("helium_usage", 0.0)),
            )
            if new_point[0] <= fp[0] and new_point[1] <= fp[1] \
                    and new_point[2] <= fp[2] and new_point != fp:
                continue   # f is dominated by the new point
            new_frontier.append(f)

        new_frontier.append(new_metrics)
        self.pareto_frontier_cache["frontier"] = new_frontier
        return new_frontier

    def _generate_recommendations(
        self, metrics: Dict, frontier: List[Dict],
    ) -> List[str]:
        """Generate recommendations (P1 3.19: honest empty-frontier msg)."""
        recommendations: List[str] = []
        th = self.recommendation_thresholds

        helium_usage = _safe_float(metrics.get("helium_usage", 0.0))
        energy_kwh = _safe_float(metrics.get("energy_kwh", 0.0))

        if helium_usage > th["helium_quantization_threshold"]:
            recommendations.append(
                "Consider quantization to reduce helium footprint")
        if energy_kwh > th["energy_pruning_threshold_kwh"]:
            recommendations.append(
                "High energy usage detected - consider pruning or "
                "distillation")

        if frontier:
            helium_values = [
                _safe_float(f.get("helium_usage", 0.0)) for f in frontier
            ]
            avg_helium = (
                sum(helium_values) / len(helium_values)
                if helium_values else 0.0)
            if (avg_helium > 0
                    and helium_usage > avg_helium * th["helium_frontier_factor"]):
                recommendations.append(
                    f"Helium usage {helium_usage:.2f} is "
                    f"{(helium_usage / avg_helium - 1) * 100:.0f}% above "
                    "Pareto optimal - consider alternative hardware")

        if not recommendations:
            if not frontier:
                recommendations.append(
                    "Insufficient data for comparison — submit more results")
            else:
                recommendations.append(
                    "Helium efficiency is optimal - maintaining current strategy")
        return recommendations

    def get_helium_ranking(self, top_n: int = 10) -> List[Dict]:
        """Top N tasks by helium efficiency (P1 3.1: includes task_id)."""
        with self._lock:
            history = list(self.metrics_history)
        if not history:
            return []
        efficiencies: List[Dict] = []
        for m in history:
            helium_usage = _safe_float(m.get("helium_usage", 0.0))
            if helium_usage > 0:
                efficiencies.append({
                    "task_id": m.get("task_id", "unknown"),
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

        Returns a ``BenchmarkSubmissionResult`` with pass/fail and
        detailed reasons.
        """
        # P0 2.4: lock counter increments
        with self._lock:
            self._submission_count += 1

        context = dict(submission_context or {})

        # P0 2.5: reject None inputs
        if execution_result is None:
            with self._lock:
                self._rejected_count += 1
            return BenchmarkSubmissionResult(
                benchmark_id=self.spec.benchmark_id,
                task_id="unknown",
                passed=False,
                rejection_reasons=["execution_result must not be None"],
            )

        task_id = str(getattr(execution_result, "task_id", "unknown") or "unknown")
        task_type = str(getattr(execution_result, "task_type", "unknown") or "unknown")

        metrics = self._extract_metrics(execution_result)
        reasons: List[str] = []
        accept_reasons: List[str] = []

        # --- 1. Task eligibility ---
        if (self.features["governance_enforcement"]
                and self.spec.task_eligibility
                and task_type not in self.spec.task_eligibility):
            reasons.append(
                f"task type {task_type!r} not eligible for benchmark "
                f"{self.spec.benchmark_id!r}")

        # --- 2. Anti-gaming ---
        if self.features.get("anti_gaming_checks", True):
            reasons.extend(self._run_anti_gaming_checks(metrics))

        # --- 3. Reproducibility ---
        if self.features.get("reproducibility_enforcement", True):
            submission_payload = {
                **context,
                "system_version": getattr(
                    execution_result, "system_version", None),
                "policy_version": getattr(
                    execution_result, "policy_version", None),
                "dataset_id": getattr(
                    execution_result, "dataset_id", None),
                "random_seed": getattr(
                    execution_result, "random_seed", None),
                "hardware_profile": getattr(
                    execution_result, "hardware_profile", None),
                "region": getattr(execution_result, "region", None),
            }
            missing = self.spec.reproducibility.validate(submission_payload)
            if missing:
                reasons.append(f"missing reproducibility fields: {missing}")

        # --- 4. Acceptance thresholds ---
        for metric_name, threshold in self.spec.acceptance_thresholds.items():
            value = metrics.get(metric_name)
            if not isinstance(value, (int, float)):
                reasons.append(
                    f"metric {metric_name!r} missing or non-numeric")
                continue
            if threshold >= 0 and value < threshold:
                reasons.append(
                    f"metric {metric_name!r}={value} below "
                    f"threshold {threshold}")
            elif threshold < 0 and value > abs(threshold):
                reasons.append(
                    f"metric {metric_name!r}={value} above allowed "
                    f"{abs(threshold)}")
            else:
                accept_reasons.append(
                    f"metric {metric_name!r}={value} passes "
                    f"threshold {threshold}")

        # --- 5. Optional all-zero rejection (opt-in) ---
        if self.features.get("reject_all_zero_metrics", False):
            if all(v == 0.0 for v in metrics.values()):
                reasons.append(
                    "all metrics are zero — likely a missing or "
                    "failed result")

        # --- 6. Optional PDP routing ---
        decision = None
        pdp_verdict: Optional[str] = None
        if (self._decision_point is not None
                and self.features["governance_enforcement"]):
            decision = await self._route_through_pdp(
                task_id=task_id,
                metrics=metrics,
                reasons=reasons,
                context=context,
            )
            if decision is not None:
                pdp_verdict = str(getattr(decision, "verdict", None))
                if pdp_verdict == "deny":
                    reasons.extend(
                        f"PDP: {r}" for r in getattr(decision, "reasons", []))
            elif self.features.get("fail_closed_on_pdp_error", False):
                reasons.append("PDP unavailable; submission rejected")

        passed = not reasons

        # --- 7. Audit log ---
        event_id = self._record_audit(
            task_id=task_id,
            passed=passed,
            reasons=reasons,
            metrics=metrics,
        )

        if not passed:
            with self._lock:
                self._rejected_count += 1

        # --- Update frontier on accept (analytics continues either way) ---
        frontier_size = len(self._last_frontier)
        if passed:
            self.update_pareto_frontier(execution_result, helium_supply_status)
            frontier_size = len(self._last_frontier)

        self._emit_metric("submission_processed", {
            "benchmark_id": self.spec.benchmark_id,
            "task_id": task_id,
            "passed": passed,
            "rejection_reasons": list(reasons),
            "pdp_verdict": pdp_verdict,
        })

        return BenchmarkSubmissionResult(
            benchmark_id=self.spec.benchmark_id,
            task_id=task_id,
            passed=passed,
            rejection_reasons=reasons,
            acceptance_reasons=accept_reasons,
            metrics=metrics,
            pareto_frontier_size=frontier_size,
            audit_event_id=event_id,
            simulated=bool(getattr(execution_result, "simulated", False)),
            source=str(getattr(execution_result, "source", None) or "unknown"),
            pdp_verdict=pdp_verdict,
        )

    # ------------------------------------------------------------------
    # Governance internals
    # ------------------------------------------------------------------

    def _extract_metrics(self, execution_result: Any) -> Dict[str, float]:
        """Extract metrics with field-alias support (P0 2.7)."""
        def _get_num(*keys: str, default: float = 0.0) -> float:
            for key in keys:
                val = getattr(execution_result, key, None)
                if val is not None:
                    return _safe_float(val, default=default)
            return default

        return {
            "energy_kwh": _get_num(
                "energy_consumed_kwh", "energy_consumed", "energy_kwh"),
            "execution_time_ms": _get_num(
                "execution_time_ms", "latency_ms", "latency"),
            "helium_usage": _get_num("helium_usage"),
            "accuracy": _get_num("accuracy"),
            "carbon_kg": _get_num(
                "carbon_emitted_kg", "carbon_kg", "carbon_co2e_kg"),
        }

    def _run_anti_gaming_checks(
        self, metrics: Dict[str, float],
    ) -> List[str]:
        """Dispatch to the registered anti-gaming checks (P1 3.6)."""
        reasons: List[str] = []
        for check_name in self.spec.anti_gaming_checks:
            check_fn = _ANTI_GAMING_CHECKS.get(check_name)
            if check_fn is None:
                logger.warning(
                    "unknown anti-gaming check %r; skipping", check_name)
                continue
            try:
                reasons.extend(check_fn(self, metrics) or [])
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "anti-gaming check %r raised: %s", check_name, exc)
        return reasons

    async def _route_through_pdp(
        self,
        *,
        task_id: str,
        metrics: Dict[str, float],
        reasons: List[str],
        context: Dict[str, Any],
    ) -> Optional[Any]:
        """Submit a DecisionRequest to the PDP (P0 2.3: engine fields win)."""
        try:
            from ...contracts.decision_request import DecisionRequest
        except Exception:  # noqa: BLE001
            try:
                from src.governance.contracts.decision_request import (
                    DecisionRequest,
                )
            except Exception:  # noqa: BLE001
                return None

        # P0 2.3: engine's fields must win over caller context.
        pdp_context = dict(context)
        conflicts = {"metrics", "reasons", "benchmark_id"} & set(pdp_context)
        if conflicts:
            logger.warning(
                "submission_context may not override engine fields %s; "
                "ignoring caller values", sorted(conflicts))
            for key in conflicts:
                pdp_context.pop(key, None)
        pdp_context.update({
            "metrics": dict(metrics),
            "reasons": list(reasons),
            "benchmark_id": self.spec.benchmark_id,
        })

        request = DecisionRequest(
            request_id=uuid.uuid4().hex[:12],
            action=f"benchmark_submit:{self.spec.benchmark_id}",
            task_id=task_id,
            context=pdp_context,
        )
        try:
            return await self._decision_point.decide(request)
        except Exception as e:  # noqa: BLE001
            logger.warning("PDP routing failed: %s", e)
            return None

    def _record_audit(
        self,
        *,
        task_id: str,
        passed: bool,
        reasons: List[str],
        metrics: Dict[str, float],
    ) -> Optional[str]:
        """Record the submission in the audit log (P1 3.2: None on failure)."""
        if self._audit_log is None:
            return None
        try:
            from ...contracts.governance_event import (
                GovernanceEvent, GovernanceEventKind,
            )
        except Exception:  # noqa: BLE001
            try:
                from src.governance.contracts.governance_event import (
                    GovernanceEvent, GovernanceEventKind,
                )
            except Exception:  # noqa: BLE001
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
                "metrics": _safe_dict(metrics),
            },
        )
        try:
            self._audit_log.append(event)
        except Exception as e:  # noqa: BLE001
            logger.warning("Audit log append failed: %s", e)
            return None   # P1 3.2
        return event_id

    # ------------------------------------------------------------------
    # Statistics (P0 2.4: locked)
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
                "frontier_size": len(self._last_frontier),
                "pdp_attached": self._decision_point is not None,
                "audit_attached": self._audit_log is not None,
                "features": dict(self.features),
            }

    # ------------------------------------------------------------------
    # Pickling, context managers, dunder
    # ------------------------------------------------------------------

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def __enter__(self) -> "HeliumAwareBenchmarkEngine":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "HeliumAwareBenchmarkEngine":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    def __repr__(self) -> str:
        with self._lock:
            return (
                "HeliumAwareBenchmarkEngine("
                f"version={__version__!r}, "
                f"benchmark={self.spec.benchmark_id!r}, "
                f"submissions={self._submission_count}, "
                f"rejections={self._rejected_count}, "
                f"frontier={len(self._last_frontier)})")


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "__version__",
    "BENCHMARK_SCHEMA_VERSION",
    "BenchmarkError",
    "BenchmarkReport",
    "BenchmarkSpec",
    "BenchmarkSubmissionResult",
    "BenchmarkValidationError",
    "HeliumAwareBenchmarkEngine",
    "MetricDefinition",
    "ReproducibilityRules",
]


# =============================================================================
# Self-tests
# =============================================================================

def _run_selftests() -> None:
    """Run the module's self-tests."""
    import asyncio
    import json
    import pickle
    from dataclasses import dataclass as _dc

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s")

    print(f"=== benchmark_engine v{__version__} self-tests ===\n")

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

    # ---------------------------------------------------------------- #
    # P0 2.1 — _efficiency(0) returns None
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: _efficiency(0) returns None ===")
    assert _efficiency(0.0) is None
    assert _efficiency(-1.0) is None
    assert _efficiency(None) is None
    assert _efficiency(float("nan")) is None
    assert _efficiency(2.0) == 0.5
    print(f"  ✓ _efficiency(0)=None, _efficiency(2.0)=0.5")

    engine = HeliumAwareBenchmarkEngine()
    zero_result = _Result(energy_consumed_kwh=0.0, carbon_emitted_kg=0.0)
    report = engine.update_pareto_frontier(zero_result, None)
    assert report.energy_efficiency is None
    assert report.carbon_efficiency is None
    assert report.has_efficiency_data is False
    print(f"  ✓ zero-energy report: efficiency=None, has_data=False")

    # Real efficiency still works.
    nonzero_result = _Result()
    engine2 = HeliumAwareBenchmarkEngine()
    report = engine2.update_pareto_frontier(nonzero_result, None)
    assert report.energy_efficiency is not None
    assert report.helium_efficiency is not None
    print(f"  ✓ real metrics: efficiency={report.energy_efficiency:.2f}")

    # ---------------------------------------------------------------- #
    # P0 2.2 — enum helium zone
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: HeliumZone enum normalization ===")

    class _Zone:
        HELIUM_RED = type("Z", (), {"value": "helium_red"})()
        HELIUM_CRITICAL = type("Z", (), {"value": "helium_critical"})()

    @_dc
    class _ZoneResult:
        helium_zone: Any = None
        fallback_used: bool = False
        accuracy: float = 0.5

    engine = HeliumAwareBenchmarkEngine()
    # String zone → low resilience
    r_str = engine.calculate_helium_resilience_score(
        _ZoneResult(helium_zone="helium_red", accuracy=0.5), None)
    assert r_str <= 0.4, f"string 'helium_red': {r_str}"
    print(f"  ✓ string 'helium_red' → resilience={r_str:.2f}")

    # Enum-like zone (with .value) → same low resilience
    r_enum = engine.calculate_helium_resilience_score(
        _ZoneResult(helium_zone=_Zone.HELIUM_RED, accuracy=0.5), None)
    assert r_enum <= 0.4, (
        f"P0 2.2 regression: enum zone gave resilience={r_enum:.2f}")
    print(f"  ✓ enum HELIUM_RED → resilience={r_enum:.2f}")

    # ---------------------------------------------------------------- #
    # P0 2.3 — PDP context cannot be overridden
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: PDP context protection ===")

    class _MockPDP:
        def __init__(self):
            self.last_request = None

        async def decide(self, request):
            self.last_request = request

            class _D:
                verdict = "allow"
                reasons = []
            return _D()

    spec = BenchmarkSpec(
        benchmark_id="test-pdp",
        name="PDP Test",
        anti_gaming_checks=["plausibility_bounds"],
        reproducibility=ReproducibilityRules(required_fields=[]),
    )
    pdp = _MockPDP()
    engine = HeliumAwareBenchmarkEngine(spec=spec, decision_point=pdp)

    async def _test_pdp():
        await engine.submit(
            _Result(), None,
            submission_context={
                "metrics": {"hacked": 1.0},
                "reasons": ["nothing wrong"],
                "benchmark_id": "spoofed",
            })
        req = pdp.last_request
        assert req is not None
        # The engine's metrics must be present, not the caller's.
        assert "energy_kwh" in req.context["metrics"]
        assert "hacked" not in req.context["metrics"]
        assert req.context["benchmark_id"] == "test-pdp"
        print(f"  ✓ submission_context could not override engine fields")

    asyncio.run(_test_pdp())

    # ---------------------------------------------------------------- #
    # P0 2.4 — locked counters
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: locked counters ===")

    async def _test_concurrent():
        engine = HeliumAwareBenchmarkEngine()
        n = 100
        await asyncio.gather(*(
            engine.submit(_Result(), None) for _ in range(n)
        ))
        stats = engine.get_statistics()
        assert stats["submissions"] == n, (
            f"P0 2.4 regression: submissions={stats['submissions']}, "
            f"expected {n}")
        print(f"  ✓ {n} concurrent submissions → submissions={n}")

    asyncio.run(_test_concurrent())

    # ---------------------------------------------------------------- #
    # P0 2.5 — submit(None) rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: submit(None) rejected ===")

    async def _test_none():
        engine = HeliumAwareBenchmarkEngine()
        result = await engine.submit(None, None)
        assert result.passed is False
        assert "must not be None" in result.rejection_reasons[0]
        print(f"  ✓ submit(None) → passed=False: "
              f"{result.rejection_reasons[0]}")

    asyncio.run(_test_none())

    # ---------------------------------------------------------------- #
    # P0 2.6 — pareto_frontier_size correct
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: frontier size with incremental off ===")

    async def _test_frontier_size():
        engine = HeliumAwareBenchmarkEngine(
            features={"incremental_pareto": False})
        for i in range(3):
            r = _Result()
            r.energy_consumed_kwh = 0.01 + i * 0.01
            r.execution_time_ms = 100.0 + i * 10
            r.helium_usage = 0.01 + i * 0.001
            result = await engine.submit(r, None)
            assert result.passed
            assert result.pareto_frontier_size >= 1, (
                f"P0 2.6 regression: frontier_size="
                f"{result.pareto_frontier_size}")
        print(f"  ✓ incremental_pareto=False: frontier_size > 0")

    asyncio.run(_test_frontier_size())

    # ---------------------------------------------------------------- #
    # P0 2.7 — field aliases
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: energy_consumed field alias ===")

    @_dc
    class _AltResult:
        energy_consumed: float = 0.05
        execution_time_ms: float = 100.0
        helium_usage: float = 0.01
        accuracy: float = 0.9
        carbon_emitted_kg: float = 0.02

    engine = HeliumAwareBenchmarkEngine()
    metrics = engine._extract_metrics(_AltResult())
    assert metrics["energy_kwh"] == 0.05, (
        f"P0 2.7 regression: energy_kwh={metrics['energy_kwh']}, "
        f"expected 0.05")
    print(f"  ✓ 'energy_consumed' alias → energy_kwh=0.05")

    # And the primary name still wins.
    primary = _Result(energy_consumed_kwh=0.1)
    metrics = engine._extract_metrics(primary)
    assert metrics["energy_kwh"] == 0.1
    print(f"  ✓ 'energy_consumed_kwh' (primary) → 0.1")

    # ---------------------------------------------------------------- #
    # P1 3.1 — ranking includes task_id
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: ranking includes task_id ===")

    async def _test_ranking():
        engine = HeliumAwareBenchmarkEngine()
        for i in range(3):
            r = _Result(task_id=f"t-{i}")
            r.helium_usage = 0.01 * (i + 1)
            await engine.submit(r, None)
        ranking = engine.get_helium_ranking(top_n=10)
        assert len(ranking) > 0
        assert "task_id" in ranking[0], ranking[0]
        print(f"  ✓ ranking[0]={{task_id:{ranking[0]['task_id']!r}, "
              f"efficiency:{ranking[0]['efficiency']:.2f}}}")

    asyncio.run(_test_ranking())

    # ---------------------------------------------------------------- #
    # P1 3.2 — _record_audit returns None on failure
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: audit failure returns None ===")

    class _BrokenAudit:
        def append(self, event):
            raise RuntimeError("simulated audit failure")

    async def _test_audit_fail():
        engine = HeliumAwareBenchmarkEngine(audit_log=_BrokenAudit())
        result = await engine.submit(_Result(), None)
        assert result.audit_event_id is None, (
            f"P1 3.2 regression: audit_event_id="
            f"{result.audit_event_id} (expected None on failure)")
        print(f"  ✓ audit failure → audit_event_id=None")

    asyncio.run(_test_audit_fail())

    # ---------------------------------------------------------------- #
    # P1 3.4 + 3.5 — dataclass validation
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4/3.5: dataclass validation ===")

    try:
        MetricDefinition(name="m", unit="u", direction="sideways")
    except BenchmarkValidationError as exc:
        print(f"  ✓ bad direction rejected: {exc}")

    try:
        MetricDefinition(
            name="m", unit="u", direction="min",
            min_plausible=10.0, max_plausible=1.0,
        )
    except BenchmarkValidationError:
        print(f"  ✓ min>max rejected")

    try:
        BenchmarkSpec(
            benchmark_id="",   # empty
            name="n",
            anti_gaming_checks=["plausibility_bounds"],
            reproducibility=ReproducibilityRules(required_fields=["a"]),
        )
    except BenchmarkValidationError:
        print(f"  ✓ empty benchmark_id rejected")

    # ---------------------------------------------------------------- #
    # P1 3.6 — anti-gaming registry
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: anti-gaming check registry ===")
    spec = BenchmarkSpec(
        benchmark_id="test",
        name="test",
        metric_definitions={
            "accuracy": MetricDefinition(
                name="accuracy", unit="ratio", direction="max",
                min_plausible=0.0, max_plausible=1.0),
        },
        anti_gaming_checks=["accuracy_range"],
        reproducibility=ReproducibilityRules(required_fields=[]),
    )
    engine = HeliumAwareBenchmarkEngine(spec=spec)

    async def _test_check():
        bad = _Result(accuracy=1.5)
        result = await engine.submit(bad, None)
        assert not result.passed
        assert any("accuracy" in r for r in result.rejection_reasons)
        print(f"  ✓ accuracy=1.5 → rejected: "
              f"{result.rejection_reasons[0]}")

    asyncio.run(_test_check())

    # ---------------------------------------------------------------- #
    # P1 3.7 — from_legacy_dict
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: BenchmarkSpec.from_legacy_dict ===")
    spec2 = BenchmarkSpec.from_legacy_dict({
        "benchmark_id": "b2",
        "name": "B2",
        "anti_gaming_checks": ["accuracy_range"],
        "reproducibility": ReproducibilityRules(required_fields=[]),
        "unknown_field": "ignored",
    })
    assert spec2.benchmark_id == "b2"
    print(f"  ✓ from_legacy_dict works; unknown fields ignored")

    # ---------------------------------------------------------------- #
    # P1 3.10 — incremental pareto is O(frontier)
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: incremental pareto performance ===")

    async def _test_incremental():
        import time
        engine = HeliumAwareBenchmarkEngine()
        n = 100
        t0 = time.perf_counter()
        for i in range(n):
            r = _Result()
            r.energy_consumed_kwh = 1.0 + i * 0.01   # never improves
            r.execution_time_ms = 1000.0 + i * 0.1
            r.helium_usage = 1.0
            await engine.submit(r, None)
        elapsed = time.perf_counter() - t0
        # 100 non-improving submissions must not be O(100²).
        assert elapsed < 1.0, (
            f"P1 3.10 regression: {n} submissions took "
            f"{elapsed:.2f}s")
        print(f"  ✓ {n} dominated submissions in {elapsed:.3f}s")

    asyncio.run(_test_incremental())

    # ---------------------------------------------------------------- #
    # P1 3.12 — simulated in submission result
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.12: simulated on submission result ===")

    async def _test_sim():
        engine = HeliumAwareBenchmarkEngine()
        sim = _Result(simulated=True)
        result = await engine.submit(sim, None)
        assert result.simulated is True
        print(f"  ✓ simulated submission → result.simulated=True")

    asyncio.run(_test_sim())

    # ---------------------------------------------------------------- #
    # P1 3.13 — bool rejected
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.13: bool rejected by _safe_float ===")
    assert _safe_float(True) == 0.0
    assert _safe_float(False) == 0.0
    assert _safe_float(1.5) == 1.5
    print(f"  ✓ _safe_float(True)=0.0, _safe_float(1.5)=1.5")

    # ---------------------------------------------------------------- #
    # P1 3.18 — sanitized report to_dict
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.18: JSON-safe report ===")

    engine = HeliumAwareBenchmarkEngine()
    report = engine.update_pareto_frontier(_Result(), None)
    payload = report.to_dict()
    json.dumps(payload)  # must not raise
    assert "has_efficiency_data" in payload
    print(f"  ✓ report.to_dict JSON-serializable")

    # ---------------------------------------------------------------- #
    # Bonus: pickle, context manager
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: pickle / context ===")

    engine = HeliumAwareBenchmarkEngine()
    blob = pickle.dumps(engine)
    restored = pickle.loads(blob)
    assert restored._lock is not None
    assert restored.spec.benchmark_id == engine.spec.benchmark_id
    print(f"  ✓ pickle round-trip works")

    with HeliumAwareBenchmarkEngine() as engine:
        assert engine is not None
    print(f"  ✓ sync context manager works")

    async def _ctx():
        async with HeliumAwareBenchmarkEngine() as engine:
            assert engine is not None

    asyncio.run(_ctx())
    print(f"  ✓ async context manager works")

    # ---------------------------------------------------------------- #
    # Regression: v2.0.0 API still works
    # ---------------------------------------------------------------- #
    print("\n=== Regression: v2.0.0 API ===")

    engine = HeliumAwareBenchmarkEngine()
    report = engine.update_pareto_frontier(_Result(), None)
    assert report.task_id == "task-001"
    assert report.helium_resilience_score >= 0.0
    print(f"  ✓ update_pareto_frontier preserves behaviour")

    # Backward-compat: sentinel no longer used in reports.
    zero = _Result(energy_consumed_kwh=0.0)
    engine2 = HeliumAwareBenchmarkEngine()
    r = engine2.update_pareto_frontier(zero, None)
    assert r.energy_efficiency is None, (
        f"Regression: expected None, got {r.energy_efficiency}")
    print(f"  ✓ zero-energy efficiency is None (not 1e12)")

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    import sys
    if "--selftest" in sys.argv or len(sys.argv) == 1:
        _run_selftests()
    else:
        # Original demo preserved
        import asyncio
        import json

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

        engine = HeliumAwareBenchmarkEngine()
        report = engine.update_pareto_frontier(_Result(), None)
        print(f"Report: task={report.task_id}")
        print(f"  helium_efficiency: {report.helium_efficiency}")
        print(f"  has_efficiency_data: {report.has_efficiency_data}")
        print(f"  recommendations: {report.recommendations}")

        print("\nJSON-safe report:")
        print(json.dumps(report.to_dict(), indent=2))

        spec = BenchmarkSpec(
            benchmark_id="helium-v1",
            name="Helium Benchmark v1",
            task_eligibility=["inference", "fine_tuning"],
            metric_definitions={
                "energy_kwh": MetricDefinition(
                    name="energy_kwh", unit="kWh", direction="min",
                    min_plausible=0.0, max_plausible=10.0),
                "helium_usage": MetricDefinition(
                    name="helium_usage", unit="units", direction="min",
                    min_plausible=0.001, max_plausible=10.0),
            },
            acceptance_thresholds={
                "accuracy": 0.85,
                "carbon_kg": -1.0,
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

            bad = _Result()
            bad.helium_usage = 0.0001
            bad.accuracy = 1.5
            bad_result = await governed.submit(bad, None)
            print(f"\n  Fabricated submission passed: {bad_result.passed}")
            print(f"  Rejection reasons: {bad_result.rejection_reasons}")

            print("\n=== Statistics ===")
            print(json.dumps(governed.get_statistics(), indent=2))

        asyncio.run(_demo())
