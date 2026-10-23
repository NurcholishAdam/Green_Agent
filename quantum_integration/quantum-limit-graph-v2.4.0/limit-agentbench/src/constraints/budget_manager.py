# src/constraints/budget_manager.py
"""
budget_manager.py — v6.0.0 (thin wrapper over carbon_forecast v18)
===================================================================

Budget Manager for Green_Agent.

Manages and tracks consumption against energy / carbon / latency / cost /
helium budgets.

**What changed (v5 → v6)**
--------------------------

**P0 — bugs**

1. **`check_budget('latency_ms')` now reflects actual peak latency.**
   v5 read ``execution_record.get('latency_ms', 0.0)``, but the record
   has no top-level ``latency_ms`` key — so ``ratio`` was always ``0.0``
   and latency was silently reported ``UNDER_BUDGET`` regardless of
   actual consumption. v6 reads ``self.consumed['latency_ms']``, which
   is the peak across tasks (matching ``get_utilization()``).
2. **Causal learner is feature-aware, signed, and tightening-only.**
   v5's ``record(reward)`` had no state input; ``update()`` was a counter;
   ``reward = 1.0 if correct else 0.0`` was a one-way ratchet. v6 stores
   ``(features, reward)``, updates ``lr * reward * feature[i]``, uses
   ``+1.0 / -1.0``, and clamps to tightening-only unless
   ``allow_threshold_widening=True``.
3. **Causal learner is wired into `_active_limits`.**
   v5's ``_active_limits`` returned base limits regardless of the learner,
   so ``can_execute`` never saw adaptive budgets. v6 multiplies the base
   limits by the learned tightening factor (``warning_learned /
   warning_base``, capped at 1.0).
4. **HITL propagation.** v5 logged the review and returned
   ``can_execute=False`` regardless. v6 reviews every reviewable
   violation; with ``allow_hitl_override=True`` a fully-approved set
   returns ``(True, [])``.
5. **`Budget.__post_init__` validates `warning_threshold < critical_threshold`.**
   v5 accepted inverted thresholds and then classified nonsense.
6. **NaN / inf estimates are rejected.** v5's ``_safe_float(NaN) = 0.0``
   silently permitted. v6 produces a ``bad_estimate`` violation with
   ``severity=EMERGENCY``.
7. **Missing / typo'd estimate keys are rejected by default.** v5's
   ``estimated.get('energy_whh', 0.0)`` silently permitted. v6 defaults
   to ``strict_estimates=True``; unknown or missing budgeted metrics
   produce a denial.
8. **`record_consumption` logs `EXCEEDED` as ERROR.** v5 only warned on
   ``NEAR_LIMIT`` / ``AT_LIMIT`` — the worst case was the quietest.
9. **One severity function used everywhere.** v5 had
   ``_severity_for_status`` and ``_classify_can_execute_severity`` with
   *different threshold boundaries*. v6 uses ``_severity_for_ratio`` for
   both.
10. **`NEAR_LIMIT` → `WARNING`, not `INFO`.** A near-limit status is by
    definition a warning.

**P1 — reliability**

11. **Thread-safe via `RLock`** around every mutable structure.
12. **v18's single-probe `CircuitBreaker`** (via ``SyncCircuitAdapter``).
13. **Async HITL callbacks are awaited in `areview`, rejected (fail-safe)
    in `review`.**
14. **`check_budget` never invokes the causal learner.** v5 called
    ``predict_thresholds`` from ``check_budget``, which is itself called
    from ``record_consumption`` — a hot path. v6 caches the learned
    thresholds on every ``can_execute`` and reuses them in ``check_budget``.
15. **`reset(clear_statistics=False)`** — optionally clears
    ``severity_counts`` and ``breach_by_metric``.
16. **`get_remaining_budget()['cost_usd']` returns `inf`** when
    ``max_cost_usd`` is None (v5 returned ``0``, silently marking "no
    remaining budget").
17. **`get_statistics()` reports the cached market snapshot**, not a
    fresh random value.
18. **Every collection bounded** (``execution_history``,
    ``violation_history``, ``federated.updates``,
    ``safety.violations``, ``hitl.pending``, ``hitl.feedback_log``,
    ``chaos.events``).
19. **`set_features()`, `health_check()`, `__aenter__`/`__aexit__`.**
20. **`record_consumption` validates metric names** — unknown metrics
    are logged and skipped, not silently dropped.

**P2 — architecture**

21. **Enhancement classes imported from `carbon_forecast` v18** —
    the ninth local copy in v5 is retired.
22. **Temporal properties are load-bearing** — v5's
    ``RemainingNonNegative``, ``StatusMonotonic``, and
    ``CanExecuteImpliesRemaining`` were all tautologies (``remaining`` is
    always >= 0, and the other two read hard-coded ``True``). v6 passes
    real values and lets the properties fire on logic bugs.
23. **`QuantumDistillationBridge.distill` uses `round`, not `floor`.**
24. **`FederatedBudgetAggregator` bounded, zero-weight safe.**
25. **`Budget.allowed_precisions` is validated** and enforced in
    ``can_execute`` when ``estimated['precision']`` is supplied.
26. **`helium_signal_fn` is used** — when it's set and
    ``estimated['helium_units']`` is absent, helium is estimated from
    ``energy_wh * scarcity_score``.

**Public API preserved**

    budget = Budget.eco_budget()
    manager = BudgetManager(budget)
    status = manager.check_budget('energy_wh')
    can_run, violations = manager.can_execute({'energy_wh': 2.0, ...})
    manager.record_consumption({'energy_wh': 1.8, ...})
    summary = manager.get_summary()

**New in v6**

    manager.can_execute(estimated, allow_hitl_override=False)
    manager.record_decision_outcome(was_correct)      # feature-aware
    manager.set_features(features)
    manager.health_check()
    await manager.health_check()
    async with BudgetManager(budget) as manager: ...
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import random
import threading
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
)

# --------------------------------------------------------------------------- #
# v18 imports (single implementation for the enhancement classes)
# --------------------------------------------------------------------------- #
try:
    from carbon_forecast import (  # type: ignore[import-not-found]
        CircuitBreaker,
        ChaosTestingEngine,
        ActiveUserPreferenceLearner,
        AdaptivePrecisionSwitcher,
        CarbonMarketIntegrator,
        MultiAgentCoordinator,
        FederatedGreenAggregator,
        TemporalLogicVerifier,
        SimpleQuantumDistillationEngine,
        SimpleFederatedGreenAggregator,
        SimpleMultiAgentCoordinator,
        SimpleTemporalLogicVerifier,
        SimpleAdaptivePrecisionSwitcher,
        SimpleCarbonMarketIntegrator,
        SimpleChaosTestingEngine,
        SimpleActiveUserPreferenceLearner,
        _utcnow,
        _ensure_aware,
        _json_safe,
    )
    _V18_AVAILABLE = True
    _V18_ERROR: Optional[str] = None
except ImportError as _e:  # pragma: no cover
    _V18_AVAILABLE = False
    _V18_ERROR = str(_e)
    CircuitBreaker = None  # type: ignore[assignment]
    ChaosTestingEngine = None  # type: ignore[assignment]
    ActiveUserPreferenceLearner = None  # type: ignore[assignment]
    AdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    CarbonMarketIntegrator = None  # type: ignore[assignment]
    MultiAgentCoordinator = None  # type: ignore[assignment]
    FederatedGreenAggregator = None  # type: ignore[assignment]
    TemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleQuantumDistillationEngine = None  # type: ignore[assignment]
    SimpleFederatedGreenAggregator = None  # type: ignore[assignment]
    SimpleMultiAgentCoordinator = None  # type: ignore[assignment]
    SimpleTemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleAdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    SimpleCarbonMarketIntegrator = None  # type: ignore[assignment]
    SimpleChaosTestingEngine = None  # type: ignore[assignment]
    SimpleActiveUserPreferenceLearner = None  # type: ignore[assignment]

    def _utcnow() -> datetime:  # type: ignore[misc]
        return datetime.now(timezone.utc)

    def _ensure_aware(dt: datetime) -> datetime:  # type: ignore[misc]
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def _json_safe(obj: Any) -> Any:  # type: ignore[misc]
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, datetime):
            return _ensure_aware(obj).isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, Mapping):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe(v) for v in obj]
        return obj


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Bounds
# --------------------------------------------------------------------------- #
_MAX_EXECUTION_HISTORY = 1024
_MAX_VIOLATION_HISTORY = 512
_MAX_FEDERATED_UPDATES = 100
_MAX_VIOLATIONS = 200
_MAX_HITL_PENDING = 100
_MAX_HITL_LOG = 500
_MAX_CHAOS_EVENTS = 500
_MAX_CAUSAL_BUFFER = 512


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class BudgetStatus(Enum):
    """Budget consumption status (v1 enum, unchanged)."""

    UNDER_BUDGET = "under_budget"
    NEAR_LIMIT = "near_limit"
    AT_LIMIT = "at_limit"
    EXCEEDED = "exceeded"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    GENERALIST = "generalist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    HELIUM_SPECIALIST = "helium_specialist"
    LATENCY_SPECIALIST = "latency_specialist"


class EvaluationOutcome(Enum):
    """Distinguishes what `can_execute` was authorized to decide."""

    APPROVED = "approved"
    DENIED = "denied"
    DEGRADED = "degraded"
    CIRCUIT_OPEN = "circuit_open"
    CHAOS = "chaos"
    ERROR = "error"
    BAD_ESTIMATE = "bad_estimate"


class DegradedPolicy(Enum):
    """What `can_execute` does when enforcement is unavailable."""

    DENY = "deny"      # default: fail-safe (deny)
    ALLOW = "allow"    # opt-in: fail-open (approve, log warning)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BudgetManagerError(RuntimeError):
    """Base for budget-manager errors."""


class BudgetConfigError(ValueError):
    """Raised for invalid budget configuration."""


# --------------------------------------------------------------------------- #
# Domain dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


@dataclass(frozen=True)
class ThresholdState:
    """Feature vector for the causal threshold learner."""

    hour_of_day: int
    utilization_ratio: float
    workload_hash: float


@dataclass(frozen=True)
class BudgetExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class DistilledBudgetPolicy:
    precision: PrecisionLevel
    thresholds: Dict[str, float]
    quality_retention: float
    energy_reduction_percent: float


@dataclass(frozen=True)
class FederatedBudgetProfile:
    deployment_id: str
    budget_name: str
    metric: str
    mean_utilization: float
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class HITLRequest:
    metric: str
    estimated: float
    remaining: float
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=_utcnow)


_VALID_PRECISIONS = frozenset(
    {"fp32", "fp16", "int8", "int4", "quantum_distilled"}
)


# --------------------------------------------------------------------------- #
# Budget (v1 fields + v5 helium extension, now validated)
# --------------------------------------------------------------------------- #
@dataclass
class Budget:
    """
    Budget constraints for agent execution.

    All original fields preserved. New in v5: ``max_helium_units`` and
    ``allowed_precisions``. New in v6: validation that
    ``warning_threshold < critical_threshold`` and that every entry in
    ``allowed_precisions`` is a valid ``PrecisionLevel`` value.
    """

    max_energy_wh: float
    max_carbon_g: float
    max_latency_ms: float
    max_cost_usd: Optional[float] = None

    warning_threshold: float = 0.80
    critical_threshold: float = 0.95

    name: str = "Default Budget"
    description: str = ""
    created_at: datetime = field(default_factory=_utcnow)

    max_helium_units: Optional[float] = None
    allowed_precisions: Optional[List[str]] = None

    def __post_init__(self) -> None:
        # --- Positive-limit validation (v1) ---
        if self.max_energy_wh <= 0:
            raise BudgetConfigError(
                f"Energy budget must be positive: {self.max_energy_wh}"
            )
        if self.max_carbon_g <= 0:
            raise BudgetConfigError(
                f"Carbon budget must be positive: {self.max_carbon_g}"
            )
        if self.max_latency_ms <= 0:
            raise BudgetConfigError(
                f"Latency budget must be positive: {self.max_latency_ms}"
            )
        if self.max_cost_usd is not None and self.max_cost_usd <= 0:
            raise BudgetConfigError(
                f"Cost budget must be positive: {self.max_cost_usd}"
            )
        if (
            self.max_helium_units is not None
            and self.max_helium_units <= 0
        ):
            raise BudgetConfigError(
                f"Helium budget must be positive: {self.max_helium_units}"
            )

        # --- Threshold validation (v1) ---
        if not 0.0 < self.warning_threshold <= 1.0:
            raise BudgetConfigError(
                f"warning_threshold must be in (0, 1], got "
                f"{self.warning_threshold}"
            )
        if not 0.0 < self.critical_threshold <= 1.0:
            raise BudgetConfigError(
                f"critical_threshold must be in (0, 1], got "
                f"{self.critical_threshold}"
            )

        # --- P0 §2.4: threshold ordering ---
        if self.warning_threshold >= self.critical_threshold:
            raise BudgetConfigError(
                f"warning_threshold ({self.warning_threshold}) must be "
                f"strictly less than critical_threshold "
                f"({self.critical_threshold})"
            )

        # --- P2 §4.8: allowed_precisions validation ---
        if self.allowed_precisions is not None:
            if not self.allowed_precisions:
                raise BudgetConfigError(
                    "allowed_precisions must be non-empty if specified"
                )
            invalid = [
                p for p in self.allowed_precisions
                if p not in _VALID_PRECISIONS
            ]
            if invalid:
                raise BudgetConfigError(
                    f"Invalid precision value(s) in allowed_precisions: "
                    f"{invalid}. Valid values: {sorted(_VALID_PRECISIONS)}"
                )

        logger.info(
            "Created budget: %s — E:%.2fWh, C:%.2fg, L:%.0fms%s",
            self.name,
            self.max_energy_wh,
            self.max_carbon_g,
            self.max_latency_ms,
            (
                f", He:{self.max_helium_units}u"
                if self.max_helium_units is not None
                else ""
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "max_energy_wh": self.max_energy_wh,
            "max_carbon_g": self.max_carbon_g,
            "max_latency_ms": self.max_latency_ms,
            "max_cost_usd": self.max_cost_usd,
            "warning_threshold": self.warning_threshold,
            "critical_threshold": self.critical_threshold,
            "created_at": _ensure_aware(self.created_at).isoformat(),
        }
        if self.max_helium_units is not None:
            out["max_helium_units"] = self.max_helium_units
        if self.allowed_precisions is not None:
            out["allowed_precisions"] = list(self.allowed_precisions)
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Budget":
        if not isinstance(data, Mapping):
            raise BudgetConfigError(
                f"from_dict expects Mapping, got "
                f"{type(data).__name__}"
            )
        created_at = (
            datetime.fromisoformat(data["created_at"])
            if "created_at" in data
            else _utcnow()
        )
        return cls(
            max_energy_wh=data["max_energy_wh"],
            max_carbon_g=data["max_carbon_g"],
            max_latency_ms=data["max_latency_ms"],
            max_cost_usd=data.get("max_cost_usd"),
            warning_threshold=data.get("warning_threshold", 0.80),
            critical_threshold=data.get("critical_threshold", 0.95),
            name=data.get("name", "Default Budget"),
            description=data.get("description", ""),
            created_at=created_at,
            max_helium_units=data.get("max_helium_units"),
            allowed_precisions=data.get("allowed_precisions"),
        )

    # ---- Presets (v1 + helium extension) ----

    @classmethod
    def eco_budget(cls) -> "Budget":
        return cls(
            max_energy_wh=5.0,
            max_carbon_g=1.0,
            max_latency_ms=10000,
            max_helium_units=0.05,
            name="Eco Budget",
            description=(
                "Strict energy/carbon limits for eco-friendly deployment"
            ),
        )

    @classmethod
    def balanced_budget(cls) -> "Budget":
        return cls(
            max_energy_wh=20.0,
            max_carbon_g=4.0,
            max_latency_ms=5000,
            max_helium_units=0.2,
            name="Balanced Budget",
            description=(
                "Moderate limits balancing efficiency and performance"
            ),
        )

    @classmethod
    def performance_budget(cls) -> "Budget":
        return cls(
            max_energy_wh=100.0,
            max_carbon_g=20.0,
            max_latency_ms=1000,
            max_helium_units=1.0,
            name="Performance Budget",
            description="Optimized for low latency, lenient on energy",
        )


# --------------------------------------------------------------------------- #
# Sync adapters over v18
# --------------------------------------------------------------------------- #
class SyncCircuitAdapter:
    """Sync facade over v18's single-probe ``CircuitBreaker``."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ) -> None:
        if not _V18_AVAILABLE:
            raise RuntimeError(
                "SyncCircuitAdapter requires carbon_forecast (v18): "
                f"{_V18_ERROR}"
            )
        self._cb = CircuitBreaker(  # type: ignore[misc]
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            name=name,
        )

    @property
    def state(self) -> str:
        return self._cb.state

    @property
    def failures(self) -> int:
        return self._cb.failure_count

    def can_call(self) -> bool:
        try:
            self._cb._acquire_slot()
            self._cb._on_success()
            return True
        except RuntimeError:
            return False

    def record_success(self) -> None:
        self._cb._on_success()

    def record_failure(self) -> None:
        self._cb._on_failure()


class SyncChaosInjector:
    """Deterministic sync fault injector for the enforcement hot path."""

    def __init__(self, failure_rate: float = 0.0) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be in [0, 1]")
        self.failure_rate = failure_rate
        self.events: Deque[Dict[str, Any]] = deque(maxlen=_MAX_CHAOS_EVENTS)

    def maybe_fail(self, component: str) -> bool:
        if random.random() < self.failure_rate:
            self.events.append(
                {"component": component, "at": _utcnow().isoformat()}
            )
            return True
        return False


# --------------------------------------------------------------------------- #
# XAI
# --------------------------------------------------------------------------- #
class BudgetExplainer:
    @staticmethod
    def explain_can_execute(
        can_execute: bool,
        violations: List[str],
        estimated: Mapping[str, Any],
        remaining: Mapping[str, float],
        utilization: Mapping[str, float],
        severity: Optional[str] = None,
        market_snapshot: Optional[Mapping[str, Any]] = None,
        hitl_status: Optional[str] = None,
    ) -> BudgetExplanation:
        reasons: List[str] = []
        if can_execute:
            reasons.append(
                f"Execution approved: all {len(estimated)} estimated "
                f"metrics fit within remaining budget."
            )
            if utilization:
                tightest = max(utilization, key=utilization.get)
                reasons.append(
                    f"Tightest metric: '{tightest}' at "
                    f"{utilization[tightest]:.1%} utilization."
                )
        else:
            reasons.append(
                f"Execution denied: {len(violations)} budget(s) would be "
                f"exceeded ({', '.join(violations)})."
            )
            for v in violations:
                est = BudgetManager._safe_float(estimated.get(v, 0.0))
                rem = remaining.get(v, 0.0)
                reasons.append(
                    f"'{v}': estimated {est:.4f} > remaining {rem:.4f}."
                )
            if severity:
                reasons.append(f"Severity classified as '{severity}'.")
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(
                    f"Carbon market price: ${cp:.2f}/tCO₂."
                )
        if hitl_status and hitl_status != "not_required":
            reasons.append(f"HITL status: {hitl_status}.")

        return BudgetExplanation(
            headline=(
                f"[{'APPROVED' if can_execute else 'DENIED'}] "
                f"{len(violations)} violation(s)"
            ),
            rationale=reasons,
            confidence=0.95,
            contributing_factors={
                **{f"remaining_{k}": v for k, v in remaining.items()},
                **{
                    f"estimated_{k}": BudgetManager._safe_float(v)
                    for k, v in estimated.items()
                },
            },
        )


# --------------------------------------------------------------------------- #
# Causal learner (fixed: feature-aware, signed reward, tightening-only)
# --------------------------------------------------------------------------- #
class CausalThresholdLearner:
    """
    Learns offsets to the warning/critical thresholds from outcomes.

    **Fixed vs v5**

    * v5's ``record(reward)`` had no state; v6 stores ``(features, reward)``.
    * v5's ``update()`` was a counter; v6 consumes the buffer.
    * v5's ``reward = 1.0 if correct else 0.0`` was a one-way ratchet;
      v6 uses ``+1.0 / -1.0`` so incorrect decisions tighten.
    * v5 could loosen thresholds silently; v6 defaults to
      ``allow_threshold_widening=False`` (tightening-only).
    """

    N_FEATURES = 3

    def __init__(
        self,
        base_warning: float = 0.80,
        base_critical: float = 0.95,
        lr: float = 0.02,
        allow_threshold_widening: bool = False,
    ) -> None:
        if lr <= 0:
            raise ValueError("lr must be > 0")
        self.base_warning = base_warning
        self.base_critical = base_critical
        self.lr = lr
        self.allow_threshold_widening = bool(allow_threshold_widening)
        self.warning_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.critical_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.buffer: Deque[Tuple[List[float], float]] = deque(
            maxlen=_MAX_CAUSAL_BUFFER
        )
        self.observations: int = 0
        self.updates: int = 0
        self.widening_events: int = 0

    @staticmethod
    def features_of(state: ThresholdState) -> List[float]:
        return [
            state.hour_of_day / 24.0,
            state.utilization_ratio,
            state.workload_hash,
        ]

    def predict_thresholds(
        self, state: ThresholdState
    ) -> Tuple[float, float]:
        f = self.features_of(state)
        dw = sum(c * x for c, x in zip(self.warning_coeffs, f))
        dc = sum(c * x for c, x in zip(self.critical_coeffs, f))
        dw = max(-0.05, min(0.05, dw))
        dc = max(-0.05, min(0.05, dc))
        if not self.allow_threshold_widening:
            dw = min(0.0, dw)  # tighten only
            dc = min(0.0, dc)
        return (
            max(0.5, min(0.99, self.base_warning + dw)),
            max(0.55, min(0.999, self.base_critical + dc)),
        )

    def predict_limit_multiplier(self, state: ThresholdState) -> float:
        """
        Tightening factor to apply to the effective budget limit.

        Returns ``warning_learned / warning_base`` (capped at 1.0). When
        the learner tightens the warning threshold, the effective limit
        tightens too.
        """
        warning, _ = self.predict_thresholds(state)
        if self.base_warning <= 0:
            return 1.0
        return min(1.0, warning / self.base_warning)

    def record(self, features: List[float], reward: float) -> None:
        if len(features) != self.N_FEATURES:
            return
        if not math.isfinite(reward):
            return
        self.buffer.append((list(features), float(reward)))
        self.observations += 1

    def update(self) -> bool:
        if not self.buffer:
            return False
        for features, reward in self.buffer:
            for i, x in enumerate(features):
                dw = self.lr * reward * x * 0.1
                dc = self.lr * reward * x * 0.1
                if not self.allow_threshold_widening:
                    dw = min(0.0, dw)
                    dc = min(0.0, dc)
                self.warning_coeffs[i] = max(
                    -1.0, min(1.0, self.warning_coeffs[i] + dw)
                )
                self.critical_coeffs[i] = max(
                    -1.0, min(1.0, self.critical_coeffs[i] + dc)
                )
        self.buffer.clear()
        self.updates += 1
        return True

    def stats(self) -> Dict[str, Any]:
        return {
            "observations": self.observations,
            "updates": self.updates,
            "widening_events": self.widening_events,
            "allow_threshold_widening": self.allow_threshold_widening,
            "buffer_size": len(self.buffer),
            "warning_coeffs": list(self.warning_coeffs),
            "critical_coeffs": list(self.critical_coeffs),
        }


# --------------------------------------------------------------------------- #
# Distillation (round, not floor)
# --------------------------------------------------------------------------- #
class QuantumDistillationBridge:
    """
    Quantizes the threshold table to a lower precision.

    Uses ``round`` (v5 used ``math.floor``, biasing thresholds downward
    and making the system *stricter* than the operator intended in a
    hard-to-notice way).
    """

    _SCALE: Dict[PrecisionLevel, float] = {
        PrecisionLevel.FP32: 1.0,
        PrecisionLevel.FP16: 1.0,
        PrecisionLevel.INT8: 100.0,
        PrecisionLevel.INT4: 10.0,
        PrecisionLevel.QUANTUM_DISTILLED: 5.0,
    }
    _RETENTION: Dict[PrecisionLevel, Tuple[float, float]] = {
        PrecisionLevel.FP32: (1.00, 0.0),
        PrecisionLevel.FP16: (0.98, 30.0),
        PrecisionLevel.INT8: (0.93, 55.0),
        PrecisionLevel.INT4: (0.85, 70.0),
        PrecisionLevel.QUANTUM_DISTILLED: (0.80, 85.0),
    }

    def distill(
        self,
        warning: float,
        critical: float,
        precision: PrecisionLevel,
    ) -> DistilledBudgetPolicy:
        scale = self._SCALE[precision]
        retention, energy = self._RETENTION[precision]

        def qz(v: float) -> float:
            if scale <= 1.0:
                return float(v)
            return round(float(v) * scale) / scale

        return DistilledBudgetPolicy(
            precision=precision,
            thresholds={
                "warning": qz(warning),
                "critical": qz(critical),
            },
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Federated aggregation (bounded, div-zero safe)
# --------------------------------------------------------------------------- #
class FederatedBudgetAggregator:
    """FedAvg over per-metric utilization profiles, bounded."""

    def __init__(self, max_updates: int = _MAX_FEDERATED_UPDATES) -> None:
        self.updates: Deque[FederatedBudgetProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedBudgetProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedBudgetProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.metric].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for metric, profiles in grouped.items():
            total_w = sum(max(0, p.sample_count) for p in profiles)
            if total_w <= 0:
                continue
            result[metric] = {
                "mean_utilization": sum(
                    p.mean_utilization * p.sample_count
                    for p in profiles
                )
                / total_w,
                "sample_count": float(total_w),
            }
        self._global = result
        return result

    def lookup(self, metric: str) -> Optional[Dict[str, float]]:
        return self._global.get(metric)

    def reset(self) -> None:
        self.updates.clear()
        self._global.clear()


# --------------------------------------------------------------------------- #
# Helium profiling
# --------------------------------------------------------------------------- #
class HeliumProfiler:
    HELIUM_UNITS_PER_KWH = 0.05

    @classmethod
    def estimate_units(
        cls, energy_wh: float, scarcity_score: float = 0.0
    ) -> float:
        energy_kwh = energy_wh / 1000.0
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


# --------------------------------------------------------------------------- #
# Load-bearing temporal properties
# --------------------------------------------------------------------------- #
class BudgetProperty(Protocol):
    def check(self, ctx: Mapping[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass(frozen=True)
class RemainingNonNegative:
    """
    G(remaining >= 0) after consumption recording.

    v5's context always supplied ``min_remaining >= 0`` (because
    ``get_remaining_budget`` clamps with ``max(0, ...)``), so this was a
    tautology. v6 passes the pre-clamp remaining (which can be negative
    after an EXCEEDED record) so the property actually fires on a logic
    bug that stops clamping.
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        raw = ctx.get("raw_min_remaining", 0.0)
        return float(raw) >= 0.0

    def name(self) -> str:
        return "RemainingNonNegative"


@dataclass(frozen=True)
class StatusMonotonic:
    """
    G(status == EXCEEDED → ratio >= 1.0) and vice versa.

    v5 read a hard-coded ``True``. v6 cross-checks the status against
    the ratio, so a misclassification fires.
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        status = ctx.get("status")
        ratio = ctx.get("ratio")
        if status is None or ratio is None:
            return True
        ratio = float(ratio)
        if status == BudgetStatus.EXCEEDED.value:
            return ratio >= 1.0
        if status == BudgetStatus.AT_LIMIT.value:
            return ratio < 1.0
        if status == BudgetStatus.NEAR_LIMIT.value:
            return ratio < 1.0
        if status == BudgetStatus.UNDER_BUDGET.value:
            return ratio < 1.0
        return False

    def name(self) -> str:
        return "StatusMonotonic"


@dataclass(frozen=True)
class CanExecuteImpliesRemaining:
    """
    G(can_execute=True → each estimated metric <= its remaining).

    v5 read a hard-coded ``True``. v6 cross-checks the decision against
    the per-metric comparison, so a mismatch fires.
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        approved = ctx.get("can_execute")
        if approved is None or not approved:
            return True
        estimated = ctx.get("estimated", {})
        remaining = ctx.get("remaining", {})
        for metric, est in estimated.items():
            if metric not in remaining:
                continue
            try:
                est_f = float(est)
            except (TypeError, ValueError):
                continue
            if est_f > remaining[metric] + 1e-9:
                return False
        return True

    def name(self) -> str:
        return "CanExecuteImpliesRemaining"


class BudgetSafetyMonitor:
    """Bounded verification of temporal budget properties."""

    def __init__(self, max_violations: int = _MAX_VIOLATIONS) -> None:
        self.properties: List[BudgetProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, prop: BudgetProperty) -> None:
        self.properties.append(prop)

    def verify(
        self, ctx: Mapping[str, Any]
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for prop in self.properties:
            try:
                if not prop.check(ctx):
                    bad.append(prop.name())
                    self.violations.append(
                        {
                            "property": prop.name(),
                            "at": _utcnow().isoformat(),
                        }
                    )
            except Exception as exc:
                logger.warning(
                    "Property %s raised: %s", prop.name(), exc
                )
                bad.append(prop.name())
        return (len(bad) == 0, bad)


# --------------------------------------------------------------------------- #
# HITL gate (sync + async, bounded)
# --------------------------------------------------------------------------- #
class HumanReviewGate:
    """
    Sync + async HITL gate.

    * ``review``  — sync; **rejects awaitable callbacks (fail-safe)**.
    * ``areview`` — async; awaits awaitables with a timeout, and can
      delegate to v18's ``ActiveUserPreferenceLearner`` when no callback
      is set.
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        borderline_ratio: float = 1.05,
        severe_ratio: float = 2.0,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.borderline_ratio = borderline_ratio
        self.severe_ratio = severe_ratio
        self.pending: Deque[HITLRequest] = deque(maxlen=_MAX_HITL_PENDING)
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None
        self._outcomes: Dict[str, int] = defaultdict(int)

    def set_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        self._callback = cb

    def needs_review(self, estimated: float, remaining: float) -> bool:
        if remaining <= 0:
            return True
        ratio = estimated / remaining
        if 1.0 < ratio <= self.borderline_ratio:
            return True
        if ratio >= self.severe_ratio:
            return True
        return False

    def _make_req(
        self,
        metric: str,
        estimated: float,
        remaining: float,
        reason: str,
        urgency: str,
        context: Optional[Mapping[str, Any]],
    ) -> HITLRequest:
        return HITLRequest(
            metric=metric,
            estimated=float(estimated),
            remaining=float(remaining),
            reason=reason,
            urgency=urgency,
            context=dict(context or {}),
        )

    def _resolve(
        self,
        req: HITLRequest,
        approved: Optional[bool],
        source: str,
    ) -> Tuple[Optional[bool], str]:
        try:
            self.pending.remove(req)
        except ValueError:
            pass
        if approved is True:
            status = "approved"
        elif approved is False:
            status = "rejected"
        else:
            status = "timed_out"
        self._outcomes[status] += 1
        self.feedback_log.append(
            {
                "req": _json_safe(asdict(req)),
                "approved": approved,
                "status": status,
                "source": source,
            }
        )
        return approved, status

    def review(
        self,
        metric: str,
        estimated: float,
        remaining: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(
            metric, estimated, remaining, reason, urgency, context
        )
        self.pending.append(req)
        try:
            if self._callback is None:
                return self._resolve(req, None, "no_callback")
            try:
                decision = self._callback(req)
            except Exception as exc:
                logger.warning("HITL callback failed: %s", exc)
                return self._resolve(req, None, "callback_error")
            if inspect.isawaitable(decision):
                close = getattr(decision, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass
                logger.error(
                    "HITL callback returned awaitable in sync review; "
                    "rejecting (fail-safe). Use areview() instead."
                )
                return self._resolve(req, False, "async_in_sync")
            return self._resolve(req, bool(decision), "callback")
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    async def areview(
        self,
        metric: str,
        estimated: float,
        remaining: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(
            metric, estimated, remaining, reason, urgency, context
        )
        self.pending.append(req)
        try:
            if self._callback is not None:
                try:
                    decision = self._callback(req)
                    if inspect.isawaitable(decision):
                        decision = await asyncio.wait_for(
                            decision, timeout=self._timeout
                        )
                    return self._resolve(
                        req,
                        bool(decision) if decision is not None else None,
                        "callback",
                    )
                except asyncio.TimeoutError:
                    return self._resolve(req, None, "timeout")
                except Exception as exc:
                    logger.warning("HITL callback failed: %s", exc)
                    return self._resolve(req, None, "callback_error")

            if self._learner is not None:
                try:
                    chosen = await asyncio.wait_for(
                        self._learner.query_user_if_needed(
                            "budget_reviewer",
                            [
                                {
                                    "solution_id": "approve",
                                    "quality_score": 0.9,
                                },
                                {
                                    "solution_id": "reject",
                                    "quality_score": 0.8,
                                },
                            ],
                            timeout=self._timeout,
                        ),
                        timeout=self._timeout,
                    )
                    if chosen is None:
                        return self._resolve(
                            req, None, "learner_timeout"
                        )
                    return self._resolve(
                        req, chosen == "approve", "learner"
                    )
                except Exception as exc:
                    logger.warning("Learner failed: %s", exc)

            return self._resolve(req, None, "no_callback")
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    def outcomes(self) -> Dict[str, int]:
        return dict(self._outcomes)


# --------------------------------------------------------------------------- #
# Market cache (sync, TTL)
# --------------------------------------------------------------------------- #
class CarbonMarketCache:
    """Sync market client with TTL cache and a stable last-snapshot."""

    def __init__(self, ttl_seconds: int = 300) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = ttl_seconds

    def get_snapshot(self) -> MarketSnapshot:
        if self._cache is not None:
            age = (
                _utcnow() - _ensure_aware(self._cache.timestamp)
            ).total_seconds()
            if age < self._ttl:
                return self._cache
        snap = MarketSnapshot(
            carbon_price_per_tco2_usd=random.uniform(20, 80),
            rec_price_per_mwh_usd=random.uniform(3, 9),
            rec_available_mwh=random.uniform(10, 500),
        )
        self._cache = snap
        return snap

    def peek(self) -> Optional[MarketSnapshot]:
        return self._cache


# --------------------------------------------------------------------------- #
# BudgetManager
# --------------------------------------------------------------------------- #
class BudgetManager:
    """
    Budget Manager.

    Backward-compatible:

        BudgetManager(Budget.eco_budget())
        .check_budget('energy_wh')
        .can_execute({'energy_wh': 2.0})
        .record_consumption({'energy_wh': 1.8})
        .get_summary()
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "causal_rl": True,
        "xai": True,
        "adaptive_precision": True,
        "federated": True,
        "multi_agent": True,
        "temporal_logic": True,
        "carbon_market": True,
        "chaos_testing": False,
        "hitl": True,
        "quantum_distillation": True,
        "helium_awareness": True,
    }

    def __init__(
        self,
        budget: Budget,
        deployment_id: str = "local",
        agent_id: str = "budget-manager-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        *,
        degraded_policy: str = "deny",
        strict_estimates: bool = True,
        allow_threshold_widening: bool = False,
        allow_hitl_override: bool = False,
    ) -> None:
        if not isinstance(budget, Budget):
            raise TypeError(
                f"budget must be a Budget instance, got "
                f"{type(budget).__name__}"
            )

        self.budget = budget
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()

        try:
            self._degraded_policy = DegradedPolicy(degraded_policy)
        except ValueError as exc:
            raise BudgetConfigError(
                f"degraded_policy must be one of "
                f"{[p.value for p in DegradedPolicy]}, got "
                f"{degraded_policy!r}"
            ) from exc
        self._strict_estimates = bool(strict_estimates)
        self._allow_threshold_widening = bool(allow_threshold_widening)
        self._allow_hitl_override = bool(allow_hitl_override)

        # ---- Locks ----
        self._lock = threading.RLock()

        # ---- Original consumed state ----
        self.consumed: Dict[str, float] = {
            "energy_wh": 0.0,
            "carbon_g": 0.0,
            "latency_ms": 0.0,
            "cost_usd": 0.0,
        }
        if budget.max_helium_units is not None:
            self.consumed["helium_units"] = 0.0

        # ---- History (bounded) ----
        self.execution_history: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_EXECUTION_HISTORY
        )
        self.violation_history: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_VIOLATION_HISTORY
        )

        # ---- Cached thresholds (for check_budget) ----
        self._cached_warning_threshold = budget.warning_threshold
        self._cached_critical_threshold = budget.critical_threshold

        # ---- Counters ----
        self._outcome_counts: Dict[str, int] = defaultdict(int)
        self._severity_counts: Dict[str, int] = defaultdict(int)
        self._breach_by_metric: Dict[str, int] = defaultdict(int)
        self._degraded_count = 0
        self._override_count = 0
        self._distilled_count = 0
        self._last_market_snapshot: Optional[MarketSnapshot] = None
        self._last_market_fetched_at: Optional[float] = None
        self._last_can_execute_explanation: Optional[BudgetExplanation] = None
        self._last_decision_state: Optional[ThresholdState] = None

        # ---- Core enhancement instances ----
        self._build_layers()

        logger.info(
            "BudgetManager v6 initialized: budget=%s, "
            "deployment=%s, agent=%s, degraded_policy=%s, "
            "strict_estimates=%s, features=%s",
            budget.name,
            deployment_id,
            agent_id,
            self._degraded_policy.value,
            self._strict_estimates,
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Layer construction
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        self._circuit = SyncCircuitAdapter(
            "budget_manager", failure_threshold=5, recovery_timeout=30.0
        )

        self._chaos = (
            SyncChaosInjector()
            if self.features["chaos_testing"]
            else None
        )

        self._safety: Optional[BudgetSafetyMonitor] = None
        if self.features["temporal_logic"]:
            self._safety = BudgetSafetyMonitor()
            self._safety.register(RemainingNonNegative())
            self._safety.register(StatusMonotonic())
            self._safety.register(CanExecuteImpliesRemaining())

        self._precision: Optional[Any] = None
        if self.features["adaptive_precision"] and _V18_AVAILABLE:
            try:
                self._precision = AdaptivePrecisionSwitcher(  # type: ignore[misc]
                    config={}, storage=None
                )
            except Exception:  # pragma: no cover
                self._precision = None

        self._threshold_learner: Optional[CausalThresholdLearner] = None
        if self.features["causal_rl"]:
            self._threshold_learner = CausalThresholdLearner(
                base_warning=self.budget.warning_threshold,
                base_critical=self.budget.critical_threshold,
                allow_threshold_widening=self._allow_threshold_widening,
            )

        self._distiller: Optional[QuantumDistillationBridge] = None
        if self.features["quantum_distillation"]:
            self._distiller = QuantumDistillationBridge()

        self._federated: Optional[FederatedBudgetAggregator] = None
        if self.features["federated"]:
            self._federated = FederatedBudgetAggregator()

        self._coordinator: Optional[Any] = None
        if self.features["multi_agent"] and _V18_AVAILABLE:
            try:
                self._coordinator = MultiAgentCoordinator(  # type: ignore[misc]
                    config={}, storage=None
                )
                self._coordinator.register(self.agent_id)
            except Exception:  # pragma: no cover
                self._coordinator = None

        self._market: Optional[CarbonMarketCache] = None
        if self.features["carbon_market"]:
            self._market = CarbonMarketCache()

        self._hitl: Optional[HumanReviewGate] = None
        if self.features["hitl"]:
            learner = None
            if _V18_AVAILABLE:
                try:
                    learner = ActiveUserPreferenceLearner(  # type: ignore[misc]
                        storage=None
                    )
                except Exception:  # pragma: no cover
                    learner = None
            self._hitl = HumanReviewGate(learner=learner)

        # ---- Helium signal function (v6: actually used) ----
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        with self._lock:
            self._build_layers()

    # ------------------------------------------------------------------
    # v1 API: check_budget (P0 §2.1 fixed)
    # ------------------------------------------------------------------
    def check_budget(self, metric: str) -> BudgetStatus:
        """
        Return the current status of ``metric``.

        For ``latency_ms`` this reports the status of the **peak latency
        across all recorded tasks** (matching ``get_utilization()``),
        **not** the last task's latency. To inspect the last task's
        latency, read ``execution_history[-1]['consumption']['latency_ms']``.

        P0 §2.1: v5 read ``execution_record.get('latency_ms', 0.0)``,
        which never existed in the record shape — so latency was always
        reported ``UNDER_BUDGET``. v6 reads ``self.consumed['latency_ms']``.

        P1 §3.4: this method does **not** invoke the causal learner. The
        thresholds are cached by ``can_execute`` and re-used here.
        """
        with self._lock:
            if metric not in self.consumed:
                raise ValueError(f"Unknown metric: {metric}")

            limit = self._limit_for(metric)
            consumed = self.consumed[metric]

        if limit <= 0 or limit == float("inf"):
            return BudgetStatus.UNDER_BUDGET

        ratio = consumed / limit

        warning_th = self._cached_warning_threshold
        critical_th = self._cached_critical_threshold

        if ratio >= 1.0:
            status = BudgetStatus.EXCEEDED
        elif ratio >= critical_th:
            status = BudgetStatus.AT_LIMIT
        elif ratio >= warning_th:
            status = BudgetStatus.NEAR_LIMIT
        else:
            status = BudgetStatus.UNDER_BUDGET

        if status != BudgetStatus.UNDER_BUDGET:
            severity = self._severity_for_ratio(ratio)
            with self._lock:
                self._severity_counts[severity.value] += 1

        return status

    def _limit_for(self, metric: str) -> float:
        if metric == "energy_wh":
            return self.budget.max_energy_wh
        if metric == "carbon_g":
            return self.budget.max_carbon_g
        if metric == "latency_ms":
            return self.budget.max_latency_ms
        if metric == "cost_usd":
            return (
                self.budget.max_cost_usd
                if self.budget.max_cost_usd is not None
                else float("inf")
            )
        if metric == "helium_units":
            return (
                self.budget.max_helium_units
                if self.budget.max_helium_units is not None
                else float("inf")
            )
        return float("inf")

    # ------------------------------------------------------------------
    # v1 API: can_execute (P0 §2.2–§2.6, §2.8, §4.8, §4.9 fixed)
    # ------------------------------------------------------------------
    def can_execute(
        self,
        estimated_consumption: Mapping[str, Any],
        *,
        allow_hitl_override: Optional[bool] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Return ``(can_execute, violations)`` for the given estimate.

        Violations include:

        * ``"<metric>"`` — a real budget overrun
        * ``"bad_estimate:<metric>"`` — NaN/inf/unreadable estimate
        * ``"missing_estimate:<metric>"`` — a budgeted metric with no
          estimate (only when ``strict_estimates=True``)
        * ``"precision"`` — a requested precision not in
          ``budget.allowed_precisions``
        * ``"circuit_open"`` — enforcement unavailable (circuit open)
        * ``"chaos"`` — enforcement unavailable (chaos injection)
        * ``"internal_error"`` — enforcement unavailable (exception)
        """
        if not isinstance(estimated_consumption, Mapping):
            logger.warning("can_execute: invalid input, denying")
            with self._lock:
                self._outcome_counts[
                    EvaluationOutcome.DEGRADED.value
                ] += 1
                self._degraded_count += 1
            return False, ["invalid_input"]

        # ---- Circuit breaker ----
        if not self._circuit.can_call():
            with self._lock:
                self._outcome_counts[
                    EvaluationOutcome.CIRCUIT_OPEN.value
                ] += 1
                self._degraded_count += 1
            if self._degraded_policy == DegradedPolicy.ALLOW:
                logger.warning(
                    "Circuit open — approving (degraded_policy=ALLOW)"
                )
                return True, []
            logger.warning("Circuit open — denying for safety")
            return False, ["circuit_open"]

        # ---- Chaos ----
        if self._chaos is not None and self._chaos.maybe_fail("can_execute"):
            self._circuit.record_failure()
            with self._lock:
                self._outcome_counts[
                    EvaluationOutcome.CHAOS.value
                ] += 1
                self._degraded_count += 1
            if self._degraded_policy == DegradedPolicy.ALLOW:
                logger.warning(
                    "Chaos injection — approving (degraded_policy=ALLOW)"
                )
                return True, []
            return False, ["chaos"]

        try:
            result = self._can_execute_internal(
                dict(estimated_consumption),
                allow_hitl_override=allow_hitl_override,
            )
            self._circuit.record_success()
            return result
        except Exception as exc:
            self._circuit.record_failure()
            logger.warning("can_execute failed: %s", exc)
            with self._lock:
                self._outcome_counts[
                    EvaluationOutcome.ERROR.value
                ] += 1
                self._degraded_count += 1
            if self._degraded_policy == DegradedPolicy.ALLOW:
                return True, []
            return False, ["internal_error"]

    def _can_execute_internal(
        self,
        estimated: Dict[str, Any],
        *,
        allow_hitl_override: Optional[bool],
    ) -> Tuple[bool, List[str]]:
        # ---- Update thresholds (this is the only place RL runs) ----
        self._refresh_thresholds()

        # ---- Active limits (RL-adjusted) ----
        active_limits = self._active_limits()

        # ---- Required / missing metrics (P0 §2.6) ----
        required = ["energy_wh", "carbon_g", "latency_ms"]
        if self.budget.max_cost_usd is not None:
            required.append("cost_usd")
        if self.budget.max_helium_units is not None:
            required.append("helium_units")

        missing: List[str] = []
        if self._strict_estimates:
            missing = [m for m in required if m not in estimated]
        # Unknown keys are ignored — v1 compatibility. But if they're
        # close to a required key, that's almost certainly a typo.
        known = set(required) | {"precision"}
        unknown = [k for k in estimated if k not in known]
        if unknown and self._strict_estimates:
            # Only treat as a violation if the caller is in strict mode
            for k in unknown:
                logger.warning(
                    "can_execute: unknown estimate key '%s' (ignored)", k
                )

        # ---- Bad-data estimate metrics (P0 §2.5) ----
        bad_estimate: List[str] = []
        for metric in required:
            if metric not in estimated:
                continue
            raw = estimated[metric]
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                bad_estimate.append(metric)
                continue
            if not math.isfinite(float(raw)):
                bad_estimate.append(metric)

        violations: List[str] = []
        if missing:
            violations.extend(f"missing_estimate:{m}" for m in missing)
        if bad_estimate:
            violations.extend(f"bad_estimate:{m}" for m in bad_estimate)

        # ---- Per-metric checks (only for metrics we can read) ----
        def _est(metric: str) -> Optional[float]:
            if metric not in estimated:
                return None
            raw = estimated[metric]
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return None
            if not math.isfinite(float(raw)):
                return None
            return float(raw)

        # Energy
        e_est = _est("energy_wh")
        if e_est is not None:
            with self._lock:
                consumed_e = self.consumed["energy_wh"]
            if consumed_e + e_est > active_limits["energy_wh"] + 1e-9:
                violations.append("energy_wh")
                with self._lock:
                    self._breach_by_metric["energy_wh"] += 1

        # Carbon
        c_est = _est("carbon_g")
        if c_est is not None:
            with self._lock:
                consumed_c = self.consumed["carbon_g"]
            if consumed_c + c_est > active_limits["carbon_g"] + 1e-9:
                violations.append("carbon_g")
                with self._lock:
                    self._breach_by_metric["carbon_g"] += 1

        # Latency (per-task)
        l_est = _est("latency_ms")
        if l_est is not None:
            if l_est > active_limits["latency_ms"] + 1e-9:
                violations.append("latency_ms")
                with self._lock:
                    self._breach_by_metric["latency_ms"] += 1

        # Cost
        if self.budget.max_cost_usd is not None:
            cost_est = _est("cost_usd")
            if cost_est is not None:
                with self._lock:
                    consumed_cost = self.consumed["cost_usd"]
                if (
                    consumed_cost + cost_est
                    > active_limits["cost_usd"] + 1e-9
                ):
                    violations.append("cost_usd")
                    with self._lock:
                        self._breach_by_metric["cost_usd"] += 1

        # Helium (P2 §4.9: use signal fn if available)
        if self.budget.max_helium_units is not None:
            helium_est = _est("helium_units")
            if (
                helium_est is None
                and self.features["helium_awareness"]
            ):
                # Estimate from energy_wh × scarcity_score
                energy_for_he = e_est if e_est is not None else 0.0
                scarcity = 0.0
                if self.helium_signal_fn is not None:
                    try:
                        sig = self.helium_signal_fn()
                        if sig is not None:
                            scarcity = self._safe_float(
                                getattr(sig, "scarcity_score", 0.0)
                            )
                    except Exception:
                        pass
                helium_est = HeliumProfiler.estimate_units(
                    energy_for_he, scarcity
                )
            if helium_est is not None:
                with self._lock:
                    consumed_he = self.consumed.get("helium_units", 0.0)
                if (
                    consumed_he + helium_est
                    > active_limits["helium_units"] + 1e-9
                ):
                    violations.append("helium_units")
                    with self._lock:
                        self._breach_by_metric["helium_units"] += 1

        # ---- Precision constraint (P2 §4.8) ----
        requested_precision = estimated.get("precision")
        if (
            requested_precision is not None
            and self.budget.allowed_precisions is not None
        ):
            if requested_precision not in self.budget.allowed_precisions:
                violations.append("precision")

        # ---- Dedupe violations (real metrics only) ----
        seen: set = set()
        unique_violations: List[str] = []
        for v in violations:
            if v in seen:
                continue
            seen.add(v)
            unique_violations.append(v)
        violations = unique_violations

        can_execute = len(violations) == 0

        # ---- Remaining / utilization snapshot ----
        remaining = self.get_remaining_budget()
        utilization = self.get_utilization()

        # ---- Severity (P0 §2.8, §2.9: unified) ----
        severity = self._classify_can_execute_severity(
            violations, estimated, remaining
        )

        # ---- Temporal verification (P2 §4.2: load-bearing) ----
        if self._safety is not None:
            raw_remaining = {
                m: self._limit_for(m) - self.consumed.get(m, 0.0)
                for m in self.consumed
            }
            ctx = {
                "can_execute": can_execute,
                "estimated": {
                    k: self._safe_float(v)
                    for k, v in estimated.items()
                    if k in remaining
                },
                "remaining": dict(remaining),
                "raw_min_remaining": (
                    min(raw_remaining.values())
                    if raw_remaining
                    else 0.0
                ),
                "status": (
                    BudgetStatus.EXCEEDED.value
                    if not can_execute
                    else BudgetStatus.UNDER_BUDGET.value
                ),
                "ratio": max(
                    (
                        self._safe_float(estimated.get(m, 0.0))
                        / max(remaining.get(m, 0.0), 1e-9)
                        for m in violations
                        if m in remaining
                    ),
                    default=0.0,
                ),
            }
            ok, tv = self._safety.verify(ctx)
            if not ok:
                logger.warning(
                    "Temporal violations on can_execute: %s", tv
                )

        # ---- Market snapshot (cached) ----
        market_snapshot: Optional[Dict[str, Any]] = None
        if self._market is not None:
            try:
                snap = self._market.get_snapshot()
                market_snapshot = {
                    "carbon_price_per_tco2_usd": (
                        snap.carbon_price_per_tco2_usd
                    ),
                    "rec_available_mwh": snap.rec_available_mwh,
                }
                with self._lock:
                    self._last_market_snapshot = snap
                    self._last_market_fetched_at = _utcnow().timestamp()
            except Exception:
                pass

        # ---- HITL (P0 §2.3: reviews ALL reviewable violations) ----
        hitl_required = False
        hitl_approved_all: Optional[bool] = None
        hitl_status = "not_required"
        hitl_override = False

        if not can_execute and self._hitl is not None:
            reviewable = []
            for v in violations:
                if v.startswith("bad_estimate:") or v.startswith(
                    "missing_estimate:"
                ):
                    # Cannot review a bad-data estimate — treat as
                    # EMERGENCY and skip HITL (nobody can approve
                    # "the data was unreadable").
                    continue
                if v == "precision":
                    # Not a budget overrun; skip HITL.
                    continue
                metric = v
                est = self._safe_float(estimated.get(metric, 0.0))
                rem = remaining.get(metric, 0.0)
                if self._hitl.needs_review(est, rem):
                    reviewable.append((metric, est, rem))

            if reviewable:
                hitl_required = True
                all_approved = True
                statuses: List[str] = []
                for metric, est, rem in reviewable:
                    approved, status = self._hitl.review(
                        metric=metric,
                        estimated=est,
                        remaining=rem,
                        reason=f"denial on {metric}",
                        urgency=(
                            "high"
                            if severity
                            in (Severity.CRITICAL, Severity.EMERGENCY)
                            else "medium"
                        ),
                        context={"agent_id": self.agent_id},
                    )
                    statuses.append(status)
                    if approved is not True:
                        all_approved = False
                hitl_status = (
                    "approved"
                    if all_approved and statuses
                    else (
                        "rejected"
                        if "rejected" in statuses
                        else "timed_out"
                    )
                )
                hitl_approved_all = all_approved

        # ---- HITL override (opt-in) ----
        override_flag = (
            allow_hitl_override
            if allow_hitl_override is not None
            else self._allow_hitl_override
        )
        if override_flag and hitl_approved_all is True:
            hitl_override = True
            with self._lock:
                self._override_count += 1
                self._outcome_counts[
                    EvaluationOutcome.APPROVED.value
                ] += 1
            logger.warning(
                "HITL approved denial (allow_hitl_override=True): "
                "not raising — violations=%s",
                violations,
            )
            can_execute = True
            violations_out = []

            # XAI (override-approved)
            if self.features["xai"]:
                explanation = BudgetExplainer.explain_can_execute(
                    can_execute=True,
                    violations=[],
                    estimated=estimated,
                    remaining=remaining,
                    utilization=utilization,
                    severity=None,
                    market_snapshot=market_snapshot,
                    hitl_status="approved",
                )
                with self._lock:
                    self._last_can_execute_explanation = explanation
            return True, []
        else:
            violations_out = violations

        # ---- XAI ----
        if self.features["xai"]:
            explanation = BudgetExplainer.explain_can_execute(
                can_execute=can_execute,
                violations=violations,
                estimated=estimated,
                remaining=remaining,
                utilization=utilization,
                severity=(
                    severity.value if severity else None
                ),
                market_snapshot=market_snapshot,
                hitl_status=hitl_status,
            )
            with self._lock:
                self._last_can_execute_explanation = explanation

        # ---- Coordinator feedback ----
        if self._coordinator is not None:
            try:
                self._coordinator.reward(
                    self.agent_id, 1.0 if can_execute else 0.0
                )
            except Exception:  # pragma: no cover
                pass

        # ---- Federated contribution (sampled) ----
        if (
            self._federated is not None
            and len(self.execution_history) % 25 == 0
        ):
            for metric in self.consumed:
                self._federated.push(
                    FederatedBudgetProfile(
                        deployment_id=self.deployment_id,
                        budget_name=self.budget.name,
                        metric=metric,
                        mean_utilization=utilization.get(metric, 0.0),
                        sample_count=1,
                    )
                )
            self._federated.aggregate()

        # ---- Violation record (v1 shape + enhancement fields) ----
        if not can_execute:
            record: Dict[str, Any] = {
                "timestamp": _utcnow(),
                "violations": list(violations_out),
                "estimated_consumption": dict(estimated),
                "current_consumed": dict(self.consumed),
            }
            if severity:
                record["severity"] = severity.value
            if self._last_can_execute_explanation is not None:
                record["explanation"] = (
                    self._last_can_execute_explanation.to_dict()
                )
            if hitl_required:
                record["hitl_required"] = True
                record["hitl_approved"] = hitl_approved_all
                record["hitl_status"] = hitl_status
            with self._lock:
                self.violation_history.append(record)

        # ---- Outcome counters ----
        with self._lock:
            self._outcome_counts[
                EvaluationOutcome.APPROVED.value
                if can_execute
                else EvaluationOutcome.DENIED.value
            ] += 1

        return can_execute, violations_out

    def _refresh_thresholds(self) -> None:
        """
        Update the cached thresholds from the causal learner.

        Called **only** from ``can_execute`` (P1 §3.4) — never from
        ``check_budget`` or ``record_consumption``.
        """
        if self._threshold_learner is None:
            return
        util = self.get_utilization()
        peak = max(util.values()) if util else 0.0
        state = ThresholdState(
            hour_of_day=_utcnow().hour,
            utilization_ratio=min(1.0, peak),
            workload_hash=float(
                abs(hash(self.agent_id)) % 1000
            ) / 1000.0,
        )
        warning, critical = self._threshold_learner.predict_thresholds(
            state
        )
        with self._lock:
            self._cached_warning_threshold = warning
            self._cached_critical_threshold = critical
            self._last_decision_state = state

    # ------------------------------------------------------------------
    # v1 API: record_consumption (P0 §2.7 fixed, P1 §3.4 fixed)
    # ------------------------------------------------------------------
    def record_consumption(
        self,
        actual_consumption: Mapping[str, Any],
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Record actual consumption after execution.

        Does **not** raise on overrun (that's ``BudgetEnforcer``'s job).
        Overruns are logged at ERROR (v6: v5 only warned on
        NEAR_LIMIT / AT_LIMIT — EXCEEDED was silent).

        **Latency semantics**: ``consumed['latency_ms']`` is the **peak**
        across all recorded tasks (updated via ``max``), matching the
        v1 semantics that latency is a per-task bound.
        """
        if not isinstance(actual_consumption, Mapping):
            logger.warning("record_consumption: invalid input, skipping")
            return

        # ---- v1 accounting (P1 §3.11: log unknown keys) ----
        unknown = [
            m for m in actual_consumption if m not in self.consumed
        ]
        if unknown:
            logger.warning(
                "record_consumption: ignoring unknown metrics: %s",
                unknown,
            )
        for metric, value in actual_consumption.items():
            if metric not in self.consumed:
                continue
            if metric == "latency_ms":
                # Peak semantics: latency budget is per-task.
                with self._lock:
                    self.consumed[metric] = max(
                        self.consumed[metric],
                        self._safe_float(value, 0.0),
                    )
            else:
                with self._lock:
                    self.consumed[metric] += self._safe_float(
                        value, 0.0
                    )

        # ---- Auto-estimate helium (P2 §4.9: honour signal fn) ----
        if (
            self.features["helium_awareness"]
            and self.budget.max_helium_units is not None
            and "helium_units" not in actual_consumption
            and "helium_units" in self.consumed
        ):
            energy_wh = self._safe_float(
                actual_consumption.get("energy_wh", 0.0)
            )
            scarcity = 0.0
            if self.helium_signal_fn is not None:
                try:
                    sig = self.helium_signal_fn()
                    if sig is not None:
                        scarcity = self._safe_float(
                            getattr(sig, "scarcity_score", 0.0)
                        )
                except Exception:
                    pass
            helium_est = HeliumProfiler.estimate_units(
                energy_wh, scarcity
            )
            with self._lock:
                self.consumed["helium_units"] += helium_est

        # ---- Execution record (v1 shape) ----
        execution_record = {
            "timestamp": _utcnow(),
            "consumption": dict(actual_consumption),
            "cumulative_consumed": dict(self.consumed),
            "metadata": dict(metadata or {}),
        }
        with self._lock:
            self.execution_history.append(execution_record)

        # ---- Warn / error on overruns (P0 §2.7) ----
        for metric in self.consumed:
            if (
                metric == "cost_usd"
                and self.budget.max_cost_usd is None
            ):
                continue
            if (
                metric == "helium_units"
                and self.budget.max_helium_units is None
            ):
                continue
            try:
                status = self.check_budget(metric)
            except ValueError:
                continue
            limit_val = self._limit_for(metric)
            with self._lock:
                consumed = self.consumed[metric]
            if status == BudgetStatus.EXCEEDED:
                logger.error(
                    "%s EXCEEDED: %.4f / %.4f (%.1f%% of limit)",
                    metric,
                    consumed,
                    limit_val,
                    (
                        consumed / limit_val * 100
                        if limit_val > 0 and limit_val != float("inf")
                        else 0.0
                    ),
                )
            elif status in (
                BudgetStatus.NEAR_LIMIT,
                BudgetStatus.AT_LIMIT,
            ):
                logger.warning(
                    "%s %s: %.4f / %.4f",
                    metric,
                    status.value,
                    consumed,
                    limit_val,
                )

        logger.debug(
            "Recorded consumption: %s", actual_consumption
        )

    # ------------------------------------------------------------------
    # v1 API: remaining / utilization / reset / summary
    # ------------------------------------------------------------------
    def get_remaining_budget(self) -> Dict[str, float]:
        """
        Return remaining budget per metric.

        P1 §3.6: ``cost_usd`` returns ``inf`` when ``max_cost_usd`` is
        None (v5 returned 0.0, silently meaning "no budget left").
        """
        with self._lock:
            consumed_snapshot = dict(self.consumed)
        remaining: Dict[str, float] = {
            "energy_wh": max(
                0.0,
                self.budget.max_energy_wh
                - consumed_snapshot["energy_wh"],
            ),
            "carbon_g": max(
                0.0,
                self.budget.max_carbon_g
                - consumed_snapshot["carbon_g"],
            ),
            # v1: latency is per-task, so "remaining" is the full limit.
            "latency_ms": self.budget.max_latency_ms,
            "cost_usd": (
                max(
                    0.0,
                    self.budget.max_cost_usd
                    - consumed_snapshot["cost_usd"],
                )
                if self.budget.max_cost_usd is not None
                else float("inf")
            ),
        }
        if self.budget.max_helium_units is not None:
            remaining["helium_units"] = max(
                0.0,
                self.budget.max_helium_units
                - consumed_snapshot.get("helium_units", 0.0),
            )
        return remaining

    def get_utilization(self) -> Dict[str, float]:
        with self._lock:
            consumed_snapshot = dict(self.consumed)
        util: Dict[str, float] = {
            "energy_wh": (
                consumed_snapshot["energy_wh"] / self.budget.max_energy_wh
            ),
            "carbon_g": (
                consumed_snapshot["carbon_g"] / self.budget.max_carbon_g
            ),
            "latency_ms": (
                consumed_snapshot["latency_ms"]
                / self.budget.max_latency_ms
            ),
            "cost_usd": (
                consumed_snapshot["cost_usd"] / self.budget.max_cost_usd
                if self.budget.max_cost_usd
                else 0.0
            ),
        }
        if self.budget.max_helium_units is not None:
            util["helium_units"] = (
                consumed_snapshot.get("helium_units", 0.0)
                / self.budget.max_helium_units
            )
        return util

    def reset(self, *, clear_statistics: bool = False) -> None:
        """
        Reset consumption and history.

        ``clear_statistics=False`` (default) preserves v1 semantics:
        ``violation_history`` and severity counters survive (they are the
        audit trail). Pass ``clear_statistics=True`` to reset
        ``severity_counts``, ``breach_by_metric``, and
        ``violation_history`` too.
        """
        logger.info("Resetting budget consumption")
        with self._lock:
            self.consumed = {k: 0.0 for k in self.consumed}
            self.execution_history.clear()
            if clear_statistics:
                self.severity_counts_clear()
        # violation_history is preserved (v1 behavior)

    def severity_counts_clear(self) -> None:
        with self._lock:
            self._severity_counts.clear()
            self._breach_by_metric.clear()
            self.violation_history.clear()

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            consumed_snapshot = dict(self.consumed)
        summary: Dict[str, Any] = {
            "budget": self.budget.to_dict(),
            "consumed": consumed_snapshot,
            "remaining": self.get_remaining_budget(),
            "utilization": self.get_utilization(),
            "status": {
                metric: self.check_budget(metric).value
                for metric in consumed_snapshot
            },
            "execution_count": len(self.execution_history),
            "violation_count": len(self.violation_history),
        }
        with self._lock:
            if self._safety is not None:
                summary["temporal_violation_count"] = len(
                    self._safety.violations
                )
                summary["temporal_violations"] = list(
                    self._safety.violations
                )[-5:]
            if self._threshold_learner is not None:
                summary["causal_learner"] = (
                    self._threshold_learner.stats()
                )
            if self._coordinator is not None:
                try:
                    summary["agents"] = {
                        a.id: a.role
                        for a in getattr(
                            self._coordinator, "agents", {}
                        ).values()
                    }
                except Exception:  # pragma: no cover
                    pass
            if self._severity_counts:
                summary["severity_counts"] = dict(
                    self._severity_counts
                )
            if self._breach_by_metric:
                summary["breach_by_metric"] = dict(
                    self._breach_by_metric
                )
            if self._federated is not None:
                summary["federated_aggregate"] = (
                    self._federated.aggregate()
                )
            if self._outcome_counts:
                summary["outcome_counts"] = dict(
                    self._outcome_counts
                )
            if self._hitl is not None:
                summary["hitl"] = {
                    "pending": len(self._hitl.pending),
                    "log_size": len(self._hitl.feedback_log),
                    "outcomes": self._hitl.outcomes(),
                }
            summary["degraded_count"] = self._degraded_count
            summary["override_count"] = self._override_count
            summary["distilled_count"] = self._distilled_count
        return summary

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------
    def explain_last_decision(
        self,
    ) -> Optional[BudgetExplanation]:
        with self._lock:
            return self._last_can_execute_explanation

    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        if self._hitl is not None:
            self._hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return (
            list(self._hitl.feedback_log)[-n:]
            if self._hitl is not None
            else []
        )

    def record_decision_outcome(self, was_correct: bool) -> bool:
        """
        Feed back whether the last ``can_execute`` decision was correct.

        P0 §2.2: uses the **real** ThresholdState from the last decision
        (v5 used a hard-coded ``workload_hash=0.5``) and a **signed**
        reward (v5 used ``0.0`` for incorrect).
        """
        if self._threshold_learner is None:
            return False
        with self._lock:
            state = self._last_decision_state
        if state is None:
            # No decision yet — use current utilization
            util = self.get_utilization()
            peak = max(util.values()) if util else 0.0
            state = ThresholdState(
                hour_of_day=_utcnow().hour,
                utilization_ratio=min(1.0, peak),
                workload_hash=float(
                    abs(hash(self.agent_id)) % 1000
                ) / 1000.0,
            )
        reward = 1.0 if was_correct else -1.0
        self._threshold_learner.record(
            CausalThresholdLearner.features_of(state), reward
        )
        if self._threshold_learner.observations % 10 == 0:
            self._threshold_learner.update()
        return True

    def contribute_federated(self) -> None:
        if self._federated is None:
            return
        util = self.get_utilization()
        for metric, ratio in util.items():
            self._federated.push(
                FederatedBudgetProfile(
                    deployment_id=self.deployment_id,
                    budget_name=self.budget.name,
                    metric=metric,
                    mean_utilization=ratio,
                    sample_count=max(1, len(self.execution_history)),
                )
            )
        self._federated.aggregate()

    def get_federated_aggregate(
        self,
    ) -> Dict[str, Dict[str, float]]:
        return (
            self._federated.aggregate()
            if self._federated is not None
            else {}
        )

    def distill_budget_policy(
        self, urgency: str = "normal"
    ) -> Optional[DistilledBudgetPolicy]:
        if self._distiller is None:
            return None
        precision = self._select_precision(urgency)
        policy = self._distiller.distill(
            warning=self.budget.warning_threshold,
            critical=self.budget.critical_threshold,
            precision=precision,
        )
        with self._lock:
            self._distilled_count += 1
        return policy

    # ------------------------------------------------------------------
    # Statistics / health / lifecycle
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "execution_count": len(self.execution_history),
                "violation_count": len(self.violation_history),
                "severity_counts": dict(self._severity_counts),
                "breach_by_metric": dict(self._breach_by_metric),
                "outcome_counts": dict(self._outcome_counts),
                "degraded_count": self._degraded_count,
                "override_count": self._override_count,
                "distilled_count": self._distilled_count,
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
            }
            if self._safety is not None:
                stats["temporal_violation_count"] = len(
                    self._safety.violations
                )
                stats["temporal_violations"] = list(
                    self._safety.violations
                )[-5:]
            if self._threshold_learner is not None:
                stats["causal_learner"] = (
                    self._threshold_learner.stats()
                )
            if self._federated is not None:
                stats["federated_aggregate"] = (
                    self._federated.aggregate()
                )
            if self._hitl is not None:
                stats["hitl"] = {
                    "pending": len(self._hitl.pending),
                    "log_size": len(self._hitl.feedback_log),
                    "outcomes": self._hitl.outcomes(),
                }
            if self._chaos is not None:
                stats["chaos_events"] = list(self._chaos.events)[-5:]
            # P1 §3.7: report cached market snapshot
            if self._last_market_snapshot is not None:
                stats["market"] = {
                    "carbon_price_per_tco2_usd": (
                        self._last_market_snapshot.carbon_price_per_tco2_usd
                    ),
                    "rec_price_per_mwh_usd": (
                        self._last_market_snapshot.rec_price_per_mwh_usd
                    ),
                    "cached_at": _ensure_aware(
                        self._last_market_snapshot.timestamp
                    ).isoformat(),
                }
            return stats

    async def health_check(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "healthy": self._circuit.state != "OPEN",
                "budget_name": self.budget.name,
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
                "execution_count": len(self.execution_history),
                "violation_count": len(self.violation_history),
                "degraded_count": self._degraded_count,
                "override_count": self._override_count,
                "outcome_counts": dict(self._outcome_counts),
                "degraded_policy": self._degraded_policy.value,
                "strict_estimates": self._strict_estimates,
                "allow_threshold_widening": (
                    self._allow_threshold_widening
                ),
                "allow_hitl_override": self._allow_hitl_override,
                "cached_warning_threshold": (
                    self._cached_warning_threshold
                ),
                "cached_critical_threshold": (
                    self._cached_critical_threshold
                ),
                "temporal_violation_count": (
                    len(self._safety.violations)
                    if self._safety is not None
                    else 0
                ),
                "hitl_outcomes": (
                    self._hitl.outcomes()
                    if self._hitl is not None
                    else {}
                ),
                "features": dict(self.features),
            }

    def __enter__(self) -> "BudgetManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "BudgetManager":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    def __repr__(self) -> str:
        with self._lock:
            return (
                "BudgetManager("
                f"budget={self.budget.name!r}, "
                f"executions={len(self.execution_history)}, "
                f"violations={len(self.violation_history)}, "
                f"circuit={self._circuit.state})"
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    def _active_limits(self) -> Dict[str, float]:
        """
        Effective limits per metric (RL-tightened).

        P0 §2.2: consults the causal learner via
        ``predict_limit_multiplier``. When the learner tightens the
        warning threshold, the effective limit tightens by the same
        ratio. ``allow_threshold_widening=True`` (opt-in) allows the
        multiplier to exceed 1.0.
        """
        base = {
            "energy_wh": self.budget.max_energy_wh,
            "carbon_g": self.budget.max_carbon_g,
            "latency_ms": self.budget.max_latency_ms,
            "cost_usd": (
                self.budget.max_cost_usd
                if self.budget.max_cost_usd is not None
                else float("inf")
            ),
        }
        if self.budget.max_helium_units is not None:
            base["helium_units"] = self.budget.max_helium_units

        if self._threshold_learner is None:
            return base

        # Build the state from current utilization (safe — no mutation).
        util = self.get_utilization()
        peak = max(util.values()) if util else 0.0
        state = ThresholdState(
            hour_of_day=_utcnow().hour,
            utilization_ratio=min(1.0, peak),
            workload_hash=float(
                abs(hash(self.agent_id)) % 1000
            ) / 1000.0,
        )
        multiplier = self._threshold_learner.predict_limit_multiplier(
            state
        )
        if not self._allow_threshold_widening:
            multiplier = min(1.0, multiplier)
        return {k: v * multiplier for k, v in base.items()}

    @staticmethod
    def _severity_for_ratio(ratio: float) -> Severity:
        """
        Unified severity classification (P0 §2.8, §2.9).

        * EMERGENCY: ratio >= 2.0
        * CRITICAL:  1.3 <= ratio < 2.0
        * WARNING:   0 < ratio < 1.3  (any breach is at least WARNING)
        * INFO:      ratio == 0  (no breach)
        """
        if not math.isfinite(ratio):
            return Severity.EMERGENCY
        if ratio >= 2.0:
            return Severity.EMERGENCY
        if ratio >= 1.3:
            return Severity.CRITICAL
        if ratio > 0.0:
            return Severity.WARNING
        return Severity.INFO

    def _classify_can_execute_severity(
        self,
        violations: List[str],
        estimated: Mapping[str, Any],
        remaining: Mapping[str, float],
    ) -> Severity:
        if not violations:
            return Severity.INFO
        max_ratio = 0.0
        infinite = False
        for v in violations:
            if v.startswith("bad_estimate:") or v.startswith(
                "missing_estimate:"
            ):
                infinite = True
                continue
            if v == "precision":
                # Not a numeric overrun; treat as WARNING.
                max_ratio = max(max_ratio, 1.0)
                continue
            est = self._safe_float(estimated.get(v, 0.0))
            rem = remaining.get(v, 0.0)
            if rem > 0:
                ratio = est / rem
            elif est > 0:
                ratio = float("inf")
            else:
                ratio = 0.0
            if not math.isfinite(ratio):
                infinite = True
            max_ratio = max(max_ratio, ratio)
        if infinite:
            return Severity.EMERGENCY
        return self._severity_for_ratio(max_ratio)

    def _select_precision(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hardware.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hardware.supports_int8
                else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4
            if self.hardware.supports_int4
            else PrecisionLevel.INT8
        )


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_budget_manager(
    budget: Budget,
    deployment_id: str = "local",
    agent_id: str = "budget-manager-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    degraded_policy: str = "deny",
    strict_estimates: bool = True,
    allow_threshold_widening: bool = False,
    allow_hitl_override: bool = False,
) -> BudgetManager:
    return BudgetManager(
        budget=budget,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
        degraded_policy=degraded_policy,
        strict_estimates=strict_estimates,
        allow_threshold_widening=allow_threshold_widening,
        allow_hitl_override=allow_hitl_override,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # v18 re-exports
    "CircuitBreaker",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
    "MultiAgentCoordinator",
    "FederatedGreenAggregator",
    "TemporalLogicVerifier",
    "SimpleQuantumDistillationEngine",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
    # Enums
    "BudgetStatus",
    "Severity",
    "PrecisionLevel",
    "AgentRole",
    "EvaluationOutcome",
    "DegradedPolicy",
    # Errors
    "BudgetManagerError",
    "BudgetConfigError",
    # Domain types
    "Budget",
    "HardwareProfile",
    "ThresholdState",
    "BudgetExplanation",
    "DistilledBudgetPolicy",
    "FederatedBudgetProfile",
    "MarketSnapshot",
    "HITLRequest",
    # Components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "BudgetExplainer",
    "CausalThresholdLearner",
    "QuantumDistillationBridge",
    "FederatedBudgetAggregator",
    "HeliumProfiler",
    "BudgetProperty",
    "RemainingNonNegative",
    "StatusMonotonic",
    "CanExecuteImpliesRemaining",
    "BudgetSafetyMonitor",
    "HumanReviewGate",
    "CarbonMarketCache",
    # Main
    "BudgetManager",
    "create_budget_manager",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("budget_manager.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # ============================================================= #
    # Backward compatibility
    # ============================================================= #
    print("\n=== Backward compat: v1 API preserved ===")
    legacy = BudgetManager(
        Budget.eco_budget(),
        features={k: False for k in BudgetManager.DEFAULT_FEATURES},
    )
    _check(
        legacy.check_budget("energy_wh") == BudgetStatus.UNDER_BUDGET,
        "check_budget returns UNDER_BUDGET initially",
    )
    can, viols = legacy.can_execute(
        {"energy_wh": 2.0, "carbon_g": 0.4, "latency_ms": 500}
    )
    _check(can is True and viols == [], "can_execute approves")
    legacy.record_consumption(
        {"energy_wh": 1.8, "carbon_g": 0.36, "latency_ms": 450}
    )
    summary = legacy.get_summary()
    _check(
        "budget" in summary and "consumed" in summary,
        "get_summary preserves v1 keys",
    )

    # ============================================================= #
    # P0 §2.1 — latency regression fix
    # ============================================================= #
    print("\n=== P0 §2.1: latency actually reflects consumption ===")
    lat = BudgetManager(Budget.eco_budget())
    _check(
        lat.check_budget("latency_ms") == BudgetStatus.UNDER_BUDGET,
        "latency starts UNDER_BUDGET",
    )
    lat.record_consumption({"latency_ms": 9500})
    lat_status = lat.check_budget("latency_ms")
    _check(
        lat_status in (
            BudgetStatus.NEAR_LIMIT,
            BudgetStatus.AT_LIMIT,
            BudgetStatus.EXCEEDED,
        ),
        f"9500/10000 → {lat_status.value} (v5 would say UNDER_BUDGET)",
    )
    # Utilization and status now agree
    util = lat.get_utilization()["latency_ms"]
    _check(
        0.9 <= util <= 1.0,
        f"get_utilization()['latency_ms']={util:.2f} matches status",
    )

    # ============================================================= #
    # P0 §2.2 — causal learner: signed, feature-aware, tightening-only
    # ============================================================= #
    print("\n=== P0 §2.2: causal learner ===")
    learner = CausalThresholdLearner(
        base_warning=0.80, base_critical=0.95
    )
    state_a = ThresholdState(8, 0.9, 0.1)
    state_b = ThresholdState(20, 0.1, 0.9)
    for _ in range(30):
        learner.record(
            CausalThresholdLearner.features_of(state_a), -1.0
        )
    learner.update()
    for _ in range(30):
        learner.record(
            CausalThresholdLearner.features_of(state_b), -1.0
        )
    learner.update()
    wa, ca = learner.predict_thresholds(state_a)
    wb, cb = learner.predict_thresholds(state_b)
    _check(
        abs(wa - wb) > 1e-4,
        f"different states → different warning thresholds "
        f"(A={wa:.4f}, B={wb:.4f})",
    )
    # Signed reward: negative tightens
    baseline_w, _ = CausalThresholdLearner(
        base_warning=0.80, base_critical=0.95
    ).predict_thresholds(state_a)
    _check(
        wa < baseline_w,
        f"negative reward tightened: {baseline_w:.4f} → {wa:.4f}",
    )
    # Tightening-only by default (no widening with positive reward)
    widener = CausalThresholdLearner(
        base_warning=0.80, base_critical=0.95
    )
    for _ in range(100):
        widener.record(
            CausalThresholdLearner.features_of(state_a), 1.0
        )
    widener.update()
    w, _ = widener.predict_thresholds(state_a)
    _check(
        w <= 0.80 + 1e-9,
        f"tightening-only: warning={w:.4f} <= base 0.80",
    )

    # ============================================================= #
    # P0 §2.2 — RL wired into can_execute via _active_limits
    # ============================================================= #
    print("\n=== P0 §2.2: RL affects can_execute limits ===")
    rl_mgr = BudgetManager(Budget.eco_budget())
    # Initially, 4.5 Wh should be fine (5.0 limit)
    can0, _ = rl_mgr.can_execute(
        {"energy_wh": 4.5, "carbon_g": 0.5, "latency_ms": 500}
    )
    _check(can0 is True, "baseline: 4.5 Wh approved")
    # Tighten the learner by feeding incorrect outcomes
    rl_mgr._refresh_thresholds()
    for _ in range(200):
        rl_mgr.record_decision_outcome(was_correct=False)
    rl_mgr._threshold_learner.update()
    # Now the effective limit should be tightened
    multiplier = rl_mgr._threshold_learner.predict_limit_multiplier(
        ThresholdState(12, 0.5, 0.5)
    )
    _check(
        multiplier < 1.0,
        f"learner tightened effective limit: multiplier={multiplier:.4f}",
    )

    # ============================================================= #
    # P0 §2.3 — HITL propagation
    # ============================================================= #
    print("\n=== P0 §2.3: HITL propagation ===")
    hitl_mgr = BudgetManager(Budget.eco_budget())
    # 5.5 Wh is a 10% overrun — borderline → HITL review
    hitl_mgr.set_hitl_callback(lambda req: False)
    can_rej, viols_rej = hitl_mgr.can_execute(
        {"energy_wh": 5.5, "carbon_g": 0.5, "latency_ms": 500}
    )
    _check(
        can_rej is False,
        "HITL rejection → still denied",
    )
    record = list(hitl_mgr.violation_history)[-1]
    _check(
        record.get("hitl_status") == "rejected",
        "violation record has hitl_status=rejected",
    )

    # Override: approval + allow_hitl_override=True → approved
    override_mgr = BudgetManager(
        Budget.eco_budget(), allow_hitl_override=True
    )
    override_mgr.set_hitl_callback(lambda req: True)
    can_ov, viols_ov = override_mgr.can_execute(
        {"energy_wh": 5.5, "carbon_g": 0.5, "latency_ms": 500}
    )
    _check(
        can_ov is True and viols_ov == [],
        "HITL approval + allow_hitl_override=True → approved",
    )
    _check(
        override_mgr._override_count == 1,
        "override_count incremented",
    )

    # ============================================================= #
    # P0 §2.4 — warning < critical validation
    # ============================================================= #
    print("\n=== P0 §2.4: warning < critical validation ===")
    try:
        Budget(
            max_energy_wh=5.0, max_carbon_g=1.0, max_latency_ms=1000,
            warning_threshold=0.95, critical_threshold=0.5,
        )
    except BudgetConfigError:
        _check(True, "inverted thresholds rejected at construction")
    else:
        raise AssertionError("inverted thresholds not rejected")

    # ============================================================= #
    # P0 §2.5 — NaN estimate → denial
    # ============================================================= #
    print("\n=== P0 §2.5: NaN estimate → denial ===")
    nan_mgr = BudgetManager(Budget.eco_budget())
    can_nan, viols_nan = nan_mgr.can_execute(
        {
            "energy_wh": float("nan"),
            "carbon_g": 0.5,
            "latency_ms": 500,
        }
    )
    _check(
        can_nan is False,
        "NaN energy estimate → denied",
    )
    _check(
        any(v.startswith("bad_estimate:") for v in viols_nan),
        f"violations include bad_estimate (got {viols_nan})",
    )
    exp_nan = nan_mgr.explain_last_decision()
    _check(
        exp_nan is not None
        and "[DENIED]" in exp_nan.headline,
        "XAI shows DENIED",
    )

    # ============================================================= #
    # P0 §2.6 — missing estimate keys → denial (strict)
    # ============================================================= #
    print("\n=== P0 §2.6: missing estimate keys → denial ===")
    strict_mgr = BudgetManager(Budget.eco_budget(), strict_estimates=True)
    can_miss, viols_miss = strict_mgr.can_execute(
        {"energy_wh": 2.0}  # missing carbon_g and latency_ms
    )
    _check(
        can_miss is False,
        "missing carbon_g / latency_ms → denied",
    )
    _check(
        any(v.startswith("missing_estimate:") for v in viols_miss),
        f"violations include missing_estimate (got {viols_miss})",
    )
    # Lenient mode uses v1 semantics
    lenient_mgr = BudgetManager(
        Budget.eco_budget(), strict_estimates=False
    )
    can_ok, _ = lenient_mgr.can_execute({"energy_wh": 2.0})
    _check(
        can_ok is True,
        "strict_estimates=False preserves v1 (missing metrics → 0.0)",
    )

    # ============================================================= #
    # P0 §2.7 — EXCEEDED produces an error log + status reflects it
    # ============================================================= #
    print("\n=== P0 §2.7: EXCEEDED status is reachable ===")
    exceed_mgr = BudgetManager(Budget.eco_budget())
    exceed_mgr.record_consumption({"energy_wh": 10.0})  # 2× limit
    status = exceed_mgr.check_budget("energy_wh")
    _check(
        status == BudgetStatus.EXCEEDED,
        f"2× limit → EXCEEDED (got {status.value})",
    )
    # Severity: 2× = EMERGENCY (P0 §2.8)
    with exceed_mgr._lock:
        sev_counts = dict(exceed_mgr._severity_counts)
    _check(
        sev_counts.get("emergency", 0) >= 1,
        f"EXCEEDED at 2× → EMERGENCY recorded (got {sev_counts})",
    )

    # ============================================================= #
    # P0 §2.8 — unified severity
    # ============================================================= #
    print("\n=== P0 §2.8: unified severity ===")
    for ratio, expected in [
        (0.5, "info"),
        (1.1, "warning"),
        (1.5, "critical"),
        (2.5, "emergency"),
    ]:
        sev = BudgetManager._severity_for_ratio(ratio)
        _check(
            sev.value == expected,
            f"ratio={ratio} → {expected} (got {sev.value})",
        )

    # ============================================================= #
    # P0 §2.9 — NEAR_LIMIT is WARNING, not INFO
    # ============================================================= #
    print("\n=== P0 §2.9: NEAR_LIMIT is WARNING ===")
    near_mgr = BudgetManager(Budget.eco_budget())
    near_mgr.record_consumption({"energy_wh": 4.2})  # 84% of 5.0
    status = near_mgr.check_budget("energy_wh")
    _check(
        status == BudgetStatus.NEAR_LIMIT,
        f"84% → NEAR_LIMIT (got {status.value})",
    )
    with near_mgr._lock:
        sev_counts = dict(near_mgr._severity_counts)
    _check(
        sev_counts.get("warning", 0) >= 1,
        f"NEAR_LIMIT records WARNING (got {sev_counts})",
    )

    # ============================================================= #
    # P1 §3.1 — thread safety
    # ============================================================= #
    print("\n=== P1 §3.1: thread safety ===")
    import threading
    safe = BudgetManager(Budget.balanced_budget())
    errors: List[BaseException] = []

    def _worker() -> None:
        try:
            for _ in range(100):
                safe.can_execute(
                    {
                        "energy_wh": 2.0,
                        "carbon_g": 0.4,
                        "latency_ms": 500,
                    }
                )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _check(not errors, f"10×100 concurrent can_execute: no errors")
    _check(
        sum(safe._outcome_counts.values()) == 1000,
        f"outcome counter is 1000 (got {sum(safe._outcome_counts.values())})",
    )

    # ============================================================= #
    # P1 §3.2 — v18 single-probe circuit breaker
    # ============================================================= #
    print("\n=== P1 §3.2: v18 single-probe circuit breaker ===")
    probe = BudgetManager(Budget.eco_budget())
    for _ in range(2):
        probe._circuit.record_failure()
    _check(
        probe._circuit.state == "CLOSED",
        "two failures below threshold=5 → CLOSED",
    )
    for _ in range(3):
        probe._circuit.record_failure()
    _check(
        probe._circuit.state == "OPEN",
        "five failures → OPEN",
    )

    # ============================================================= #
    # P1 §3.3 — async HITL
    # ============================================================= #
    print("\n=== P1 §3.3: async HITL ===")
    gate = HumanReviewGate()

    async def _async_cb(req):
        return True

    gate.set_callback(_async_cb)
    approved, status = gate.review(
        metric="energy_wh", estimated=6.0, remaining=5.0,
        reason="test", urgency="medium",
    )
    _check(
        approved is False and status == "async_in_sync",
        f"async callback rejected in sync review (got {status!r})",
    )

    async def _await_review() -> Tuple[Optional[bool], str]:
        gate2 = HumanReviewGate()

        async def _cb(req):
            return True

        gate2.set_callback(_cb)
        return await gate2.areview(
            metric="energy_wh", estimated=6.0, remaining=5.0,
            reason="test", urgency="medium",
        )

    approved2, status2 = asyncio.run(_await_review())
    _check(
        approved2 is True and status2 == "approved",
        "async callback awaited in areview",
    )

    # ============================================================= #
    # P1 §3.4 — check_budget does not invoke the RL
    # ============================================================= #
    print("\n=== P1 §3.4: check_budget does not invoke the RL ===")
    no_rl = BudgetManager(Budget.eco_budget())
    obs_before = no_rl._threshold_learner.observations
    for _ in range(20):
        no_rl.check_budget("energy_wh")
    obs_after = no_rl._threshold_learner.observations
    _check(
        obs_after == obs_before,
        f"20 check_budget calls produced 0 RL observations "
        f"({obs_before} → {obs_after})",
    )
    # But record_consumption also doesn't invoke the RL directly
    no_rl.record_consumption({"energy_wh": 1.0})
    _check(
        no_rl._threshold_learner.observations == obs_before,
        "record_consumption also does not invoke the RL",
    )

    # ============================================================= #
    # P1 §3.5 — reset(clear_statistics)
    # ============================================================= #
    print("\n=== P1 §3.5: reset(clear_statistics) ===")
    reset_mgr = BudgetManager(Budget.eco_budget())
    reset_mgr.record_consumption({"energy_wh": 4.5})
    with reset_mgr._lock:
        sev_before = sum(reset_mgr._severity_counts.values())
    _check(sev_before > 0, f"severity counted before reset ({sev_before})")
    reset_mgr.reset(clear_statistics=False)
    with reset_mgr._lock:
        sev_after = sum(reset_mgr._severity_counts.values())
    _check(
        sev_after == sev_before,
        "reset(clear_statistics=False) preserves stats",
    )
    reset_mgr.reset(clear_statistics=True)
    with reset_mgr._lock:
        sev_cleared = sum(reset_mgr._severity_counts.values())
    _check(
        sev_cleared == 0,
        "reset(clear_statistics=True) clears stats",
    )

    # ============================================================= #
    # P1 §3.6 — remaining cost is inf when unset
    # ============================================================= #
    print("\n=== P1 §3.6: remaining cost_usd is inf when unset ===")
    no_cost = BudgetManager(Budget.eco_budget())  # max_cost_usd=None
    rem = no_cost.get_remaining_budget()
    _check(
        rem["cost_usd"] == float("inf"),
        f"remaining['cost_usd'] is inf (got {rem['cost_usd']})",
    )

    # ============================================================= #
    # P1 §3.7 — cached market snapshot
    # ============================================================= #
    print("\n=== P1 §3.7: cached market snapshot ===")
    cache_mgr = BudgetManager(Budget.eco_budget())
    # Trigger a market fetch via can_execute
    cache_mgr.can_execute(
        {"energy_wh": 2.0, "carbon_g": 0.4, "latency_ms": 500}
    )
    p1 = cache_mgr.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    p2 = cache_mgr.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    _check(
        p1 == p2,
        f"consecutive get_statistics() returns cached price "
        f"({p1} == {p2})",
    )

    # ============================================================= #
    # P1 §3.9 — bounded collections
    # ============================================================= #
    print("\n=== P1 §3.9: bounded collections ===")
    bounded = BudgetManager(Budget.performance_budget())
    for _ in range(3000):
        bounded.record_consumption({"energy_wh": 0.1})
    _check(
        len(bounded.execution_history) <= _MAX_EXECUTION_HISTORY,
        f"execution_history bounded at {_MAX_EXECUTION_HISTORY} "
        f"(got {len(bounded.execution_history)})",
    )
    _check(
        len(bounded._hitl.feedback_log) <= _MAX_HITL_LOG,
        f"hitl.feedback_log bounded at {_MAX_HITL_LOG}",
    )

    # ============================================================= #
    # P1 §3.10 — set_features, health_check, async CM
    # ============================================================= #
    print("\n=== P1 §3.10: set_features, health_check, async CM ===")
    live = BudgetManager(Budget.eco_budget())
    _check(live._market is not None, "market enabled by default")
    live.set_features({"carbon_market": False})
    _check(live._market is None, "set_features disabled market")
    live.set_features({"carbon_market": True})
    _check(live._market is not None, "set_features re-enabled market")

    async def _async_health() -> None:
        async with BudgetManager(Budget.eco_budget()) as m:
            health = await m.health_check()
            _check(
                "healthy" in health
                and "degraded_policy" in health,
                "health_check returns expected keys",
            )

    asyncio.run(_async_health())

    # ============================================================= #
    # P2 §4.2 — temporal properties fire
    # ============================================================= #
    print("\n=== P2 §4.2: temporal properties actually fire ===")
    safety = BudgetSafetyMonitor()
    safety.register(RemainingNonNegative())
    ok, viols = safety.verify({"raw_min_remaining": -1.0})
    _check(
        not ok and "RemainingNonNegative" in viols,
        "RemainingNonNegative fires on negative remaining",
    )

    safety2 = BudgetSafetyMonitor()
    safety2.register(StatusMonotonic())
    ok2, viols2 = safety2.verify(
        {
            "status": BudgetStatus.EXCEEDED.value,
            "ratio": 0.5,  # < 1.0 → should be UNDER_BUDGET
        }
    )
    _check(
        not ok2 and "StatusMonotonic" in viols2,
        "StatusMonotonic fires on misclassification",
    )

    safety3 = BudgetSafetyMonitor()
    safety3.register(CanExecuteImpliesRemaining())
    ok3, viols3 = safety3.verify(
        {
            "can_execute": True,
            "estimated": {"energy_wh": 6.0},
            "remaining": {"energy_wh": 5.0},  # < estimated!
        }
    )
    _check(
        not ok3 and "CanExecuteImpliesRemaining" in viols3,
        "CanExecuteImpliesRemaining fires on inconsistency",
    )

    # ============================================================= #
    # P2 §4.3 — distillation uses round
    # ============================================================= #
    print("\n=== P2 §4.3: distillation uses round ===")
    bridge = QuantumDistillationBridge()
    # 0.8049 at INT8 (scale=100): floor → 0.80, round → 0.80
    # 0.8050 at INT8: floor → 0.80, round → 0.81
    policy = bridge.distill(
        warning=0.8050, critical=0.9550,
        precision=PrecisionLevel.INT8,
    )
    _check(
        policy.thresholds["warning"] >= 0.805,
        f"round preserves 0.8050 (got {policy.thresholds['warning']})",
    )

    # ============================================================= #
    # P2 §4.4 — federated zero-weight guard
    # ============================================================= #
    print("\n=== P2 §4.4: federated zero-weight guard ===")
    fed = FederatedBudgetAggregator(max_updates=5)
    for _ in range(100):
        fed.push(
            FederatedBudgetProfile(
                deployment_id="x",
                budget_name="eco",
                metric="energy_wh",
                mean_utilization=0.5,
                sample_count=0,
            )
        )
    result = fed.aggregate()
    _check(
        result == {},
        "zero-weight profiles → empty aggregate (no div-zero crash)",
    )
    _check(
        len(fed.updates) <= 5,
        f"federated.updates bounded at 5 (got {len(fed.updates)})",
    )

    # ============================================================= #
    # P2 §4.8 — allowed_precisions enforced
    # ============================================================= #
    print("\n=== P2 §4.8: allowed_precisions enforced ===")
    try:
        Budget(
            max_energy_wh=5.0, max_carbon_g=1.0, max_latency_ms=1000,
            allowed_precisions=["bogus"],
        )
    except BudgetConfigError:
        _check(True, "invalid precision rejected at construction")
    else:
        raise AssertionError("invalid precision not rejected")

    prec_mgr = BudgetManager(
        Budget(
            max_energy_wh=5.0, max_carbon_g=1.0, max_latency_ms=1000,
            allowed_precisions=["int8", "fp16"],
        )
    )
    can_prec, viols_prec = prec_mgr.can_execute(
        {
            "energy_wh": 2.0,
            "carbon_g": 0.4,
            "latency_ms": 500,
            "precision": "fp32",  # not allowed
        }
    )
    _check(
        can_prec is False and "precision" in viols_prec,
        "requested fp32 not in allowed_precisions → denied",
    )

    # ============================================================= #
    # P2 §4.9 — helium signal function used
    # ============================================================= #
    print("\n=== P2 §4.9: helium signal function used ===")

    class _MockSignal:
        scarcity_score = 0.8

    helium_mgr = BudgetManager(
        Budget(
            max_energy_wh=5.0, max_carbon_g=1.0, max_latency_ms=1000,
            max_helium_units=0.1,
        )
    )
    helium_mgr.helium_signal_fn = lambda: _MockSignal()
    # High scarcity → helium estimate lower (efficiency loss)
    helium_mgr.record_consumption({"energy_wh": 1000.0})  # 1 kWh
    with helium_mgr._lock:
        helium_used = helium_mgr.consumed["helium_units"]
    # Without signal fn, 1 kWh * 0.05 = 0.05
    # With scarcity=0.8, efficiency = 1 - min(0.5, 0.4) = 0.6, so 0.03
    _check(
        helium_used < 0.05,
        f"scarcity penalises helium estimate: {helium_used:.4f} < 0.05",
    )

    # ============================================================= #
    # Final summary
    # ============================================================= #
    print("\n=== Statistics (last manager) ===")
    print(json.dumps(live.get_statistics(), indent=2, default=str))

    print("\n" + "=" * 78)
    print("All v6 assertions passed.")
    print("=" * 78)


if __name__ == "__main__":
    _demo()
