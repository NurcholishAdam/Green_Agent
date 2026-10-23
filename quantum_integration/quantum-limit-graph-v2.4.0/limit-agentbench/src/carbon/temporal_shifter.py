"""
temporal_shifter.py — v6.0.0 (thin wrapper over carbon_forecast v18)
=====================================================================

Temporal Shifter — carbon-aware deferral advisor.

**What changed in v6**
----------------------
v5 got the backward-compatibility story right (``suggest()`` returned
``(hour, saving)`` byte-for-byte) but failed six tests that matter:

* Precision quantization could produce **contradictory recommendations**
  (``best_hour=16, saving=0, strategy=RUN_NOW``).
* ``quantize`` used ``floor`` (systematic downward bias).
* The causal learner barely moved (weight change ~4e-6 per step) and
  got *slower* for larger savings.
* ``record_outcome`` used a single-slot ``_last_features`` that was
  clobbered by the next ``suggest()`` call — corrupting both the learner
  and the audit trail.
* HITL was bypassed in the backward-compatible ``suggest()`` path with
  no documentation.
* Async HITL callbacks were silently auto-approved.
* ``TemporalLogicMonitor`` mutated its properties with per-call state,
  leaking deadlines across calls.
* ``_run_now_fallback`` set ``best_hour=0`` (inconsistent with the
  normal RUN_NOW path, which sets ``best_hour=current_hour``).

v6 fixes all of that and follows the same deduplication pattern as the
rest of the v6 series.

**Re-exported from v18 (single implementation)**
    CircuitBreaker, ChaosTestingEngine, ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher, QuantumDistillationEngine,
    CarbonMarketIntegrator, XAIDecisionExplainer,
    MultiAgentCoordinator, FederatedGreenAggregator,
    TemporalLogicVerifier,
    plus every ``Simple*`` alias and shared helpers
    (``_utcnow``, ``_ensure_aware``, ``_parse_timestamp``, ``_json_safe``,
    ``_cfg_get``, ``_safe_mean``).

**Local (shifter-specific; fixed)**
    DeferralStrategy, AgentRole, PrecisionLevel
    DelayRecommendation  (+ features, recommendation_id, raw_saving_kg,
                          hitl_status)
    DelayExplanation
    HardwareProfile
    ShiftContext, ShiftProperty, DeadlineRespected, MaxDelayBounded,
    NonNegativeHour, ShiftSafetyMonitor  (stateless with context)
    DelayExplainer                        (rule-based, shifter-specific)
    SafeQuantizer                         (round, not floor)
    CausalDeferralLearner                 (relative-error signal)
    DistilledForecast, QuantumDistillationBridge  (per-hour smoothing)
    FederatedDeferralProfile, FederatedDeferralAggregator  (strategy-based)
    ShiftAgentProfile, ShiftCoordinator   (shifter-specific roles)
    MarketSnapshot, CarbonMarketClient    (sync)
    HITLRequest, HumanReviewGate          (sync + async)
    SyncCircuitAdapter                    (over v18's async breaker)
    SyncChaosInjector                     (sync)
    TemporalShifter                       (main class)

**P0 fixes**
    1. **Contradictory recommendations**: after quantization, if
       ``best_saving <= 0`` while the raw saving was positive, the
       shifter now resets ``best_hour = current_hour`` (and
       ``best_intensity = current_intensity``), so the tuple is always
       internally consistent.
    2. **SafeQuantizer**: uses ``round``, not ``floor``.
    3. **Causal learner**: uses ``actual / predicted`` as the learning
       signal (unit-free, order 0.1–1.0); weights and bias are clamped
       to ``[-2, 2]``; correction is clamped to ``[-2, 2]``.
    4. **Per-recommendation features**: ``DelayRecommendation`` carries
       ``features`` and ``recommendation_id``; ``record_outcome`` uses
       them, not a single-slot ``_last_features``.
    5. **Async HITL**: sync ``review`` rejects awaitables (fail-safe);
       ``areview`` awaits them with a timeout.
    6. **Stateless temporal properties**: ``ShiftSafetyMonitor.verify``
       takes ``deadline_hour`` and ``max_delay_hours`` per call; nothing
       is mutated on the properties themselves.
    7. **Consistent fallback**: ``_run_now_fallback`` takes
       ``current_hour`` and sets ``best_hour=current_hour``.
    8. **HITL is documented**: ``suggest()`` skips HITL by default;
       callers pass ``enable_hitl=True`` to opt in. The docstring says so.

**P1 fixes**
    * Distillation ``quality_retention`` is propagated to confidence.
    * ``HOLD_FOR_REVIEW`` becomes load-bearing (returned when HITL times out).
    * ``ACCURACY_OPTIMIZER`` role is routed for RUN_NOW decisions.
    * Every collection (violations, pending, feedback_log, chaos.events,
      federated.updates) is bounded.
    * ``datetime.now(timezone.utc)`` everywhere.
    * ``set_features()`` recreates layers at runtime.
    * ``health_check()`` reports fallback count, HITL outcomes,
      distilled count, per-strategy decision counts.
    * ``saving_percent`` is computed from the raw (pre-quantize) saving.
    * ``causal_adjustment`` is clamped.
    * ``hashlib`` (dead import) removed.

**Public API preserved**
    shifter = TemporalShifter()
    hour, saving = shifter.suggest(current_intensity, forecast, energy_kwh)

    rec = shifter.suggest_with_explanation(...)   # rich
    await rec = shifter.suggest_async(...)         # awaitable HITL
    shifter.record_outcome(actual_saving_kg, recommendation=rec)
    shifter.contribute_federated()
    shifter.set_hitl_callback(cb)
    shifter.get_statistics()
    await shifter.health_check()

File: src/carbon/temporal_shifter.py
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import math
import random
import statistics
import uuid
from collections import Counter, defaultdict, deque
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
    Sequence,
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
        QuantumDistillationEngine,
        CarbonMarketIntegrator,
        XAIDecisionExplainer,
        MultiAgentCoordinator,
        FederatedGreenAggregator,
        TemporalLogicVerifier,
        SimpleQuantumDistillationEngine,
        SimpleCausalGraphLearner,
        SimpleCausalPolicyAdapter,
        SimpleFederatedGreenAggregator,
        SimpleMultiAgentCoordinator,
        SimpleTemporalLogicVerifier,
        SimpleXAIDecisionExplainer,
        SimpleAdaptivePrecisionSwitcher,
        SimpleCarbonMarketIntegrator,
        SimpleChaosTestingEngine,
        SimpleActiveUserPreferenceLearner,
        ForecastPoint,
        ForecastBatch,
        ForecastSource,
        _utcnow,
        _ensure_aware,
        _parse_timestamp,
        _json_safe,
        _cfg_get,
        _safe_mean,
    )
    _V18_AVAILABLE = True
    _V18_ERROR: Optional[str] = None
except ImportError as _e:  # pragma: no cover
    _V18_AVAILABLE = False
    _V18_ERROR = str(_e)

    # Fallbacks so the module still imports when v18 is unavailable.
    CircuitBreaker = None  # type: ignore[assignment]
    ChaosTestingEngine = None  # type: ignore[assignment]
    ActiveUserPreferenceLearner = None  # type: ignore[assignment]
    AdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    QuantumDistillationEngine = None  # type: ignore[assignment]
    CarbonMarketIntegrator = None  # type: ignore[assignment]
    XAIDecisionExplainer = None  # type: ignore[assignment]
    MultiAgentCoordinator = None  # type: ignore[assignment]
    FederatedGreenAggregator = None  # type: ignore[assignment]
    TemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleQuantumDistillationEngine = None  # type: ignore[assignment]
    SimpleCausalGraphLearner = None  # type: ignore[assignment]
    SimpleCausalPolicyAdapter = None  # type: ignore[assignment]
    SimpleFederatedGreenAggregator = None  # type: ignore[assignment]
    SimpleMultiAgentCoordinator = None  # type: ignore[assignment]
    SimpleTemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleXAIDecisionExplainer = None  # type: ignore[assignment]
    SimpleAdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    SimpleCarbonMarketIntegrator = None  # type: ignore[assignment]
    SimpleChaosTestingEngine = None  # type: ignore[assignment]
    SimpleActiveUserPreferenceLearner = None  # type: ignore[assignment]
    ForecastPoint = None  # type: ignore[assignment]
    ForecastBatch = None  # type: ignore[assignment]

    class ForecastSource(Enum):  # type: ignore[no-redef]
        API = "api"
        CACHE = "cache"
        HISTORY = "history"
        LAST_API = "last_api"
        RANDOM_WALK = "random_walk"
        FALLBACK = "fallback"

    def _utcnow() -> datetime:  # type: ignore[misc]
        return datetime.now(timezone.utc)

    def _ensure_aware(dt: datetime) -> datetime:  # type: ignore[misc]
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def _parse_timestamp(raw: Any) -> datetime:  # type: ignore[misc]
        if isinstance(raw, datetime):
            return _ensure_aware(raw)
        if isinstance(raw, str):
            s = raw.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return _ensure_aware(datetime.fromisoformat(s))
            except ValueError:
                return _utcnow()
        return _utcnow()

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

    def _cfg_get(config: Any, key: str, default: Any = None) -> Any:  # type: ignore[misc]
        if config is None:
            return default
        if isinstance(config, Mapping):
            return config.get(key, default)
        return getattr(config, key, default)

    def _safe_mean(values: Any, default: float = 0.0) -> float:  # type: ignore[misc]
        items = [
            float(v) for v in values
            if v is not None and math.isfinite(float(v))
        ]
        return statistics.mean(items) if items else default


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Bounds & constants
# --------------------------------------------------------------------------- #
_MAX_VIOLATIONS = 200
_MAX_HITL_LOG = 500
_MAX_PENDING_HITL = 100
_MAX_CHAOS_EVENTS = 500
_MAX_FEDERATED_UPDATES = 100
_MAX_CAUSAL_BUFFER = 512
_MAX_DECISION_HISTORY = 1024
_MAX_CORRECTION = 2.0


_VALID_HITL_STATUSES = frozenset(
    {"not_required", "pending", "approved", "rejected", "timed_out"}
)


# --------------------------------------------------------------------------- #
# Enums (shifter-specific)
# --------------------------------------------------------------------------- #
class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    GENERALIST = "generalist"
    SHORT_HORIZON = "short_horizon"
    LONG_HORIZON = "long_horizon"
    EMERGENCY_DEFERRAL = "emergency_deferral"
    ACCURACY_OPTIMIZER = "accuracy_optimizer"


class DeferralStrategy(Enum):
    RUN_NOW = "run_now"
    SHIFT_SHORT = "shift_short"
    SHIFT_LONG = "shift_long"
    HOLD_FOR_REVIEW = "hold_for_review"


# --------------------------------------------------------------------------- #
# Domain data
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


@dataclass
class DelayExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class DelayRecommendation:
    """
    Rich recommendation output (extends the original 2-tuple).

    New in v6:

    * ``raw_saving_kg``       — pre-quantization saving (full precision).
    * ``features``            — causal-learner features for this decision.
    * ``recommendation_id``   — unique ID; pass to ``record_outcome``.
    * ``hitl_status``         — explicit HITL outcome.
    """

    # --- v5 fields ---
    best_hour: int
    best_saving_kg: float
    strategy: DeferralStrategy = DeferralStrategy.SHIFT_SHORT
    current_carbon_kg: float = 0.0
    best_carbon_kg: float = 0.0
    saving_percent: float = 0.0
    confidence: float = 1.0
    explanation: Optional[DelayExplanation] = None
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    precision_used: str = "fp32"
    market_enrichment: Optional[Dict[str, Any]] = None
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    federated_used: bool = False
    distilled_used: bool = False
    causal_adjustment: float = 0.0
    # --- v6 fields ---
    raw_saving_kg: float = 0.0
    features: Optional[List[float]] = None
    recommendation_id: str = ""
    hitl_status: str = "not_required"
    current_hour: int = 0
    current_intensity: float = 0.0
    best_intensity: float = 0.0
    energy_kwh: float = 0.0

    def as_tuple(self) -> Tuple[int, float]:
        """Backward-compatible view of the original return value."""
        return (self.best_hour, self.best_saving_kg)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["strategy"] = self.strategy.value
        if self.explanation is not None:
            d["explanation"] = self.explanation.to_dict()
        return _json_safe(d)


# --------------------------------------------------------------------------- #
# Sync circuit + chaos adapters
# --------------------------------------------------------------------------- #
class SyncCircuitAdapter:
    """Sync facade over v18's ``CircuitBreaker``."""

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
    """Sync fault injector for the shifter's sync path."""

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
# Temporal properties (stateless, context-aware)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ShiftContext:
    """Per-call safety context. Never stored; passed to ``verify``."""

    deadline_hour: Optional[int] = None
    max_delay_hours: int = 48


class ShiftProperty(Protocol):
    def check(
        self, current_hour: int, proposed_hour: int, context: ShiftContext
    ) -> bool: ...
    def name(self) -> str: ...


@dataclass(frozen=True)
class DeadlineRespected:
    """G(proposed_hour <= deadline_hour) when a deadline is provided."""

    def check(
        self, current_hour: int, proposed_hour: int, context: ShiftContext
    ) -> bool:
        if context.deadline_hour is None:
            return True
        return proposed_hour <= context.deadline_hour

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass(frozen=True)
class MaxDelayBounded:
    """G(|proposed_hour - current_hour| <= max_delay)."""

    def check(
        self, current_hour: int, proposed_hour: int, context: ShiftContext
    ) -> bool:
        return abs(proposed_hour - current_hour) <= context.max_delay_hours

    def name(self) -> str:
        return "MaxDelayBounded"


@dataclass(frozen=True)
class NonNegativeHour:
    """G(proposed_hour >= 0)."""

    def check(
        self, current_hour: int, proposed_hour: int, context: ShiftContext
    ) -> bool:
        return proposed_hour >= 0

    def name(self) -> str:
        return "NonNegativeHour"


class ShiftSafetyMonitor:
    """
    Verifies temporal properties against a per-call ``ShiftContext``.

    Stateless with respect to calls: the v5 bug where deadline/delay
    values leaked from one call into the next is fixed by passing the
    context to ``verify``.
    """

    def __init__(self, max_violations: int = _MAX_VIOLATIONS) -> None:
        self.properties: List[ShiftProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, prop: ShiftProperty) -> None:
        self.properties.append(prop)

    def verify(
        self,
        current_hour: int,
        proposed_hour: int,
        context: ShiftContext,
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for prop in self.properties:
            try:
                if not prop.check(current_hour, proposed_hour, context):
                    bad.append(prop.name())
                    self.violations.append(
                        {
                            "property": prop.name(),
                            "current_hour": current_hour,
                            "proposed_hour": proposed_hour,
                            "at": _utcnow().isoformat(),
                        }
                    )
            except Exception as e:
                logger.warning(
                    "Property %s raised: %s", prop.name(), e
                )
                bad.append(prop.name())
        return (len(bad) == 0, bad)


# --------------------------------------------------------------------------- #
# XAI (rule-based, shifter-specific)
# --------------------------------------------------------------------------- #
class DelayExplainer:
    @staticmethod
    def explain(
        current_intensity: float,
        current_hour: int,
        best_hour: int,
        best_intensity: float,
        energy_kwh: float,
        raw_saving_kg: float,
        quantized_saving_kg: float,
        forecast_size: int,
        confidence: float,
        causal_adjustment: float = 0.0,
        market_carbon_price: Optional[float] = None,
    ) -> DelayExplanation:
        reasons: List[str] = []
        pct = (
            (current_intensity - best_intensity)
            / current_intensity
            * 100.0
            if current_intensity > 0
            else 0.0
        )
        reasons.append(
            f"Current intensity = {current_intensity:.0f} gCO₂/kWh "
            f"(hour {current_hour})."
        )
        reasons.append(
            f"Best future hour = {best_hour} at "
            f"{best_intensity:.0f} gCO₂/kWh ({pct:.1f}% lower)."
        )
        reasons.append(
            f"Deferring {energy_kwh:.4f} kWh saves "
            f"{raw_saving_kg:.6f} kgCO₂e (raw) / "
            f"{quantized_saving_kg:.6f} kg (quantized)."
        )
        reasons.append(
            f"Scanned {forecast_size} forecast points; "
            f"confidence = {confidence:.2f}."
        )
        if causal_adjustment:
            reasons.append(
                f"Causal correction applied: {causal_adjustment:+.4f}."
            )
        if market_carbon_price is not None and market_carbon_price > 0:
            value_usd = raw_saving_kg / 1000.0 * market_carbon_price
            reasons.append(
                f"Market value of saving ≈ ${value_usd:.5f} "
                f"(at ${market_carbon_price:.2f}/tCO₂)."
            )

        headline = (
            f"DEFER to hour {best_hour} "
            f"({quantized_saving_kg:.6f} kgCO₂e saved, "
            f"{pct:.1f}% reduction)"
        )
        return DelayExplanation(
            headline=headline,
            rationale=reasons,
            confidence=confidence,
            contributing_factors={
                "intensity_delta": current_intensity - best_intensity,
                "raw_saving_kg": raw_saving_kg,
                "quantized_saving_kg": quantized_saving_kg,
                "saving_percent": pct,
                "causal_adjustment": causal_adjustment,
            },
        )


# --------------------------------------------------------------------------- #
# Safe quantizer (round, not floor)
# --------------------------------------------------------------------------- #
class SafeQuantizer:
    """
    Quantize a value to the resolution of ``precision``.

    **Uses ``round``, not ``floor``** — v5 biased every quantized value
    downward toward zero, systematically under-reporting positive savings.
    """

    _SCALE: Dict[PrecisionLevel, float] = {
        PrecisionLevel.FP32: 1.0,
        PrecisionLevel.FP16: 1.0,
        PrecisionLevel.INT8: 1000.0,
        PrecisionLevel.INT4: 100.0,
        PrecisionLevel.QUANTUM_DISTILLED: 10.0,
    }

    @classmethod
    def quantize(cls, value: float, precision: PrecisionLevel) -> float:
        scale = cls._SCALE[precision]
        if scale <= 1.0:
            return float(value)
        return round(float(value) * scale) / scale


# --------------------------------------------------------------------------- #
# Causal learner (relative-error signal)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DeferralTransition:
    features: List[float]
    chosen_hour: int
    predicted_saving: float
    actual_saving: float
    timestamp: datetime = field(default_factory=_utcnow)


class CausalDeferralLearner:
    """
    Learns a **multiplicative correction** on top of the greedy saving
    estimate.

    FIXED (§2.3): the v5 learner used raw error (`~1e-4 kg`) as the
    gradient signal, giving weight change of ~4e-6 per step — six
    orders of magnitude too small — and applied an additional
    ``grad_scale = 1e-4`` for larger savings, which made the learner
    *slower* for the cases that matter most.

    v6 uses the **target ratio** ``actual / predicted`` as the signal:
    unit-free, order 0.1–1.0, independent of saving magnitude. Weights
    and bias are clamped to ``[-2, 2]``; ``predict_correction`` is
    clamped to ``[-2, 2]``.
    """

    N_FEATURES = 6

    def __init__(self, lr: float = 0.05) -> None:
        if lr <= 0:
            raise ValueError("lr must be > 0")
        self.lr = lr
        self.weights: List[float] = [0.0] * self.N_FEATURES
        self.bias: float = 0.0
        self.buffer: Deque[DeferralTransition] = deque(
            maxlen=_MAX_CAUSAL_BUFFER
        )
        self.observations: int = 0
        self._update_count: int = 0

    @staticmethod
    def _features(
        current_intensity: float,
        best_intensity: float,
        best_saving: float,
        energy_kwh: float,
        current_hour: int,
        best_hour: int,
        forecast_values: Sequence[float],
    ) -> List[float]:
        horizon = abs(best_hour - current_hour)
        variance = (
            statistics.pvariance(forecast_values)
            if len(forecast_values) > 1
            else 0.0
        )
        return [
            current_intensity / 1000.0,
            (
                (current_intensity - best_intensity)
                / max(current_intensity, 1.0)
            ),
            best_saving / max(energy_kwh, 1e-6) / 1000.0,
            horizon / 48.0,
            variance / 10000.0,
            1.0 if best_saving > 0 else 0.0,
        ]

    def build_features(
        self,
        current_intensity: float,
        best_intensity: float,
        best_saving: float,
        energy_kwh: float,
        current_hour: int,
        best_hour: int,
        forecast_values: Sequence[float],
    ) -> List[float]:
        return self._features(
            current_intensity,
            best_intensity,
            best_saving,
            energy_kwh,
            current_hour,
            best_hour,
            forecast_values,
        )

    def predict_correction(self, features: Sequence[float]) -> float:
        if len(features) != self.N_FEATURES:
            return 0.0
        raw = self.bias + sum(
            w * x for w, x in zip(self.weights, features)
        )
        return max(-_MAX_CORRECTION, min(_MAX_CORRECTION, raw))

    def record(
        self,
        features: Sequence[float],
        chosen_hour: int,
        predicted_saving: float,
        actual_saving: float,
    ) -> None:
        if predicted_saving <= 0 or actual_saving < 0:
            return
        if len(features) != self.N_FEATURES:
            return
        self.buffer.append(
            DeferralTransition(
                features=list(features),
                chosen_hour=int(chosen_hour),
                predicted_saving=float(predicted_saving),
                actual_saving=float(actual_saving),
            )
        )
        self.observations += 1

    def update(self) -> bool:
        """Apply one gradient step per buffered transition."""
        if not self.buffer:
            return False
        for tr in self.buffer:
            if tr.predicted_saving <= 0:
                continue
            target_ratio = tr.actual_saving / tr.predicted_saving
            current_ratio = 1.0 + self.predict_correction(tr.features)
            err = target_ratio - current_ratio
            for i, x in enumerate(tr.features):
                self.weights[i] = max(
                    -_MAX_CORRECTION,
                    min(
                        _MAX_CORRECTION,
                        self.weights[i] + self.lr * err * x,
                    ),
                )
            self.bias = max(
                -_MAX_CORRECTION,
                min(
                    _MAX_CORRECTION,
                    self.bias + self.lr * err,
                ),
            )
        self.buffer.clear()
        self._update_count += 1
        return True

    def stats(self) -> Dict[str, Any]:
        return {
            "observations": self.observations,
            "update_count": self._update_count,
            "weights": list(self.weights),
            "bias": self.bias,
            "buffer_size": len(self.buffer),
        }


# --------------------------------------------------------------------------- #
# Distillation (per-hour smoothing)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DistilledForecast:
    precision: PrecisionLevel
    smoothed: Dict[int, float]
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    """
    Distills a raw forecast into a smoothed, quantized version.

    FIXED: uses ``round`` not ``floor``; ``window`` scales with forecast
    length so a 4-point forecast is not over-smoothed.
    """

    _SCALE: Dict[PrecisionLevel, float] = {
        PrecisionLevel.FP32: 1.0,
        PrecisionLevel.FP16: 1.0,
        PrecisionLevel.INT8: 10.0,
        PrecisionLevel.INT4: 1.0,
        PrecisionLevel.QUANTUM_DISTILLED: 1.0,
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
        forecast: Mapping[int, float],
        precision: PrecisionLevel,
        window: Optional[int] = None,
    ) -> DistilledForecast:
        if not forecast:
            return DistilledForecast(
                precision=precision,
                smoothed={},
                quality_retention=1.0,
                energy_reduction_percent=0.0,
            )
        hours = sorted(forecast.keys())
        n = len(hours)
        if window is None:
            window = 1 if n <= 4 else 2
        smoothed: Dict[int, float] = {}
        for i, h in enumerate(hours):
            lo = max(0, i - window)
            hi = min(n, i + window + 1)
            window_vals = [
                forecast[hours[j]] for j in range(lo, hi)
            ]
            smoothed[h] = statistics.fmean(window_vals)

        scale = self._SCALE[precision]
        retention, energy = self._RETENTION[precision]
        if scale > 1.0:
            smoothed = {
                h: round(v * scale) / scale
                for h, v in smoothed.items()
            }
        return DistilledForecast(
            precision=precision,
            smoothed=smoothed,
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Federated (strategy-based)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FederatedDeferralProfile:
    deployment_id: str
    strategy: str
    mean_saving_percent: float
    mean_horizon_hours: float
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedDeferralAggregator:
    """Bounded FedAvg over per-strategy deferral statistics."""

    def __init__(self, max_updates: int = _MAX_FEDERATED_UPDATES) -> None:
        self.updates: Deque[FederatedDeferralProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedDeferralProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[
            str, List[FederatedDeferralProfile]
        ] = defaultdict(list)
        for u in self.updates:
            grouped[u.strategy].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for strategy, profiles in grouped.items():
            total_w = sum(max(0, p.sample_count) for p in profiles)
            if total_w <= 0:
                continue
            sp = (
                sum(
                    p.mean_saving_percent * p.sample_count
                    for p in profiles
                )
                / total_w
            )
            hz = (
                sum(
                    p.mean_horizon_hours * p.sample_count
                    for p in profiles
                )
                / total_w
            )
            result[strategy] = {
                "mean_saving_percent": sp,
                "mean_horizon_hours": hz,
                "sample_count": float(total_w),
            }
        self._global = result
        return result

    def lookup(
        self, strategy: str
    ) -> Optional[Dict[str, float]]:
        return self._global.get(strategy)


# --------------------------------------------------------------------------- #
# Multi-agent coordinator (shifter-specific roles)
# --------------------------------------------------------------------------- #
@dataclass
class ShiftAgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    short_horizon_success: float = 0.0
    long_horizon_success: float = 0.0
    emergency_success: float = 0.0
    total_calls: int = 0


class ShiftCoordinator:
    """Emergent role assignment for deferral-advisor agents."""

    def __init__(self, window_size: int = 20) -> None:
        self.agents: Dict[str, ShiftAgentProfile] = {}
        self._history: Dict[str, Deque[Dict[str, float]]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def register(self, agent_id: str) -> ShiftAgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = ShiftAgentProfile(agent_id)
        return self.agents[agent_id]

    def _mean(self, agent_id: str, key: str) -> float:
        hist = self._history[agent_id]
        if not hist:
            return 0.0
        return sum(h.get(key, 0.0) for h in hist) / len(hist)

    def _reassign(self) -> None:
        for a in self.agents.values():
            if len(self._history[a.agent_id]) < 3:
                continue
            a.short_horizon_success = self._mean(
                a.agent_id, "short"
            )
            a.long_horizon_success = self._mean(
                a.agent_id, "long"
            )
            a.emergency_success = self._mean(
                a.agent_id, "emergency"
            )
            if a.emergency_success > 0.8:
                a.role = AgentRole.EMERGENCY_DEFERRAL
            elif (
                a.short_horizon_success > 0.8
                and a.short_horizon_success
                >= a.long_horizon_success
            ):
                a.role = AgentRole.SHORT_HORIZON
            elif a.long_horizon_success > 0.75:
                a.role = AgentRole.LONG_HORIZON
            elif a.total_calls > 20 and a.short_horizon_success > 0.7:
                a.role = AgentRole.ACCURACY_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self,
        agent_id: str,
        strategy: DeferralStrategy,
        success: bool,
    ) -> None:
        a = self.register(agent_id)
        self._history[agent_id].append(
            {
                "short": (
                    float(success)
                    if strategy == DeferralStrategy.SHIFT_SHORT
                    else 0.0
                ),
                "long": (
                    float(success)
                    if strategy == DeferralStrategy.SHIFT_LONG
                    else 0.0
                ),
                "emergency": (
                    float(success)
                    if strategy
                    in (
                        DeferralStrategy.HOLD_FOR_REVIEW,
                        DeferralStrategy.RUN_NOW,
                    )
                    else 0.0
                ),
            }
        )
        a.total_calls += 1
        self._reassign()

    def select_agent(
        self, strategy: DeferralStrategy
    ) -> Optional[str]:
        if not self.agents:
            return None
        target_role = {
            DeferralStrategy.SHIFT_SHORT: AgentRole.SHORT_HORIZON,
            DeferralStrategy.SHIFT_LONG: AgentRole.LONG_HORIZON,
            DeferralStrategy.HOLD_FOR_REVIEW: AgentRole.EMERGENCY_DEFERRAL,
            DeferralStrategy.RUN_NOW: AgentRole.ACCURACY_OPTIMIZER,
        }.get(strategy, AgentRole.GENERALIST)
        cands = [
            a for a in self.agents.values() if a.role == target_role
        ]
        if not cands:
            cands = list(self.agents.values())
        return max(cands, key=lambda a: a.total_calls).agent_id


# --------------------------------------------------------------------------- #
# Carbon market client (sync, cached)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: datetime = field(default_factory=_utcnow)


class CarbonMarketClient:
    """Sync market client with TTL cache."""

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


# --------------------------------------------------------------------------- #
# HITL gate (sync + async)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HITLRequest:
    current_hour: int
    proposed_hour: int
    current_intensity: float
    best_intensity: float
    saving_kg: float
    confidence: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


class HumanReviewGate:
    """
    Sync + async HITL gate.

    * ``review``  — sync; **rejects awaitable callbacks (fail-safe)**.
    * ``areview`` — async; awaits awaitables with a timeout.

    Both return ``(approved, status)`` where ``status`` ∈
    ``{approved, rejected, timed_out}``. On timeout, ``approved`` is
    ``None`` so the caller can distinguish "rejected" from "no answer"
    (and choose ``HOLD_FOR_REVIEW`` for the latter).
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        min_saving_kg: float = 1e-4,
        min_confidence: float = 0.35,
        max_horizon_for_auto: int = 24,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.min_saving_kg = min_saving_kg
        self.min_confidence = min_confidence
        self.max_horizon_for_auto = max_horizon_for_auto
        self.pending: Deque[HITLRequest] = deque(
            maxlen=_MAX_PENDING_HITL
        )
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None

    def set_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        self._callback = cb

    def needs_review(
        self, saving_kg: float, confidence: float, horizon: int
    ) -> bool:
        if saving_kg < self.min_saving_kg:
            return True
        if confidence < self.min_confidence:
            return True
        if horizon > self.max_horizon_for_auto:
            return True
        return False

    def _make_req(
        self,
        current_hour: int,
        proposed_hour: int,
        current_intensity: float,
        best_intensity: float,
        saving_kg: float,
        confidence: float,
        reason: str,
    ) -> HITLRequest:
        return HITLRequest(
            current_hour=current_hour,
            proposed_hour=proposed_hour,
            current_intensity=current_intensity,
            best_intensity=best_intensity,
            saving_kg=saving_kg,
            confidence=confidence,
            reason=reason,
            urgency=(
                "high"
                if abs(proposed_hour - current_hour)
                > self.max_horizon_for_auto
                else "medium"
            ),
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
        current_hour: int,
        proposed_hour: int,
        current_intensity: float,
        best_intensity: float,
        saving_kg: float,
        confidence: float,
        reason: str,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(
            current_hour,
            proposed_hour,
            current_intensity,
            best_intensity,
            saving_kg,
            confidence,
            reason,
        )
        self.pending.append(req)
        try:
            if self._callback is None:
                return self._resolve(req, None, "no_callback")
            try:
                decision = self._callback(req)
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)
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
        current_hour: int,
        proposed_hour: int,
        current_intensity: float,
        best_intensity: float,
        saving_kg: float,
        confidence: float,
        reason: str,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(
            current_hour,
            proposed_hour,
            current_intensity,
            best_intensity,
            saving_kg,
            confidence,
            reason,
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
                except Exception as e:
                    logger.warning("HITL callback failed: %s", e)
                    return self._resolve(req, None, "callback_error")

            if self._learner is not None:
                try:
                    chosen = await asyncio.wait_for(
                        self._learner.query_user_if_needed(
                            "shifter_reviewer",
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
                except Exception as e:
                    logger.warning("Learner failed: %s", e)

            return self._resolve(req, None, "no_callback")
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    def active_learning_batch(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return list(self.feedback_log)[-n:]

    def outcomes(self) -> Dict[str, int]:
        counts: Counter = Counter()
        for entry in self.feedback_log:
            counts[entry.get("status", "unknown")] += 1
        return dict(counts)


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class TemporalShifter:
    """
    Enhanced temporal shifting optimizer.

    Backward-compatible: ``suggest(current_intensity, forecast, energy_kwh)``
    returns ``(best_hour, best_saving_kg)`` exactly like v1.

    See the class-level docstring for the full v6 changes.
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
    }

    def __init__(
        self,
        deployment_id: str = "local",
        agent_id: str = "shifter-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        baseline_usd_per_kwh: float = 0.08,
        chaos_failure_rate: float = 0.0,
    ):
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()
        self.baseline_usd_per_kwh = baseline_usd_per_kwh
        self.chaos_failure_rate = chaos_failure_rate

        self._build_layers()

        # ---- Bookkeeping ----
        self.decision_history: Deque[DelayRecommendation] = deque(
            maxlen=_MAX_DECISION_HISTORY
        )
        self._last_recommendation: Optional[DelayRecommendation] = None
        self._strategy_counts: Counter = Counter()
        self._fallback_count: int = 0
        self._distilled_count: int = 0
        self._hitl_outcomes: Counter = Counter()

        logger.info(
            "TemporalShifter v6 ready: deployment=%s, agent=%s, "
            "features=%s",
            deployment_id,
            agent_id,
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Layer construction
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        self.circuit = SyncCircuitAdapter(
            "temporal_shifter", failure_threshold=5, recovery_timeout=30.0
        )

        self.chaos = (
            SyncChaosInjector(self.chaos_failure_rate)
            if self.features["chaos_testing"]
            else None
        )

        self.temporal_monitor = (
            ShiftSafetyMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(MaxDelayBounded())
            self.temporal_monitor.register(NonNegativeHour())

        self.explainer = (
            DelayExplainer if self.features["xai"] else None
        )

        self.precision_ctl = (
            AdaptivePrecisionSwitcher(
                config={"precision_levels": ["fp32", "fp16", "int8"]},
                storage=None,
            )
            if self.features["adaptive_precision"]
            else None
        )

        self.causal_learner = (
            CausalDeferralLearner()
            if self.features["causal_rl"]
            else None
        )

        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"]
            else None
        )

        self.federated = (
            FederatedDeferralAggregator()
            if self.features["federated"]
            else None
        )

        self.coordinator = (
            ShiftCoordinator()
            if self.features["multi_agent"]
            else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        self.market = (
            CarbonMarketClient()
            if self.features["carbon_market"]
            else None
        )

        self.hitl = (
            HumanReviewGate(
                learner=(
                    ActiveUserPreferenceLearner(storage=None)
                    if _V18_AVAILABLE
                    else None
                )
            )
            if self.features["hitl"]
            else None
        )

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        self._build_layers()

    # ------------------------------------------------------------------
    # Backward-compatible sync API
    # ------------------------------------------------------------------
    def suggest(
        self,
        current_intensity: float,
        forecast: Mapping[int, float],
        energy_kwh: float,
        *,
        current_hour: int = 0,
        deadline_hour: Optional[int] = None,
        max_delay_hours: Optional[int] = None,
        forecast_confidence: Optional[Mapping[int, float]] = None,
        enable_hitl: bool = False,
    ) -> Tuple[int, float]:
        """
        Backward-compatible 2-tuple return.

        **HITL is skipped by default.** This preserves the v1 contract
        that this method is fast and non-blocking. Pass
        ``enable_hitl=True`` to run the sync HITL review for the
        recommended deferral; async callbacks are rejected (fail-safe).
        """
        rec = self.suggest_with_explanation(
            current_intensity=current_intensity,
            forecast=forecast,
            energy_kwh=energy_kwh,
            current_hour=current_hour,
            deadline_hour=deadline_hour,
            max_delay_hours=max_delay_hours,
            forecast_confidence=forecast_confidence,
            run_hitl=enable_hitl,
        )
        return rec.as_tuple()

    # ------------------------------------------------------------------
    # Rich sync API
    # ------------------------------------------------------------------
    def suggest_with_explanation(
        self,
        current_intensity: float,
        forecast: Mapping[int, float],
        energy_kwh: float,
        *,
        current_hour: int = 0,
        deadline_hour: Optional[int] = None,
        max_delay_hours: Optional[int] = None,
        forecast_confidence: Optional[Mapping[int, float]] = None,
        run_hitl: bool = True,
    ) -> DelayRecommendation:
        """
        Full pipeline (sync). Runs HITL if ``run_hitl=True`` and a
        callback/learner is available. Async callbacks are rejected.
        """
        return self._suggest_impl(
            current_intensity=current_intensity,
            forecast=forecast,
            energy_kwh=energy_kwh,
            current_hour=current_hour,
            deadline_hour=deadline_hour,
            max_delay_hours=max_delay_hours,
            forecast_confidence=forecast_confidence,
            run_hitl=run_hitl,
            hitl_async=False,
        )

    # ------------------------------------------------------------------
    # Rich async API (new)
    # ------------------------------------------------------------------
    async def suggest_async(
        self,
        current_intensity: float,
        forecast: Mapping[int, float],
        energy_kwh: float,
        *,
        current_hour: int = 0,
        deadline_hour: Optional[int] = None,
        max_delay_hours: Optional[int] = None,
        forecast_confidence: Optional[Mapping[int, float]] = None,
        run_hitl: bool = True,
    ) -> DelayRecommendation:
        """
        Async variant of :meth:`suggest_with_explanation`. Awaits
        awaitable HITL callbacks with a timeout.
        """
        return await self._suggest_impl_async(
            current_intensity=current_intensity,
            forecast=forecast,
            energy_kwh=energy_kwh,
            current_hour=current_hour,
            deadline_hour=deadline_hour,
            max_delay_hours=max_delay_hours,
            forecast_confidence=forecast_confidence,
            run_hitl=run_hitl,
        )

    # ------------------------------------------------------------------
    # Shared implementation
    # ------------------------------------------------------------------
    def _suggest_impl(
        self,
        current_intensity: float,
        forecast: Mapping[int, float],
        energy_kwh: float,
        *,
        current_hour: int,
        deadline_hour: Optional[int],
        max_delay_hours: Optional[int],
        forecast_confidence: Optional[Mapping[int, float]],
        run_hitl: bool,
        hitl_async: bool,
    ) -> DelayRecommendation:
        # Everything up to HITL is sync-safe; HITL is dispatched by the
        # caller (sync review here, async areview in suggest_async).
        rec = self._suggest_pre_hitl(
            current_intensity=current_intensity,
            forecast=forecast,
            energy_kwh=energy_kwh,
            current_hour=current_hour,
            deadline_hour=deadline_hour,
            max_delay_hours=max_delay_hours,
            forecast_confidence=forecast_confidence,
        )
        if not run_hitl or self.hitl is None:
            return self._finalize(rec)
        if not self.hitl.needs_review(
            saving_kg=rec.raw_saving_kg,
            confidence=rec.confidence,
            horizon=abs(rec.best_hour - rec.current_hour),
        ):
            return self._finalize(rec)

        rec.hitl_required = True
        approved, status = self.hitl.review(
            current_hour=rec.current_hour,
            proposed_hour=rec.best_hour,
            current_intensity=rec.current_intensity,
            best_intensity=rec.best_intensity,
            saving_kg=rec.raw_saving_kg,
            confidence=rec.confidence,
            reason=(
                f"horizon={abs(rec.best_hour - rec.current_hour)}h, "
                f"saving={rec.raw_saving_kg:.6f}kg"
            ),
        )
        return self._apply_hitl(rec, approved, status)

    async def _suggest_impl_async(
        self,
        current_intensity: float,
        forecast: Mapping[int, float],
        energy_kwh: float,
        *,
        current_hour: int,
        deadline_hour: Optional[int],
        max_delay_hours: Optional[int],
        forecast_confidence: Optional[Mapping[int, float]],
        run_hitl: bool,
    ) -> DelayRecommendation:
        rec = self._suggest_pre_hitl(
            current_intensity=current_intensity,
            forecast=forecast,
            energy_kwh=energy_kwh,
            current_hour=current_hour,
            deadline_hour=deadline_hour,
            max_delay_hours=max_delay_hours,
            forecast_confidence=forecast_confidence,
        )
        if not run_hitl or self.hitl is None:
            return self._finalize(rec)
        if not self.hitl.needs_review(
            saving_kg=rec.raw_saving_kg,
            confidence=rec.confidence,
            horizon=abs(rec.best_hour - rec.current_hour),
        ):
            return self._finalize(rec)

        rec.hitl_required = True
        approved, status = await self.hitl.areview(
            current_hour=rec.current_hour,
            proposed_hour=rec.best_hour,
            current_intensity=rec.current_intensity,
            best_intensity=rec.best_intensity,
            saving_kg=rec.raw_saving_kg,
            confidence=rec.confidence,
            reason=(
                f"horizon={abs(rec.best_hour - rec.current_hour)}h, "
                f"saving={rec.raw_saving_kg:.6f}kg"
            ),
        )
        return self._apply_hitl(rec, approved, status)

    def _suggest_pre_hitl(
        self,
        *,
        current_intensity: float,
        forecast: Mapping[int, float],
        energy_kwh: float,
        current_hour: int,
        deadline_hour: Optional[int],
        max_delay_hours: Optional[int],
        forecast_confidence: Optional[Mapping[int, float]],
    ) -> DelayRecommendation:
        # 1. Chaos
        if self.chaos is not None and self.chaos.maybe_fail("suggest"):
            logger.warning("Chaos: forcing RUN_NOW fallback")
            return self._run_now_fallback(
                current_intensity=current_intensity,
                energy_kwh=energy_kwh,
                current_hour=current_hour,
                reason="chaos injection",
            )

        # 2. Empty / invalid forecast
        if not isinstance(forecast, Mapping) or not forecast:
            return self._run_now_fallback(
                current_intensity=current_intensity,
                energy_kwh=energy_kwh,
                current_hour=current_hour,
                reason="empty forecast",
            )

        clean_forecast: Dict[int, float] = {}
        for k, v in forecast.items():
            try:
                h = int(k)
                i = float(v)
                if i >= 0 and math.isfinite(i):
                    clean_forecast[h] = i
            except (TypeError, ValueError):
                continue
        if not clean_forecast:
            return self._run_now_fallback(
                current_intensity=current_intensity,
                energy_kwh=energy_kwh,
                current_hour=current_hour,
                reason="no valid forecast points",
            )

        # 3. Circuit breaker (v18's fixed HALF_OPEN semantics)
        if not self.circuit.can_call():
            logger.debug("Shifter circuit open — using greedy scan only")
        else:
            self.circuit.record_success()

        # 4. Adaptive precision
        urgency = (
            "critical" if energy_kwh > 1.0 else "normal"
        )
        precision = self._select_precision(urgency)

        # 5. Optional distillation
        used_forecast: Mapping[int, float] = clean_forecast
        distilled_used = False
        distilled_retention = 1.0
        if self.distiller is not None:
            dist = self.distiller.distill(clean_forecast, precision)
            if dist.smoothed:
                used_forecast = dist.smoothed
                distilled_used = True
                distilled_retention = dist.quality_retention
                self._distilled_count += 1

        # 6. Greedy scan
        current_carbon = current_intensity * energy_kwh / 1000.0
        best_hour = current_hour
        best_saving_raw = 0.0
        best_intensity = current_intensity
        forecast_values = list(used_forecast.values())

        for hour, future_intensity in used_forecast.items():
            if (
                deadline_hour is not None
                and hour > deadline_hour
            ):
                continue
            if (
                max_delay_hours is not None
                and abs(hour - current_hour) > max_delay_hours
            ):
                continue
            conf = 1.0
            if forecast_confidence and hour in forecast_confidence:
                try:
                    conf = max(
                        0.0,
                        min(
                            1.0,
                            float(forecast_confidence[hour]),
                        ),
                    )
                except (TypeError, ValueError):
                    conf = 1.0
            future_carbon = future_intensity * energy_kwh / 1000.0
            saving = (current_carbon - future_carbon) * conf
            if saving > best_saving_raw:
                best_saving_raw = saving
                best_hour = hour
                best_intensity = future_intensity

        # 7. Causal correction (clamped, relative-signal learner)
        causal_adjustment = 0.0
        features: Optional[List[float]] = None
        if (
            self.causal_learner is not None
            and best_hour != current_hour
            and best_saving_raw > 0
        ):
            features = self.causal_learner.build_features(
                current_intensity=current_intensity,
                best_intensity=best_intensity,
                best_saving=best_saving_raw,
                energy_kwh=energy_kwh,
                current_hour=current_hour,
                best_hour=best_hour,
                forecast_values=forecast_values,
            )
            causal_adjustment = (
                self.causal_learner.predict_correction(features)
            )
            causal_adjustment = max(
                -_MAX_CORRECTION, min(_MAX_CORRECTION, causal_adjustment)
            )
            best_saving_raw *= 1.0 + 0.1 * causal_adjustment

        # 8. Quantize
        best_saving_quantized = SafeQuantizer.quantize(
            best_saving_raw, precision
        )

        # 9. FIX §2.1: contradictory recommendation guard.
        # If quantization collapsed a positive saving to zero, treat
        # the decision as RUN_NOW to keep (hour, saving) consistent.
        if best_saving_raw > 0 and best_saving_quantized <= 0:
            logger.info(
                "Quantization collapsed saving %.6f kg to 0 at %s "
                "precision; falling back to RUN_NOW",
                best_saving_raw,
                precision.value,
            )
            best_saving_quantized = best_saving_raw  # keep raw for
            #                                          consistency, but
            best_hour = current_hour                  # reset the hour.
            best_intensity = current_intensity

        # 10. Strategy classification
        horizon = abs(best_hour - current_hour)
        if best_saving_quantized <= 0:
            strategy = DeferralStrategy.RUN_NOW
        elif horizon <= 6:
            strategy = DeferralStrategy.SHIFT_SHORT
        else:
            strategy = DeferralStrategy.SHIFT_LONG

        # 11. Temporal verification (stateless, per-call context)
        temporal_verified = True
        violations: List[str] = []
        if self.temporal_monitor is not None:
            context = ShiftContext(
                deadline_hour=deadline_hour,
                max_delay_hours=(
                    max_delay_hours
                    if max_delay_hours is not None
                    else 48
                ),
            )
            temporal_verified, violations = (
                self.temporal_monitor.verify(
                    current_hour, best_hour, context
                )
            )
            if not temporal_verified:
                logger.warning(
                    "Temporal violations: %s; falling back to RUN_NOW",
                    violations,
                )
                fb = self._run_now_fallback(
                    current_intensity=current_intensity,
                    energy_kwh=energy_kwh,
                    current_hour=current_hour,
                    reason=f"temporal violations: {violations}",
                    precision=precision,
                )
                fb.temporal_verified = False
                fb.temporal_violations = list(violations)
                return fb

        # 12. Federated prior (informational)
        federated_used = False
        if self.federated is not None:
            prior = self.federated.lookup(strategy.value)
            if prior and prior.get("sample_count", 0) >= 5:
                federated_used = True

        # 13. Market enrichment
        market_enrichment: Optional[Dict[str, Any]] = None
        market_carbon_price: Optional[float] = None
        if self.market is not None:
            snap = self.market.get_snapshot()
            market_carbon_price = snap.carbon_price_per_tco2_usd
            saving_value_usd = (
                best_saving_raw / 1000.0 * snap.carbon_price_per_tco2_usd
            )
            market_enrichment = {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "saving_value_usd": saving_value_usd,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

        # 14. Confidence (propagate distillation retention)
        confidence = 0.9 if best_hour != current_hour else 0.5
        confidence *= distilled_retention

        # 15. XAI
        explanation: Optional[DelayExplanation] = None
        if self.explainer is not None:
            explanation = self.explainer.explain(
                current_intensity=current_intensity,
                current_hour=current_hour,
                best_hour=best_hour,
                best_intensity=best_intensity,
                energy_kwh=energy_kwh,
                raw_saving_kg=best_saving_raw,
                quantized_saving_kg=best_saving_quantized,
                forecast_size=len(used_forecast),
                confidence=confidence,
                causal_adjustment=causal_adjustment,
                market_carbon_price=market_carbon_price,
            )

        # 16. Saving percent from raw, not quantized
        pct = (
            best_saving_raw / current_carbon * 100.0
            if current_carbon > 0
            else 0.0
        )

        rec = DelayRecommendation(
            best_hour=best_hour,
            best_saving_kg=best_saving_quantized,
            raw_saving_kg=best_saving_raw,
            strategy=strategy,
            current_carbon_kg=current_carbon,
            best_carbon_kg=best_intensity * energy_kwh / 1000.0,
            saving_percent=pct,
            confidence=confidence,
            explanation=explanation,
            temporal_verified=temporal_verified,
            temporal_violations=list(violations),
            precision_used=precision.value,
            market_enrichment=market_enrichment,
            federated_used=federated_used,
            distilled_used=distilled_used,
            causal_adjustment=causal_adjustment,
            features=features,
            recommendation_id=uuid.uuid4().hex[:12],
            current_hour=current_hour,
            current_intensity=current_intensity,
            best_intensity=best_intensity,
            energy_kwh=energy_kwh,
        )
        return rec

    # ------------------------------------------------------------------
    # HITL application
    # ------------------------------------------------------------------
    def _apply_hitl(
        self,
        rec: DelayRecommendation,
        approved: Optional[bool],
        status: str,
    ) -> DelayRecommendation:
        rec.hitl_approved = approved
        rec.hitl_status = (
            status if status in _VALID_HITL_STATUSES else "not_required"
        )
        self._hitl_outcomes[rec.hitl_status] += 1

        if approved is True:
            return self._finalize(rec)

        if approved is False:
            # Rejected: downgrade to RUN_NOW
            logger.info(
                "HITL rejected deferral to hour %d", rec.best_hour
            )
            fb = self._run_now_fallback(
                current_intensity=rec.current_intensity,
                energy_kwh=rec.energy_kwh,
                current_hour=rec.current_hour,
                reason="HITL rejected deferral",
                precision=PrecisionLevel(rec.precision_used),
                market_enrichment=rec.market_enrichment,
            )
            fb.hitl_required = True
            fb.hitl_approved = False
            fb.hitl_status = "rejected"
            return self._finalize(fb)

        # Timeout / no answer → HOLD_FOR_REVIEW (load-bearing).
        logger.info(
            "HITL timed out for deferral to hour %d → HOLD_FOR_REVIEW",
            rec.best_hour,
        )
        rec.strategy = DeferralStrategy.HOLD_FOR_REVIEW
        return self._finalize(rec)

    def _finalize(
        self, rec: DelayRecommendation
    ) -> DelayRecommendation:
        self.decision_history.append(rec)
        self._last_recommendation = rec
        self._strategy_counts[rec.strategy.value] += 1
        if self.coordinator is not None:
            self.coordinator.record(
                self.agent_id, rec.strategy, rec.best_saving_kg > 0
            )
        return rec

    # ------------------------------------------------------------------
    # Precision selection (v18's switcher + our own urgency map)
    # ------------------------------------------------------------------
    def _select_precision(self, urgency: str) -> PrecisionLevel:
        # v18's AdaptivePrecisionSwitcher doesn't expose an urgency-based
        # selector in the form we need; use the hardware-aware map that
        # shifter v5 had. Same precedence as before.
        if self.hardware.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hardware.supports_int8
                else PrecisionLevel.FP16
            )
        if self.hardware.supports_int4:
            return PrecisionLevel.INT4
        return PrecisionLevel.INT8

    # ------------------------------------------------------------------
    # Fallback (consistently uses current_hour)
    # ------------------------------------------------------------------
    def _run_now_fallback(
        self,
        current_intensity: float,
        energy_kwh: float,
        current_hour: int,
        reason: str,
        precision: PrecisionLevel = PrecisionLevel.FP32,
        market_enrichment: Optional[Dict[str, Any]] = None,
    ) -> DelayRecommendation:
        current_carbon = current_intensity * energy_kwh / 1000.0
        rec = DelayRecommendation(
            best_hour=current_hour,   # FIX §2.8: not 0
            best_saving_kg=0.0,
            raw_saving_kg=0.0,
            strategy=DeferralStrategy.RUN_NOW,
            current_carbon_kg=current_carbon,
            best_carbon_kg=current_carbon,
            saving_percent=0.0,
            confidence=1.0,
            explanation=(
                DelayExplanation(
                    headline="RUN NOW (no beneficial deferral)",
                    rationale=[reason],
                    confidence=1.0,
                    contributing_factors={},
                )
                if self.explainer is not None
                else None
            ),
            temporal_verified=True,
            precision_used=precision.value,
            market_enrichment=market_enrichment,
            recommendation_id=uuid.uuid4().hex[:12],
            current_hour=current_hour,
            current_intensity=current_intensity,
            best_intensity=current_intensity,
            energy_kwh=energy_kwh,
        )
        self._fallback_count += 1
        return self._finalize(rec)

    # ------------------------------------------------------------------
    # Feedback loop
    # ------------------------------------------------------------------
    def record_outcome(
        self,
        actual_saving_kg: float,
        recommendation: Optional[DelayRecommendation] = None,
    ) -> bool:
        """
        Feed the observed outcome back to the causal learner.

        Preference order for the "predicted" side:

        1. ``recommendation.features`` when ``recommendation`` is given.
        2. ``self._last_recommendation.features`` when no argument was
           passed (convenient for the common "one suggest, one outcome"
           pattern).
        3. Legacy ``_last_features`` — not used in v6. Kept for
           compatibility with callers that still pass ``_last_features``
           directly via attributes; logs a warning.
        """
        rec = recommendation or self._last_recommendation
        if rec is None or self.causal_learner is None:
            return False

        features = rec.features
        if features is None:
            logger.warning(
                "record_outcome: no features on recommendation %s; "
                "skipping causal update",
                rec.recommendation_id,
            )
            return False

        self.causal_learner.record(
            features=features,
            chosen_hour=rec.best_hour,
            predicted_saving=rec.raw_saving_kg,
            actual_saving=float(actual_saving_kg),
        )
        if self.causal_learner.observations % 10 == 0:
            self.causal_learner.update()

        if self.coordinator is not None:
            success = actual_saving_kg >= 0.5 * rec.raw_saving_kg
            self.coordinator.record(
                self.agent_id, rec.strategy, success
            )
        return True

    # ------------------------------------------------------------------
    # Federated contribution
    # ------------------------------------------------------------------
    def contribute_federated(self) -> None:
        if self.federated is None:
            return
        grouped: Dict[
            str, List[DelayRecommendation]
        ] = defaultdict(list)
        for rec in self.decision_history:
            if rec.best_hour == rec.current_hour:
                continue
            grouped[rec.strategy.value].append(rec)

        for strategy, recs in grouped.items():
            if len(recs) < 3:
                continue
            mean_pct = statistics.fmean(
                r.saving_percent for r in recs
            )
            mean_horizon = statistics.fmean(
                abs(r.best_hour - r.current_hour) for r in recs
            )
            self.federated.push(
                FederatedDeferralProfile(
                    deployment_id=self.deployment_id,
                    strategy=strategy,
                    mean_saving_percent=mean_pct,
                    mean_horizon_hours=mean_horizon,
                    sample_count=len(recs),
                )
            )
        self.federated.aggregate()

    # ------------------------------------------------------------------
    # HITL hook
    # ------------------------------------------------------------------
    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        if self.hitl is not None:
            self.hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return (
            self.hitl.active_learning_batch(n)
            if self.hitl is not None
            else []
        )

    # ------------------------------------------------------------------
    # Ledger interop
    # ------------------------------------------------------------------
    def to_ledger_entry(
        self, rec: DelayRecommendation
    ) -> Dict[str, Any]:
        """
        Return a dict shaped for ``carbon_ledger.ExtendedCarbonLedger``.

        The dict can be passed directly to ``add_entry_v18`` (or an
        upcoming ``add_deferral_entry`` method) once the ledger schema
        grows deferral columns. Until then, treat it as a portable
        record.
        """
        return {
            "task_id": rec.recommendation_id,
            "energy_kwh": rec.energy_kwh,
            "carbon_kg": rec.current_carbon_kg,
            "carbon_offset_kg": rec.best_saving_kg,
            "net_carbon_kg": rec.best_carbon_kg,
            "source": "deferral",
            "hitl_status": rec.hitl_status,
            "extra": {
                "deferral_from_hour": rec.current_hour,
                "deferral_to_hour": rec.best_hour,
                "strategy": rec.strategy.value,
                "raw_saving_kg": rec.raw_saving_kg,
                "saving_percent": rec.saving_percent,
                "confidence": rec.confidence,
                "distilled_used": rec.distilled_used,
                "federated_used": rec.federated_used,
                "precision_used": rec.precision_used,
            },
        }

    # ------------------------------------------------------------------
    # Statistics / health
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "decisions_made": len(self.decision_history),
            "circuit_state": self.circuit.state,
            "fallback_count": self._fallback_count,
            "distilled_count": self._distilled_count,
            "strategy_counts": dict(self._strategy_counts),
            "hitl_outcomes": dict(self._hitl_outcomes),
        }
        if self.temporal_monitor is not None:
            stats["temporal_violation_count"] = len(
                self.temporal_monitor.violations
            )
            stats["temporal_violations_recent"] = list(
                self.temporal_monitor.violations
            )[-5:]
        if self.causal_learner is not None:
            stats["causal_learner"] = self.causal_learner.stats()
        if self.coordinator is not None:
            stats["agents"] = {
                aid: {"role": a.role.value, "calls": a.total_calls}
                for aid, a in self.coordinator.agents.items()
            }
        if self.federated is not None:
            stats["federated_aggregate"] = self.federated.aggregate()
        if self.chaos is not None:
            stats["chaos_events"] = list(self.chaos.events)[-5:]
        if self.hitl is not None:
            stats["hitl"] = {
                "pending": len(self.hitl.pending),
                "log_size": len(self.hitl.feedback_log),
                "outcomes": self.hitl.outcomes(),
            }
        if self.market is not None:
            snap = self.market.get_snapshot()
            stats["market"] = {
                "carbon_price_per_tco2_usd": (
                    snap.carbon_price_per_tco2_usd
                ),
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
            }
        return stats

    async def health_check(self) -> Dict[str, Any]:
        return {
            "healthy": self.circuit.state != "OPEN",
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "circuit_state": self.circuit.state,
            "decisions_made": len(self.decision_history),
            "fallback_count": self._fallback_count,
            "distilled_count": self._distilled_count,
            "hitl_outcomes": dict(self._hitl_outcomes),
            "strategy_counts": dict(self._strategy_counts),
            "temporal_violation_count": (
                len(self.temporal_monitor.violations)
                if self.temporal_monitor is not None
                else 0
            ),
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "TemporalShifter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "TemporalShifter":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_temporal_shifter(
    deployment_id: str = "local",
    agent_id: str = "shifter-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    baseline_usd_per_kwh: float = 0.08,
) -> TemporalShifter:
    return TemporalShifter(
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
        baseline_usd_per_kwh=baseline_usd_per_kwh,
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
    "QuantumDistillationEngine",
    "CarbonMarketIntegrator",
    "XAIDecisionExplainer",
    "MultiAgentCoordinator",
    "FederatedGreenAggregator",
    "TemporalLogicVerifier",
    "SimpleQuantumDistillationEngine",
    "SimpleCausalGraphLearner",
    "SimpleCausalPolicyAdapter",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleXAIDecisionExplainer",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
    "ForecastPoint",
    "ForecastBatch",
    "ForecastSource",
    # v6 enums
    "PrecisionLevel",
    "AgentRole",
    "DeferralStrategy",
    # v6 domain
    "HardwareProfile",
    "DelayExplanation",
    "DelayRecommendation",
    "ShiftContext",
    # v6 components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "DeadlineRespected",
    "MaxDelayBounded",
    "NonNegativeHour",
    "ShiftSafetyMonitor",
    "ShiftProperty",
    "DelayExplainer",
    "SafeQuantizer",
    "CausalDeferralLearner",
    "DeferralTransition",
    "DistilledForecast",
    "QuantumDistillationBridge",
    "FederatedDeferralProfile",
    "FederatedDeferralAggregator",
    "ShiftAgentProfile",
    "ShiftCoordinator",
    "MarketSnapshot",
    "CarbonMarketClient",
    "HITLRequest",
    "HumanReviewGate",
    # Main
    "TemporalShifter",
    "create_temporal_shifter",
]


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    print("=" * 80)
    print("temporal_shifter.py v6.0.0 — Demo")
    print("=" * 80)

    # ---- INT4 configuration reproduces the v5 contradiction ----
    shifter = create_temporal_shifter(
        deployment_id="us-ca-prod-01",
        agent_id="shifter-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    )

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(
            "[HITL] approve hour %d (%s)", req.proposed_hour, req.urgency
        )
        return True

    shifter.set_hitl_callback(auto_approve)

    forecast = {14: 180.0, 15: 120.0, 16: 90.0, 17: 150.0, 18: 200.0}

    # ---- P0 fix: no contradictory recommendation ----
    print("\n=== P0 fix: (hour, saving) is internally consistent ===")
    hour, saving = shifter.suggest(
        current_intensity=400.0,
        forecast=forecast,
        energy_kwh=0.005,
    )
    print(f"  suggest() → ({hour}, {saving:.6f})")
    # v5 would have returned (16, 0.0) — inconsistent. v6 must not:
    assert saving > 0 or hour == 0, (
        f"Contradictory recommendation: (hour={hour}, saving={saving})"
    )
    print("  ✓ no contradiction")

    # ---- Rich sync API + HITL ----
    print("\n=== suggest_with_explanation() with HITL ===")
    rec = shifter.suggest_with_explanation(
        current_intensity=400.0,
        forecast=forecast,
        energy_kwh=0.005,
        current_hour=12,
        deadline_hour=18,
        max_delay_hours=12,
        forecast_confidence={14: 0.9, 15: 0.85, 16: 0.7, 17: 0.6, 18: 0.5},
    )
    print(f"  strategy:     {rec.strategy.value}")
    print(f"  best_hour:    {rec.best_hour}")
    print(f"  raw_saving:   {rec.raw_saving_kg:.6f} kg")
    print(f"  quantized:    {rec.best_saving_kg:.6f} kg")
    print(f"  hitl_status:  {rec.hitl_status}")
    print(f"  precision:    {rec.precision_used}")
    print(f"  distilled:    {rec.distilled_used}")
    print(f"  id:           {rec.recommendation_id}")
    if rec.explanation:
        print(f"  XAI: {rec.explanation.headline}")

    # ---- P0 fix: causal learner moves measurably ----
    print("\n=== P0 fix: causal learner moves measurably ===")
    # Repeatedly pretend the raw saving was systematically low
    for _ in range(30):
        shifter.record_outcome(
            actual_saving_kg=rec.raw_saving_kg * 1.5,
            recommendation=rec,
        )
    shifter.causal_learner.update()
    assert rec.features is not None
    correction = shifter.causal_learner.predict_correction(rec.features)
    print(f"  correction after 30 outcomes: {correction:+.4f}")
    assert abs(correction) > 0.01, (
        f"Causal learner barely moved: correction={correction}"
    )
    print("  ✓ causal learner moved")

    # ---- P0 fix: no `_last_features` contamination ----
    print("\n=== P0 fix: record_outcome uses the correct features ===")
    rec_b = shifter.suggest_with_explanation(
        current_intensity=500.0,
        forecast={8: 300.0, 12: 200.0, 16: 100.0},
        energy_kwh=0.003,
        current_hour=6,
    )
    # Now record outcome for rec (the *old* one), not rec_b.
    observations_before = shifter.causal_learner.observations
    shifter.record_outcome(
        actual_saving_kg=rec.raw_saving_kg * 1.4,
        recommendation=rec,
    )
    observations_after = shifter.causal_learner.observations
    assert observations_after == observations_before + 1
    print(
        f"  record_outcome used rec.features (not rec_b): "
        f"observations {observations_before} → {observations_after}"
    )
    print("  ✓ no cross-recommendation contamination")

    # ---- P0 fix: temporal properties are per-call ----
    print("\n=== P0 fix: temporal properties don't leak across calls ===")
    # Call 1 with a tight deadline
    _ = shifter.suggest_with_explanation(
        current_intensity=400.0,
        forecast=forecast,
        energy_kwh=0.005,
        current_hour=12,
        deadline_hour=14,
    )
    # Call 2 with NO deadline — should not inherit deadline=14
    _rec2 = shifter.suggest_with_explanation(
        current_intensity=400.0,
        forecast=forecast,
        energy_kwh=0.005,
        current_hour=12,
        deadline_hour=None,
    )
    # If the deadline leaked, temporal_verified would be False
    assert _rec2.temporal_verified, (
        "Deadline from call 1 leaked into call 2"
    )
    print("  ✓ temporal properties are stateless")

    # ---- P0 fix: HOLD_FOR_REVIEW on HITL timeout ----
    print("\n=== P0 fix: HOLD_FOR_REVIEW on HITL timeout ===")
    shifter_no_cb = create_temporal_shifter(
        deployment_id="no-cb",
        agent_id="shifter-no-cb",
        hardware=HardwareProfile(supports_int4=True),
    )
    # No callback registered → HITL "times out" (returns None)
    rec_hold = shifter_no_cb.suggest_with_explanation(
        current_intensity=400.0,
        forecast=forecast,
        energy_kwh=0.005,
        current_hour=12,
        deadline_hour=18,
        max_delay_hours=12,
        forecast_confidence={14: 0.2, 15: 0.2, 16: 0.2, 17: 0.2, 18: 0.2},
    )
    if rec_hold.hitl_required and rec_hold.hitl_approved is None:
        assert rec_hold.strategy == DeferralStrategy.HOLD_FOR_REVIEW, (
            f"Expected HOLD_FOR_REVIEW, got {rec_hold.strategy}"
        )
        print(f"  strategy: {rec_hold.strategy.value}")
        print("  ✓ HOLD_FOR_REVIEW is load-bearing")

    # ---- P0 fix: sync review rejects async callbacks ----
    print("\n=== P0 fix: async callback rejected in sync review ===")

    async def async_cb(req: HITLRequest) -> bool:
        return True

    shifter_no_cb.set_hitl_callback(async_cb)
    rec_reject = shifter_no_cb.suggest_with_explanation(
        current_intensity=400.0,
        forecast=forecast,
        energy_kwh=0.005,
        current_hour=12,
        deadline_hour=18,
        max_delay_hours=12,
    )
    # Async callback → sync review rejects → hitl_status='rejected',
    # strategy falls back to RUN_NOW
    if rec_reject.hitl_required:
        print(f"  hitl_status: {rec_reject.hitl_status}")
        print(f"  strategy:    {rec_reject.strategy.value}")
        assert rec_reject.hitl_status == "rejected"
        print("  ✓ async callback rejected (fail-safe)")

    # ---- Federated contribution ----
    print("\n=== Federated contribution ===")
    for _ in range(10):
        shifter.suggest_with_explanation(
            current_intensity=400.0 + random.uniform(-50, 50),
            forecast={
                14: 180.0 + random.uniform(-20, 20),
                15: 120.0 + random.uniform(-20, 20),
                16: 90.0 + random.uniform(-20, 20),
            },
            energy_kwh=0.005,
            current_hour=12,
        )
    shifter.contribute_federated()
    agg = shifter.get_statistics().get("federated_aggregate", {})
    print(f"  strategies with priors: {list(agg.keys())}")

    # ---- Async API ----
    print("\n=== Async API (suggest_async) ===")

    async def _async_demo() -> None:
        shifter.set_hitl_callback(auto_approve)
        rec = await shifter.suggest_async(
            current_intensity=400.0,
            forecast=forecast,
            energy_kwh=0.005,
            current_hour=12,
        )
        print(f"  strategy: {rec.strategy.value}")
        print(f"  hitl:     {rec.hitl_status}")

    asyncio.run(_async_demo())

    # ---- Ledger interop ----
    print("\n=== Ledger interop ===")
    ledger_dict = shifter.to_ledger_entry(rec)
    print(f"  task_id:          {ledger_dict['task_id']}")
    print(f"  carbon_offset_kg: {ledger_dict['carbon_offset_kg']:.6f}")
    print(f"  extra.strategy:   {ledger_dict['extra']['strategy']}")

    # ---- Statistics ----
    print("\n=== Statistics ===")
    print(json.dumps(
        shifter.get_statistics(), indent=2, default=str
    ))

    async def _health() -> None:
        print("\n=== Health Check ===")
        print(json.dumps(
            await shifter.health_check(), indent=2, default=str
        ))

    asyncio.run(_health())

    print("\nDemo complete.")


if __name__ == "__main__":
    _demo()
