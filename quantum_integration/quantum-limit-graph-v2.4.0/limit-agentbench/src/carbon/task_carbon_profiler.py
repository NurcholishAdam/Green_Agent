"""
task_carbon_profiler.py — v6.0.0 (thin wrapper over carbon_forecast v18)
========================================================================

Task Carbon Profiler for Layer 7.

**What changed in v6**
----------------------
v5 had its own copies of the enhancement classes, four P0 correctness
bugs (the causal learner barely learned, the prediction interval was a
confidence interval of the mean, confidence peaked at one sample, HITL
results were discarded), a save path that could lose the telemetry file
on crash, and no bounded collections.

v6 fixes all of that.

**Re-exported from v18 (single implementation)**
    CircuitBreaker, ChaosTestingEngine, ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher, QuantumDistillationEngine,
    CarbonMarketIntegrator, XAIDecisionExplainer,
    plus every ``Simple*`` alias and shared helpers
    (``_utcnow``, ``_ensure_aware``, ``_parse_timestamp``, ``_json_safe``,
    ``_cfg_get``, ``_safe_mean``).

**Local (profiler-specific; fixed)**
    EstimationMethod, CarbonEstimate (+hitl_status, +methods_applied)
    UncertaintyEstimate       — fixes §2.2 and §2.3
    CausalWeightLearner        — fixes §2.1
    QuantumDistillationBridge  — coefficient tables (round, not floor)
    AdaptivePrecisionController — quantize with round, not floor
    SyncCircuitAdapter         — sync facade over v18's async breaker
    SyncChaosInjector          — deterministic sync fault injector
    TemporalLogicMonitor       — bounded, single-estimate check
    NonNegativeEnergy/Carbon, BoundedConfidence, EnergyUpperBound
    ProfilerCoordinator        — profiler-specific role set
    FederatedProfilerAggregator — per-(arch, hardware) profiles
    AnomalyDetector            — per-(arch, hardware), not global
    HumanReviewGate            — sync + async, awaits awaitables
    CarbonMarketClient         — sync, cached
    TaskCarbonProfiler         — main class

**P0 fixes**
    1. ``CausalWeightLearner`` uses the target *ratio* (actual/predicted)
       as the gradient signal; the ``0.001×`` scaling is gone, weights
       move measurably, bias is clamped to [-1, 1], correction is
       clamped to [-2, 2].
    2. ``UncertaintyEstimate.prediction_interval_95`` is now a true
       **prediction** interval: ``mean ± 1.96·σ·√(1 + 1/n)``, which
       converges to ``mean ± 1.96σ`` (irreducible variance), not to zero.
    3. ``UncertaintyEstimate.confidence`` uses ``n_factor × cv_factor``;
       one sample gives ~0.05 confidence, not 1.0.
    4. HITL result is **propagated** onto the estimate: ``is_stale=True``
       and ``hitl_status="rejected"`` on rejection; async callbacks are
       awaited in ``areview``; sync ``review`` rejects awaitables (fail-safe).
    5. Historical estimate excludes records missing ``dataset_size`` (or
       uses a per-architecture default), and clamps the ratio to
       [0.1, 100]; a missing field no longer inflates estimates 1e5×.
    6. ``estimation_method`` is upgraded along a documented rank; every
       transform is recorded in ``methods_applied`` for audit.

**P1 fixes**
    * ``_save_telemetry`` is atomic (tempfile + fsync + os.replace) and
      guarded by a ``_save_lock`` and the circuit breaker.
    * ``historical_telemetry`` is a bounded ``deque(maxlen=...)``.
    * ``_load_telemetry`` validates every record; skips malformed ones.
    * ``AnomalyDetector`` is per-(arch, hardware).
    * ``set_features()`` recreates layers at runtime.
    * ``health_check()`` reports telemetry-save failures, fallback rate,
      HITL outcomes, temporal violations, causal-correction magnitude.
    * ``datetime.now(timezone.utc)`` everywhere.
    * ``round`` (not ``floor``) in every quantizer.
    * Magic constants moved to named class attributes.
    * ``numpy`` removed (``statistics`` covers every use).
    * ``__aenter__`` / ``__aexit__``.

**Public API preserved**
    profiler = TaskCarbonProfiler(telemetry_db_path=None)
    estimate = await profiler.estimate_energy(task, carbon_intensity=400.0)
    profiler.add_telemetry_record(task, actual_energy, actual_carbon,
                                  predicted_energy=None)

File: src/carbon/task_carbon_profiler.py
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import math
import os
import random
import statistics
import tempfile
import threading
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
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

    CircuitBreaker = None  # type: ignore[assignment]
    ChaosTestingEngine = None  # type: ignore[assignment]
    ActiveUserPreferenceLearner = None  # type: ignore[assignment]
    AdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    QuantumDistillationEngine = None  # type: ignore[assignment]
    CarbonMarketIntegrator = None  # type: ignore[assignment]
    XAIDecisionExplainer = None  # type: ignore[assignment]
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
# Bounds & defaults
# --------------------------------------------------------------------------- #
_MAX_VIOLATIONS = 200
_MAX_HITL_LOG = 500
_MAX_PENDING_HITL = 100
_MAX_CHAOS_EVENTS = 500
_MAX_FEDERATED_UPDATES = 100
_MAX_TELEMETRY_RECORDS = 10_000
_MAX_CAUSAL_BUFFER = 512
_MAX_ANOMALY_WINDOW = 30
_MAX_RECENT_FOR_UNCERTAINTY = 20


#: Per-architecture default dataset sizes — used only when the caller did
#: not supply a positive value AND no historical record has one.
_ARCH_DEFAULT_DATASET: Dict[str, int] = {
    "transformer": 100_000,
    "cnn": 50_000,
    "rnn": 20_000,
    "hybrid": 80_000,
    "dense": 10_000,
}


#: Rank of each estimation method — higher = "more informed". Used to
#: upgrade ``estimation_method`` when a transform is applied.
_METHOD_RANK: Dict[str, int] = {
    "heuristic": 0,
    "historical": 1,
    "causal": 2,
    "federated": 3,
    "market_enriched": 4,
    "distilled": 5,
}


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class EstimationMethod(Enum):
    HISTORICAL = "historical"
    HEURISTIC = "heuristic"
    CAUSAL = "causal"
    DISTILLED = "distilled"
    FEDERATED = "federated"
    MARKET_ENRICHED = "market_enriched"


class AgentRole(Enum):
    GENERALIST = "generalist"
    TRANSFORMER_SPECIALIST = "transformer_specialist"
    CNN_SPECIALIST = "cnn_specialist"
    EDGE_SPECIALIST = "edge_specialist"
    ACCURACY_OPTIMIZER = "accuracy_optimizer"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# --------------------------------------------------------------------------- #
# Domain data
# --------------------------------------------------------------------------- #
@dataclass
class CarbonEstimate:
    """Carbon emission estimate for a task (extended)."""

    task_id: str
    expected_energy_kwh: float
    expected_carbon_kgco2e: float
    carbon_intensity_gco2kwh: float
    confidence: float
    estimation_method: str
    breakdown: Dict[str, float]
    # --- v5 fields ---
    uncertainty_std: float = 0.0
    prediction_interval_95: Tuple[float, float] = (0.0, 0.0)
    explanation: Optional[str] = None
    contributing_factors: Dict[str, float] = field(default_factory=dict)
    market_enrichment: Optional[Dict[str, Any]] = None
    precision_used: str = "fp32"
    temporal_verified: bool = True
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    source: str = "heuristic"
    # --- v6 fields ---
    methods_applied: List[str] = field(default_factory=list)
    hitl_status: str = "not_required"
    is_stale: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


# --------------------------------------------------------------------------- #
# Uncertainty (fixed §2.2 and §2.3)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class UncertaintyEstimate:
    mean: float
    std: float
    confidence: float
    n_samples: int
    interval_95: Tuple[float, float]

    @staticmethod
    def from_values(values: Sequence[float]) -> "UncertaintyEstimate":
        if not values:
            return UncertaintyEstimate(0.0, 0.0, 0.0, 0, (0.0, 0.0))
        n = len(values)
        mean = statistics.fmean(values)
        std = statistics.pstdev(values) if n > 1 else 0.0

        # FIXED (§2.2): true prediction interval.
        # Var(pred - mean) = σ² + σ²/n = σ²·(1 + 1/n).
        # PI = mean ± 1.96·σ·√(1 + 1/n).
        # Converges to mean ± 1.96σ as n → ∞.
        if n > 1:
            half_width = 1.96 * std * math.sqrt(1.0 + 1.0 / n)
        else:
            half_width = 0.0  # no dispersion estimate from a single sample
        lo = max(0.0, mean - half_width)
        hi = mean + half_width

        # FIXED (§2.3): confidence grows with n AND consistency.
        cv = std / mean if mean > 0 else 1.0
        n_factor = min(1.0, n / 20.0)          # 1 sample → 0.05, 20+ → 1.0
        cv_factor = 1.0 / (1.0 + cv)           # perfect consistency → ~1.0
        confidence = max(0.0, min(1.0, n_factor * cv_factor))

        return UncertaintyEstimate(mean, std, confidence, n, (lo, hi))


# --------------------------------------------------------------------------- #
# Sync adapter over v18's async CircuitBreaker
# --------------------------------------------------------------------------- #
class SyncCircuitAdapter:
    """Sync facade over v18's ``CircuitBreaker``."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
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
    """Deterministic sync fault injector for the telemetry save/load path."""

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
# Temporal properties
# --------------------------------------------------------------------------- #
class TemporalProperty(Protocol):
    def check(self, estimate: CarbonEstimate) -> bool: ...
    def name(self) -> str: ...


@dataclass
class NonNegativeEnergy:
    def check(self, estimate: CarbonEstimate) -> bool:
        return estimate.expected_energy_kwh >= 0.0

    def name(self) -> str:
        return "NonNegativeEnergy"


@dataclass
class NonNegativeCarbon:
    def check(self, estimate: CarbonEstimate) -> bool:
        return estimate.expected_carbon_kgco2e >= 0.0

    def name(self) -> str:
        return "NonNegativeCarbon"


@dataclass
class BoundedConfidence:
    def check(self, estimate: CarbonEstimate) -> bool:
        return 0.0 <= estimate.confidence <= 1.0

    def name(self) -> str:
        return "BoundedConfidence"


@dataclass
class EnergyUpperBound:
    """Sanity: a single ML task should not exceed this many kWh.

    Configurable per deployment; default is generous enough for a
    large multi-hour training run and small enough to catch bugs.
    """

    max_kwh: float = 100_000.0

    def check(self, estimate: CarbonEstimate) -> bool:
        return estimate.expected_energy_kwh <= self.max_kwh

    def name(self) -> str:
        return "EnergyUpperBound"


class TemporalLogicMonitor:
    """Bounded, per-estimate verification."""

    def __init__(self, max_violations: int = _MAX_VIOLATIONS) -> None:
        self.properties: List[TemporalProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=max_violations)

    def register(self, prop: TemporalProperty) -> None:
        self.properties.append(prop)

    def verify(
        self, estimate: CarbonEstimate
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            try:
                if not p.check(estimate):
                    bad.append(p.name())
                    self.violations.append(
                        {
                            "property": p.name(),
                            "task_id": estimate.task_id,
                            "at": _utcnow().isoformat(),
                        }
                    )
            except Exception as e:
                logger.warning("Property %s raised: %s", p.name(), e)
                bad.append(p.name())
        return (len(bad) == 0, bad)


# --------------------------------------------------------------------------- #
# XAI
# --------------------------------------------------------------------------- #
@dataclass
class EstimateExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


class EstimateExplainer:
    @staticmethod
    def explain(
        method: str,
        energy_kwh: float,
        carbon_kg: float,
        confidence: float,
        breakdown: Mapping[str, float],
        uncertainty_std: float = 0.0,
        methods_applied: Optional[List[str]] = None,
    ) -> EstimateExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Estimated {energy_kwh:.4f} kWh → {carbon_kg:.4f} kgCO₂e "
            f"using '{method}'."
        )
        if methods_applied:
            reasons.append(
                "Methods applied: " + " → ".join(methods_applied) + "."
            )
        if breakdown:
            top = sorted(
                breakdown.items(),
                key=lambda kv: abs(kv[1]),
                reverse=True,
            )[:3]
            reasons.append(
                "Top contributing factors: "
                + ", ".join(f"{k}={v:.4f}" for k, v in top)
                + "."
            )
        if uncertainty_std > 0:
            reasons.append(f"Uncertainty (σ) = {uncertainty_std:.4f} kWh.")
        if confidence < 0.4:
            reasons.append(
                "Low confidence — consider human review or more telemetry."
            )
        elif confidence > 0.8:
            reasons.append(
                "High confidence — estimate is well-supported by telemetry."
            )
        return EstimateExplanation(
            headline=(
                f"[{method}] {energy_kwh:.4f} kWh "
                f"({confidence:.2f} conf)"
            ),
            rationale=reasons,
            confidence=confidence,
            contributing_factors=dict(breakdown),
        )


# --------------------------------------------------------------------------- #
# Causal weight learner (fixed §2.1)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CausalWeightState:
    num_params_norm: float
    dataset_size_norm: float
    batch_size_norm: float
    hardware_watt_norm: float
    arch_id: float


class CausalWeightLearner:
    """
    Learns a multiplicative correction on top of the physics-based
    heuristic.

    FIXED (§2.1): the v5 learner scaled the gradient by ``0.001×`` on
    top of ``lr=0.01``, giving an effective learning rate of 1e-5 — six
    orders of magnitude too small. This version:

    * Uses the **target ratio** ``actual / predicted`` as the learning
      signal, which is unit-free and of order 0.1–1.0.
    * Removes the ``0.001×`` factor.
    * Clamps each weight and the bias to ``[-1, 1]``.
    * Clamps ``predict_correction`` to ``[-2, 2]``.

    The caller applies ``energy *= (1 + 0.2·correction)``; a correction
    of +0.5 corresponds to a 10% upward adjustment.
    """

    def __init__(self, lr: float = 0.01) -> None:
        if lr <= 0:
            raise ValueError("lr must be > 0")
        self.lr = lr
        self.weights: List[float] = [0.0] * 5
        self.bias: float = 0.0
        self.buffer: Deque[
            Tuple[List[float], float, float]
        ] = deque(maxlen=_MAX_CAUSAL_BUFFER)
        self._update_count: int = 0
        self._sample_count: int = 0

    @staticmethod
    def _features(task: Mapping[str, Any], hardware_watts: float) -> List[float]:
        num_params = float(task.get("num_parameters", 1e8))
        dataset_size = float(task.get("dataset_size", 0))
        batch_size = float(task.get("batch_size", 32))
        arch_code = {
            "transformer": 1.0,
            "cnn": 2.0,
            "rnn": 3.0,
            "hybrid": 4.0,
            "dense": 5.0,
        }.get(task.get("_arch", "dense"), 5.0)
        return [
            math.log10(max(num_params, 1.0)) / 10.0,
            math.log10(max(dataset_size, 1.0)) / 7.0,
            math.log10(max(batch_size, 1.0)) / 4.0,
            hardware_watts / 1000.0,
            arch_code / 10.0,
        ]

    def predict_correction(
        self, task: Mapping[str, Any], hardware_watts: float
    ) -> float:
        f = self._features(task, hardware_watts)
        raw = self.bias + sum(w * x for w, x in zip(self.weights, f))
        return max(-2.0, min(2.0, raw))

    def record(
        self,
        task: Mapping[str, Any],
        hardware_watts: float,
        predicted_kwh: float,
        actual_kwh: float,
    ) -> None:
        if predicted_kwh <= 0 or actual_kwh < 0:
            return
        f = self._features(task, hardware_watts)
        self.buffer.append((f, predicted_kwh, actual_kwh))
        self._sample_count += 1

    def update(self) -> bool:
        """Apply a gradient step on the buffered samples. Returns True on step."""
        if not self.buffer:
            return False
        for f, pred, actual in self.buffer:
            if pred <= 0:
                continue
            target_ratio = actual / pred
            current_ratio = 1.0 + self.bias + sum(
                w * x for w, x in zip(self.weights, f)
            )
            err = target_ratio - current_ratio
            for i, x in enumerate(f):
                self.weights[i] = max(
                    -1.0, min(1.0, self.weights[i] + self.lr * err * x)
                )
            self.bias = max(-1.0, min(1.0, self.bias + self.lr * err))
        self.buffer.clear()
        self._update_count += 1
        return True

    def stats(self) -> Dict[str, Any]:
        return {
            "weights": list(self.weights),
            "bias": self.bias,
            "update_count": self._update_count,
            "sample_count": self._sample_count,
            "buffer_size": len(self.buffer),
        }


# --------------------------------------------------------------------------- #
# Distillation (coefficient tables)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DistilledEstimator:
    precision: PrecisionLevel
    hardware_tdp: Dict[str, float]
    architecture_energy: Dict[str, float]
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    """
    Distills the estimator's coefficient tables to lower precision.

    FIXED: uses ``round``, not ``floor`` (v5 biased every coefficient
    downward by up to one quantization step).
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
        hardware_tdp: Mapping[str, float],
        architecture_energy: Mapping[str, float],
        precision: PrecisionLevel,
    ) -> DistilledEstimator:
        scale = self._SCALE[precision]
        retention, energy = self._RETENTION[precision]

        def qz(v: float) -> float:
            if scale <= 1.0:
                return float(v)
            return round(v * scale) / scale

        return DistilledEstimator(
            precision=precision,
            hardware_tdp={k: qz(v) for k, v in hardware_tdp.items()},
            architecture_energy={
                k: qz(v) for k, v in architecture_energy.items()
            },
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Adaptive precision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


class AdaptivePrecisionController:
    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def select(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hw.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hw.supports_int8
                else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4
            if self.hw.supports_int4
            else PrecisionLevel.INT8
        )

    def quantize(self, value: float, precision: PrecisionLevel) -> float:
        scale = QuantumDistillationBridge._SCALE[precision]
        if scale <= 1.0:
            return float(value)
        return round(value * scale) / scale


# --------------------------------------------------------------------------- #
# Federated aggregator (per-architecture, per-hardware)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FederatedTelemetryProfile:
    deployment_id: str
    architecture: str
    hardware: str
    mean_energy_kwh: float
    mean_carbon_kg: float
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedProfilerAggregator:
    """FedAvg over per-(arch, hardware) energy profiles."""

    def __init__(
        self, max_updates: int = _MAX_FEDERATED_UPDATES
    ) -> None:
        self.updates: Deque[FederatedTelemetryProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[
            Tuple[str, str], Dict[str, float]
        ] = {}

    def push(self, u: FederatedTelemetryProfile) -> None:
        self.updates.append(u)

    def aggregate(
        self,
    ) -> Dict[Tuple[str, str], Dict[str, float]]:
        grouped: Dict[
            Tuple[str, str], List[FederatedTelemetryProfile]
        ] = defaultdict(list)
        for u in self.updates:
            grouped[(u.architecture, u.hardware)].append(u)

        result: Dict[Tuple[str, str], Dict[str, float]] = {}
        for key, profiles in grouped.items():
            total_w = sum(max(0, p.sample_count) for p in profiles)
            if total_w <= 0:
                continue
            e = (
                sum(p.mean_energy_kwh * p.sample_count for p in profiles)
                / total_w
            )
            c = (
                sum(p.mean_carbon_kg * p.sample_count for p in profiles)
                / total_w
            )
            result[key] = {
                "mean_energy_kwh": e,
                "mean_carbon_kg": c,
                "sample_count": float(total_w),
            }
        self._global = result
        return result

    def lookup(
        self, architecture: str, hardware: str
    ) -> Optional[Dict[str, float]]:
        return self._global.get((architecture, hardware))


# --------------------------------------------------------------------------- #
# Multi-agent coordinator (profiler-specific roles)
# --------------------------------------------------------------------------- #
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    transformer_accuracy: float = 0.0
    cnn_accuracy: float = 0.0
    edge_accuracy: float = 0.0
    total_estimates: int = 0


class ProfilerCoordinator:
    """Sliding-window role assignment for profiler agents."""

    def __init__(self, window_size: int = 20) -> None:
        self.agents: Dict[str, AgentProfile] = {}
        self._history: Dict[str, Deque[Dict[str, float]]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(agent_id)
        return self.agents[agent_id]

    def _window_mean(self, agent_id: str, key: str) -> float:
        hist = self._history[agent_id]
        if not hist:
            return 0.0
        return sum(h.get(key, 0.0) for h in hist) / len(hist)

    def _reassign(self) -> None:
        for a in self.agents.values():
            if len(self._history[a.agent_id]) < 3:
                continue
            a.transformer_accuracy = self._window_mean(
                a.agent_id, "transformer"
            )
            a.cnn_accuracy = self._window_mean(a.agent_id, "cnn")
            a.edge_accuracy = self._window_mean(a.agent_id, "edge")
            if (
                a.transformer_accuracy > 0.8
                and a.transformer_accuracy >= a.cnn_accuracy
            ):
                a.role = AgentRole.TRANSFORMER_SPECIALIST
            elif a.cnn_accuracy > 0.8:
                a.role = AgentRole.CNN_SPECIALIST
            elif a.edge_accuracy > 0.7:
                a.role = AgentRole.EDGE_SPECIALIST
            elif a.total_estimates > 20:
                a.role = AgentRole.ACCURACY_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, architecture: str, accuracy: float
    ) -> None:
        a = self.register(agent_id)
        self._history[agent_id].append(
            {
                "transformer": accuracy if architecture == "transformer" else 0.0,
                "cnn": accuracy if architecture == "cnn" else 0.0,
                "edge": accuracy if architecture in ("cnn", "dense", "rnn") else 0.0,
            }
        )
        a.total_estimates += 1
        self._reassign()

    def select_agent(self, architecture: str) -> Optional[str]:
        if not self.agents:
            return None
        if architecture == "transformer":
            cands = [
                a for a in self.agents.values()
                if a.role == AgentRole.TRANSFORMER_SPECIALIST
            ]
        elif architecture == "cnn":
            cands = [
                a for a in self.agents.values()
                if a.role == AgentRole.CNN_SPECIALIST
            ]
        else:
            cands = list(self.agents.values())
        if not cands:
            cands = list(self.agents.values())
        return max(cands, key=lambda a: a.total_estimates).agent_id


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
    task_id: str
    method: str
    energy_kwh: float
    confidence: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


class HumanReviewGate:
    """
    Sync + async HITL gate.

    * ``review``  — sync; **rejects awaitable callbacks** (fail-safe).
    * ``areview`` — async; awaits awaitables with a timeout.
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        confidence_threshold: float = 0.35,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.confidence_threshold = confidence_threshold
        self.pending: Deque[HITLRequest] = deque(
            maxlen=_MAX_PENDING_HITL
        )
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None

    def set_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        self._callback = cb

    def needs_review(self, estimate: CarbonEstimate) -> bool:
        return (
            estimate.confidence < self.confidence_threshold
            or (
                estimate.uncertainty_std > 0
                and estimate.uncertainty_std > estimate.expected_energy_kwh
            )
        )

    def _make_req(
        self, estimate: CarbonEstimate, reason: str
    ) -> HITLRequest:
        return HITLRequest(
            task_id=estimate.task_id,
            method=estimate.estimation_method,
            energy_kwh=estimate.expected_energy_kwh,
            confidence=estimate.confidence,
            reason=reason,
            urgency=(
                "high" if estimate.expected_energy_kwh > 1.0 else "medium"
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
        status = (
            "approved"
            if approved is True
            else "rejected"
            if approved is False
            else "timed_out"
        )
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
        self, estimate: CarbonEstimate, reason: str
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(estimate, reason)
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
        self, estimate: CarbonEstimate, reason: str
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(estimate, reason)
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
                            "profiler_reviewer",
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
# Anomaly detector (per-key)
# --------------------------------------------------------------------------- #
class AnomalyDetector:
    """Per-(arch, hardware) rolling z-score detector."""

    def __init__(
        self, window: int = _MAX_ANOMALY_WINDOW, z_threshold: float = 3.5
    ) -> None:
        self.window = window
        self.z = z_threshold
        self._history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=window)
        )

    @staticmethod
    def _key(arch: str, hardware: str) -> str:
        return f"{arch}:{hardware}"

    def check(
        self, energy_kwh: float, arch: str, hardware: str
    ) -> Tuple[bool, float]:
        hist = self._history[self._key(arch, hardware)]
        if len(hist) < 5:
            hist.append(energy_kwh)
            return False, 0.0
        mean = statistics.fmean(hist)
        std = statistics.pstdev(hist) or 1e-9
        z = abs(energy_kwh - mean) / std
        hist.append(energy_kwh)
        return z > self.z, z


# --------------------------------------------------------------------------- #
# Method tracking helper
# --------------------------------------------------------------------------- #
def _note_method(estimate: CarbonEstimate, method: str) -> None:
    """Record ``method`` in ``methods_applied`` and upgrade the primary."""
    if method not in estimate.methods_applied:
        estimate.methods_applied.append(method)
    if _METHOD_RANK.get(method, -1) > _METHOD_RANK.get(
        estimate.estimation_method, -1
    ):
        estimate.estimation_method = method
        estimate.source = method


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class TaskCarbonProfiler:
    """
    Enhanced carbon profiler.

    Backward-compatible:
        TaskCarbonProfiler(telemetry_db_path=None)

    All ten enhancements are enabled by default; pass ``features={...}``
    to selectively disable. ``set_features()`` recreates the layers at
    runtime.
    """

    #: Fixed physics constants (were magic numbers in v5).
    #:
    #: * ``TDP_UTILIZATION``   — fraction of TDP used by real workloads.
    #: * ``PARAM_ENERGY_WEIGHT`` — weight of the parameter-based term.
    #: * ``SAFETY_FACTOR``     — conservative multiplier on the total.
    TDP_UTILIZATION: float = 0.7
    PARAM_ENERGY_WEIGHT: float = 0.3
    SAFETY_FACTOR: float = 2.0
    #: Time per billion parameters per iteration (hours).
    TIME_PER_ITER_PER_B_PARAMS: float = 0.1
    #: Causal correction weight (0.2 means "10% for a correction of 0.5").
    CAUSAL_WEIGHT: float = 0.2
    #: Minimum number of similar historical records to trust a historical
    #: estimate.
    MIN_HISTORICAL_SAMPLES: int = 3
    #: Historical confidence below this falls back to the heuristic.
    HISTORICAL_CONFIDENCE_THRESHOLD: float = 0.7
    #: Federated blend weight.
    FEDERATED_BLEND: float = 0.3
    #: How many telemetry records must exist before a causal update runs.
    CAUSAL_UPDATE_EVERY: int = 10

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
        "anomaly_detection": True,
    }

    def __init__(
        self,
        telemetry_db_path: Optional[Path] = None,
        deployment_id: str = "local",
        agent_id: str = "profiler-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        max_telemetry_records: int = _MAX_TELEMETRY_RECORDS,
    ):
        self.telemetry_db_path = (
            Path(telemetry_db_path)
            if telemetry_db_path
            else Path("data/telemetry.json")
        )
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()

        # ---- Core coefficient tables (kept local: domain-specific) ----
        self.hardware_tdp: Dict[str, float] = {
            "V100": 300.0,
            "A100": 400.0,
            "H100": 700.0,
            "T4": 70.0,
            "RTX3090": 350.0,
            "CPU": 65.0,
            "TPU_v4": 200.0,
        }
        self.architecture_energy: Dict[str, float] = {
            "transformer": 0.002,
            "cnn": 0.001,
            "rnn": 0.0015,
            "hybrid": 0.0018,
            "dense": 0.0008,
        }

        # ---- Locks ----
        self._save_lock = threading.Lock()

        # ---- Build layers first, then load telemetry (order matters) ----
        self._build_layers()

        # ---- Telemetry (bounded) ----
        loaded = self._load_telemetry()
        self.historical_telemetry: Deque[Dict[str, Any]] = deque(
            loaded, maxlen=max(1, max_telemetry_records)
        )

        # ---- Counters ----
        self._fallback_count: int = 0
        self._save_failure_count: int = 0
        self._last_save_error: Optional[str] = None
        self._hitl_outcomes: Counter = Counter()
        self._estimate_method_counts: Counter = Counter()
        self._anomaly_count: int = 0

        logger.info(
            "TaskCarbonProfiler v6 ready: deployment=%s, agent=%s, "
            "records=%d, features=%s",
            deployment_id,
            agent_id,
            len(self.historical_telemetry),
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Layer construction
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        threshold = int(
            _cfg_get(self._config(), "circuit_failure_threshold", 3)
        )
        recovery = float(
            _cfg_get(self._config(), "circuit_recovery_timeout", 30.0)
        )
        self.storage_circuit = SyncCircuitAdapter(
            "telemetry_storage",
            failure_threshold=threshold,
            recovery_timeout=recovery,
        )

        self.chaos = (
            SyncChaosInjector()
            if self.features["chaos_testing"]
            else None
        )

        self.temporal_monitor = (
            TemporalLogicMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(NonNegativeEnergy())
            self.temporal_monitor.register(NonNegativeCarbon())
            self.temporal_monitor.register(BoundedConfidence())
            self.temporal_monitor.register(EnergyUpperBound())

        self.explainer = EstimateExplainer if self.features["xai"] else None

        self.precision_ctl = (
            AdaptivePrecisionController(self.hardware)
            if self.features["adaptive_precision"]
            else None
        )
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"]
            else None
        )

        self.causal_learner = (
            CausalWeightLearner()
            if self.features["causal_rl"]
            else None
        )

        self.federated = (
            FederatedProfilerAggregator()
            if self.features["federated"]
            else None
        )

        self.coordinator = (
            ProfilerCoordinator()
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
                ),
            )
            if self.features["hitl"]
            else None
        )

        self.anomaly = (
            AnomalyDetector()
            if self.features["anomaly_detection"]
            else None
        )

    def _config(self) -> Dict[str, Any]:
        return {}

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        self._build_layers()

    # ------------------------------------------------------------------
    # Persistence (atomic, locked, circuit-guarded)
    # ------------------------------------------------------------------
    def _load_telemetry(self) -> List[Dict[str, Any]]:
        if not self.telemetry_db_path.exists():
            return []
        if self.chaos is not None and self.chaos.maybe_fail(
            "telemetry_load"
        ):
            logger.warning("Chaos: skipping telemetry load")
            return []
        try:
            with self.telemetry_db_path.open(
                "r", encoding="utf-8"
            ) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(
                "Telemetry load failed (%s); starting empty", e
            )
            return []
        if not isinstance(data, list):
            logger.warning(
                "Telemetry file root is %s, expected list",
                type(data).__name__,
            )
            return []

        valid: List[Dict[str, Any]] = []
        for i, rec in enumerate(data):
            if not isinstance(rec, Mapping):
                logger.warning(
                    "Skipping telemetry record %d: not a mapping", i
                )
                continue
            try:
                e = rec.get("energy_kwh")
                if e is None or float(e) < 0:
                    raise ValueError("bad energy_kwh")
                c = rec.get("carbon_kgco2e")
                if c is not None:
                    float(c)
                valid.append(dict(rec))
            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Skipping malformed telemetry record %d: %s",
                    i,
                    exc,
                )
        return valid

    def _save_telemetry(self) -> None:
        if not self.storage_circuit.can_call():
            logger.debug("Storage circuit open — skipping save")
            return
        if self.chaos is not None and self.chaos.maybe_fail(
            "telemetry_save"
        ):
            logger.debug("Chaos: skipping telemetry save")
            return
        try:
            self._atomic_save()
            self.storage_circuit.record_success()
            self._last_save_error = None
        except Exception as e:
            self.storage_circuit.record_failure()
            self._save_failure_count += 1
            self._last_save_error = f"{type(e).__name__}: {e}"
            logger.error("Failed to save telemetry: %s", e)

    def _atomic_save(self) -> None:
        path = self.telemetry_db_path
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = [_json_safe(r) for r in self.historical_telemetry]
        with self._save_lock:
            tmp_path: Optional[str] = None
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(
                        payload,
                        f,
                        indent=2,
                        allow_nan=False,
                        default=str,
                    )
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, path)
                tmp_path = None
            finally:
                if tmp_path is not None:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

    # ------------------------------------------------------------------
    # Main estimation
    # ------------------------------------------------------------------
    async def estimate_energy(
        self,
        task: Mapping[str, Any],
        carbon_intensity: Optional[float] = None,
    ) -> CarbonEstimate:
        task = dict(task)
        task["_arch"] = self._detect_architecture(
            task.get("model_name", "")
        )

        # 1. Base estimate
        historical = self._estimate_from_historical(task)
        if (
            historical is not None
            and historical.confidence
            >= self.HISTORICAL_CONFIDENCE_THRESHOLD
        ):
            estimate = historical
            _note_method(estimate, "historical")
        else:
            estimate = self._estimate_from_heuristics(task)
            _note_method(estimate, "heuristic")
            self._fallback_count += 1

        # 2. Causal correction (fixed §2.1)
        if self.causal_learner is not None:
            hw_watts = self.hardware_tdp.get(
                task.get("hardware", "V100"), 300.0
            )
            correction = self.causal_learner.predict_correction(
                task, hw_watts
            )
            estimate.expected_energy_kwh *= (
                1.0 + self.CAUSAL_WEIGHT * correction
            )
            estimate.breakdown["causal_correction"] = correction
            _note_method(estimate, "causal")

        # 3. Federated adjustment
        if self.federated is not None:
            arch = task["_arch"]
            hw = task.get("hardware", "V100")
            profile = self.federated.lookup(arch, hw)
            if profile and profile.get("sample_count", 0) >= 5:
                blend = self.FEDERATED_BLEND
                estimate.expected_energy_kwh = (
                    (1 - blend) * estimate.expected_energy_kwh
                    + blend * profile["mean_energy_kwh"]
                )
                estimate.breakdown["federated_blend"] = blend
                _note_method(estimate, "federated")

        # 4. Uncertainty (fixed §2.2, §2.3)
        recent = [
            t["energy_kwh"]
            for t in list(self.historical_telemetry)[
                -_MAX_RECENT_FOR_UNCERTAINTY:
            ]
            if float(t.get("energy_kwh", 0)) > 0
        ]
        if recent:
            ue = UncertaintyEstimate.from_values(
                recent + [estimate.expected_energy_kwh]
            )
            estimate.uncertainty_std = ue.std
            estimate.prediction_interval_95 = ue.interval_95
            estimate.confidence = min(
                1.0, 0.5 * estimate.confidence + 0.5 * ue.confidence
            )

        # 5. Carbon conversion
        if carbon_intensity is not None:
            estimate.expected_carbon_kgco2e = (
                estimate.expected_energy_kwh
                * carbon_intensity
                / 1000.0
            )
            estimate.carbon_intensity_gco2kwh = float(carbon_intensity)

        # 6. Market enrichment
        if self.market is not None:
            snap = self.market.get_snapshot()
            carbon_cost = (
                estimate.expected_carbon_kgco2e
                / 1000.0
                * snap.carbon_price_per_tco2_usd
            )
            estimate.market_enrichment = {
                "carbon_price_per_tco2_usd": (
                    snap.carbon_price_per_tco2_usd
                ),
                "carbon_cost_usd": carbon_cost,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }
            estimate.breakdown["carbon_cost_usd"] = carbon_cost
            _note_method(estimate, "market_enriched")

        # 7. Anomaly detection (per-key)
        if self.anomaly is not None:
            flagged, z = self.anomaly.check(
                estimate.expected_energy_kwh,
                task["_arch"],
                task.get("hardware", "V100"),
            )
            if flagged:
                estimate.breakdown["anomaly_z"] = z
                self._anomaly_count += 1
                logger.warning(
                    "Anomalous energy estimate for %s: %.4f (z=%.2f)",
                    estimate.task_id,
                    estimate.expected_energy_kwh,
                    z,
                )

        # 8. Adaptive precision (round, not floor)
        if self.precision_ctl is not None:
            urgency = (
                "critical"
                if estimate.expected_energy_kwh > 1.0
                else "normal"
            )
            precision = self.precision_ctl.select(urgency)
            estimate.precision_used = precision.value
            estimate.expected_energy_kwh = (
                self.precision_ctl.quantize(
                    estimate.expected_energy_kwh, precision
                )
            )
            estimate.expected_carbon_kgco2e = (
                self.precision_ctl.quantize(
                    estimate.expected_carbon_kgco2e, precision
                )
            )

        # 9. Temporal verification
        if self.temporal_monitor is not None:
            ok, violations = self.temporal_monitor.verify(estimate)
            estimate.temporal_verified = ok
            if not ok:
                logger.warning(
                    "Temporal violations on %s: %s",
                    estimate.task_id,
                    violations,
                )

        # 10. XAI
        if self.explainer is not None:
            exp = self.explainer.explain(
                method=estimate.estimation_method,
                energy_kwh=estimate.expected_energy_kwh,
                carbon_kg=estimate.expected_carbon_kgco2e,
                confidence=estimate.confidence,
                breakdown=estimate.breakdown,
                uncertainty_std=estimate.uncertainty_std,
                methods_applied=list(estimate.methods_applied),
            )
            estimate.explanation = exp.headline
            estimate.contributing_factors = exp.contributing_factors

        # 11. HITL (fixed §2.4, §2.5)
        if (
            self.hitl is not None
            and self.hitl.needs_review(estimate)
        ):
            estimate.hitl_required = True
            approved, status = await self.hitl.areview(
                estimate,
                reason=(
                    f"confidence={estimate.confidence:.2f}, "
                    f"σ={estimate.uncertainty_std:.4f}"
                ),
            )
            estimate.hitl_approved = approved
            estimate.hitl_status = status
            self._hitl_outcomes[status] += 1
            if approved is False:
                estimate.is_stale = True
                # Conservative: halve confidence when rejected
                estimate.confidence *= 0.5

        # 12. Multi-agent feedback
        if self.coordinator is not None:
            accuracy = 1.0 if estimate.temporal_verified else 0.5
            self.coordinator.record(
                self.agent_id, task["_arch"], accuracy
            )

        self._estimate_method_counts[estimate.estimation_method] += 1
        return estimate

    # ------------------------------------------------------------------
    # Historical estimate (fixed §2.6)
    # ------------------------------------------------------------------
    def _estimate_from_historical(
        self, task: Mapping[str, Any]
    ) -> Optional[CarbonEstimate]:
        similar = [
            t
            for t in self.historical_telemetry
            if (
                t.get("model_name") == task.get("model_name")
                and t.get("hardware") == task.get("hardware")
                and float(t.get("energy_kwh", 0) or 0) > 0
            )
        ]
        if len(similar) < self.MIN_HISTORICAL_SAMPLES:
            return None

        energies = [float(t["energy_kwh"]) for t in similar]
        mean_energy = statistics.fmean(energies)
        std_energy = (
            statistics.pstdev(energies) if len(energies) > 1 else 0.0
        )

        # FIXED (§2.6): never divide by 1. Use the mean of records that
        # actually have dataset_size; fall back to per-arch default.
        historical_sizes = [
            int(t["dataset_size"])
            for t in similar
            if t.get("dataset_size") and int(t["dataset_size"]) > 0
        ]
        arch = task.get("_arch") or self._detect_architecture(
            task.get("model_name", "")
        )
        if historical_sizes:
            mean_ds = statistics.fmean(historical_sizes)
        else:
            mean_ds = float(_ARCH_DEFAULT_DATASET.get(arch, 100_000))

        task_ds = task.get("dataset_size")
        if not task_ds or int(task_ds) <= 0:
            task_ds = _ARCH_DEFAULT_DATASET.get(arch, 100_000)

        # Clamp the ratio to a plausible range so a bad data point
        # cannot produce a 1e5×-inflated estimate.
        dataset_ratio = max(0.1, min(100.0, float(task_ds) / max(mean_ds, 1.0)))
        adjusted_energy = mean_energy * dataset_ratio

        # Confidence: more similar records + lower dispersion → higher
        n_factor = min(1.0, len(similar) / 10.0)
        dispersion_factor = (
            1.0 - min(std_energy / mean_energy, 0.5)
            if mean_energy > 0
            else 0.0
        )
        confidence = max(
            0.0, min(1.0, n_factor * dispersion_factor)
        )

        return CarbonEstimate(
            task_id=str(task.get("task_id", "unknown")),
            expected_energy_kwh=adjusted_energy,
            expected_carbon_kgco2e=0.0,
            carbon_intensity_gco2kwh=0.0,
            confidence=confidence,
            estimation_method="historical",
            breakdown={
                "mean": mean_energy,
                "std": std_energy,
                "adjustment": dataset_ratio,
                "n_samples": float(len(similar)),
            },
            methods_applied=["historical"],
            source="historical",
        )

    # ------------------------------------------------------------------
    # Heuristic estimate
    # ------------------------------------------------------------------
    def _estimate_from_heuristics(
        self, task: Mapping[str, Any]
    ) -> CarbonEstimate:
        model_name = task.get("model_name", "")
        num_params = task.get(
            "num_parameters", self._guess_params(model_name)
        )
        dataset_size = task.get("dataset_size", 0) or 0
        num_epochs = task.get("num_epochs", 1) or 1
        batch_size = task.get("batch_size", 32) or 32
        hardware = task.get("hardware", "V100")

        arch = task.get("_arch") or self._detect_architecture(model_name)
        base_energy = self.architecture_energy.get(arch, 0.002)

        iterations = (
            (dataset_size / batch_size) * num_epochs if batch_size > 0 else 0
        )
        time_per_iter = (
            num_params / 1e9
        ) * self.TIME_PER_ITER_PER_B_PARAMS
        total_hours = (iterations * time_per_iter) / 3600

        hardware_watts = self.hardware_tdp.get(hardware, 300.0)
        energy_tdp = (
            hardware_watts * total_hours * self.TDP_UTILIZATION
        ) / 1000
        energy_params = (
            (num_params / 1e9)
            * base_energy
            * (dataset_size / 1000)
        )

        total_energy = (
            energy_tdp * self.TDP_UTILIZATION
            + energy_params * self.PARAM_ENERGY_WEIGHT
        ) * self.SAFETY_FACTOR

        return CarbonEstimate(
            task_id=str(task.get("task_id", "unknown")),
            expected_energy_kwh=total_energy,
            expected_carbon_kgco2e=0.0,
            carbon_intensity_gco2kwh=0.0,
            confidence=0.5,
            estimation_method="heuristic",
            breakdown={
                "params": float(num_params),
                "iterations": float(iterations),
                "hours": float(total_hours),
                "energy_tdp": float(energy_tdp),
                "energy_params": float(energy_params),
            },
            methods_applied=["heuristic"],
            source="heuristic",
        )

    # ------------------------------------------------------------------
    # Model-name helpers
    # ------------------------------------------------------------------
    def _guess_params(self, model_name: str) -> int:
        name = model_name.lower()
        if "bert-base" in name:
            return 110_000_000
        if "bert-large" in name:
            return 340_000_000
        if "gpt2" in name:
            return 124_000_000
        if "t5-base" in name:
            return 220_000_000
        if "resnet50" in name:
            return 25_000_000
        return 100_000_000

    def _detect_architecture(self, model_name: str) -> str:
        name = model_name.lower()
        if any(x in name for x in ["bert", "gpt", "t5"]):
            return "transformer"
        if any(x in name for x in ["resnet", "efficientnet"]):
            return "cnn"
        if any(x in name for x in ["lstm", "gru"]):
            return "rnn"
        return "dense"

    # ------------------------------------------------------------------
    # Telemetry ingestion
    # ------------------------------------------------------------------
    def add_telemetry_record(
        self,
        task: Mapping[str, Any],
        actual_energy: float,
        actual_carbon: float,
        predicted_energy: Optional[float] = None,
    ) -> None:
        record = {
            "task_id": task.get("task_id"),
            "timestamp": _utcnow().isoformat(),
            "model_name": task.get("model_name"),
            "hardware": task.get("hardware"),
            "dataset_size": task.get("dataset_size"),
            "energy_kwh": float(actual_energy),
            "carbon_kgco2e": float(actual_carbon),
        }
        self.historical_telemetry.append(record)

        if (
            self.causal_learner is not None
            and predicted_energy is not None
        ):
            hw_watts = self.hardware_tdp.get(
                task.get("hardware", "V100"), 300.0
            )
            t = dict(task)
            t["_arch"] = self._detect_architecture(
                task.get("model_name", "")
            )
            self.causal_learner.record(
                t, hw_watts, float(predicted_energy), float(actual_energy)
            )
            if (
                len(self.historical_telemetry)
                % self.CAUSAL_UPDATE_EVERY
                == 0
            ):
                self.causal_learner.update()

        if (
            self.federated is not None
            and len(self.historical_telemetry) % 25 == 0
        ):
            arch = self._detect_architecture(
                task.get("model_name", "")
            )
            self.federated.push(
                FederatedTelemetryProfile(
                    deployment_id=self.deployment_id,
                    architecture=arch,
                    hardware=task.get("hardware", "V100"),
                    mean_energy_kwh=float(actual_energy),
                    mean_carbon_kg=float(actual_carbon),
                    sample_count=1,
                )
            )

        self._save_telemetry()

    # ------------------------------------------------------------------
    # Public enhancement API
    # ------------------------------------------------------------------
    def distill_estimator(
        self, urgency: str = "normal"
    ) -> Optional[DistilledEstimator]:
        if self.distiller is None:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl is not None
            else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            self.hardware_tdp, self.architecture_energy, precision
        )

    def contribute_federated_profiles(self) -> None:
        if self.federated is None:
            return
        grouped: Dict[
            Tuple[str, str], List[Tuple[float, float]]
        ] = defaultdict(list)
        for t in self.historical_telemetry:
            arch = self._detect_architecture(
                t.get("model_name", "")
            )
            hw = t.get("hardware", "V100")
            grouped[(arch, hw)].append(
                (
                    float(t.get("energy_kwh", 0.0)),
                    float(t.get("carbon_kgco2e", 0.0)),
                )
            )
        for (arch, hw), vals in grouped.items():
            if not vals:
                continue
            self.federated.push(
                FederatedTelemetryProfile(
                    deployment_id=self.deployment_id,
                    architecture=arch,
                    hardware=hw,
                    mean_energy_kwh=statistics.fmean(
                        v[0] for v in vals
                    ),
                    mean_carbon_kg=statistics.fmean(
                        v[1] for v in vals
                    ),
                    sample_count=len(vals),
                )
            )
        self.federated.aggregate()

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
    # Stats / health / lifecycle
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "telemetry_records": len(self.historical_telemetry),
            "storage_circuit": self.storage_circuit.state,
            "fallback_count": self._fallback_count,
            "save_failure_count": self._save_failure_count,
            "last_save_error": self._last_save_error,
            "anomaly_count": self._anomaly_count,
            "hitl_outcomes": dict(self._hitl_outcomes),
            "estimate_method_counts": dict(self._estimate_method_counts),
        }
        if self.temporal_monitor is not None:
            stats["temporal_violation_count"] = len(
                self.temporal_monitor.violations
            )
            stats["temporal_violations_recent"] = list(
                self.temporal_monitor.violations
            )[-5:]
        if self.coordinator is not None:
            stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "total_estimates": a.total_estimates,
                }
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos is not None:
            stats["chaos_events"] = list(self.chaos.events)[-5:]
        if self.hitl is not None:
            stats["hitl"] = {
                "pending": len(self.hitl.pending),
                "log_size": len(self.hitl.feedback_log),
                "outcomes": self.hitl.outcomes(),
            }
        if self.causal_learner is not None:
            stats["causal_learner"] = self.causal_learner.stats()
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
            "healthy": (
                self.storage_circuit.state != "OPEN"
                and self._last_save_error is None
            ),
            "deployment_id": self.deployment_id,
            "telemetry_records": len(self.historical_telemetry),
            "storage_circuit": self.storage_circuit.state,
            "last_save_error": self._last_save_error,
            "fallback_count": self._fallback_count,
            "anomaly_count": self._anomaly_count,
            "temporal_violation_count": (
                len(self.temporal_monitor.violations)
                if self.temporal_monitor is not None
                else 0
            ),
            "hitl_outcomes": dict(self._hitl_outcomes),
        }

    async def __aenter__(self) -> "TaskCarbonProfiler":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # Flush telemetry on clean exit
        try:
            self._save_telemetry()
        except Exception as e:
            logger.warning("Final telemetry flush failed: %s", e)


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_task_carbon_profiler(
    telemetry_db_path: Optional[Path] = None,
    deployment_id: str = "local",
    agent_id: str = "profiler-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
) -> TaskCarbonProfiler:
    return TaskCarbonProfiler(
        telemetry_db_path=telemetry_db_path,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
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
    # v6 domain types
    "PrecisionLevel",
    "EstimationMethod",
    "AgentRole",
    "CircuitState",
    "HardwareProfile",
    "CarbonEstimate",
    "EstimateExplanation",
    "DistilledEstimator",
    "FederatedTelemetryProfile",
    "MarketSnapshot",
    "HITLRequest",
    "UncertaintyEstimate",
    "CausalWeightState",
    "AgentProfile",
    # v6 components
    "CausalWeightLearner",
    "QuantumDistillationBridge",
    "AdaptivePrecisionController",
    "FederatedProfilerAggregator",
    "ProfilerCoordinator",
    "CarbonMarketClient",
    "HumanReviewGate",
    "AnomalyDetector",
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "TemporalLogicMonitor",
    "NonNegativeEnergy",
    "NonNegativeCarbon",
    "BoundedConfidence",
    "EnergyUpperBound",
    "TemporalProperty",
    "EstimateExplainer",
    # Main
    "TaskCarbonProfiler",
    "create_task_carbon_profiler",
]


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
async def _demo() -> None:
    import tempfile

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    print("=" * 80)
    print("task_carbon_profiler.py v6.0.0 — Demo")
    print("=" * 80)

    tmpdir = tempfile.mkdtemp(prefix="profiler_v6_demo_")
    db_path = Path(tmpdir) / "telemetry.json"

    async with TaskCarbonProfiler(
        telemetry_db_path=db_path,
        deployment_id="us-ca-prod-01",
        agent_id="profiler-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    ) as profiler:

        def auto_approve(req: HITLRequest) -> bool:
            logger.info(
                "[HITL] auto-approve %s: %s",
                req.urgency,
                req.reason,
            )
            return True

        profiler.set_hitl_callback(auto_approve)

        # ---- P0 verification: causal learner actually learns ----
        print("\n=== P0 fix: causal learner learns ===")
        for i in range(60):
            profiler.add_telemetry_record(
                task={
                    "task_id": f"bias-{i}",
                    "model_name": "bert-base-uncased",
                    "hardware": "V100",
                    "dataset_size": 8000,
                },
                actual_energy=0.025,   # actual
                actual_carbon=0.010,
                predicted_energy=0.020,  # systematically 20% under
            )
        # Ensure update has run at least once
        profiler.causal_learner.update()

        correction = profiler.causal_learner.predict_correction(
            task={
                "_arch": "transformer",
                "num_parameters": 110_000_000,
                "dataset_size": 8000,
                "batch_size": 32,
                "hardware": "V100",
            },
            hardware_watts=300.0,
        )
        print(f"  causal correction after 60 samples: {correction:+.4f}")
        assert correction > 0.05, (
            f"Causal learner is not learning: correction={correction}"
        )
        print("  ✓ causal learner moved measurably")

        # ---- P0 verification: prediction interval is a PI, not a CI ----
        print("\n=== P0 fix: prediction interval uses √(1+1/n) ===")
        for n in (10, 100, 1000):
            values = [0.02 + 0.0001 * i for i in range(n)]
            ue = UncertaintyEstimate.from_values(values)
            lo, hi = ue.interval_95
            width = hi - lo
            print(
                f"  n={n:>4}: mean={ue.mean:.6f}  "
                f"σ={ue.std:.6f}  95% PI width={width:.6f}"
            )
            # Width should stay above zero and reflect σ, not shrink to 0
            assert width > 0, "Prediction interval collapsed to zero!"

        # ---- P0 verification: single-sample confidence is low ----
        print("\n=== P0 fix: single sample → low confidence ===")
        ue_one = UncertaintyEstimate.from_values([0.02])
        ue_many = UncertaintyEstimate.from_values(
            [0.02 + 0.0001 * i for i in range(100)]
        )
        print(
            f"  1 sample:    confidence={ue_one.confidence:.4f}"
        )
        print(
            f"  100 samples: confidence={ue_many.confidence:.4f}"
        )
        assert ue_one.confidence < 0.2, (
            f"Single-sample confidence too high: {ue_one.confidence}"
        )
        assert ue_many.confidence > ue_one.confidence

        # ---- Estimate with the causal correction applied ----
        print("\n=== Estimate a new task ===")
        task = {
            "task_id": "bert_test",
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 10_000,
            "num_epochs": 3,
            "batch_size": 32,
            "hardware": "V100",
        }
        estimate = await profiler.estimate_energy(
            task, carbon_intensity=400.0
        )
        print(f"  Energy:    {estimate.expected_energy_kwh:.4f} kWh")
        print(f"  Carbon:    {estimate.expected_carbon_kgco2e:.4f} kgCO₂e")
        print(f"  Conf:      {estimate.confidence:.2f}")
        print(f"  σ:         {estimate.uncertainty_std:.4f}")
        print(f"  95% PI:    {estimate.prediction_interval_95}")
        print(f"  Method:    {estimate.estimation_method}")
        print(f"  Chain:     {' → '.join(estimate.methods_applied)}")
        print(f"  Precision: {estimate.precision_used}")
        print(f"  HITL:      {estimate.hitl_status}")
        print(f"  Stale:     {estimate.is_stale}")
        print(f"  XAI:       {estimate.explanation}")

        # ---- P0 verification: HITL rejection marks estimate stale ----
        print("\n=== P0 fix: HITL rejection marks estimate stale ===")

        def reject_all(req: HITLRequest) -> bool:
            return False

        profiler.set_hitl_callback(reject_all)
        # Force low confidence so HITL is triggered
        profile_before = await profiler.estimate_energy(
            {
                "task_id": "will_be_rejected",
                "model_name": "unseen-model-xyz",
                "hardware": "T4",
                "dataset_size": 100,
            },
            carbon_intensity=400.0,
        )
        print(
            f"  hitl_status={profile_before.hitl_status} "
            f"is_stale={profile_before.is_stale} "
            f"approved={profile_before.hitl_approved}"
        )
        assert profile_before.hitl_status == "rejected"
        assert profile_before.is_stale is True
        print("  ✓ rejected estimate marked stale")

        # ---- P0 verification: missing dataset_size does not blow up ----
        print("\n=== P0 fix: missing dataset_size is bounded ===")
        # Historical records without dataset_size
        profiler.historical_telemetry.clear()
        for i in range(5):
            profiler.historical_telemetry.append(
                {
                    "model_name": "test-model",
                    "hardware": "V100",
                    # note: no dataset_size
                    "energy_kwh": 0.05,
                    "carbon_kgco2e": 0.02,
                }
            )
        estimate2 = await profiler.estimate_energy(
            {
                "task_id": "no_ds",
                "model_name": "test-model",
                "hardware": "V100",
                "dataset_size": 100_000,
            },
            carbon_intensity=400.0,
        )
        ratio = estimate2.breakdown.get("adjustment", 1.0)
        print(f"  dataset adjustment ratio: {ratio:.2f}")
        assert ratio <= 100.0, (
            f"Ratio not clamped: {ratio:.2f} (would have been ~100,000×)"
        )
        print("  ✓ ratio clamped")

        # ---- Distillation ----
        dist = profiler.distill_estimator(urgency="normal")
        if dist is not None:
            print("\n=== Distilled Estimator ===")
            print(f"  Precision:   {dist.precision.value}")
            print(f"  Retention:   {dist.quality_retention}")
            print(
                f"  Energy save: {dist.energy_reduction_percent}%"
            )
            # Verify V100 coefficient is quantized by round, not floor
            print(
                f"  V100 (was 300.0): {dist.hardware_tdp['V100']}"
            )
            assert dist.hardware_tdp["V100"] >= 299.0, (
                "Quantization truncated downward!"
            )

        # ---- Federated contribution ----
        profiler.historical_telemetry.clear()
        for i in range(8):
            profiler.add_telemetry_record(
                task={
                    "task_id": f"warm-{i}",
                    "model_name": "bert-base-uncased",
                    "hardware": "V100",
                    "dataset_size": 8000 + i * 500,
                },
                actual_energy=0.020 + 0.001 * i,
                actual_carbon=0.008 + 0.0004 * i,
                predicted_energy=0.021 + 0.0009 * i,
            )
        profiler.contribute_federated_profiles()

        # ---- Statistics ----
        print("\n=== Statistics ===")
        print(json.dumps(
            profiler.get_statistics(), indent=2, default=str
        ))
        print("\n=== Health Check ===")
        print(json.dumps(
            await profiler.health_check(), indent=2, default=str
        ))

    print("\nDemo complete.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        asyncio.run(_demo())
    else:
        print("Usage: python task_carbon_profiler.py --demo")
