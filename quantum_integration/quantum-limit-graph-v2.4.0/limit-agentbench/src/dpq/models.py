# src/dpq/models.py

"""
DPQ Shared Models — v2.0.0
===========================

Single source of truth for enums and dataclasses shared across the DPQ
subsystem: :class:`ModelPrecision`, :class:`CarbonZone`, :class:`ModelVariant`,
:class:`ConversionResult`, :class:`PrecisionTransition`, and :class:`DPQMetrics`.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``ConversionResult.conversion_time_ms`` now rejects ``NaN`` / ``Inf``.
        Previously ``math.nan < 0`` was ``False``, so a NaN slipped through
        ``__post_init__`` and poisoned every downstream ``statistics()`` mean.
  * 2.2 ``ModelVariant.precision`` is now type-validated. Previously a string
        ``"fp16"`` passed ``__post_init__`` and crashed later with a raw
        ``KeyError`` inside ``is_available()``.
  * 2.3 ``DPQConfig.min_accuracy()`` raises ``DPQModelError`` (not
        ``KeyError``) on unknown precision.
  * 2.4 ``DPQConfig.from_legacy_dict`` added — the *single* helper that
        downstream modules (``carbon_intensity_monitor``,
        ``model_converter``) need for drift-free config reconstruction. The
        root cause of the config-drift bug in all eight earlier files.
  * 2.5 All three ``from_dict`` methods normalise enum strings
        case-insensitively (``"FP16"``, ``"RED"``, ``"  green  "``) and
        raise ``DPQModelError`` (not ``ValueError``) on invalid values.

**P1 robustness**
  * 3.2 New ``TransitionStatus`` / ``TransitionTrigger`` enums for the
        free-form strings on ``PrecisionTransition``. Field types remain
        ``str`` (backward compatible); ``from_dict`` normalises case.
  * 3.4 ``DPQConfig.__post_init__`` now validates ``min_accuracy`` ordering
        (int4 ≤ int8 ≤ fp16 ≤ fp32). Backwards-incompatible for invalid
        configs — which were always semantically wrong.
  * 3.5 Numeric config fields reject ``bool``.
  * 3.6 ``PrecisionTransition.carbon_intensity`` validated (finite, >= 0).
  * 3.7 ``PrecisionTransition.carbon_zone`` type-validated.
  * 3.8 ``ModelVariant.conversion_time_ms`` validated (finite, >= 0).
  * 3.9 ``PrecisionTransition.duration_ms()`` clamps negative deltas to 0
        with a warning.
  * 3.10 ``DPQMetrics.precision_counts`` keys coerced to ``ModelPrecision``
         in ``__post_init__``; ``carbon_zone_current`` type-checked;
         accuracy fields bounded; timestamp UTC-normalised.

**P2 polish**
  * 4.1 ``ModelPrecision.__str__`` / ``CarbonZone.__str__`` return ``.value``
        (avoids the ``f"{precision}"`` → ``"ModelPrecision.FP16"`` footgun).
  * 4.2 ``ConversionResult.size_reduction_percent`` bounded to ``[-100, 100]``.
  * 4.5 ``ModelVariant.record_usage()`` convenience method.

Backward compatible: every field, every method, every serialized payload
from v1.0.0 loads unchanged.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DPQModelError(ValueError):
    """Raised for invalid DPQ model values."""


# --------------------------------------------------------------------------- #
# Enum coercion helper (P0 2.5)
# --------------------------------------------------------------------------- #
def _coerce_enum(
    enum_cls: type,
    value: Any,
    *,
    field_name: str,
) -> Any:
    """
    Coerce ``value`` to an instance of ``enum_cls``.

    Accepts an existing enum, or a string (case-insensitive, whitespace
    stripped). Raises ``DPQModelError`` on invalid values — never a raw
    ``ValueError``.
    """
    if isinstance(value, enum_cls):
        return value
    if value is None or isinstance(value, bool):
        raise DPQModelError(
            f"{field_name} must be a {enum_cls.__name__}, got {value!r}.")
    try:
        normalized = str(value).strip().lower()
    except Exception:  # noqa: BLE001
        raise DPQModelError(
            f"{field_name} must be a {enum_cls.__name__}, "
            f"got {type(value).__name__}.") from None
    try:
        return enum_cls(normalized)
    except ValueError as exc:
        raise DPQModelError(
            f"{field_name} must be a valid {enum_cls.__name__}, "
            f"got {value!r}.") from exc


def _coerce_ts(value: Any, *, field_name: str) -> Optional[datetime]:
    """Parse a timestamp (datetime or ISO string), normalising to UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise DPQModelError(
            f"{field_name} is not a valid ISO 8601 timestamp: "
            f"{value!r}.") from exc
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _validate_finite_nonneg(value: Any, *, field_name: str) -> float:
    """Reject bool, non-numeric, NaN, Inf, or negative values."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DPQModelError(
            f"{field_name} must be numeric, got {type(value).__name__}.")
    fvalue = float(value)
    if math.isnan(fvalue) or math.isinf(fvalue):
        raise DPQModelError(f"{field_name} must be finite, got {value!r}.")
    if fvalue < 0:
        raise DPQModelError(f"{field_name} must be >= 0, got {fvalue}.")
    return fvalue


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ModelPrecision(str, Enum):
    """Supported model precision levels."""

    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"

    def __str__(self) -> str:  # P2 4.1
        return self.value


class CarbonZone(str, Enum):
    """Carbon intensity zones."""

    GREEN = "green"        # < 50 gCO2/kWh
    YELLOW = "yellow"      # 50-200 gCO2/kWh
    RED = "red"            # 200-400 gCO2/kWh
    CRITICAL = "critical"  # > 400 gCO2/kWh

    def __str__(self) -> str:  # P2 4.1
        return self.value


class TransitionStatus(str, Enum):
    """Standard lifecycle states for a precision transition (P1 3.2)."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class TransitionTrigger(str, Enum):
    """Standard triggers for a precision transition (P1 3.2)."""

    CARBON_ZONE_CHANGE = "carbon_zone_change"
    MANUAL = "manual"
    ACCURACY_GUARDIAN = "accuracy_guardian"

    def __str__(self) -> str:
        return self.value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DPQConfig:
    """Tunable parameters shared across DPQ components."""

    green_threshold: float = 50.0
    yellow_threshold: float = 200.0
    red_threshold: float = 400.0

    min_accuracy_fp32: float = 0.95
    min_accuracy_fp16: float = 0.93
    min_accuracy_int8: float = 0.90
    min_accuracy_int4: float = 0.85

    def __post_init__(self) -> None:
        # P1 3.5: reject bools (they pass isinstance(x, (int, float))).
        for name in (
            "green_threshold", "yellow_threshold", "red_threshold",
            "min_accuracy_fp32", "min_accuracy_fp16",
            "min_accuracy_int8", "min_accuracy_int4",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise DPQModelError(
                    f"{name} must be numeric, "
                    f"got {type(value).__name__}.")
            if math.isnan(value) or math.isinf(value):
                raise DPQModelError(
                    f"{name} must be finite, got {value!r}.")

        # Carbon thresholds: strictly increasing, positive.
        if not (
            0 < self.green_threshold < self.yellow_threshold
            < self.red_threshold
        ):
            raise DPQModelError(
                "Carbon thresholds must be strictly increasing and "
                "positive.")

        # min_accuracy values: in [0, 1].
        for name in (
            "min_accuracy_fp32", "min_accuracy_fp16",
            "min_accuracy_int8", "min_accuracy_int4",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise DPQModelError(f"{name} must be in [0, 1].")

        # P1 3.4: min_accuracy ordering.
        if not (
            self.min_accuracy_int4 <= self.min_accuracy_int8
            <= self.min_accuracy_fp16 <= self.min_accuracy_fp32
        ):
            raise DPQModelError(
                "min_accuracy must satisfy: "
                "int4 ≤ int8 ≤ fp16 ≤ fp32 (got "
                f"int4={self.min_accuracy_int4}, "
                f"int8={self.min_accuracy_int8}, "
                f"fp16={self.min_accuracy_fp16}, "
                f"fp32={self.min_accuracy_fp32}).")

    # ---------------------------------------------------------------- helpers
    def classify(self, intensity: float) -> CarbonZone:
        """Return the :class:`CarbonZone` for ``intensity`` (gCO2/kWh)."""
        if intensity < self.green_threshold:
            return CarbonZone.GREEN
        if intensity < self.yellow_threshold:
            return CarbonZone.YELLOW
        if intensity < self.red_threshold:
            return CarbonZone.RED
        return CarbonZone.CRITICAL

    def min_accuracy(self, precision: ModelPrecision) -> float:
        """Return the minimum acceptable accuracy for ``precision`` (P0 2.3)."""
        try:
            return {
                ModelPrecision.FP32: self.min_accuracy_fp32,
                ModelPrecision.FP16: self.min_accuracy_fp16,
                ModelPrecision.INT8: self.min_accuracy_int8,
                ModelPrecision.INT4: self.min_accuracy_int4,
            }[precision]
        except (KeyError, TypeError) as exc:
            raise DPQModelError(
                f"unknown precision {precision!r}; expected a "
                f"ModelPrecision instance.") from exc

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "DPQConfig":
        """
        Build from a flat mapping; unknown keys ignored (P0 2.4).

        This is the single helper every downstream module should use when
        reconstructing a ``DPQConfig`` from a serialized payload — it
        guarantees that new fields added here use their declared defaults,
        not the old hardcoded ones.
        """
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Conversion result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ConversionResult:
    """Immutable result of a single model conversion."""

    success: bool
    from_precision: str
    to_precision: str
    model_name: str
    conversion_time_ms: float
    size_reduction_percent: float
    accuracy_delta: float           # negative = degradation

    def __post_init__(self) -> None:
        if not isinstance(self.model_name, str) or not self.model_name:
            raise DPQModelError(
                "model_name must be a non-empty string.")

        # P0 2.1: NaN/Inf guard for conversion_time_ms.
        _validate_finite_nonneg(
            self.conversion_time_ms,
            field_name="conversion_time_ms")

        # Existing NaN/Inf checks — now with type checking too.
        if isinstance(self.size_reduction_percent, bool) or \
                not isinstance(self.size_reduction_percent,
                               (int, float)):
            raise DPQModelError(
                "size_reduction_percent must be numeric.")
        if math.isnan(self.size_reduction_percent) or \
                math.isinf(self.size_reduction_percent):
            raise DPQModelError(
                "size_reduction_percent must be finite.")
        # P2 4.2: bound to [-100, 100].
        if not -100.0 <= self.size_reduction_percent <= 100.0:
            raise DPQModelError(
                "size_reduction_percent must be in [-100, 100], "
                f"got {self.size_reduction_percent}.")

        if isinstance(self.accuracy_delta, bool) or \
                not isinstance(self.accuracy_delta, (int, float)):
            raise DPQModelError("accuracy_delta must be numeric.")
        if math.isnan(self.accuracy_delta) or \
                math.isinf(self.accuracy_delta):
            raise DPQModelError("accuracy_delta must be finite.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ConversionResult":
        if not isinstance(data, Mapping):
            raise DPQModelError(
                f"ConversionResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        return cls(
            success=bool(data["success"]),
            from_precision=str(data.get("from_precision", "none")),
            to_precision=str(data.get("to_precision", "none")),
            model_name=str(data["model_name"]),
            conversion_time_ms=float(
                data.get("conversion_time_ms", 0.0)),
            size_reduction_percent=float(
                data.get("size_reduction_percent", 0.0)),
            accuracy_delta=float(data.get("accuracy_delta", 0.0)),
        )


# --------------------------------------------------------------------------- #
# Model variant
# --------------------------------------------------------------------------- #
@dataclass
class ModelVariant:
    """A model at a specific precision level."""

    model_name: str
    precision: ModelPrecision
    size_mb: float
    accuracy: float
    conversion_time_ms: float
    backend: str
    created_at: datetime
    last_used: Optional[datetime] = None
    usage_count: int = 0

    def __post_init__(self) -> None:
        # P0 2.2: precision must be a ModelPrecision, not a raw string.
        if not isinstance(self.precision, ModelPrecision):
            raise DPQModelError(
                f"precision must be a ModelPrecision instance, "
                f"got {type(self.precision).__name__} "
                f"({self.precision!r}). Use "
                f"ModelVariant.from_dict() for case-insensitive "
                f"string parsing.")

        if not isinstance(self.model_name, str) or not self.model_name:
            raise DPQModelError(
                "model_name must be a non-empty string.")

        # P1 3.8: conversion_time_ms validation.
        _validate_finite_nonneg(
            self.conversion_time_ms,
            field_name="conversion_time_ms")

        if isinstance(self.size_mb, bool) or \
                not isinstance(self.size_mb, (int, float)):
            raise DPQModelError("size_mb must be numeric.")
        if math.isnan(self.size_mb) or math.isinf(self.size_mb):
            raise DPQModelError("size_mb must be finite.")
        if self.size_mb < 0:
            raise DPQModelError("size_mb must be >= 0.")

        if isinstance(self.accuracy, bool) or \
                not isinstance(self.accuracy, (int, float)):
            raise DPQModelError("accuracy must be numeric.")
        if not 0.0 <= self.accuracy <= 1.0:
            raise DPQModelError("accuracy must be in [0, 1].")

        if isinstance(self.usage_count, bool) or \
                not isinstance(self.usage_count, int):
            raise DPQModelError("usage_count must be an int.")
        if self.usage_count < 0:
            raise DPQModelError("usage_count must be >= 0.")

        # Normalise timestamps to UTC.
        if not isinstance(self.created_at, datetime):
            raise DPQModelError("created_at must be a datetime.")
        if not self.created_at.tzinfo:
            self.created_at = self.created_at.replace(tzinfo=timezone.utc)
        if self.last_used is not None:
            if not isinstance(self.last_used, datetime):
                raise DPQModelError(
                    "last_used must be a datetime or None.")
            if not self.last_used.tzinfo:
                self.last_used = self.last_used.replace(
                    tzinfo=timezone.utc)

    def is_available(self, config: Optional[DPQConfig] = None) -> bool:
        """Return True when the variant meets its precision's floor."""
        cfg = config or DPQConfig()
        return self.accuracy >= cfg.min_accuracy(self.precision)

    def record_usage(self) -> None:
        """Bump ``usage_count`` and refresh ``last_used`` (P2 4.5)."""
        self.usage_count += 1
        self.last_used = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "precision": self.precision.value,
            "size_mb": self.size_mb,
            "accuracy": self.accuracy,
            "conversion_time_ms": self.conversion_time_ms,
            "backend": self.backend,
            "created_at": self.created_at.isoformat(),
            "last_used": (
                self.last_used.isoformat() if self.last_used else None
            ),
            "usage_count": self.usage_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ModelVariant":
        if not isinstance(data, Mapping):
            raise DPQModelError(
                f"ModelVariant.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        return cls(
            model_name=str(data["model_name"]),
            # P0 2.5: case-insensitive enum coercion.
            precision=_coerce_enum(
                ModelPrecision, data["precision"],
                field_name="precision"),
            size_mb=float(data.get("size_mb", 0.0)),
            accuracy=float(data.get("accuracy", 0.0)),
            conversion_time_ms=float(
                data.get("conversion_time_ms", 0.0)),
            backend=str(data.get("backend", "unknown")),
            created_at=(
                _coerce_ts(
                    data.get("created_at"),
                    field_name="created_at")
                or datetime.now(timezone.utc)
            ),
            last_used=_coerce_ts(
                data.get("last_used"), field_name="last_used"),
            usage_count=int(data.get("usage_count", 0)),
        )


# --------------------------------------------------------------------------- #
# Precision transition
# --------------------------------------------------------------------------- #
@dataclass
class PrecisionTransition:
    """Records a precision transition event."""

    transition_id: str
    region: str
    model_name: str
    from_precision: Optional[ModelPrecision]
    to_precision: ModelPrecision
    triggered_by: str
    carbon_intensity: float
    carbon_zone: CarbonZone
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "pending"
    conversion_results: List[ConversionResult] = field(
        default_factory=list)
    error_message: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.transition_id, str) or \
                not self.transition_id:
            raise DPQModelError(
                "transition_id must be a non-empty string.")
        if not isinstance(self.region, str) or not self.region:
            raise DPQModelError(
                "region must be a non-empty string.")
        if not isinstance(self.model_name, str) or not self.model_name:
            raise DPQModelError(
                "model_name must be a non-empty string.")

        # Precision type checks.
        if self.from_precision is not None and not isinstance(
            self.from_precision, ModelPrecision
        ):
            raise DPQModelError(
                "from_precision must be a ModelPrecision or None.")
        if not isinstance(self.to_precision, ModelPrecision):
            raise DPQModelError(
                "to_precision must be a ModelPrecision instance.")

        # P1 3.7: carbon_zone type check.
        if not isinstance(self.carbon_zone, CarbonZone):
            raise DPQModelError(
                "carbon_zone must be a CarbonZone instance.")

        # P1 3.6: carbon_intensity validation.
        _validate_finite_nonneg(
            self.carbon_intensity, field_name="carbon_intensity")

        # P1 3.2: status validated against TransitionStatus.
        if self.status not in [s.value for s in TransitionStatus]:
            raise DPQModelError(
                f"status must be one of "
                f"{sorted(s.value for s in TransitionStatus)}, "
                f"got {self.status!r}.")

        # P1 3.2: warn on unknown triggered_by (do not fail — free-form).
        if self.triggered_by not in [
            t.value for t in TransitionTrigger
        ]:
            logger.warning(
                "PrecisionTransition %s has non-standard triggered_by=%r "
                "(known: %s).",
                self.transition_id,
                self.triggered_by,
                sorted(t.value for t in TransitionTrigger))

        # Type-validate conversion_results.
        for i, r in enumerate(self.conversion_results):
            if not isinstance(r, ConversionResult):
                raise DPQModelError(
                    f"conversion_results[{i}] must be a ConversionResult, "
                    f"got {type(r).__name__}.")

        # Normalise timestamps to UTC.
        if not isinstance(self.start_time, datetime):
            raise DPQModelError("start_time must be a datetime.")
        if not self.start_time.tzinfo:
            self.start_time = self.start_time.replace(tzinfo=timezone.utc)
        if self.end_time is not None:
            if not isinstance(self.end_time, datetime):
                raise DPQModelError(
                    "end_time must be a datetime or None.")
            if not self.end_time.tzinfo:
                self.end_time = self.end_time.replace(
                    tzinfo=timezone.utc)

    def duration_ms(self) -> Optional[float]:
        """Return transition duration in ms, or None (P1 3.9)."""
        if self.end_time is None:
            return None
        delta = (
            self.end_time - self.start_time
        ).total_seconds() * 1000.0
        if delta < 0:
            logger.warning(
                "PrecisionTransition %s: end_time (%s) is before "
                "start_time (%s); clamping duration to 0.",
                self.transition_id,
                self.end_time.isoformat(),
                self.start_time.isoformat())
            return 0.0
        return delta

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "region": self.region,
            "model_name": self.model_name,
            "from_precision": (
                self.from_precision.value
                if self.from_precision else None
            ),
            "to_precision": self.to_precision.value,
            "triggered_by": self.triggered_by,
            "carbon_intensity": self.carbon_intensity,
            "carbon_zone": self.carbon_zone.value,
            "start_time": self.start_time.isoformat(),
            "end_time": (
                self.end_time.isoformat() if self.end_time else None
            ),
            "status": self.status,
            "conversion_results": [
                r.to_dict() for r in self.conversion_results
            ],
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PrecisionTransition":
        if not isinstance(data, Mapping):
            raise DPQModelError(
                f"PrecisionTransition.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        from_raw = data.get("from_precision")

        # P1 3.2: normalise status/triggered_by case-insensitively.
        raw_status = str(data.get("status", "pending")).strip().lower()
        raw_triggered = str(
            data.get("triggered_by", "manual")).strip().lower()

        return cls(
            transition_id=str(data["transition_id"]),
            region=str(data["region"]),
            model_name=str(data["model_name"]),
            from_precision=(
                _coerce_enum(
                    ModelPrecision, from_raw,
                    field_name="from_precision")
                if from_raw else None
            ),
            to_precision=_coerce_enum(
                ModelPrecision, data["to_precision"],
                field_name="to_precision"),
            triggered_by=raw_triggered,
            carbon_intensity=float(
                data.get("carbon_intensity", 0.0)),
            carbon_zone=_coerce_enum(
                CarbonZone, data["carbon_zone"],
                field_name="carbon_zone"),
            start_time=_coerce_ts(
                data["start_time"], field_name="start_time"),
            end_time=_coerce_ts(
                data.get("end_time"), field_name="end_time"),
            status=raw_status,
            conversion_results=[
                ConversionResult.from_dict(r)
                for r in data.get("conversion_results", [])
            ],
            error_message=data.get("error_message"),
        )


# --------------------------------------------------------------------------- #
# Aggregated metrics
# --------------------------------------------------------------------------- #
@dataclass
class DPQMetrics:
    """Aggregated metrics for the DPQ module."""

    region: str
    timestamp: datetime
    precision_counts: Dict[ModelPrecision, int]
    energy_fp32_baseline_kwh: float
    energy_current_kwh: float
    energy_savings_percent: float
    accuracy_fp32_baseline: float
    accuracy_current: float
    accuracy_delta: float
    avg_conversion_time_ms: float
    p99_conversion_time_ms: float
    transition_count_24h: int
    carbon_intensity_current: float
    carbon_zone_current: CarbonZone
    estimated_carbon_saved_kg: float

    def __post_init__(self) -> None:
        """P1 3.10 / 6.2: validate everything that could silently corrupt."""
        if not isinstance(self.region, str) or not self.region:
            raise DPQModelError(
                "region must be a non-empty string.")

        # Timestamp.
        if not isinstance(self.timestamp, datetime):
            raise DPQModelError("timestamp must be a datetime.")
        if not self.timestamp.tzinfo:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)

        # precision_counts: coerce keys to ModelPrecision.
        if not isinstance(self.precision_counts, Mapping):
            raise DPQModelError(
                "precision_counts must be a Mapping.")
        coerced_counts: Dict[ModelPrecision, int] = {}
        for k, v in self.precision_counts.items():
            key = _coerce_enum(
                ModelPrecision, k,
                field_name="precision_counts key")
            if isinstance(v, bool) or not isinstance(v, int):
                raise DPQModelError(
                    f"precision_counts[{key.value}] must be an int, "
                    f"got {type(v).__name__}.")
            if v < 0:
                raise DPQModelError(
                    f"precision_counts[{key.value}] must be >= 0.")
            coerced_counts[key] = v
        self.precision_counts = coerced_counts

        # carbon_zone_current: coerce if string, validate type.
        if not isinstance(self.carbon_zone_current, CarbonZone):
            self.carbon_zone_current = _coerce_enum(
                CarbonZone, self.carbon_zone_current,
                field_name="carbon_zone_current")

        # Finite + non-negative numerics.
        for name in (
            "energy_fp32_baseline_kwh", "energy_current_kwh",
            "avg_conversion_time_ms", "p99_conversion_time_ms",
            "carbon_intensity_current", "estimated_carbon_saved_kg",
        ):
            _validate_finite_nonneg(
                getattr(self, name), field_name=name)

        # P1 6.2: accuracy bounds.
        for name in ("accuracy_fp32_baseline", "accuracy_current"):
            value = getattr(self, name)
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise DPQModelError(f"{name} must be numeric.")
            if math.isnan(value) or math.isinf(value):
                raise DPQModelError(f"{name} must be finite.")
            if not 0.0 <= value <= 1.0:
                raise DPQModelError(
                    f"{name} must be in [0, 1], got {value}.")

        # accuracy_delta: finite.
        if isinstance(self.accuracy_delta, bool) or \
                not isinstance(self.accuracy_delta, (int, float)):
            raise DPQModelError("accuracy_delta must be numeric.")
        if math.isnan(self.accuracy_delta) or \
                math.isinf(self.accuracy_delta):
            raise DPQModelError("accuracy_delta must be finite.")

        # energy_savings_percent: finite + a semantic sanity check.
        if isinstance(self.energy_savings_percent, bool) or \
                not isinstance(self.energy_savings_percent,
                               (int, float)):
            raise DPQModelError(
                "energy_savings_percent must be numeric.")
        if math.isnan(self.energy_savings_percent) or \
                math.isinf(self.energy_savings_percent):
            raise DPQModelError(
                "energy_savings_percent must be finite.")
        if not -100.0 <= self.energy_savings_percent <= 100.0:
            # Not an error — some accounting schemes count "savings" as
            # including offset credits. Warn instead of reject.
            logger.warning(
                "DPQMetrics energy_savings_percent=%.2f is outside "
                "[-100, 100]; check the baseline.",
                self.energy_savings_percent)

        # transition_count_24h: non-negative int.
        if isinstance(self.transition_count_24h, bool) or \
                not isinstance(self.transition_count_24h, int):
            raise DPQModelError(
                "transition_count_24h must be an int.")
        if self.transition_count_24h < 0:
            raise DPQModelError(
                "transition_count_24h must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "region": self.region,
            "timestamp": self.timestamp.isoformat(),
            "precision_counts": {
                p.value: c for p, c in self.precision_counts.items()
            },
            "energy_fp32_baseline_kwh": self.energy_fp32_baseline_kwh,
            "energy_current_kwh": self.energy_current_kwh,
            "energy_savings_percent": self.energy_savings_percent,
            "accuracy_fp32_baseline": self.accuracy_fp32_baseline,
            "accuracy_current": self.accuracy_current,
            "accuracy_delta": self.accuracy_delta,
            "avg_conversion_time_ms": self.avg_conversion_time_ms,
            "p99_conversion_time_ms": self.p99_conversion_time_ms,
            "transition_count_24h": self.transition_count_24h,
            "carbon_intensity_current": self.carbon_intensity_current,
            "carbon_zone_current": self.carbon_zone_current.value,
            "estimated_carbon_saved_kg": self.estimated_carbon_saved_kg,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DPQMetrics":
        if not isinstance(data, Mapping):
            raise DPQModelError(
                f"DPQMetrics.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        return cls(
            region=str(data["region"]),
            timestamp=_coerce_ts(
                data["timestamp"], field_name="timestamp"),
            # P0 2.5: case-insensitive key coercion.
            precision_counts={
                _coerce_enum(
                    ModelPrecision, k,
                    field_name="precision_counts key"): int(v)
                for k, v in (data.get("precision_counts") or {}).items()
            },
            energy_fp32_baseline_kwh=float(
                data.get("energy_fp32_baseline_kwh", 0.0)),
            energy_current_kwh=float(
                data.get("energy_current_kwh", 0.0)),
            energy_savings_percent=float(
                data.get("energy_savings_percent", 0.0)),
            accuracy_fp32_baseline=float(
                data.get("accuracy_fp32_baseline", 0.0)),
            accuracy_current=float(
                data.get("accuracy_current", 0.0)),
            accuracy_delta=float(
                data.get("accuracy_delta", 0.0)),
            avg_conversion_time_ms=float(
                data.get("avg_conversion_time_ms", 0.0)),
            p99_conversion_time_ms=float(
                data.get("p99_conversion_time_ms", 0.0)),
            transition_count_24h=int(
                data.get("transition_count_24h", 0)),
            carbon_intensity_current=float(
                data.get("carbon_intensity_current", 0.0)),
            carbon_zone_current=_coerce_enum(
                CarbonZone, data["carbon_zone_current"],
                field_name="carbon_zone_current"),
            estimated_carbon_saved_kg=float(
                data.get("estimated_carbon_saved_kg", 0.0)),
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def new_transition_id() -> str:
    """Return a collision-resistant transition identifier."""
    return f"tr-{uuid.uuid4().hex[:12]}"


__all__ = [
    "CarbonZone",
    "ConversionResult",
    "DPQConfig",
    "DPQMetrics",
    "DPQModelError",
    "ModelPrecision",
    "ModelVariant",
    "PrecisionTransition",
    "TransitionStatus",
    "TransitionTrigger",
    "new_transition_id",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m dpq.models
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    from datetime import timedelta

    # ---------------------------------------------------------------- #
    # P0 2.1 — ConversionResult(conversion_time_ms=nan) raises
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: ConversionResult NaN guard ===")

    def _base_cr_kwargs() -> Dict[str, Any]:
        return dict(
            success=True,
            from_precision="fp32",
            to_precision="int8",
            model_name="bert-base",
            conversion_time_ms=180.0,
            size_reduction_percent=72.0,
            accuracy_delta=-0.03,
        )

    # NaN
    try:
        ConversionResult(
            **{**_base_cr_kwargs(),
               "conversion_time_ms": float("nan")})
    except DPQModelError as exc:
        print(f"  ✓ NaN conversion_time_ms rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.1 regression: NaN conversion_time_ms accepted")

    # Inf
    try:
        ConversionResult(
            **{**_base_cr_kwargs(),
               "conversion_time_ms": float("inf")})
    except DPQModelError:
        print("  ✓ Inf conversion_time_ms rejected")
    else:
        raise AssertionError(
            "P0 2.1 regression: Inf conversion_time_ms accepted")

    # Negative (pre-existing check still works)
    try:
        ConversionResult(
            **{**_base_cr_kwargs(), "conversion_time_ms": -1.0})
    except DPQModelError:
        print("  ✓ negative conversion_time_ms rejected")
    else:
        raise AssertionError(
            "P0 2.1 regression: negative conversion_time_ms accepted")

    # Valid still works.
    cr = ConversionResult(**_base_cr_kwargs())
    assert cr.conversion_time_ms == 180.0
    print("  ✓ valid ConversionResult accepted")

    # ---------------------------------------------------------------- #
    # P0 2.2 — ModelVariant(precision="fp16") raises at construction
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: ModelVariant precision type check ===")

    def _base_variant_kwargs() -> Dict[str, Any]:
        return dict(
            model_name="bert-base",
            precision=ModelPrecision.INT8,
            size_mb=110.0,
            accuracy=0.91,
            conversion_time_ms=180.0,
            backend="tensorrt",
            created_at=datetime.now(timezone.utc),
        )

    try:
        ModelVariant(
            **{**_base_variant_kwargs(), "precision": "fp16"})
    except DPQModelError as exc:
        print(f"  ✓ string 'fp16' rejected at __post_init__: {exc}")
    else:
        raise AssertionError(
            "P0 2.2 regression: string precision accepted")

    # Valid enum still works.
    v = ModelVariant(**_base_variant_kwargs())
    assert v.precision == ModelPrecision.INT8
    print("  ✓ valid ModelVariant (enum precision) accepted")

    # ---------------------------------------------------------------- #
    # P0 2.3 — DPQConfig.min_accuracy("garbage") raises DPQModelError
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: min_accuracy raises DPQModelError ===")

    cfg = DPQConfig()
    try:
        cfg.min_accuracy("garbage")
    except DPQModelError as exc:
        print(f"  ✓ 'garbage' → DPQModelError: {exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: 'garbage' did not raise")

    try:
        cfg.min_accuracy("fp16")  # string that IS a valid value
    except DPQModelError as exc:
        # String "fp16" is not ModelPrecision.FP16 (different hash).
        print(f"  ✓ bare string 'fp16' → DPQModelError (not KeyError)")
    else:
        raise AssertionError(
            "P0 2.3 regression: bare string was accepted")

    # Valid enum still works.
    assert cfg.min_accuracy(ModelPrecision.INT8) == 0.90
    print("  ✓ valid ModelPrecision.INT8 → 0.90")

    # ---------------------------------------------------------------- #
    # P0 2.4 — DPQConfig.from_legacy_dict
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: DPQConfig.from_legacy_dict ===")

    partial = DPQConfig.from_legacy_dict({"green_threshold": 10.0})
    assert partial.green_threshold == 10.0, (
        f"P0 2.4 regression: green_threshold="
        f"{partial.green_threshold}, expected 10.0")
    assert partial.yellow_threshold == 200.0, (
        f"P0 2.4 regression: yellow_threshold="
        f"{partial.yellow_threshold}, expected 200.0 (default)")
    assert partial.red_threshold == 400.0
    assert partial.min_accuracy_fp32 == 0.95
    assert partial.min_accuracy_int4 == 0.85
    print(f"  ✓ green_threshold=10.0 (explicit), "
          f"yellow={partial.yellow_threshold} (default), "
          f"min_accuracy_int4={partial.min_accuracy_int4} (default)")

    # Unknown keys are ignored.
    partial2 = DPQConfig.from_legacy_dict({
        "green_threshold": 10.0,
        "new_field_we_dont_know_about": 42,
    })
    assert partial2.green_threshold == 10.0
    print("  ✓ unknown keys ignored")

    # None / empty dict works.
    assert DPQConfig.from_legacy_dict(None) == DPQConfig()
    assert DPQConfig.from_legacy_dict({}) == DPQConfig()
    print("  ✓ None / {} → defaults")

    # ---------------------------------------------------------------- #
    # P0 2.5 — Case-insensitive from_dict for all three dataclasses
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: from_dict case-insensitive enums ===")

    # ModelVariant
    v_payload = {
        "model_name": "bert",
        "precision": "FP16",                # uppercase
        "size_mb": 220.0,
        "accuracy": 0.93,
        "conversion_time_ms": 180.0,
        "backend": "tensorrt",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    v_restored = ModelVariant.from_dict(v_payload)
    assert v_restored.precision == ModelPrecision.FP16, (
        f"P0 2.5 regression: ModelVariant precision="
        f"{v_restored.precision}")
    print(f"  ✓ ModelVariant: 'FP16' → {v_restored.precision}")

    # Whitespace
    v_payload_ws = dict(v_payload)
    v_payload_ws["precision"] = "  fp16  "
    assert ModelVariant.from_dict(v_payload_ws).precision == \
        ModelPrecision.FP16
    print("  ✓ ModelVariant: '  fp16  ' → FP16")

    # Invalid value raises DPQModelError (not ValueError)
    v_bad = dict(v_payload)
    v_bad["precision"] = "garbage"
    try:
        ModelVariant.from_dict(v_bad)
    except DPQModelError as exc:
        print(f"  ✓ ModelVariant: 'garbage' → DPQModelError")
    except ValueError as exc:
        raise AssertionError(
            f"P0 2.5 regression: 'garbage' raised plain ValueError: "
            f"{exc}")
    else:
        raise AssertionError(
            "P0 2.5 regression: 'garbage' was accepted")

    # PrecisionTransition
    t_payload = {
        "transition_id": "tr-test001",
        "region": "eu-west",
        "model_name": "bert",
        "to_precision": "INT8",             # uppercase
        "carbon_zone": "RED",               # uppercase
        "carbon_intensity": 250.0,
        "start_time": datetime.now(timezone.utc).isoformat(),
        "status": "COMPLETED",              # uppercase
    }
    t_restored = PrecisionTransition.from_dict(t_payload)
    assert t_restored.to_precision == ModelPrecision.INT8
    assert t_restored.carbon_zone == CarbonZone.RED
    assert t_restored.status == "completed", (
        f"P0 2.5 regression: status={t_restored.status!r}, "
        f"expected 'completed'")
    print(f"  ✓ PrecisionTransition: 'INT8' → {t_restored.to_precision}, "
          f"'RED' → {t_restored.carbon_zone}, "
          f"'COMPLETED' → {t_restored.status!r}")

    # Invalid carbon_zone
    t_bad = dict(t_payload)
    t_bad["carbon_zone"] = "garbage"
    try:
        PrecisionTransition.from_dict(t_bad)
    except DPQModelError:
        print(f"  ✓ PrecisionTransition: 'garbage' → DPQModelError")
    else:
        raise AssertionError(
            "P0 2.5 regression: 'garbage' carbon_zone was accepted")

    # DPQMetrics
    m_payload = {
        "region": "eu-west",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "precision_counts": {"FP16": 5, "INT8": 2},   # uppercase keys
        "energy_fp32_baseline_kwh": 100.0,
        "energy_current_kwh": 50.0,
        "energy_savings_percent": 50.0,
        "accuracy_fp32_baseline": 0.95,
        "accuracy_current": 0.93,
        "accuracy_delta": -0.02,
        "avg_conversion_time_ms": 250.0,
        "p99_conversion_time_ms": 500.0,
        "transition_count_24h": 5,
        "carbon_intensity_current": 180.0,
        "carbon_zone_current": "YELLOW",              # uppercase
        "estimated_carbon_saved_kg": 1.5,
    }
    m_restored = DPQMetrics.from_dict(m_payload)
    assert m_restored.precision_counts == {
        ModelPrecision.FP16: 5, ModelPrecision.INT8: 2}, (
        f"P0 2.5 regression: precision_counts="
        f"{m_restored.precision_counts}")
    assert m_restored.carbon_zone_current == CarbonZone.YELLOW
    print(f"  ✓ DPQMetrics: 'FP16'/'INT8' keys → ModelPrecision, "
          f"'YELLOW' → {m_restored.carbon_zone_current}")

    # Invalid precision_counts key
    m_bad = dict(m_payload)
    m_bad["precision_counts"] = {"garbage": 5}
    try:
        DPQMetrics.from_dict(m_bad)
    except DPQModelError:
        print("  ✓ DPQMetrics: 'garbage' key → DPQModelError")
    else:
        raise AssertionError(
            "P0 2.5 regression: 'garbage' precision_counts key "
            "was accepted")

    # ---------------------------------------------------------------- #
    # P1 6.1 — min_accuracy ordering validation
    # ---------------------------------------------------------------- #
    print("\n=== P1 6.1: min_accuracy ordering ===")

    try:
        DPQConfig(min_accuracy_fp32=0.5, min_accuracy_int4=0.99)
    except DPQModelError as exc:
        print(f"  ✓ bad ordering rejected: {exc}")
    else:
        raise AssertionError(
            "P1 6.1 regression: bad min_accuracy ordering accepted")

    # Default config (fp32=0.95 ≥ fp16=0.93 ≥ int8=0.90 ≥ int4=0.85)
    valid_cfg = DPQConfig()
    assert valid_cfg.min_accuracy_fp32 >= valid_cfg.min_accuracy_int4
    print("  ✓ default config passes ordering check")

    # Even stricter FP32 accepted (as long as ordering holds).
    DPQConfig(
        min_accuracy_fp32=0.99, min_accuracy_fp16=0.95,
        min_accuracy_int8=0.90, min_accuracy_int4=0.85)
    print("  ✓ tighter FP32 (0.99) with monotone chain accepted")

    # ---------------------------------------------------------------- #
    # P1 6.2 — DPQMetrics(accuracy_current=2.0) raises
    # ---------------------------------------------------------------- #
    print("\n=== P1 6.2: DPQMetrics validation ===")

    def _valid_metrics_kwargs() -> Dict[str, Any]:
        return dict(
            region="eu-west",
            timestamp=datetime.now(timezone.utc),
            precision_counts={ModelPrecision.FP16: 3,
                              ModelPrecision.INT8: 2},
            energy_fp32_baseline_kwh=100.0,
            energy_current_kwh=50.0,
            energy_savings_percent=50.0,
            accuracy_fp32_baseline=0.95,
            accuracy_current=0.93,
            accuracy_delta=-0.02,
            avg_conversion_time_ms=250.0,
            p99_conversion_time_ms=500.0,
            transition_count_24h=5,
            carbon_intensity_current=180.0,
            carbon_zone_current=CarbonZone.YELLOW,
            estimated_carbon_saved_kg=1.5,
        )

    # Valid metrics accepted.
    m = DPQMetrics(**_valid_metrics_kwargs())
    print("  ✓ valid DPQMetrics accepted")

    # accuracy_current out of [0, 1] → error.
    try:
        DPQMetrics(
            **{**_valid_metrics_kwargs(),
               "accuracy_current": 2.0})
    except DPQModelError as exc:
        print(f"  ✓ accuracy_current=2.0 rejected: {exc}")
    else:
        raise AssertionError(
            "P1 6.2 regression: accuracy_current=2.0 accepted")

    # accuracy_fp32_baseline negative → error.
    try:
        DPQMetrics(
            **{**_valid_metrics_kwargs(),
               "accuracy_fp32_baseline": -0.1})
    except DPQModelError:
        print("  ✓ negative accuracy_fp32_baseline rejected")
    else:
        raise AssertionError(
            "P1 6.2 regression: negative accuracy rejected")

    # precision_counts with string keys → coerced (not error).
    m_coerced = DPQMetrics(
        **{**_valid_metrics_kwargs(),
           "precision_counts": {"fp16": 3, "int8": 2}})
    assert ModelPrecision.FP16 in m_coerced.precision_counts
    print("  ✓ string precision_counts keys coerced to enum")

    # NaN in avg_conversion_time_ms → error.
    try:
        DPQMetrics(
            **{**_valid_metrics_kwargs(),
               "avg_conversion_time_ms": float("nan")})
    except DPQModelError:
        print("  ✓ NaN avg_conversion_time_ms rejected")
    else:
        raise AssertionError(
            "P1 6.2 regression: NaN avg accepted")

    # ---------------------------------------------------------------- #
    # Bonus: enum __str__, record_usage, duration_ms clamping
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: polish ===")

    # P2 4.1: __str__ returns .value
    assert str(ModelPrecision.FP16) == "fp16", (
        f"P2 4.1 regression: str(ModelPrecision.FP16)="
        f"{str(ModelPrecision.FP16)!r}")
    assert str(CarbonZone.RED) == "red"
    assert f"{ModelPrecision.INT8}" == "int8"
    print("  ✓ enum __str__ returns .value")

    # P2 4.5: record_usage
    variant = ModelVariant(**_base_variant_kwargs())
    original_count = variant.usage_count
    variant.record_usage()
    assert variant.usage_count == original_count + 1
    assert variant.last_used is not None
    print(f"  ✓ record_usage: usage_count={variant.usage_count}, "
          f"last_used set")

    # P1 3.9: duration_ms clamps negative deltas
    now = datetime.now(timezone.utc)
    t_neg = PrecisionTransition(
        transition_id="tr-neg",
        region="eu-west",
        model_name="bert",
        from_precision=ModelPrecision.FP16,
        to_precision=ModelPrecision.INT8,
        triggered_by="manual",
        carbon_intensity=250.0,
        carbon_zone=CarbonZone.RED,
        start_time=now,
        end_time=now - timedelta(seconds=10),    # before start_time
        status="failed",
    )
    assert t_neg.duration_ms() == 0.0, (
        f"P1 3.9 regression: negative duration returned "
        f"{t_neg.duration_ms()}")
    print("  ✓ duration_ms clamps negative delta to 0.0")

    # TransitionStatus / TransitionTrigger enums
    assert TransitionStatus.COMPLETED.value == "completed"
    assert TransitionStatus.COMPLETED == "completed"  # str-enum eq
    assert TransitionTrigger.CARBON_ZONE_CHANGE.value == \
        "carbon_zone_change"
    print("  ✓ TransitionStatus / TransitionTrigger enums compare "
          "equal to their string values")

    # ---------------------------------------------------------------- #
    # Serialization round-trip (backward compat)
    # ---------------------------------------------------------------- #
    print("\n=== Serialization round-trip ===")

    # ModelVariant
    v_payload = v.to_dict()
    v_rt = ModelVariant.from_dict(v_payload)
    assert v_rt.to_dict() == v_payload, "ModelVariant round-trip failed"
    print("  ✓ ModelVariant round-trip")

    # ConversionResult
    cr_payload = cr.to_dict()
    cr_rt = ConversionResult.from_dict(cr_payload)
    assert cr_rt.to_dict() == cr_payload
    print("  ✓ ConversionResult round-trip")

    # PrecisionTransition
    t = PrecisionTransition(
        transition_id=new_transition_id(),
        region="eu-west",
        model_name="bert",
        from_precision=ModelPrecision.FP16,
        to_precision=ModelPrecision.INT8,
        triggered_by="carbon_zone_change",
        carbon_intensity=250.0,
        carbon_zone=CarbonZone.RED,
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        status="completed",
        conversion_results=[cr],
    )
    t_rt = PrecisionTransition.from_dict(t.to_dict())
    assert t_rt.to_dict() == t.to_dict()
    print("  ✓ PrecisionTransition round-trip (with nested result)")

    # DPQMetrics
    m_rt = DPQMetrics.from_dict(m.to_dict())
    assert m_rt.to_dict() == m.to_dict()
    print("  ✓ DPQMetrics round-trip")

    # Full JSON round-trip
    payload = json.dumps(v.to_dict(), default=str)
    v_json = ModelVariant.from_dict(json.loads(payload))
    assert v_json.to_dict() == v.to_dict()
    print("  ✓ JSON round-trip")

    # ---------------------------------------------------------------- #
    # Legacy v1.0.0 payload compat
    # ---------------------------------------------------------------- #
    print("\n=== Legacy v1.0.0 payload compat ===")

    legacy_v_payload = {
        "model_name": "bert",
        "precision": "INT8",
        "size_mb": 110.0,
        "accuracy": 0.91,
        "conversion_time_ms": 180.0,
        "backend": "tensorrt",
        "created_at": "2024-01-15T12:00:00+00:00",
    }
    lv = ModelVariant.from_dict(legacy_v_payload)
    assert lv.precision == ModelPrecision.INT8
    print("  ✓ legacy uppercase payload from v1.0.0 loads cleanly")

    print("\n✓ All self-tests passed.")
