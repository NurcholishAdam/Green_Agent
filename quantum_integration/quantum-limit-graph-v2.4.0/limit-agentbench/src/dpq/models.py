# src/dpq/models.py

"""
DPQ Shared Models
=================

Single source of truth for enums and dataclasses shared across the DPQ
subsystem: :class:`ModelPrecision`, :class:`CarbonZone`, :class:`ModelVariant`,
:class:`ConversionResult`, :class:`PrecisionTransition`, and :class:`DPQMetrics`.

Enhancements
------------
- Single canonical definition of every shared type (no more duplicates across
  ``carbon_intensity_monitor`` / ``precision_controller``).
- :class:`DPQConfig` — frozen config with validation.
- Full serialization on every dataclass (``to_dict`` / ``from_dict``).
- UTC timestamps; strict validation on accuracy / size / latency.
- Custom :class:`DPQModelError`.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DPQModelError(ValueError):
    """Raised for invalid DPQ model values."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ModelPrecision(str, Enum):
    """Supported model precision levels."""

    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"


class CarbonZone(str, Enum):
    """Carbon intensity zones."""

    GREEN = "green"        # < 50 gCO2/kWh
    YELLOW = "yellow"      # 50-200 gCO2/kWh
    RED = "red"            # 200-400 gCO2/kWh
    CRITICAL = "critical"  # > 400 gCO2/kWh


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
        if not (
            0 < self.green_threshold < self.yellow_threshold < self.red_threshold
        ):
            raise DPQModelError(
                "Carbon thresholds must be strictly increasing and positive."
            )
        for name in (
            "min_accuracy_fp32",
            "min_accuracy_fp16",
            "min_accuracy_int8",
            "min_accuracy_int4",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise DPQModelError(f"{name} must be in [0, 1].")

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
        """Return the minimum acceptable accuracy for ``precision``."""
        return {
            ModelPrecision.FP32: self.min_accuracy_fp32,
            ModelPrecision.FP16: self.min_accuracy_fp16,
            ModelPrecision.INT8: self.min_accuracy_int8,
            ModelPrecision.INT4: self.min_accuracy_int4,
        }[precision]


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
            raise DPQModelError("model_name must be a non-empty string.")
        if self.conversion_time_ms < 0:
            raise DPQModelError("conversion_time_ms must be >= 0.")
        if math.isnan(self.size_reduction_percent) or math.isinf(
            self.size_reduction_percent
        ):
            raise DPQModelError("size_reduction_percent must be finite.")
        if math.isnan(self.accuracy_delta) or math.isinf(self.accuracy_delta):
            raise DPQModelError("accuracy_delta must be finite.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ConversionResult":
        if not isinstance(data, Mapping):
            raise DPQModelError(
                f"ConversionResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            success=bool(data["success"]),
            from_precision=str(data.get("from_precision", "none")),
            to_precision=str(data.get("to_precision", "none")),
            model_name=str(data["model_name"]),
            conversion_time_ms=float(data.get("conversion_time_ms", 0.0)),
            size_reduction_percent=float(data.get("size_reduction_percent", 0.0)),
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
        if self.size_mb < 0:
            raise DPQModelError("size_mb must be >= 0.")
        if not 0.0 <= self.accuracy <= 1.0:
            raise DPQModelError("accuracy must be in [0, 1].")
        if self.usage_count < 0:
            raise DPQModelError("usage_count must be >= 0.")
        # Normalize naive timestamps to UTC.
        if not self.created_at.tzinfo:
            self.created_at = self.created_at.replace(tzinfo=timezone.utc)
        if self.last_used is not None and not self.last_used.tzinfo:
            self.last_used = self.last_used.replace(tzinfo=timezone.utc)

    def is_available(self, config: Optional[DPQConfig] = None) -> bool:
        """Return True when the variant meets its precision's accuracy floor."""
        cfg = config or DPQConfig()
        return self.accuracy >= cfg.min_accuracy(self.precision)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "precision": self.precision.value,
            "size_mb": self.size_mb,
            "accuracy": self.accuracy,
            "conversion_time_ms": self.conversion_time_ms,
            "backend": self.backend,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "usage_count": self.usage_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ModelVariant":
        def _ts(value: Any) -> Optional[datetime]:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value
            dt = datetime.fromisoformat(str(value))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        return cls(
            model_name=str(data["model_name"]),
            precision=ModelPrecision(str(data["precision"])),
            size_mb=float(data.get("size_mb", 0.0)),
            accuracy=float(data.get("accuracy", 0.0)),
            conversion_time_ms=float(data.get("conversion_time_ms", 0.0)),
            backend=str(data.get("backend", "unknown")),
            created_at=_ts(data.get("created_at")) or datetime.now(timezone.utc),
            last_used=_ts(data.get("last_used")),
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
    triggered_by: str  # "carbon_zone_change" | "manual" | "accuracy_guardian"
    carbon_intensity: float
    carbon_zone: CarbonZone
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending | in_progress | completed | failed
    conversion_results: List[ConversionResult] = field(default_factory=list)
    error_message: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.transition_id, str) or not self.transition_id:
            raise DPQModelError("transition_id must be a non-empty string.")
        if self.status not in (
            "pending", "in_progress", "completed", "failed"
        ):
            raise DPQModelError(
                f"status must be one of pending/in_progress/completed/failed, "
                f"got {self.status!r}."
            )
        if not self.start_time.tzinfo:
            self.start_time = self.start_time.replace(tzinfo=timezone.utc)
        if self.end_time is not None and not self.end_time.tzinfo:
            self.end_time = self.end_time.replace(tzinfo=timezone.utc)

    def duration_ms(self) -> Optional[float]:
        """Return the transition duration in milliseconds, or None."""
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time).total_seconds() * 1000.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "region": self.region,
            "model_name": self.model_name,
            "from_precision": (
                self.from_precision.value if self.from_precision else None
            ),
            "to_precision": self.to_precision.value,
            "triggered_by": self.triggered_by,
            "carbon_intensity": self.carbon_intensity,
            "carbon_zone": self.carbon_zone.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "conversion_results": [r.to_dict() for r in self.conversion_results],
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PrecisionTransition":
        start = datetime.fromisoformat(str(data["start_time"]))
        if not start.tzinfo:
            start = start.replace(tzinfo=timezone.utc)
        end_raw = data.get("end_time")
        end = datetime.fromisoformat(str(end_raw)) if end_raw else None
        if end is not None and not end.tzinfo:
            end = end.replace(tzinfo=timezone.utc)
        from_raw = data.get("from_precision")
        return cls(
            transition_id=str(data["transition_id"]),
            region=str(data["region"]),
            model_name=str(data["model_name"]),
            from_precision=ModelPrecision(str(from_raw)) if from_raw else None,
            to_precision=ModelPrecision(str(data["to_precision"])),
            triggered_by=str(data.get("triggered_by", "manual")),
            carbon_intensity=float(data.get("carbon_intensity", 0.0)),
            carbon_zone=CarbonZone(str(data["carbon_zone"])),
            start_time=start,
            end_time=end,
            status=str(data.get("status", "pending")),
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
        ts = datetime.fromisoformat(str(data["timestamp"]))
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        return cls(
            region=str(data["region"]),
            timestamp=ts,
            precision_counts={
                ModelPrecision(k): int(v)
                for k, v in (data.get("precision_counts") or {}).items()
            },
            energy_fp32_baseline_kwh=float(
                data.get("energy_fp32_baseline_kwh", 0.0)
            ),
            energy_current_kwh=float(data.get("energy_current_kwh", 0.0)),
            energy_savings_percent=float(
                data.get("energy_savings_percent", 0.0)
            ),
            accuracy_fp32_baseline=float(
                data.get("accuracy_fp32_baseline", 0.0)
            ),
            accuracy_current=float(data.get("accuracy_current", 0.0)),
            accuracy_delta=float(data.get("accuracy_delta", 0.0)),
            avg_conversion_time_ms=float(
                data.get("avg_conversion_time_ms", 0.0)
            ),
            p99_conversion_time_ms=float(
                data.get("p99_conversion_time_ms", 0.0)
            ),
            transition_count_24h=int(data.get("transition_count_24h", 0)),
            carbon_intensity_current=float(
                data.get("carbon_intensity_current", 0.0)
            ),
            carbon_zone_current=CarbonZone(str(data["carbon_zone_current"])),
            estimated_carbon_saved_kg=float(
                data.get("estimated_carbon_saved_kg", 0.0)
            ),
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
    "new_transition_id",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    cfg = DPQConfig()
    print("Zone(20)   :", cfg.classify(20.0))
    print("Zone(120)  :", cfg.classify(120.0))
    print("Zone(300)  :", cfg.classify(300.0))
    print("Zone(500)  :", cfg.classify(500.0))

    v = ModelVariant(
        model_name="bert-base",
        precision=ModelPrecision.INT8,
        size_mb=110.0,
        accuracy=0.91,
        conversion_time_ms=180.0,
        backend="tensorrt",
        created_at=datetime.now(timezone.utc),
    )
    print("Variant OK :", v.is_available(cfg))

    t = PrecisionTransition(
        transition_id=new_transition_id(),
        region="eu-west-1",
        model_name="bert-base",
        from_precision=ModelPrecision.FP16,
        to_precision=ModelPrecision.INT8,
        triggered_by="carbon_zone_change",
        carbon_intensity=250.0,
        carbon_zone=CarbonZone.RED,
        start_time=datetime.now(timezone.utc),
        status="completed",
        end_time=datetime.now(timezone.utc),
    )
    print("Transition :", t.duration_ms())

    payload = json.dumps(v.to_dict(), default=str)
    restored = ModelVariant.from_dict(json.loads(payload))
    assert restored.to_dict() == v.to_dict()
    print("Round-trip OK.")
