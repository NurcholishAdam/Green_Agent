# src/continuum/types.py
"""
continuum.types — shared continuum primitives (v6.0.0)
=======================================================

Single source of truth for the enums and data classes used by
``continuum_scheduler`` and ``offloading_decision_engine``.

**Why this module exists**
--------------------------
v5 duplicated ``PlacementTier``, ``PlacementDecision``, ``DeviceType``, and
``TDPReading`` across the two modules. Even though their string values
matched, they were **distinct enum classes** — so an equality check like
``decision.selected_tier == PlacementTier.TIER_1_LOCAL`` silently failed
when the two ``PlacementTier`` classes came from different modules.

v6 puts them here. Both modules import from ``continuum.types``. The
classes are identical objects, so identity, equality, and hashing all
agree.

**Cross-module import**

    from .types import PlacementTier, PlacementDecision  # from continuum

**What this module guarantees**

* ``PlacementDecision.from_dict`` normalizes naive timestamps to UTC —
  the v5 "TypeError on cache hit after restore" bug.
* ``PlacementDecision`` gains ``margin``, ``runner_up_tier``, and
  ``target_is_fallback`` for audit trail.
* ``TDPReading`` gains ``is_synthetic`` so callers can distinguish a
  fabricated reading from a real one.
* ``_json_safe`` + ``_coerce_timestamp`` are shared helpers.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple


# --------------------------------------------------------------------------- #
# Base error
# --------------------------------------------------------------------------- #
class ContinuumError(ValueError):
    """Base error for the continuum package."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _json_safe(obj: Any) -> Any:
    """Recursively make ``obj`` JSON-serialisable."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    return obj


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default


def _coerce_timestamp(raw: Any) -> datetime:
    """Normalize to timezone-aware UTC."""
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, str):
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ContinuumError(
                f"invalid timestamp: {raw!r}"
            ) from exc
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise ContinuumError(
        f"timestamp must be str or datetime, got {type(raw).__name__}"
    )


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class PlacementTier(Enum):
    """Continuum placement tiers."""

    TIER_1_LOCAL = "tier_1_local"
    TIER_2_REGIONAL = "tier_2_regional"
    TIER_3_CLOUD = "tier_3_cloud"


class DeviceType(Enum):
    """Device classifications used for TDP metadata."""

    UNKNOWN = "unknown"
    RASPBERRY_PI = "raspberry_pi"
    JETSON_NANO = "jetson_nano"
    JETSON_XAVIER = "jetson_xavier"
    INTEL_NUC = "intel_nuc"
    MOBILE = "mobile"
    SERVER = "server"


# --------------------------------------------------------------------------- #
# Device / TDP reading
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TDPReading:
    """Snapshot of a device's thermal / power state."""

    timestamp: datetime
    device_id: str
    device_type: DeviceType
    current_power_watts: float
    tdp_threshold_watts: float
    utilization_percent: float = 0.0
    temperature_celsius: float = 0.0
    thermal_throttling: bool = False
    predicted_breach_seconds: Optional[float] = None
    is_synthetic: bool = False   # v6: signal a fabricated reading

    def __post_init__(self) -> None:
        if self.current_power_watts < 0:
            raise ContinuumError(
                "current_power_watts must be >= 0"
            )
        if self.tdp_threshold_watts <= 0:
            raise ContinuumError(
                "tdp_threshold_watts must be > 0"
            )
        if not 0.0 <= self.utilization_percent <= 100.0:
            raise ContinuumError(
                "utilization_percent must be in [0, 100]"
            )
        object.__setattr__(
            self, "timestamp", _coerce_timestamp(self.timestamp)
        )

    @property
    def headroom_watts(self) -> float:
        return self.tdp_threshold_watts - self.current_power_watts

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(
            {
                **asdict(self),
                "timestamp": self.timestamp.isoformat(),
                "device_type": self.device_type.value,
            }
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TDPReading":
        if not isinstance(data, Mapping):
            raise ContinuumError(
                f"TDPReading.from_dict expects Mapping, "
                f"got {type(data).__name__}"
            )
        dt_raw = data.get("device_type", "unknown")
        try:
            dt = (
                dt_raw
                if isinstance(dt_raw, DeviceType)
                else DeviceType(str(dt_raw))
            )
        except ValueError:
            dt = DeviceType.UNKNOWN
        pbs = data.get("predicted_breach_seconds")
        return cls(
            timestamp=_coerce_timestamp(data["timestamp"]),
            device_id=str(data["device_id"]),
            device_type=dt,
            current_power_watts=float(data["current_power_watts"]),
            tdp_threshold_watts=float(data["tdp_threshold_watts"]),
            utilization_percent=_coerce_float(
                data.get("utilization_percent"), 0.0
            ),
            temperature_celsius=_coerce_float(
                data.get("temperature_celsius"), 0.0
            ),
            thermal_throttling=bool(
                data.get("thermal_throttling", False)
            ),
            predicted_breach_seconds=(
                _coerce_float(pbs, 0.0) if pbs is not None else None
            ),
            is_synthetic=bool(data.get("is_synthetic", False)),
        )


# --------------------------------------------------------------------------- #
# Placement decision (immutable, serializable, with audit fields)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PlacementDecision:
    """Immutable result of an offloading decision."""

    task_id: str
    device_id: str
    selected_tier: PlacementTier
    target_node: str
    confidence_score: float
    reasoning: Tuple[str, ...]
    estimated_latency_ms: float
    estimated_carbon_gco2: float
    estimated_cost_usd: float
    timestamp: datetime
    # v6 additions:
    margin: float = 0.0
    runner_up_tier: Optional[PlacementTier] = None
    target_is_fallback: bool = False

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence_score <= 1.0:
            raise ContinuumError(
                f"confidence_score must be in [0, 1], "
                f"got {self.confidence_score}"
            )
        if not 0.0 <= self.margin <= 1.0:
            raise ContinuumError(
                f"margin must be in [0, 1], got {self.margin}"
            )
        object.__setattr__(
            self, "timestamp", _coerce_timestamp(self.timestamp)
        )
        if not isinstance(self.reasoning, tuple):
            object.__setattr__(
                self, "reasoning", tuple(self.reasoning)
            )

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(
            {
                "task_id": self.task_id,
                "device_id": self.device_id,
                "selected_tier": self.selected_tier.value,
                "target_node": self.target_node,
                "confidence_score": self.confidence_score,
                "reasoning": list(self.reasoning),
                "estimated_latency_ms": self.estimated_latency_ms,
                "estimated_carbon_gco2": self.estimated_carbon_gco2,
                "estimated_cost_usd": self.estimated_cost_usd,
                "timestamp": self.timestamp.isoformat(),
                "margin": self.margin,
                "runner_up_tier": (
                    self.runner_up_tier.value
                    if self.runner_up_tier else None
                ),
                "target_is_fallback": self.target_is_fallback,
            }
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PlacementDecision":
        if not isinstance(data, Mapping):
            raise ContinuumError(
                f"PlacementDecision.from_dict expects Mapping, "
                f"got {type(data).__name__}"
            )
        raw_tier = data["selected_tier"]
        tier = (
            raw_tier
            if isinstance(raw_tier, PlacementTier)
            else PlacementTier(str(raw_tier))
        )
        raw_runner = data.get("runner_up_tier")
        runner_up = None
        if raw_runner is not None:
            runner_up = (
                raw_runner
                if isinstance(raw_runner, PlacementTier)
                else PlacementTier(str(raw_runner))
            )
        return cls(
            task_id=str(data["task_id"]),
            device_id=str(data["device_id"]),
            selected_tier=tier,
            target_node=str(data["target_node"]),
            confidence_score=_coerce_float(
                data["confidence_score"]
            ),
            reasoning=tuple(data.get("reasoning", ()) or ()),
            estimated_latency_ms=_coerce_float(
                data.get("estimated_latency_ms"), 0.0
            ),
            estimated_carbon_gco2=_coerce_float(
                data.get("estimated_carbon_gco2"), 0.0
            ),
            estimated_cost_usd=_coerce_float(
                data.get("estimated_cost_usd"), 0.0
            ),
            timestamp=_coerce_timestamp(data["timestamp"]),
            margin=_coerce_float(data.get("margin"), 0.0),
            runner_up_tier=runner_up,
            target_is_fallback=bool(
                data.get("target_is_fallback", False)
            ),
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ContinuumError",
    "PlacementTier",
    "DeviceType",
    "TDPReading",
    "PlacementDecision",
    "_json_safe",
    "_coerce_float",
    "_coerce_timestamp",
]
