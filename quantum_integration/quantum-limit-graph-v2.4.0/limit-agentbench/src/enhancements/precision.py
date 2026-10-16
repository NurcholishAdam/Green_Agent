"""
precision.py — Adaptive precision switching (Enhancement 7).

Canonical PrecisionLevel enum and cost table. Both consumers import these.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, Sequence, Tuple


class PrecisionLevel(str, Enum):
    FP32 = "fp32"
    BF16 = "bf16"
    FP16 = "fp16"
    FP8 = "fp8"
    INT8 = "int8"
    INT4 = "int4"


PRECISION_ORDER: Tuple[PrecisionLevel, ...] = (
    PrecisionLevel.FP32,
    PrecisionLevel.BF16,
    PrecisionLevel.FP16,
    PrecisionLevel.FP8,
    PrecisionLevel.INT8,
    PrecisionLevel.INT4,
)


PRECISION_COST: Dict[PrecisionLevel, Dict[str, float]] = {
    PrecisionLevel.FP32: {"speed": 1.0, "energy": 1.00, "quality": 1.000, "memory": 1.00, "bits": 32.0},
    PrecisionLevel.BF16: {"speed": 1.7, "energy": 0.72, "quality": 0.997, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP16: {"speed": 2.0, "energy": 0.65, "quality": 0.994, "memory": 0.50, "bits": 16.0},
    PrecisionLevel.FP8:  {"speed": 3.1, "energy": 0.50, "quality": 0.985, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT8: {"speed": 3.6, "energy": 0.44, "quality": 0.972, "memory": 0.25, "bits": 8.0},
    PrecisionLevel.INT4: {"speed": 5.0, "energy": 0.32, "quality": 0.905, "memory": 0.125, "bits": 4.0},
}


class PrecisionController:
    """
    Chooses a precision level as a monotonic function of carbon stress,
    price stress, and latency headroom, bounded by a quality floor.
    """

    def __init__(
        self,
        supported: Optional[Sequence[PrecisionLevel]] = None,
        quality_floor: float = 0.95,
        carbon_aware: bool = True,
    ):
        self.supported = list(supported) if supported else list(PrecisionLevel)
        self.quality_floor = quality_floor
        self.carbon_aware = carbon_aware

    def _feasible_levels(self) -> list[PrecisionLevel]:
        levels = [p for p in PrecisionLevel if p in self.supported]
        levels.sort(key=lambda p: PRECISION_COST[p]["energy"], reverse=True)
        return [p for p in levels if PRECISION_COST[p]["quality"] >= self.quality_floor]

    def select(
        self,
        carbon_intensity: float,
        latency_headroom_ratio: float,
        carbon_price: float = 0.0,
    ) -> PrecisionLevel:
        feasible = self._feasible_levels()
        if not feasible:
            return PrecisionLevel.FP32
        stress = min(1.0, max(0.0, carbon_intensity / 600.0)) if self.carbon_aware else 0.0
        price_stress = min(1.0, max(0.0, carbon_price / 0.2))
        headroom = min(1.0, max(0.0, latency_headroom_ratio))
        agg = 0.5 * stress + 0.3 * price_stress + 0.2 * (1.0 - headroom)
        idx = int(round(agg * (len(feasible) - 1)))
        idx = max(0, min(len(feasible) - 1, idx))
        return feasible[idx]


class HardwareAwareAdapter:
    """Applies a precision level to a policy dict."""

    def __init__(self, controller: PrecisionController):
        self.controller = controller

    def adapt(
        self,
        policy: Dict[str, Any],
        carbon_intensity: float,
        latency_headroom: float,
        carbon_price: float,
    ) -> Tuple[Dict[str, Any], PrecisionLevel]:
        level = self.controller.select(carbon_intensity, latency_headroom, carbon_price)
        out = dict(policy)
        out["precision_level"] = level.value
        out["effective_bits"] = PRECISION_COST[level]["bits"]
        return out, level
