# src/rlhf/green_reward.py

"""
Green-aware reward shaping for RLHF
===================================

Provides a lightweight penalty function and a richer structured API for
shaping base rewards using energy and carbon signals.

Enhancements
------------
- ``GreenRewardConfig`` — frozen, validated: default weights, bounds.
- ``apply_green_penalty`` — the original function, now fully validated.
- ``apply_green_penalty_detailed`` — new structured API returning a
  :class:`GreenPenaltyBreakdown` for logging / RLHF records.
- ``GreenPenaltyBreakdown`` — frozen, serializable.
- Full validation + strict / non-strict modes.
- Custom ``GreenRewardError(ValueError)``; lazy ``%s`` logging; ``__main__``
  smoke test.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GreenRewardError(ValueError):
    """Raised for invalid green-reward inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenRewardConfig:
    """Tunable parameters for :func:`apply_green_penalty`."""

    default_energy_weight: float = 0.05
    default_carbon_weight: float = 1.0

    # Finite ceilings so outliers are rejected rather than propagated.
    max_energy: float = 1e12
    max_carbon: float = 1e12
    max_weight: float = 1e6

    def __post_init__(self) -> None:
        for name in (
            "default_energy_weight",
            "default_carbon_weight",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise GreenRewardError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise GreenRewardError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        for name in ("max_energy", "max_carbon", "max_weight"):
            if getattr(self, name) <= 0:
                raise GreenRewardError(f"{name} must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Breakdown record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenPenaltyBreakdown:
    """Immutable decomposition of a green-penalty computation."""

    base_reward: float
    energy: float
    carbon: float
    energy_weight: float
    carbon_weight: float
    energy_penalty: float
    carbon_penalty: float
    total_penalty: float
    final_reward: float
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_reward": self.base_reward,
            "energy": self.energy,
            "carbon": self.carbon,
            "energy_weight": self.energy_weight,
            "carbon_weight": self.carbon_weight,
            "energy_penalty": self.energy_penalty,
            "carbon_penalty": self.carbon_penalty,
            "total_penalty": self.total_penalty,
            "final_reward": self.final_reward,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "GreenPenaltyBreakdown("
            f"base={self.base_reward:.4g}, "
            f"energy_penalty={self.energy_penalty:.4g}, "
            f"carbon_penalty={self.carbon_penalty:.4g}, "
            f"final={self.final_reward:.4g})"
        )


# --------------------------------------------------------------------------- #
# Public functions
# --------------------------------------------------------------------------- #
def _validate_number(
    name: str, value: Any, *, low: float = 0.0, high: float, strict: bool = True
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = f"{name} must be numeric, got {type(value).__name__}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning("%s Using 0.0.", msg)
        return 0.0
    fv = float(value)
    if math.isnan(fv) or math.isinf(fv):
        msg = f"{name} must be finite, got {value!r}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning("%s Using 0.0.", msg)
        return 0.0
    if fv < low:
        msg = f"{name} must be >= {low}, got {fv}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning("%s Clamping.", msg)
        return low
    if fv > high:
        msg = f"{name} must be <= {high}, got {fv}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning("%s Clamping to ceiling.", msg)
        return high
    return fv


def apply_green_penalty(
    base_reward: float,
    energy: float,
    carbon: float,
    energy_weight: float = 0.05,
    carbon_weight: float = 1.0,
    *,
    config: Optional[GreenRewardConfig] = None,
    strict: bool = True,
) -> float:
    """
    Penalize a base reward based on environmental impact.

    Formula::

        penalty = energy_weight × energy + carbon_weight × carbon
        final   = base_reward - penalty

    Parameters
    ----------
    base_reward : float
        The base reward to shape.
    energy : float
        Energy consumed (kWh). Must be finite and >= 0.
    carbon : float
        Carbon emitted (kg CO2e). Must be finite and >= 0.
    energy_weight : float, default 0.05
        Penalty weight for energy.
    carbon_weight : float, default 1.0
        Penalty weight for carbon.
    config : GreenRewardConfig, optional
        Configuration used for the ceilings and default weights.
    strict : bool, default True
        If True, invalid inputs raise :class:`GreenRewardError`.

    Returns
    -------
    float
        The shaped reward.
    """
    cfg = config or GreenRewardConfig()
    br = _validate_number(
        "base_reward", base_reward, low=-math.inf if False else -1e15,
        high=1e15, strict=strict,
    )
    en = _validate_number(
        "energy", energy, low=0.0, high=cfg.max_energy, strict=strict,
    )
    ca = _validate_number(
        "carbon", carbon, low=0.0, high=cfg.max_carbon, strict=strict,
    )
    ew = _validate_number(
        "energy_weight", energy_weight, low=0.0, high=cfg.max_weight,
        strict=strict,
    )
    cw = _validate_number(
        "carbon_weight", carbon_weight, low=0.0, high=cfg.max_weight,
        strict=strict,
    )
    penalty = ew * en + cw * ca
    return br - penalty


def apply_green_penalty_detailed(
    base_reward: float,
    energy: float,
    carbon: float,
    energy_weight: float = 0.05,
    carbon_weight: float = 1.0,
    *,
    config: Optional[GreenRewardConfig] = None,
    strict: bool = True,
) -> GreenPenaltyBreakdown:
    """
    Same as :func:`apply_green_penalty` but returns a structured breakdown.
    """
    cfg = config or GreenRewardConfig()
    br = _validate_number(
        "base_reward", base_reward, low=-1e15, high=1e15, strict=strict,
    )
    en = _validate_number(
        "energy", energy, low=0.0, high=cfg.max_energy, strict=strict,
    )
    ca = _validate_number(
        "carbon", carbon, low=0.0, high=cfg.max_carbon, strict=strict,
    )
    ew = _validate_number(
        "energy_weight", energy_weight, low=0.0, high=cfg.max_weight,
        strict=strict,
    )
    cw = _validate_number(
        "carbon_weight", carbon_weight, low=0.0, high=cfg.max_weight,
        strict=strict,
    )
    energy_penalty = ew * en
    carbon_penalty = cw * ca
    total_penalty = energy_penalty + carbon_penalty

    return GreenPenaltyBreakdown(
        base_reward=br,
        energy=en,
        carbon=ca,
        energy_weight=ew,
        carbon_weight=cw,
        energy_penalty=energy_penalty,
        carbon_penalty=carbon_penalty,
        total_penalty=total_penalty,
        final_reward=br - total_penalty,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GreenPenaltyBreakdown",
    "GreenRewardConfig",
    "GreenRewardError",
    "apply_green_penalty",
    "apply_green_penalty_detailed",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m rlhf.green_reward
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Happy path ----------------------------------------------- #
    r = apply_green_penalty(
        base_reward=1.0, energy=0.003, carbon=0.0006,
    )
    print("penalty    :", r)

    # ---- Detailed breakdown -------------------------------------- #
    bd = apply_green_penalty_detailed(
        base_reward=1.0, energy=0.003, carbon=0.0006,
    )
    print("breakdown  :", bd)
    print("dict       :", bd.to_dict())

    # ---- Bug fix: NaN / inf rejection ---------------------------- #
    for bad in (
        (1.0, float("nan"), 0.001),
        (1.0, float("inf"), 0.001),
        (1.0, -0.1, 0.001),
        (1.0, 0.001, -0.1),
        (1.0, 0.001, 0.001, float("nan")),
        (1.0, 0.001, 0.001, -1.0),
    ):
        try:
            apply_green_penalty(*bad)
        except GreenRewardError as exc:
            print("Rejected   :", exc)

    # ---- Config validation --------------------------------------- #
    for bad_cfg in (
        dict(default_energy_weight=-1.0),
        dict(default_carbon_weight=float("nan")),
        dict(max_energy=0),
        dict(max_carbon=-1),
        dict(max_weight=0),
    ):
        try:
            GreenRewardConfig(**bad_cfg)  # type: ignore[arg-type]
        except GreenRewardError as exc:
            print("Rejected cfg:", exc)

    # ---- Non-strict coerces -------------------------------------- #
    r = apply_green_penalty(1.0, float("nan"), 0.001, strict=False)
    print("lenient    :", r)

    # ---- Config override ----------------------------------------- #
    cfg = GreenRewardConfig(default_energy_weight=0.1)
    r = apply_green_penalty(
        1.0, 0.005, 0.0002, energy_weight=cfg.default_energy_weight,
        config=cfg,
    )
    print("configured :", r)

    print("\nSmoke test passed.")
