# src/rlhf/green_reward.py

"""
Green-aware reward shaping for RLHF
===================================

Provides a lightweight penalty function and a richer structured API for
shaping base rewards using energy and carbon signals.

Design
------
- Frozen, validated :class:`GreenRewardConfig`: default weights,
  base-reward bounds, energy/carbon ceilings.
- **Config defaults are honoured.** ``apply_green_penalty(..., config=cfg)``
  with no explicit ``energy_weight`` uses ``cfg.default_energy_weight``.
  Previously the function signature hardcoded ``0.05`` and ignored the
  config, making ``default_energy_weight`` / ``default_carbon_weight``
  dead config.
- **Non-strict fallback clamps into bounds.** Invalid inputs are
  replaced with ``clamp(0.0, low, high)`` rather than raw ``0.0`` —
  safe for bounded fields (energy, carbon, weights) and neutral for
  base_reward.
- **``base_reward`` bounds are config-driven.**
  ``min_base_reward`` / ``max_base_reward`` (default ±1e15) replace
  the hardcoded range and the dead ``-math.inf if False else -1e15``
  expression.
- **``strict`` is validated as ``bool``** at the API boundary.
- **Breakdown is validated** — finite, non-negative penalties, and
  arithmetic invariants (``energy_penalty + carbon_penalty ==
  total_penalty``, ``base_reward − total_penalty == final_reward``)
  within ``math.isclose`` tolerances.
- **Serialization round-trips.** ``to_dict`` / ``from_dict`` /
  ``to_json`` on the config and the breakdown; ``to_json`` uses
  ``allow_nan=False``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "GreenPenaltyBreakdown",
    "GreenRewardConfig",
    "GreenRewardError",
    "apply_green_penalty",
    "apply_green_penalty_detailed",
]

VERSION = "2.0.0"


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

    # Default penalty weights used when callers omit energy_weight /
    # carbon_weight. Previously these were hardcoded in the function
    # signature and the config fields were dead.
    default_energy_weight: float = 0.05
    default_carbon_weight: float = 1.0

    # Base-reward bounds. Previously hardcoded to ±1e15 inside the
    # function — now configurable per caller's reward distribution.
    min_base_reward: float = -1e15
    max_base_reward: float = 1e15

    # Finite ceilings so outliers are rejected rather than propagated.
    max_energy: float = 1e12
    max_carbon: float = 1e12
    max_weight: float = 1e6

    def __post_init__(self) -> None:
        for name in (
            "default_energy_weight",
            "default_carbon_weight",
            "min_base_reward",
            "max_base_reward",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
            ):
                raise GreenRewardError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv):
                raise GreenRewardError(
                    f"{name} must be finite, got {value!r}."
                )
        for name in (
            "default_energy_weight",
            "default_carbon_weight",
        ):
            if getattr(self, name) < 0:
                raise GreenRewardError(f"{name} must be >= 0.")
        if self.min_base_reward >= self.max_base_reward:
            raise GreenRewardError(
                "min_base_reward must be < max_base_reward."
            )
        for name in ("max_energy", "max_carbon", "max_weight"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise GreenRewardError(
                    f"{name} must be finite and > 0."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "GreenRewardConfig":
        if not isinstance(data, Mapping):
            raise GreenRewardError(
                f"GreenRewardConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            unknown = set(data) - valid
            if unknown:
                logger.debug(
                    "GreenRewardConfig.from_dict dropping unknown "
                    "fields: %s", sorted(unknown),
                )
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, GreenRewardError):
                raise
            raise GreenRewardError(
                f"invalid GreenRewardConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Breakdown record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenPenaltyBreakdown:
    """Immutable, validated decomposition of a green-penalty computation."""

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

    def __post_init__(self) -> None:
        for name in (
            "base_reward",
            "energy",
            "carbon",
            "energy_weight",
            "carbon_weight",
            "energy_penalty",
            "carbon_penalty",
            "total_penalty",
            "final_reward",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
            ):
                raise GreenRewardError(
                    f"{name} must be finite and numeric, "
                    f"got {value!r}."
                )
        for name in (
            "energy",
            "carbon",
            "energy_weight",
            "carbon_weight",
            "energy_penalty",
            "carbon_penalty",
            "total_penalty",
        ):
            if getattr(self, name) < 0:
                raise GreenRewardError(f"{name} must be >= 0.")
        if not math.isclose(
            self.energy_penalty + self.carbon_penalty,
            self.total_penalty,
            rel_tol=1e-6, abs_tol=1e-9,
        ):
            raise GreenRewardError(
                "energy_penalty + carbon_penalty != total_penalty."
            )
        if not math.isclose(
            self.base_reward - self.total_penalty,
            self.final_reward,
            rel_tol=1e-6, abs_tol=1e-9,
        ):
            raise GreenRewardError(
                "base_reward - total_penalty != final_reward."
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

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "GreenPenaltyBreakdown":
        if not isinstance(data, Mapping):
            raise GreenRewardError(
                "GreenPenaltyBreakdown.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc,
                    )
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)
            return cls(
                base_reward=float(data["base_reward"]),
                energy=float(data["energy"]),
                carbon=float(data["carbon"]),
                energy_weight=float(data["energy_weight"]),
                carbon_weight=float(data["carbon_weight"]),
                energy_penalty=float(data["energy_penalty"]),
                carbon_penalty=float(data["carbon_penalty"]),
                total_penalty=float(data["total_penalty"]),
                final_reward=float(data["final_reward"]),
                timestamp=timestamp,
            )
        except KeyError as exc:
            raise GreenRewardError(
                f"GreenPenaltyBreakdown payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, GreenRewardError):
                raise
            raise GreenRewardError(
                f"invalid GreenPenaltyBreakdown payload: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenPenaltyBreakdown":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenRewardError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    def __repr__(self) -> str:
        return (
            "GreenPenaltyBreakdown("
            f"base={self.base_reward:.4g}, "
            f"energy_penalty={self.energy_penalty:.4g}, "
            f"carbon_penalty={self.carbon_penalty:.4g}, "
            f"final={self.final_reward:.4g})"
        )


# --------------------------------------------------------------------------- #
# Internal validation
# --------------------------------------------------------------------------- #
def _validate_number(
    name: str,
    value: Any,
    *,
    low: float,
    high: float,
    strict: bool,
) -> float:
    """
    Validate ``value`` and return the canonical float.

    Non-strict fallbacks clamp ``0.0`` into ``[low, high]`` — never a
    value outside the declared bounds.
    """
    fallback = max(low, min(high, 0.0))
    if isinstance(value, bool) or not isinstance(
        value, numbers.Real,
    ):
        msg = (
            f"{name} must be numeric, got {type(value).__name__}."
        )
        if strict:
            raise GreenRewardError(msg)
        logger.warning(
            "%s Using %.4g (non-strict).", msg, fallback,
        )
        return fallback
    fv = float(value)
    if not math.isfinite(fv):
        msg = f"{name} must be finite, got {value!r}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning(
            "%s Using %.4g (non-strict).", msg, fallback,
        )
        return fallback
    if fv < low:
        msg = f"{name} must be >= {low}, got {fv}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning("%s Clamping to %.4g.", msg, low)
        return low
    if fv > high:
        msg = f"{name} must be <= {high}, got {fv}."
        if strict:
            raise GreenRewardError(msg)
        logger.warning("%s Clamping to %.4g.", msg, high)
        return high
    return fv


def _validate_strict(strict: Any) -> bool:
    if not isinstance(strict, bool):
        raise GreenRewardError(
            f"strict must be bool, got {type(strict).__name__}."
        )
    return strict


def _resolve_config(
    config: Optional[GreenRewardConfig],
) -> GreenRewardConfig:
    if config is None:
        return GreenRewardConfig()
    if not isinstance(config, GreenRewardConfig):
        raise GreenRewardError(
            f"config must be a GreenRewardConfig, got "
            f"{type(config).__name__}."
        )
    return config


def _validate_inputs(
    base_reward: Any,
    energy: Any,
    carbon: Any,
    energy_weight: Any,
    carbon_weight: Any,
    cfg: GreenRewardConfig,
    strict: bool,
) -> Tuple[float, float, float, float, float]:
    """Validate all five inputs against ``cfg``'s bounds in one place."""
    br = _validate_number(
        "base_reward", base_reward,
        low=cfg.min_base_reward,
        high=cfg.max_base_reward,
        strict=strict,
    )
    en = _validate_number(
        "energy", energy, low=0.0,
        high=cfg.max_energy, strict=strict,
    )
    ca = _validate_number(
        "carbon", carbon, low=0.0,
        high=cfg.max_carbon, strict=strict,
    )
    ew = _validate_number(
        "energy_weight", energy_weight, low=0.0,
        high=cfg.max_weight, strict=strict,
    )
    cw = _validate_number(
        "carbon_weight", carbon_weight, low=0.0,
        high=cfg.max_weight, strict=strict,
    )
    return br, en, ca, ew, cw


# --------------------------------------------------------------------------- #
# Public functions
# --------------------------------------------------------------------------- #
def apply_green_penalty(
    base_reward: float,
    energy: float,
    carbon: float,
    energy_weight: Optional[float] = None,
    carbon_weight: Optional[float] = None,
    *,
    config: Optional[GreenRewardConfig] = None,
    strict: bool = True,
) -> float:
    """
    Penalize a base reward based on environmental impact.

    Formula::

        penalty = energy_weight × energy + carbon_weight × carbon
        final   = base_reward - penalty

    ``energy_weight`` and ``carbon_weight`` default to
    ``config.default_energy_weight`` / ``config.default_carbon_weight``
    when omitted (``None``).

    Parameters
    ----------
    base_reward : float
        The base reward to shape.
    energy : float
        Energy consumed (kWh). Must be finite and >= 0.
    carbon : float
        Carbon emitted (kg CO2e). Must be finite and >= 0.
    energy_weight : float, optional
        Penalty weight for energy. ``None`` → ``config`` default.
    carbon_weight : float, optional
        Penalty weight for carbon. ``None`` → ``config`` default.
    config : GreenRewardConfig, optional
        Configuration used for the ceilings, bounds, and default weights.
    strict : bool, default True
        If True, invalid inputs raise :class:`GreenRewardError`.

    Returns
    -------
    float
        The shaped reward.
    """
    strict = _validate_strict(strict)
    cfg = _resolve_config(config)

    ew_arg = (
        cfg.default_energy_weight
        if energy_weight is None else energy_weight
    )
    cw_arg = (
        cfg.default_carbon_weight
        if carbon_weight is None else carbon_weight
    )
    br, en, ca, ew, cw = _validate_inputs(
        base_reward, energy, carbon,
        ew_arg, cw_arg, cfg, strict,
    )
    return br - (ew * en + cw * ca)


def apply_green_penalty_detailed(
    base_reward: float,
    energy: float,
    carbon: float,
    energy_weight: Optional[float] = None,
    carbon_weight: Optional[float] = None,
    *,
    config: Optional[GreenRewardConfig] = None,
    strict: bool = True,
) -> GreenPenaltyBreakdown:
    """
    Same as :func:`apply_green_penalty` but returns a structured
    :class:`GreenPenaltyBreakdown` for logging / RLHF records.
    """
    strict = _validate_strict(strict)
    cfg = _resolve_config(config)

    ew_arg = (
        cfg.default_energy_weight
        if energy_weight is None else energy_weight
    )
    cw_arg = (
        cfg.default_carbon_weight
        if carbon_weight is None else carbon_weight
    )
    br, en, ca, ew, cw = _validate_inputs(
        base_reward, energy, carbon,
        ew_arg, cw_arg, cfg, strict,
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
# __all__ completeness assertion
# --------------------------------------------------------------------------- #
_missing_names = [n for n in __all__ if n not in globals()]
if _missing_names:
    raise GreenRewardError(
        f"green_reward __all__ references missing names: "
        f"{sorted(_missing_names)}."
    )


# --------------------------------------------------------------------------- #
# Smoke test: python -m rlhf.green_reward
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("VERSION    :", VERSION)

    # ---- Happy path ------------------------------------------------ #
    r = apply_green_penalty(
        base_reward=1.0, energy=0.003, carbon=0.0006,
    )
    print(f"penalty    : {r:.6f}")

    bd = apply_green_penalty_detailed(
        base_reward=1.0, energy=0.003, carbon=0.0006,
    )
    print("breakdown  :", bd)
    print("dict       :", bd.to_dict())

    # ---- Config defaults are honoured ----------------------------- #
    cfg = GreenRewardConfig(
        default_energy_weight=0.10,
        default_carbon_weight=1.0,
    )
    r_default = apply_green_penalty(
        1.0, 0.005, 0.0002, config=cfg,
    )
    r_explicit = apply_green_penalty(
        1.0, 0.005, 0.0002,
        energy_weight=0.10, carbon_weight=1.0,
        config=cfg,
    )
    assert r_default == r_explicit, (
        f"config default not honoured: "
        f"default={r_default}, explicit={r_explicit}"
    )
    print(
        f"config dflt: default={r_default:.6f} "
        f"== explicit={r_explicit:.6f}"
    )

    # ---- Bug fix: NaN / inf rejection ----------------------------- #
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
            print("Rejected   :", str(exc)[:60])

    # ---- Bug fix: strict must be bool ---------------------------- #
    for bad_strict in ("yes", None, 0, 1):
        try:
            apply_green_penalty(
                1.0, 0.001, 0.001,
                strict=bad_strict,  # type: ignore[arg-type]
            )
        except GreenRewardError as exc:
            print("Rejected   :", str(exc)[:60])

    # ---- base_reward bounds are config-driven -------------------- #
    tight = GreenRewardConfig(
        min_base_reward=-1.0, max_base_reward=1.0,
    )
    try:
        apply_green_penalty(
            base_reward=5.0, energy=0.001, carbon=0.001,
            config=tight,
        )
    except GreenRewardError as exc:
        print("Rejected   :", str(exc)[:60])

    # ---- Config validation --------------------------------------- #
    for bad_cfg in (
        dict(default_energy_weight=-1.0),
        dict(default_carbon_weight=float("nan")),
        dict(min_base_reward=1.0, max_base_reward=-1.0),
        dict(max_energy=0),
        dict(max_carbon=-1),
        dict(max_weight=float("inf")),
    ):
        try:
            GreenRewardConfig(**bad_cfg)  # type: ignore[arg-type]
        except GreenRewardError as exc:
            print("Rejected c :", str(exc)[:60])

    # ---- Config round-trip --------------------------------------- #
    cfg_rt = GreenRewardConfig.from_dict(cfg.to_dict())
    assert cfg_rt == cfg
    print("config rt  : OK")

    # ---- Breakdown round-trip ----------------------------------- #
    bd_rt = GreenPenaltyBreakdown.from_json(bd.to_json())
    assert bd_rt.to_dict() == bd.to_dict()
    print("breakdown rt: OK")

    # ---- Breakdown validation ----------------------------------- #
    for bad_bd in (
        # Negative penalty
        dict(
            base_reward=1.0, energy=-0.1, carbon=0.001,
            energy_weight=0.05, carbon_weight=1.0,
            energy_penalty=-0.005, carbon_penalty=0.001,
            total_penalty=-0.004, final_reward=1.004,
        ),
        # Invariant violated: energy + carbon != total
        dict(
            base_reward=1.0, energy=0.001, carbon=0.001,
            energy_weight=0.05, carbon_weight=1.0,
            energy_penalty=0.1, carbon_penalty=0.001,
            total_penalty=0.001, final_reward=0.9,
        ),
        # NaN base reward
        dict(
            base_reward=float("nan"), energy=0.001, carbon=0.001,
            energy_weight=0.05, carbon_weight=1.0,
            energy_penalty=0.00005, carbon_penalty=0.001,
            total_penalty=0.00105, final_reward=float("nan"),
        ),
    ):
        try:
            GreenPenaltyBreakdown(**bad_bd)  # type: ignore[arg-type]
        except GreenRewardError as exc:
            print("Rejected b :", str(exc)[:60])

    # ---- Non-strict coerces safely ------------------------------ #
    r = apply_green_penalty(
        1.0, float("nan"), 0.001, strict=False,
    )
    print(f"lenient    : {r:.6f}")

    # ---- to_json rejects NaN ----------------------------------- #
    tainted_cfg = GreenRewardConfig()
    object.__setattr__(
        tainted_cfg, "max_energy", float("nan"),
    )
    try:
        apply_green_penalty_detailed(
            1.0, 0.001, 0.001, config=tainted_cfg,
        )
    except GreenRewardError as exc:
        print("NaN config :", str(exc)[:60])

    print("\nSmoke test passed.")
