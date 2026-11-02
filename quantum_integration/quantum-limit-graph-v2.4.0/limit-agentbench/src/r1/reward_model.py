# src/r1/reward_model.py

"""
Green Reward Model
==================

Multi-objective reward function for energy-aware reinforcement learning.
Lower energy, latency, and carbon → higher reward.

Design
------
- Frozen, validated :class:`RewardModelConfig`: weights must be finite,
  non-negative, and sum to 1.0; metric keys must be non-empty and
  distinct.
- **Non-strict mode returns ``None``** for invalid metrics, so callers
  can distinguish "reward unknown" from "reward is zero". It does *not*
  silently substitute ``0.0`` (which would fabricate the best possible
  reward for corrupt inputs).
- **Live config views**: ``energy_weight`` / ``latency_weight`` /
  ``carbon_weight`` are read-only properties backed by the frozen
  config. Mutating the model's weights requires constructing a new
  model — there is no silent no-op.
- Accepts ``Decimal``, ``Fraction``, and NumPy scalars via
  ``numbers.Real``.
- Optional ``normalize=True`` maps the reward into ``(-1, 0]`` via
  ``-cost / (1 + cost)`` — monotone, equal to ``-cost`` for small costs.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json``; ``to_json`` uses ``allow_nan=False``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "GreenRewardModel",
    "RewardModelConfig",
    "RewardModelError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RewardModelError(ValueError):
    """Raised for invalid reward-model inputs or configuration."""


# --------------------------------------------------------------------------- #
# Validation helper
# --------------------------------------------------------------------------- #
def _validate_weight(name: str, value: Any) -> float:
    """
    Validate a single weight and return its float value.

    Rejects ``bool``, non-numeric, non-finite (``NaN``/``Inf``), and
    negative values. The ``math.isfinite`` check is the critical one —
    ``NaN < 0`` and ``abs(NaN) > eps`` are both ``False``, so a NaN
    weight previously passed every subsequent check.
    """
    if isinstance(value, bool):
        raise RewardModelError(
            f"{name} must be a real number, got bool."
        )
    if not isinstance(value, (numbers.Real, Decimal)):
        raise RewardModelError(
            f"{name} must be a real number, "
            f"got {type(value).__name__}."
        )
    try:
        fv = float(value)
    except (TypeError, ValueError) as exc:
        raise RewardModelError(
            f"{name} not convertible to float: {exc}"
        ) from exc
    if not math.isfinite(fv):
        raise RewardModelError(
            f"{name} must be finite, got {value!r}."
        )
    if fv < 0:
        raise RewardModelError(
            f"{name} must be >= 0, got {fv}."
        )
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RewardModelConfig:
    """Tunable parameters for the green reward model."""

    energy_weight: float = 0.5
    latency_weight: float = 0.3
    carbon_weight: float = 0.2

    # Keys expected in the metrics mapping. Must be non-empty and distinct.
    key_energy: str = "energy_kwh"
    key_latency: str = "latency"
    key_carbon: str = "carbon_kg"

    # When True, reward = -cost / (1 + cost) ∈ (-1, 0]. Monotone with
    # reward = -cost; equal to -cost for small cost. Useful for PPO-style
    # advantage estimation where an unbounded reward destabilizes updates.
    normalize: bool = False

    def __post_init__(self) -> None:
        # ---- Weights: validate + coerce to float -------------------
        energy_f = _validate_weight("energy_weight", self.energy_weight)
        latency_f = _validate_weight(
            "latency_weight", self.latency_weight
        )
        carbon_f = _validate_weight("carbon_weight", self.carbon_weight)

        # Frozen dataclass: normalise the stored values so subsequent
        # arithmetic and __hash__ compare consistently across Decimal /
        # Fraction / float inputs.
        object.__setattr__(self, "energy_weight", energy_f)
        object.__setattr__(self, "latency_weight", latency_f)
        object.__setattr__(self, "carbon_weight", carbon_f)

        total = energy_f + latency_f + carbon_f
        if abs(total - 1.0) > 1e-6:
            raise RewardModelError(
                f"weights must sum to 1.0 (got {total:.6f})."
            )

        # ---- Keys --------------------------------------------------
        keys = (self.key_energy, self.key_latency, self.key_carbon)
        for k in keys:
            if not isinstance(k, str) or not k:
                raise RewardModelError(
                    f"metric keys must be non-empty strings, got {k!r}."
                )
        if len(set(keys)) != 3:
            raise RewardModelError(
                f"metric keys must be distinct, got {keys}."
            )

        # ---- normalize --------------------------------------------
        if not isinstance(self.normalize, bool):
            raise RewardModelError("normalize must be bool.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardModelConfig":
        if not isinstance(data, Mapping):
            raise RewardModelError(
                f"RewardModelConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                energy_weight=data.get("energy_weight", 0.5),
                latency_weight=data.get("latency_weight", 0.3),
                carbon_weight=data.get("carbon_weight", 0.2),
                key_energy=str(
                    data.get("key_energy", "energy_kwh")
                ),
                key_latency=str(
                    data.get("key_latency", "latency")
                ),
                key_carbon=str(
                    data.get("key_carbon", "carbon_kg")
                ),
                normalize=data.get("normalize", False),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, RewardModelError):
                raise
            raise RewardModelError(
                f"invalid RewardModelConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #
class GreenRewardModel:
    """
    Computes a multi-objective reward for RL policy tuning.

    The original ``compute(metrics)`` signature is preserved; new
    parameters are keyword-only.

    Returns
    -------
    ``compute`` returns ``float`` in strict mode, or
    ``Optional[float]`` in non-strict mode (``None`` when any metric is
    invalid). Weights are exposed as read-only properties — the config
    is frozen, so weight changes require constructing a new model.
    """

    def __init__(
        self,
        energy_weight: float = 0.5,
        latency_weight: float = 0.3,
        carbon_weight: float = 0.2,
        *,
        config: Optional[RewardModelConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = RewardModelConfig(
                energy_weight=energy_weight,
                latency_weight=latency_weight,
                carbon_weight=carbon_weight,
            )
        self._strict = bool(strict)

        logger.debug(
            "GreenRewardModel initialized "
            "(weights=(%.2f, %.2f, %.2f), normalize=%s, strict=%s)",
            self._config.energy_weight,
            self._config.latency_weight,
            self._config.carbon_weight,
            self._config.normalize,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RewardModelConfig:
        return self._config

    @property
    def energy_weight(self) -> float:
        return self._config.energy_weight

    @property
    def latency_weight(self) -> float:
        return self._config.latency_weight

    @property
    def carbon_weight(self) -> float:
        return self._config.carbon_weight

    # ---------------------------------------------------------- compute
    def compute(self, metrics: Mapping[str, Any]) -> Optional[float]:
        """
        Compute the reward for the given metrics.

        Returns ``-cost`` (minimize energy + latency + carbon), or
        ``None`` in non-strict mode when any metric is invalid. In strict
        mode, invalid metrics raise :class:`RewardModelError`.

        Parameters
        ----------
        metrics : Mapping
            Must contain the three keys configured in
            :class:`RewardModelConfig` (``key_energy``,
            ``key_latency``, ``key_carbon``). Values must be finite,
            non-negative reals.
        """
        if not isinstance(metrics, Mapping):
            raise RewardModelError(
                f"metrics must be a Mapping, got "
                f"{type(metrics).__name__}."
            )

        cfg = self._config
        energy = self._coerce_metric(metrics, cfg.key_energy)
        latency = self._coerce_metric(metrics, cfg.key_latency)
        carbon = self._coerce_metric(metrics, cfg.key_carbon)

        if energy is None or latency is None or carbon is None:
            # Non-strict mode encountered at least one invalid metric.
            return None

        cost = (
            cfg.energy_weight * energy
            + cfg.latency_weight * latency
            + cfg.carbon_weight * carbon
        )
        if cfg.normalize:
            # Monotone map into (-1, 0]; equals -cost for small cost.
            reward = -(cost / (1.0 + cost))
        else:
            reward = -cost

        logger.debug(
            "Reward computed: energy=%.4g latency=%.4g carbon=%.4g "
            "cost=%.4g reward=%.4g normalize=%s",
            energy, latency, carbon, cost, reward, cfg.normalize,
        )
        return reward

    def _coerce_metric(
        self, metrics: Mapping[str, Any], key: str
    ) -> Optional[float]:
        if key not in metrics:
            msg = f"metrics missing required key '{key}'."
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Returning None.", msg)
            return None

        value = metrics[key]
        if isinstance(value, bool) or not isinstance(
            value, (numbers.Real, Decimal)
        ):
            msg = (
                f"metrics['{key}'] must be real, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Returning None.", msg)
            return None

        try:
            fv = float(value)
        except (TypeError, ValueError) as exc:
            msg = (
                f"metrics['{key}'] not convertible to float: {exc}."
            )
            if self._strict:
                raise RewardModelError(msg) from exc
            logger.warning("%s Returning None.", msg)
            return None

        if not math.isfinite(fv):
            msg = (
                f"metrics['{key}'] must be finite, got {value!r}."
            )
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Returning None.", msg)
            return None

        if fv < 0:
            msg = f"metrics['{key}'] must be >= 0, got {fv}."
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Returning None.", msg)
            return None

        return fv

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": VERSION,
            "config": self._config.to_dict(),
            "strict": self._strict,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
    ) -> "GreenRewardModel":
        if not isinstance(data, Mapping):
            raise RewardModelError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        cfg = RewardModelConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        strict_flag = (
            bool(data.get("strict", True)) if strict is None else strict
        )
        return cls(config=cfg, strict=strict_flag)

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenRewardModel":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RewardModelError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        cfg = self._config
        return (
            "GreenRewardModel("
            f"weights=(energy={cfg.energy_weight:.2f}, "
            f"latency={cfg.latency_weight:.2f}, "
            f"carbon={cfg.carbon_weight:.2f}), "
            f"normalize={cfg.normalize}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m r1.reward_model
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    from fractions import Fraction

    logging.basicConfig(level=logging.INFO)
    model = GreenRewardModel()
    print("repr       :", model)
    print("VERSION    :", VERSION)

    r1 = model.compute({
        "energy_kwh": 0.01, "latency": 1.0, "carbon_kg": 0.003,
    })
    r2 = model.compute({
        "energy_kwh": 0.05, "latency": 2.0, "carbon_kg": 0.01,
    })
    print("reward 1   :", r1)
    print("reward 2   :", r2)
    assert r1 is not None and r2 is not None and r1 > r2, (
        "lower-energy metrics must yield higher reward"
    )

    # ---- NaN / Inf weights rejected (the critical fix) -------- #
    for bad_cfg in (
        dict(energy_weight=float("nan")),
        dict(energy_weight=float("inf")),
        dict(
            energy_weight=0.5,
            latency_weight=float("nan"),
            carbon_weight=float("nan"),
        ),
        dict(
            energy_weight=0.5,
            latency_weight=0.5,
            carbon_weight=0.5,
        ),
        dict(energy_weight=-0.1),
        dict(energy_weight=True),
    ):
        try:
            RewardModelConfig(**bad_cfg)  # type: ignore[arg-type]
        except RewardModelError as exc:
            print("Rejected cfg:", exc)

    # ---- Duplicate / empty keys rejected ----------------------- #
    for bad_cfg in (
        dict(
            energy_weight=0.5, latency_weight=0.3, carbon_weight=0.2,
            key_energy="", 
        ),
        dict(
            energy_weight=0.5, latency_weight=0.3, carbon_weight=0.2,
            key_energy="x", key_latency="x", key_carbon="x",
        ),
    ):
        try:
            RewardModelConfig(**bad_cfg)  # type: ignore[arg-type]
        except RewardModelError as exc:
            print("Rejected cfg:", exc)

    # ---- Dead-attribute fix: weights are read-only ------------ #
    try:
        model.energy_weight = 0.9  # type: ignore[misc]
    except AttributeError as exc:
        print("Read-only  :", exc)
    assert model.energy_weight == 0.5

    # ---- Non-strict returns None, not 0.0 --------------------- #
    lenient = GreenRewardModel(strict=False)
    result = lenient.compute({"energy_kwh": 0.01})   # missing 2 keys
    print("lenient    :", result, "(None, not 0.0)")
    assert result is None

    # Missing single key
    assert lenient.compute({
        "energy_kwh": 0.01, "latency": 1.0,
    }) is None

    # Negative metric
    assert lenient.compute({
        "energy_kwh": -0.01, "latency": 1.0, "carbon_kg": 0.003,
    }) is None

    # NaN metric
    assert lenient.compute({
        "energy_kwh": float("nan"),
        "latency": 1.0,
        "carbon_kg": 0.003,
    }) is None

    # All valid → real reward
    good = lenient.compute({
        "energy_kwh": 0.01, "latency": 1.0, "carbon_kg": 0.003,
    })
    print("lenient good:", good)
    assert good is not None

    # Strict mode still raises on any issue
    for bad_call in (
        lambda: model.compute({"energy_kwh": 0.01}),
        lambda: model.compute({
            "energy_kwh": -1.0, "latency": 1.0, "carbon_kg": 0.003,
        }),
        lambda: model.compute({
            "energy_kwh": float("nan"),
            "latency": 1.0, "carbon_kg": 0.003,
        }),
        lambda: model.compute("not a mapping"),  # type: ignore[arg-type]
    ):
        try:
            bad_call()
        except RewardModelError as exc:
            print("Rejected   :", exc)

    # ---- Decimal / Fraction / numeric-types accepted ---------- #
    numeric = GreenRewardModel()
    r = numeric.compute({
        "energy_kwh": Decimal("0.01"),
        "latency": Fraction(1, 1),
        "carbon_kg": Decimal("0.003"),
    })
    print("numeric    :", r)
    assert r is not None

    # ---- Normalization option ---------------------------------- #
    norm = GreenRewardModel(
        config=RewardModelConfig(
            energy_weight=0.5, latency_weight=0.3, carbon_weight=0.2,
            normalize=True,
        ),
    )
    raw_val = model.compute({
        "energy_kwh": 0.01, "latency": 1.0, "carbon_kg": 0.003,
    })
    norm_val = norm.compute({
        "energy_kwh": 0.01, "latency": 1.0, "carbon_kg": 0.003,
    })
    print("normalize  :", norm_val, "(raw =", raw_val, ")")
    assert norm_val is not None and raw_val is not None
    assert -1.0 < norm_val <= 0.0
    assert norm_val > raw_val  # -cost/(1+cost) is closer to 0

    # Zero cost → zero reward under both modes
    zero_metrics = {
        "energy_kwh": 0.0, "latency": 0.0, "carbon_kg": 0.0,
    }
    assert model.compute(zero_metrics) == 0.0
    assert norm.compute(zero_metrics) == 0.0

    # ---- from_dict / from_json round-trip --------------------- #
    payload = model.to_dict()
    assert payload["version"] == VERSION
    rt = GreenRewardModel.from_dict(payload)
    assert rt.to_dict() == model.to_dict()
    assert rt.energy_weight == model.energy_weight

    rt_norm = GreenRewardModel.from_dict(norm.to_dict())
    assert rt_norm.config.normalize is True

    rt_json = GreenRewardModel.from_json(model.to_json())
    assert rt_json.to_dict() == model.to_dict()
    print("Round-trip OK.")

    # ---- from_dict errors wrapped ----------------------------- #
    for bad_payload in (
        {"config": {"energy_weight": float("nan")}},
        {"config": {"energy_weight": 5.0}},
        {"config": {"key_energy": ""}},
        "not-a-mapping",
    ):
        try:
            GreenRewardModel.from_dict(bad_payload)  # type: ignore[arg-type]
        except RewardModelError as exc:
            print("Rejected payload:", exc)

    # ---- to_json refuses NaN (defensive) ---------------------- #
    # Build a config that would only be NaN-tainted if someone bypassed
    # the validator via object.__setattr__.
    tainted = RewardModelConfig()
    object.__setattr__(tainted, "energy_weight", float("nan"))
    tainted_model = GreenRewardModel(config=tainted)
    try:
        tainted_model.to_json()
    except ValueError as exc:
        print("NaN in to_json rejected:", exc)

    print("\nSmoke test passed.")
