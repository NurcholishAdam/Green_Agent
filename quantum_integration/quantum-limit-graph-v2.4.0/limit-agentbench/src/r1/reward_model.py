# src/r1/reward_model.py

"""
Green Reward Model
==================

Multi-objective reward function for energy-aware reinforcement learning.
Lower energy, latency, and carbon → higher reward.

Enhancements
------------
- ``RewardModelConfig`` — frozen, validated: weights must sum to 1.0.
- Full validation of every metric input; missing keys handled per strict mode.
- ``to_dict`` / ``from_dict`` on the model.
- ``__repr__``, custom ``RewardModelError``, lazy ``%s`` logging.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import json, logging, math
from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


class RewardModelError(ValueError):
    """Raised for invalid reward-model inputs or configuration."""


@dataclass(frozen=True)
class RewardModelConfig:
    """Tunable parameters for the green reward model."""
    energy_weight: float = 0.5
    latency_weight: float = 0.3
    carbon_weight: float = 0.2

    # Keys expected in the metrics mapping.
    key_energy: str = "energy_kwh"
    key_latency: str = "latency"
    key_carbon: str = "carbon_kg"

    def __post_init__(self) -> None:
        for name in ("energy_weight", "latency_weight", "carbon_weight"):
            value = getattr(self, name)
            if value < 0:
                raise RewardModelError(f"{name} must be >= 0 (got {value}).")
        total = self.energy_weight + self.latency_weight + self.carbon_weight
        if abs(total - 1.0) > 1e-6:
            raise RewardModelError(
                f"weights must sum to 1.0 (got {total:.6f})."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class GreenRewardModel:
    """
    Computes a multi-objective reward for RL policy tuning.

    The original ``compute(metrics)`` signature is preserved; new parameters
    are keyword-only.
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

        # Legacy attributes preserved.
        self.energy_weight = self._config.energy_weight
        self.latency_weight = self._config.latency_weight
        self.carbon_weight = self._config.carbon_weight

        logger.debug(
            "GreenRewardModel initialized (weights=(%.2f, %.2f, %.2f), strict=%s)",
            self.energy_weight, self.latency_weight, self.carbon_weight,
            self._strict,
        )

    @property
    def config(self) -> RewardModelConfig:
        return self._config

    def compute(self, metrics: Mapping[str, Any]) -> float:
        """
        Compute the reward for the given metrics.

        Returns ``-cost`` (minimize energy + latency + carbon).
        """
        if not isinstance(metrics, Mapping):
            raise RewardModelError(
                f"metrics must be a Mapping, got {type(metrics).__name__}."
            )

        cfg = self._config
        energy = self._coerce_metric(metrics, cfg.key_energy)
        latency = self._coerce_metric(metrics, cfg.key_latency)
        carbon = self._coerce_metric(metrics, cfg.key_carbon)

        cost = (
            cfg.energy_weight * energy
            + cfg.latency_weight * latency
            + cfg.carbon_weight * carbon
        )
        reward = -cost
        logger.debug(
            "Reward computed: energy=%.4g latency=%.4g carbon=%.4g reward=%.4g",
            energy, latency, carbon, reward,
        )
        return reward

    def _coerce_metric(self, metrics: Mapping[str, Any], key: str) -> float:
        if key not in metrics:
            msg = f"metrics missing required key '{key}'."
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        value = metrics[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"metrics['{key}'] must be numeric, got {type(value).__name__}."
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv) or fv < 0:
            msg = f"metrics['{key}'] must be finite and >= 0, got {fv}."
            if self._strict:
                raise RewardModelError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fv

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self._config.to_dict(),
            "strict": self._strict,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "GreenRewardModel("
            f"weights=(energy={self.energy_weight:.2f}, "
            f"latency={self.latency_weight:.2f}, "
            f"carbon={self.carbon_weight:.2f}), "
            f"strict={self._strict})"
        )


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    model = GreenRewardModel()
    print("repr       :", model)

    r1 = model.compute({"energy_kwh": 0.01, "latency": 1.0, "carbon_kg": 0.003})
    r2 = model.compute({"energy_kwh": 0.05, "latency": 2.0, "carbon_kg": 0.01})
    print("reward 1   :", r1)
    print("reward 2   :", r2)
    assert r1 > r2, "lower-energy agent should have higher reward"

    # Strict mode rejects missing keys.
    try:
        model.compute({"energy_kwh": 0.01})
    except RewardModelError as exc:
        print("Rejected   :", exc)

    # Non-strict coerces.
    lenient = GreenRewardModel(strict=False)
    print("lenient    :", lenient.compute({"energy_kwh": 0.01}))

    # Config validation.
    for bad in (
        dict(energy_weight=0.5, latency_weight=0.5, carbon_weight=0.5),
        dict(energy_weight=-0.1),
    ):
        try:
            RewardModelConfig(**bad)  # type: ignore[arg-type]
        except RewardModelError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
