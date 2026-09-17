# src/rewards/negawatt_reward.py

"""
Negawatt Reward Shaping
=======================

Sustainability-oriented reward shaping for reinforcement learning and
benchmarking pipelines:

- ``accuracy_per_watt``  — classical efficiency metric.
- ``negawatt_score``     — relative energy savings, weighted by accuracy.
- ``combined_reward``    — linear blend of accuracy and negawatt score.

Enhancements
------------
- ``NegawattRewardConfig`` — frozen, validated: baseline energy, default
  weights, accuracy range, energy floor/ceiling.
- ``RewardBreakdown`` — frozen, serializable decomposition of a reward
  computation.
- **Full validation** of every argument; strict / non-strict modes.
- **Fixed negative-accuracy bug** — both ``accuracy`` and the derived
  savings ratio are range-checked so ``negawatt_score`` cannot go positive
  from a negative input.
- **Fixed unvalidated blend weights** — ``alpha`` / ``beta`` are rejected
  when non-finite or negative.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` on the
  reward module and the config.
- ``breakdown()`` — structured decomposition for logging / RLHF.
- ``__call__`` — ``reward(accuracy, energy)`` shorthand.
- ``__repr__``, custom ``NegawattRewardError``, lazy ``%s`` logging, and a
  comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class NegawattRewardError(ValueError):
    """Raised for invalid reward-shaping inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class NegawattRewardConfig:
    """Tunable parameters for :class:`NegawattReward`."""

    baseline_energy: float = 1.0
    default_alpha: float = 1.0
    default_beta: float = 2.0

    # Validation ranges.
    min_accuracy: float = 0.0
    max_accuracy: float = 1.0
    min_energy: float = 0.0
    max_energy: float = 1e12

    def __post_init__(self) -> None:
        for name in ("baseline_energy", "default_alpha", "default_beta"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise NegawattRewardError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise NegawattRewardError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise NegawattRewardError(f"{name} must be >= 0.")
        if not 0.0 <= self.min_accuracy < self.max_accuracy:
            raise NegawattRewardError(
                "accuracy range must satisfy 0 <= min < max."
            )
        if self.min_energy < 0:
            raise NegawattRewardError("min_energy must be >= 0.")
        if self.max_energy <= self.min_energy:
            raise NegawattRewardError(
                "max_energy must be > min_energy."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NegawattRewardConfig":
        if not isinstance(data, Mapping):
            raise NegawattRewardError(
                f"NegawattRewardConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Breakdown record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RewardBreakdown:
    """Immutable, structured decomposition of a reward computation."""

    accuracy: float
    energy: float
    baseline_energy: float
    saved_energy: float
    saved_ratio: float
    accuracy_per_watt: float
    negawatt_score: float
    combined_reward: float
    alpha: float
    beta: float
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accuracy": self.accuracy,
            "energy": self.energy,
            "baseline_energy": self.baseline_energy,
            "saved_energy": self.saved_energy,
            "saved_ratio": self.saved_ratio,
            "accuracy_per_watt": self.accuracy_per_watt,
            "negawatt_score": self.negawatt_score,
            "combined_reward": self.combined_reward,
            "alpha": self.alpha,
            "beta": self.beta,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "RewardBreakdown("
            f"accuracy={self.accuracy:.3f}, "
            f"energy={self.energy:.4g}, "
            f"apw={self.accuracy_per_watt:.4g}, "
            f"negawatt={self.negawatt_score:.4f}, "
            f"combined={self.combined_reward:.4f})"
        )


# --------------------------------------------------------------------------- #
# Reward module
# --------------------------------------------------------------------------- #
class NegawattReward:
    """
    Sustainability reward-shaping module.

    Thread-safe, serializable, and validation-first. All original public
    methods are preserved; new parameters are keyword-only.

    Responsibilities
    ----------------
    - Accuracy per Watt.
    - Negawatt score (relative energy savings × accuracy).
    - RL-compatible combined reward.
    """

    def __init__(
        self,
        baseline_energy: float,
        *,
        config: Optional[NegawattRewardConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            if isinstance(baseline_energy, bool) or not isinstance(
                baseline_energy, (int, float)
            ):
                raise NegawattRewardError(
                    f"baseline_energy must be numeric, got "
                    f"{type(baseline_energy).__name__}."
                )
            self._config = NegawattRewardConfig(
                baseline_energy=float(baseline_energy),
            )
        self._strict = bool(strict)

        # Legacy attribute preserved.
        self.baseline_energy: float = self._config.baseline_energy
        self._lock = threading.RLock()
        self._started_at: float = time.time()

        logger.debug(
            "NegawattReward initialized "
            "(baseline_energy=%.6g, alpha_default=%.3f, beta_default=%.3f, "
            "strict=%s)",
            self.baseline_energy,
            self._config.default_alpha,
            self._config.default_beta,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> NegawattRewardConfig:
        return self._config

    # ---------------------------------------------------------- public API
    def accuracy_per_watt(self, accuracy: float, energy: float) -> float:
        """Return ``accuracy / energy`` (higher is better)."""
        acc = self._validate_accuracy(accuracy, raise_on_error=True)
        en = self._validate_energy(energy, raise_on_error=True)

        if acc is None or en is None:
            return 0.0
        if en <= self._config.min_energy:
            logger.debug(
                "accuracy_per_watt: energy %.6g <= floor %.6g; returning 0.0.",
                en, self._config.min_energy,
            )
            return 0.0
        return acc / en

    def negawatt_score(self, accuracy: float, energy: float) -> float:
        """
        Return ``accuracy × max(0, (baseline − energy) / baseline)``.

        Returns ``0.0`` when ``baseline_energy`` is zero or when the caller
        is not saving energy.
        """
        acc = self._validate_accuracy(accuracy, raise_on_error=True)
        en = self._validate_energy(energy, raise_on_error=True)
        if acc is None or en is None:
            return 0.0

        baseline = self.baseline_energy
        if baseline <= 0:
            logger.debug(
                "negawatt_score: baseline_energy=%.6g <= 0; returning 0.0.",
                baseline,
            )
            return 0.0

        saved_ratio = (baseline - en) / baseline
        saved_ratio = max(0.0, min(1.0, saved_ratio))
        return acc * saved_ratio

    def combined_reward(
        self,
        accuracy: float,
        energy: float,
        alpha: Optional[float] = None,
        beta: Optional[float] = None,
    ) -> float:
        """Return ``alpha * accuracy + beta * negawatt_score(...)``."""
        acc = self._validate_accuracy(accuracy, raise_on_error=True)
        en = self._validate_energy(energy, raise_on_error=True)
        if acc is None or en is None:
            return 0.0

        a = self._validate_weight(
            "alpha", alpha, default=self._config.default_alpha
        )
        b = self._validate_weight(
            "beta", beta, default=self._config.default_beta
        )
        if a is None or b is None:
            return 0.0

        nw = self.negawatt_score(acc, en)
        return a * acc + b * nw

    def breakdown(
        self,
        accuracy: float,
        energy: float,
        alpha: Optional[float] = None,
        beta: Optional[float] = None,
    ) -> RewardBreakdown:
        """Return a structured decomposition of the reward computation."""
        acc = self._validate_accuracy(accuracy, raise_on_error=True)
        en = self._validate_energy(energy, raise_on_error=True)
        if acc is None or en is None:
            raise NegawattRewardError(
                "cannot produce a breakdown from invalid inputs."
            )

        a = self._validate_weight(
            "alpha", alpha, default=self._config.default_alpha
        )
        b = self._validate_weight(
            "beta", beta, default=self._config.default_beta
        )
        if a is None or b is None:
            raise NegawattRewardError("invalid blend weights.")

        baseline = self.baseline_energy
        saved_energy = max(0.0, baseline - en)
        saved_ratio = (
            saved_energy / baseline if baseline > 0 else 0.0
        )
        apw = self.accuracy_per_watt(acc, en)
        nw = self.negawatt_score(acc, en)
        combined = a * acc + b * nw

        return RewardBreakdown(
            accuracy=acc,
            energy=en,
            baseline_energy=baseline,
            saved_energy=saved_energy,
            saved_ratio=saved_ratio,
            accuracy_per_watt=apw,
            negawatt_score=nw,
            combined_reward=combined,
            alpha=a,
            beta=b,
        )

    # ---------------------------------------------------------- validation
    def _validate_accuracy(
        self, value: Any, *, raise_on_error: bool
    ) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"accuracy must be numeric, got {type(value).__name__}."
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"accuracy must be finite, got {value!r}."
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if not self._config.min_accuracy <= fv <= self._config.max_accuracy:
            msg = (
                f"accuracy must be in "
                f"[{self._config.min_accuracy}, {self._config.max_accuracy}], "
                f"got {fv}."
            )
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Clamping.", msg)
            return max(
                self._config.min_accuracy,
                min(self._config.max_accuracy, fv),
            )
        return fv

    def _validate_energy(
        self, value: Any, *, raise_on_error: bool
    ) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"energy must be numeric, got {type(value).__name__}."
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"energy must be finite, got {value!r}."
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv < self._config.min_energy:
            msg = f"energy must be >= {self._config.min_energy}, got {fv}."
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv > self._config.max_energy:
            msg = f"energy must be <= {self._config.max_energy}, got {fv}."
            if raise_on_error and self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Clamping to ceiling.", msg)
            return self._config.max_energy
        return fv

    def _validate_weight(
        self, name: str, value: Any, *, default: float
    ) -> Optional[float]:
        if value is None:
            return float(default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"{name} must be numeric, got {type(value).__name__}."
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using default %.3f.", msg, default)
            return float(default)
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"{name} must be finite, got {value!r}."
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using default %.3f.", msg, default)
            return float(default)
        if fv < 0:
            msg = f"{name} must be >= 0, got {fv}."
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning("%s Using default %.3f.", msg, default)
            return float(default)
        return fv

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the reward module state."""
        return {
            "baseline_energy": self.baseline_energy,
            "default_alpha": self._config.default_alpha,
            "default_beta": self._config.default_beta,
            "accuracy_range": (
                self._config.min_accuracy, self._config.max_accuracy,
            ),
            "energy_range": (
                self._config.min_energy, self._config.max_energy,
            ),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self._config.to_dict(),
            "strict": self._strict,
            "started_at": self._started_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NegawattReward":
        if not isinstance(data, Mapping):
            raise NegawattRewardError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = NegawattRewardConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        reward = cls(
            baseline_energy=cfg.baseline_energy,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )
        reward._started_at = float(data.get("started_at", time.time()))
        return reward

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "NegawattReward":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise NegawattRewardError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "NegawattReward":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "NegawattReward scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __call__(self, accuracy: float, energy: float) -> float:
        """Allow the reward module to be called as a function."""
        return self.combined_reward(accuracy, energy)

    def __repr__(self) -> str:
        return (
            "NegawattReward("
            f"baseline_energy={self.baseline_energy:.6g}, "
            f"alpha={self._config.default_alpha:.3f}, "
            f"beta={self._config.default_beta:.3f}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "NegawattReward",
    "NegawattRewardConfig",
    "NegawattRewardError",
    "RewardBreakdown",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m rewards.negawatt_reward
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path ------------------------------------------------ #
    reward = NegawattReward(baseline_energy=0.01)
    print("repr       :", reward)

    apw = reward.accuracy_per_watt(accuracy=0.95, energy=0.005)
    nw = reward.negawatt_score(accuracy=0.95, energy=0.005)
    combined = reward.combined_reward(accuracy=0.95, energy=0.005)
    print(f"apw        : {apw:.4f}")
    print(f"negawatt   : {nw:.4f}")
    print(f"combined   : {combined:.4f}")

    # ---- Breakdown ------------------------------------------------- #
    bd = reward.breakdown(accuracy=0.95, energy=0.005)
    print("breakdown  :", bd)

    # ---- Behavior checks ------------------------------------------ #
    assert nw > 0.0
    assert reward.negawatt_score(0.95, 0.02) == 0.0
    zero_base = NegawattReward(baseline_energy=0.0)
    assert zero_base.negawatt_score(0.95, 0.005) == 0.0
    assert reward.accuracy_per_watt(0.95, 0.0) == 0.0
    print("behavior   : OK")

    # ---- Bug fix: negative accuracy ------------------------------- #
    for bad in (-0.1, 1.5, float("nan"), float("inf"), "high", None):
        try:
            reward.negawatt_score(bad, 0.005)  # type: ignore[arg-type]
        except NegawattRewardError as exc:
            print("Rejected   :", exc)

    # ---- Bug fix: negative blend weights -------------------------- #
    for bad_w in (-1.0, float("nan"), float("inf"), "big"):
        try:
            reward.combined_reward(0.95, 0.005, beta=bad_w)  # type: ignore[arg-type]
        except NegawattRewardError as exc:
            print("Rejected   :", exc)

    # ---- Config validation --------------------------------------- #
    for bad_cfg in (
        dict(baseline_energy=-1.0),
        dict(baseline_energy=float("nan")),
        dict(default_alpha=-1.0),
        dict(min_accuracy=-0.1),
        dict(min_accuracy=0.9, max_accuracy=0.5),
        dict(min_energy=10.0, max_energy=5.0),
    ):
        try:
            NegawattRewardConfig(**bad_cfg)  # type: ignore[arg-type]
        except NegawattRewardError as exc:
            print("Rejected cfg:", exc)

    # ---- Non-strict coerces -------------------------------------- #
    lenient = NegawattReward(baseline_energy=0.01, strict=False)
    print(f"lenient    : apw={lenient.accuracy_per_watt(-0.5, -1.0):.4f}")

    # ---- Serialization round-trip -------------------------------- #
    payload = reward.to_json()
    restored = NegawattReward.from_json(payload)
    assert restored.to_dict() == reward.to_dict()
    print("Round-trip OK.")

    # ---- Callable protocol --------------------------------------- #
    assert reward(0.95, 0.005) == reward.combined_reward(0.95, 0.005)
    print("callable   : OK")

    # ---- Statistics ---------------------------------------------- #
    print("statistics :", {
        k: v for k, v in reward.statistics().items()
        if k != "uptime_seconds"
    })

    # ---- Context manager ---------------------------------------- #
    with NegawattReward(baseline_energy=0.01) as scoped:
        assert scoped.combined_reward(0.9, 0.005) > 0
    print("Context    : OK")

    print("\nSmoke test passed.")
