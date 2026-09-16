# src/r1/reward_normalizer.py

"""
Reward Normalizer
=================

Running mean/std normalization (Welford's online algorithm) for RL rewards.

Enhancements
------------
- Fixed ``count`` being a float (``1e-4``) then ``+= 1`` — now ``int``.
- Renamed ``var`` → ``m2`` (Welford's sum of squared deltas).
- ``RewardNormalizerConfig`` with validated ``epsilon`` and ``min_count``.
- Full validation of every reward; strict / non-strict modes.
- ``statistics()``, serialization, ``__repr__``, ``__main__`` smoke test.
"""

from __future__ import annotations

import json, logging, math
from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


class RewardNormalizerError(ValueError):
    """Raised for invalid normalizer inputs or configuration."""


@dataclass(frozen=True)
class RewardNormalizerConfig:
    """Tunable parameters for the reward normalizer."""
    epsilon: float = 1e-8
    min_count: int = 2
    clip_range: Optional[float] = 10.0

    def __post_init__(self) -> None:
        if self.epsilon <= 0:
            raise RewardNormalizerError("epsilon must be > 0.")
        if self.min_count < 1:
            raise RewardNormalizerError("min_count must be >= 1.")
        if self.clip_range is not None and self.clip_range <= 0:
            raise RewardNormalizerError("clip_range must be > 0 or None.")


class RewardNormalizer:
    """
    Welford's online algorithm for running mean/std normalization.

    The original ``update(reward)`` and ``normalize(reward)`` signatures are
    preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[RewardNormalizerConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or RewardNormalizerConfig()
        self._strict = bool(strict)
        self.mean: float = 0.0
        self.m2: float = 0.0           # renamed from ``var`` (was Welford's M2)
        self.count: int = 0            # fixed: was ``1e-4`` (float)
        logger.debug(
            "RewardNormalizer initialized (epsilon=%.1e, clip=%s, strict=%s)",
            self._config.epsilon, self._config.clip_range, self._strict,
        )

    @property
    def config(self) -> RewardNormalizerConfig:
        return self._config

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / self.count

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    def update(self, reward: float) -> None:
        """Update the running statistics with one reward."""
        r = self._validate_reward(reward)
        self.count += 1
        delta = r - self.mean
        self.mean += delta / self.count
        self.m2 += delta * (r - self.mean)

    def normalize(self, reward: float) -> float:
        """Return the z-score of ``reward`` using the current statistics."""
        r = self._validate_reward(reward)
        if self.count < self._config.min_count:
            return 0.0
        std = math.sqrt(self.m2 / self.count) + self._config.epsilon
        normalized = (r - self.mean) / std
        if self._config.clip_range is not None:
            normalized = max(
                -self._config.clip_range,
                min(self._config.clip_range, normalized),
            )
        return normalized

    def _validate_reward(self, reward: Any) -> float:
        if isinstance(reward, bool) or not isinstance(reward, (int, float)):
            msg = f"reward must be numeric, got {type(reward).__name__}."
            if self._strict:
                raise RewardNormalizerError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(reward)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._strict:
                raise RewardNormalizerError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fv

    def statistics(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "mean": self.mean,
            "variance": self.variance,
            "std": self.std,
            "m2": self.m2,
        }

    def reset(self) -> None:
        self.mean = 0.0
        self.m2 = 0.0
        self.count = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": asdict(self._config),
            "strict": self._strict,
            "mean": self.mean,
            "m2": self.m2,
            "count": self.count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardNormalizer":
        cfg_data = dict(data.get("config", {}) or {})
        cfg = RewardNormalizerConfig(
            epsilon=float(cfg_data.get("epsilon", 1e-8)),
            min_count=int(cfg_data.get("min_count", 2)),
            clip_range=cfg_data.get("clip_range", 10.0),
        )
        norm = cls(config=cfg, strict=bool(data.get("strict", True)))
        norm.mean = float(data.get("mean", 0.0))
        norm.m2 = float(data.get("m2", 0.0))
        norm.count = int(data.get("count", 0))
        return norm

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "RewardNormalizer("
            f"count={self.count}, mean={self.mean:.4f}, std={self.std:.4f}, "
            f"strict={self._strict})"
        )


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    norm = RewardNormalizer()
    print("repr       :", norm)

    for r in [1.0, 2.0, 3.0, 4.0, 5.0]:
        norm.update(r)
    print("stats      :", norm.statistics())
    print("normalize 3:", norm.normalize(3.0))

    # Validation.
    try:
        norm.update(float("nan"))
    except RewardNormalizerError as exc:
        print("Rejected   :", exc)

    payload = norm.to_json()
    restored = RewardNormalizer.from_dict(json.loads(payload))
    assert restored.to_dict() == norm.to_dict()
    print("Round-trip OK.")
