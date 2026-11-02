# src/r1/reward_normalizer.py

"""
Reward Normalizer
=================

Running mean / std normalization via Welford's online algorithm.

Design
------
- Frozen, validated :class:`RewardNormalizerConfig`: ``epsilon > 0``,
  ``min_count >= 1``, ``clip_range > 0`` or ``None`` — all **finite**
  (rejects ``NaN`` / ``Inf``; note ``NaN <= 0`` is ``False``, so the
  older checks silently passed on NaN).
- **Invalid input is skipped, not coerced.** In non-strict mode,
  :meth:`update` returns ``None`` for ``NaN`` / ``Inf`` / non-numeric
  rewards **without** touching ``count`` / ``mean`` / ``m2`` (the
  previous behaviour coerced to ``0.0`` and incremented ``count``,
  silently corrupting the running statistics).
- :meth:`normalize` returns ``0.0`` when ``count < min_count`` (no
  baseline yet — identity signal) and ``None`` for invalid input. This
  distinguishes "no data" from "bad data".
- :attr:`variance` clamps Welford's ``m2`` to ``>= 0`` before
  ``sqrt`` — floating-point rounding after millions of updates can
  produce a tiny negative ``m2``.
- Thread-safe: :meth:`update` / :meth:`normalize` / :meth:`reset` /
  :meth:`from_dict` perform their compound operations under an
  ``RLock``. Public attributes ``mean`` / ``m2`` / ``count`` remain
  plain attributes for backward compatibility.
- Accepts ``Decimal``, ``Fraction``, NumPy scalars (via
  ``numbers.Real``).
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``from_json``. ``from_dict`` validates every field; malformed
  payloads raise :class:`RewardNormalizerError`.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "RewardNormalizer",
    "RewardNormalizerConfig",
    "RewardNormalizerError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RewardNormalizerError(ValueError):
    """Raised for invalid normalizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _coerce_finite(value: Any, *, name: str) -> float:
    """Validate a config number: real, finite, non-bool."""
    if isinstance(value, bool):
        raise RewardNormalizerError(
            f"{name} must be a real number, got bool."
        )
    if not isinstance(value, (numbers.Real, Decimal)):
        raise RewardNormalizerError(
            f"{name} must be a real number, "
            f"got {type(value).__name__}."
        )
    try:
        fv = float(value)
    except (TypeError, ValueError) as exc:
        raise RewardNormalizerError(
            f"{name} not convertible to float: {exc}"
        ) from exc
    if not math.isfinite(fv):
        raise RewardNormalizerError(
            f"{name} must be finite, got {value!r}."
        )
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RewardNormalizerConfig:
    """Tunable parameters for the reward normalizer."""

    epsilon: float = 1e-8
    min_count: int = 2
    clip_range: Optional[float] = 10.0

    def __post_init__(self) -> None:
        # ---- epsilon (finite, > 0) --------------------------------
        eps = _coerce_finite(self.epsilon, name="epsilon")
        if eps <= 0:
            raise RewardNormalizerError(
                f"epsilon must be > 0, got {eps}."
            )
        object.__setattr__(self, "epsilon", eps)

        # ---- min_count (int, >= 1) --------------------------------
        if isinstance(self.min_count, bool) or not isinstance(
            self.min_count, int
        ):
            raise RewardNormalizerError(
                "min_count must be an int, "
                f"got {type(self.min_count).__name__}."
            )
        if self.min_count < 1:
            raise RewardNormalizerError(
                f"min_count must be >= 1, got {self.min_count}."
            )

        # ---- clip_range (finite, > 0, or None) --------------------
        if self.clip_range is not None:
            cr = _coerce_finite(self.clip_range, name="clip_range")
            if cr <= 0:
                raise RewardNormalizerError(
                    f"clip_range must be > 0 or None, got {cr}."
                )
            object.__setattr__(self, "clip_range", cr)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardNormalizerConfig":
        if not isinstance(data, Mapping):
            raise RewardNormalizerError(
                f"RewardNormalizerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                epsilon=data.get("epsilon", 1e-8),
                min_count=int(data.get("min_count", 2)),
                clip_range=data.get("clip_range", 10.0),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, RewardNormalizerError):
                raise
            raise RewardNormalizerError(
                f"invalid RewardNormalizerConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Normalizer
# --------------------------------------------------------------------------- #
class RewardNormalizer:
    """
    Welford's online algorithm for running mean / std normalization.

    Public attributes ``mean``, ``m2``, ``count`` are preserved for
    backward compatibility. They are mutated atomically inside
    :meth:`update` (under an internal lock) but reading them directly
    outside a lock is not synchronised with concurrent updates — for
    exactly-consistent reads, use :meth:`statistics`.
    """

    def __init__(
        self,
        *,
        config: Optional[RewardNormalizerConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or RewardNormalizerConfig()
        self._strict = bool(strict)

        # Public state (kept as plain attributes for compat).
        self.mean: float = 0.0
        self.m2: float = 0.0
        self.count: int = 0

        # Counters (per-instance / per-reset).
        self._updates: int = 0
        self._skipped: int = 0

        # Serialises compound mutations. Reading public attributes
        # outside a lock is allowed but not guaranteed consistent with
        # concurrent updates (documented above).
        self._lock = threading.RLock()

        logger.debug(
            "RewardNormalizer initialized "
            "(epsilon=%.1e, min_count=%d, clip=%s, strict=%s)",
            self._config.epsilon,
            self._config.min_count,
            self._config.clip_range,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RewardNormalizerConfig:
        return self._config

    @property
    def variance(self) -> float:
        with self._lock:
            if self.count < 2:
                return 0.0
            v = self.m2 / self.count
            # Welford's M2 is theoretically non-negative; rounding can
            # produce a tiny negative. Clamp so sqrt() never raises.
            if v < 0.0:
                if v < -self._config.epsilon:
                    logger.warning(
                        "negative variance %.3e from m2=%.3e; "
                        "clamped to 0.",
                        v, self.m2,
                    )
                return 0.0
            return v

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    # ------------------------------------------------------------------ update
    def update(self, reward: Any) -> Optional[int]:
        """
        Update the running statistics with one reward.

        Returns
        -------
        Optional[int]
            The new ``count`` on success, or ``None`` when the reward
            was rejected in non-strict mode (``count``, ``mean``,
            ``m2`` are **not** mutated in that case).
        """
        r = self._validate_reward(reward)
        if r is None:
            with self._lock:
                self._skipped += 1
            return None

        with self._lock:
            self.count += 1
            delta = r - self.mean
            self.mean += delta / self.count
            self.m2 += delta * (r - self.mean)
            self._updates += 1
            return self.count

    def normalize(self, reward: Any) -> Optional[float]:
        """
        Return the z-score of ``reward`` using the current statistics.

        Returns
        -------
        Optional[float]
            - ``0.0`` when ``count < min_count`` (no baseline yet).
            - ``None`` when the reward is invalid in non-strict mode.
            - The clipped z-score otherwise.

        Raises
        ------
        RewardNormalizerError
            In strict mode, on invalid reward.
        """
        r = self._validate_reward(reward)
        if r is None:
            return None
        with self._lock:
            if self.count < self._config.min_count:
                return 0.0
            variance = self.m2 / self.count
            if variance < 0.0:
                variance = 0.0
            std = math.sqrt(variance) + self._config.epsilon
            normalized = (r - self.mean) / std
            if self._config.clip_range is not None:
                normalized = max(
                    -self._config.clip_range,
                    min(self._config.clip_range, normalized),
                )
            return normalized

    # ------------------------------------------------------------------ helpers
    def _validate_reward(self, reward: Any) -> Optional[float]:
        """
        Return the coerced float, or ``None`` (non-strict) / raise (strict).
        """
        if isinstance(reward, bool) or not isinstance(
            reward, (numbers.Real, Decimal)
        ):
            msg = (
                f"reward must be a real number, "
                f"got {type(reward).__name__}."
            )
            if self._strict:
                raise RewardNormalizerError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        try:
            fv = float(reward)
        except (TypeError, ValueError) as exc:
            msg = f"reward not convertible to float: {exc}."
            if self._strict:
                raise RewardNormalizerError(msg) from exc
            logger.warning("%s Returning None.", msg)
            return None
        if not math.isfinite(fv):
            msg = f"reward must be finite, got {reward!r}."
            if self._strict:
                raise RewardNormalizerError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        return fv

    # ------------------------------------------------------------------ lifecycle
    def reset(self) -> None:
        """Clear the running statistics and counters."""
        with self._lock:
            self.mean = 0.0
            self.m2 = 0.0
            self.count = 0
            self._updates = 0
            self._skipped = 0

    # ------------------------------------------------------------------ statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "count": self.count,
                "mean": self.mean,
                "m2": self.m2,
                "variance": self.variance,
                "std": self.std,
                "updates": self._updates,
                "skipped_updates": self._skipped,
                "strict": self._strict,
            }

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": VERSION,
                "config": asdict(self._config),
                "strict": self._strict,
                "mean": self.mean,
                "m2": self.m2,
                "count": self.count,
                "counters": {
                    "updates": self._updates,
                    "skipped_updates": self._skipped,
                },
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardNormalizer":
        if not isinstance(data, Mapping):
            raise RewardNormalizerError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )

        cfg = RewardNormalizerConfig.from_dict(
            dict(data.get("config", {}) or {})
        )

        strict_raw = data.get("strict", True)
        if not isinstance(strict_raw, bool):
            raise RewardNormalizerError(
                f"strict must be bool, got "
                f"{type(strict_raw).__name__}."
            )

        try:
            mean = float(data.get("mean", 0.0))
            m2 = float(data.get("m2", 0.0))
            count = int(data.get("count", 0))
        except (TypeError, ValueError) as exc:
            raise RewardNormalizerError(
                f"invalid state payload: {exc}"
            ) from exc

        if not math.isfinite(mean):
            raise RewardNormalizerError(
                f"mean must be finite, got {mean!r}."
            )
        if not math.isfinite(m2):
            raise RewardNormalizerError(
                f"m2 must be finite, got {m2!r}."
            )
        if m2 < 0:
            raise RewardNormalizerError(
                f"m2 must be >= 0, got {m2!r}."
            )
        if count < 0:
            raise RewardNormalizerError(
                f"count must be >= 0, got {count}."
            )

        norm = cls(config=cfg, strict=strict_raw)

        counters = dict(data.get("counters", {}) or {})
        try:
            updates = int(counters.get("updates", 0))
            skipped = int(counters.get("skipped_updates", 0))
        except (TypeError, ValueError) as exc:
            raise RewardNormalizerError(
                f"invalid counter: {exc}"
            ) from exc
        if updates < 0 or skipped < 0:
            raise RewardNormalizerError(
                "counters must be non-negative."
            )

        with norm._lock:
            norm.mean = mean
            norm.m2 = m2
            norm.count = count
            norm._updates = updates
            norm._skipped = skipped

        return norm

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "RewardNormalizer":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RewardNormalizerError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ------------------------------------------------------------------ dunder
    def __repr__(self) -> str:
        return (
            "RewardNormalizer("
            f"count={self.count}, "
            f"mean={self.mean:.4f}, "
            f"std={self.std:.4f}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    from fractions import Fraction as F

    logging.basicConfig(level=logging.INFO)
    norm = RewardNormalizer()
    print("repr       :", norm)
    print("VERSION    :", VERSION)

    for r in [1.0, 2.0, 3.0, 4.0, 5.0]:
        n = norm.update(r)
        assert n is not None
    print("stats      :", {
        k: v for k, v in norm.statistics().items() if k != "strict"
    })
    z = norm.normalize(3.0)
    print("normalize 3:", z)
    assert z is not None and -1.0 < z < 1.0

    # ---- Strict raises; non-strict skips state ------------------ #
    try:
        norm.update(float("nan"))
    except RewardNormalizerError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict: state untouched on invalid ------------- #
    lenient = RewardNormalizer(strict=False)
    lenient.update(1.0)
    lenient.update(2.0)
    before = (lenient.count, lenient.mean, lenient.m2)
    skipped = lenient.update(float("nan"))
    after = (lenient.count, lenient.mean, lenient.m2)
    print("skip stats :", "unchanged" if before == after else "CHANGED")
    assert skipped is None
    assert before == after
    assert lenient.statistics()["skipped_updates"] == 1

    # normalize invalid → None (was: z-score of 0.0)
    assert lenient.normalize(float("nan")) is None
    print("normalize invalid: None")

    # normalize valid but no baseline → 0.0 (unchanged)
    fresh = RewardNormalizer(
        config=RewardNormalizerConfig(min_count=5),
        strict=False,
    )
    assert fresh.normalize(1.0) == 0.0
    print("normalize fresh  : 0.0 (no baseline)")

    # ---- Config validation: NaN / Inf / bool ---------------- #
    for bad in (
        dict(epsilon=float("nan")),
        dict(epsilon=float("inf")),
        dict(epsilon=0.0),
        dict(epsilon=True),
        dict(min_count=0),
        dict(min_count=True),
        dict(min_count=2.5),
        dict(clip_range=float("nan")),
        dict(clip_range=-1.0),
        dict(clip_range=0.0),
    ):
        try:
            RewardNormalizerConfig(**bad)  # type: ignore[arg-type]
        except RewardNormalizerError as exc:
            print("Rejected cfg:", exc)

    # ---- Numbers with Decimal / Fraction -------------------- #
    numeric = RewardNormalizer()
    numeric.update(Decimal("1.5"))
    numeric.update(F(3, 2))
    print("numeric    :", numeric.statistics()["count"], "updates accepted")

    # ---- from_dict validation ------------------------------ #
    # Round-trip is lossless
    payload = norm.to_dict()
    assert payload["version"] == VERSION
    rt = RewardNormalizer.from_dict(payload)
    assert rt.to_dict() == norm.to_dict()
    print("Round-trip OK.")

    # Corrupted state → clean errors
    for bad_payload in (
        {"config": {}, "mean": float("nan")},
        {"config": {}, "m2": float("inf")},
        {"config": {}, "m2": -1.0},
        {"config": {}, "count": -1},
        {"config": {}, "strict": "yes"},
        {"config": {"epsilon": float("nan")}},
        {"config": {"min_count": 0}},
        {"config": {"clip_range": -1.0}},
    ):
        try:
            RewardNormalizer.from_dict(bad_payload)
        except RewardNormalizerError as exc:
            print("Rejected payload:", exc)

    # ---- to_json rejects NaN (defensive) ------------------- #
    tainted = RewardNormalizer()
    tainted.mean = float("nan")
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN in to_json rejected:", exc)

    # ---- variance clamps tiny negative m2 ------------------ #
    clamped = RewardNormalizer()
    clamped.update(1.0)
    clamped.update(2.0)
    clamped.m2 = -1e-15  # simulate float rounding
    assert clamped.variance == 0.0
    assert clamped.std == 0.0
    print("clamp      : OK (negative m2 clamped)")

    # ---- reset clears state AND counters ------------------ #
    norm.reset()
    st = norm.statistics()
    assert st["count"] == 0 and st["updates"] == 0
    assert st["skipped_updates"] == 0
    print("reset      : OK")

    print("\nSmoke test passed.")
