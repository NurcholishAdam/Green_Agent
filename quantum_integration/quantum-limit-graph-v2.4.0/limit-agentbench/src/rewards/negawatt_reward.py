# src/rewards/negawatt_reward.py

"""
Negawatt Reward Shaping
=======================

Sustainability-oriented reward shaping for reinforcement learning and
benchmarking pipelines:

- ``accuracy_per_watt``  — classical efficiency metric.
- ``negawatt_score``     — relative energy savings, weighted by accuracy.
- ``combined_reward``    — linear blend of accuracy and negawatt score.

Design
------
- **Validators return ``None`` on invalid input** (type mismatch,
  ``NaN``/``Inf``) in non-strict mode — matching their
  ``Optional[float]`` type hints. Callers that need to distinguish
  "coerced to zero" from "successfully parsed" now can.
- **``breakdown()`` requires valid inputs** even in non-strict mode —
  it raises :class:`NegawattRewardError` rather than producing a
  fabricated decomposition from zeroed-out values.
- **Clamping and capping are visible.** ``RewardBreakdown`` records
  whether accuracy was clamped, energy was clamped, and whether
  ``accuracy_per_watt`` was capped by the energy floor.
- **Excess energy is reported.** When a caller uses more than
  ``baseline_energy``, ``RewardBreakdown.excess_energy`` exposes how
  much. ``saved_energy``/``saved_ratio`` are clamped to ``>= 0`` (the
  reward is never penalised), but the excess is available for
  dashboards.
- **Zero-baseline is signalled.** ``RewardBreakdown.metadata``
  includes ``baseline_zero: True`` when ``baseline_energy == 0``.
- **Serialization is strict** — ``to_json(allow_nan=False)``;
  every ``from_dict`` wraps errors in :class:`NegawattRewardError` and
  validates ``strict`` is ``bool``.
- **Counters.** Per-instance counters track calls, mean reward, and
  accumulated energy for telemetry. ``statistics()`` no longer
  includes ``uptime_seconds`` by default.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "NegawattReward",
    "NegawattRewardConfig",
    "NegawattRewardError",
    "RewardBreakdown",
]

VERSION = "2.0.0"


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

    min_accuracy: float = 0.0
    max_accuracy: float = 1.0
    min_energy: float = 0.0
    max_energy: float = 1e12

    def __post_init__(self) -> None:
        for name in (
            "baseline_energy", "default_alpha", "default_beta",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
            ):
                raise NegawattRewardError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise NegawattRewardError(
                    f"{name} must be finite and >= 0, "
                    f"got {value!r}."
                )
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
        if not math.isfinite(float(self.max_energy)):
            raise NegawattRewardError(
                "max_energy must be finite."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "NegawattRewardConfig":
        if not isinstance(data, Mapping):
            raise NegawattRewardError(
                f"NegawattRewardConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            unknown = set(data) - valid
            if unknown:
                logger.debug(
                    "NegawattRewardConfig.from_dict dropping "
                    "unknown fields: %s", sorted(unknown),
                )
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, NegawattRewardError):
                raise
            raise NegawattRewardError(
                f"invalid NegawattRewardConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Breakdown record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RewardBreakdown:
    """
    Immutable, structured decomposition of a reward computation.

    ``saved_energy`` / ``saved_ratio`` are clamped to ``>= 0`` — a
    caller who uses more energy than the baseline gets no negative
    savings (the reward is not penalised) but sees the actual excess
    in ``excess_energy``.
    """

    accuracy: float
    energy: float
    baseline_energy: float
    saved_energy: float
    saved_ratio: float
    excess_energy: float
    accuracy_per_watt: float
    negawatt_score: float
    combined_reward: float
    alpha: float
    beta: float
    accuracy_clamped: bool = False
    energy_clamped: bool = False
    apw_energy_capped: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in (
            "accuracy", "energy", "baseline_energy",
            "saved_energy", "saved_ratio",
            "excess_energy", "accuracy_per_watt",
            "negawatt_score", "combined_reward",
            "alpha", "beta",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
            ):
                raise NegawattRewardError(
                    f"{name} must be finite and numeric, "
                    f"got {value!r}."
                )
        if self.accuracy < 0:
            raise NegawattRewardError(
                "accuracy must be >= 0."
            )
        if self.energy < 0:
            raise NegawattRewardError("energy must be >= 0.")
        if self.baseline_energy < 0:
            raise NegawattRewardError(
                "baseline_energy must be >= 0."
            )
        if self.saved_energy < 0:
            raise NegawattRewardError(
                "saved_energy must be >= 0."
            )
        if self.excess_energy < 0:
            raise NegawattRewardError(
                "excess_energy must be >= 0."
            )
        if not 0.0 <= self.saved_ratio <= 1.0:
            raise NegawattRewardError(
                f"saved_ratio must be in [0, 1], "
                f"got {self.saved_ratio}."
            )
        if self.accuracy_per_watt < 0:
            raise NegawattRewardError(
                "accuracy_per_watt must be >= 0."
            )
        if self.negawatt_score < 0:
            raise NegawattRewardError(
                "negawatt_score must be >= 0."
            )
        if self.alpha < 0 or self.beta < 0:
            raise NegawattRewardError(
                "alpha and beta must be >= 0."
            )
        for name in (
            "accuracy_clamped", "energy_clamped",
            "apw_energy_capped",
        ):
            if not isinstance(getattr(self, name), bool):
                raise NegawattRewardError(
                    f"{name} must be a bool."
                )
        if not isinstance(self.metadata, Mapping):
            raise NegawattRewardError(
                "metadata must be a Mapping."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accuracy": self.accuracy,
            "energy": self.energy,
            "baseline_energy": self.baseline_energy,
            "saved_energy": self.saved_energy,
            "saved_ratio": self.saved_ratio,
            "excess_energy": self.excess_energy,
            "accuracy_per_watt": self.accuracy_per_watt,
            "negawatt_score": self.negawatt_score,
            "combined_reward": self.combined_reward,
            "alpha": self.alpha,
            "beta": self.beta,
            "accuracy_clamped": self.accuracy_clamped,
            "energy_clamped": self.energy_clamped,
            "apw_energy_capped": self.apw_energy_capped,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "RewardBreakdown":
        if not isinstance(data, Mapping):
            raise NegawattRewardError(
                "RewardBreakdown.from_dict expects a Mapping."
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

            metadata = data.get("metadata") or {}
            if not isinstance(metadata, Mapping):
                raise NegawattRewardError(
                    "metadata must be a Mapping."
                )

            def _b(key: str) -> bool:
                v = data.get(key, False)
                if not isinstance(v, bool):
                    raise NegawattRewardError(
                        f"{key} must be a bool, got "
                        f"{type(v).__name__}."
                    )
                return v

            return cls(
                accuracy=float(data["accuracy"]),
                energy=float(data["energy"]),
                baseline_energy=float(
                    data["baseline_energy"]
                ),
                saved_energy=float(data["saved_energy"]),
                saved_ratio=float(data["saved_ratio"]),
                excess_energy=float(
                    data.get("excess_energy", 0.0)
                ),
                accuracy_per_watt=float(
                    data["accuracy_per_watt"]
                ),
                negawatt_score=float(
                    data["negawatt_score"]
                ),
                combined_reward=float(
                    data["combined_reward"]
                ),
                alpha=float(data["alpha"]),
                beta=float(data["beta"]),
                accuracy_clamped=_b("accuracy_clamped"),
                energy_clamped=_b("energy_clamped"),
                apw_energy_capped=_b("apw_energy_capped"),
                metadata=dict(metadata),
                timestamp=timestamp,
            )
        except KeyError as exc:
            raise NegawattRewardError(
                f"RewardBreakdown payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, NegawattRewardError):
                raise
            raise NegawattRewardError(
                f"invalid RewardBreakdown payload: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "RewardBreakdown":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise NegawattRewardError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

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

    Thread-safe, serializable, validation-first.
    """

    def __init__(
        self,
        baseline_energy: float = 1.0,
        *,
        config: Optional[NegawattRewardConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(strict, bool):
            raise NegawattRewardError(
                f"strict must be bool, got "
                f"{type(strict).__name__}."
            )
        self._strict = strict

        if config is not None:
            if not isinstance(config, NegawattRewardConfig):
                raise NegawattRewardError(
                    f"config must be a NegawattRewardConfig, got "
                    f"{type(config).__name__}."
                )
            self._config = config
        else:
            if (
                isinstance(baseline_energy, bool)
                or not isinstance(baseline_energy, numbers.Real)
            ):
                raise NegawattRewardError(
                    f"baseline_energy must be numeric, got "
                    f"{type(baseline_energy).__name__}."
                )
            fv = float(baseline_energy)
            if not math.isfinite(fv) or fv < 0:
                raise NegawattRewardError(
                    f"baseline_energy must be finite and >= 0, "
                    f"got {baseline_energy!r}."
                )
            self._config = NegawattRewardConfig(
                baseline_energy=fv,
            )

        self.baseline_energy: float = (
            self._config.baseline_energy
        )
        self._lock = threading.RLock()
        self._started_at: float = time.time()

        # Telemetry counters.
        self._combined_calls: int = 0
        self._combined_reward_sum: float = 0.0
        self._energy_sum: float = 0.0
        self._accuracy_sum: float = 0.0
        self._invalid_inputs: int = 0

        logger.debug(
            "NegawattReward initialized "
            "(baseline_energy=%.6g, alpha_default=%.3f, "
            "beta_default=%.3f, strict=%s)",
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
    def accuracy_per_watt(
        self, accuracy: float, energy: float,
    ) -> float:
        """
        Return ``accuracy / energy`` (higher is better).

        Returns ``0.0`` when energy is at or below the floor. Callers
        can distinguish "zero energy" from "zero accuracy" by checking
        :meth:`breakdown`, which sets ``apw_energy_capped=True`` for
        the former case.
        """
        acc = self._validate_accuracy(accuracy)
        en = self._validate_energy(energy)
        if acc is None or en is None:
            return 0.0
        return self._accuracy_per_watt_unchecked(acc, en)

    def negawatt_score(
        self, accuracy: float, energy: float,
    ) -> float:
        """Return ``accuracy × max(0, (baseline − energy) / baseline)``."""
        acc = self._validate_accuracy(accuracy)
        en = self._validate_energy(energy)
        if acc is None or en is None:
            return 0.0
        return self._negawatt_unchecked(acc, en)

    def combined_reward(
        self,
        accuracy: float,
        energy: float,
        alpha: Optional[float] = None,
        beta: Optional[float] = None,
    ) -> float:
        """Return ``alpha * accuracy + beta * negawatt_score(...)``."""
        acc = self._validate_accuracy(accuracy)
        en = self._validate_energy(energy)
        if acc is None or en is None:
            return 0.0
        a = self._validate_weight(
            "alpha", alpha, default=self._config.default_alpha,
        )
        b = self._validate_weight(
            "beta", beta, default=self._config.default_beta,
        )
        reward = a * acc + b * self._negawatt_unchecked(acc, en)
        with self._lock:
            self._combined_calls += 1
            self._combined_reward_sum += reward
            self._energy_sum += en
            self._accuracy_sum += acc
        return reward

    def breakdown(
        self,
        accuracy: float,
        energy: float,
        alpha: Optional[float] = None,
        beta: Optional[float] = None,
    ) -> RewardBreakdown:
        """
        Return a structured decomposition of the reward computation.

        Unlike :meth:`combined_reward`, this **requires valid inputs**
        even in non-strict mode — a fabricated decomposition from
        zeroed-out values would be misleading.
        """
        acc, acc_clamped = self._validate_accuracy_with_meta(
            accuracy
        )
        if acc is None:
            with self._lock:
                self._invalid_inputs += 1
            raise NegawattRewardError(
                f"cannot produce a breakdown from invalid accuracy: "
                f"{accuracy!r}."
            )
        en, en_clamped = self._validate_energy_with_meta(energy)
        if en is None:
            with self._lock:
                self._invalid_inputs += 1
            raise NegawattRewardError(
                f"cannot produce a breakdown from invalid energy: "
                f"{energy!r}."
            )
        a = self._validate_weight(
            "alpha", alpha, default=self._config.default_alpha,
        )
        b = self._validate_weight(
            "beta", beta, default=self._config.default_beta,
        )

        baseline = self.baseline_energy
        apw, apw_capped = self._accuracy_per_watt_with_meta(
            acc, en,
        )
        nw = self._negawatt_unchecked(acc, en)
        combined = a * acc + b * nw

        baseline_zero = baseline <= 0.0
        delta = baseline - en
        saved_energy = max(0.0, delta)
        excess_energy = max(0.0, -delta)
        saved_ratio = (
            min(1.0, saved_energy / baseline)
            if not baseline_zero else 0.0
        )

        return RewardBreakdown(
            accuracy=acc,
            energy=en,
            baseline_energy=baseline,
            saved_energy=saved_energy,
            saved_ratio=saved_ratio,
            excess_energy=excess_energy,
            accuracy_per_watt=apw,
            negawatt_score=nw,
            combined_reward=combined,
            alpha=a,
            beta=b,
            accuracy_clamped=acc_clamped,
            energy_clamped=en_clamped,
            apw_energy_capped=apw_capped,
            metadata={
                "baseline_zero": baseline_zero,
                "accuracy_validated": True,
                "energy_validated": True,
            },
        )

    # ---------------------------------------------------------- core math
    def _accuracy_per_watt_unchecked(
        self, accuracy: float, energy: float,
    ) -> float:
        if energy <= self._config.min_energy:
            return 0.0
        return accuracy / energy

    def _accuracy_per_watt_with_meta(
        self, accuracy: float, energy: float,
    ) -> Tuple[float, bool]:
        if energy <= self._config.min_energy:
            return 0.0, True
        return accuracy / energy, False

    def _negawatt_unchecked(
        self, accuracy: float, energy: float,
    ) -> float:
        baseline = self.baseline_energy
        if baseline <= 0.0:
            return 0.0
        saved_ratio = (baseline - energy) / baseline
        # Defensive clamp — with validated inputs ``energy >= 0`` bounds
        # ``saved_ratio <= 1``; ``max(0, ...)`` handles energy > baseline.
        saved_ratio = max(0.0, min(1.0, saved_ratio))
        return accuracy * saved_ratio

    # ---------------------------------------------------------- validation
    def _validate_finite(
        self, name: str, value: Any,
    ) -> Optional[float]:
        """
        Return ``float(value)`` when finite; ``None`` when invalid
        (in non-strict mode); raise in strict mode.
        """
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real,
        ):
            msg = (
                f"{name} must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning(
                "%s Returning None (non-strict).", msg,
            )
            return None
        fv = float(value)
        if not math.isfinite(fv):
            msg = f"{name} must be finite, got {value!r}."
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning(
                "%s Returning None (non-strict).", msg,
            )
            return None
        return fv

    def _validate_accuracy(
        self, value: Any,
    ) -> Optional[float]:
        fv, _ = self._validate_accuracy_with_meta(value)
        return fv

    def _validate_accuracy_with_meta(
        self, value: Any,
    ) -> Tuple[Optional[float], bool]:
        fv = self._validate_finite("accuracy", value)
        if fv is None:
            return None, False
        lo = self._config.min_accuracy
        hi = self._config.max_accuracy
        if fv < lo:
            if self._strict:
                raise NegawattRewardError(
                    f"accuracy must be in [{lo}, {hi}], got {fv}."
                )
            logger.warning(
                "accuracy %.3f below range; clamping to %.3f.",
                fv, lo,
            )
            return lo, True
        if fv > hi:
            if self._strict:
                raise NegawattRewardError(
                    f"accuracy must be in [{lo}, {hi}], got {fv}."
                )
            logger.warning(
                "accuracy %.3f above range; clamping to %.3f.",
                fv, hi,
            )
            return hi, True
        return fv, False

    def _validate_energy(
        self, value: Any,
    ) -> Optional[float]:
        fv, _ = self._validate_energy_with_meta(value)
        return fv

    def _validate_energy_with_meta(
        self, value: Any,
    ) -> Tuple[Optional[float], bool]:
        fv = self._validate_finite("energy", value)
        if fv is None:
            return None, False
        lo = self._config.min_energy
        hi = self._config.max_energy
        if fv < lo:
            if self._strict:
                raise NegawattRewardError(
                    f"energy must be >= {lo}, got {fv}."
                )
            logger.warning(
                "energy %.6g below floor %.6g; clamping to %.6g.",
                fv, lo, lo,
            )
            return lo, True
        if fv > hi:
            if self._strict:
                raise NegawattRewardError(
                    f"energy must be <= {hi}, got {fv}."
                )
            logger.warning(
                "energy %.6g above ceiling %.6g; clamping to %.6g.",
                fv, hi, hi,
            )
            return hi, True
        return fv, False

    def _validate_weight(
        self, name: str, value: Any, *, default: float,
    ) -> float:
        if value is None:
            return float(default)
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real,
        ):
            msg = (
                f"{name} must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning(
                "%s Using default %.3f.", msg, default,
            )
            return float(default)
        fv = float(value)
        if not math.isfinite(fv) or fv < 0:
            msg = (
                f"{name} must be finite and >= 0, got {value!r}."
            )
            if self._strict:
                raise NegawattRewardError(msg)
            logger.warning(
                "%s Using default %.3f.", msg, default,
            )
            return float(default)
        return fv

    # ---------------------------------------------------------- statistics
    def statistics(
        self, *, include_uptime: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            calls = self._combined_calls
            reward_sum = self._combined_reward_sum
            energy_sum = self._energy_sum
            accuracy_sum = self._accuracy_sum
            invalid = self._invalid_inputs
            uptime = time.time() - self._started_at

        payload: Dict[str, Any] = {
            "baseline_energy": self.baseline_energy,
            "default_alpha": self._config.default_alpha,
            "default_beta": self._config.default_beta,
            "accuracy_range": [
                self._config.min_accuracy,
                self._config.max_accuracy,
            ],
            "energy_range": [
                self._config.min_energy,
                self._config.max_energy,
            ],
            "strict": self._strict,
            "combined_calls": calls,
            "mean_combined_reward": (
                reward_sum / calls if calls > 0 else 0.0
            ),
            "mean_energy": (
                energy_sum / calls if calls > 0 else 0.0
            ),
            "mean_accuracy": (
                accuracy_sum / calls if calls > 0 else 0.0
            ),
            "invalid_inputs": invalid,
        }
        if include_uptime:
            payload["uptime_seconds"] = uptime
        return payload

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            counters = {
                "combined_calls": self._combined_calls,
                "combined_reward_sum": (
                    self._combined_reward_sum
                ),
                "energy_sum": self._energy_sum,
                "accuracy_sum": self._accuracy_sum,
                "invalid_inputs": self._invalid_inputs,
            }
        return {
            "version": VERSION,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "started_at": self._started_at,
            "counters": counters,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "NegawattReward":
        if not isinstance(data, Mapping):
            raise NegawattRewardError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise NegawattRewardError(
                    "config must be a Mapping."
                )
            cfg = NegawattRewardConfig.from_dict(
                dict(raw_config)
            )
            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise NegawattRewardError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )
            reward = cls(
                baseline_energy=cfg.baseline_energy,
                config=cfg,
                strict=strict_raw,
            )

            raw_started = data.get("started_at")
            if raw_started is not None:
                try:
                    started = float(raw_started)
                except (TypeError, ValueError) as exc:
                    raise NegawattRewardError(
                        f"invalid started_at: {exc}"
                    ) from exc
                if not math.isfinite(started):
                    raise NegawattRewardError(
                        "started_at must be finite."
                    )
                reward._started_at = started

            raw_counters = data.get("counters") or {}
            if not isinstance(raw_counters, Mapping):
                raise NegawattRewardError(
                    "counters must be a Mapping."
                )
            with reward._lock:
                for key, attr in (
                    ("combined_calls", "_combined_calls"),
                    (
                        "combined_reward_sum",
                        "_combined_reward_sum",
                    ),
                    ("energy_sum", "_energy_sum"),
                    ("accuracy_sum", "_accuracy_sum"),
                    ("invalid_inputs", "_invalid_inputs"),
                ):
                    if key not in raw_counters:
                        continue
                    raw = raw_counters[key]
                    try:
                        if key in (
                            "combined_calls", "invalid_inputs",
                        ):
                            value = int(raw)
                        else:
                            value = float(raw)
                    except (TypeError, ValueError) as exc:
                        raise NegawattRewardError(
                            f"invalid counter {key!r}: {exc}"
                        ) from exc
                    if not math.isfinite(value) or value < 0:
                        raise NegawattRewardError(
                            f"counter {key!r} must be finite and "
                            f">= 0."
                        )
                    setattr(reward, attr, value)
            return reward
        except NegawattRewardError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise NegawattRewardError(
                f"invalid NegawattReward payload: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "NegawattReward":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise NegawattRewardError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "NegawattReward":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "NegawattReward scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __call__(
        self,
        accuracy: float,
        energy: float,
        alpha: Optional[float] = None,
        beta: Optional[float] = None,
    ) -> float:
        """Callable shorthand for :meth:`combined_reward`."""
        return self.combined_reward(
            accuracy, energy, alpha=alpha, beta=beta,
        )

    def __repr__(self) -> str:
        return (
            "NegawattReward("
            f"baseline_energy={self.baseline_energy:.6g}, "
            f"alpha={self._config.default_alpha:.3f}, "
            f"beta={self._config.default_beta:.3f}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    reward = NegawattReward(baseline_energy=0.01)
    print("repr       :", reward)

    apw = reward.accuracy_per_watt(accuracy=0.95, energy=0.005)
    nw = reward.negawatt_score(accuracy=0.95, energy=0.005)
    combined = reward.combined_reward(accuracy=0.95, energy=0.005)
    print(f"apw        : {apw:.4f}")
    print(f"negawatt   : {nw:.4f}")
    print(f"combined   : {combined:.4f}")

    bd = reward.breakdown(accuracy=0.95, energy=0.005)
    print("breakdown  :", bd)

    # ---- Behavior checks ---------------------------------------- #
    assert nw > 0.0
    assert reward.negawatt_score(0.95, 0.02) == 0.0
    zero_base = NegawattReward(baseline_energy=0.0)
    assert zero_base.negawatt_score(0.95, 0.005) == 0.0

    # ---- Breakdown on excess energy ---------------------------- #
    excess_bd = reward.breakdown(accuracy=0.95, energy=0.02)
    print(
        f"excess bd  : saved={excess_bd.saved_energy:.4f}, "
        f"excess={excess_bd.excess_energy:.4f}, "
        f"ratio={excess_bd.saved_ratio:.3f}"
    )
    assert excess_bd.saved_energy == 0.0
    assert excess_bd.excess_energy > 0.0
    assert excess_bd.saved_ratio == 0.0

    # ---- Zero-baseline flag in breakdown ----------------------- #
    zero_bd = zero_base.breakdown(0.95, 0.005)
    assert zero_bd.metadata["baseline_zero"] is True
    print("zero base  : metadata flagged")

    # ---- apw_energy_capped flag -------------------------------- #
    zero_e_bd = reward.breakdown(0.95, 0.0)
    assert zero_e_bd.apw_energy_capped is True
    assert zero_e_bd.accuracy_per_watt == 0.0
    print("apw cap    : flagged")

    # ---- Bug fix: negative accuracy ---------------------------- #
    for bad in (
        -0.1, 1.5, float("nan"), float("inf"),
        "high", None,
    ):
        try:
            reward.negawatt_score(bad, 0.005)  # type: ignore[arg-type]
        except NegawattRewardError as exc:
            print("Rejected   :", str(exc)[:60])

    # ---- Bug fix: negative blend weights ----------------------- #
    for bad_w in (-1.0, float("nan"), float("inf"), "big"):
        try:
            reward.combined_reward(
                0.95, 0.005, beta=bad_w,  # type: ignore[arg-type]
            )
        except NegawattRewardError as exc:
            print("Rejected   :", str(exc)[:60])

    # ---- Bug fix: breakdown rejects invalid inputs ------------- #
    strict = NegawattReward(baseline_energy=0.01, strict=True)
    for bad_call in (
        lambda: strict.breakdown(float("nan"), 0.005),
        lambda: strict.breakdown(0.95, float("nan")),
        lambda: strict.breakdown("bad", 0.005),  # type: ignore[arg-type]
        lambda: strict.breakdown(0.95, "bad"),  # type: ignore[arg-type]
    ):
        try:
            bad_call()
        except NegawattRewardError as exc:
            print("Rejected   :", str(exc)[:60])

    # Non-strict also rejects breakdown on invalid input.
    lenient = NegawattReward(baseline_energy=0.01, strict=False)
    for bad_call in (
        lambda: lenient.breakdown(float("nan"), 0.005),
        lambda: lenient.breakdown(0.95, float("nan")),
    ):
        try:
            bad_call()
        except NegawattRewardError as exc:
            print("Rejected L :", str(exc)[:60])

    # But accuracy_per_watt / combined_reward still coerce to 0.0.
    assert lenient.accuracy_per_watt(
        float("nan"), 0.005,  # type: ignore[arg-type]
    ) == 0.0
    assert lenient.combined_reward(
        float("nan"), 0.005,  # type: ignore[arg-type]
    ) == 0.0
    print("lenient    : non-breakdown paths return 0.0")

    # ---- Config validation ------------------------------------- #
    for bad_cfg in (
        dict(baseline_energy=-1.0),
        dict(baseline_energy=float("nan")),
        dict(default_alpha=-1.0),
        dict(min_accuracy=-0.1),
        dict(min_accuracy=0.9, max_accuracy=0.5),
        dict(min_energy=10.0, max_energy=5.0),
        dict(max_energy=float("inf")),
    ):
        try:
            NegawattRewardConfig(**bad_cfg)  # type: ignore[arg-type]
        except NegawattRewardError as exc:
            print("Rejected c :", str(exc)[:50])

    # ---- Serialization round-trip ------------------------------ #
    payload = reward.to_json()
    restored = NegawattReward.from_json(payload)
    assert restored.to_dict() == reward.to_dict()
    print("round-trip : OK")

    # ---- Breakdown round-trip ---------------------------------- #
    bd_payload = bd.to_json()
    bd_restored = RewardBreakdown.from_json(bd_payload)
    assert bd_restored.to_dict() == bd.to_dict()
    print("bd rt      : OK")

    # ---- Callable protocol (with alpha/beta) ------------------- #
    assert reward(0.95, 0.005) == reward.combined_reward(
        0.95, 0.005,
    )
    assert reward(
        0.95, 0.005, alpha=1.5, beta=3.0,
    ) == reward.combined_reward(
        0.95, 0.005, alpha=1.5, beta=3.0,
    )
    print("callable   : OK")

    # ---- Statistics -------------------------------------------- #
    print("statistics :", reward.statistics())

    # ---- to_json rejects NaN ---------------------------------- #
    tainted = NegawattReward(baseline_energy=0.01)
    object.__setattr__(
        tainted._config, "baseline_energy", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    # ---- from_dict error wrapping ----------------------------- #
    for bad in (
        {"strict": "yes"},
        {"config": {"baseline_energy": "abc"}},
        {"started_at": "abc"},
        {"started_at": float("nan")},
        {"counters": {"combined_calls": -1}},
        {"counters": {"combined_reward_sum": "abc"}},
    ):
        try:
            NegawattReward.from_dict(bad)
        except NegawattRewardError as exc:
            print("Rejected p :", str(exc)[:60])

    # ---- Context manager -------------------------------------- #
    with NegawattReward(baseline_energy=0.01) as scoped:
        assert scoped.combined_reward(0.9, 0.005) > 0
    print("context    : OK")

    print("\nSmoke test passed.")
