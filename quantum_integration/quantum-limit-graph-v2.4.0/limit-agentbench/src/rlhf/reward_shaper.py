# src/rlhf/reward_shaper.py

"""
RLHF Reward Shaping for Green_Agent
====================================

Provides scenario-dependent reward functions for policy optimization.

Formula::

    R = TaskSuccess − λ₁·(Energy/scale) − λ₂·(CO₂/scale)
                    − λ₃·(Latency/scale) − λ₄·(Cost/scale)

Optional bounding: ``RewardConfig.transform`` ∈ {"none", "tanh"} and/or
``RewardConfig.reward_clip`` (low, high) can bound the final reward.

Enhancements over the previous revision
---------------------------------------
- ``RewardConfig`` is ``frozen`` + ``slots``; validates every field,
  including ``reward_clip``, ``transform`` and ``nan_policy``.
- Configurable validation ceilings: ``max_energy_kwh``, ``max_carbon_kg``,
  ``max_latency_ms``, ``max_cost_usd``.
- Optional reward bounding (``tanh`` compression and/or hard clip).
- All parse failures in ``from_dict``/``from_json`` raise
  :class:`RewardShaperError` (no more leaking ``ValueError``/``TypeError``).
- ``RewardConfig.from_dict(..., strict=True)`` raises on unknown keys.
- Preset ``_MODE_CONFIGS`` is a :class:`MappingProxyType` (truly immutable).
- ``RewardResult`` / ``RewardComponents`` / ``RewardPenalties`` TypedDicts
  give static typing to ``compute_reward``.
- ``compute_rewards_batch`` — vectorised reward computation via NumPy
  (finally justifies the NumPy dependency). Raises a clear error if NumPy
  is not installed.
- ``reset(started_at=None)`` — deterministic start time for tests.
- ``__eq__`` / ``__hash__`` on ``RewardShaper``.
- ``to_json`` no longer uses ``default=str``; the payload is primitive-only.
- Thread-safety claim is now true: ``from_dict`` and ``reset`` hold the lock.
- Non-strict ``nan_policy`` ∈ {"zero", "clamp"}; clamping paths log at
  INFO, NaN/inf coercion at WARNING.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Literal, Mapping, Optional, Tuple, TypedDict

logger = logging.getLogger(__name__)

try:  # pragma: no cover — optional dependency
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False

# Public alias (no leading underscore) — see __all__.
NUMPY_AVAILABLE = _NUMPY_AVAILABLE


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RewardShaperError(ValueError):
    """Raised for invalid reward-shaper inputs or configuration."""


# --------------------------------------------------------------------------- #
# Execution modes
# --------------------------------------------------------------------------- #
class ExecutionMode(Enum):
    """
    Optimization modes for agent execution.

    Each mode applies different penalty weights:

    - ``ECO_MODE``       — minimize energy/carbon (heavy penalties).
    - ``FAST_MODE``      — minimize latency (light energy penalties).
    - ``ACCURACY_MODE``  — maximize accuracy (minimal resource penalties).
    - ``BALANCED_MODE``  — balance all factors.
    - ``CUSTOM``         — user-defined weights.
    """

    ECO_MODE = "eco"
    FAST_MODE = "fast"
    ACCURACY_MODE = "accuracy"
    BALANCED_MODE = "balanced"
    CUSTOM = "custom"


# --------------------------------------------------------------------------- #
# Typed result shapes
# --------------------------------------------------------------------------- #
class RewardComponents(TypedDict):
    task_success: float
    energy_kwh: float
    carbon_kg: float
    latency_ms: float
    cost_usd: float


class RewardPenalties(TypedDict):
    energy: float
    carbon: float
    latency: float
    cost: float
    total_penalty: float


class RewardResult(TypedDict):
    reward: float
    mode: str
    components: RewardComponents
    penalties: RewardPenalties


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
_NAN_POLICIES = ("zero", "clamp")
_TRANSFORMS = ("none", "tanh")


@dataclass(frozen=True, slots=True)
class RewardConfig:
    """
    Immutable configuration for a reward function.

    λ (lambda) values determine how strongly each resource is penalized.
    Higher λ = stronger penalty for that resource.
    """

    mode: ExecutionMode
    lambda_energy: float
    lambda_carbon: float
    lambda_latency: float
    lambda_cost: float = 0.0

    # Normalization factors (scale raw metrics to a ~1.0 penalty range).
    energy_scale: float = 1000.0
    carbon_scale: float = 100.0
    latency_scale: float = 0.001
    cost_scale: float = 1.0

    # Validation ceilings — tune per deployment.
    max_energy_kwh: float = 1e6
    max_carbon_kg: float = 1e6
    max_latency_ms: float = 3.6e6
    max_cost_usd: float = 1e6

    # Reward bounding (optional).
    reward_clip: Optional[Tuple[float, float]] = None
    transform: Literal["none", "tanh"] = "none"

    # Non-strict behavior for NaN/inf: "zero" (legacy) or "clamp".
    nan_policy: Literal["zero", "clamp"] = "zero"

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ExecutionMode):
            raise RewardShaperError(
                f"mode must be an ExecutionMode, got {type(self.mode).__name__}."
            )
        for name in (
            "lambda_energy", "lambda_carbon", "lambda_latency", "lambda_cost",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RewardShaperError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv):
                raise RewardShaperError(f"{name} must be finite, got {value!r}.")
            if fv < 0:
                raise RewardShaperError(f"{name} must be >= 0.")

        for name in (
            "energy_scale", "carbon_scale", "latency_scale", "cost_scale",
            "max_energy_kwh", "max_carbon_kg", "max_latency_ms", "max_cost_usd",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RewardShaperError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv <= 0:
                raise RewardShaperError(
                    f"{name} must be finite and > 0, got {value!r}."
                )

        if self.transform not in _TRANSFORMS:
            raise RewardShaperError(
                f"transform must be one of {_TRANSFORMS}, got {self.transform!r}."
            )
        if self.nan_policy not in _NAN_POLICIES:
            raise RewardShaperError(
                f"nan_policy must be one of {_NAN_POLICIES}, "
                f"got {self.nan_policy!r}."
            )
        if self.reward_clip is not None:
            if not (isinstance(self.reward_clip, tuple)
                    and len(self.reward_clip) == 2):
                raise RewardShaperError(
                    f"reward_clip must be a 2-tuple, got {self.reward_clip!r}."
                )
            lo, hi = self.reward_clip
            for v in (lo, hi):
                if isinstance(v, bool) or not isinstance(v, (int, float)):
                    raise RewardShaperError(
                        "reward_clip values must be numeric."
                    )
                if not math.isfinite(float(v)):
                    raise RewardShaperError(
                        "reward_clip values must be finite."
                    )
            if float(lo) >= float(hi):
                raise RewardShaperError(
                    f"reward_clip low must be < high, got {self.reward_clip!r}."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {**asdict(self), "mode": self.mode.value}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> "RewardConfig":
        if not isinstance(data, Mapping):
            raise RewardShaperError("RewardConfig.from_dict expects a Mapping.")
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        unknown = []
        for k, v in data.items():
            if k not in valid:
                unknown.append(k)
                continue
            if k == "mode" and isinstance(v, str):
                try:
                    kwargs[k] = ExecutionMode(v)
                except ValueError as exc:
                    raise RewardShaperError(
                        f"invalid mode {v!r}: {exc}"
                    ) from exc
            elif k == "reward_clip" and v is not None:
                if not isinstance(v, (list, tuple)) or len(v) != 2:
                    raise RewardShaperError(
                        f"invalid reward_clip {v!r}; expected [low, high]."
                    )
                kwargs[k] = (v[0], v[1])
            else:
                kwargs[k] = v
        if unknown:
            msg = f"unknown RewardConfig keys: {unknown}"
            if strict:
                raise RewardShaperError(msg)
            logger.warning(msg)
        try:
            return cls(**kwargs)
        except TypeError as exc:
            raise RewardShaperError(
                f"invalid RewardConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        extras = ""
        if self.reward_clip is not None:
            extras += f", clip={self.reward_clip}"
        if self.transform != "none":
            extras += f", transform={self.transform}"
        return (
            "RewardConfig("
            f"mode={self.mode.value}, "
            f"λ_energy={self.lambda_energy:.3g}, "
            f"λ_carbon={self.lambda_carbon:.3g}, "
            f"λ_latency={self.lambda_latency:.3g}, "
            f"λ_cost={self.lambda_cost:.3g}{extras})"
        )


# --------------------------------------------------------------------------- #
# Shaper
# --------------------------------------------------------------------------- #
class RewardShaper:
    """
    Shapes rewards for RLHF policy optimization.

    Provides scenario-dependent reward functions that enable agents to be
    optimized for different deployment contexts (eco, fast, accuracy).
    """

    # Immutable mode configurations. The MappingProxyType forbids mutation.
    _MODE_CONFIGS: Mapping[ExecutionMode, RewardConfig] = MappingProxyType({
        ExecutionMode.ECO_MODE: RewardConfig(
            mode=ExecutionMode.ECO_MODE,
            lambda_energy=10.0,
            lambda_carbon=10.0,
            lambda_latency=1.0,
            lambda_cost=2.0,
        ),
        ExecutionMode.FAST_MODE: RewardConfig(
            mode=ExecutionMode.FAST_MODE,
            lambda_energy=1.0,
            lambda_carbon=1.0,
            lambda_latency=10.0,
            lambda_cost=1.0,
        ),
        ExecutionMode.ACCURACY_MODE: RewardConfig(
            mode=ExecutionMode.ACCURACY_MODE,
            lambda_energy=0.5,
            lambda_carbon=0.5,
            lambda_latency=0.5,
            lambda_cost=0.5,
        ),
        ExecutionMode.BALANCED_MODE: RewardConfig(
            mode=ExecutionMode.BALANCED_MODE,
            lambda_energy=3.0,
            lambda_carbon=3.0,
            lambda_latency=3.0,
            lambda_cost=2.0,
        ),
    })

    @property
    def MODE_CONFIGS(self) -> Mapping[ExecutionMode, RewardConfig]:
        """Backward-compatible, immutable view of preset configurations."""
        return self._MODE_CONFIGS

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.BALANCED_MODE,
        *,
        config: Optional[RewardConfig] = None,
        strict: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        mode : ExecutionMode, default BALANCED_MODE
            Preset mode to apply when ``config`` is not provided.
        config : RewardConfig, optional
            Explicit reward configuration. Overrides ``mode``.
        strict : bool, default True
            If True, invalid inputs raise :class:`RewardShaperError`.
            If False, invalid inputs are coerced using
            ``RewardConfig.nan_policy`` and clamping.
        """
        self._strict = bool(strict)

        if config is not None:
            if not isinstance(config, RewardConfig):
                raise RewardShaperError(
                    f"config must be a RewardConfig, got "
                    f"{type(config).__name__}."
                )
            self._config = config
        else:
            if mode == ExecutionMode.CUSTOM:
                raise RewardShaperError(
                    "ExecutionMode.CUSTOM requires an explicit config."
                )
            cfg = self._MODE_CONFIGS.get(mode)
            if cfg is None:
                raise RewardShaperError(
                    f"no preset configuration for mode {mode!r}; "
                    "supply a RewardConfig."
                )
            self._config = cfg

        self._lock = threading.RLock()
        self._started_at: float = time.time()

        cfg = self._config
        self._bounds: Dict[str, Tuple[float, float]] = {
            "task_success": (0.0, 1.0),
            "energy_kwh":   (0.0, float(cfg.max_energy_kwh)),
            "carbon_kg":    (0.0, float(cfg.max_carbon_kg)),
            "latency_ms":   (0.0, float(cfg.max_latency_ms)),
            "cost_usd":     (0.0, float(cfg.max_cost_usd)),
        }

        logger.debug(
            "RewardShaper initialized (mode=%s, strict=%s, numpy=%s)",
            cfg.mode.value, self._strict, _NUMPY_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RewardConfig:
        return self._config

    @property
    def mode(self) -> ExecutionMode:
        return self._config.mode

    @property
    def strict(self) -> bool:
        return self._strict

    # ---------------------------------------------------------- reset / timing
    def reset(self, started_at: Optional[float] = None) -> None:
        """Reset the internal start timestamp (used by ``statistics``)."""
        with self._lock:
            self._started_at = (
                float(started_at) if started_at is not None else time.time()
            )

    # ---------------------------------------------------------- compute
    def compute_reward(
        self,
        task_success: float,
        energy_kwh: float = 0.0,
        carbon_kg: float = 0.0,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
    ) -> RewardResult:
        """
        Compute the shaped reward for a single execution.

        The returned ``reward`` is
        ``task_success − Σ penalties``, then optionally transformed
        (``tanh``) and/or clipped according to ``RewardConfig``.
        """
        ts = self._validate("task_success", task_success)
        en = self._validate("energy_kwh", energy_kwh)
        ca = self._validate("carbon_kg", carbon_kg)
        la = self._validate("latency_ms", latency_ms)
        co = self._validate("cost_usd", cost_usd)

        cfg = self._config
        energy_penalty = cfg.lambda_energy * (en / cfg.energy_scale)
        carbon_penalty = cfg.lambda_carbon * (ca / cfg.carbon_scale)
        latency_penalty = cfg.lambda_latency * (la / cfg.latency_scale)
        cost_penalty = cfg.lambda_cost * (co / cfg.cost_scale)

        total_penalty = (
            energy_penalty + carbon_penalty + latency_penalty + cost_penalty
        )
        raw_reward = ts - total_penalty
        reward = self._bound(raw_reward)

        logger.debug(
            "compute_reward mode=%s raw=%.6f reward=%.6f",
            cfg.mode.value, raw_reward, reward,
        )

        return {
            "reward": float(reward),
            "mode": cfg.mode.value,
            "components": {
                "task_success": float(ts),
                "energy_kwh": float(en),
                "carbon_kg": float(ca),
                "latency_ms": float(la),
                "cost_usd": float(co),
            },
            "penalties": {
                "energy": float(energy_penalty),
                "carbon": float(carbon_penalty),
                "latency": float(latency_penalty),
                "cost": float(cost_penalty),
                "total_penalty": float(total_penalty),
            },
        }

    # ---------------------------------------------------------- batch compute
    def compute_rewards_batch(
        self,
        task_success: Any,
        energy_kwh: Any = 0.0,
        carbon_kg: Any = 0.0,
        latency_ms: Any = 0.0,
        cost_usd: Any = 0.0,
    ) -> Dict[str, Any]:
        """
        Vectorised reward computation for a batch of executions.

        Uses NumPy; raises :class:`RewardShaperError` if NumPy is not
        installed. Scalars broadcast against arrays.

        Returns
        -------
        dict
            ``{"rewards": np.ndarray, "mode": str,
                "components": {name: np.ndarray},
                "penalties":  {name: np.ndarray}}``
        """
        if not _NUMPY_AVAILABLE:
            raise RewardShaperError(
                "compute_rewards_batch requires numpy; install `numpy` to use it."
            )

        ts = self._validate_array("task_success", task_success)
        en = self._validate_array("energy_kwh", energy_kwh)
        ca = self._validate_array("carbon_kg", carbon_kg)
        la = self._validate_array("latency_ms", latency_ms)
        co = self._validate_array("cost_usd", cost_usd)

        ts, en, ca, la, co = np.broadcast_arrays(ts, en, ca, la, co)

        cfg = self._config
        energy_penalty = cfg.lambda_energy * (en / cfg.energy_scale)
        carbon_penalty = cfg.lambda_carbon * (ca / cfg.carbon_scale)
        latency_penalty = cfg.lambda_latency * (la / cfg.latency_scale)
        cost_penalty = cfg.lambda_cost * (co / cfg.cost_scale)
        total_penalty = (
            energy_penalty + carbon_penalty + latency_penalty + cost_penalty
        )
        rewards = ts - total_penalty

        if cfg.transform == "tanh":
            rewards = np.tanh(rewards)
        if cfg.reward_clip is not None:
            rewards = np.clip(rewards, cfg.reward_clip[0], cfg.reward_clip[1])

        return {
            "rewards": rewards,
            "mode": cfg.mode.value,
            "components": {
                "task_success": ts,
                "energy_kwh": en,
                "carbon_kg": ca,
                "latency_ms": la,
                "cost_usd": co,
            },
            "penalties": {
                "energy": energy_penalty,
                "carbon": carbon_penalty,
                "latency": latency_penalty,
                "cost": cost_penalty,
                "total_penalty": total_penalty,
            },
        }

    # ---------------------------------------------------------- bounding
    def _bound(self, value: float) -> float:
        cfg = self._config
        if cfg.transform == "tanh":
            value = math.tanh(value)
        if cfg.reward_clip is not None:
            lo, hi = cfg.reward_clip
            value = max(lo, min(hi, value))
        return value

    # ---------------------------------------------------------- validation
    def _validate(self, name: str, value: Any) -> float:
        low, high = self._bounds[name]

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return self._invalid(name, value, "numeric", low)
        fv = float(value)
        if not math.isfinite(fv):
            return self._invalid(name, value, "finite", low)
        if fv < low:
            return self._out_of_range(name, fv, low, high)
        if fv > high:
            return self._out_of_range(name, fv, low, high)
        return fv

    def _invalid(
        self, name: str, value: Any, kind: str, low: float
    ) -> float:
        msg = f"{name} must be {kind}, got {value!r}."
        if self._strict:
            raise RewardShaperError(msg)
        if self._config.nan_policy == "clamp":
            logger.warning("%s Clamping to low bound (%s).", msg, low)
            return float(low)
        logger.warning("%s Using 0.0.", msg)
        return 0.0

    def _out_of_range(
        self, name: str, fv: float, low: float, high: float
    ) -> float:
        if self._strict:
            raise RewardShaperError(
                f"{name} must be in [{low}, {high}], got {fv}."
            )
        clamped = max(low, min(high, fv))
        logger.info("%s Clamping %s -> %s.", name, fv, clamped)
        return clamped

    def _validate_array(self, name: str, arr: Any):
        low, high = self._bounds[name]
        try:
            a = np.asarray(arr, dtype=np.float64)
        except (TypeError, ValueError) as exc:
            raise RewardShaperError(
                f"{name} is not array-like of numbers: {exc}"
            ) from exc

        finite = np.isfinite(a)
        if not np.all(finite):
            if self._strict:
                idx = int(np.flatnonzero(~finite)[0])
                raise RewardShaperError(
                    f"{name}[{idx}] = {a.flat[idx]!r} is not finite."
                )
            if self._config.nan_policy == "clamp":
                a = np.where(np.isnan(a), low, a)
                a = np.where(np.isposinf(a), high, a)
                a = np.where(np.isneginf(a), low, a)
                logger.warning(
                    "%s: coerced non-finite values (nan_policy='clamp').", name
                )
            else:
                a = np.where(finite, a, 0.0)
                logger.warning(
                    "%s: coerced non-finite values to 0.0.", name
                )

        out_of_range = (a < low) | (a > high)
        if np.any(out_of_range):
            if self._strict:
                idx = int(np.flatnonzero(out_of_range)[0])
                raise RewardShaperError(
                    f"{name}[{idx}] = {a.flat[idx]!r} outside [{low}, {high}]."
                )
            logger.info(
                "%s: clamped %d element(s) into [%s, %s].",
                name, int(np.sum(out_of_range)), low, high,
            )
            a = np.clip(a, low, high)

        return a

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            uptime = time.time() - self._started_at
        return {
            "mode": self._config.mode.value,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "numpy_available": _NUMPY_AVAILABLE,
            "uptime_seconds": uptime,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            started = self._started_at
        return {
            "config": self._config.to_dict(),
            "strict": self._strict,
            "started_at": started,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardShaper":
        if not isinstance(data, Mapping):
            raise RewardShaperError("from_dict expects a Mapping.")

        strict = bool(data.get("strict", True))
        try:
            cfg_data = dict(data.get("config") or {})
        except (TypeError, ValueError) as exc:
            raise RewardShaperError(
                f"invalid config payload: {exc}"
            ) from exc
        cfg = RewardConfig.from_dict(cfg_data, strict=True)

        shaper = cls(config=cfg, strict=strict)

        if "started_at" in data and data["started_at"] is not None:
            try:
                shaper.reset(float(data["started_at"]))
            except (TypeError, ValueError) as exc:
                raise RewardShaperError(
                    f"invalid started_at: {exc}"
                ) from exc

        return shaper

    def to_json(self, **kwargs: Any) -> str:
        # Payload is primitive-only; no default= required.
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "RewardShaper":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RewardShaperError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "RewardShaper("
            f"mode={self._config.mode.value}, "
            f"strict={self._strict})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RewardShaper):
            return NotImplemented
        return (
            self._config == other._config
            and self._strict == other._strict
        )

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ExecutionMode",
    "RewardConfig",
    "RewardShaper",
    "RewardShaperError",
    "RewardResult",
    "RewardComponents",
    "RewardPenalties",
    "NUMPY_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m rlhf.reward_shaper
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- All modes ------------------------------------------------- #
    for mode in ExecutionMode:
        if mode == ExecutionMode.CUSTOM:
            continue
        shaper = RewardShaper(mode)
        r = shaper.compute_reward(
            task_success=0.95,
            energy_kwh=0.003,
            carbon_kg=0.0006,
            latency_ms=150.0,
            cost_usd=0.001,
        )
        print(f"{mode.value:<10} reward={r['reward']:.4f} "
              f"penalty={r['penalties']['total_penalty']:.4f}")

    # ---- Custom config with reward bounding ------------------------ #
    custom = RewardConfig(
        mode=ExecutionMode.CUSTOM,
        lambda_energy=5.0, lambda_carbon=5.0,
        lambda_latency=5.0, lambda_cost=5.0,
        reward_clip=(-10.0, 1.0),
        transform="tanh",
    )
    shaper = RewardShaper(config=custom)
    print("custom    :", shaper.compute_reward(0.9, 0.001, 0.0002, 100, 0.001))

    # ---- Bug fix: NaN / inf / negative ---------------------------- #
    strict = RewardShaper(ExecutionMode.BALANCED_MODE, strict=True)
    for bad in (
        dict(task_success=float("nan")),
        dict(task_success=1.5),
        dict(task_success=-0.1),
        dict(energy_kwh=-1.0),
        dict(carbon_kg=float("inf")),
        dict(latency_ms=-1.0),
    ):
        try:
            strict.compute_reward(**bad)  # type: ignore[arg-type]
        except RewardShaperError as exc:
            print("Rejected   :", exc)

    # ---- Config validation ---------------------------------------- #
    for bad_cfg in (
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=-1.0,
             lambda_carbon=1.0, lambda_latency=1.0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=float("nan"), lambda_latency=1.0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=1.0, lambda_latency=1.0, energy_scale=0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=1.0, lambda_latency=1.0, latency_scale=-1.0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=1.0, lambda_latency=1.0,
             reward_clip=(1.0, -1.0)),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=1.0, lambda_latency=1.0,
             transform="sigmoid"),
    ):
        try:
            RewardConfig(**bad_cfg)  # type: ignore[arg-type]
        except RewardShaperError as exc:
            print("Rejected cfg:", exc)

    # ---- Non-strict coerces -------------------------------------- #
    lenient = RewardShaper(ExecutionMode.BALANCED_MODE, strict=False)
    print("lenient    :", lenient.compute_reward(float("nan"))["reward"])

    # ---- Serialization ------------------------------------------- #
    payload = RewardShaper(ExecutionMode.ECO_MODE).to_json()
    restored = RewardShaper.from_json(payload)
    assert restored.config.mode == ExecutionMode.ECO_MODE
    assert restored == RewardShaper(ExecutionMode.ECO_MODE)
    print("Round-trip OK.")

    # ---- CUSTOM without config ----------------------------------- #
    try:
        RewardShaper(ExecutionMode.CUSTOM)
    except RewardShaperError as exc:
        print("Rejected   :", exc)

    # ---- Batch (if NumPy available) ------------------------------ #
    if NUMPY_AVAILABLE:
        import numpy as _np
        batch = RewardShaper(ExecutionMode.BALANCED_MODE).compute_rewards_batch(
            task_success=_np.array([0.9, 0.5, 0.99]),
            energy_kwh=0.002,
            latency_ms=_np.array([100.0, 250.0, 80.0]),
        )
        print("batch      :", batch["rewards"])
    else:
        print("numpy      : not installed — batch API unavailable")

    print("\nSmoke test passed.")
