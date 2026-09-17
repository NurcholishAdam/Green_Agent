# src/rlhf/reward_shaper.py

"""
RLHF Reward Shaping for Green_Agent
====================================

Provides scenario-dependent reward functions for policy optimization.

Formula::

    R = TaskSuccess − λ₁·Energy − λ₂·CO₂ − λ₃·Latency − λ₄·Cost

Enhancements
------------
- Optional ``numpy`` import guarded by ``_NUMPY_AVAILABLE``.
- ``ExecutionMode`` — unchanged (plain :class:`Enum`).
- ``RewardConfig`` — frozen dataclass with full ``__post_init__`` validation.
- ``RewardShaper`` — mode configs are now immutable; ``compute_reward``
  validates every input; thread-safe.
- ``RewardShaperError(ValueError)`` for narrow error handling.
- Serialization: ``to_dict`` / ``from_dict`` on the shaper and the config.
- ``__repr__``, lazy ``%s`` logging, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

try:  # pragma: no cover — optional dependency
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


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
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
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

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ExecutionMode):
            raise RewardShaperError(
                f"mode must be an ExecutionMode, got "
                f"{type(self.mode).__name__}."
            )
        for name in (
            "lambda_energy", "lambda_carbon", "lambda_latency", "lambda_cost",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RewardShaperError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise RewardShaperError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise RewardShaperError(f"{name} must be >= 0.")
        for name in (
            "energy_scale", "carbon_scale", "latency_scale", "cost_scale",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RewardShaperError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv <= 0:
                raise RewardShaperError(
                    f"{name} must be finite and > 0, got {value!r}."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "mode": self.mode.value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardConfig":
        if not isinstance(data, Mapping):
            raise RewardShaperError(
                "RewardConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "mode" and isinstance(v, str):
                kwargs[k] = ExecutionMode(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)

    def __repr__(self) -> str:
        return (
            "RewardConfig("
            f"mode={self.mode.value}, "
            f"λ_energy={self.lambda_energy:.3g}, "
            f"λ_carbon={self.lambda_carbon:.3g}, "
            f"λ_latency={self.lambda_latency:.3g}, "
            f"λ_cost={self.lambda_cost:.3g})"
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

    # Immutable mode configurations. Class attribute — callers must not
    # mutate it; the shaper copies entries on construction.
    _MODE_CONFIGS: Dict[ExecutionMode, RewardConfig] = {
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
    }

    @property
    def MODE_CONFIGS(self) -> Dict[ExecutionMode, RewardConfig]:
        """Backward-compatible view — returns a shallow copy."""
        return dict(self._MODE_CONFIGS)

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
        """
        self._strict = bool(strict)

        if config is not None:
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

        logger.debug(
            "RewardShaper initialized (mode=%s, strict=%s)",
            self._config.mode.value, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RewardConfig:
        return self._config

    @property
    def mode(self) -> ExecutionMode:
        return self._config.mode

    # ---------------------------------------------------------- compute
    def compute_reward(
        self,
        task_success: float,
        energy_kwh: float = 0.0,
        carbon_kg: float = 0.0,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Compute the shaped reward for a single execution.

        Returns a dict with the same keys as the original::

            {
                "reward": float,
                "components": {"task_success": float, ...},
                "penalties": {"energy": ..., "carbon": ..., "total_penalty": ...}
            }
        """
        # ---- Validate -----------------------------------------------
        ts = self._validate("task_success", task_success, low=0.0, high=1.0)
        en = self._validate("energy_kwh", energy_kwh, low=0.0, high=1e12)
        ca = self._validate("carbon_kg", carbon_kg, low=0.0, high=1e12)
        la = self._validate("latency_ms", latency_ms, low=0.0, high=1e15)
        co = self._validate("cost_usd", cost_usd, low=0.0, high=1e12)

        # ---- Penalties ----------------------------------------------
        cfg = self._config
        energy_penalty = cfg.lambda_energy * (en / cfg.energy_scale)
        carbon_penalty = cfg.lambda_carbon * (ca / cfg.carbon_scale)
        latency_penalty = cfg.lambda_latency * (la / cfg.latency_scale)
        cost_penalty = cfg.lambda_cost * (co / cfg.cost_scale)

        total_penalty = (
            energy_penalty + carbon_penalty + latency_penalty + cost_penalty
        )
        reward = ts - total_penalty

        return {
            "reward": float(reward),
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

    # ---------------------------------------------------------- validation
    def _validate(
        self, name: str, value: Any, *, low: float, high: float
    ) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"{name} must be numeric, got {type(value).__name__}."
            if self._strict:
                raise RewardShaperError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"{name} must be finite, got {value!r}."
            if self._strict:
                raise RewardShaperError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv < low:
            msg = f"{name} must be >= {low}, got {fv}."
            if self._strict:
                raise RewardShaperError(msg)
            logger.warning("%s Clamping.", msg)
            return low
        if fv > high:
            msg = f"{name} must be <= {high}, got {fv}."
            if self._strict:
                raise RewardShaperError(msg)
            logger.warning("%s Clamping to ceiling.", msg)
            return high
        return fv

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        return {
            "mode": self._config.mode.value,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "numpy_available": _NUMPY_AVAILABLE,
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
    def from_dict(cls, data: Mapping[str, Any]) -> "RewardShaper":
        if not isinstance(data, Mapping):
            raise RewardShaperError("from_dict expects a Mapping.")
        cfg = RewardConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        shaper = cls(config=cfg, strict=bool(data.get("strict", True)))
        shaper._started_at = float(data.get("started_at", time.time()))
        return shaper

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "RewardShaper":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise RewardShaperError(f"Invalid JSON: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "RewardShaper("
            f"mode={self._config.mode.value}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ExecutionMode",
    "RewardConfig",
    "RewardShaper",
    "RewardShaperError",
    "_NUMPY_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m rlhf.reward_shaper
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- All modes ------------------------------------------------ #
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

    # ---- Custom config ------------------------------------------- #
    custom = RewardConfig(
        mode=ExecutionMode.CUSTOM,
        lambda_energy=5.0, lambda_carbon=5.0,
        lambda_latency=5.0, lambda_cost=5.0,
    )
    shaper = RewardShaper(config=custom)
    print("custom    :", shaper.compute_reward(0.9, 0.001, 0.0002, 100, 0.001))

    # ---- Bug fix: NaN / inf / negative -------------------------- #
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

    # ---- Config validation -------------------------------------- #
    for bad_cfg in (
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=-1.0,
             lambda_carbon=1.0, lambda_latency=1.0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=float("nan"), lambda_latency=1.0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=1.0, lambda_latency=1.0, energy_scale=0),
        dict(mode=ExecutionMode.CUSTOM, lambda_energy=1.0,
             lambda_carbon=1.0, lambda_latency=1.0, latency_scale=-1.0),
    ):
        try:
            RewardConfig(**bad_cfg)  # type: ignore[arg-type]
        except RewardShaperError as exc:
            print("Rejected cfg:", exc)

    # ---- Non-strict coerces ------------------------------------- #
    lenient = RewardShaper(ExecutionMode.BALANCED_MODE, strict=False)
    print("lenient    :", lenient.compute_reward(float("nan"))["reward"])

    # ---- Serialization ------------------------------------------ #
    payload = RewardShaper(ExecutionMode.ECO_MODE).to_json()
    restored = RewardShaper.from_json(payload)
    assert restored.config.mode == ExecutionMode.ECO_MODE
    print("Round-trip OK.")

    # ---- CUSTOM without config ---------------------------------- #
    try:
        RewardShaper(ExecutionMode.CUSTOM)
    except RewardShaperError as exc:
        print("Rejected   :", exc)

    print("\nSmoke test passed.")
