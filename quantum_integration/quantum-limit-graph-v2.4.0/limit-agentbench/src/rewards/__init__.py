# src/rewards/__init__.py

"""
Sustainability Reward Shaping
=============================

Reward-shaping primitives for carbon-aware, helium-aware AI orchestration:

- :class:`NegawattReward` — accuracy-per-watt, negawatt score, and a
  combined RL-compatible reward signal.
- :class:`NegawattRewardConfig` — frozen, validated configuration.
- :class:`RewardBreakdown` — structured decomposition for logging / RLHF.

Enhancements
------------
- Registry pattern (``REWARDS_REGISTRY`` + ``register_*`` / ``get_*`` /
  ``list_*``) consistent with the rest of the upgraded ``src/`` tree.
- Availability manifest (``REWARDS_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_REWARDS=1``.
- ``__version__`` constant.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

logger = logging.getLogger(__name__)

__version__ = "2.4.0"

# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
REWARDS_AVAILABILITY: Dict[str, bool] = {}

try:
    from .negawatt_reward import (
        NegawattReward,
        NegawattRewardConfig,
        NegawattRewardError,
        RewardBreakdown,
    )

    REWARDS_AVAILABILITY["negawatt_reward"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("negawatt_reward unavailable: %s", exc)
    NegawattReward = None  # type: ignore[assignment,misc]
    NegawattRewardConfig = None  # type: ignore[assignment,misc]
    NegawattRewardError = None  # type: ignore[assignment,misc]
    RewardBreakdown = None  # type: ignore[assignment,misc]
    REWARDS_AVAILABILITY["negawatt_reward"] = False


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "NegawattReward",
    "NegawattRewardConfig",
    "NegawattRewardError",
    "RewardBreakdown",
    "REWARDS_REGISTRY",
    "REWARDS_AVAILABILITY",
    "get_reward_component",
    "list_reward_components",
    "register_reward_component",
]


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
REWARDS_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        REWARDS_REGISTRY[name] = cls


_register_if_available("negawatt", NegawattReward)
_register_if_available("config", NegawattRewardConfig)
_register_if_available("breakdown", RewardBreakdown)


def register_reward_component(name: str, cls: Type[Any]) -> None:
    """Register a reward component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = REWARDS_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Reward component '{name}' already registered to "
            f"{existing.__name__}."
        )
    REWARDS_REGISTRY[name] = cls
    logger.debug("Registered reward component '%s' -> %s", name, cls.__name__)


def get_reward_component(name: str) -> Type[Any]:
    """Return the reward component class registered under ``name``."""
    try:
        return REWARDS_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown reward component '{name}'. "
            f"Available: {sorted(REWARDS_REGISTRY)}"
        ) from exc


def list_reward_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in REWARDS_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_REWARDS") == "1":
    _missing = [k for k, ok in REWARDS_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Reward sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(REWARDS_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Reward component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Reward component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
