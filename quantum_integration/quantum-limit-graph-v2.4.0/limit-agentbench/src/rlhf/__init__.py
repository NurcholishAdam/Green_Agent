# src/rlhf/__init__.py

"""
RLHF modules for Green_Agent
============================

Reward shaping and policy evaluation for carbon-aware RLHF.

Components
----------
- :class:`RewardShaper` — scenario-dependent reward functions.
- :class:`ExecutionMode` — eco / fast / accuracy / balanced / custom.
- :class:`RewardConfig` — frozen, validated configuration for a mode.
- :class:`PolicyEvaluationEnvironment` — evaluates an agent policy across
  tasks and modes.
- :func:`apply_green_penalty` — lightweight reward-shaping function.

Enhancements
------------
- Registry pattern (``RLHF_REGISTRY`` + ``register_rlhf_component`` /
  ``get_rlhf_component`` / ``list_rlhf_components``) consistent with the
  rest of the upgraded ``src/`` tree.
- Availability manifest (``RLHF_AVAILABILITY``).
- ``apply_green_penalty`` is now re-exported at the package root.
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_RLHF=1``.
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
RLHF_AVAILABILITY: Dict[str, bool] = {}

# ---- green_reward ----------------------------------------------------------
try:
    from .green_reward import (
        GreenRewardConfig,
        GreenRewardError,
        apply_green_penalty,
        apply_green_penalty_detailed,
    )

    RLHF_AVAILABILITY["green_reward"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("green_reward unavailable: %s", exc)
    GreenRewardConfig = None  # type: ignore[assignment,misc]
    GreenRewardError = None  # type: ignore[assignment,misc]
    apply_green_penalty = None  # type: ignore[assignment,misc]
    apply_green_penalty_detailed = None  # type: ignore[assignment,misc]
    RLHF_AVAILABILITY["green_reward"] = False


# ---- reward_shaper ---------------------------------------------------------
try:
    from .reward_shaper import (
        ExecutionMode,
        RewardConfig,
        RewardShaper,
        RewardShaperError,
    )

    RLHF_AVAILABILITY["reward_shaper"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("reward_shaper unavailable: %s", exc)
    ExecutionMode = None  # type: ignore[assignment,misc]
    RewardConfig = None  # type: ignore[assignment,misc]
    RewardShaper = None  # type: ignore[assignment,misc]
    RewardShaperError = None  # type: ignore[assignment,misc]
    RLHF_AVAILABILITY["reward_shaper"] = False


# ---- policy_evaluator ------------------------------------------------------
try:
    from .policy_evaluator import (
        PolicyEvaluationConfig,
        PolicyEvaluationEnvironment,
        PolicyEvaluatorError,
        PolicyEvaluationRecord,
    )

    RLHF_AVAILABILITY["policy_evaluator"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("policy_evaluator unavailable: %s", exc)
    PolicyEvaluationConfig = None  # type: ignore[assignment,misc]
    PolicyEvaluationEnvironment = None  # type: ignore[assignment,misc]
    PolicyEvaluatorError = None  # type: ignore[assignment,misc]
    PolicyEvaluationRecord = None  # type: ignore[assignment,misc]
    RLHF_AVAILABILITY["policy_evaluator"] = False


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    # reward shaping
    "ExecutionMode",
    "RewardConfig",
    "RewardShaper",
    "RewardShaperError",
    # green penalty
    "apply_green_penalty",
    "apply_green_penalty_detailed",
    "GreenRewardConfig",
    "GreenRewardError",
    # policy evaluation
    "PolicyEvaluationEnvironment",
    "PolicyEvaluationConfig",
    "PolicyEvaluationRecord",
    "PolicyEvaluatorError",
    # registry
    "RLHF_REGISTRY",
    "RLHF_AVAILABILITY",
    "get_rlhf_component",
    "list_rlhf_components",
    "register_rlhf_component",
]


# --------------------------------------------------------------------------- #
# Component registry
# --------------------------------------------------------------------------- #
RLHF_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        RLHF_REGISTRY[name] = cls


_register_if_available("shaper", RewardShaper)
_register_if_available("environment", PolicyEvaluationEnvironment)
_register_if_available("mode", ExecutionMode)
_register_if_available("config", RewardConfig)


def register_rlhf_component(name: str, cls: Type[Any]) -> None:
    """
    Register an RLHF component class under ``name``.

    Raises
    ------
    ValueError
        If ``name`` is empty or already registered to a different class.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = RLHF_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"RLHF component '{name}' already registered to "
            f"{existing.__name__}."
        )
    RLHF_REGISTRY[name] = cls
    logger.debug("Registered RLHF component '%s' -> %s", name, cls.__name__)


def get_rlhf_component(name: str) -> Type[Any]:
    """Return the RLHF component class registered under ``name``."""
    try:
        return RLHF_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown RLHF component '{name}'. "
            f"Available: {sorted(RLHF_REGISTRY)}"
        ) from exc


def list_rlhf_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in RLHF_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_RLHF") == "1":
    _missing = [k for k, ok in RLHF_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "RLHF sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(RLHF_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "RLHF component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "RLHF component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
