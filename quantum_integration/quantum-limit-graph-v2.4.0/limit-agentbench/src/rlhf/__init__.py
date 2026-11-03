from __future__ import annotations

import importlib
import inspect
import logging
import os
import threading
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Type, Union

logger = logging.getLogger(__name__)
__version__ = "2.4.0"

# --------------------------------------------------------------------------- #
# Read-only public views over mutable internals
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
RLHF_AVAILABILITY: Mapping[str, bool] = MappingProxyType(_availability)

_registry: Dict[str, Union[Type[Any], Callable[..., Any]]] = {}
RLHF_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
_registry_lock = threading.RLock()


class _MissingComponent:
    """Placeholder for an optional component that failed to import."""
    def __init__(self, name: str, reason: BaseException) -> None:
        self._name, self._reason = name, reason

    def _fail(self) -> None:
        raise ImportError(
            f"RLHF component '{self._name}' is unavailable: {self._reason}"
        ) from self._reason

    def __call__(self, *a: Any, **kw: Any) -> Any: self._fail()
    def __getattr__(self, item: str) -> Any: self._fail()
    def __repr__(self) -> str:
        return f"<unavailable RLHF component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Guarded import helper
# --------------------------------------------------------------------------- #
def _load(
    module: str, names: Tuple[str, ...]
) -> Dict[str, Any]:
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
        _availability[module] = True
        return {n: getattr(mod, n) for n in names}
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {n: _MissingComponent(n, exc) for n in names}


globals().update(_load(
    "green_reward",
    ("GreenRewardConfig", "GreenRewardError",
     "apply_green_penalty", "apply_green_penalty_detailed"),
))
globals().update(_load(
    "reward_shaper",
    ("ExecutionMode", "RewardConfig", "RewardShaper", "RewardShaperError",
     "RewardResult", "RewardComponents", "RewardPenalties",
     "NUMPY_AVAILABLE"),
))
globals().update(_load(
    "policy_evaluator",
    ("PolicyEvaluationConfig", "PolicyEvaluationEnvironment",
     "PolicyEvaluatorError", "PolicyEvaluationRecord"),
))

# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
def register_rlhf_component(
    name: str, obj: Union[Type[Any], Callable[..., Any]]
) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not callable(obj):
        raise TypeError(f"Expected a callable/class, got {type(obj).__name__}.")
    with _registry_lock:
        existing = _registry.get(name)
        if existing is not None and existing is not obj:
            raise ValueError(
                f"RLHF component '{name}' already registered to {existing!r}."
            )
        _registry[name] = obj
        logger.debug("Registered RLHF component '%s' -> %r", name, obj)


def _register_default(name: str, obj: Any) -> None:
    if not isinstance(obj, _MissingComponent) and callable(obj):
        try:
            register_rlhf_component(name, obj)
        except ValueError:
            # Already present at import time — unlikely, but don't crash import.
            logger.debug("Default registration of '%s' skipped.", name)


_register_default("shaper", RewardShaper)
_register_default("environment", PolicyEvaluationEnvironment)
_register_default("config", RewardConfig)
_register_default("mode", ExecutionMode)


def get_rlhf_component(name: str) -> Any:
    try:
        return _registry[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown RLHF component '{name}'. Available: {sorted(_registry)}"
        ) from exc


def list_rlhf_components() -> Dict[str, str]:
    return {
        n: getattr(c, "__name__", type(c).__name__)
        for n, c in _registry.items()
    }


# --------------------------------------------------------------------------- #
# Optional import-time validation (now actually meaningful)
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_RLHF") == "1":
    for _module, _ok in _availability.items():
        _level = logging.INFO if _ok else logging.WARNING
        logger.log(_level, "RLHF submodule %-20s : %s",
                   _module, "available" if _ok else "MISSING")

    for _name, _obj in _registry.items():
        if not inspect.isclass(_obj):
            logger.warning(
                "RLHF component '%s' (%r) is not a class; skipping class checks.",
                _name, _obj,
            )
            continue
        if not (_obj.__doc__ or "").strip():
            logger.warning("RLHF component '%s' has no docstring.", _name)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = (
    "__version__",
    # reward shaping
    "ExecutionMode", "RewardConfig", "RewardShaper", "RewardShaperError",
    "RewardResult", "RewardComponents", "RewardPenalties", "NUMPY_AVAILABLE",
    # green penalty
    "apply_green_penalty", "apply_green_penalty_detailed",
    "GreenRewardConfig", "GreenRewardError",
    # policy evaluation
    "PolicyEvaluationEnvironment", "PolicyEvaluationConfig",
    "PolicyEvaluationRecord", "PolicyEvaluatorError",
    # registry
    "RLHF_REGISTRY", "RLHF_AVAILABILITY",
    "get_rlhf_component", "list_rlhf_components", "register_rlhf_component",
)


def __dir__() -> list[str]:
    return sorted(__all__)
