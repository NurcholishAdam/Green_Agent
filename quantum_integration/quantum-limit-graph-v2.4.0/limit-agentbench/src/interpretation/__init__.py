# src/interpretation/__init__.py

"""
Workload Interpretation & Helium Profiling
==========================================

Converts raw task JSON into structured :class:`WorkloadProfile` objects,
including helium-dependency analysis via :class:`HeliumProfile`.

All components follow the conventions used across the upgraded ``src/`` tree:
frozen config dataclasses, validation in ``__post_init__``, UTC timestamps,
serialization, strict / non-strict modes, custom ``*Error`` exceptions,
``statistics()``, ``__repr__``, and ``__main__`` smoke tests.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

from .helium_profile import (
    HardwareType,
    HeliumDependencyLevel,
    HeliumProfile,
    HeliumProfileConfig,
    HeliumProfileError,
)

from .workload_interpreter import (
    InterpretationConfig,
    InterpretationError,
    WorkloadInterpreter,
    WorkloadProfile,
)

logger = logging.getLogger(__name__)

__all__ = [
    "HardwareType",
    "HeliumDependencyLevel",
    "HeliumProfile",
    "HeliumProfileConfig",
    "HeliumProfileError",
    "InterpretationConfig",
    "InterpretationError",
    "WorkloadInterpreter",
    "WorkloadProfile",
    # registry
    "INTERPRETATION_REGISTRY",
    "get_interpretation_component",
    "list_interpretation_components",
    "register_interpretation_component",
]

INTERPRETATION_REGISTRY: Dict[str, Type[Any]] = {
    "interpreter":  WorkloadInterpreter,
    "helium":       HeliumProfile,
    "workload":     WorkloadProfile,
}


def register_interpretation_component(name: str, cls: Type[Any]) -> None:
    """Register an interpretation component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = INTERPRETATION_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Interpretation component '{name}' already registered to "
            f"{existing.__name__}."
        )
    INTERPRETATION_REGISTRY[name] = cls
    logger.debug("Registered interpretation component '%s' -> %s", name, cls.__name__)


def get_interpretation_component(name: str) -> Type[Any]:
    """Return the interpretation component class registered under ``name``."""
    try:
        return INTERPRETATION_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown interpretation component '{name}'. "
            f"Available: {sorted(INTERPRETATION_REGISTRY)}"
        ) from exc


def list_interpretation_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in INTERPRETATION_REGISTRY.items()}


if _os.environ.get("GREEN_AGENT_VALIDATE_INTERPRETATION") == "1":
    for _name, _cls in INTERPRETATION_REGISTRY.items():
        try:
            logger.info("Interpretation component '%s' importable.", _name)
        except Exception:  # pragma: no cover
            logger.exception("Interpretation component '%s' failed.", _name)
            raise
