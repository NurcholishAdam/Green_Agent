# src/dpq/__init__.py

"""
Dynamic Precision Quantization (DPQ)
====================================

Carbon-aware inference layer that dynamically adjusts model precision based on
grid carbon intensity. Coordinates:

- :class:`CarbonIntensityMonitor` — tracks carbon zones and predicts transitions.
- :class:`ModelConverter` — converts models between FP32 / FP16 / INT8 / INT4.
- :class:`PrecisionController` — maps zones to precision levels and triggers
  conversions.

All public classes share the same conventions used across the upgraded
``src/`` tree: ``RLock``-guarded state, bounded histories, strict / non-strict
modes, serialization, async context managers, custom ``*Error`` exceptions,
``statistics()``, ``__repr__``, and ``__main__`` smoke tests.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Optional, Type

# --------------------------------------------------------------------------- #
# Shared enums / dataclasses (single source of truth)
# --------------------------------------------------------------------------- #
from .models import (
    CarbonZone,
    ConversionResult,
    DPQConfig,
    DPQMetrics,
    ModelPrecision,
    ModelVariant,
    PrecisionTransition,
)

# --------------------------------------------------------------------------- #
# Components
# --------------------------------------------------------------------------- #
from .carbon_intensity_monitor import (
    CarbonIntensityMonitor,
    CarbonIntensityUpdate,
    CarbonMonitorConfig,
    CarbonMonitorError,
)

from .model_converter import (
    ConverterConfig,
    ModelConverter,
    ModelConverterError,
)

from .precision_controller import (
    ControllerConfig,
    PrecisionController,
    PrecisionControllerError,
    PrecisionPolicy,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # shared models
    "CarbonZone",
    "ConversionResult",
    "DPQConfig",
    "DPQMetrics",
    "ModelPrecision",
    "ModelVariant",
    "PrecisionTransition",
    # carbon monitor
    "CarbonIntensityMonitor",
    "CarbonIntensityUpdate",
    "CarbonMonitorConfig",
    "CarbonMonitorError",
    # converter
    "ConverterConfig",
    "ModelConverter",
    "ModelConverterError",
    # controller
    "ControllerConfig",
    "PrecisionController",
    "PrecisionControllerError",
    "PrecisionPolicy",
    # registry
    "DPQ_REGISTRY",
    "get_dpq_component",
    "list_dpq_components",
    "register_dpq_component",
]

# --------------------------------------------------------------------------- #
# Component registry
# --------------------------------------------------------------------------- #
DPQ_REGISTRY: Dict[str, Type[Any]] = {
    "monitor":    CarbonIntensityMonitor,
    "converter":  ModelConverter,
    "controller": PrecisionController,
}


def register_dpq_component(name: str, cls: Type[Any]) -> None:
    """Register a DPQ component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = DPQ_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"DPQ component '{name}' already registered to {existing.__name__}."
        )
    DPQ_REGISTRY[name] = cls
    logger.debug("Registered DPQ component '%s' -> %s", name, cls.__name__)


def get_dpq_component(name: str) -> Type[Any]:
    """Return the DPQ component class registered under ``name``."""
    try:
        return DPQ_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown DPQ component '{name}'. "
            f"Available: {sorted(DPQ_REGISTRY)}"
        ) from exc


def list_dpq_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {name: cls.__name__ for name, cls in DPQ_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time sanity check
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_DPQ") == "1":
    for _name, _cls in DPQ_REGISTRY.items():
        try:
            logger.info("DPQ component '%s' (%s) importable.", _name, _cls.__name__)
        except Exception:  # pragma: no cover
            logger.exception("DPQ component '%s' failed validation.", _name)
            raise
