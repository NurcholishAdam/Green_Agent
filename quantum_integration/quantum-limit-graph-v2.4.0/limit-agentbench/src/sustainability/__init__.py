# src/sustainability/__init__.py

"""
Sustainability modules for Green_Agent
======================================

Carbon-awareness and eco-mode control:

- :class:`CarbonForecast` — moving-average / EWMA carbon intensity predictor.
- :class:`CarbonIntensityProvider` — current intensity + zone classification.
- :class:`EcoModeController` — adaptive throttling driven by carbon intensity.
- :class:`SustainabilityPareto` — energy vs. accuracy Pareto frontier.
- :class:`TemporalShifter` — defer work when the grid is forecast to be dirty.

Enhancements
------------
- Registry pattern (``SUSTAINABILITY_REGISTRY`` + ``register_sustainability_component``
  / ``get_sustainability_component`` / ``list_sustainability_components``)
  consistent with the rest of the upgraded ``src/`` tree.
- Availability manifest (``SUSTAINABILITY_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SUSTAINABILITY=1``.
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
SUSTAINABILITY_AVAILABILITY: Dict[str, bool] = {}

try:
    from .carbon_forecast import (
        CarbonForecast,
        CarbonForecastConfig,
        CarbonForecastError,
        ForecastMethod,
    )

    SUSTAINABILITY_AVAILABILITY["carbon_forecast"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("carbon_forecast unavailable: %s", exc)
    CarbonForecast = None  # type: ignore[assignment,misc]
    CarbonForecastConfig = None  # type: ignore[assignment,misc]
    CarbonForecastError = None  # type: ignore[assignment,misc]
    ForecastMethod = None  # type: ignore[assignment,misc]
    SUSTAINABILITY_AVAILABILITY["carbon_forecast"] = False

try:
    from .carbon_intensity_provider import (
        CarbonIntensityProvider,
        CarbonIntensityProviderConfig,
        CarbonIntensityProviderError,
    )

    SUSTAINABILITY_AVAILABILITY["carbon_intensity_provider"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("carbon_intensity_provider unavailable: %s", exc)
    CarbonIntensityProvider = None  # type: ignore[assignment,misc]
    CarbonIntensityProviderConfig = None  # type: ignore[assignment,misc]
    CarbonIntensityProviderError = None  # type: ignore[assignment,misc]
    SUSTAINABILITY_AVAILABILITY["carbon_intensity_provider"] = False

try:
    from .eco_mode_controller import (
        EcoModeController,
        EcoModeControllerConfig,
        EcoModeControllerError,
        EcoModeTransition,
    )

    SUSTAINABILITY_AVAILABILITY["eco_mode_controller"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("eco_mode_controller unavailable: %s", exc)
    EcoModeController = None  # type: ignore[assignment,misc]
    EcoModeControllerConfig = None  # type: ignore[assignment,misc]
    EcoModeControllerError = None  # type: ignore[assignment,misc]
    EcoModeTransition = None  # type: ignore[assignment,misc]
    SUSTAINABILITY_AVAILABILITY["eco_mode_controller"] = False

try:
    from .pareto_frontier import (
        AgentPerformancePoint,
        SustainabilityPareto,
        SustainabilityParetoConfig,
        SustainabilityParetoError,
    )

    SUSTAINABILITY_AVAILABILITY["pareto_frontier"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("pareto_frontier unavailable: %s", exc)
    AgentPerformancePoint = None  # type: ignore[assignment,misc]
    SustainabilityPareto = None  # type: ignore[assignment,misc]
    SustainabilityParetoConfig = None  # type: ignore[assignment,misc]
    SustainabilityParetoError = None  # type: ignore[assignment,misc]
    SUSTAINABILITY_AVAILABILITY["pareto_frontier"] = False

try:
    from .temporal_shifter import (
        TemporalShifter,
        TemporalShifterConfig,
        TemporalShifterError,
        ShiftDecision,
    )

    SUSTAINABILITY_AVAILABILITY["temporal_shifter"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("temporal_shifter unavailable: %s", exc)
    TemporalShifter = None  # type: ignore[assignment,misc]
    TemporalShifterConfig = None  # type: ignore[assignment,misc]
    TemporalShifterError = None  # type: ignore[assignment,misc]
    ShiftDecision = None  # type: ignore[assignment,misc]
    SUSTAINABILITY_AVAILABILITY["temporal_shifter"] = False

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    # carbon forecast
    "CarbonForecast",
    "CarbonForecastConfig",
    "CarbonForecastError",
    "ForecastMethod",
    # carbon intensity provider
    "CarbonIntensityProvider",
    "CarbonIntensityProviderConfig",
    "CarbonIntensityProviderError",
    # eco mode controller
    "EcoModeController",
    "EcoModeControllerConfig",
    "EcoModeControllerError",
    "EcoModeTransition",
    # pareto frontier
    "AgentPerformancePoint",
    "SustainabilityPareto",
    "SustainabilityParetoConfig",
    "SustainabilityParetoError",
    # temporal shifter
    "TemporalShifter",
    "TemporalShifterConfig",
    "TemporalShifterError",
    "ShiftDecision",
    # registry
    "SUSTAINABILITY_REGISTRY",
    "SUSTAINABILITY_AVAILABILITY",
    "get_sustainability_component",
    "list_sustainability_components",
    "register_sustainability_component",
]

# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
SUSTAINABILITY_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        SUSTAINABILITY_REGISTRY[name] = cls


_register_if_available("forecast", CarbonForecast)
_register_if_available("provider", CarbonIntensityProvider)
_register_if_available("eco_mode", EcoModeController)
_register_if_available("pareto", SustainabilityPareto)
_register_if_available("shifter", TemporalShifter)


def register_sustainability_component(name: str, cls: Type[Any]) -> None:
    """Register a sustainability component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = SUSTAINABILITY_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Sustainability component '{name}' already registered to "
            f"{existing.__name__}."
        )
    SUSTAINABILITY_REGISTRY[name] = cls
    logger.debug("Registered sustainability component '%s' -> %s", name, cls.__name__)


def get_sustainability_component(name: str) -> Type[Any]:
    """Return the sustainability component class registered under ``name``."""
    try:
        return SUSTAINABILITY_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown sustainability component '{name}'. "
            f"Available: {sorted(SUSTAINABILITY_REGISTRY)}"
        ) from exc


def list_sustainability_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in SUSTAINABILITY_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_SUSTAINABILITY") == "1":
    _missing = [k for k, ok in SUSTAINABILITY_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Sustainability sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(SUSTAINABILITY_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Sustainability component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Sustainability component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
