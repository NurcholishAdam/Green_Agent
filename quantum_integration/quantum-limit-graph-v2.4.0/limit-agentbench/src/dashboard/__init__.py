# src/dashboard/__init__.py

"""
Dashboard modules for Green_Agent (Layer 11 — Visualization & Monitoring).

Components
----------
- :class:`GreenDashboard` — meta-cognitive leaderboard + HTML export.
- :class:`SymbolicVisualizer` — symbolic rule violation display.
- :class:`TelemetryLoader` — JSON report → DataFrame helper.
- :mod:`dashboard.api_server` — FastAPI + WebSocket REST layer.

Enhancements
------------
- Registry pattern (``DASHBOARD_REGISTRY`` + ``register_dashboard_component``
  / ``get_dashboard_component`` / ``list_dashboard_components``) consistent
  with the rest of the upgraded ``src/`` tree.
- Availability manifest (``DASHBOARD_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_DASHBOARD=1``.
- ``__version__`` constant.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

logger = logging.getLogger(__name__)

__version__ = "5.0.0"

# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
DASHBOARD_AVAILABILITY: Dict[str, bool] = {}

try:
    from .green_dashboard import (
        GreenDashboard,
        GreenDashboardConfig,
        GreenDashboardError,
    )
    DASHBOARD_AVAILABILITY["green_dashboard"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("green_dashboard unavailable: %s", exc)
    GreenDashboard = None  # type: ignore[assignment,misc]
    GreenDashboardConfig = None  # type: ignore[assignment,misc]
    GreenDashboardError = None  # type: ignore[assignment,misc]
    DASHBOARD_AVAILABILITY["green_dashboard"] = False

try:
    from .symbolic_visualizer import (
        SymbolicVisualizer,
        SymbolicVisualizerConfig,
        SymbolicVisualizerError,
    )
    DASHBOARD_AVAILABILITY["symbolic_visualizer"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("symbolic_visualizer unavailable: %s", exc)
    SymbolicVisualizer = None  # type: ignore[assignment,misc]
    SymbolicVisualizerConfig = None  # type: ignore[assignment,misc]
    SymbolicVisualizerError = None  # type: ignore[assignment,misc]
    DASHBOARD_AVAILABILITY["symbolic_visualizer"] = False

try:
    from .telemetry_loader import (
        TelemetryLoader,
        TelemetryLoaderConfig,
        TelemetryLoaderError,
    )
    DASHBOARD_AVAILABILITY["telemetry_loader"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("telemetry_loader unavailable: %s", exc)
    TelemetryLoader = None  # type: ignore[assignment,misc]
    TelemetryLoaderConfig = None  # type: ignore[assignment,misc]
    TelemetryLoaderError = None  # type: ignore[assignment,misc]
    DASHBOARD_AVAILABILITY["telemetry_loader"] = False

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "GreenDashboard",
    "GreenDashboardConfig",
    "GreenDashboardError",
    "SymbolicVisualizer",
    "SymbolicVisualizerConfig",
    "SymbolicVisualizerError",
    "TelemetryLoader",
    "TelemetryLoaderConfig",
    "TelemetryLoaderError",
    "DASHBOARD_REGISTRY",
    "DASHBOARD_AVAILABILITY",
    "get_dashboard_component",
    "list_dashboard_components",
    "register_dashboard_component",
]

# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
DASHBOARD_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        DASHBOARD_REGISTRY[name] = cls


_register_if_available("dashboard", GreenDashboard)
_register_if_available("visualizer", SymbolicVisualizer)
_register_if_available("loader", TelemetryLoader)


def register_dashboard_component(name: str, cls: Type[Any]) -> None:
    """Register a dashboard component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = DASHBOARD_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Dashboard component '{name}' already registered to "
            f"{existing.__name__}."
        )
    DASHBOARD_REGISTRY[name] = cls
    logger.debug("Registered dashboard component '%s' -> %s", name, cls.__name__)


def get_dashboard_component(name: str) -> Type[Any]:
    """Return the dashboard component class registered under ``name``."""
    try:
        return DASHBOARD_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown dashboard component '{name}'. "
            f"Available: {sorted(DASHBOARD_REGISTRY)}"
        ) from exc


def list_dashboard_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in DASHBOARD_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_DASHBOARD") == "1":
    _missing = [k for k, ok in DASHBOARD_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Dashboard sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(DASHBOARD_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Dashboard component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Dashboard component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
