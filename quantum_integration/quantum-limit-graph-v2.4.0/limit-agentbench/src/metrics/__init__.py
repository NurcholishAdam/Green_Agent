"""
Metrics modules for Green_Agent
Includes efficiency calculators, normalized metrics, and quantum metrics.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Type

# --------------------------------------------------------------------------- #
# Classical / general metrics
# --------------------------------------------------------------------------- #
from .efficiency_calculator import NormalizedEfficiencyCalculator
from .sustainability_index import SustainabilityIndexCalculator

# --------------------------------------------------------------------------- #
# Quantum metrics (enhanced)
# --------------------------------------------------------------------------- #
from .quantum_efficiency import (
    QuantumEfficiencyMetric,
    QuantumEfficiencyError,
    EfficiencySample,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # classical
    "NormalizedEfficiencyCalculator",
    "SustainabilityIndexCalculator",
    # quantum
    "QuantumEfficiencyMetric",
    "QuantumEfficiencyError",
    "EfficiencySample",
    # registry helpers
    "METRIC_REGISTRY",
    "get_metric",
    "list_metrics",
    "register_metric",
]


# --------------------------------------------------------------------------- #
# Metric registry
# --------------------------------------------------------------------------- #
# Central lookup so benchmarking / instrumentation / dashboard layers can
# resolve a metric by name without importing concrete classes directly.
METRIC_REGISTRY: Dict[str, Type[Any]] = {
    "classical":       NormalizedEfficiencyCalculator,
    "normalized":      NormalizedEfficiencyCalculator,
    "sustainability":  SustainabilityIndexCalculator,
    "quantum":         QuantumEfficiencyMetric,
    "quantum_efficiency": QuantumEfficiencyMetric,
}


def register_metric(name: str, cls: Type[Any]) -> None:
    """
    Register a metric class under a given name.

    Raises
    ------
    ValueError
        If ``name`` is empty or already registered to a different class.
    TypeError
        If ``cls`` is not a class.
    """
    if not name or not isinstance(name, str):
        raise ValueError("Metric name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = METRIC_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Metric '{name}' already registered to {existing.__name__}."
        )
    METRIC_REGISTRY[name] = cls
    logger.debug("Registered metric '%s' -> %s", name, cls.__name__)


def get_metric(name: str) -> Type[Any]:
    """
    Return the metric class registered under ``name``.

    Raises
    ------
    KeyError
        If ``name`` is not registered.
    """
    try:
        return METRIC_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown metric '{name}'. Available: {sorted(METRIC_REGISTRY)}"
        ) from exc


def list_metrics() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {name: cls.__name__ for name, cls in METRIC_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time sanity check (only when explicitly enabled)
# --------------------------------------------------------------------------- #
# This keeps import cheap by default, but lets consumers fail-fast in CI
# by setting GREEN_AGENT_VALIDATE_METRICS=1.
import os as _os  # local alias to avoid namespace pollution

if _os.environ.get("GREEN_AGENT_VALIDATE_METRICS") == "1":
    for _name, _cls in METRIC_REGISTRY.items():
        try:
            _instance = _cls()
            logger.info("Metric '%s' (%s) instantiated OK.", _name, _cls.__name__)
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Metric '%s' (%s) failed to instantiate.", _name, _cls.__name__
            )
            raise
