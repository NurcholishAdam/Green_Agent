# src/optimization/__init__.py

"""
Optimization Layer
==================

Multi-objective optimization for carbon-aware, helium-aware AI orchestration.

Components
----------
- :class:`GeneticOptimizer` — NSGA-II multi-objective configuration tuning.
- :class:`BayesianOptimizer` — sample-efficient hyperparameter search.
- :class:`HyperparameterTuner` — grid / random / adaptive search.
- :class:`QuantizationOptimizer` — precision selection per carbon zone.
- :class:`ScheduleOptimizer` — continuum tier scheduling optimization.

All components follow the conventions used across the upgraded ``src/`` tree:
frozen config dataclasses, validation, UTC timestamps, bounded histories,
serialization, strict / non-strict modes, custom ``*Error`` exceptions,
``statistics()``, ``__repr__``, and ``__main__`` smoke tests.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

from .genetic_optimizer import (
    GeneticOptimizer,
    GeneticOptimizerConfig,
    GeneticOptimizerError,
    OptimizationResult,
)
from .bayesian_optimizer import (
    BayesianOptimizer,
    BayesianOptimizerConfig,
    BayesianOptimizerError,
)
from .hyperparameter_tuner import (
    HyperparameterTuner,
    HyperparameterTunerConfig,
    HyperparameterTunerError,
    TunerResult,
)
from .quantization_optimizer import (
    QuantizationOptimizer,
    QuantizationOptimizerConfig,
    QuantizationOptimizerError,
)
from .schedule_optimizer import (
    ScheduleOptimizer,
    ScheduleOptimizerConfig,
    ScheduleOptimizerError,
)

logger = logging.getLogger(__name__)

__all__ = [
    "GeneticOptimizer",
    "GeneticOptimizerConfig",
    "GeneticOptimizerError",
    "OptimizationResult",
    "BayesianOptimizer",
    "BayesianOptimizerConfig",
    "BayesianOptimizerError",
    "HyperparameterTuner",
    "HyperparameterTunerConfig",
    "HyperparameterTunerError",
    "TunerResult",
    "QuantizationOptimizer",
    "QuantizationOptimizerConfig",
    "QuantizationOptimizerError",
    "ScheduleOptimizer",
    "ScheduleOptimizerConfig",
    "ScheduleOptimizerError",
    "OPTIMIZATION_REGISTRY",
    "get_optimization_component",
    "list_optimization_components",
    "register_optimization_component",
]

OPTIMIZATION_REGISTRY: Dict[str, Type[Any]] = {
    "genetic":        GeneticOptimizer,
    "bayesian":       BayesianOptimizer,
    "tuner":          HyperparameterTuner,
    "quantization":   QuantizationOptimizer,
    "schedule":       ScheduleOptimizer,
}


def register_optimization_component(name: str, cls: Type[Any]) -> None:
    """Register an optimization component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = OPTIMIZATION_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Optimization component '{name}' already registered to "
            f"{existing.__name__}."
        )
    OPTIMIZATION_REGISTRY[name] = cls
    logger.debug("Registered optimization component '%s' -> %s", name, cls.__name__)


def get_optimization_component(name: str) -> Type[Any]:
    """Return the optimization component class registered under ``name``."""
    try:
        return OPTIMIZATION_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown optimization component '{name}'. "
            f"Available: {sorted(OPTIMIZATION_REGISTRY)}"
        ) from exc


def list_optimization_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in OPTIMIZATION_REGISTRY.items()}


if _os.environ.get("GREEN_AGENT_VALIDATE_OPTIMIZATION") == "1":
    for _name, _cls in OPTIMIZATION_REGISTRY.items():
        try:
            logger.info("Optimization component '%s' importable.", _name)
        except Exception:  # pragma: no cover
            logger.exception("Optimization component '%s' failed.", _name)
            raise
