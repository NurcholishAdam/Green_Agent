# src/feedback/__init__.py

"""
Feedback & RLHF Subsystem
=========================

Layer 1 (Meta-Cognition) feedback pipeline. Collects agent reasoning traces,
computes sustainability metrics, emits them through pluggable sinks, and
produces human-readable feedback plus actionable improvement suggestions.

Components
----------
- :func:`generate_energy_feedback` — human-readable energy feedback.
- :class:`SustainabilityImprovementSuggester` — actionable suggestions.
- :class:`MetricSink` / :class:`StdoutSink` / :class:`InMemorySink` /
  :class:`JsonlSink` — pluggable metric emitters.
- :class:`ReasoningTraceAnalyzer` — reasoning quality analysis.
- :class:`RLHFFeedbackEngine` — top-level orchestrator.

All components follow the conventions used across the upgraded ``src/`` tree:
``RLock``-guarded state, bounded histories, strict / non-strict modes,
serialization, custom ``*Error`` exceptions, ``statistics()``, ``__repr__``,
and ``__main__`` smoke tests.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

from .energy_feedback import (
    EnergyFeedback,
    EnergyFeedbackConfig,
    EnergyFeedbackError,
    generate_energy_feedback,
)

from .improvement_suggester import (
    ImprovementSuggestion,
    SuggesterConfig,
    SuggesterError,
    SustainabilityImprovementSuggester,
)

from .metric_sink import (
    InMemorySink,
    JsonlSink,
    MetricSink,
    MetricSinkError,
    StdoutSink,
)

from .reasoning_analyzer import (
    ReasoningAnalysis,
    ReasoningConfig,
    ReasoningError,
    ReasoningTraceAnalyzer,
)

from .rlhf_engine import (
    RLHFConfig,
    RLHFFeedbackEngine,
    RLHFRecord,
    RLHFScore,
    RLHFEngineError,
)

logger = logging.getLogger(__name__)

__all__ = [
    # energy feedback
    "EnergyFeedback",
    "EnergyFeedbackConfig",
    "EnergyFeedbackError",
    "generate_energy_feedback",
    # improvement suggester
    "ImprovementSuggestion",
    "SuggesterConfig",
    "SuggesterError",
    "SustainabilityImprovementSuggester",
    # metric sinks
    "InMemorySink",
    "JsonlSink",
    "MetricSink",
    "MetricSinkError",
    "StdoutSink",
    # reasoning analyzer
    "ReasoningAnalysis",
    "ReasoningConfig",
    "ReasoningError",
    "ReasoningTraceAnalyzer",
    # rlhf engine
    "RLHFConfig",
    "RLHFFeedbackEngine",
    "RLHFRecord",
    "RLHFScore",
    "RLHFEngineError",
    # registry
    "FEEDBACK_REGISTRY",
    "get_feedback_component",
    "list_feedback_components",
    "register_feedback_component",
]

# --------------------------------------------------------------------------- #
# Component registry
# --------------------------------------------------------------------------- #
FEEDBACK_REGISTRY: Dict[str, Type[Any]] = {
    "suggester":  SustainabilityImprovementSuggester,
    "analyzer":   ReasoningTraceAnalyzer,
    "engine":     RLHFFeedbackEngine,
    "sink":       MetricSink,
    "stdout":     StdoutSink,
    "memory":     InMemorySink,
    "jsonl":      JsonlSink,
}


def register_feedback_component(name: str, cls: Type[Any]) -> None:
    """Register a feedback component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = FEEDBACK_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Feedback component '{name}' already registered to "
            f"{existing.__name__}."
        )
    FEEDBACK_REGISTRY[name] = cls
    logger.debug("Registered feedback component '%s' -> %s", name, cls.__name__)


def get_feedback_component(name: str) -> Type[Any]:
    """Return the feedback component class registered under ``name``."""
    try:
        return FEEDBACK_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown feedback component '{name}'. "
            f"Available: {sorted(FEEDBACK_REGISTRY)}"
        ) from exc


def list_feedback_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {name: cls.__name__ for name, cls in FEEDBACK_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_FEEDBACK") == "1":
    for _name, _cls in FEEDBACK_REGISTRY.items():
        try:
            logger.info("Feedback component '%s' (%s) importable.", _name, _cls.__name__)
        except Exception:  # pragma: no cover
            logger.exception("Feedback component '%s' failed validation.", _name)
            raise
