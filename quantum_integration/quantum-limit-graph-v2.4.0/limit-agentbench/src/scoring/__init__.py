# src/scoring/__init__.py

"""
Scoring modules for Green_Agent
===============================

Robust scoring and failure-classification for agent evaluation:

- :class:`FailureClassifier` — categorizes agent failures for debugging.
- :class:`RobustSustainabilityScorer` — handles all failure modes gracefully
  and returns consistent scoring dictionaries.

Enhancements
------------
- Registry pattern (``SCORING_REGISTRY`` + ``register_scoring_component`` /
  ``get_scoring_component`` / ``list_scoring_components``) consistent with
  the rest of the upgraded ``src/`` tree.
- Availability manifest (``SCORING_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SCORING=1``.
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
SCORING_AVAILABILITY: Dict[str, bool] = {}

try:
    from .failure_classifier import (
        FailureCategory,
        FailureClassifier,
        FailureClassifierConfig,
        FailureClassifierError,
    )

    SCORING_AVAILABILITY["failure_classifier"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("failure_classifier unavailable: %s", exc)
    FailureCategory = None  # type: ignore[assignment,misc]
    FailureClassifier = None  # type: ignore[assignment,misc]
    FailureClassifierConfig = None  # type: ignore[assignment,misc]
    FailureClassifierError = None  # type: ignore[assignment,misc]
    SCORING_AVAILABILITY["failure_classifier"] = False

try:
    from .robust_scorer import (
        RobustScorerConfig,
        RobustScorerError,
        RobustSustainabilityScorer,
        ScoringResult,
    )

    SCORING_AVAILABILITY["robust_scorer"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("robust_scorer unavailable: %s", exc)
    RobustScorerConfig = None  # type: ignore[assignment,misc]
    RobustScorerError = None  # type: ignore[assignment,misc]
    RobustSustainabilityScorer = None  # type: ignore[assignment,misc]
    ScoringResult = None  # type: ignore[assignment,misc]
    SCORING_AVAILABILITY["robust_scorer"] = False

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "FailureCategory",
    "FailureClassifier",
    "FailureClassifierConfig",
    "FailureClassifierError",
    "RobustSustainabilityScorer",
    "RobustScorerConfig",
    "RobustScorerError",
    "ScoringResult",
    "SCORING_REGISTRY",
    "SCORING_AVAILABILITY",
    "get_scoring_component",
    "list_scoring_components",
    "register_scoring_component",
]

# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
SCORING_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        SCORING_REGISTRY[name] = cls


_register_if_available("classifier", FailureClassifier)
_register_if_available("scorer", RobustSustainabilityScorer)

def register_scoring_component(name: str, cls: Type[Any]) -> None:
    """Register a scoring component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = SCORING_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Scoring component '{name}' already registered to "
            f"{existing.__name__}."
        )
    SCORING_REGISTRY[name] = cls
    logger.debug("Registered scoring component '%s' -> %s", name, cls.__name__)

def get_scoring_component(name: str) -> Type[Any]:
    """Return the scoring component class registered under ``name``."""
    try:
        return SCORING_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown scoring component '{name}'. "
            f"Available: {sorted(SCORING_REGISTRY)}"
        ) from exc

def list_scoring_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in SCORING_REGISTRY.items()}

# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_SCORING") == "1":
    _missing = [k for k, ok in SCORING_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Scoring sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(SCORING_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Scoring component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Scoring component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
