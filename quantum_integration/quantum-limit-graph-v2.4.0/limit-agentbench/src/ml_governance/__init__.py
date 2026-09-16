# src/ml_governance/__init__.py

"""
ML Governance
=============

Carbon-aware governance layer for ML adaptation:

- :class:`AdaptationStrategyClassifier` — recommends fine-tuning strategies
  based on task characteristics and carbon budget.
- :class:`CarbonLedgerService` — tracks carbon budgets and expenditures per
  team / model / experiment.
- :class:`ParameterEfficiencyPolicyEngine` — enforces parameter-efficiency
  policies (soft / moderate / strict).

All components follow the conventions used across the upgraded ``src/`` tree:
frozen config dataclasses, validation, UTC timestamps, bounded histories,
serialization, strict / non-strict modes, custom ``*Error`` exceptions,
``statistics()``, ``__repr__``, and ``__main__`` smoke tests.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

from .adaptation_classifier import (
    AdaptationClassifierError,
    AdaptationStrategy,
    AdaptationStrategyClassifier,
    ClassifierConfig,
    StrategyRecommendation,
)
from .carbon_ledger import (
    CarbonLedgerError,
    CarbonLedgerService,
    CarbonTransaction,
    LedgerConfig,
    TeamBudget,
)
from .policy_engine import (
    ParameterEfficiencyPolicyEngine,
    PolicyDecision,
    PolicyEngineError,
    PolicyMode,
)

logger = logging.getLogger(__name__)

__all__ = [
    # adaptation classifier
    "AdaptationClassifierError",
    "AdaptationStrategy",
    "AdaptationStrategyClassifier",
    "ClassifierConfig",
    "StrategyRecommendation",
    # carbon ledger
    "CarbonLedgerError",
    "CarbonLedgerService",
    "CarbonTransaction",
    "LedgerConfig",
    "TeamBudget",
    # policy engine
    "ParameterEfficiencyPolicyEngine",
    "PolicyDecision",
    "PolicyEngineError",
    "PolicyMode",
    # registry
    "GOVERNANCE_REGISTRY",
    "get_governance_component",
    "list_governance_components",
    "register_governance_component",
]

GOVERNANCE_REGISTRY: Dict[str, Type[Any]] = {
    "classifier":  AdaptationStrategyClassifier,
    "ledger":      CarbonLedgerService,
    "policy":      ParameterEfficiencyPolicyEngine,
}


def register_governance_component(name: str, cls: Type[Any]) -> None:
    """Register a governance component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = GOVERNANCE_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Governance component '{name}' already registered to "
            f"{existing.__name__}."
        )
    GOVERNANCE_REGISTRY[name] = cls
    logger.debug("Registered governance component '%s' -> %s", name, cls.__name__)


def get_governance_component(name: str) -> Type[Any]:
    """Return the governance component class registered under ``name``."""
    try:
        return GOVERNANCE_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown governance component '{name}'. "
            f"Available: {sorted(GOVERNANCE_REGISTRY)}"
        ) from exc


def list_governance_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in GOVERNANCE_REGISTRY.items()}


if _os.environ.get("GREEN_AGENT_VALIDATE_GOVERNANCE") == "1":
    for _name, _cls in GOVERNANCE_REGISTRY.items():
        try:
            logger.info("Governance component '%s' importable.", _name)
        except Exception:  # pragma: no cover
            logger.exception("Governance component '%s' failed.", _name)
            raise
