# src/constraints/__init__.py

"""
Constraints modules for Green_Agent (Enhanced)
================================================

Provides budget management and enforcement for the Green Agent's dual-axis
(carbon + helium) sustainability mission.

Original public API preserved:
    Budget, BudgetStatus, BudgetManager, BudgetEnforcer

Enhanced public API (grouped by enhancement layer)
--------------------------------------------------
  1. Quantum-Distillation        QuantumDistillationBridge,
                                 DistilledBudgetPolicy
  2. Causal RL                   CausalThresholdLearner, ThresholdState,
                                 CausalBudgetLearner, BudgetState
  3. Federated Green Learning    FederatedAggregator,
                                 FederatedBudgetProfile,
                                 FederatedBreachProfile
  4. Multi-Agent Coordination    MultiAgentCoordinator, AgentProfile,
                                 AgentRole
  5. Temporal Logic              TemporalLogicMonitor, RemainingNonNegative,
                                 StatusMonotonic, CanExecuteImpliesRemaining,
                                 AllViolationsReported, SeverityMonotonic
  6. Explainable AI              BudgetExplainer, BudgetExplanation,
                                 FeasibilityExplanation
  7. Adaptive Precision          AdaptivePrecisionController,
                                 HardwareProfile, PrecisionLevel
  8. Carbon Markets              CarbonMarketClient, MarketSnapshot
  9. Resilience & Chaos          CircuitBreaker, ChaosInjector, CircuitState
 10. Human-in-the-Loop           HumanInTheLoopGate, HITLRequest
 +   Helium awareness            HeliumProfiler
 +   Severity classification     Severity

Convenience helpers
-------------------
    create_budget_manager()    factory for BudgetManager
    create_budget_enforcer()   factory for BudgetEnforcer
    get_capabilities()         runtime introspection
    DEFAULT_FEATURES           default enhancement toggles
    ENABLE_ALL_FEATURES        preset to enable every layer
    DISABLE_ALL_FEATURES       preset to disable every layer

Metadata
--------
    __version__, __author__, __license__
"""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__version__ = "5.0.0"
__author__ = "AI Research Agent Team"
__license__ = "MIT"


# =============================================================================
# 1. Submodule loading with graceful fallbacks
# =============================================================================

_SUBMODULE_NAMES = (
    "budget_manager",
    "budget_enforcer",
    "budget_constraints",   # optional; may not exist
)

_SUBMODULES: Dict[str, Any] = {}


def _load_submodule(name: str) -> Optional[Any]:
    """Try relative, then absolute, then src-prefixed import paths."""
    for spec in (f".{name}", name, f"src.constraints.{name}"):
        try:
            if spec.startswith("."):
                mod = import_module(spec, package=__name__)
            else:
                mod = import_module(spec)
            logger.debug(f"constraints.{name} loaded from '{spec}'")
            return mod
        except ImportError:
            continue
        except Exception as e:
            logger.warning(
                f"constraints.{name} raised on import from '{spec}': {e}"
            )
            return None
    logger.debug(f"Optional submodule constraints.{name} not available")
    return None


for _name in _SUBMODULE_NAMES:
    _SUBMODULES[_name] = _load_submodule(_name)


# =============================================================================
# 2. Inline fallback stubs (used only when a submodule is missing)
# =============================================================================

if _SUBMODULES.get("budget_manager") is None:
    from enum import Enum as _Enum
    from dataclasses import dataclass as _dc, field as _f
    from datetime import datetime as _dt

    class BudgetStatus(_Enum):  # type: ignore[no-redef]
        UNDER_BUDGET = "under_budget"
        NEAR_LIMIT = "near_limit"
        AT_LIMIT = "at_limit"
        EXCEEDED = "exceeded"

    @_dc
    class Budget:  # type: ignore[no-redef]
        max_energy_wh: float
        max_carbon_g: float
        max_latency_ms: float
        max_cost_usd: Optional[float] = None
        warning_threshold: float = 0.80
        critical_threshold: float = 0.95
        name: str = "Default Budget"
        description: str = ""
        created_at: _dt = _f(default_factory=_dt.now)
        max_helium_units: Optional[float] = None

        def to_dict(self):
            return {
                'name': self.name,
                'max_energy_wh': self.max_energy_wh,
                'max_carbon_g': self.max_carbon_g,
                'max_latency_ms': self.max_latency_ms,
            }

    class BudgetManager:  # type: ignore[no-redef]
        def __init__(self, budget: Budget, **kwargs):
            self.budget = budget
            self.consumed = {
                'energy_wh': 0.0, 'carbon_g': 0.0,
                'latency_ms': 0.0, 'cost_usd': 0.0,
            }

        def check_budget(self, metric: str) -> BudgetStatus:
            return BudgetStatus.UNDER_BUDGET

        def can_execute(self, estimated):
            return True, []

        def record_consumption(self, actual, metadata=None):
            for k, v in actual.items():
                if k in self.consumed:
                    self.consumed[k] += v

        def get_summary(self):
            return {'consumed': dict(self.consumed)}


if _SUBMODULES.get("budget_enforcer") is None:
    class BudgetExceeded(Exception):  # type: ignore[no-redef]
        def __init__(self, message, details=None):
            super().__init__(message)
            self.details = details or {}

    class BudgetEnforcer:  # type: ignore[no-redef]
        def __init__(self, max_energy=None, max_carbon=None, max_latency=None, **kwargs):
            self.max_energy = max_energy
            self.max_carbon = max_carbon
            self.max_latency = max_latency

        def check(self, metrics: dict) -> None:
            if self.max_energy is not None and metrics.get("energy", 0) > self.max_energy:
                raise BudgetExceeded("Energy budget exceeded")
            if self.max_carbon is not None and metrics.get("carbon", 0) > self.max_carbon:
                raise BudgetExceeded("Carbon budget exceeded")
            if self.max_latency is not None and metrics.get("latency", 0) > self.max_latency:
                raise BudgetExceeded("Latency budget exceeded")


# =============================================================================
# 3. Symbol exposure helper
# =============================================================================

def _expose(
    submodule_name: str,
    *symbols: str,
    required: bool = False,
) -> List[str]:
    """
    Lift `symbols` from `submodule_name` into this package's globals.
    Missing symbols resolve to None so `from constraints import X` never
    raises for optional APIs. Returns the list of successfully exposed names.
    """
    mod = _SUBMODULES.get(submodule_name)
    exposed: List[str] = []
    for sym in symbols:
        if mod is not None and hasattr(mod, sym):
            globals()[sym] = getattr(mod, sym)
            exposed.append(sym)
        else:
            globals()[sym] = None
            if required:
                logger.error(
                    f"Required symbol '{sym}' missing from "
                    f"constraints.{submodule_name}"
                )
    return exposed


# =============================================================================
# 4. ORIGINAL four exports (backward compatible, required)
# =============================================================================

_CORE_BUDGET_MANAGER = _expose(
    "budget_manager", "Budget", "BudgetStatus", "BudgetManager",
    required=True,
)
_CORE_BUDGET_ENFORCER = _expose(
    "budget_enforcer", "BudgetEnforcer", required=True,
)
# Preserve original exception class if it exists in the enforcer module
_expose("budget_enforcer", "BudgetExceeded")


# =============================================================================
# 5. Enhancement-layer exports from budget_manager
# =============================================================================

_EXPORTED_FROM_MANAGER = _expose(
    "budget_manager",
    # 1. Quantum-Distillation
    "QuantumDistillationBridge", "DistilledBudgetPolicy",
    # 2. Causal RL
    "CausalThresholdLearner", "ThresholdState",
    # 3. Federated
    "FederatedAggregator", "FederatedBudgetProfile",
    # 4. Multi-Agent
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    # 5. Temporal Logic
    "TemporalLogicMonitor",
    "RemainingNonNegative", "StatusMonotonic", "CanExecuteImpliesRemaining",
    # 6. XAI
    "BudgetExplainer", "BudgetExplanation",
    # 7. Adaptive Precision
    "AdaptivePrecisionController", "HardwareProfile", "PrecisionLevel",
    # 8. Carbon Markets
    "CarbonMarketClient", "MarketSnapshot",
    # 9. Resilience
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    # 10. HITL
    "HumanInTheLoopGate", "HITLRequest",
    # Extras
    "Severity", "HeliumProfiler",
)


# =============================================================================
# 6. Enhancement-layer exports from budget_enforcer
# =============================================================================

_EXPORTED_FROM_ENFORCER = _expose(
    "budget_enforcer",
    # 2. Causal RL
    "CausalBudgetLearner", "BudgetState",
    # 3. Federated (breach profiles)
    "FederatedBreachProfile",
    # 5. Temporal Logic (additional props)
    "AllViolationsReported", "SeverityMonotonic",
    # 6. XAI (feasibility explanation)
    "FeasibilityExplanation",
)


# =============================================================================
# 7. Optional: budget_constraints module (check_energy_budget + enhanced)
# =============================================================================

_EXPORTED_FROM_CONSTRAINTS = _expose(
    "budget_constraints",
    "check_energy_budget",
    "check_energy_budget_detailed",
    "BudgetChecker", "EnhancedBudgetResult",
)


# =============================================================================
# 8. Feature toggle constants
# =============================================================================

DEFAULT_FEATURES: Dict[str, bool] = {
    "causal_rl": True,
    "xai": True,
    "adaptive_precision": True,
    "federated": True,
    "multi_agent": True,
    "temporal_logic": True,
    "carbon_market": True,
    "chaos_testing": False,
    "hitl": True,
    "quantum_distillation": True,
    "helium_awareness": True,
    "aggregate_violations": True,
}

ENABLE_ALL_FEATURES: Dict[str, bool] = {k: True for k in DEFAULT_FEATURES}
DISABLE_ALL_FEATURES: Dict[str, bool] = {k: False for k in DEFAULT_FEATURES}


# =============================================================================
# 9. Convenience factories
# =============================================================================

def _resolve_features(
    features: Optional[Dict[str, bool]],
) -> Optional[Dict[str, bool]]:
    return dict(features) if features is not None else None


def create_budget_manager(
    budget: Any,
    *,
    deployment_id: str = "local",
    agent_id: str = "budget-manager",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
) -> Any:
    """Build a fully-enhanced BudgetManager (or fallback)."""
    cls = globals().get("BudgetManager")
    if cls is None:
        raise ImportError("BudgetManager not available")
    try:
        return cls(
            budget=budget,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=_resolve_features(features),
            hardware=hardware,
        )
    except TypeError:
        # Submodule is the original, non-enhanced version
        return cls(budget)


def create_budget_enforcer(
    max_energy: Optional[float] = None,
    max_carbon: Optional[float] = None,
    max_latency: Optional[float] = None,
    max_helium: Optional[float] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "budget-enforcer",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
) -> Any:
    """Build a fully-enhanced BudgetEnforcer (or fallback)."""
    cls = globals().get("BudgetEnforcer")
    if cls is None:
        raise ImportError("BudgetEnforcer not available")
    try:
        return cls(
            max_energy=max_energy,
            max_carbon=max_carbon,
            max_latency=max_latency,
            max_helium=max_helium,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=_resolve_features(features),
            hardware=hardware,
        )
    except TypeError:
        # Fallback to original signature
        return cls(
            max_energy=max_energy,
            max_carbon=max_carbon,
            max_latency=max_latency,
        )


def create_constraints_suite(
    budget: Optional[Any] = None,
    *,
    max_energy: Optional[float] = None,
    max_carbon: Optional[float] = None,
    max_latency: Optional[float] = None,
    max_helium: Optional[float] = None,
    deployment_id: str = "local",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
) -> Dict[str, Any]:
    """
    Build manager + enforcer with a consistent configuration.

    Returns:
        {"manager": BudgetManager, "enforcer": BudgetEnforcer}
    """
    manager = None
    if budget is not None:
        try:
            manager = create_budget_manager(
                budget=budget,
                deployment_id=deployment_id,
                agent_id=f"{deployment_id}-manager",
                features=features,
                hardware=hardware,
            )
        except ImportError:
            manager = None

    enforcer = None
    try:
        enforcer = create_budget_enforcer(
            max_energy=max_energy,
            max_carbon=max_carbon,
            max_latency=max_latency,
            max_helium=max_helium,
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-enforcer",
            features=features,
            hardware=hardware,
        )
    except ImportError:
        enforcer = None

    return {"manager": manager, "enforcer": enforcer}


# =============================================================================
# 10. Capability introspection
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """
    Return a summary of what this constraints package can do.

    Useful for runtime feature discovery and self-documentation.
    """
    return {
        "version": __version__,
        "submodules_loaded": {
            name: (mod is not None)
            for name, mod in _SUBMODULES.items()
        },
        "using_fallback_stubs": {
            name: (mod is None)
            for name, mod in _SUBMODULES.items()
        },
        "exported_symbols": {
            "core_budget_manager": list(_CORE_BUDGET_MANAGER),
            "core_budget_enforcer": list(_CORE_BUDGET_ENFORCER),
            "from_budget_manager": list(_EXPORTED_FROM_MANAGER),
            "from_budget_enforcer": list(_EXPORTED_FROM_ENFORCER),
            "from_budget_constraints": list(_EXPORTED_FROM_CONSTRAINTS),
        },
        "features_available": list(DEFAULT_FEATURES),
        "components": {
            "budget_manager_available": globals().get("BudgetManager") is not None,
            "budget_enforcer_available": globals().get("BudgetEnforcer") is not None,
            "budget_checker_available": globals().get("BudgetChecker") is not None,
        },
    }


# =============================================================================
# 11. Public API surface (`__all__`)
# =============================================================================

__all__: List[str] = []

# --- Original four (always present, even via fallback) ---
for _core in ("Budget", "BudgetStatus", "BudgetManager", "BudgetEnforcer"):
    if globals().get(_core) is not None:
        __all__.append(_core)

# --- Original exception (if submodule provides it) ---
if globals().get("BudgetExceeded") is not None:
    __all__.append("BudgetExceeded")

# --- Factory & introspection helpers ---
__all__ += [
    "create_budget_manager",
    "create_budget_enforcer",
    "create_constraints_suite",
    "get_capabilities",
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "DISABLE_ALL_FEATURES",
    "__version__",
    "__author__",
    "__license__",
]

# --- Enhancement symbols (added only if resolved to non-None) ---
_ENHANCEMENT_SYMBOLS = (
    # 1. Distillation
    "QuantumDistillationBridge", "DistilledBudgetPolicy",
    # 2. Causal RL
    "CausalThresholdLearner", "ThresholdState",
    "CausalBudgetLearner", "BudgetState",
    # 3. Federated
    "FederatedAggregator", "FederatedBudgetProfile",
    "FederatedBreachProfile",
    # 4. Multi-agent
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    # 5. Temporal logic
    "TemporalLogicMonitor",
    "RemainingNonNegative", "StatusMonotonic",
    "CanExecuteImpliesRemaining",
    "AllViolationsReported", "SeverityMonotonic",
    # 6. XAI
    "BudgetExplainer", "BudgetExplanation", "FeasibilityExplanation",
    # 7. Adaptive precision
    "AdaptivePrecisionController", "HardwareProfile", "PrecisionLevel",
    # 8. Carbon markets
    "CarbonMarketClient", "MarketSnapshot",
    # 9. Resilience
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    # 10. HITL
    "HumanInTheLoopGate", "HITLRequest",
    # Extras
    "Severity", "HeliumProfiler",
    # Optional budget_constraints module
    "check_energy_budget", "check_energy_budget_detailed",
    "BudgetChecker", "EnhancedBudgetResult",
)

for _sym in _ENHANCEMENT_SYMBOLS:
    if globals().get(_sym) is not None and _sym not in __all__:
        __all__.append(_sym)


# =============================================================================
# 12. Load diagnostics
# =============================================================================

if logger.isEnabledFor(logging.DEBUG):
    _loaded = [n for n, m in _SUBMODULES.items() if m is not None]
    _missing = [n for n, m in _SUBMODULES.items() if m is None]
    logger.debug(
        f"constraints package ready: {len(__all__)} public symbols "
        f"(loaded={_loaded}, stubs_used={_missing})"
    )
