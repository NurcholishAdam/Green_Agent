# src/integration/__init__.py

"""
Integration module for Green Agent (Enhanced)
=============================================

Provides free API integrations and community data sharing.

Original public API (backward compatible)
-----------------------------------------
    FreeAPIManager
    StaticDataProvider
    FreeWeatherAPI
    FreeGridCarbonAPI
    SelfHostedGridCarbon
    HeliumMarketSimulator
    CommunityDataHub
    CarbonData
    WeatherData
    HeliumData
    GridMixData

Enhanced public API (new, grouped by layer)
-------------------------------------------
    1. Quantum-Distillation:
         QuantumDistillationBridge, DistilledHeliumModel
    2. Causal RL:
         CausalSourcePolicy, SourceState
    3. Federated Green Learning:
         FederatedObservationAggregator, FederatedObservationProfile
    4. Multi-Agent Coordination:
         MultiAgentCoordinator, AgentProfile, AgentRole
    5. Temporal Logic:
         TemporalLogicMonitor, DataFreshness, CarbonBounds,
         PriceBounds, ConfidenceBounded
    6. Explainable AI:
         DataExplainer, SourceExplanation
    7. Adaptive Precision:
         AdaptivePrecisionController, HardwareProfile
    8. Carbon Markets:
         CarbonMarketClient, MarketSnapshot
    9. Resilience & Chaos:
         CircuitBreaker, ChaosInjector, CircuitState
   10. Human-in-the-Loop:
         HumanInTheLoopGate, HITLRequest

Supporting enums / helpers
--------------------------
    PrecisionLevel, ProviderKind, DataTier
    DEFAULT_FEATURES  (feature toggle set)
    create_manager()  (convenience factory)
    ENABLE_ALL_FEATURES / DISABLE_ALL_FEATURES  (preset dicts)
    __version__
"""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"
__author__ = "AI Research Agent Team"
__license__ = "MIT"


# =============================================================================
# Module access
# =============================================================================
# Import the submodule once. All exports are lifted from `.free_apis`, which
# may be the enhanced version or the legacy version. Missing symbols are
# tolerated so this package stays importable during partial upgrades.

try:
    _free_apis = import_module(".free_apis", package=__name__)
    _SUBMODULE_AVAILABLE = True
except ImportError as _exc:  # pragma: no cover — submodule should exist
    logger.error(f"Could not import .free_apis: {_exc}")
    _free_apis = None  # type: ignore[assignment]
    _SUBMODULE_AVAILABLE = False


def _expose(
    *names: str,
    required: bool = False,
    layer: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    """
    Lift `names` from the `.free_apis` submodule into this module's globals.

    Returns (exposed_names, missing_names). Missing symbols are set to None
    so that `from src.integration import X` never raises for optional APIs.
    """
    exposed: List[str] = []
    missing: List[str] = []

    if _free_apis is None:
        for n in names:
            globals()[n] = None
            missing.append(n)
        return exposed, missing

    for n in names:
        if hasattr(_free_apis, n):
            globals()[n] = getattr(_free_apis, n)
            exposed.append(n)
        else:
            globals()[n] = None
            missing.append(n)

    if missing:
        msg = (
            f"Missing {'required' if required else 'optional'} "
            f"symbols{(' for layer ' + layer) if layer else ''}: {missing}"
        )
        (logger.error if required else logger.debug)(msg)

    return exposed, missing


# =============================================================================
# 1. Core exports (original API — required)
# =============================================================================

_CORE, _CORE_MISSING = _expose(
    "FreeAPIManager",
    "StaticDataProvider",
    "FreeWeatherAPI",
    "FreeGridCarbonAPI",
    "SelfHostedGridCarbon",
    "HeliumMarketSimulator",
    "CommunityDataHub",
    "CarbonData",
    "WeatherData",
    "HeliumData",
    "GridMixData",
    required=True,
)


# =============================================================================
# 2. Enhancement exports (grouped by layer, all optional)
# =============================================================================

# --- 1. Quantum-Distillation ---
_ENH_QUANTUM, _ = _expose(
    "QuantumDistillationBridge", "DistilledHeliumModel",
    layer="Quantum-Distillation",
)

# --- 2. Causal RL ---
_ENH_CAUSAL_RL, _ = _expose(
    "CausalSourcePolicy", "SourceState",
    layer="Causal RL",
)

# --- 3. Federated Green Learning ---
_ENH_FEDERATED, _ = _expose(
    "FederatedObservationAggregator", "FederatedObservationProfile",
    layer="Federated",
)

# --- 4. Multi-Agent Coordination ---
_ENH_MULTI_AGENT, _ = _expose(
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    layer="Multi-Agent",
)

# --- 5. Temporal Logic ---
_ENH_TEMPORAL, _ = _expose(
    "TemporalLogicMonitor", "DataFreshness", "CarbonBounds",
    "PriceBounds", "ConfidenceBounded",
    layer="Temporal Logic",
)

# --- 6. Explainable AI ---
_ENH_XAI, _ = _expose(
    "DataExplainer", "SourceExplanation",
    layer="XAI",
)

# --- 7. Adaptive Precision ---
_ENH_PRECISION, _ = _expose(
    "AdaptivePrecisionController", "HardwareProfile",
    layer="Adaptive Precision",
)

# --- 8. Carbon Markets ---
_ENH_MARKETS, _ = _expose(
    "CarbonMarketClient", "MarketSnapshot",
    layer="Carbon Markets",
)

# --- 9. Resilience & Chaos ---
_ENH_RESILIENCE, _ = _expose(
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    layer="Resilience & Chaos",
)

# --- 10. HITL & Active Learning ---
_ENH_HITL, _ = _expose(
    "HumanInTheLoopGate", "HITLRequest",
    layer="HITL",
)

# --- Supporting enums ---
_ENH_ENUMS, _ = _expose(
    "PrecisionLevel", "ProviderKind", "DataTier",
    layer="Enums",
)


# =============================================================================
# 3. Feature toggle exposure
# =============================================================================

# Expose FreeAPIManager.DEFAULT_FEATURES as DEFAULT_FEATURES at package level.
DEFAULT_FEATURES: Dict[str, bool] = {}
if _free_apis is not None:
    _manager = getattr(_free_apis, "FreeAPIManager", None)
    if _manager is not None and hasattr(_manager, "DEFAULT_FEATURES"):
        DEFAULT_FEATURES = dict(_manager.DEFAULT_FEATURES)

# Convenience presets so callers can enable/disable every layer with one line.
ENABLE_ALL_FEATURES: Dict[str, bool] = {
    k: True for k in DEFAULT_FEATURES
}
DISABLE_ALL_FEATURES: Dict[str, bool] = {
    k: False for k in DEFAULT_FEATURES
}


# =============================================================================
# 4. Convenience factory
# =============================================================================

def create_manager(
    config: Optional[Dict[str, Any]] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "free-api-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
    enable_all: bool = True,
) -> "FreeAPIManager":
    """
    Create a fully-configured FreeAPIManager with enhancement layers wired in.

    Args:
        config: Optional raw config dict passed to FreeAPIManager.
        deployment_id: Identifier for federated learning.
        agent_id: Identifier for multi-agent coordination.
        features: Optional explicit feature toggle dict. Overrides enable_all.
        hardware: Optional HardwareProfile instance.
        enable_all: When True and `features` is None, enables all enhancements.

    Returns:
        A FreeAPIManager instance (or the closest available substitute).
    """
    if FreeAPIManager is None:  # type: ignore[truthy-function]
        raise ImportError(
            "FreeAPIManager is not available. Ensure "
            "src/integration/free_apis.py is importable."
        )

    # Resolve feature toggles
    if features is None:
        features = ENABLE_ALL_FEATURES if enable_all else DISABLE_ALL_FEATURES

    # Build kwargs conservatively — older FreeAPIManager versions may not
    # accept all keyword args, so fall back gracefully.
    kwargs: Dict[str, Any] = {"config": config or {}}
    for key, value in {
        "deployment_id": deployment_id,
        "agent_id": agent_id,
        "features": features,
        "hardware": hardware,
    }.items():
        if value is None:
            continue
        try:
            # Probe by inspecting the constructor signature once
            import inspect
            sig = inspect.signature(FreeAPIManager.__init__)  # type: ignore[misc]
            if key in sig.parameters:
                kwargs[key] = value
        except (TypeError, ValueError):
            # If signature inspection fails, optimistically pass it
            kwargs[key] = value

    return FreeAPIManager(**kwargs)  # type: ignore[misc]


# =============================================================================
# 5. Capability introspection
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """
    Return a summary of which enhancement layers are available.

    Useful for runtime feature discovery and self-documentation.
    """
    return {
        "version": __version__,
        "submodule_available": _SUBMODULE_AVAILABLE,
        "core_symbols": list(_CORE),
        "core_missing": list(_CORE_MISSING),
        "layers": {
            "quantum_distillation": [n for n in _ENH_QUANTUM],
            "causal_rl": [n for n in _ENH_CAUSAL_RL],
            "federated": [n for n in _ENH_FEDERATED],
            "multi_agent": [n for n in _ENH_MULTI_AGENT],
            "temporal_logic": [n for n in _ENH_TEMPORAL],
            "xai": [n for n in _ENH_XAI],
            "adaptive_precision": [n for n in _ENH_PRECISION],
            "carbon_markets": [n for n in _ENH_MARKETS],
            "resilience": [n for n in _ENH_RESILIENCE],
            "hitl": [n for n in _ENH_HITL],
            "enums": [n for n in _ENH_ENUMS],
        },
        "features_available": list(DEFAULT_FEATURES),
    }


# =============================================================================
# 6. Public API surface (`__all__`)
# =============================================================================
# Grouped by category for readability. Only symbols that are non-None are
# included, so `from src.integration import *` never leaks None placeholders.

__all__: List[str] = []

# --- Core (original) ---
__all__ += [
    "FreeAPIManager",
    "StaticDataProvider",
    "FreeWeatherAPI",
    "FreeGridCarbonAPI",
    "SelfHostedGridCarbon",
    "HeliumMarketSimulator",
    "CommunityDataHub",
    "CarbonData",
    "WeatherData",
    "HeliumData",
    "GridMixData",
]

# --- 1. Quantum-Distillation ---
__all__ += ["QuantumDistillationBridge", "DistilledHeliumModel"]

# --- 2. Causal RL ---
__all__ += ["CausalSourcePolicy", "SourceState"]

# --- 3. Federated ---
__all__ += [
    "FederatedObservationAggregator", "FederatedObservationProfile",
]

# --- 4. Multi-Agent ---
__all__ += ["MultiAgentCoordinator", "AgentProfile", "AgentRole"]

# --- 5. Temporal Logic ---
__all__ += [
    "TemporalLogicMonitor", "DataFreshness", "CarbonBounds",
    "PriceBounds", "ConfidenceBounded",
]

# --- 6. XAI ---
__all__ += ["DataExplainer", "SourceExplanation"]

# --- 7. Adaptive Precision ---
__all__ += ["AdaptivePrecisionController", "HardwareProfile"]

# --- 8. Carbon Markets ---
__all__ += ["CarbonMarketClient", "MarketSnapshot"]

# --- 9. Resilience & Chaos ---
__all__ += ["CircuitBreaker", "ChaosInjector", "CircuitState"]

# --- 10. HITL ---
__all__ += ["HumanInTheLoopGate", "HITLRequest"]

# --- Supporting enums ---
__all__ += ["PrecisionLevel", "ProviderKind", "DataTier"]

# --- Package-level helpers ---
__all__ += [
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "DISABLE_ALL_FEATURES",
    "create_manager",
    "get_capabilities",
    "__version__",
]

# Filter out any names that resolved to None (defensive; keeps `*` imports clean)
__all__ = [n for n in __all__ if globals().get(n, object()) is not None]


# =============================================================================
# 7. Package load diagnostics
# =============================================================================

if _SUBMODULE_AVAILABLE and not _CORE_MISSING:
    logger.debug(
        "src.integration ready: %d core, %d enhancement symbols exposed",
        len(_CORE),
        sum(len(x) for x in (
            _ENH_QUANTUM, _ENH_CAUSAL_RL, _ENH_FEDERATED,
            _ENH_MULTI_AGENT, _ENH_TEMPORAL, _ENH_XAI,
            _ENH_PRECISION, _ENH_MARKETS, _ENH_RESILIENCE,
            _ENH_HITL, _ENH_ENUMS,
        )),
    )
elif _CORE_MISSING:
    logger.warning(
        "src.integration loaded with missing core symbols: %s",
        _CORE_MISSING,
    )
