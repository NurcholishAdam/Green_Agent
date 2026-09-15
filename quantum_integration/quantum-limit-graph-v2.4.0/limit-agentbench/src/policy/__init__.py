# policy/__init__.py

"""
Policy enforcement and feedback modules (Enhanced)
==================================================

Public API for the Green Agent's policy layer — reward shaping, temporal
shifting, reflective explanation, and multi-category metric alerting — all
enhanced with the ten cross-cutting enhancement layers.

Layer-to-class mapping
----------------------
  1. Quantum-Distillation         DistilledRewardPolicy
                                  DistilledExplanationPolicy
                                  DistilledAlertPolicy
                                  QuantumDistillationBridge
  2. Causal RL                    CausalPenaltyLearner
                                  CausalConfidenceLearner
                                  CausalThresholdLearner
  3. Federated Green Learning     FederatedAggregator
                                  FederatedRewardProfile
                                  FederatedExplanationProfile
                                  FederatedAlertProfile
  4. Multi-Agent Coordination     MultiAgentCoordinator
                                  AgentProfile, AgentRole
  5. Temporal Logic               TemporalLogicMonitor
                                  DeadlineRespected, NonNegativeSaving,
                                  MaxDelayBounded, NonNegativeEnergy,
                                  MaxAlertRate, NoAlertDuringCriticalSection
  6. Explainable AI (XAI)         RewardExplainer, RewardExplanation
                                  StructuredExplainer, StructuredExplanation
                                  AlertExplainer, StructuredAlert
                                  ExplanationTier
  7. Adaptive Precision           AdaptivePrecisionController
                                  HardwareProfile, PrecisionLevel
  8. Carbon Markets               CarbonMarketClient, MarketSnapshot
  9. Resilience & Chaos           CircuitBreaker, ChaosInjector, CircuitState
 10. Human-in-the-Loop            HumanInTheLoopGate, HITLRequest

Public entry points
-------------------
    PolicyEngine        reward shaping + temporal shift
    PolicyFeedback      reflective explanation generator
    PolicyReporter      multi-category metric alerting

Convenience helpers
-------------------
    create_engine()          factory for PolicyEngine
    create_feedback()        factory for PolicyFeedback
    create_reporter()        factory for PolicyReporter
    create_policy_suite()    all three, sharing configuration
    get_capabilities()       runtime introspection
    DEFAULT_FEATURES         default enhancement toggles
    ENABLE_ALL_FEATURES      preset to enable every layer
    DISABLE_ALL_FEATURES     preset to disable every layer

Metadata
--------
    __version__, __author__, __license__
"""

from __future__ import annotations

import logging
import math
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"
__author__ = "Green Agent Team"
__license__ = "MIT"


# =============================================================================
# 1. Load submodules with graceful fallbacks
# =============================================================================

_SUBMODULE_NAMES = (
    "policy_engine",
    "policy_feedback",
    "policy_reporter",
)

_SUBMODULES: Dict[str, Any] = {}


def _load_submodule(name: str) -> Optional[Any]:
    """Attempt to import `name`; try relative first, then absolute."""
    for spec in (f".{name}", name, f"src.policy.{name}"):
        try:
            if spec.startswith("."):
                mod = import_module(spec, package=__name__)
            else:
                mod = import_module(spec)
            logger.debug(f"policy.{name} loaded from '{spec}'")
            return mod
        except ImportError:
            continue
        except Exception as e:
            logger.warning(f"policy.{name} raised on import from '{spec}': {e}")
            return None
    logger.debug(f"Optional submodule policy.{name} not available")
    return None


for _name in _SUBMODULE_NAMES:
    _SUBMODULES[_name] = _load_submodule(_name)


# =============================================================================
# 2. Inline fallback stubs (used only when a submodule is missing)
# =============================================================================

# -- Fallback PolicyEngine --
if _SUBMODULES.get("policy_engine") is None:
    from dataclasses import dataclass as _dc, field as _f
    from enum import Enum as _Enum
    from datetime import datetime as _dt
    from collections import deque as _deque, defaultdict as _dd
    import random as _rnd
    import statistics as _st

    class PolicyEngine:  # type: ignore[no-redef]
        """Minimal fallback PolicyEngine (matches original semantics)."""
        def __init__(self, energy_budget, baseline_energy=None, **kwargs):
            self.energy_budget = float(energy_budget)
            self.baseline_energy = baseline_energy
            try:
                from green_agent.rewards.negawatt_reward import NegawattReward
                self.negawatt_module = (
                    NegawattReward(baseline_energy)
                    if baseline_energy is not None else None
                )
            except Exception:
                self.negawatt_module = None

        def compute_sustainability_reward(self, accuracy: float, energy: float) -> float:
            if self.negawatt_module is None:
                return 0.0
            reward = float(self.negawatt_module.combined_reward(
                accuracy=accuracy, energy=energy
            ))
            if energy > self.energy_budget:
                reward -= 1.0
            return reward

        def suggest_temporal_shift(self, shifter, forecast_engine,
                                    energy_kwh, shiftable=True, urgency="low",
                                    **_):
            if not shiftable or urgency == "high":
                return 0, 0.0
            current = forecast_engine.current_intensity()
            forecast = forecast_engine.forecast_next_hours(4)
            return shifter.suggest(current, forecast, energy_kwh)


# -- Fallback PolicyFeedback --
if _SUBMODULES.get("policy_feedback") is None:
    class PolicyFeedback:  # type: ignore[no-redef]
        """Minimal fallback PolicyFeedback (matches original semantics)."""
        def generate(self, decision, before, after, **_):
            return {
                "decision": decision,
                "explanation": (
                    f"I selected mode '{decision}' because energy shifted "
                    f"from {before.get('energy')} to {after.get('energy')}."
                ),
                "tradeoff": {
                    "latency_delta": after.get("latency", 0) - before.get("latency", 0),
                    "energy_delta": after.get("energy", 0) - before.get("energy", 0),
                },
                "confidence": 0.75,
            }


# -- Fallback PolicyReporter --
if _SUBMODULES.get("policy_reporter") is None:
    class PolicyReporter:  # type: ignore[no-redef]
        """Minimal fallback PolicyReporter (matches original semantics)."""
        def __init__(self, policy: Dict):
            self.threshold = policy.get("reporting", {}).get(
                "alert_threshold_energy_pct", 80
            )

        def report(self, metrics: Dict) -> Dict:
            alerts = []
            if metrics.get("energy_pct", 0) >= self.threshold:
                alerts.append("Energy budget nearing limit")
            metrics["policy_alerts"] = alerts
            return metrics


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

    Missing symbols are set to None so `from policy import X` never
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
                    f"Required symbol '{sym}' missing from policy.{submodule_name}"
                )
    return exposed


# =============================================================================
# 4. Expose core components
# =============================================================================

_CORE_ENGINE = _expose(
    "policy_engine", "PolicyEngine", required=True,
)
_CORE_FEEDBACK = _expose(
    "policy_feedback", "PolicyFeedback", required=True,
)
_CORE_REPORTER = _expose(
    "policy_reporter", "PolicyReporter", required=True,
)


# =============================================================================
# 5. Expose enhancement-layer symbols
# =============================================================================

# --- From policy_engine ---
_EXPORTED_ENGINE_ENH = _expose(
    "policy_engine",
    # 1. Quantum-Distillation
    "QuantumDistillationBridge", "DistilledRewardPolicy",
    # 2. Causal RL
    "CausalPenaltyLearner", "PenaltyState",
    # 3. Federated
    "FederatedAggregator", "FederatedRewardProfile",
    # 4. Multi-agent
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    # 5. Temporal logic
    "TemporalLogicMonitor", "DeadlineRespected", "NonNegativeSaving",
    "MaxDelayBounded", "NonNegativeEnergy",
    # 6. XAI
    "RewardExplainer", "RewardExplanation",
    # 7. Precision
    "AdaptivePrecisionController", "HardwareProfile", "PrecisionLevel",
    # 8. Carbon markets
    "CarbonMarketClient", "MarketSnapshot",
    # 9. Resilience
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    # 10. HITL
    "HumanInTheLoopGate", "HITLRequest",
)

# --- From policy_feedback ---
_EXPORTED_FEEDBACK_ENH = _expose(
    "policy_feedback",
    # 1. Quantum-Distillation
    "DistilledExplanationPolicy",
    # 2. Causal RL
    "CausalConfidenceLearner", "ConfidenceState",
    # 3. Federated
    "FederatedExplanationProfile",
    # 5. Temporal logic (additional props)
    "NonNegativeLatency", "NonNegativeCarbon",
    # 6. XAI
    "StructuredExplainer", "StructuredExplanation", "ExplanationTier",
    # 8. Carbon markets
    # (shares CarbonMarketClient + MarketSnapshot with engine)
    # 10. HITL
    # (shares HumanInTheLoopGate + HITLRequest with engine)
)

# --- From policy_reporter ---
_EXPORTED_REPORTER_ENH = _expose(
    "policy_reporter",
    # 1. Quantum-Distillation
    "DistilledAlertPolicy",
    # 2. Causal RL
    "CausalThresholdLearner", "ThresholdState",
    # 3. Federated
    "FederatedAlertProfile",
    # 5. Temporal logic (additional props)
    "MaxAlertRate", "NoAlertDuringCriticalSection",
    # 6. XAI
    "AlertExplainer", "StructuredAlert",
    # 10. HITL
    # (shares HumanInTheLoopGate + HITLRequest with engine)
    # Reporting enums
    "Severity", "AlertCategory",
)


# =============================================================================
# 6. Feature toggle constants
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
    "carbon_awareness": True,
    "multi_category": True,
    "deduplication": True,
}

ENABLE_ALL_FEATURES: Dict[str, bool] = {k: True for k in DEFAULT_FEATURES}
DISABLE_ALL_FEATURES: Dict[str, bool] = {k: False for k in DEFAULT_FEATURES}


# =============================================================================
# 7. Convenience factories
# =============================================================================

def _resolve_features(
    features: Optional[Dict[str, bool]],
) -> Optional[Dict[str, bool]]:
    """Return a copy of `features` or None if the caller didn't pass any."""
    return dict(features) if features is not None else None


def create_engine(
    energy_budget: float,
    baseline_energy: Optional[float] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "policy-engine",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
) -> Any:
    """
    Build a fully-enhanced PolicyEngine.

    Falls back to the inline stub if the submodule is unavailable.
    """
    cls = globals().get("PolicyEngine")
    if cls is None:
        raise ImportError(
            "PolicyEngine is not available. Ensure policy/policy_engine.py "
            "is importable, or that the inline stub has been loaded."
        )
    try:
        return cls(
            energy_budget=energy_budget,
            baseline_energy=baseline_energy,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=_resolve_features(features),
            hardware=hardware,
        )
    except TypeError:
        # Submodule is the original, non-enhanced version
        return cls(
            energy_budget=energy_budget,
            baseline_energy=baseline_energy,
        )


def create_feedback(
    *,
    deployment_id: str = "local",
    agent_id: str = "policy-feedback",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
    base_confidence: float = 0.75,
) -> Any:
    """Build a fully-enhanced PolicyFeedback (or fallback stub)."""
    cls = globals().get("PolicyFeedback")
    if cls is None:
        raise ImportError(
            "PolicyFeedback is not available. Ensure policy/policy_feedback.py "
            "is importable."
        )
    try:
        return cls(
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=_resolve_features(features),
            hardware=hardware,
            base_confidence=base_confidence,
        )
    except TypeError:
        return cls()


def create_reporter(
    policy: Optional[Dict] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "policy-reporter",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
    dedup_window_seconds: float = 30.0,
) -> Any:
    """Build a fully-enhanced PolicyReporter (or fallback stub)."""
    cls = globals().get("PolicyReporter")
    if cls is None:
        raise ImportError(
            "PolicyReporter is not available. Ensure policy/policy_reporter.py "
            "is importable."
        )
    policy = policy or {}
    try:
        return cls(
            policy=policy,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=_resolve_features(features),
            hardware=hardware,
            dedup_window_seconds=dedup_window_seconds,
        )
    except TypeError:
        return cls(policy)


def create_policy_suite(
    energy_budget: float,
    baseline_energy: Optional[float] = None,
    policy: Optional[Dict] = None,
    *,
    deployment_id: str = "local",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
) -> Dict[str, Any]:
    """
    Build all three policy components with a consistent configuration.

    Returns:
        {"engine": PolicyEngine, "feedback": PolicyFeedback, "reporter": PolicyReporter}
    """
    return {
        "engine": create_engine(
            energy_budget=energy_budget,
            baseline_energy=baseline_energy,
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-engine",
            features=features,
            hardware=hardware,
        ),
        "feedback": create_feedback(
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-feedback",
            features=features,
            hardware=hardware,
        ),
        "reporter": create_reporter(
            policy=policy or {},
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-reporter",
            features=features,
            hardware=hardware,
        ),
    }


# =============================================================================
# 8. Capability introspection
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """
    Return a summary of the policy package's current capabilities.

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
            "core_engine": list(_CORE_ENGINE),
            "core_feedback": list(_CORE_FEEDBACK),
            "core_reporter": list(_CORE_REPORTER),
            "engine_enhancements": list(_EXPORTED_ENGINE_ENH),
            "feedback_enhancements": list(_EXPORTED_FEEDBACK_ENH),
            "reporter_enhancements": list(_EXPORTED_REPORTER_ENH),
        },
        "features_available": list(DEFAULT_FEATURES),
        "components": {
            "engine_available": globals().get("PolicyEngine") is not None,
            "feedback_available": globals().get("PolicyFeedback") is not None,
            "reporter_available": globals().get("PolicyReporter") is not None,
        },
    }


# =============================================================================
# 9. Public API surface (`__all__`)
# =============================================================================

__all__: List[str] = []

# --- Core components (always present via stub fallback) ---
for _core in ("PolicyEngine", "PolicyFeedback", "PolicyReporter"):
    if globals().get(_core) is not None:
        __all__.append(_core)

# --- Factory & introspection helpers ---
__all__ += [
    "create_engine",
    "create_feedback",
    "create_reporter",
    "create_policy_suite",
    "get_capabilities",
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "DISABLE_ALL_FEATURES",
    "__version__",
    "__author__",
    "__license__",
]

# --- Enhancement symbols (added only if resolved to non-None) ---
_ALL_ENHANCEMENT_SYMBOLS = (
    # Distillation
    "QuantumDistillationBridge",
    "DistilledRewardPolicy",
    "DistilledExplanationPolicy",
    "DistilledAlertPolicy",
    # Causal RL
    "CausalPenaltyLearner", "PenaltyState",
    "CausalConfidenceLearner", "ConfidenceState",
    "CausalThresholdLearner", "ThresholdState",
    # Federated
    "FederatedAggregator",
    "FederatedRewardProfile",
    "FederatedExplanationProfile",
    "FederatedAlertProfile",
    # Multi-agent
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    # Temporal logic
    "TemporalLogicMonitor",
    "DeadlineRespected", "NonNegativeSaving", "MaxDelayBounded",
    "NonNegativeEnergy", "NonNegativeLatency", "NonNegativeCarbon",
    "MaxAlertRate", "NoAlertDuringCriticalSection",
    # XAI
    "RewardExplainer", "RewardExplanation",
    "StructuredExplainer", "StructuredExplanation",
    "AlertExplainer", "StructuredAlert",
    "ExplanationTier",
    # Precision
    "AdaptivePrecisionController", "HardwareProfile", "PrecisionLevel",
    # Carbon markets
    "CarbonMarketClient", "MarketSnapshot",
    # Resilience
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    # HITL
    "HumanInTheLoopGate", "HITLRequest",
    # Reporting enums
    "Severity", "AlertCategory",
)

for _sym in _ALL_ENHANCEMENT_SYMBOLS:
    if globals().get(_sym) is not None and _sym not in __all__:
        __all__.append(_sym)


# =============================================================================
# 10. Load diagnostics
# =============================================================================

if logger.isEnabledFor(logging.DEBUG):
    _loaded = [n for n, m in _SUBMODULES.items() if m is not None]
    _missing = [n for n, m in _SUBMODULES.items() if m is None]
    logger.debug(
        f"policy package ready: {len(__all__)} public symbols exposed "
        f"(loaded={_loaded}, stubs_used={_missing})"
    )
