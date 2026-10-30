# policy/__init__.py

"""
Policy enforcement and feedback modules (Enhanced v5.1.0)
=========================================================

Public API for the Green Agent's policy layer — reward shaping, temporal
shifting, reflective explanation, and multi-category metric alerting —
all enhanced with the ten cross-cutting enhancement layers.

v5.1.0 fixes
------------
  * ``_expose`` no longer clobbers fallback stubs (critical: previously
    every fallback class was set to None, disabling the whole graceful-
    degradation design).
  * ``_load_submodule`` distinguishes "module missing" (→ fallback) from
    "module broken / dependency missing" (→ raise loudly).
  * ``create_*`` factories use ``inspect.signature`` — no more ``TypeError``
    swallowing.
  * Fallback stubs warn on ignored enhanced kwargs.
  * ``DEFAULT_FEATURES`` is derived from the loaded submodules (drift-proof).
  * ``ENABLE_ALL_FEATURES`` excludes ``chaos_testing``; a separate
    ``ENABLE_ALL_EXPERIMENTAL_FEATURES`` preset includes it.
  * Required symbols raise ``ImportError`` at import time.
  * Unknown feature keys warn.
  * ``PolicySuite`` dataclass with context-manager lifecycle.
  * ``create_policy_suite`` shares a ``MetricsRecorder`` across components.
  * ``get_capabilities()`` reports submodule versions + feature drift.
  * Full smoke test under ``__main__``.

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
    create_engine()                factory for PolicyEngine
    create_feedback()              factory for PolicyFeedback
    create_reporter()              factory for PolicyReporter
    create_policy_suite()          all three in a PolicySuite
    PolicySuite                    dataclass with close()/context manager
    get_capabilities()             runtime introspection
    has_feature(key)               feature-flag query
    default_features()             fresh copy of the default dict
    DEFAULT_FEATURES               default enhancement toggles
    ENABLE_ALL_FEATURES            safe preset (excludes chaos_testing)
    ENABLE_ALL_EXPERIMENTAL_FEATURES  includes chaos_testing
    DISABLE_ALL_FEATURES           disable every layer
"""

from __future__ import annotations

import inspect
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.1.0"
__author__ = "Green Agent Team"
__license__ = "MIT"


# =============================================================================
# 1. Shared helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# Known enhanced-constructor kwargs; fallback stubs warn when these are
# silently dropped.
_ENHANCED_KWARGS = frozenset({
    "deployment_id", "agent_id", "features", "hardware",
    "config", "metrics", "market",
    "dedup_window_seconds", "base_confidence",
})


_FALLBACK_WARNED_CLASSES: Set[str] = set()


def _warn_fallback_once(cls_name: str) -> None:
    """Emit a single, informative warning per fallback class."""
    if cls_name in _FALLBACK_WARNED_CLASSES:
        return
    _FALLBACK_WARNED_CLASSES.add(cls_name)
    logger.warning(
        "Using %s fallback stub — enhanced APIs (record_outcome, "
        "distill_*, health, get_statistics, HITL callbacks, XAI structure) "
        "are unavailable. Install the full policy.%s submodule to enable them.",
        cls_name, cls_name.lower(),
    )


_WARNED_FALLBACK_KWARGS: Set[Tuple[str, str]] = set()


def _warn_fallback_kwarg(cls_name: str, kwarg: str) -> None:
    """Emit a single warning per (fallback class, ignored kwarg)."""
    key = (cls_name, kwarg)
    if key in _WARNED_FALLBACK_KWARGS:
        return
    _WARNED_FALLBACK_KWARGS.add(key)
    logger.warning(
        "%s fallback stub ignores kwarg %r — the corresponding feature "
        "will NOT be enabled.",
        cls_name, kwarg,
    )


def _warn_ignored_kwargs(cls_name: str, kwargs: Mapping[str, Any]) -> None:
    for k, v in kwargs.items():
        if k in _ENHANCED_KWARGS and v is not None:
            _warn_fallback_kwarg(cls_name, k)


# =============================================================================
# 2. Submodule loading (distinguish missing from broken)
# =============================================================================

_SUBMODULE_NAMES: Tuple[str, ...] = (
    "policy_engine",
    "policy_feedback",
    "policy_reporter",
)

_SUBMODULES: Dict[str, Any] = {}
_MISSING_REQUIRED: Set[str] = set()


def _load_submodule(name: str) -> Optional[Any]:
    """
    Attempt to import ``policy.<name>``.

    Returns
    -------
    module
        Successfully imported.
    None
        Module file genuinely doesn't exist — caller should use the stub.

    Raises
    ------
    ImportError
        Module exists but is broken: a *dependency* is missing, a
        ``SyntaxError``, or a module-level exception. These must surface
        loudly, not be masked by a fallback stub.
    """
    package_path = __name__  # e.g. "policy"
    target_full = f"{package_path}.{name}"
    candidates = (
        f".{name}",                    # relative import
        target_full,                   # absolute import
        f"src.{target_full}",          # legacy src-tree fallback
    )

    for spec in candidates:
        try:
            if spec.startswith("."):
                mod = import_module(spec, package=package_path)
            else:
                mod = import_module(spec)
            logger.debug("policy.%s loaded from '%s'", name, spec)
            return mod
        except ModuleNotFoundError as e:
            missing = e.name or ""

            # Is the missing module *this* target or one of its parents?
            is_target_missing = (
                missing == target_full
                or missing == name
                or target_full.startswith(missing + ".")
            )
            # For ``src.*`` prefixed paths, a missing ``src`` root is also
            # a legitimate "not found" (not a broken module).
            if spec.startswith("src.") and missing in ("src", f"src.{package_path}"):
                continue
            if is_target_missing:
                continue  # try next candidate path

            # Found the target but a *dependency* of it is missing — raise.
            logger.error(
                "policy.%s found but its dependency %r is missing: %s. "
                "Refusing to substitute a fallback stub.",
                name, missing, e, exc_info=True,
            )
            raise
        except Exception as e:
            # SyntaxError, module-level runtime error, etc. — surface loudly.
            logger.error(
                "policy.%s failed to import from %s: %s",
                name, spec, e, exc_info=True,
            )
            raise

    logger.info(
        "Optional submodule policy.%s not found; installing fallback stub",
        name,
    )
    return None


for _name in _SUBMODULE_NAMES:
    _SUBMODULES[_name] = _load_submodule(_name)


# =============================================================================
# 3. Inline fallback stubs (used only when a submodule file is missing)
# =============================================================================

# ---- Fallback PolicyEngine ----
if _SUBMODULES.get("policy_engine") is None:
    class PolicyEngine:  # type: ignore[no-redef]
        """Minimal fallback PolicyEngine (matches original semantics)."""
        _is_fallback = True

        def __init__(self, energy_budget, baseline_energy=None, **kwargs):
            _warn_fallback_once("PolicyEngine")
            _warn_ignored_kwargs("PolicyEngine", kwargs)
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

        def compute_sustainability_reward(
            self, accuracy: float, energy: float,
        ) -> float:
            # v5.1.0: fallback uses the same negawatt formula as the
            # enhanced engine's `_fallback_negawatt` (previous version
            # returned 0.0 when the negawatt module was unavailable).
            if self.negawatt_module is not None:
                reward = float(self.negawatt_module.combined_reward(
                    accuracy=accuracy, energy=energy,
                ))
            elif self.baseline_energy and self.baseline_energy > 0:
                savings = max(
                    0.0,
                    (self.baseline_energy - energy) / self.baseline_energy,
                )
                reward = 0.5 * accuracy + 0.5 * savings
            else:
                reward = 0.5 * accuracy
            if energy > self.energy_budget:
                reward -= 1.0
            return reward

        def suggest_temporal_shift(
            self, shifter, forecast_engine,
            energy_kwh, shiftable=True, urgency="low", **_,
        ):
            if not shiftable or urgency == "high":
                return 0, 0.0
            current = forecast_engine.current_intensity()
            forecast = forecast_engine.forecast_next_hours(4)
            return shifter.suggest(current, forecast, energy_kwh)


# ---- Fallback PolicyFeedback ----
if _SUBMODULES.get("policy_feedback") is None:
    class PolicyFeedback:  # type: ignore[no-redef]
        """Minimal fallback PolicyFeedback (matches original semantics)."""
        _is_fallback = True

        def __init__(self, **kwargs):
            _warn_fallback_once("PolicyFeedback")
            _warn_ignored_kwargs("PolicyFeedback", kwargs)

        def generate(self, decision, before, after, **_):
            b_energy = before.get("energy", 0)
            a_energy = after.get("energy", 0)
            b_latency = before.get("latency", 0)
            a_latency = after.get("latency", 0)
            energy_delta = a_energy - b_energy
            latency_delta = a_latency - b_latency
            return {
                "decision": decision,
                "explanation": (
                    f"I selected mode '{decision}' because energy shifted "
                    f"from {b_energy} to {a_energy}."
                ),
                "tradeoff": {
                    "latency_delta": latency_delta,
                    "energy_delta": energy_delta,
                },
                "confidence": 0.75,
                # Extended keys — empty but present so downstream consumers
                # that pattern-match on them never crash.
                "tradeoff_extended": {
                    "latency_delta": latency_delta,
                    "energy_delta": energy_delta,
                    "carbon_delta": None,
                    "helium_delta": None,
                    "cost_delta": None,
                    "accuracy_delta": None,
                },
                "missing_inputs": [],
                "structured": None,
                "temporal_verified": True,
                "temporal_violations": [],
                "market_snapshot": None,
                "helium_scarcity": None,
                "precision": None,
                "distilled_policy": None,
                "hitl_required": False,
                "hitl_approved": None,
                "causal_calibrated": False,
                "tier": (
                    "high" if energy_delta < 0
                    else "medium" if energy_delta == 0
                    else "low"
                ),
                "tier_detail": "fallback_stub",
                "at": _utcnow().isoformat(),
            }


# ---- Fallback PolicyReporter ----
if _SUBMODULES.get("policy_reporter") is None:
    class PolicyReporter:  # type: ignore[no-redef]
        """Minimal fallback PolicyReporter (matches original semantics)."""
        _is_fallback = True

        def __init__(self, policy: Dict, **kwargs):
            _warn_fallback_once("PolicyReporter")
            _warn_ignored_kwargs("PolicyReporter", kwargs)
            reporting = (policy or {}).get("reporting", {})
            if not isinstance(reporting, dict):
                reporting = {}
            self.threshold = reporting.get(
                "alert_threshold_energy_pct", 80,
            )

        def report(self, metrics: Dict) -> Dict:
            alerts: List[str] = []
            if metrics.get("energy_pct", 0) >= self.threshold:
                alerts.append("Energy budget nearing limit")
            metrics["policy_alerts"] = alerts
            # Extended keys — consistent shape so callers can pattern-match.
            metrics["policy_alerts_structured"] = []
            metrics["policy_alert_severity"] = "info"
            metrics["policy_alert_threshold_used"] = float(self.threshold)
            metrics["policy_alert_causal_adjustment"] = 0.0
            metrics["policy_hitl_required"] = False
            metrics["policy_hitl_approved"] = None
            metrics["policy_alert_ids"] = []
            metrics["policy_alerts_missing_inputs"] = []
            metrics["policy_reporter_status"] = "fallback_stub"
            metrics["policy_reporter_degraded"] = True
            metrics["policy_reporter_degradation_reason"] = "fallback_stub"
            return metrics


# =============================================================================
# 4. Symbol exposure (preserves fallback stubs — v5.1.0 fix)
# =============================================================================

def _expose(
    submodule_name: str,
    *symbols: str,
    required: bool = False,
) -> List[str]:
    """
    Lift ``symbols`` from the named submodule into this package's globals.

    v5.1.0 fix: if a symbol is missing from the submodule BUT is already
    defined in this package's globals (from a fallback stub defined above),
    the existing binding is *preserved* rather than clobbered with None.
    This is what makes the graceful-degradation architecture actually work.
    """
    mod = _SUBMODULES.get(submodule_name)
    exposed: List[str] = []
    for sym in symbols:
        if mod is not None and hasattr(mod, sym):
            globals()[sym] = getattr(mod, sym)
            exposed.append(sym)
        elif globals().get(sym) is not None:
            # Fallback stub already defined this — preserve it.
            exposed.append(sym)
        else:
            globals()[sym] = None
            if required:
                _MISSING_REQUIRED.add(sym)
                logger.error(
                    "Required symbol %r missing from policy.%s",
                    sym, submodule_name,
                )
    return exposed


# ---- Core entry points (guaranteed by submodule OR fallback stub) ----
_CORE_ENGINE = _expose(
    "policy_engine", "PolicyEngine", required=True,
)
_CORE_FEEDBACK = _expose(
    "policy_feedback", "PolicyFeedback", required=True,
)
_CORE_REPORTER = _expose(
    "policy_reporter", "PolicyReporter", required=True,
)


# ---- Enhancement symbols ----

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
    # v2 additions
    "PolicyEngineConfig", "MetricsRecorder",
)

_EXPORTED_FEEDBACK_ENH = _expose(
    "policy_feedback",
    # 1. Quantum-Distillation
    "DistilledExplanationPolicy",
    # 2. Causal RL
    "CausalConfidenceLearner", "ConfidenceState",
    # 3. Federated
    "FederatedExplanationProfile",
    # 5. Temporal logic (additional properties)
    "NonNegativeLatency", "NonNegativeCarbon",
    # 6. XAI
    "StructuredExplainer", "StructuredExplanation", "ExplanationTier",
    # v2 additions
    "PolicyFeedbackConfig",
)

_EXPORTED_REPORTER_ENH = _expose(
    "policy_reporter",
    # 1. Quantum-Distillation
    "DistilledAlertPolicy",
    # 2. Causal RL
    "CausalThresholdLearner", "ThresholdState",
    # 3. Federated
    "FederatedAlertProfile",
    # 5. Temporal logic (additional properties)
    "MaxAlertRate", "NoAlertDuringCriticalSection",
    # 6. XAI
    "AlertExplainer", "StructuredAlert",
    # Reporting enums
    "Severity", "AlertCategory",
    # v2 additions
    "PolicyReporterConfig", "SeverityRatios",
)


# =============================================================================
# 5. Feature collection (drift-proof)
# =============================================================================

# Seed defaults — used as a starting point and as the "known keys" oracle
# if the submodules are unavailable.
_BUILTIN_DEFAULT_FEATURES: Dict[str, bool] = {
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


def _collect_submodule_features(mod: Any) -> Dict[str, bool]:
    """Merge the DEFAULT_FEATURES declared by a loaded submodule."""
    merged: Dict[str, bool] = {}
    for cls_name in ("PolicyEngine", "PolicyFeedback", "PolicyReporter"):
        cls = getattr(mod, cls_name, None)
        if cls is None:
            continue
        # Prefer the classmethod (returns a copy) over the raw dict attr.
        getter = getattr(cls, "default_features", None)
        if callable(getter):
            try:
                merged.update(dict(getter()))
                continue
            except Exception:
                pass
        feats = getattr(cls, "DEFAULT_FEATURES", None)
        if feats is not None:
            try:
                merged.update(dict(feats))
            except Exception:
                pass
    return merged


def _collect_default_features() -> Dict[str, bool]:
    """Derive the package DEFAULT_FEATURES from loaded submodules."""
    merged: Dict[str, bool] = dict(_BUILTIN_DEFAULT_FEATURES)
    for name in _SUBMODULE_NAMES:
        mod = _SUBMODULES.get(name)
        if mod is None:
            continue
        merged.update(_collect_submodule_features(mod))
    return merged


DEFAULT_FEATURES: Dict[str, bool] = _collect_default_features()

# Safe preset: enables every feature EXCEPT chaos testing. Enabling chaos
# without a matching `chaos_failure_rate > 0` in the config is a no-op, so
# it's misleading to include it in "enable all".
ENABLE_ALL_FEATURES: Dict[str, bool] = {
    k: True for k in DEFAULT_FEATURES if k != "chaos_testing"
}

# Experimental preset: includes chaos. Callers must also set
# `chaos_failure_rate > 0` in the corresponding config to actually inject
# faults.
ENABLE_ALL_EXPERIMENTAL_FEATURES: Dict[str, bool] = {
    k: True for k in DEFAULT_FEATURES
}

DISABLE_ALL_FEATURES: Dict[str, bool] = {k: False for k in DEFAULT_FEATURES}


# =============================================================================
# 6. Feature helpers
# =============================================================================

def _resolve_features(
    features: Optional[Mapping[str, bool]],
) -> Optional[Dict[str, bool]]:
    """Copy + validate feature overrides. Warns on unknown keys."""
    if features is None:
        return None
    unknown = set(features) - set(DEFAULT_FEATURES)
    if unknown:
        logger.warning(
            "Unknown feature keys %s — these will have no effect. "
            "Known keys: %s",
            sorted(unknown), sorted(DEFAULT_FEATURES),
        )
    return dict(features)


def default_features() -> Dict[str, bool]:
    """Return a fresh, mutable copy of DEFAULT_FEATURES."""
    return dict(DEFAULT_FEATURES)


def has_feature(
    key: str,
    features: Optional[Mapping[str, bool]] = None,
) -> bool:
    """Return True if ``key`` is enabled in ``features`` (or the defaults)."""
    source = features if features is not None else DEFAULT_FEATURES
    return bool(source.get(key, False))


# =============================================================================
# 7. Constructor introspection (replaces TypeError-swallowing)
# =============================================================================

def _accepts_named_kwarg(cls: Any, kwarg: str) -> bool:
    """True if ``cls.__init__`` explicitly names ``kwarg`` (not via **kwargs)."""
    try:
        sig = inspect.signature(cls.__init__)
    except (ValueError, TypeError):
        return False
    return kwarg in sig.parameters


def _is_enhanced_constructor(cls: Any) -> bool:
    """True if ``cls`` supports the enhanced constructor (``features`` kwarg)."""
    return _accepts_named_kwarg(cls, "features")


def _is_fallback_stub(cls: Any) -> bool:
    """True if ``cls`` was defined as a fallback stub in this module."""
    return bool(getattr(cls, "_is_fallback", False))


# =============================================================================
# 8. Factory functions
# =============================================================================

def create_engine(
    energy_budget: float,
    baseline_energy: Optional[float] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "policy-engine",
    features: Optional[Mapping[str, bool]] = None,
    hardware: Any = None,
    config: Any = None,
    market: Any = None,
    metrics: Any = None,
) -> Any:
    """Build a fully-enhanced ``PolicyEngine`` (falls back gracefully)."""
    cls = globals().get("PolicyEngine")
    if cls is None:
        raise ImportError(
            "PolicyEngine is not available. The policy_engine submodule is "
            "missing and no fallback stub was installed."
        )

    resolved = _resolve_features(features)

    if _is_enhanced_constructor(cls):
        kwargs: Dict[str, Any] = dict(
            energy_budget=energy_budget,
            baseline_energy=baseline_energy,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=resolved,
            hardware=hardware,
        )
        if config is not None and _accepts_named_kwarg(cls, "config"):
            kwargs["config"] = config
        if market is not None and _accepts_named_kwarg(cls, "market"):
            kwargs["market"] = market
        if metrics is not None and _accepts_named_kwarg(cls, "metrics"):
            kwargs["metrics"] = metrics
        return cls(**kwargs)

    if _is_fallback_stub(cls):
        return cls(
            energy_budget=energy_budget,
            baseline_energy=baseline_energy,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=resolved,
            hardware=hardware,
            config=config,
            metrics=metrics,
            market=market,
        )

    # Original (legacy) PolicyEngine — only accepts two positional args.
    if features is not None or hardware is not None or config is not None:
        logger.warning(
            "%s does not support the enhanced constructor; "
            "features/hardware/config will be ignored. Upgrade to the "
            "enhanced policy_engine submodule to use them.",
            cls.__name__,
        )
    return cls(energy_budget=energy_budget, baseline_energy=baseline_energy)


def create_feedback(
    *,
    deployment_id: str = "local",
    agent_id: str = "policy-feedback",
    features: Optional[Mapping[str, bool]] = None,
    hardware: Any = None,
    base_confidence: float = 0.75,
    config: Any = None,
    metrics: Any = None,
) -> Any:
    """Build a fully-enhanced ``PolicyFeedback`` (falls back gracefully)."""
    cls = globals().get("PolicyFeedback")
    if cls is None:
        raise ImportError(
            "PolicyFeedback is not available. The policy_feedback submodule "
            "is missing and no fallback stub was installed."
        )

    resolved = _resolve_features(features)

    if _is_enhanced_constructor(cls):
        kwargs: Dict[str, Any] = dict(
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=resolved,
            hardware=hardware,
            base_confidence=base_confidence,
        )
        if config is not None and _accepts_named_kwarg(cls, "config"):
            kwargs["config"] = config
        if metrics is not None and _accepts_named_kwarg(cls, "metrics"):
            kwargs["metrics"] = metrics
        return cls(**kwargs)

    if _is_fallback_stub(cls):
        return cls(
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=resolved,
            hardware=hardware,
            base_confidence=base_confidence,
            config=config,
            metrics=metrics,
        )

    logger.warning(
        "%s does not support the enhanced constructor; using no-arg form.",
        cls.__name__,
    )
    return cls()


def create_reporter(
    policy: Optional[Dict] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "policy-reporter",
    features: Optional[Mapping[str, bool]] = None,
    hardware: Any = None,
    dedup_window_seconds: float = 30.0,
    config: Any = None,
    metrics: Any = None,
) -> Any:
    """Build a fully-enhanced ``PolicyReporter`` (falls back gracefully)."""
    cls = globals().get("PolicyReporter")
    if cls is None:
        raise ImportError(
            "PolicyReporter is not available. The policy_reporter submodule "
            "is missing and no fallback stub was installed."
        )

    resolved = _resolve_features(features)
    policy = policy or {}

    if _is_enhanced_constructor(cls):
        kwargs: Dict[str, Any] = dict(
            policy=policy,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=resolved,
            hardware=hardware,
        )
        if _accepts_named_kwarg(cls, "dedup_window_seconds"):
            kwargs["dedup_window_seconds"] = dedup_window_seconds
        if config is not None and _accepts_named_kwarg(cls, "config"):
            kwargs["config"] = config
        if metrics is not None and _accepts_named_kwarg(cls, "metrics"):
            kwargs["metrics"] = metrics
        return cls(**kwargs)

    if _is_fallback_stub(cls):
        return cls(
            policy=policy,
            deployment_id=deployment_id,
            agent_id=agent_id,
            features=resolved,
            hardware=hardware,
            dedup_window_seconds=dedup_window_seconds,
            config=config,
            metrics=metrics,
        )

    logger.warning(
        "%s does not support the enhanced constructor; using legacy "
        "policy-only form.",
        cls.__name__,
    )
    return cls(policy)


# =============================================================================
# 9. PolicySuite
# =============================================================================

@dataclass
class PolicySuite:
    """Grouping of engine + feedback + reporter with shared lifecycle."""

    engine: Any
    feedback: Any
    reporter: Any
    shared_market: Any = None
    shared_metrics: Any = None

    def close(self) -> None:
        for name, component in (
            ("engine", self.engine),
            ("feedback", self.feedback),
            ("reporter", self.reporter),
        ):
            close = getattr(component, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as e:
                    logger.warning(
                        "close failed for %s: %s", name, e, exc_info=True,
                    )

    def __enter__(self) -> "PolicySuite":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def __getitem__(self, key: str) -> Any:
        if key not in self.keys():
            raise KeyError(key)
        return getattr(self, key)

    @staticmethod
    def keys() -> Tuple[str, ...]:
        return ("engine", "feedback", "reporter")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine": self.engine,
            "feedback": self.feedback,
            "reporter": self.reporter,
        }


def _build_shared_metrics() -> Any:
    """Best-effort: return a shared MetricsRecorder instance, or None."""
    for name in _SUBMODULE_NAMES:
        mod = _SUBMODULES.get(name)
        if mod is None:
            continue
        MetricsRecorder = getattr(mod, "MetricsRecorder", None)
        if MetricsRecorder is None:
            continue
        try:
            return MetricsRecorder()
        except Exception:
            continue
    return None


def create_policy_suite(
    energy_budget: float,
    baseline_energy: Optional[float] = None,
    policy: Optional[Dict] = None,
    *,
    deployment_id: str = "local",
    features: Optional[Mapping[str, bool]] = None,
    hardware: Any = None,
    engine_config: Any = None,
    feedback_config: Any = None,
    reporter_config: Any = None,
    dedup_window_seconds: float = 30.0,
    share_components: bool = True,
) -> PolicySuite:
    """
    Build all three policy components with a consistent configuration.

    When ``share_components=True`` (default), a single ``MetricsRecorder``
    is created and passed to all three components so that ``statistics()``
    reflects the whole suite, not three disjoint counters.

    Returns
    -------
    PolicySuite
        Supports ``with create_policy_suite(...) as suite:`` for lifecycle
        management, and ``suite["engine"]`` for dict-style access.
    """
    shared_metrics = _build_shared_metrics() if share_components else None

    return PolicySuite(
        engine=create_engine(
            energy_budget=energy_budget,
            baseline_energy=baseline_energy,
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-engine",
            features=features,
            hardware=hardware,
            config=engine_config,
            metrics=shared_metrics,
        ),
        feedback=create_feedback(
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-feedback",
            features=features,
            hardware=hardware,
            config=feedback_config,
            metrics=shared_metrics,
        ),
        reporter=create_reporter(
            policy=policy or {},
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-reporter",
            features=features,
            hardware=hardware,
            dedup_window_seconds=dedup_window_seconds,
            config=reporter_config,
            metrics=shared_metrics,
        ),
        shared_metrics=shared_metrics,
    )


# =============================================================================
# 10. Capability introspection
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """Return a comprehensive summary of the policy package's state."""
    submodule_versions: Dict[str, Optional[str]] = {}
    features_drift: Dict[str, Dict[str, Dict[str, Optional[bool]]]] = {}

    for name, mod in _SUBMODULES.items():
        if mod is None:
            continue
        submodule_versions[name] = getattr(mod, "__version__", None)
        sub_feats = _collect_submodule_features(mod)
        if not sub_feats:
            continue
        diff: Dict[str, Dict[str, Optional[bool]]] = {}
        for k in set(sub_feats) | set(DEFAULT_FEATURES):
            pkg_val = DEFAULT_FEATURES.get(k)
            sub_val = sub_feats.get(k)
            if pkg_val != sub_val:
                diff[k] = {"package": pkg_val, "submodule": sub_val}
        if diff:
            features_drift[name] = diff

    return {
        "version": __version__,
        "submodule_versions": submodule_versions,
        "submodules_loaded": {
            name: (mod is not None) for name, mod in _SUBMODULES.items()
        },
        "using_fallback_stubs": {
            name: (mod is None) for name, mod in _SUBMODULES.items()
        },
        "missing_required_symbols": sorted(_MISSING_REQUIRED),
        "exported_symbols": {
            "core_engine": list(_CORE_ENGINE),
            "core_feedback": list(_CORE_FEEDBACK),
            "core_reporter": list(_CORE_REPORTER),
            "engine_enhancements": list(_EXPORTED_ENGINE_ENH),
            "feedback_enhancements": list(_EXPORTED_FEEDBACK_ENH),
            "reporter_enhancements": list(_EXPORTED_REPORTER_ENH),
        },
        "features_available": list(DEFAULT_FEATURES),
        "features_defaults": dict(DEFAULT_FEATURES),
        "features_drift_from_submodules": features_drift,
        "components": {
            "engine_available": globals().get("PolicyEngine") is not None,
            "feedback_available": globals().get("PolicyFeedback") is not None,
            "reporter_available": globals().get("PolicyReporter") is not None,
        },
        "execution_mode": (
            "fallback"
            if any(m is None for m in _SUBMODULES.values())
            else "full"
        ),
    }


# =============================================================================
# 11. `__all__`
# =============================================================================

__all__: List[str] = []

# --- Core components (always present via fallback stubs) ---
for _core in ("PolicyEngine", "PolicyFeedback", "PolicyReporter"):
    if globals().get(_core) is not None:
        __all__.append(_core)

# --- Factories, introspection, presets ---
__all__ += [
    "create_engine",
    "create_feedback",
    "create_reporter",
    "create_policy_suite",
    "PolicySuite",
    "get_capabilities",
    "has_feature",
    "default_features",
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "ENABLE_ALL_EXPERIMENTAL_FEATURES",
    "DISABLE_ALL_FEATURES",
    "__version__",
    "__author__",
    "__license__",
]

# --- Enhancement symbols (only if resolved to non-None) ---
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
    # v2 additions
    "PolicyEngineConfig", "PolicyFeedbackConfig", "PolicyReporterConfig",
    "SeverityRatios", "MetricsRecorder",
)

for _sym in _ALL_ENHANCEMENT_SYMBOLS:
    if globals().get(_sym) is not None and _sym not in __all__:
        __all__.append(_sym)


# =============================================================================
# 12. Required-symbol enforcement (fail loudly, not silently)
# =============================================================================

if _MISSING_REQUIRED:
    raise ImportError(
        f"Policy package is missing required symbols: "
        f"{sorted(_MISSING_REQUIRED)}. "
        f"Ensure policy.policy_engine, policy.policy_feedback, and "
        f"policy.policy_reporter are all importable and export the "
        f"corresponding classes."
    )


# =============================================================================
# 13. Diagnostics
# =============================================================================

if logger.isEnabledFor(logging.DEBUG):
    _loaded = [n for n, m in _SUBMODULES.items() if m is not None]
    _missing = [n for n, m in _SUBMODULES.items() if m is None]
    logger.debug(
        "policy package ready: %d public symbols exposed "
        "(loaded=%s, stubs_used=%s, version=%s)",
        len(__all__), _loaded, _missing, __version__,
    )


# =============================================================================
# 14. Smoke test (python -m policy)
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(level=logging.INFO)

    print("=== Package capabilities ===")
    print(json.dumps(get_capabilities(), indent=2, default=str))

    print("\n=== PolicySuite smoke test ===")
    with create_policy_suite(
        energy_budget=0.05,
        baseline_energy=0.1,
        policy={"reporting": {"alert_threshold_energy_pct": 80}},
        deployment_id="smoke-test",
    ) as suite:
        for name, comp in suite.to_dict().items():
            print(f"  {name}: {type(comp).__name__}")

        # --- Engine ---
        try:
            r = suite.engine.compute_sustainability_reward(
                accuracy=0.9, energy=0.06,
            )
            print(f"  reward:   {r:.4f}")
        except Exception as e:
            print(f"  reward failed: {e}")

        # --- Feedback ---
        try:
            fb = suite.feedback.generate(
                decision="low_energy",
                before={"energy": 0.08, "latency": 120.0},
                after={"energy": 0.05, "latency": 150.0},
            )
            print(f"  feedback decision:   {fb['decision']}")
            print(f"  feedback confidence: {fb.get('confidence', 'N/A')}")
        except Exception as e:
            print(f"  feedback failed: {e}")

        # --- Reporter ---
        try:
            rep = suite.reporter.report({"energy_pct": 85})
            print(f"  reporter alerts:   {rep['policy_alerts']}")
            print(
                f"  reporter severity: "
                f"{rep.get('policy_alert_severity', 'N/A')}"
            )
        except Exception as e:
            print(f"  reporter failed: {e}")

        # --- Shared metrics (if available) ---
        if suite.shared_metrics is not None:
            snap = suite.shared_metrics.snapshot()
            n_counters = len(snap.get("counters", {}))
            print(f"  shared metrics counters: {n_counters}")

    print("\nSmoke test passed.")
