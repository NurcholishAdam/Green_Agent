# src/constraints/__init__.py
"""
constraints — Green Agent budget & policy enforcement (v6.0.0)
===============================================================

The single package import point for the budget and policy family.

**The six submodules**

    budget_domain           shared primitives (single source of truth)
    budget_manager          BudgetManager + Budget + BudgetStatus
    budget_enforcer         BudgetEnforcer + BudgetExceeded
    check_energy_budget     check_energy_budget + BudgetChecker
    budget_constrained      BudgetConstrainedEvaluator + exceptions
    green_policy_enforcer   GreenPolicyEnforcer + PolicyViolation

**v6 guarantees**

* **No fallback stubs.** A missing symbol raises ``ImportError`` at
  attribute access — never ``None``. Callers cannot accidentally invoke
  an approving stub in place of the real enforcer.
* **Lazy loading via PEP 562 ``__getattr__``.** Importing the package
  does not import any submodule. The first attribute access loads only
  the submodule that owns the requested symbol.
* **Complete re-exports.** Every public symbol from every submodule is
  reachable as ``constraints.<name>``.
* **Capability introspection.** ``get_capabilities()`` reports
  per-submodule load state, per-symbol availability, missing symbols,
  version consistency, and available feature flags.
* **Fail-safe factories.** ``create_constraints_suite(strict=True)``
  (the v6 default) raises ``ImportError`` when a required component is
  unavailable; ``strict=False`` returns the best-effort suite.
* **Safe feature preset.** ``ENABLE_ALL_FEATURES`` deliberately excludes
  ``chaos_testing`` — chaos injection must be an explicit opt-in.

**Backward-compatible v1/v5 surface preserved**

    Budget, BudgetStatus, BudgetManager, BudgetEnforcer, BudgetExceeded,
    check_energy_budget, check_energy_budget_detailed.

**Feature detection**

    __version__ = "6.0.0"
    __compatible_modules__ = "6.x"

**Convenience factories**

    create_budget_manager, create_budget_enforcer, create_constraints_suite

**Capability introspection**

    get_capabilities

**Feature presets**

    DEFAULT_FEATURES, ENABLE_ALL_FEATURES, DISABLE_ALL_FEATURES

**Running the demo**

    PYTHONPATH=src:src/carbon python -m constraints._demo
    (or)
    PYTHONPATH=src:src/carbon python src/constraints/__init__.py
"""

from __future__ import annotations

import importlib
import logging
import threading
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "6.0.0"
__compatible_modules__ = "6.x"
__author__ = "AI Research Agent Team"
__license__ = "MIT"


# --------------------------------------------------------------------------- #
# 1. Submodule registry
# --------------------------------------------------------------------------- #
_SUBMODULE_NAMES: Tuple[str, ...] = (
    "budget_domain",
    "budget_manager",
    "budget_enforcer",
    "check_energy_budget",
    "budget_constrained",
    "green_policy_enforcer",
)


# --------------------------------------------------------------------------- #
# 2. Symbol → (submodule, attribute) map
# --------------------------------------------------------------------------- #
_SYMBOL_MAP: Dict[str, Tuple[str, str]] = {
    # ---- budget_domain: shared primitives ----
    "Severity": ("budget_domain", "Severity"),
    "PrecisionLevel": ("budget_domain", "PrecisionLevel"),
    "AgentRole": ("budget_domain", "AgentRole"),
    "DegradedPolicy": ("budget_domain", "DegradedPolicy"),
    "EvaluationOutcome": ("budget_domain", "EvaluationOutcome"),
    "HardwareProfile": ("budget_domain", "HardwareProfile"),
    "BudgetState": ("budget_domain", "BudgetState"),
    "ThresholdState": ("budget_domain", "ThresholdState"),
    "BudgetExplanation": ("budget_domain", "BudgetExplanation"),
    "DistilledBudgetPolicy": ("budget_domain", "DistilledBudgetPolicy"),
    "FederatedBudgetProfile": ("budget_domain", "FederatedBudgetProfile"),
    "MarketSnapshot": ("budget_domain", "MarketSnapshot"),
    "HITLRequest": ("budget_domain", "HITLRequest"),
    "SyncCircuitAdapter": ("budget_domain", "SyncCircuitAdapter"),
    "SyncChaosInjector": ("budget_domain", "SyncChaosInjector"),
    "BudgetExplainer": ("budget_domain", "BudgetExplainer"),
    "CausalBudgetLearner": ("budget_domain", "CausalBudgetLearner"),
    "QuantumDistillationBridge": ("budget_domain", "QuantumDistillationBridge"),
    "FederatedBudgetAggregator": ("budget_domain", "FederatedBudgetAggregator"),
    "HeliumProfiler": ("budget_domain", "HeliumProfiler"),
    "BudgetSafetyMonitor": ("budget_domain", "BudgetSafetyMonitor"),
    "HumanReviewGate": ("budget_domain", "HumanReviewGate"),
    "CarbonMarketCache": ("budget_domain", "CarbonMarketCache"),
    # ---- budget_domain: v18 re-exports ----
    "CircuitBreaker": ("budget_domain", "CircuitBreaker"),
    "ChaosTestingEngine": ("budget_domain", "ChaosTestingEngine"),
    "ActiveUserPreferenceLearner": (
        "budget_domain", "ActiveUserPreferenceLearner"
    ),
    "AdaptivePrecisionSwitcher": (
        "budget_domain", "AdaptivePrecisionSwitcher"
    ),
    "CarbonMarketIntegrator": ("budget_domain", "CarbonMarketIntegrator"),
    "MultiAgentCoordinator": ("budget_domain", "MultiAgentCoordinator"),
    "FederatedGreenAggregator": (
        "budget_domain", "FederatedGreenAggregator"
    ),
    "TemporalLogicVerifier": ("budget_domain", "TemporalLogicVerifier"),
    # ---- budget_manager ----
    "Budget": ("budget_manager", "Budget"),
    "BudgetStatus": ("budget_manager", "BudgetStatus"),
    "BudgetManager": ("budget_manager", "BudgetManager"),
    # ---- budget_enforcer ----
    "BudgetExceeded": ("budget_enforcer", "BudgetExceeded"),
    "BudgetEnforcer": ("budget_enforcer", "BudgetEnforcer"),
    # ---- check_energy_budget ----
    "check_energy_budget": ("check_energy_budget", "check_energy_budget"),
    "check_energy_budget_detailed": (
        "check_energy_budget", "check_energy_budget_detailed"
    ),
    "BudgetChecker": ("check_energy_budget", "BudgetChecker"),
    "EnhancedBudgetResult": (
        "check_energy_budget", "EnhancedBudgetResult"
    ),
    # ---- budget_constrained ----
    "BudgetConstrainedEvaluator": (
        "budget_constrained", "BudgetConstrainedEvaluator"
    ),
    "FeasibilityExplanation": (
        "budget_constrained", "FeasibilityExplanation"
    ),
    "UnknownAgentError": ("budget_constrained", "UnknownAgentError"),
    "EvaluationUnavailableError": (
        "budget_constrained", "EvaluationUnavailableError"
    ),
    # ---- green_policy_enforcer ----
    "EnforcementMode": ("green_policy_enforcer", "EnforcementMode"),
    "PolicyViolation": ("green_policy_enforcer", "PolicyViolation"),
    "EnforcementExplainer": (
        "green_policy_enforcer", "EnforcementExplainer"
    ),
    "EnforcementExplanation": (
        "green_policy_enforcer", "EnforcementExplanation"
    ),
    "GreenPolicyEnforcer": (
        "green_policy_enforcer", "GreenPolicyEnforcer"
    ),
}


# --------------------------------------------------------------------------- #
# 3. Lazy module loader (cached, thread-safe)
# --------------------------------------------------------------------------- #
_module_cache: Dict[str, Any] = {}
_module_cache_lock = threading.RLock()


def _load_submodule(name: str) -> Any:
    """
    Load a constraints submodule by name, or raise ``ImportError``.

    Tries three import forms so the package works whether it's imported
    as ``constraints``, run directly (``python src/constraints/__init__.py``),
    or imported via the ``src.`` prefix.
    """
    with _module_cache_lock:
        cached = _module_cache.get(name)
    if cached is not None:
        return cached

    candidates: Tuple[str, ...] = (
        f".{name}",
        name,
        f"src.constraints.{name}",
    )
    last_exc: Optional[BaseException] = None
    for spec in candidates:
        try:
            if spec.startswith("."):
                mod = importlib.import_module(spec, package=__name__)
            else:
                mod = importlib.import_module(spec)
        except ImportError as exc:
            last_exc = exc
            continue
        with _module_cache_lock:
            _module_cache[name] = mod
        return mod

    raise ImportError(
        f"constraints.{name} is unavailable "
        f"(tried {', '.join(candidates)}): {last_exc}"
    )


# --------------------------------------------------------------------------- #
# 4. PEP 562 lazy attribute access
# --------------------------------------------------------------------------- #
def __getattr__(name: str) -> Any:
    """
    Lazily resolve an attribute on the ``constraints`` package.

    * Symbols in ``_SYMBOL_MAP`` trigger the load of the submodule that
      owns them.
    * Submodule names themselves trigger a submodule load.
    * Anything else raises ``AttributeError``.
    """
    if name in _SYMBOL_MAP:
        submodule_name, attr = _SYMBOL_MAP[name]
        mod = _load_submodule(submodule_name)
        if not hasattr(mod, attr):
            raise ImportError(
                f"constraints.{submodule_name} does not expose attribute "
                f"{attr!r} (needed for {name!r})"
            )
        value = getattr(mod, attr)
        # Cache so the next access is a plain __dict__ lookup.
        globals()[name] = value
        return value

    if name in _SUBMODULE_NAMES:
        mod = _load_submodule(name)
        globals()[name] = mod
        return mod

    raise AttributeError(
        f"module 'constraints' has no attribute {name!r}"
    )


def __dir__() -> List[str]:
    """Expose lazily-resolved symbols to ``dir(constraints)``."""
    return sorted(
        set(globals().keys())
        | set(_SYMBOL_MAP.keys())
        | set(_SUBMODULE_NAMES)
    )


# --------------------------------------------------------------------------- #
# 5. Feature presets
# --------------------------------------------------------------------------- #
#: Union of every feature flag supported by any constraints submodule.
#:
#: ``chaos_testing`` defaults to ``False`` (opt-in only).
#: ``aggregate_violations`` defaults to ``False`` in v6 — the v5 mutation
#: of the v1 ``reason`` string broke exact-match callers.
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
    "cost_awareness": True,
    "aggregate_violations": False,
}

#: Enable every feature **except** ``chaos_testing``.
#:
#: Chaos testing injects random faults into the enforcement path and must
#: be an explicit opt-in — never a side effect of "enable everything
#: else". Enabling it in production would silently corrupt the enforcement
#: circuit breaker and produce intermittent ``circuit_open`` results.
#: If you want chaos testing, set ``"chaos_testing": True`` explicitly on
#: top of this preset.
ENABLE_ALL_FEATURES: Dict[str, bool] = {
    **{k: True for k in DEFAULT_FEATURES},
    "chaos_testing": False,
}

#: Disable every feature (v1-compatible baseline).
DISABLE_ALL_FEATURES: Dict[str, bool] = {k: False for k in DEFAULT_FEATURES}


# --------------------------------------------------------------------------- #
# 6. Feature filtering (per-module, used by the suite factory)
# --------------------------------------------------------------------------- #
def _filter_features_for_module(
    features: Optional[Mapping[str, bool]],
    module_name: str,
) -> Optional[Dict[str, bool]]:
    """
    Filter a feature dict to only the keys the target module supports.

    Individual factories (``create_budget_manager``,
    ``create_budget_enforcer``) pass ``features`` through **unfiltered**
    — the module raises ``ValueError`` on unknown keys, which is the
    honest behavior for a direct caller.

    The suite factory uses this helper to construct both a manager and
    an enforcer from a **single** feature dict: a feature supported by
    one but not the other is silently dropped from the other (logged at
    DEBUG). This is the pragmatic behavior for a caller who wants "enable
    federated learning everywhere".
    """
    if features is None:
        return None
    try:
        mod = _load_submodule(module_name)
    except ImportError:
        return dict(features)
    module_defaults = getattr(mod, "DEFAULT_FEATURES", None)
    if not isinstance(module_defaults, Mapping):
        return dict(features)
    filtered: Dict[str, bool] = {}
    dropped: List[str] = []
    for k, v in features.items():
        if k in module_defaults:
            filtered[k] = bool(v)
        else:
            dropped.append(k)
    if dropped:
        logger.debug(
            "constraints: %s does not support features %s; ignoring",
            module_name,
            dropped,
        )
    return filtered


# --------------------------------------------------------------------------- #
# 7. Convenience factories
# --------------------------------------------------------------------------- #
def create_budget_manager(
    budget: Any,
    *,
    deployment_id: str = "local",
    agent_id: str = "budget-manager",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
) -> Any:
    """
    Build a fully-enhanced ``BudgetManager``.

    Raises
    ------
    ImportError
        If ``constraints.budget_manager`` (or ``BudgetManager`` within it)
        is unavailable.

    Notes
    -----
    ``features`` is passed through unfiltered. The module raises
    ``ValueError`` on unknown feature names.
    """
    mod = _load_submodule("budget_manager")
    if not hasattr(mod, "BudgetManager"):
        raise ImportError(
            "constraints.budget_manager does not expose BudgetManager"
        )
    return mod.BudgetManager(
        budget=budget,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=dict(features) if features is not None else None,
        hardware=hardware,
    )


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
    """
    Build a fully-enhanced ``BudgetEnforcer``.

    Raises
    ------
    ImportError
        If ``constraints.budget_enforcer`` (or ``BudgetEnforcer`` within
        it) is unavailable.
    """
    mod = _load_submodule("budget_enforcer")
    if not hasattr(mod, "BudgetEnforcer"):
        raise ImportError(
            "constraints.budget_enforcer does not expose BudgetEnforcer"
        )
    return mod.BudgetEnforcer(
        max_energy=max_energy,
        max_carbon=max_carbon,
        max_latency=max_latency,
        max_helium=max_helium,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=dict(features) if features is not None else None,
        hardware=hardware,
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
    strict: bool = True,
) -> Dict[str, Any]:
    """
    Build a manager and an enforcer with a consistent configuration.

    Parameters
    ----------
    budget : optional
        If provided, a ``BudgetManager`` is constructed.
    strict : bool, default True
        If True (**the v6 default**), ``ImportError`` is raised when a
        requested component is unavailable. Set ``False`` for a
        best-effort suite that returns ``None`` for missing components.

    Returns
    -------
    dict
        ``{"manager": BudgetManager | None, "enforcer": BudgetEnforcer | None}``
    """
    suite: Dict[str, Any] = {"manager": None, "enforcer": None}

    # Manager — only when a budget was provided
    if budget is not None:
        try:
            suite["manager"] = create_budget_manager(
                budget=budget,
                deployment_id=deployment_id,
                agent_id=f"{deployment_id}-manager",
                features=_filter_features_for_module(
                    features, "budget_manager"
                ),
                hardware=hardware,
            )
        except ImportError as exc:
            if strict:
                raise
            logger.warning(
                "constraints: BudgetManager unavailable (%s); "
                "returning suite with manager=None",
                exc,
            )

    # Enforcer — always attempted
    try:
        suite["enforcer"] = create_budget_enforcer(
            max_energy=max_energy,
            max_carbon=max_carbon,
            max_latency=max_latency,
            max_helium=max_helium,
            deployment_id=deployment_id,
            agent_id=f"{deployment_id}-enforcer",
            features=_filter_features_for_module(
                features, "budget_enforcer"
            ),
            hardware=hardware,
        )
    except ImportError as exc:
        if strict:
            raise
        logger.warning(
            "constraints: BudgetEnforcer unavailable (%s); "
            "returning suite with enforcer=None",
            exc,
        )

    return suite


# --------------------------------------------------------------------------- #
# 8. Capability introspection
# --------------------------------------------------------------------------- #
def get_capabilities() -> Dict[str, Any]:
    """
    Return a summary of what this constraints package can do.

    Includes per-submodule load state and version, per-symbol
    availability, version consistency across submodules, and the list
    of available feature flags.

    ``stub_symbols`` is always empty in v6 — this package does **not**
    ship fallback stubs. A missing symbol raises ``ImportError``.
    """
    submodules_loaded: Dict[str, Dict[str, Any]] = {}
    missing_symbols: Dict[str, List[str]] = defaultdict(list)

    for mod_name in _SUBMODULE_NAMES:
        try:
            mod = _load_submodule(mod_name)
        except ImportError as exc:
            submodules_loaded[mod_name] = {
                "loaded": False,
                "version": None,
                "error": str(exc),
            }
            for _, (owner, attr) in _SYMBOL_MAP.items():
                if owner == mod_name:
                    missing_symbols[mod_name].append(attr)
            continue

        version = getattr(mod, "__version__", None)
        submodules_loaded[mod_name] = {
            "loaded": True,
            "version": version,
            "path": getattr(mod, "__file__", None),
        }
        for _, (owner, attr) in _SYMBOL_MAP.items():
            if owner == mod_name and not hasattr(mod, attr):
                missing_symbols[mod_name].append(attr)

    declared_versions = [
        info["version"]
        for info in submodules_loaded.values()
        if info["version"] is not None
    ]
    if declared_versions:
        version_consistency: Optional[bool] = (
            len(set(declared_versions)) == 1
        )
    else:
        version_consistency = None

    def _component_available(symbol: str) -> bool:
        submod, _ = _SYMBOL_MAP[symbol]
        return (
            submodules_loaded.get(submod, {}).get("loaded", False)
            and symbol not in missing_symbols.get(submod, [])
        )

    return {
        "version": __version__,
        "compatible_modules": __compatible_modules__,
        "submodules_loaded": submodules_loaded,
        "missing_symbols": dict(missing_symbols),
        "stub_symbols": [],   # v6 guarantees no fallback stubs
        "version_consistency": version_consistency,
        "features_available": list(DEFAULT_FEATURES),
        "components": {
            "budget_manager_available": _component_available(
                "BudgetManager"
            ),
            "budget_enforcer_available": _component_available(
                "BudgetEnforcer"
            ),
            "budget_checker_available": _component_available(
                "BudgetChecker"
            ),
            "budget_constrained_evaluator_available": (
                _component_available("BudgetConstrainedEvaluator")
            ),
            "green_policy_enforcer_available": _component_available(
                "GreenPolicyEnforcer"
            ),
        },
    }


# --------------------------------------------------------------------------- #
# 9. Public API surface
# --------------------------------------------------------------------------- #
__all__: List[str] = sorted(
    list(_SYMBOL_MAP.keys())
    + [
        "DEFAULT_FEATURES",
        "ENABLE_ALL_FEATURES",
        "DISABLE_ALL_FEATURES",
        "create_budget_manager",
        "create_budget_enforcer",
        "create_constraints_suite",
        "get_capabilities",
    ]
    # Note: submodule names are intentionally excluded from __all__ —
    # `from constraints import *` should not import submodules (that
    # would defeat lazy loading). Use `from constraints import budget_domain`
    # to access a specific submodule.
)


# --------------------------------------------------------------------------- #
# 10. Load diagnostics (DEBUG only)
# --------------------------------------------------------------------------- #
if logger.isEnabledFor(logging.DEBUG):
    logger.debug(
        "constraints v%s ready (compatible with modules %s): "
        "%d symbols, %d submodules, lazy-loaded",
        __version__,
        __compatible_modules__,
        len(_SYMBOL_MAP),
        len(_SUBMODULE_NAMES),
    )


# --------------------------------------------------------------------------- #
# 11. Demo
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    import sys as _sys

    # Fail fast if the demo's prerequisites are missing
    try:
        import carbon_forecast  # noqa: F401
    except ImportError as exc:
        print(f"\nFATAL: carbon_forecast is not importable: {exc}")
        print(
            "Run with: "
            "PYTHONPATH=src:src/carbon python src/constraints/__init__.py"
        )
        print(
            "  (or)     "
            "PYTHONPATH=src:src/carbon python -c "
            "\"from constraints import _demo; _demo()\""
        )
        return

    print("=" * 78)
    print("src/constraints/__init__.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    mod = _sys.modules[__name__]

    # ============================================================= #
    # Version metadata
    # ============================================================= #
    print("\n=== Version metadata ===")
    _check(
        __version__ == "6.0.0",
        f"__version__ is 6.0.0 (got {__version__})",
    )
    _check(
        __compatible_modules__ == "6.x",
        f"__compatible_modules__ is '6.x' "
        f"(got {__compatible_modules__!r})",
    )

    # ============================================================= #
    # Every submodule loads
    # ============================================================= #
    print("\n=== Every submodule loads ===")
    for name in _SUBMODULE_NAMES:
        try:
            submod = _load_submodule(name)
        except ImportError as exc:
            raise AssertionError(
                f"constraints.{name} failed to load: {exc}"
            )
        _check(
            submod is not None,
            f"constraints.{name} loaded "
            f"({getattr(submod, '__file__', '?')})",
        )

    # ============================================================= #
    # Every symbol resolves (not None)
    # ============================================================= #
    print("\n=== Every symbol resolves ===")
    unresolved: List[str] = []
    for sym in _SYMBOL_MAP:
        try:
            value = getattr(mod, sym)
        except (ImportError, AttributeError) as exc:
            unresolved.append(f"{sym} ({exc})")
            continue
        if value is None:
            unresolved.append(f"{sym} (resolved to None)")
    _check(
        not unresolved,
        f"all {len(_SYMBOL_MAP)} symbols resolve "
        f"({'no unresolved' if not unresolved else unresolved})",
    )

    # ============================================================= #
    # Symbol identity — package symbols are the module symbols
    # ============================================================= #
    print("\n=== Symbol identity ===")
    for sym, (submod_name, attr) in (
        ("BudgetManager", ("budget_manager", "BudgetManager")),
        ("BudgetEnforcer", ("budget_enforcer", "BudgetEnforcer")),
        ("BudgetChecker", ("check_energy_budget", "BudgetChecker")),
        (
            "GreenPolicyEnforcer",
            ("green_policy_enforcer", "GreenPolicyEnforcer"),
        ),
        ("CausalBudgetLearner", ("budget_domain", "CausalBudgetLearner")),
    ):
        pkg_sym = getattr(mod, sym)
        mod_obj = _load_submodule(submod_name)
        mod_sym = getattr(mod_obj, attr, None)
        _check(
            pkg_sym is mod_sym,
            f"{sym} is the same object as "
            f"constraints.{submod_name}.{attr}",
        )

    # ============================================================= #
    # Unknown attribute raises AttributeError
    # ============================================================= #
    print("\n=== Unknown attribute raises AttributeError ===")
    try:
        getattr(mod, "_definitely_not_a_symbol_xyz")
    except AttributeError as exc:
        _check(True, f"AttributeError: {exc}")
    else:
        raise AssertionError("unknown attribute did not raise")

    # ============================================================= #
    # Missing symbol raises ImportError (not None) — the v6 guarantee
    # ============================================================= #
    print("\n=== Missing symbol raises ImportError (not None) ===")
    _SYMBOL_MAP["_fake_symbol_test"] = (
        "_nonexistent_submodule",
        "_fake_attr",
    )
    try:
        try:
            getattr(mod, "_fake_symbol_test")
        except ImportError as exc:
            _check(True, f"ImportError: {exc}")
        except AttributeError:
            raise AssertionError(
                "expected ImportError, got AttributeError"
            )
        else:
            raise AssertionError("missing symbol did not raise")
    finally:
        _SYMBOL_MAP.pop("_fake_symbol_test", None)
        globals().pop("_fake_symbol_test", None)

    # ============================================================= #
    # Lazy loading — after the first access, the value is cached
    # ============================================================= #
    print("\n=== Lazy loading caches after first access ===")
    _check(
        "BudgetManager" in mod.__dict__,
        "BudgetManager is cached in constraints.__dict__ "
        "after first access",
    )
    _check(
        "GreenPolicyEnforcer" in mod.__dict__,
        "GreenPolicyEnforcer is cached in constraints.__dict__",
    )

    # ============================================================= #
    # Factories
    # ============================================================= #
    print("\n=== Factories ===")
    Budget_cls = getattr(mod, "Budget")
    eco = Budget_cls.eco_budget()

    mgr = mod.create_budget_manager(
        eco, deployment_id="demo", agent_id="demo-mgr",
    )
    _check(
        mgr is not None and mgr.__class__.__name__ == "BudgetManager",
        f"create_budget_manager returned {type(mgr).__name__}",
    )
    _check(
        mgr.budget is eco,
        "create_budget_manager passes the budget through",
    )

    enf = mod.create_budget_enforcer(
        max_energy=5.0, max_carbon=1.0, max_latency=1000.0,
        deployment_id="demo", agent_id="demo-enf",
    )
    _check(
        enf is not None and enf.__class__.__name__ == "BudgetEnforcer",
        f"create_budget_enforcer returned {type(enf).__name__}",
    )

    suite = mod.create_constraints_suite(
        eco,
        max_energy=5.0, max_carbon=1.0, max_latency=1000.0,
        deployment_id="demo",
    )
    _check(
        isinstance(suite, dict)
        and suite.get("manager") is not None
        and suite.get("enforcer") is not None,
        "create_constraints_suite(strict=True) returns both components",
    )

    # ============================================================= #
    # Suite factory strict=True raises on missing component
    # ============================================================= #
    print(
        "\n=== create_constraints_suite(strict=True) raises on "
        "missing component ==="
    )
    from unittest.mock import patch as _patch

    real_load = _load_submodule

    def _fake_load(name: str) -> Any:
        if name == "budget_enforcer":
            raise ImportError(
                "simulated: budget_enforcer unavailable"
            )
        return real_load(name)

    with _patch.object(mod, "_load_submodule", _fake_load):
        try:
            mod.create_constraints_suite(
                eco,
                max_energy=5.0,
                deployment_id="strict-test",
                strict=True,
            )
        except ImportError as exc:
            _check(
                True,
                f"strict=True raises ImportError ({exc})",
            )
        else:
            raise AssertionError(
                "strict=True did not raise on missing enforcer"
            )

        lenient = mod.create_constraints_suite(
            eco,
            max_energy=5.0,
            deployment_id="lenient-test",
            strict=False,
        )
        _check(
            lenient["enforcer"] is None,
            "strict=False returns enforcer=None",
        )
        _check(
            lenient["manager"] is not None,
            "strict=False still returns the manager",
        )

    # ============================================================= #
    # Feature presets
    # ============================================================= #
    print("\n=== Feature presets ===")
    _check(
        DEFAULT_FEATURES["chaos_testing"] is False,
        "DEFAULT_FEATURES leaves chaos_testing off",
    )
    _check(
        DEFAULT_FEATURES["aggregate_violations"] is False,
        "DEFAULT_FEATURES leaves aggregate_violations off (v6)",
    )
    _check(
        ENABLE_ALL_FEATURES["chaos_testing"] is False,
        "ENABLE_ALL_FEATURES deliberately excludes chaos_testing",
    )
    other_features = {
        k: v for k, v in ENABLE_ALL_FEATURES.items()
        if k != "chaos_testing"
    }
    _check(
        all(other_features.values()),
        f"all {len(other_features)} other features enabled",
    )
    _check(
        all(not v for v in DISABLE_ALL_FEATURES.values()),
        "DISABLE_ALL_FEATURES disables everything",
    )

    # ============================================================= #
    # Capability introspection
    # ============================================================= #
    print("\n=== Capability introspection ===")
    caps = mod.get_capabilities()
    for key in (
        "version",
        "compatible_modules",
        "submodules_loaded",
        "missing_symbols",
        "stub_symbols",
        "version_consistency",
        "features_available",
        "components",
    ):
        _check(key in caps, f"get_capabilities()['{key}'] present")

    _check(
        caps["version"] == "6.0.0",
        "capabilities.version is 6.0.0",
    )
    _check(
        caps["compatible_modules"] == "6.x",
        "capabilities.compatible_modules is '6.x'",
    )
    _check(
        caps["stub_symbols"] == [],
        "capabilities.stub_symbols is empty (no fallback stubs in v6)",
    )
    _check(
        caps["missing_symbols"] == {},
        f"capabilities.missing_symbols is empty "
        f"(got {caps['missing_symbols']})",
    )
    loaded_count = sum(
        1
        for info in caps["submodules_loaded"].values()
        if info.get("loaded")
    )
    _check(
        loaded_count == len(_SUBMODULE_NAMES),
        f"all {len(_SUBMODULE_NAMES)} submodules loaded "
        f"(got {loaded_count})",
    )
    for component, available in caps["components"].items():
        _check(available, f"component '{component}' is available")

    # ============================================================= #
    # Submodule-level `__version__` consistency
    # ============================================================= #
    print("\n=== Version consistency across submodules ===")
    if caps["version_consistency"] is None:
        _check(
            True,
            "no submodule declares __version__ "
            "(version_consistency=None, honest)",
        )
    else:
        _check(
            caps["version_consistency"] is True,
            "every submodule that declares __version__ agrees",
        )

    # ============================================================= #
    # Backward-compatible exceptions
    # ============================================================= #
    print("\n=== Backward-compatible exceptions ===")
    BudgetExceeded_cls = getattr(mod, "BudgetExceeded")
    _check(
        issubclass(BudgetExceeded_cls, Exception),
        "BudgetExceeded is an Exception subclass",
    )
    PolicyViolation_cls = getattr(mod, "PolicyViolation")
    _check(
        issubclass(PolicyViolation_cls, Exception),
        "PolicyViolation is an Exception subclass",
    )
    _check(
        BudgetExceeded_cls("test").details == {},
        "BudgetExceeded(message) defaults details to {}",
    )

    # ============================================================= #
    # `from constraints import submodule` works via __getattr__
    # ============================================================= #
    print("\n=== `from constraints import budget_domain` works ===")
    bd_mod = getattr(mod, "budget_domain", None)
    _check(
        bd_mod is not None
        and bd_mod.__name__.endswith("budget_domain"),
        f"constraints.budget_domain resolves ({bd_mod.__name__})",
    )

    # ============================================================= #
    # Final summary
    # ============================================================= #
    print("\n=== Summary ===")
    import json as _json

    print(_json.dumps(caps, indent=2, default=str))

    print("\n" + "=" * 78)
    print("All v6 assertions passed.")
    print("=" * 78)


if __name__ == "__main__":
    _demo()
