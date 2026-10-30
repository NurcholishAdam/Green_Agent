# src/integration/__init__.py

"""
Integration module for Green Agent v5.1 (Enhanced, auditable)
=============================================================

Provides free API integrations and community data sharing, plus first-class
access to the ten cross-cutting enhancement layers.

Fixes in v5.1 (see review)
--------------------------
  * P0 — `globals()[name] = None` for missing symbols replaced by PEP 562
         `__getattr__` so `hasattr()` and `from ... import` behave correctly
         for optional symbols (previously every missing name looked present).
  * P0 — `required=True` symbols now raise `ImportError` at import if the
         submodule doesn't provide them (previously only logged).
  * P0 — `__all__` filter aligned with the new binding semantics.
  * P1 — `create_manager` resolves the constructor signature *once* at
         import; unsupported kwargs are dropped on `TypeError` rather than
         probed per-argument inside the loop.
  * P1 — `CANONICAL_FEATURES` guarantees the presets are never empty even
         when `.free_apis` fails to import (so `enable_all=False` is not
         a silent no-op).
  * P1 — `enable_all` no longer silently overridden by `features=`; both
         are merged explicitly.
  * P1 — Diagnostics expose `IMPORT_ERROR`, `FREE_APIS_VERSION`,
         `CORE_MISSING`, so `get_capabilities()` distinguishes
         "old submodule" from "import broke".
  * P1 — Version compatibility check (MIN / RECOMMENDED) at import.
  * P2 — New helpers: `is_available`, `list_layers`, `list_features`,
         `enabled_features`, `health`, `test_smoke`.
  * P2 — `__dir__` returns the public surface, not the module `__dict__`.
  * P2 — Lazy `%s` logging on the hot import path.
  * P2 — `hardware=` coerces a dict to `HardwareProfile` (or raises).
  * P3 — `__author__` / `__license__` in `__all__`.
  * P3 — Callable `enable_all(manager)` / `disable_all(manager)` helpers.
  * P3 — `features_available` split into `features_canonical`,
         `features_advertised`, `features_missing`.
  * P3 — `_expose` records per-layer misses in `_MISSING_BY_LAYER`.
  * P3 — `test_smoke()` returns a structured health-check dict.

Original public API (backward compatible)
-----------------------------------------
    FreeAPIManager, StaticDataProvider, FreeWeatherAPI, FreeGridCarbonAPI,
    SelfHostedGridCarbon, HeliumMarketSimulator, CommunityDataHub,
    CarbonData, WeatherData, HeliumData, GridMixData

Enhanced public API (grouped by layer)
--------------------------------------
    1. Quantum-Distillation:  QuantumDistillationBridge, DistilledHeliumModel
    2. Causal RL:             CausalSourcePolicy, SourceState
    3. Federated:             FederatedObservationAggregator,
                              FederatedObservationProfile
    4. Multi-Agent:           MultiAgentCoordinator, AgentProfile, AgentRole
    5. Temporal Logic:        TemporalLogicMonitor, DataFreshness,
                              CarbonBounds, PriceBounds, ConfidenceBounded
    6. XAI:                   DataExplainer, SourceExplanation
    7. Adaptive Precision:    AdaptivePrecisionController, HardwareProfile
    8. Carbon Markets:        CarbonMarketClient, MarketSnapshot
    9. Resilience & Chaos:    CircuitBreaker, ChaosInjector, CircuitState
   10. HITL:                  HumanInTheLoopGate, HITLRequest

Supporting enums / helpers
--------------------------
    PrecisionLevel, ProviderKind, DataTier
    DEFAULT_FEATURES, ENABLE_ALL_FEATURES, DISABLE_ALL_FEATURES
    CANONICAL_FEATURES, CANONICAL_LAYERS
    create_manager(), enable_all(), disable_all()
    get_capabilities(), health(), test_smoke()
    is_available(), list_layers(), list_features(), enabled_features()
    __version__, __author__, __license__
    SUBMODULE_AVAILABLE, IMPORT_ERROR, FREE_APIS_VERSION, CORE_MISSING
"""

from __future__ import annotations

import inspect
import logging
import re
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.1.0"
__author__ = "AI Research Agent Team"
__license__ = "MIT"


# =============================================================================
# 0. Canonical names — stable even if .free_apis fails to import
# =============================================================================

CANONICAL_FEATURES: Tuple[str, ...] = (
    "quantum_distillation",
    "causal_rl",
    "federated",
    "multi_agent",
    "temporal_logic",
    "xai",
    "adaptive_precision",
    "carbon_markets",
    "resilience",
    "hitl",
)

CANONICAL_LAYERS: Tuple[str, ...] = (
    "quantum_distillation",
    "causal_rl",
    "federated",
    "multi_agent",
    "temporal_logic",
    "xai",
    "adaptive_precision",
    "carbon_markets",
    "resilience",
    "hitl",
    "enums",
)

#: Oldest submodule version this API tolerates.
MIN_FREE_APIS_VERSION = "5.0.0"
#: Submodule version that provides every enhancement symbol.
RECOMMENDED_FREE_APIS_VERSION = "5.1.0"


# =============================================================================
# 1. Submodule import + version compatibility
# =============================================================================

_free_apis: Any = None
SUBMODULE_AVAILABLE = False
IMPORT_ERROR: Optional[str] = None
FREE_APIS_VERSION: Optional[str] = None

try:
    _free_apis = import_module(".free_apis", package=__name__)
    SUBMODULE_AVAILABLE = True
    FREE_APIS_VERSION = getattr(_free_apis, "__version__", None)
except ImportError as _exc:  # pragma: no cover
    logger.error("Could not import .free_apis: %s", _exc)
    IMPORT_ERROR = f"{type(_exc).__name__}: {_exc}"
except Exception as _exc:  # pragma: no cover
    logger.exception("Unexpected error importing .free_apis")
    IMPORT_ERROR = f"{type(_exc).__name__}: {_exc}"


def _check_submodule_version() -> None:
    """Raise if free_apis is too old; warn if merely below recommended."""
    if not FREE_APIS_VERSION:
        return
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        logger.debug("packaging not installed; skipping version check")
        return
    try:
        installed = Version(FREE_APIS_VERSION)
        minimum = Version(MIN_FREE_APIS_VERSION)
        recommended = Version(RECOMMENDED_FREE_APIS_VERSION)
    except InvalidVersion:
        logger.warning(
            "free_apis has non-PEP-440 version %r; skipping check",
            FREE_APIS_VERSION,
        )
        return
    if installed < minimum:
        raise ImportError(
            f"src.integration requires free_apis >= {MIN_FREE_APIS_VERSION}; "
            f"found {FREE_APIS_VERSION}"
        )
    if installed < recommended:
        logger.warning(
            "free_apis %s is older than recommended %s; "
            "some enhancement symbols may be missing.",
            FREE_APIS_VERSION,
            RECOMMENDED_FREE_APIS_VERSION,
        )


_check_submodule_version()


# =============================================================================
# 2. Symbol exposure — PEP 562 __getattr__ for missing optionals
# =============================================================================

_MISSING_OPTIONAL: Set[str] = set()
_MISSING_BY_LAYER: Dict[str, List[str]] = {}


def _expose(
    *names: str,
    required: bool = False,
    layer: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    """
    Lift `names` from `.free_apis` into this module's `globals()`.

    Required symbols that are missing raise `ImportError` immediately.
    Optional symbols that are missing are recorded in `_MISSING_OPTIONAL`;
    their access (via `module.name` / `from module import name`) triggers
    `__getattr__`, which raises a descriptive `AttributeError`.
    """
    exposed: List[str] = []
    missing: List[str] = []

    for n in names:
        if _free_apis is not None and hasattr(_free_apis, n):
            globals()[n] = getattr(_free_apis, n)
            exposed.append(n)
        else:
            missing.append(n)
            if not required:
                _MISSING_OPTIONAL.add(n)

    if required and missing:
        raise ImportError(
            f"src.integration requires {missing} from .free_apis. "
            f"submodule_available={_free_apis is not None}, "
            f"version={FREE_APIS_VERSION!r}, "
            f"import_error={IMPORT_ERROR!r}"
        )

    if layer is not None:
        _MISSING_BY_LAYER[layer] = list(missing)
    elif missing and not required:
        _MISSING_BY_LAYER.setdefault("<ungrouped>", []).extend(missing)

    if missing and not required:
        logger.debug(
            "Missing optional symbols%s: %s",
            f" for layer '{layer}'" if layer else "",
            missing,
        )

    return exposed, missing


def __getattr__(name: str) -> Any:
    """PEP 562 module-level attribute fallback."""
    if name in _MISSING_OPTIONAL:
        raise AttributeError(
            f"Optional symbol '{name}' is not provided by .free_apis "
            f"(package v{__version__}, "
            f"free_apis v{FREE_APIS_VERSION or 'unknown'}). "
            f"This usually means an older free_apis is installed; "
            f"upgrade free_apis to >= {RECOMMENDED_FREE_APIS_VERSION} "
            f"to enable this layer."
        )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def __dir__() -> List[str]:
    return sorted(__all__)


# --- 1a. Core (original API — required) ---

_CORE_EXPOSED, _CORE_MISSING = _expose(
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


# --- 1b. Enhancements (all optional) ---

_ENH_QUANTUM, _ = _expose(
    "QuantumDistillationBridge", "DistilledHeliumModel",
    layer="quantum_distillation",
)
_ENH_CAUSAL_RL, _ = _expose(
    "CausalSourcePolicy", "SourceState",
    layer="causal_rl",
)
_ENH_FEDERATED, _ = _expose(
    "FederatedObservationAggregator", "FederatedObservationProfile",
    layer="federated",
)
_ENH_MULTI_AGENT, _ = _expose(
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    layer="multi_agent",
)
_ENH_TEMPORAL, _ = _expose(
    "TemporalLogicMonitor", "DataFreshness", "CarbonBounds",
    "PriceBounds", "ConfidenceBounded",
    layer="temporal_logic",
)
_ENH_XAI, _ = _expose(
    "DataExplainer", "SourceExplanation",
    layer="xai",
)
_ENH_PRECISION, _ = _expose(
    "AdaptivePrecisionController", "HardwareProfile",
    layer="adaptive_precision",
)
_ENH_MARKETS, _ = _expose(
    "CarbonMarketClient", "MarketSnapshot",
    layer="carbon_markets",
)
_ENH_RESILIENCE, _ = _expose(
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    layer="resilience",
)
_ENH_HITL, _ = _expose(
    "HumanInTheLoopGate", "HITLRequest",
    layer="hitl",
)
_ENH_ENUMS, _ = _expose(
    "PrecisionLevel", "ProviderKind", "DataTier",
    layer="enums",
)


# =============================================================================
# 3. Feature toggles — canonical list guarantees presets are never empty
# =============================================================================

#: Features the submodule advertises (empty if it doesn't publish the list).
FEATURES_ADVERTISED: List[str] = []

DEFAULT_FEATURES: Dict[str, bool] = {
    name: True for name in CANONICAL_FEATURES
}

if _free_apis is not None:
    _manager_class = getattr(_free_apis, "FreeAPIManager", None)
    _advertised_raw = getattr(_manager_class, "DEFAULT_FEATURES", None)
    if isinstance(_advertised_raw, dict):
        try:
            FEATURES_ADVERTISED = sorted(_advertised_raw.keys())
            DEFAULT_FEATURES.update(dict(_advertised_raw))
        except Exception:  # noqa: BLE001 — defensive
            logger.debug(
                "Could not read FreeAPIManager.DEFAULT_FEATURES; "
                "falling back to canonical list."
            )

ENABLE_ALL_FEATURES: Dict[str, bool] = {
    k: True for k in DEFAULT_FEATURES
}
DISABLE_ALL_FEATURES: Dict[str, bool] = {
    k: False for k in DEFAULT_FEATURES
}


# =============================================================================
# 4. Manager constructor signature cache
# =============================================================================

_MANAGER_INIT_PARAMS: Optional[Set[str]] = None
_MANAGER_ACCEPTS_VAR_KW: bool = False

if "FreeAPIManager" in globals():
    try:
        _sig = inspect.signature(globals()["FreeAPIManager"].__init__)
        _MANAGER_INIT_PARAMS = set(_sig.parameters)
        _MANAGER_ACCEPTS_VAR_KW = any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in _sig.parameters.values()
        )
    except (TypeError, ValueError, AttributeError):
        _MANAGER_INIT_PARAMS = None
        _MANAGER_ACCEPTS_VAR_KW = True  # assume permissive


def _build_manager_kwargs(
    base: Dict[str, Any],
    optional: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Return `base + optional` filtered by what the constructor accepts.

    When inspection failed, pass everything through and let the retry loop
    in `create_manager()` handle unsupported kwargs.
    """
    kwargs: Dict[str, Any] = dict(base)
    if _MANAGER_INIT_PARAMS is None or _MANAGER_ACCEPTS_VAR_KW:
        kwargs.update({k: v for k, v in optional.items() if v is not None})
        return kwargs
    for k, v in optional.items():
        if v is not None and k in _MANAGER_INIT_PARAMS:
            kwargs[k] = v
    return kwargs


def _call_manager_with_retry(
    factory: Callable[..., Any],
    kwargs: Dict[str, Any],
) -> Any:
    """
    Call `factory(**kwargs)`. If it raises `TypeError` for an unknown
    keyword (e.g. a C-extension backend), drop that kwarg and retry.
    Non-kwarg TypeErrors propagate unchanged.
    """
    while True:
        try:
            return factory(**kwargs)
        except TypeError as exc:
            match = re.search(
                r"unexpected keyword argument ['\"](\w+)['\"]",
                str(exc),
            )
            if not match or match.group(1) not in kwargs:
                raise
            dropped = match.group(1)
            logger.debug(
                "Constructor rejected kwarg %r; retrying without it.",
                dropped,
            )
            kwargs.pop(dropped)


# =============================================================================
# 5. Hardware coercion
# =============================================================================

def _coerce_hardware(hardware: Any) -> Any:
    if hardware is None:
        return None
    hw_cls = globals().get("HardwareProfile")
    if hw_cls is not None and isinstance(hardware, hw_cls):
        return hardware
    if isinstance(hardware, dict):
        if hw_cls is None:
            raise ImportError(
                "Cannot coerce `hardware` dict: HardwareProfile is not "
                "available from .free_apis (upgrade the submodule)."
            )
        try:
            return hw_cls(**hardware)
        except TypeError as exc:
            raise TypeError(
                f"Could not construct HardwareProfile from dict: {exc}"
            ) from exc
    raise TypeError(
        f"`hardware` must be a HardwareProfile instance or dict; "
        f"got {type(hardware).__name__}"
    )


# =============================================================================
# 6. Feature validation / merging
# =============================================================================

def _validate_features(features: Dict[str, bool]) -> Dict[str, bool]:
    if not isinstance(features, dict):
        raise TypeError(
            f"`features` must be a dict, got {type(features).__name__}"
        )
    unknown = set(features) - set(DEFAULT_FEATURES)
    if unknown:
        raise ValueError(
            f"Unknown feature toggles: {sorted(unknown)}. "
            f"Known features: {sorted(DEFAULT_FEATURES)}"
        )
    return dict(features)


def _resolve_features(
    features: Optional[Dict[str, bool]],
    enable_all: bool,
) -> Dict[str, bool]:
    if features is None:
        if not DEFAULT_FEATURES:
            logger.warning(
                "create_manager(): DEFAULT_FEATURES is empty — the "
                ".free_apis submodule did not advertise features. "
                "The agent will run with no enhancement layers."
            )
        return dict(
            ENABLE_ALL_FEATURES if enable_all else DISABLE_ALL_FEATURES
        )
    validated = _validate_features(features)
    if enable_all is True:
        # Explicit overrides win, but unspecified defaults stay ON.
        merged = dict(ENABLE_ALL_FEATURES)
        merged.update(validated)
        return merged
    return validated


# =============================================================================
# 7. Convenience factory
# =============================================================================

def create_manager(
    config: Optional[Dict[str, Any]] = None,
    *,
    deployment_id: str = "local",
    agent_id: str = "free-api-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Any = None,
    enable_all: bool = True,
) -> "FreeAPIManager":  # type: ignore[name-defined]
    """
    Create a fully-configured `FreeAPIManager` with enhancement layers wired in.

    Args:
        config:         Optional config dict passed to `FreeAPIManager`.
        deployment_id:  Identifier for federated learning.
        agent_id:       Identifier for multi-agent coordination.
        features:       Optional explicit feature toggle dict. Unknown keys
                        raise `ValueError`. When `enable_all=True` and
                        `features` is provided, unspecified features stay ON.
        hardware:       Optional `HardwareProfile` (or a dict that will be
                        coerced to one).
        enable_all:     When `features is None`, enable every available
                        enhancement. When `features` is provided, treat it
                        as an override on top of the "all on" baseline.

    Returns:
        A configured `FreeAPIManager` instance.

    Raises:
        ImportError: `FreeAPIManager` is not available.
        ValueError:  `features` contains unknown keys.
        TypeError:   `hardware` is neither a `HardwareProfile` nor a dict.
    """
    if "FreeAPIManager" not in globals() or globals()["FreeAPIManager"] is None:
        raise ImportError(
            "FreeAPIManager is not available. Ensure "
            "src/integration/free_apis.py is importable. "
            f"submodule_available={SUBMODULE_AVAILABLE}, "
            f"import_error={IMPORT_ERROR!r}"
        )

    resolved_features = _resolve_features(features, enable_all)
    coerced_hardware = _coerce_hardware(hardware)

    kwargs = _build_manager_kwargs(
        base={"config": config or {}},
        optional={
            "deployment_id": deployment_id,
            "agent_id": agent_id,
            "features": resolved_features,
            "hardware": coerced_hardware,
        },
    )
    return _call_manager_with_retry(globals()["FreeAPIManager"], kwargs)


# =============================================================================
# 8. Capability introspection
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """Return a structured capability report suitable for logs / tickets."""
    return {
        "package_version": __version__,
        "free_apis_version": FREE_APIS_VERSION,
        "submodule_available": SUBMODULE_AVAILABLE,
        "import_error": IMPORT_ERROR,
        "core_symbols": list(_CORE_EXPOSED),
        "core_missing": list(_CORE_MISSING),
        "core_complete": not _CORE_MISSING,
        "layers": {
            "quantum_distillation": list(_ENH_QUANTUM),
            "causal_rl": list(_ENH_CAUSAL_RL),
            "federated": list(_ENH_FEDERATED),
            "multi_agent": list(_ENH_MULTI_AGENT),
            "temporal_logic": list(_ENH_TEMPORAL),
            "xai": list(_ENH_XAI),
            "adaptive_precision": list(_ENH_PRECISION),
            "carbon_markets": list(_ENH_MARKETS),
            "resilience": list(_ENH_RESILIENCE),
            "hitl": list(_ENH_HITL),
            "enums": list(_ENH_ENUMS),
        },
        "layers_missing": {
            k: list(v) for k, v in _MISSING_BY_LAYER.items()
        },
        "features_canonical": list(CANONICAL_FEATURES),
        "features_advertised": list(FEATURES_ADVERTISED),
        "features_missing": [
            n for n in CANONICAL_FEATURES
            if n not in FEATURES_ADVERTISED
        ],
        "features_enabled_default": dict(DEFAULT_FEATURES),
    }


def is_available(name: str) -> bool:
    """True if `name` was successfully exposed from `.free_apis`."""
    return name in globals() and globals()[name] is not None


def list_layers() -> List[str]:
    """Stable, documented layer names."""
    return list(CANONICAL_LAYERS)


def list_features() -> List[str]:
    """Sorted canonical feature names."""
    return sorted(CANONICAL_FEATURES)


def enabled_features(manager: Any) -> Dict[str, bool]:
    """Return the effective feature toggles of an already-built manager."""
    feats = getattr(manager, "features", None)
    return dict(feats) if isinstance(feats, dict) else {}


def health() -> Dict[str, Any]:
    """Compact health report suitable for `/health` endpoints and CI gates."""
    caps = get_capabilities()
    ok = caps["submodule_available"] and caps["core_complete"]
    if ok:
        reason: Optional[str] = None
    elif not caps["submodule_available"]:
        reason = caps["import_error"] or "submodule not importable"
    else:
        reason = f"missing core symbols: {caps['core_missing']}"

    layers_present = sum(1 for v in caps["layers"].values() if v)
    layers_total = len(caps["layers"])
    return {
        "ok": ok,
        "reason": reason,
        "package_version": caps["package_version"],
        "free_apis_version": caps["free_apis_version"],
        "layers_available": layers_present,
        "layers_total": layers_total,
        "features_missing": caps["features_missing"],
    }


def test_smoke() -> Dict[str, Any]:
    """
    End-to-end self-check: does the package load *and* can it produce a
    manager? Returns a structured result; never raises.
    """
    results: Dict[str, Any] = {
        "package_version": __version__,
        "submodule_available": SUBMODULE_AVAILABLE,
        "free_apis_version": FREE_APIS_VERSION,
        "import_error": IMPORT_ERROR,
        "manager_created": False,
        "ok": False,
    }
    if not SUBMODULE_AVAILABLE:
        results["reason"] = IMPORT_ERROR or "submodule not importable"
        return results

    try:
        mgr = create_manager(enable_all=False)
        results["manager_created"] = True
        results["manager_type"] = type(mgr).__name__
        if hasattr(mgr, "get_statistics"):
            try:
                stats = mgr.get_statistics()
                results["stats_keys"] = (
                    list(stats.keys()) if isinstance(stats, dict) else None
                )
            except Exception as exc:  # noqa: BLE001
                results["stats_error"] = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001
        results["manager_error"] = f"{type(exc).__name__}: {exc}"

    results["ok"] = results["manager_created"] and not _CORE_MISSING
    return results


# =============================================================================
# 9. Callable feature-toggle helpers (complement the dict presets)
# =============================================================================

def enable_all(manager: Any) -> None:
    """Enable every known feature on an existing manager."""
    feats = getattr(manager, "features", None)
    if isinstance(feats, dict):
        feats.update(ENABLE_ALL_FEATURES)


def disable_all(manager: Any) -> None:
    """Disable every known feature on an existing manager."""
    feats = getattr(manager, "features", None)
    if isinstance(feats, dict):
        feats.update(DISABLE_ALL_FEATURES)


# =============================================================================
# 10. Public API surface (`__all__`)
# =============================================================================

__all__: List[str] = []

# --- Core ---
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
# --- Enhancements ---
__all__ += [
    "QuantumDistillationBridge", "DistilledHeliumModel",
    "CausalSourcePolicy", "SourceState",
    "FederatedObservationAggregator", "FederatedObservationProfile",
    "MultiAgentCoordinator", "AgentProfile", "AgentRole",
    "TemporalLogicMonitor", "DataFreshness", "CarbonBounds",
    "PriceBounds", "ConfidenceBounded",
    "DataExplainer", "SourceExplanation",
    "AdaptivePrecisionController", "HardwareProfile",
    "CarbonMarketClient", "MarketSnapshot",
    "CircuitBreaker", "ChaosInjector", "CircuitState",
    "HumanInTheLoopGate", "HITLRequest",
    "PrecisionLevel", "ProviderKind", "DataTier",
]
# --- Package-level helpers & metadata ---
__all__ += [
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "DISABLE_ALL_FEATURES",
    "CANONICAL_FEATURES",
    "CANONICAL_LAYERS",
    "create_manager",
    "enable_all",
    "disable_all",
    "get_capabilities",
    "health",
    "test_smoke",
    "is_available",
    "list_layers",
    "list_features",
    "enabled_features",
    "SUBMODULE_AVAILABLE",
    "IMPORT_ERROR",
    "FREE_APIS_VERSION",
    "MIN_FREE_APIS_VERSION",
    "RECOMMENDED_FREE_APIS_VERSION",
    "__version__",
    "__author__",
    "__license__",
]

# Exclude any name we never successfully exposed (missing optionals + any
# not-yet-populated names). With the `__getattr__` model there are no
# `None` bindings, so this reduces to "present in globals()".
__all__ = [n for n in __all__ if n in globals()]


# =============================================================================
# 11. Load diagnostics
# =============================================================================

if SUBMODULE_AVAILABLE and not _CORE_MISSING:
    logger.debug(
        "src.integration v%s ready: %d core, %d enhancement symbols; "
        "%d/%d features advertised",
        __version__,
        len(_CORE_EXPOSED),
        sum(len(x) for x in (
            _ENH_QUANTUM, _ENH_CAUSAL_RL, _ENH_FEDERATED,
            _ENH_MULTI_AGENT, _ENH_TEMPORAL, _ENH_XAI,
            _ENH_PRECISION, _ENH_MARKETS, _ENH_RESILIENCE,
            _ENH_HITL, _ENH_ENUMS,
        )),
        len(FEATURES_ADVERTISED),
        len(CANONICAL_FEATURES),
    )
elif _CORE_MISSING:  # pragma: no cover — _expose would have raised
    logger.warning(
        "src.integration loaded with missing core symbols: %s",
        _CORE_MISSING,
    )
