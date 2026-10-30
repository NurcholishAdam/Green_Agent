# src/monitoring/__init__.py

"""
Real-time monitoring and metrics collection modules (v5.1)
=========================================================

Exposes the Green Agent's observability surface: Prometheus-format graph
metrics, helium supply metrics, and metrics from all ten cross-cutting
enhancement layers.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``globals()[name] = None`` for missing symbols replaced by PEP 562
         ``__getattr__``. Missing optional symbols now raise ``AttributeError``
         with import-error context instead of silently returning ``None``.
         This fixes ``hasattr(module, "X")`` reporting ``True`` for every
         symbol (the previous behaviour made ``from src.monitoring import X``
         bind ``None`` instead of raising ``ImportError``).
  * P0 — ``metrics_collector`` now resolves against multiple candidate
         paths: ``.metrics_collector`` (local), ``..metrics.metrics_collection``
         (the enhanced collector's actual location), and
         ``metrics.metrics_collection`` (absolute). Previously the loader
         only tried ``src.monitoring.metrics_collector`` — a path that does
         not exist in the enhanced tree — so ``MetricsCollector`` and
         ``MetricsSnapshot`` were always ``None``.
  * P0 — ``__version__ = "5.1.0"`` (was ``"5.0.0"``).
  * P0 — ``_expose`` distinguishes required from optional at the runtime
         level: ``required=True`` raises ``ImportError`` immediately,
         matching the pattern in ``src/integration/__init__.py``.
  * P0 — ``create_exporter()`` supports the v5.1 enhancements:
         ``collector=`` kwarg passes through to ``EnhancementMetricsSources``
         and ``helium_baseline_price_usd=`` passes through to
         ``GraphMetricsExporter``. ``inspect.signature`` is used to skip
         these kwargs when the underlying submodule is a v5.0 build.
  * P1 — Submodule version tracking: ``SUBMODULE_VERSIONS`` and
         ``IMPORT_ERRORS`` are exported so partial upgrades surface in
         ``get_capabilities()`` instead of silently proceeding.
  * P1 — Diagnostics: ``test_smoke()`` exercises the exporter end-to-end
         (constructs a stub registry, calls ``collect(dry_run=True)`` and
         ``render()``); ``health()`` returns a compact dict for ``/health``
         endpoints; ``get_capabilities()`` distinguishes "missing" from
         "present but broken".
  * P1 — New helpers: ``list_features()``, ``is_available(name)``,
         ``enable_all(manager)``, ``disable_all(manager)``.
  * P1 — ``DEFAULT_FEATURES`` includes ``helium_profiling`` (was missing
         relative to the enhanced ``MetricsCollector``).
  * P2 — ``__dir__()`` returns the public surface for clean IDE
         autocomplete; ``__all__`` is filtered against actual bindings.
  * P3 — Env-var gate (``GREEN_AGENT_VALIDATE_MONITORING``) runs
         ``test_smoke()`` and raises ``RuntimeError`` on failure;
         ``__main__`` block prints capabilities and smoke report as JSON.

Backward compatibility
----------------------
- ``create_exporter(registry, *, job_name=..., max_edges_export=...,
  helium_monitor=..., ...)`` keeps its positional and keyword-only signature.
  New kwargs (``collector=``, ``helium_baseline_price_usd=``) are optional.
- ``from src.monitoring import GraphMetricsExporter`` raises ``ImportError``
  when the exporter is not importable (previously it bound ``None``). Callers
  that relied on the ``None`` binding should switch to ``hasattr()`` or
  catch ``ImportError``.
- ``DEFAULT_FEATURES`` gains one key (``helium_profiling``). Callers that
  iterate the dict see one extra entry.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

__version__ = "5.1.0"
__author__ = "Green Agent Team"
__license__ = "MIT"

#: Minimum acceptable submodule version.
_MIN_SUBMODULE_VERSION = "5.1.0"


# =============================================================================
# Submodule loading
# =============================================================================
# Each entry maps a canonical submodule name to a list of candidate import
# paths (relative or absolute). The loader tries each in order and records
# which path succeeded. This handles the case where the enhanced
# ``metrics_collector`` lives under ``src.metrics.metrics_collection``
# rather than alongside the exporter.

_SUBMODULE_SOURCES: Dict[str, List[str]] = {
    "graph_metrics_exporter": [
        ".graph_metrics_exporter",
    ],
    "metrics_collector": [
        ".metrics_collector",                    # local (if co-located)
        "..metrics.metrics_collection",          # enhanced collector's home
        "metrics.metrics_collection",            # absolute fallback
    ],
}

#: Loaded submodules: ``{name: module_object_or_None}``.
_SUBMODULES: Dict[str, Optional[Any]] = {}

#: Which path actually loaded each submodule (``None`` if all failed).
_SUBMODULE_PATHS: Dict[str, Optional[str]] = {}

#: Submodule versions (``None`` when the submodule failed to load or lacks
#: ``__version__``).
_SUBMODULE_VERSIONS: Dict[str, Optional[str]] = {}

#: Import error messages for failed submodules.
_SUBMODULE_IMPORT_ERRORS: Dict[str, Optional[str]] = {}

#: Symbols not provided by any loaded submodule.
_MISSING_OPTIONAL: set = set()


def _load_submodule(
    name: str, candidate_paths: List[str],
) -> Optional[Any]:
    """Try each path in order; return the first successful module."""
    for path in candidate_paths:
        try:
            mod = import_module(path, package=__name__)
            _SUBMODULE_PATHS[name] = path
            _SUBMODULE_VERSIONS[name] = getattr(
                mod, "__version__", None,
            )
            _SUBMODULE_IMPORT_ERRORS[name] = None
            logger.debug(
                "Loaded %s from %s (version=%s)",
                name, path, _SUBMODULE_VERSIONS[name],
            )
            return mod
        except ImportError as exc:
            _SUBMODULE_IMPORT_ERRORS[name] = (
                f"{path}: {type(exc).__name__}: {exc}"
            )
            logger.debug(
                "Submodule %s: %s not importable (%s)",
                name, path, exc,
            )
            continue
        except Exception as exc:  # noqa: BLE001 — defensive
            _SUBMODULE_IMPORT_ERRORS[name] = (
                f"{path}: {type(exc).__name__}: {exc}"
            )
            logger.warning(
                "Submodule %s: %s raised on import (%s)",
                name, path, exc,
            )
            continue
    _SUBMODULE_PATHS[name] = None
    _SUBMODULE_VERSIONS[name] = None
    if _SUBMODULE_IMPORT_ERRORS.get(name) is None:
        _SUBMODULE_IMPORT_ERRORS[name] = "no candidate paths tried"
    return None


for _name, _paths in _SUBMODULE_SOURCES.items():
    _SUBMODULES[_name] = _load_submodule(_name, _paths)


def _check_submodule_versions() -> None:
    """Warn (not raise) on partial upgrades."""
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        logger.debug("packaging not installed; skipping version check")
        return
    required = Version(_MIN_SUBMODULE_VERSION)
    for name, version in _SUBMODULE_VERSIONS.items():
        if not version:
            continue
        try:
            installed = Version(version)
        except InvalidVersion:
            logger.warning(
                "%s has non-PEP-440 version %r; skipping check",
                name, version,
            )
            continue
        if installed < required:
            logger.warning(
                "src.monitoring v%s recommends %s >= %s; found %s. "
                "Some enhanced features may be missing.",
                __version__, name, _MIN_SUBMODULE_VERSION, version,
            )


_check_submodule_versions()


def _import_error_suffix() -> str:
    """Human-readable summary of import failures for error messages."""
    parts = [
        f"{name}: {err}"
        for name, err in _SUBMODULE_IMPORT_ERRORS.items()
        if err
    ]
    if not parts:
        return "The optional symbol is not provided by any loaded submodule."
    return "Import errors: " + "; ".join(parts)


# =============================================================================
# PEP 562 module attribute fallback
# =============================================================================

def __getattr__(name: str) -> Any:
    """Raise a descriptive ``AttributeError`` for missing optional symbols."""
    if name in _MISSING_OPTIONAL:
        raise AttributeError(
            f"Optional symbol {name!r} is unavailable from "
            f"{__name__} (v{__version__}). {_import_error_suffix()}"
        )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def __dir__() -> List[str]:
    return sorted(__all__)


# =============================================================================
# Symbol exposure
# =============================================================================

def _expose(
    submodule_name: str,
    *symbols: str,
    required: bool = False,
) -> List[str]:
    """
    Lift ``symbols`` from a submodule into this package's globals.

    Parameters
    ----------
    submodule_name : str
        Canonical submodule name (key in ``_SUBMODULES``).
    symbols : tuple of str
        Symbols to expose.
    required : bool
        When True, raises ``ImportError`` if any symbol is missing. When
        False, missing symbols are tracked in ``_MISSING_OPTIONAL`` and
        their access triggers a descriptive ``AttributeError`` via
        ``__getattr__``.

    Returns
    -------
    list of str
        Symbols actually exposed (populated into ``globals()``).
    """
    mod = _SUBMODULES.get(submodule_name)
    exposed: List[str] = []
    for sym in symbols:
        if mod is not None and hasattr(mod, sym):
            globals()[sym] = getattr(mod, sym)
            exposed.append(sym)
        else:
            _MISSING_OPTIONAL.add(sym)
            if required:
                raise ImportError(
                    f"Required symbol {sym!r} missing from "
                    f"{submodule_name}. "
                    f"{_SUBMODULE_IMPORT_ERRORS.get(submodule_name) or ''}"
                )
            logger.debug(
                "Optional symbol %r unavailable from %s",
                sym, submodule_name,
            )
    return exposed


_EXPORTED_FROM_EXPORTER = _expose(
    "graph_metrics_exporter",
    "GraphMetricsExporter",
    "EnhancementMetricsSources",
    required=False,
)

_EXPORTED_FROM_COLLECTOR = _expose(
    "metrics_collector",
    "MetricsCollector",
    "MetricsSnapshot",
    required=False,
)


# =============================================================================
# Feature toggles (matches the enhanced ``MetricsCollector``)
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
    "helium_profiling": True,          # added in v5.1
}

ENABLE_ALL_FEATURES: Dict[str, bool] = {
    k: True for k in DEFAULT_FEATURES
}
DISABLE_ALL_FEATURES: Dict[str, bool] = {
    k: False for k in DEFAULT_FEATURES
}


# =============================================================================
# Feature helpers
# =============================================================================

def list_features() -> List[str]:
    """Sorted canonical feature names."""
    return sorted(DEFAULT_FEATURES)


def is_available(name: str) -> bool:
    """
    True if ``name`` was successfully exposed from a submodule.

    Unlike ``hasattr(module, name)``, this reflects only genuinely-bound
    symbols, so a symbol that is missing (but would raise a descriptive
    ``AttributeError`` via ``__getattr__``) returns ``False``.
    """
    return name in globals()


def enable_all(manager: Any) -> None:
    """Enable every known feature on an already-constructed manager."""
    feats = getattr(manager, "features", None)
    if isinstance(feats, dict):
        feats.update(ENABLE_ALL_FEATURES)


def disable_all(manager: Any) -> None:
    """Disable every known feature on an already-constructed manager."""
    feats = getattr(manager, "features", None)
    if isinstance(feats, dict):
        feats.update(DISABLE_ALL_FEATURES)


# =============================================================================
# Factory
# =============================================================================

def _supported_kwargs(
    cls_or_func: Callable[..., Any], *names: str,
) -> set:
    """
    Return the subset of ``names`` accepted by ``cls_or_func``'s signature.

    Handles ``**kwargs`` by treating every name as supported.
    """
    try:
        params = inspect.signature(cls_or_func).parameters
    except (TypeError, ValueError):
        return set(names)
    if any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in params.values()
    ):
        return set(names)
    return {n for n in names if n in params}


def create_exporter(
    registry: Any,
    *,
    job_name: str = "green_agent",
    max_edges_export: int = 100,
    helium_monitor: Any = None,
    helium_baseline_price_usd: float = 4.0,
    collector: Any = None,
    distiller: Any = None,
    rl_policy: Any = None,
    federated: Any = None,
    coordinator: Any = None,
    temporal_monitor: Any = None,
    explainer: Any = None,
    precision_controller: Any = None,
    precision_counts: Optional[Dict[str, int]] = None,
    market: Any = None,
    circuit_breakers: Optional[Dict[str, Any]] = None,
    chaos: Any = None,
    hitl: Any = None,
) -> Any:
    """
    Build a fully-configured :class:`GraphMetricsExporter`.

    Parameters
    ----------
    registry : GraphRegistry
        Registry providing ``health()`` and ``get(...)``.
    job_name : str
        Prometheus job label (should also be set in the scrape config).
    max_edges_export : int
        Cardinality cap per graph type.
    helium_monitor : HeliumMonitor, optional
        When provided, helium metrics are emitted.
    helium_baseline_price_usd : float
        Reference price for computing the helium premium. Only passed
        through when the underlying exporter accepts it (v5.1).
    collector : MetricsCollector, optional
        When provided, its ``get_statistics()`` becomes the authoritative
        source for the layer metrics. Only passed through when the
        underlying :class:`EnhancementMetricsSources` accepts it (v5.1).
    distiller, rl_policy, federated, coordinator, temporal_monitor,
    explainer, precision_controller, market, chaos, hitl : object, optional
        Individual layer sources. Missing sources are omitted from the
        emitted metrics.
    precision_counts : dict, optional
        ``{precision_label: task_count}`` for the adaptive-precision layer.
    circuit_breakers : dict, optional
        ``{subsystem_name: CircuitBreaker}`` for the resilience layer.
    """
    if "GraphMetricsExporter" not in globals():
        raise ImportError(
            f"GraphMetricsExporter is not available. "
            f"{_SUBMODULE_IMPORT_ERRORS.get('graph_metrics_exporter') or ''}"
        )
    if "EnhancementMetricsSources" not in globals():
        raise ImportError(
            f"EnhancementMetricsSources is not available. "
            f"{_SUBMODULE_IMPORT_ERRORS.get('graph_metrics_exporter') or ''}"
        )

    exporter_cls = globals()["GraphMetricsExporter"]
    sources_cls = globals()["EnhancementMetricsSources"]

    sources_kwargs: Dict[str, Any] = {
        "distiller": distiller,
        "rl_policy": rl_policy,
        "federated": federated,
        "coordinator": coordinator,
        "temporal_monitor": temporal_monitor,
        "explainer": explainer,
        "precision_controller": precision_controller,
        "precision_counts": precision_counts or {},
        "market": market,
        "circuit_breakers": circuit_breakers or {},
        "chaos": chaos,
        "hitl": hitl,
    }
    if "collector" in _supported_kwargs(
        sources_cls.__init__, "collector",
    ):
        sources_kwargs["collector"] = collector
    elif collector is not None:
        logger.warning(
            "EnhancementMetricsSources does not accept `collector=`; "
            "the collector will not be used as an authoritative source. "
            "Upgrade the exporter to v5.1 for this feature."
        )

    sources = sources_cls(**sources_kwargs)

    exporter_kwargs: Dict[str, Any] = {
        "registry": registry,
        "job_name": job_name,
        "max_edges_export": max_edges_export,
        "helium_monitor": helium_monitor,
        "enhancement_sources": sources,
    }
    if "helium_baseline_price_usd" in _supported_kwargs(
        exporter_cls.__init__, "helium_baseline_price_usd",
    ):
        exporter_kwargs["helium_baseline_price_usd"] = (
            helium_baseline_price_usd
        )

    return exporter_cls(**exporter_kwargs)


# =============================================================================
# Diagnostics
# =============================================================================

class _StubRegistry:
    """Minimal registry for ``test_smoke()``."""
    def health(self) -> Dict[str, Any]:
        return {"execution_count": 0, "singletons": {}}

    def get(self, key: Any) -> Optional[Any]:
        return None


def test_smoke() -> Dict[str, Any]:
    """
    Structured health check: construct an exporter, collect, render.

    Never raises; returns a report. Calls ``collect(dry_run=True)`` when
    supported to avoid mutating source state.
    """
    results: Dict[str, Any] = {
        "ok": False,
        "package_version": __version__,
        "steps": [],
    }

    if "GraphMetricsExporter" not in globals():
        results["error"] = (
            f"GraphMetricsExporter not available: "
            f"{_SUBMODULE_IMPORT_ERRORS.get('graph_metrics_exporter')}"
        )
        return results

    try:
        exporter_cls = globals()["GraphMetricsExporter"]
        sources_cls = globals().get("EnhancementMetricsSources")
        stub = _StubRegistry()
        sources = sources_cls() if sources_cls is not None else None
        kwargs: Dict[str, Any] = {"registry": stub}
        if sources is not None:
            kwargs["enhancement_sources"] = sources
        exporter = exporter_cls(**kwargs)
        results["steps"].append({"name": "construct", "ok": True})
    except Exception as exc:  # noqa: BLE001
        results["error"] = (
            f"construct: {type(exc).__name__}: {exc}"
        )
        return results

    try:
        try:
            series = exporter.collect(dry_run=True)
        except TypeError:
            # v5.0 exporter without dry_run
            series = exporter.collect()
        ok = (
            isinstance(series, list)
            and len(series) > 0
            and all(
                isinstance(s, tuple)
                and len(s) == 3
                and isinstance(s[0], str)
                and isinstance(s[1], (int, float))
                and isinstance(s[2], dict)
                for s in series
            )
        )
        results["steps"].append({
            "name": "collect",
            "ok": ok,
            "series_count": (
                len(series) if isinstance(series, list) else None
            ),
        })
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "collect", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    try:
        text = exporter.render()
        ok = (
            "green_agent_exporter_up" in text
            and "green_agent_exporter_info" in text
        )
        results["steps"].append({
            "name": "render", "ok": ok,
        })
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "render", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    inner_smoke = getattr(exporter, "test_smoke", None)
    if callable(inner_smoke):
        try:
            inner = inner_smoke()
            results["steps"].append({
                "name": "exporter.test_smoke",
                "ok": bool(inner.get("ok", False)),
            })
        except Exception as exc:  # noqa: BLE001
            results["steps"].append({
                "name": "exporter.test_smoke", "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            })

    results["ok"] = (
        len(results["steps"]) > 0
        and all(s.get("ok") for s in results["steps"])
    )
    return results


def health() -> Dict[str, Any]:
    """
    Compact health report suitable for ``/health`` endpoints and CI gates.
    """
    smoke = test_smoke()
    return {
        "ok": smoke["ok"],
        "package_version": __version__,
        "exporter_available": "GraphMetricsExporter" in globals(),
        "collector_available": "MetricsCollector" in globals(),
        "steps": smoke.get("steps", []),
        "error": smoke.get("error"),
    }


def get_capabilities() -> Dict[str, Any]:
    """
    Structured capability report suitable for logs and support tickets.

    Distinguishes "submodule missing" from "submodule loaded but a symbol
    is missing" via ``submodules`` and ``import_errors``.
    """
    return {
        "package_version": __version__,
        "min_submodule_version": _MIN_SUBMODULE_VERSION,
        "submodules": {
            name: {
                "available": mod is not None,
                "path": _SUBMODULE_PATHS.get(name),
                "version": _SUBMODULE_VERSIONS.get(name),
                "error": _SUBMODULE_IMPORT_ERRORS.get(name),
            }
            for name, mod in _SUBMODULES.items()
        },
        "exported_symbols": {
            "graph_metrics_exporter": list(_EXPORTED_FROM_EXPORTER),
            "metrics_collector": list(_EXPORTED_FROM_COLLECTOR),
        },
        "missing_symbols": sorted(_MISSING_OPTIONAL),
        "features": dict(DEFAULT_FEATURES),
        "exporter_available": "GraphMetricsExporter" in globals(),
        "collector_available": "MetricsCollector" in globals(),
    }


# =============================================================================
# Public API
# =============================================================================

__all__: List[str] = [
    # --- Helpers ---
    "create_exporter",
    "get_capabilities",
    "health",
    "test_smoke",
    "list_features",
    "is_available",
    "enable_all",
    "disable_all",
    # --- Feature toggles ---
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "DISABLE_ALL_FEATURES",
    # --- Diagnostics ---
    "IMPORT_ERRORS",
    "SUBMODULE_VERSIONS",
    # --- Metadata ---
    "__version__",
    "__author__",
    "__license__",
]

# Conditionally add symbols that actually loaded. ``in globals()`` bypasses
# ``__getattr__``, so it reflects genuine bindings.
for _sym in (
    "GraphMetricsExporter",
    "EnhancementMetricsSources",
    "MetricsCollector",
    "MetricsSnapshot",
):
    if _sym in globals():
        __all__.append(_sym)


# =============================================================================
# Public diagnostics dicts (read-only views)
# =============================================================================
#: Submodule versions actually loaded.
SUBMODULE_VERSIONS: Dict[str, Optional[str]] = dict(_SUBMODULE_VERSIONS)

#: Import error messages per submodule.
IMPORT_ERRORS: Dict[str, Optional[str]] = dict(_SUBMODULE_IMPORT_ERRORS)


# =============================================================================
# Optional env-var gate for CI
# =============================================================================

if os.environ.get("GREEN_AGENT_VALIDATE_MONITORING") == "1":
    _report = test_smoke()
    if not _report["ok"]:
        raise RuntimeError(
            f"Monitoring package validation failed: {_report}"
        )
    logger.info(
        "Monitoring package validation passed "
        "(package v%s, %d step(s)).",
        __version__, len(_report["steps"]),
    )


# =============================================================================
# Load diagnostics
# =============================================================================

_missing = [
    name for name, mod in _SUBMODULES.items() if mod is None
]
if _missing:
    logger.debug(
        "src.monitoring v%s loaded with optional submodules missing: %s",
        __version__, _missing,
    )
else:
    logger.debug(
        "src.monitoring v%s ready: %d public symbols, %d submodule(s).",
        __version__, len(__all__), len(_SUBMODULES),
    )


# =============================================================================
# Local smoke test: python -m src.monitoring
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=== Capabilities ===")
    print(json.dumps(get_capabilities(), indent=2, default=str))

    print("\n=== test_smoke() ===")
    _smoke = test_smoke()
    print(json.dumps(_smoke, indent=2, default=str))

    print("\n=== health() ===")
    print(json.dumps(health(), indent=2, default=str))

    if not _smoke["ok"]:
        raise SystemExit(1)
    print("\nSmoke test passed.")
