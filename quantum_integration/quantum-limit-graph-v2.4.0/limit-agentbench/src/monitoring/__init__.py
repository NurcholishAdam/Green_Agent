# src/monitoring/__init__.py

"""
Real-time monitoring and metrics collection modules (Enhanced)
=============================================================

This package exposes the Green Agent's observability surface, including
Prometheus-format graph metrics, helium supply metrics, and — as of the
enhanced release — the ten cross-cutting enhancement-layer metrics:

  1. Quantum-Distillation      →  green_agent_distilled_models_total, ...
  2. Causal RL                 →  green_agent_rl_epsilon, ...
  3. Federated Green Learning  →  green_agent_federated_updates_total, ...
  4. Multi-Agent Coordination  →  green_agent_agents_by_role, ...
  5. Temporal Logic            →  green_agent_temporal_violations_total, ...
  6. Explainable AI (XAI)      →  green_agent_explanations_total, ...
  7. Adaptive Precision        →  green_agent_precision_share, ...
  8. Carbon Markets            →  green_agent_carbon_credits_purchased_total, ...
  9. Resilience & Chaos        →  green_agent_circuit_state, ...
 10. Human-in-the-Loop         →  green_agent_hitl_pending, ...

Public API
----------
Exporter
    GraphMetricsExporter
    EnhancementMetricsSources

Collector (if `metrics_collector` is present)
    MetricsCollector
    MetricsSnapshot

Helpers
    create_exporter()       — one-line construction of an enhanced exporter
    get_capabilities()      — runtime introspection of available submodules
    DEFAULT_FEATURES        — default enhancement feature toggles

Metadata
    __version__, __author__, __license__
"""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__version__ = "5.0.0"
__author__ = "Green Agent Team"
__license__ = "MIT"


# =============================================================================
# Submodule discovery
# =============================================================================

# Known submodules of this package. Add new entries here as the package grows.
_KNOWN_SUBMODULES = (
    "graph_metrics_exporter",
    "metrics_collector",
)

# Populated at import time: {submodule_name: module_object_or_None}
_SUBMODULES: Dict[str, Any] = {}


def _load_submodule(name: str) -> Optional[Any]:
    """Attempt to import a submodule; return None on failure."""
    try:
        mod = import_module(f".{name}", package=__name__)
        return mod
    except ImportError as e:
        logger.debug(f"Optional submodule '{name}' not available: {e}")
        return None
    except Exception as e:
        logger.warning(f"Submodule '{name}' raised on import: {e}")
        return None


for _name in _KNOWN_SUBMODULES:
    _SUBMODULES[_name] = _load_submodule(_name)


# =============================================================================
# Helper: expose symbols from a submodule
# =============================================================================

def _expose(
    submodule_name: str,
    *symbols: str,
    required: bool = False,
) -> List[str]:
    """
    Lift `symbols` from a submodule into this package's globals.

    Returns the list of symbols that were actually resolved. Missing symbols
    are set to None so that `from src.monitoring import X` never raises.
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
                    f"src.monitoring.{submodule_name}"
                )
    return exposed


# =============================================================================
# 1. Expose GraphMetricsExporter public API
# =============================================================================

_EXPORTED_FROM_EXPORTER = _expose(
    "graph_metrics_exporter",
    "GraphMetricsExporter",
    "EnhancementMetricsSources",
    required=False,
)


# =============================================================================
# 2. Expose MetricsCollector public API (optional)
# =============================================================================

_EXPORTED_FROM_COLLECTOR = _expose(
    "metrics_collector",
    "MetricsCollector",
    "MetricsSnapshot",
    required=False,
)


# =============================================================================
# 3. Feature toggle constants (re-exported for convenience)
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
}

ENABLE_ALL_FEATURES: Dict[str, bool] = {k: True for k in DEFAULT_FEATURES}
DISABLE_ALL_FEATURES: Dict[str, bool] = {k: False for k in DEFAULT_FEATURES}


# =============================================================================
# 4. Convenience factory
# =============================================================================

def create_exporter(
    registry: Any,
    *,
    job_name: str = "green_agent",
    max_edges_export: int = 100,
    helium_monitor: Any = None,
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
    Build a fully-configured `GraphMetricsExporter` with all enhancement
    sources wired in.

    Args:
        registry: GraphRegistry instance.
        job_name: Prometheus job label.
        max_edges_export: Cardinality cap per graph type.
        helium_monitor: Optional HeliumMonitor.
        distiller / rl_policy / federated / coordinator / temporal_monitor /
        explainer / precision_controller / market / chaos / hitl:
            Optional enhancement source objects. Missing sources are simply
            omitted from the emitted metrics (the exporter is duck-typed).
        precision_counts: Optional dict mapping precision level → task count.
        circuit_breakers: Optional dict mapping subsystem name → CircuitBreaker.

    Returns:
        A `GraphMetricsExporter` instance.
    """
    if GraphMetricsExporter is None:
        raise ImportError(
            "GraphMetricsExporter is not available. Ensure "
            "src/monitoring/graph_metrics_exporter.py is importable."
        )

    sources = EnhancementMetricsSources(
        distiller=distiller,
        rl_policy=rl_policy,
        federated=federated,
        coordinator=coordinator,
        temporal_monitor=temporal_monitor,
        explainer=explainer,
        precision_controller=precision_controller,
        precision_counts=precision_counts or {},
        market=market,
        circuit_breakers=circuit_breakers or {},
        chaos=chaos,
        hitl=hitl,
    )

    return GraphMetricsExporter(
        registry=registry,
        job_name=job_name,
        max_edges_export=max_edges_export,
        helium_monitor=helium_monitor,
        enhancement_sources=sources,
    )


# =============================================================================
# 5. Capability introspection
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """
    Return a summary of what this monitoring package can do.

    Useful for runtime feature discovery and self-documentation.
    """
    return {
        "version": __version__,
        "submodules": {
            name: (mod is not None)
            for name, mod in _SUBMODULES.items()
        },
        "exported_symbols": {
            "from_graph_metrics_exporter": list(_EXPORTED_FROM_EXPORTER),
            "from_metrics_collector": list(_EXPORTED_FROM_COLLECTOR),
        },
        "features_available": list(DEFAULT_FEATURES),
        "exporter_available": GraphMetricsExporter is not None,
        "collector_available": globals().get("MetricsCollector") is not None,
    }


# =============================================================================
# 6. Public API surface (`__all__`)
# =============================================================================

__all__: List[str] = []

# --- Exporter ---
if GraphMetricsExporter is not None:
    __all__.append("GraphMetricsExporter")
if EnhancementMetricsSources is not None:
    __all__.append("EnhancementMetricsSources")

# --- Collector ---
if globals().get("MetricsCollector") is not None:
    __all__.append("MetricsCollector")
if globals().get("MetricsSnapshot") is not None:
    __all__.append("MetricsSnapshot")

# --- Helpers ---
__all__ += [
    "create_exporter",
    "get_capabilities",
    "DEFAULT_FEATURES",
    "ENABLE_ALL_FEATURES",
    "DISABLE_ALL_FEATURES",
    "__version__",
]


# =============================================================================
# 7. Load diagnostics
# =============================================================================

_missing = [name for name, mod in _SUBMODULES.items() if mod is None]
if _missing:
    logger.debug(
        f"src.monitoring loaded with optional submodules missing: {_missing}"
    )
else:
    logger.debug(
        f"src.monitoring ready: {len(__all__)} public symbols exposed "
        f"from {len(_SUBMODULES)} submodules"
    )
