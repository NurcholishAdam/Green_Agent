# -*- coding: utf-8 -*-
"""
LIMIT-AgentBench: Green Agent Benchmarking Platform
====================================================

A comprehensive benchmarking platform for AI agents with:

  - AgentBench protocol compatibility
  - Energy consumption and carbon footprint tracking
  - Multi-framework support (LangChain, AutoGen, CrewAI, etc.)
  - Unified green leaderboard
  - Integration with existing LIMIT-GRAPH infrastructure

Optional advanced modules (imported if the ``enhancements`` package is
installed):

  - LIMIT Graph integration
  - MODP (Multi-Objective Decision Process)
  - RLHF (Reinforcement Learning from Human Feedback)
  - Multi-Teacher On-Policy Distillation with MoE gating
  - Bio-inspired Optimisation (Evolutionary)
  - FlexGen execution backend integration hooks

---- Package policy -----------------------------------------------------------

**Core is required.** If any of ``AgentBenchAdapter``,
``GreenMetricsTracker``, ``AgentEvaluator``, ``BenchmarkHarness``,
``EnergyTracker``, ``CarbonCalculator``, ``EfficiencyScorer``,
``SustainabilityIndex``, ``GreenLeaderboard``, ``EnergyVisualizer``,
``CarbonDashboard``, or ``ComparisonMatrix`` fails to import, the package
raises ``ImportError`` — a broken core install is a broken install, and
silent degradation of the core API is worse than failing at import time.

**Enhancements are optional.** Each sub-module is imported independently.
If a sub-module is missing or fails to import, the corresponding name is
*not* defined on this module (so ``from limit_agentbench import
FeedbackEvent`` raises ``AttributeError`` with a clear "install
enhancements" message) and the per-subsystem flag in
``Enhancements.status`` is ``False``. The top-level boolean
``ENHANCEMENTS_AVAILABLE`` is ``True`` only when **all** documented
enhancement sub-modules imported successfully.

---- Package layout assumptions ----------------------------------------------

The core imports (``from .core...``) assume this file lives at the package
root and that ``core/``, ``metrics/``, ``dashboard/``, ``adapters/`` are
sibling directories. The enhancement imports are tried against two paths
(``.src.enhancements.*`` and ``.enhancements.*``) and the first one that
works is remembered in ``Enhancements.source_path``. The two-path approach
handles both:

  * ``pip install -e .`` where ``pyproject.toml`` declares
    ``package-dir = {"" = "src"}`` so ``enhancements`` is a top-level
    package under ``src/``; and
  * a source checkout where this file is at the repo root and
    ``src/enhancements/`` is a package-relative subdirectory.

---- Version and metadata ----------------------------------------------------

``__version__`` is derived from ``importlib.metadata`` when the package is
installed, and falls back to a hardcoded string when running from a source
checkout. ``__version_info__`` is the tuple form for programmatic
comparisons.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Version and metadata
# ---------------------------------------------------------------------------
# FIX #1  version 2.4.2 → 5.0.0 (aligned with agent.toml / docker-compose.yml
#         / run_agent.py).
# FIX #7  Derived from importlib.metadata when installed, falls back to a
#         hardcoded string for a source checkout. This single-sources the
#         version from pyproject.toml / setup.cfg.
_FALLBACK_VERSION = "5.0.0"
_FALLBACK_VERSION_INFO: Tuple[int, int, int] = (5, 0, 0)


def _resolve_version() -> str:
    try:
        from importlib.metadata import version as _pkg_version
        return _pkg_version("limit-agentbench")
    except Exception:                                    # noqa: BLE001
        return _FALLBACK_VERSION


def _parse_version(v: str) -> Tuple[int, int, int]:
    """Extract (major, minor, patch) from ``v``; ignore pre-release tags."""
    parts: List[int] = []
    for token in v.split("."):
        digits = "".join(ch for ch in token if ch.isdigit())
        if digits:
            parts.append(int(digits))
        else:
            break
        if len(parts) == 3:
            break
    while len(parts) < 3:
        parts.append(0)
    return (parts[0], parts[1], parts[2])


__version__: str = _resolve_version()
__version_info__: Tuple[int, int, int] = _parse_version(__version__)
__author__ = "AI Research Agent Team"
__author_email__ = "maintainers@limit-agentbench.example"
__license__ = "Apache-2.0"


# ---------------------------------------------------------------------------
# Core imports (FIX #17 — required; fail fast)
# ---------------------------------------------------------------------------
# If any of these fails, the package import fails. This is deliberate:
# a package whose core API is silently missing some classes is worse
# than a package that refuses to import.
from .core.agentbench_adapter import AgentBenchAdapter
from .core.green_metrics import GreenMetricsTracker
from .core.agent_evaluator import AgentEvaluator
from .core.benchmark_harness import BenchmarkHarness

from .metrics.energy_tracker import EnergyTracker
from .metrics.carbon_calculator import CarbonCalculator
from .metrics.efficiency_scorer import EfficiencyScorer
from .metrics.sustainability_index import SustainabilityIndex

from .dashboard.green_leaderboard import GreenLeaderboard
from .dashboard.energy_visualizer import EnergyVisualizer
from .dashboard.carbon_dashboard import CarbonDashboard
from .dashboard.comparison_matrix import ComparisonMatrix


# ---------------------------------------------------------------------------
# Enhancement availability status (FIX #12, #13)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Enhancements:
    """
    Per-subsystem enhancement availability.

    ``status.all_available`` is True only when *every* documented
    subsystem imported successfully — the same contract as the legacy
    ``ENHANCEMENTS_AVAILABLE`` boolean, but with per-module granularity
    so a caller can degrade gracefully.
    """
    feedback_event: bool = False
    node_descriptor: bool = False
    workload_descriptor: bool = False
    zero_trust: bool = False
    graph_registry: bool = False
    source_path: Optional[str] = None
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def all_available(self) -> bool:
        return (
            self.feedback_event
            and self.node_descriptor
            and self.workload_descriptor
            and self.zero_trust
            and self.graph_registry
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feedback_event": self.feedback_event,
            "node_descriptor": self.node_descriptor,
            "workload_descriptor": self.workload_descriptor,
            "zero_trust": self.zero_trust,
            "graph_registry": self.graph_registry,
            "source_path": self.source_path,
            "all_available": self.all_available,
            "errors": dict(self.errors),
        }


# ---------------------------------------------------------------------------
# Optional enhancement imports (FIX #3, #5, #6)
# ---------------------------------------------------------------------------
# FIX #3  Each sub-module is imported independently. A failure in
#         ``node_descriptor`` (e.g. a missing ``MaintenanceStatus``) no
#         longer wipes out ``feedback_event`` or ``zero_trust``.
# FIX #5  Both candidate paths are tried — ``.src.enhancements`` (source
#         checkout) and ``.enhancements`` (installed package) — and the
#         winner is recorded so downstream code can reason about the layout.
# FIX #6  Import failures are logged at WARNING so a broken install is
#         visible instead of silently degrading.

_ENHANCEMENT_IMPORT_ATTEMPTS: List[Tuple[str, str]] = [
    # (source_path_label, dotted_prefix)
    ("package-relative src/", ".src.enhancements"),
    ("package-relative top-level", ".enhancements"),
]


def _try_import_enhancement_module(
    prefix: str, submodule: str,
) -> Tuple[bool, Optional[Any], str]:
    """
    Import ``{prefix}.{submodule}``.

    Returns ``(ok, module_or_None, error_message)``. Never raises.
    """
    import importlib
    full_path = f"{__name__}{prefix}.{submodule}"
    try:
        return True, importlib.import_module(full_path), ""
    except ImportError as exc:
        return False, None, str(exc)
    except Exception as exc:                             # noqa: BLE001
        # A non-ImportError inside an enhancement module is a bug, not an
        # install issue — surface it distinctly.
        logger.warning(
            "Unexpected error importing %s: %s: %s",
            full_path, type(exc).__name__, exc,
        )
        return False, None, f"{type(exc).__name__}: {exc}"


def _load_enhancements() -> Tuple[Enhancements, Dict[str, Any]]:
    """
    Try each candidate path and collect the results.

    Returns ``(status, symbols)`` where ``symbols`` maps names to the
    imported classes (only for names that imported successfully).
    """
    errors: Dict[str, str] = {}

    # Pick the first prefix where *at least* feedback_event resolves —
    # otherwise the whole enhancements tree is missing.
    chosen_prefix: Optional[str] = None
    chosen_label: Optional[str] = None
    for label, prefix in _ENHANCEMENT_IMPORT_ATTEMPTS:
        ok, mod, msg = _try_import_enhancement_module(
            prefix, "schemas.feedback_event",
        )
        if ok and mod is not None:
            chosen_prefix = prefix
            chosen_label = label
            break
        errors[f"prefix {prefix}"] = msg

    if chosen_prefix is None:
        logger.warning(
            "Enhancements package not importable; tried: %s",
            "; ".join(f"{k}={v}" for k, v in errors.items()),
        )
        return Enhancements(errors=errors), {}

    symbols: Dict[str, Any] = {}

    def _import_from(submodule: str) -> Optional[Any]:
        ok, mod, msg = _try_import_enhancement_module(
            chosen_prefix, submodule,
        )
        if not ok:
            errors[submodule] = msg
        return mod if ok else None

    # --- feedback_event ---
    fb_mod = _import_from("schemas.feedback_event")
    fb_ok = fb_mod is not None and hasattr(fb_mod, "FeedbackEvent")
    if fb_mod is not None and not fb_ok:
        errors["schemas.feedback_event"] = "module has no FeedbackEvent"
    if fb_ok:
        symbols["FeedbackEvent"] = fb_mod.FeedbackEvent

    # --- node_descriptor ---
    nd_mod = _import_from("schemas.node_descriptor")
    nd_ok = False
    if nd_mod is not None:
        nd_fields = (
            "NodeDescriptor", "NodeType", "CoolingType",
            "MaintenanceStatus", "RoutingStrategy",
        )
        missing = [f for f in nd_fields if not hasattr(nd_mod, f)]
        if missing:
            errors["schemas.node_descriptor"] = (
                f"missing fields: {missing}"
            )
        else:
            nd_ok = True
            for f in nd_fields:
                symbols[f] = getattr(nd_mod, f)

    # --- workload_descriptor ---
    wl_mod = _import_from("schemas.workload_descriptor")
    wl_ok = False
    if wl_mod is not None:
        wl_fields = (
            "WorkloadDescriptor", "TaskType", "Urgency",
            "Priority", "BioMode",
        )
        missing = [f for f in wl_fields if not hasattr(wl_mod, f)]
        if missing:
            errors["schemas.workload_descriptor"] = (
                f"missing fields: {missing}"
            )
        else:
            wl_ok = True
            for f in wl_fields:
                symbols[f] = getattr(wl_mod, f)

    # --- zero_trust_architecture ---
    zt_mod = _import_from("zero_trust_architecture")
    zt_ok = False
    if zt_mod is not None:
        zt_fields = ("ZeroTrustArchitecture", "ZeroTrustConfig")
        missing = [f for f in zt_fields if not hasattr(zt_mod, f)]
        if missing:
            errors["zero_trust_architecture"] = (
                f"missing fields: {missing}"
            )
        else:
            zt_ok = True
            for f in zt_fields:
                symbols[f] = getattr(zt_mod, f)

    # --- core.graph_registry (optional within enhancements) ---
    gr_mod = _import_from("core.graph_registry")
    gr_ok = False
    if gr_mod is not None:
        gr_fields = ("GraphRegistry", "GraphType")
        missing = [f for f in gr_fields if not hasattr(gr_mod, f)]
        if missing:
            errors["core.graph_registry"] = f"missing fields: {missing}"
        else:
            gr_ok = True
            for f in gr_fields:
                symbols[f] = getattr(gr_mod, f)

    if not all((fb_ok, nd_ok, wl_ok, zt_ok, gr_ok)):
        logger.warning(
            "Enhancements partially available at %s; errors: %s",
            chosen_label,
            "; ".join(f"{k}: {v}" for k, v in errors.items()),
        )

    status = Enhancements(
        feedback_event=fb_ok,
        node_descriptor=nd_ok,
        workload_descriptor=wl_ok,
        zero_trust=zt_ok,
        graph_registry=gr_ok,
        source_path=chosen_label,
        errors=errors,
    )
    return status, symbols


_ENHANCEMENTS, _SYMBOLS = _load_enhancements()

# Expose the status object (FIX #12)
EnhancementsStatus: Enhancements = _ENHANCEMENTS

# FIX #2  ENHANCEMENTS_AVAILABLE is True only when *all* documented
#         subsystems imported — not just the first one.
ENHANCEMENTS_AVAILABLE: bool = _ENHANCEMENTS.all_available

# Re-export the successfully imported symbols as module attributes so that
# `from limit_agentbench import FeedbackEvent` works when available. When a
# symbol is unavailable, it is **not** defined here, so a later import
# raises `AttributeError` and `__getattr__` can produce a helpful message
# (FIX #4, #14, #16).
for _name, _value in _SYMBOLS.items():
    globals()[_name] = _value


# ---------------------------------------------------------------------------
# PEP 562 __getattr__ — informative errors for missing optional names
# ---------------------------------------------------------------------------
_OPTIONAL_NAMES: Dict[str, str] = {
    "FeedbackEvent": "enhancements.schemas.feedback_event",
    "NodeDescriptor": "enhancements.schemas.node_descriptor",
    "NodeType": "enhancements.schemas.node_descriptor",
    "CoolingType": "enhancements.schemas.node_descriptor",
    "MaintenanceStatus": "enhancements.schemas.node_descriptor",
    "RoutingStrategy": "enhancements.schemas.node_descriptor",
    "WorkloadDescriptor": "enhancements.schemas.workload_descriptor",
    "TaskType": "enhancements.schemas.workload_descriptor",
    "Urgency": "enhancements.schemas.workload_descriptor",
    "Priority": "enhancements.schemas.workload_descriptor",
    "BioMode": "enhancements.schemas.workload_descriptor",
    "ZeroTrustArchitecture": "enhancements.zero_trust_architecture",
    "ZeroTrustConfig": "enhancements.zero_trust_architecture",
    "GraphRegistry": "enhancements.core.graph_registry",
    "GraphType": "enhancements.core.graph_registry",
}


def __getattr__(name: str) -> Any:
    """
    Raise a clear ``ImportError`` for optional enhancement names that are
    not installed, instead of the default ``AttributeError`` from module
    attribute lookup.
    """
    if name in _OPTIONAL_NAMES:
        raise ImportError(
            f"limit_agentbench.{name} requires the enhancements package "
            f"({_OPTIONAL_NAMES[name]}). Install with:\n"
            f"    pip install -e '.[enhancements]'\n"
            f"EnhancementsStatus: {_ENHANCEMENTS.to_dict()}"
        )
    raise AttributeError(
        f"module 'limit_agentbench' has no attribute {name!r}"
    )


# ---------------------------------------------------------------------------
# Version consistency check (FIX #20)
# ---------------------------------------------------------------------------
def _check_version_consistency() -> None:
    """
    If ``agent.toml`` sits next to this package, warn when its declared
    version disagrees with ``__version__``.

    The check is best-effort — it never raises, only logs — because the
    manifest is often not present in a wheel install.
    """
    from pathlib import Path
    candidates = [
        Path(__file__).resolve().parent / "agent.toml",
        Path(__file__).resolve().parent.parent / "agent.toml",
    ]
    for path in candidates:
        if not path.is_file():
            continue
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            continue
        # Minimal TOML parsing for the `version = "..."` line under [agent]
        in_agent_section = False
        for line in raw.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                in_agent_section = stripped == "[agent]"
                continue
            if not in_agent_section or not stripped.startswith("version"):
                continue
            _, _, value = stripped.partition("=")
            manifest_version = value.strip().strip('"').strip("'")
            if manifest_version and manifest_version != __version__:
                logger.warning(
                    "Version mismatch: limit_agentbench.__version__=%r "
                    "but %s declares version=%r",
                    __version__, path, manifest_version,
                )
            return


_check_version_consistency()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
__all__: List[str] = [
    # Package metadata (FIX #9)
    "__version__",
    "__version_info__",
    "__author__",
    "__author_email__",
    "__license__",
    "ENHANCEMENTS_AVAILABLE",
    "EnhancementsStatus",

    # Core
    "AgentBenchAdapter",
    "GreenMetricsTracker",
    "AgentEvaluator",
    "BenchmarkHarness",

    # Metrics
    "EnergyTracker",
    "CarbonCalculator",
    "EfficiencyScorer",
    "SustainabilityIndex",

    # Dashboard
    "GreenLeaderboard",
    "EnergyVisualizer",
    "CarbonDashboard",
    "ComparisonMatrix",
]

# FIX #11  Only extend __all__ with names that actually resolved. If
#          ENHANCEMENTS_AVAILABLE is True but a specific subsystem is
#          missing, that subsystem's names are still omitted.
__all__.extend(sorted(_SYMBOLS.keys()))
