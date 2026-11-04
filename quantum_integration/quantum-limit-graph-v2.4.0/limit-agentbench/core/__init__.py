# -*- coding: utf-8 -*-
"""
Core components for LIMIT-AgentBench.

Entry points:
    BenchmarkHarness      — orchestrates a benchmark run
    AgentEvaluator        — evaluates an agent's reasoning trace

Adapters:
    AgentBenchAdapter     — bridges external agent SDKs to the harness

Utilities:
    GreenMetricsTracker   — tracks energy / carbon / latency per task
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Dict, TYPE_CHECKING

logger = logging.getLogger(__name__)

__version__ = "0.2.0"
__author__ = "LIMIT-AgentBench contributors"
__license__ = "Apache-2.0"

# Single source of truth: name → (submodule, attribute).
_LAZY_IMPORTS: Dict[str, tuple[str, str]] = {
    "AgentBenchAdapter":   (".agentbench_adapter", "AgentBenchAdapter"),
    "GreenMetricsTracker": (".green_metrics",      "GreenMetricsTracker"),
    "AgentEvaluator":      (".agent_evaluator",     "AgentEvaluator"),
    "BenchmarkHarness":    (".benchmark_harness",   "BenchmarkHarness"),
}

__all__ = list(_LAZY_IMPORTS.keys())


# ---- Lazy imports (PEP 562) -------------------------------------------

def __getattr__(name: str) -> Any:
    if name in _LAZY_IMPORTS:
        module_path, attr_name = _LAZY_IMPORTS[name]
        try:
            module = importlib.import_module(module_path, __package__)
        except ImportError as exc:
            raise ImportError(
                f"core.{name} is unavailable: failed to import "
                f"{__package__}{module_path}. "
                f"Cause: {exc}. "
                f"Try: pip install 'core[<extras>]' or inspect the traceback."
            ) from exc
        return getattr(module, attr_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(_LAZY_IMPORTS.keys()))


# ---- Diagnostics ------------------------------------------------------

def get_available_components() -> Dict[str, bool]:
    """
    Return a dict mapping each public name to whether it can be imported.
    Useful for pre-flight checks and for logging what's installed.
    """
    availability: Dict[str, bool] = {}
    for name in _LAZY_IMPORTS:
        try:
            getattr(__import__(__name__), name)
            availability[name] = True
        except Exception as exc:  # noqa: BLE001 — diagnostic helper
            logger.warning("Component %s unavailable: %s", name, exc)
            availability[name] = False
    return availability


# ---- Static-typing support -------------------------------------------

if TYPE_CHECKING:  # pragma: no cover
    from .agentbench_adapter import AgentBenchAdapter as AgentBenchAdapter
    from .green_metrics import GreenMetricsTracker as GreenMetricsTracker
    from .agent_evaluator import AgentEvaluator as AgentEvaluator
    from .benchmark_harness import BenchmarkHarness as BenchmarkHarness
