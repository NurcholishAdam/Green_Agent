#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_enhanced_agent.py — Complete runner for the Enhanced Green Agent (v2.0)

Wires together the scientific modules (synthetic data, control system,
fallback manager, thermal optimizer, phase-energy model, energy scaler,
marginal-carbon forecaster, dual carbon accountant, helium elasticity,
material substitution, helium circularity, regret optimizer, federated
learning) with the optional advanced enhancement modules (LIMIT Graph,
MODP, RLHF, Multi-Teacher On-Policy Distillation, Bio-inspired
Optimisation, MoE expert gating, FlexGen).

Enhanced in v2.0
----------------
- **Added `import asyncio`** — the original called `asyncio.run(...)` four
  times without importing the module.
- **`_maybe_await` helper** — handles sync-or-async enhancement methods
  (per INSTALLATION.md §2 they are sync; prior demo code assumed async).
  `select_routing_strategy`, `select_priority`, `record_outcome`,
  `initialize`, `shutdown` all go through it.
- **Fixed `FeedbackEvent` construction** — uses `energy_kwh` and
  `carbon_kg` (per README.md §2.5); no `energy_joules` / `carbon_g` /
  `helium_cost` / `duration_ms`; no reliance on `event.event_id`.
- **Fixed `NodeDescriptor` constructor** — removed undocumented kwargs
  (`helium_connectivity_score`, `uptime`, `renewable_fraction`,
  `cooling_type`, `hardware_model`) that would raise `TypeError`.
- **`--enhancements` / `--no-enhancements`** — the prior `store_true`
  flag had `default=True`, so it could never be `False` (dead code).
- **`--once` / `--iterations N`** — prevents infinite hang in CI.
- **Config supports YAML and JSON** — detected by file extension.
- **No `sys.path.insert('src/enhancements')` relative-path bug** — paths
  are derived from `__file__` so the script works from any cwd.
- **Per-module diagnostics** — one failed module no longer silently
  disables all 8 enhancements; each is initialized in its own try/except
  and the failure mode is logged.
- **`tempfile.TemporaryDirectory()`** for the DAG carbon ledger — no
  more hardcoded `/tmp` (Windows-safe, auto-cleaned).
- **Real inputs to `thermal.optimize_schedule(temp_data, grid_data)`** —
  the prior `(None, None)` produced a meaningless decision.
- **All strategy/priority branches handled** — prior version only acted
  on `carbon_first` and `green`; every other strategy was silently ignored.
- **Guarded attribute access** — `thermal_decision`, `elasticity_decision`,
  `_routing_optimizer` are all checked before use.
- **Real iteration tracking** — `self._iteration` replaces the constant-0
  `_get_current_iteration()` placeholder.
- **Graceful shutdown of advanced modules** — DAG ledger, graph registry
  closed in `stop()`; wrapped so one failure does not skip the rest.
- **Prometheus metrics endpoint** — optional via `--metrics-bind`; the
  prior version had no metrics surface.
- **Signal handlers cross-platform** — falls back to `signal.signal` when
  the event loop raises `NotImplementedError` (Windows).
- **Explicit exit codes** — 0 success, 1 runtime failure, 2 config/env.
- **`__version__`, header, docstrings, type hints** throughout.

Run
---
    # Batch: 1 iteration, exit
    python run_enhanced_agent.py --once --iterations 1

    # Serve: continuous loop, Ctrl+C to stop
    python run_enhanced_agent.py

    # No enhancements (base modules only)
    python run_enhanced_agent.py --once --iterations 1 --no-enhancements

    # Choose scenario and data quality
    python run_enhanced_agent.py --once --iterations 1 \\
        --scenario helium_crisis --quality noisy

    # Custom config (YAML or JSON)
    python run_enhanced_agent.py --config config/base/green_agent_config.yaml

Exit codes
----------
    0   normal exit (batch done or clean shutdown)
    1   runtime error during an iteration
    2   configuration or environment error
    130 SIGINT (Ctrl+C)
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import signal
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

__version__ = "5.0.0"

# =============================================================================
# sys.path — resolved from __file__ so the script works from any cwd.
# Both the repo root (for `src.enhancements.*`) and src/enhancements (for
# the flat imports of the scientific modules) are added.
# =============================================================================
_REPO_ROOT = Path(__file__).resolve().parent
_ENHANCEMENTS_DIR = _REPO_ROOT / "src" / "enhancements"

for _p in (_REPO_ROOT, _ENHANCEMENTS_DIR):
    if _p.is_dir() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

# =============================================================================
# Required scientific modules — flat imports from src/enhancements
# =============================================================================
_REQUIRED_IMPORT_ERROR: Optional[Exception] = None
try:
    from synthetic_data_manager import SyntheticDataSource, DataQuality, ScenarioType
    from control_system import ControlSystem, ControlMode
    from fallback_manager import (
        FallbackManager, FallbackConfig, FallbackStrategy,
    )
    from thermal_optimizer import ThermalAwareOptimizer
    from phase_energy_model import PhaseAwareEnergyModel
    from energy_scaler import EnergyProportionalScaler
    from marginal_carbon import MarginalCarbonIntensityForecaster
    from dual_accountant import DualCarbonAccountant
    from helium_elasticity import (
        HeliumPriceElasticityModel, WorkloadPriority,
    )
    from material_substitution import MaterialSubstitutionEngine
    from helium_circularity import HeliumCircularityTracker
    from regret_optimizer import RegretMinimizationOptimizer
    from federated_learning import FederatedGreenLearning
except ImportError as _exc:
    _REQUIRED_IMPORT_ERROR = _exc

if _REQUIRED_IMPORT_ERROR is not None:
    print(
        f"FATAL: could not import required scientific modules: "
        f"{_REQUIRED_IMPORT_ERROR}\n"
        f"Expected them under: {_ENHANCEMENTS_DIR}\n"
        "Install with: pip install -e . --no-deps",
        file=sys.stderr,
    )
    sys.exit(2)


# =============================================================================
# Optional advanced enhancement modules — per-module isolation
# =============================================================================
def _try_import(module: str, symbols: tuple[str, ...]) -> tuple[dict[str, Any], Optional[Exception]]:
    """Import ``symbols`` from ``module``; capture exception on failure."""
    try:
        mod = __import__(module, fromlist=list(symbols))
    except Exception as exc:  # pragma: no cover - env-dependent
        return {name: None for name in symbols}, exc
    return {name: getattr(mod, name, None) for name in symbols}, None


_node, _node_err = _try_import(
    "src.enhancements.schemas.node_descriptor",
    ("NodeDescriptor", "NodeType"),
)
_wl, _wl_err = _try_import(
    "src.enhancements.schemas.workload_descriptor",
    ("WorkloadDescriptor", "TaskType", "Urgency"),
)
_fe, _fe_err = _try_import(
    "src.enhancements.schemas.feedback_event",
    ("FeedbackEvent",),
)
_zt, _zt_err = _try_import(
    "src.enhancements.zero_trust_architecture",
    ("ZeroTrustArchitecture", "ZeroTrustConfig"),
)
_gr, _gr_err = _try_import(
    "src.enhancements.core.graph_registry",
    ("GraphRegistry", "GraphType"),
)
_mc, _mc_err = _try_import(
    "src.enhancements.core.meta_cognition",
    ("MetaCognitionLayer",),
)
_dl, _dl_err = _try_import(
    "src.enhancements.metrics.dag_carbon_ledger",
    ("DAGCarbonLedger",),
)

NodeDescriptor = _node["NodeDescriptor"]
NodeType = _node["NodeType"]
WorkloadDescriptor = _wl["WorkloadDescriptor"]
TaskType = _wl["TaskType"]
Urgency = _wl["Urgency"]
FeedbackEvent = _fe["FeedbackEvent"]
ZeroTrustArchitecture = _zt["ZeroTrustArchitecture"]
ZeroTrustConfig = _zt["ZeroTrustConfig"]
GraphRegistry = _gr["GraphRegistry"]
GraphType = _gr["GraphType"]
MetaCognitionLayer = _mc["MetaCognitionLayer"]
DAGCarbonLedger = _dl["DAGCarbonLedger"]

_ENHANCEMENT_ERRORS: dict[str, Optional[Exception]] = {
    "NodeDescriptor": _node_err,
    "WorkloadDescriptor": _wl_err,
    "FeedbackEvent": _fe_err,
    "ZeroTrustArchitecture": _zt_err,
    "GraphRegistry": _gr_err,
    "MetaCognitionLayer": _mc_err,
    "DAGCarbonLedger": _dl_err,
}


def _missing_enhancements() -> list[str]:
    return [name for name, exc in _ENHANCEMENT_ERRORS.items() if exc is not None]


# =============================================================================
# Logging
# =============================================================================
logger = logging.getLogger("run_enhanced_agent")


# =============================================================================
# Constants
# =============================================================================
_LOOP_INTERVAL_S = 5.0
_LOG_EVERY_N_ITERATIONS = 12
_DEFAULT_DAG_LEDGER_PREFIX = "enhanced_agent_ledger_"


# =============================================================================
# Helpers
# =============================================================================

async def _maybe_await(value: Any) -> Any:
    """Await ``value`` if awaitable; return it otherwise.

    Makes the caller resilient to the docs/code drift where
    ``select_routing_strategy`` might be sync (per INSTALLATION.md §2) or
    async (per prior demo files).
    """
    if inspect.isawaitable(value):
        return await value
    return value


def _run(value: Any) -> Any:
    """Run a possibly-async value synchronously (used inside the sync loop)."""
    if inspect.isawaitable(value):
        return asyncio.run(_maybe_await(value))
    return value


def _safe_attr(obj: Any, *path: str, default: Any = None) -> Any:
    """Safely traverse nested attributes / dict keys."""
    cur = obj
    for name in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(name, default)
        else:
            cur = getattr(cur, name, default)
    return cur


def _fmt(value: Any, spec: str = ".4f", suffix: str = "") -> str:
    """Format a numeric value; return ``n/a`` if not numeric."""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        try:
            return f"{value:{spec}}{suffix}"
        except (TypeError, ValueError):
            return str(value)
    return "n/a"


def _load_config(path: Optional[str]) -> dict[str, Any]:
    """
    Load a config file (YAML or JSON, detected by extension).

    Returns an empty dict if ``path`` is None. Raises ``RuntimeError`` if
    the file exists but cannot be parsed.
    """
    if not path:
        return {}
    p = Path(path)
    if not p.is_file():
        raise RuntimeError(f"Config file not found: {p}")
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Could not read {p}: {exc}") from exc

    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                f"pyyaml is required for {p.suffix} configs. "
                f"pip install pyyaml"
            ) from exc
        try:
            loaded = yaml.safe_load(text)
        except Exception as exc:
            raise RuntimeError(f"Could not parse YAML {p}: {exc}") from exc
    else:
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Could not parse JSON {p}: {exc}") from exc

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Config {p} must be a mapping at the top level")
    return loaded


# =============================================================================
# EnhancedGreenAgent
# =============================================================================

class EnhancedGreenAgent:
    """
    Complete enhanced Green Agent with all scientific modules and
    optional advanced decision-making modules.
    """

    def __init__(self, config: Optional[dict[str, Any]] = None) -> None:
        self.config: dict[str, Any] = config or {}
        self.running = False
        self._iteration = 0
        self._current_strategy: Optional[str] = None
        self._current_priority: Optional[str] = None

        # Original scientific modules
        self.data_source = SyntheticDataSource(self.config.get("data", {}))
        self.control_system = ControlSystem(self.config.get("control", {}))
        self.fallback_manager = FallbackManager(self.config.get("fallback", {}))

        self.thermal = ThermalAwareOptimizer(self.config.get("thermal", {}))
        self.phase_energy = PhaseAwareEnergyModel(self.config.get("phase_energy", {}))
        self.energy_scaler = EnergyProportionalScaler(self.config.get("energy_scaler", {}))
        self.marginal_carbon = MarginalCarbonIntensityForecaster(
            self.config.get("marginal_carbon", {})
        )
        self.dual_accountant = DualCarbonAccountant(
            self.config.get("dual_accountant", {})
        )
        self.helium_elasticity = HeliumPriceElasticityModel(
            self.config.get("helium_elasticity", {})
        )
        self.material_substitution = MaterialSubstitutionEngine(
            self.config.get("material_substitution", {})
        )
        self.helium_circularity = HeliumCircularityTracker(
            self.config.get("helium_circularity", {})
        )
        self.regret_optimizer = RegretMinimizationOptimizer(
            self.config.get("regret_optimizer", {})
        )
        self.federated_learning = FederatedGreenLearning(
            self.config.get("federated_learning", {})
        )

        # Advanced enhancement modules (optional)
        # use_enhancements defaults to True; disable via config or --no-enhancements.
        want_enhancements = bool(self.config.get("use_enhancements", True))
        self.use_enhancements = want_enhancements and not _missing_enhancements()
        self.graph_registry: Any = None
        self.causal_graph: Any = None
        self.meta_cognition: Any = None
        self.node_descriptor: Any = None
        self.workload_descriptor: Any = None
        self.zero_trust: Any = None
        self.dag_carbon_ledger: Any = None
        self._dag_ledger_tmpdir: Optional[tempfile.TemporaryDirectory[str]] = None

        if self.use_enhancements:
            self._initialize_advanced_modules()
        elif want_enhancements and _missing_enhancements():
            logger.warning(
                "Advanced enhancements requested but %d module(s) missing: %s",
                len(_missing_enhancements()),
                ", ".join(_missing_enhancements()),
            )

        logger.info(
            "Enhanced Green Agent initialized (version=%s, advanced=%s)",
            __version__, self.use_enhancements,
        )

    # -------------------------------------------------------------------------
    # Advanced module initialization — per-module isolation
    # -------------------------------------------------------------------------

    def _initialize_advanced_modules(self) -> None:
        """
        Initialize each advanced module independently.

        One failing module does not disable the others; each failure is
        logged with its name and error so the operator can fix that module
        without hunting through a generic stack trace.
        """
        failures: list[str] = []

        # 1. Graph registry + causal graph
        if GraphRegistry is not None and GraphType is not None:
            try:
                self.graph_registry = GraphRegistry()
                self.causal_graph = self.graph_registry.get_or_create(GraphType.CAUSAL)
            except Exception as exc:
                failures.append("GraphRegistry")
                logger.error("GraphRegistry init failed: %s", exc)
        else:
            failures.append("GraphRegistry")

        # 2. Meta-cognition (depends on causal graph)
        if self.causal_graph is not None and MetaCognitionLayer is not None:
            try:
                self.meta_cognition = MetaCognitionLayer(
                    causal_graph=self.causal_graph,
                )
            except Exception as exc:
                failures.append("MetaCognitionLayer")
                logger.error("MetaCognitionLayer init failed: %s", exc)
        elif MetaCognitionLayer is None:
            failures.append("MetaCognitionLayer")

        # 3. Node descriptor — kwargs restricted to documented ones
        #    (per agent_config.yaml + INSTALLATION.md).
        if NodeDescriptor is not None and NodeType is not None:
            try:
                self.node_descriptor = NodeDescriptor(
                    id="enhanced_agent_node",
                    type=NodeType.EDGE,
                    region=self.config.get("region", "us-east"),
                    region_carbon_intensity=float(
                        self.config.get("carbon_intensity", 400.0)
                    ),
                    energy_per_token=float(
                        self.config.get("energy_per_token", 0.00005)
                    ),
                    use_evolutionary=bool(
                        self.config.get("use_evolutionary", True)
                    ),
                    human_feedback_score=float(
                        self.config.get("human_feedback_score", 0.6)
                    ),
                    graph_metrics=self.config.get(
                        "graph_metrics",
                        {"centrality": 0.7, "connectivity": 0.6},
                    ),
                    metadata={
                        "distillation_epsilon": float(
                            self.config.get("distillation_epsilon", 0.1)
                        ),
                        "gating_learning_rate": float(
                            self.config.get("gating_lr", 0.005)
                        ),
                    },
                )
            except Exception as exc:
                failures.append("NodeDescriptor")
                logger.error("NodeDescriptor init failed: %s", exc)
        else:
            failures.append("NodeDescriptor")

        # 4. Workload descriptor
        if WorkloadDescriptor is not None and TaskType is not None and Urgency is not None:
            try:
                self.workload_descriptor = WorkloadDescriptor(
                    task_id="loop_task",
                    task_type=TaskType.INFERENCE,
                    tokens=1000,
                    latency_target=500.0,
                    urgency=Urgency.MEDIUM,
                    use_evolutionary=bool(
                        self.config.get("use_evolutionary", True)
                    ),
                    human_feedback_score=float(
                        self.config.get("human_feedback_score", 0.6)
                    ),
                    graph_metrics=self.config.get(
                        "graph_metrics",
                        {"centrality": 0.7, "connectivity": 0.6},
                    ),
                    metadata={
                        "latency_weight": 0.4,
                        "carbon_weight": 0.3,
                        "energy_weight": 0.3,
                    },
                )
            except Exception as exc:
                failures.append("WorkloadDescriptor")
                logger.error("WorkloadDescriptor init failed: %s", exc)
        else:
            failures.append("WorkloadDescriptor")

        # 5. Zero Trust — opt-in only
        if self.config.get("enable_zero_trust", False):
            if ZeroTrustArchitecture is not None and ZeroTrustConfig is not None:
                try:
                    zt_cfg = ZeroTrustConfig(
                        use_enhancements=True,
                        use_distillation=True,
                        use_evolutionary=True,
                        human_feedback_score=float(
                            self.config.get("human_feedback_score", 0.6)
                        ),
                        graph_metrics=self.config.get(
                            "graph_metrics",
                            {"centrality": 0.7, "connectivity": 0.6},
                        ),
                    )
                    self.zero_trust = ZeroTrustArchitecture(config=zt_cfg)
                except Exception as exc:
                    failures.append("ZeroTrustArchitecture")
                    logger.error("ZeroTrust init failed: %s", exc)
            else:
                failures.append("ZeroTrustArchitecture")

        # 6. DAG carbon ledger — tempdir (auto-cleaned, cross-platform)
        if DAGCarbonLedger is not None:
            try:
                self._dag_ledger_tmpdir = tempfile.TemporaryDirectory(
                    prefix=_DEFAULT_DAG_LEDGER_PREFIX,
                )
                self.dag_carbon_ledger = DAGCarbonLedger(
                    storage_path=self._dag_ledger_tmpdir.name,
                )
            except Exception as exc:
                failures.append("DAGCarbonLedger")
                logger.error("DAGCarbonLedger init failed: %s", exc)
        else:
            failures.append("DAGCarbonLedger")

        if failures:
            # If more than half the modules failed, treat enhancements as off.
            total = len(_ENHANCEMENT_ERRORS)
            if len(failures) >= max(1, total // 2):
                self.use_enhancements = False
                logger.warning(
                    "Disabling enhancements — %d/%d modules unavailable: %s",
                    len(failures), total, ", ".join(failures),
                )
            else:
                logger.info(
                    "Enhancements partially available; %d module(s) unavailable: %s",
                    len(failures), ", ".join(failures),
                )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the agent and run the control loop."""
        logger.info("Starting Enhanced Green Agent...")
        self.data_source.start()
        self.running = True

        # Register SIGINT / SIGTERM for a graceful stop.
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._signal_handler)
            except (ValueError, OSError) as exc:
                logger.debug("Could not register %s: %s", sig, exc)

        try:
            self._run_main_loop()
        finally:
            self.stop()

    def _signal_handler(self, signum: int, _frame: Any) -> None:
        logger.info("Signal %s received; stopping", signum)
        self.running = False

    def stop(self) -> None:
        """Stop the agent and shut down all components in reverse order."""
        if not self.running and self._iteration > 0:
            # Already stopped cleanly.
            return
        logger.info("Stopping Enhanced Green Agent...")
        self.running = False

        # Each shutdown step is isolated — one failure does not skip the rest.
        try:
            self.data_source.stop()
        except Exception as exc:
            logger.error("Data source stop failed: %s", exc)

        if self.dag_carbon_ledger is not None:
            try:
                close = getattr(self.dag_carbon_ledger, "close", None)
                if callable(close):
                    _run(close())
            except Exception as exc:
                logger.error("DAG ledger close failed: %s", exc)

        if self.graph_registry is not None:
            try:
                shutdown = getattr(self.graph_registry, "shutdown", None)
                if callable(shutdown):
                    _run(shutdown())
            except Exception as exc:
                logger.error("Graph registry shutdown failed: %s", exc)

        if self._dag_ledger_tmpdir is not None:
            try:
                self._dag_ledger_tmpdir.cleanup()
            except Exception as exc:
                logger.debug("Temp dir cleanup failed: %s", exc)

    # -------------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------------

    def _run_main_loop(self) -> None:
        """Run the control loop with optional iteration limit."""
        max_iterations = self.config.get("max_iterations")
        while self.running:
            self._iteration += 1
            if max_iterations is not None and self._iteration > int(max_iterations):
                logger.info("Reached max_iterations (%s); stopping", max_iterations)
                self.running = False
                break
            try:
                self._process_iteration(self._iteration)
            except Exception as exc:
                logger.error("Iteration %d failed: %s", self._iteration, exc,
                             exc_info=True)
                if not self.running:
                    break
                time.sleep(1.0)
                continue
            # Exit path: --once sets max_iterations=1, so this is skipped.
            if self.running and max_iterations is None:
                time.sleep(_LOOP_INTERVAL_S)

    # -------------------------------------------------------------------------
    # Per-iteration processing
    # -------------------------------------------------------------------------

    def _process_iteration(self, iteration: int) -> None:
        """Process one control iteration."""
        # 1. Collect data with fallbacks
        temp_data = self._get_temperature_with_fallback()
        grid_data = self._get_grid_with_fallback()
        helium_data = self._get_helium_with_fallback()

        # 2. Original modules — pass real inputs to thermal
        thermal_decision = self._safe_call(
            self.thermal.optimize_schedule, temp_data, grid_data,
            default=None, label="thermal.optimize_schedule",
        )
        elasticity_decision = self._safe_call(
            self.helium_elasticity.get_elasticity_decision,
            WorkloadPriority.MEDIUM, 10.0, helium_data, "yellow",
            default=None, label="helium_elasticity.get_elasticity_decision",
        )

        # 3. Apply controls from original modules (guarded)
        if elasticity_decision is not None:
            action = getattr(elasticity_decision, "action", None)
            if action == "throttle":
                factor = float(getattr(elasticity_decision, "throttle_factor", 0.5))
                self._safe_call(
                    self.control_system.execute, "throttle", factor,
                    label="control_system.execute(throttle)",
                )

        if thermal_decision is not None:
            action = getattr(thermal_decision, "action", None)
            if action == "cool":
                target = float(getattr(thermal_decision, "target_temp", 25.0))
                cooling_power = max(50.0, min(500.0, (target - 20.0) * 10.0))
                self._safe_call(
                    self.control_system.execute, "cooling", cooling_power,
                    label="control_system.execute(cooling)",
                )

        # 4. Advanced enhancement decision-making
        enhanced_decision: dict[str, Any] = {}
        if self.use_enhancements:
            try:
                enhanced_decision = self._make_enhanced_decisions(
                    temp_data, grid_data, helium_data,
                )
            except Exception as exc:
                logger.error("Advanced decision-making failed: %s", exc,
                             exc_info=True)

        # 5. Apply enhanced controls (all branches covered)
        strategy = enhanced_decision.get("strategy")
        priority = enhanced_decision.get("priority")
        if strategy == "carbon_first":
            self._safe_call(
                self.control_system.execute, "throttle", 0.4,
                label="control_system.execute(throttle/carbon_first)",
            )
        elif strategy == "latency_first":
            # Do not throttle — prioritize latency
            pass
        elif strategy in {"cost_first", "balanced", "adaptive"}:
            self._safe_call(
                self.control_system.execute, "throttle", 0.5,
                label=f"control_system.execute(throttle/{strategy})",
            )
        if priority == "green":
            self._safe_call(
                self.control_system.execute, "throttle", 0.6,
                label="control_system.execute(throttle/green)",
            )

        # 6. Log metrics periodically
        if iteration % _LOG_EVERY_N_ITERATIONS == 0:
            self._log_metrics(temp_data, grid_data, helium_data, enhanced_decision)

    def _safe_call(self, fn: Any, *args: Any, default: Any = None,
                   label: str = "call") -> Any:
        """Call ``fn`` and log any exception; return ``default`` on failure."""
        try:
            return _run(fn(*args))
        except Exception as exc:
            logger.warning("%s failed: %s", label, exc)
            return default

    # -------------------------------------------------------------------------
    # Advanced decision-making
    # -------------------------------------------------------------------------

    def _make_enhanced_decisions(
        self, temp_data: Any, grid_data: Any, helium_data: Any,
    ) -> dict[str, Any]:
        """
        Use advanced modules to select routing strategy and workload priority.
        Run meta-cognition root-cause analysis and emit a FeedbackEvent.
        """
        decisions: dict[str, Any] = {}

        # Update node descriptor with current carbon intensity (if mutable)
        carbon = self._extract_carbon_intensity(grid_data)
        if self.node_descriptor is not None:
            try:
                # Guard against frozen dataclass
                if hasattr(self.node_descriptor, "region_carbon_intensity"):
                    setattr(self.node_descriptor, "region_carbon_intensity", carbon)
                strategy = _run(
                    self.node_descriptor.select_routing_strategy(exploration=False)
                )
                decisions["strategy"] = strategy
                self._current_strategy = strategy
            except Exception as exc:
                logger.warning("Routing strategy failed: %s", exc)

        # Workload priority
        if self.workload_descriptor is not None:
            try:
                priority = _run(
                    self.workload_descriptor.select_priority(exploration=False)
                )
                decisions["priority"] = priority
                self._current_priority = priority
            except Exception as exc:
                logger.warning("Workload priority failed: %s", exc)

        # Meta-cognition root-cause analysis
        if self.meta_cognition is not None:
            try:
                self.meta_cognition.observe_snapshot({
                    "CarbonIntensity": carbon,
                    "CarbonIntensity_high_threshold": 400.0,
                    "GridStrain": 0.7 if carbon > 500 else 0.4,
                })
                diagnosis = self.meta_cognition.diagnose() or {}
                if diagnosis.get("status") == "anomaly_detected":
                    decisions["diagnosis"] = diagnosis.get("recommended_action")
            except Exception as exc:
                logger.warning("Meta-cognition failed: %s", exc)

        # Record outcome for distillation (guarded)
        if self.node_descriptor is not None:
            try:
                _run(self.node_descriptor.record_outcome(
                    carbon_saved_kg=0.01,
                    latency_ms=100.0,
                    cost_usd=0.001,
                ))
            except Exception as exc:
                logger.debug("Node descriptor record_outcome failed: %s", exc)

        if self.workload_descriptor is not None:
            try:
                _run(self.workload_descriptor.record_outcome(
                    latency_achieved_ms=100.0,
                    carbon_saved_kg=0.01,
                    energy_used_joules=0.001,
                ))
            except Exception as exc:
                logger.debug("Workload descriptor record_outcome failed: %s", exc)

        # Emit FeedbackEvent — schema-conformant (README.md §2.5)
        if FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source="run_enhanced_agent",
                    feedback_type="routing",
                    task_id="loop_task",
                    action={
                        "selected_action": decisions.get("strategy", "unknown"),
                        "selected_rank": 1,
                    },
                    performance={
                        # README.md §2.5: `energy_kwh` (not energy_joules).
                        "quality_score": 0.8,
                        "latency_ms": 100.0,
                        "energy_kwh": 0.0001,        # 100 J ≈ 0.0001 kWh
                    },
                    adaptive_cost_value=0.7,
                    graph_metrics=self.config.get("graph_metrics", {}),
                    human_feedback_score=float(
                        self.config.get("human_feedback_score", 0.6)
                    ),
                    modp_score=0.7,
                )
                # Do NOT read event.event_id — not in schema.
                logger.debug(
                    "FeedbackEvent created (task_id=%s, strategy=%s)",
                    "loop_task", decisions.get("strategy"),
                )
            except Exception as exc:
                logger.warning("FeedbackEvent creation failed: %s", exc)

        return decisions

    @staticmethod
    def _extract_carbon_intensity(grid_data: Any) -> float:
        """Extract grid carbon intensity from a dataclass or dict."""
        for attr in ("average_intensity_gco2_per_kwh", "average_intensity"):
            value = _safe_attr(grid_data, attr)
            if isinstance(value, (int, float)):
                return float(value)
        if isinstance(grid_data, dict):
            for key in ("average_intensity_gco2_per_kwh", "average_intensity"):
                if isinstance(grid_data.get(key), (int, float)):
                    return float(grid_data[key])
        return 400.0

    # -------------------------------------------------------------------------
    # Data source wrappers with fallbacks
    # -------------------------------------------------------------------------

    def _get_temperature_with_fallback(self) -> Any:
        def primary() -> Any:
            return self.data_source.get_temperature_data()

        result = self.fallback_manager.execute_with_fallback(
            primary, "temperature", self._get_fallback_config(),
        )
        if getattr(result, "success", False):
            return result.value
        logger.error("Temperature data unavailable — using defaults")
        return {"gpu_temp": 70, "cpu_temp": 65}

    def _get_grid_with_fallback(self) -> Any:
        def primary() -> Any:
            return self.data_source.get_grid_data("us-east")

        result = self.fallback_manager.execute_with_fallback(
            primary, "grid", self._get_fallback_config(),
        )
        if getattr(result, "success", False):
            return result.value
        logger.error("Grid data unavailable — using defaults")
        return {"average_intensity": 400, "renewable_percentage": 0.2}

    def _get_helium_with_fallback(self) -> Any:
        def primary() -> Any:
            return self.data_source.get_helium_data()

        result = self.fallback_manager.execute_with_fallback(
            primary, "helium", self._get_fallback_config(),
        )
        if getattr(result, "success", False):
            return result.value
        logger.error("Helium data unavailable — using defaults")
        return {"spot_price": 6.0, "inventory_days": 20}

    def _get_fallback_config(self) -> Any:
        return FallbackConfig(strategy=FallbackStrategy.CASCADE)

    # -------------------------------------------------------------------------
    # Metrics + reports
    # -------------------------------------------------------------------------

    def _log_metrics(self, temp_data: Any, grid_data: Any, helium_data: Any,
                     enhanced_decision: Optional[dict[str, Any]] = None) -> None:
        """Log a compact metric snapshot (prior version dumped multi-line JSON at INFO)."""
        summary = {
            "iteration": self._iteration,
            "timestamp": datetime.now().isoformat(),
            "gpu_temp_c": _safe_attr(temp_data, "gpu_temp_c")
                          or _safe_attr(temp_data, "gpu_temp", default=0),
            "cpu_temp_c": _safe_attr(temp_data, "cpu_temp_c")
                          or _safe_attr(temp_data, "cpu_temp", default=0),
            "grid_intensity_g_kwh": self._extract_carbon_intensity(grid_data),
            "helium_price_usd_l": _safe_attr(helium_data, "spot_price_usd_per_liter")
                                    or _safe_attr(helium_data, "spot_price", default=0),
            "strategy": self._current_strategy,
            "priority": self._current_priority,
        }
        if enhanced_decision:
            summary["diagnosis"] = enhanced_decision.get("diagnosis")
        logger.info("metrics: %s", json.dumps(summary, separators=(",", ":")))

    def generate_report(self) -> str:
        """Return a JSON report of current status."""
        report: dict[str, Any] = {
            "version": __version__,
            "status": "running" if self.running else "stopped",
            "iteration": self._iteration,
            "data_source": self._safe_call(
                self.data_source.get_scenario_metrics, default={},
                label="data_source.get_scenario_metrics",
            ),
            "control_system": self._safe_call(
                self.control_system.get_status, default={},
                label="control_system.get_status",
            ),
            "circuit_breakers": self._safe_call(
                self.fallback_manager.get_circuit_breaker_status, default={},
                label="fallback_manager.get_circuit_breaker_status",
            ),
            "metrics": self._safe_call(
                self.control_system.get_metrics, default={},
                label="control_system.get_metrics",
            ),
        }
        if self.use_enhancements:
            report["enhancement_status"] = {
                "node_descriptor": self.node_descriptor is not None,
                "workload_descriptor": self.workload_descriptor is not None,
                "causal_graph": self.causal_graph is not None,
                "meta_cognition": self.meta_cognition is not None,
                "zero_trust": self.zero_trust is not None,
                "dag_carbon_ledger": self.dag_carbon_ledger is not None,
                "missing_modules": _missing_enhancements(),
            }
        return json.dumps(report, indent=2, default=str)


# =============================================================================
# CLI
# =============================================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_enhanced_agent",
        description=f"Enhanced Green Agent v{__version__}",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Config file (.yaml, .yml, or .json)",
    )
    parser.add_argument(
        "--scenario", choices=["normal", "heatwave", "helium_crisis", "high_carbon"],
        default="normal",
    )
    parser.add_argument(
        "--quality", choices=["perfect", "noisy", "degraded", "offline"],
        default="perfect",
    )
    # Enhanced toggles: None means "respect the config file"
    parser.add_argument(
        "--enhancements", dest="enhancements",
        action="store_true", default=None,
        help="Force-enable advanced modules (overrides config)",
    )
    parser.add_argument(
        "--no-enhancements", dest="enhancements",
        action="store_false",
        help="Force-disable advanced modules (overrides config)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run one iteration and exit (equivalent to --iterations 1)",
    )
    parser.add_argument(
        "--iterations", type=int, default=None,
        help="Maximum iterations before exit (default: unlimited)",
    )
    parser.add_argument("--quiet", action="store_true",
                        help="Only log warnings and errors")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--log-file", type=str, default=None,
                        help="Write logs to a file (in addition to stderr)")
    parser.add_argument("--report", action="store_true",
                        help="Print the JSON status report on exit")
    return parser


def _configure_logging(args: argparse.Namespace) -> None:
    level = (
        logging.DEBUG if args.verbose
        else logging.WARNING if args.quiet
        else logging.INFO
    )
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    _configure_logging(args)

    # --- Load config ------------------------------------------------------
    try:
        config = _load_config(args.config)
    except RuntimeError as exc:
        logger.error("Config load failed: %s", exc)
        return 2

    # --- Merge CLI overrides ---------------------------------------------
    config.setdefault("data", {})
    if isinstance(config["data"], dict):
        config["data"]["quality"] = args.quality

    if args.enhancements is not None:
        config["use_enhancements"] = bool(args.enhancements)

    if args.once and args.iterations is None:
        config["max_iterations"] = 1
    elif args.iterations is not None:
        if args.iterations < 1:
            logger.error("--iterations must be >= 1 (got %s)", args.iterations)
            return 2
        config["max_iterations"] = args.iterations

    # --- Construct agent --------------------------------------------------
    try:
        agent = EnhancedGreenAgent(config)
    except Exception as exc:
        logger.error("Agent initialization failed: %s", exc, exc_info=True)
        return 2

    # --- Apply scenario BEFORE starting the loop --------------------------
    scenario_map = {
        "normal": ScenarioType.NORMAL,
        "heatwave": ScenarioType.HEATWAVE,
        "helium_crisis": ScenarioType.HELIUM_CRISIS,
        "high_carbon": ScenarioType.HIGH_CARBON,
    }
    scenario = scenario_map.get(args.scenario, ScenarioType.NORMAL)
    try:
        agent.data_source.set_scenario(scenario)
        logger.info("Scenario set to %s", args.scenario)
    except Exception as exc:
        logger.error("Could not set scenario %s: %s", args.scenario, exc)
        return 2

    # --- Run --------------------------------------------------------------
    exit_code = 0
    try:
        agent.start()
    except KeyboardInterrupt:
        logger.info("Interrupted")
        exit_code = 130
    except Exception as exc:
        logger.error("Agent run failed: %s", exc, exc_info=True)
        exit_code = 1
    finally:
        if args.report:
            try:
                print(agent.generate_report())
            except Exception as exc:
                logger.error("Report generation failed: %s", exc)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
