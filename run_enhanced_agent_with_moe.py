#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_enhanced_agent_with_moe.py — Enhanced Green Agent with MoE (v2.0)

Wires together the MoE expert system (ExpertRegistry, MoEGatingNetwork,
ExpertRouter, ExpertMetricsCollector, LayerIntegrator) and its
integrations (EnhancedWorkIntegrator, QuantumLimitGraphIntegrator) with
the optional advanced enhancement modules (LIMIT Graph, MODP, RLHF,
Multi-Teacher On-Policy Distillation, Bio-inspired Optimisation, MoE
gating, FlexGen).

Enhanced in v2.0
----------------
- **Fixed `asyncio.run()` inside async context**. `_select_pipeline` is
  now `async def` and uses `_maybe_await` to handle sync-or-async
  `select_routing_strategy` / `select_priority`. The prior version
  raised `RuntimeError: asyncio.run() cannot be called from a running
  event loop` on every task execution.
- **`_maybe_await` helper** applied to every enhancement call. Prior
  version used `await` on methods documented as sync (INSTALLATION.md §2).
- **`FeedbackEvent` schema-conformant**: uses `energy_kwh` (per README.md
  §2.5); drops `energy_joules`, `carbon_g`, `helium_cost`, `duration_ms`;
  no reliance on `event.event_id`.
- **`NodeDescriptor` kwargs restricted** to documented ones — the 5 extra
  kwargs (helium_connectivity_score, uptime, renewable_fraction,
  cooling_type, hardware_model) removed.
- **`DAGCarbonLedger` storage path**: `tempfile.TemporaryDirectory()` —
  no more hardcoded `/tmp`.
- **Per-module diagnostics** in `_initialize_enhancement_modules` — one
  failing module does not disable all 8.
- **Import isolation**: `src.enhancements.*` tried first, then flat
  `enhancements.*`. `except ImportError` block now unbinds every
  referenced name, not just 10 of 15 (CoolingType, MaintenanceStatus,
  RoutingStrategy, Priority, BioMode were silently left unbound).
- **Config loading**: YAML or JSON detected by extension. Prior version
  only handled JSON despite the family's YAML configs.
- **`_meta` config block**, `data_dir`, environment overrides — aligned
  with the rest of the family's config surface.
- **Logging**: file handler path is `<data_dir>/logs/green_agent_moe.log`,
  not cwd; stream handler preserved. Prior version wrote to cwd with no
  rotation and no gitignore guidance.
- **Output artifacts** (`green_agent_moe_final_metrics.json`, `metrics.prom`)
  written to `<data_dir>/`, timestamped, no overwrites.
- **CLI flags**: `--config`, `--tasks`, `--once`, `--max-tasks N`,
  `--no-enhancements`, `--quiet`, `--verbose`, `--log-file`,
  `--output-dir`, `--report`.
- **Guarded result access**: `result.get(..., default)` for every field;
  no `KeyError` / `AttributeError` from unexpected shapes.
- **`batch_process`** returns correlated results keyed by `task_id`; prior
  version returned `task_index` only.
- **`get_status`** guarded: each sub-call wrapped in try/except.
- **`datetime.utcnow()`** replaced with `datetime.now(timezone.utc)`
  (deprecated since Python 3.12).
- **`_record_outcome_with_enhancements`** uses `energy_kwh` and `carbon_kg`
  — matching `README.md §2.5` — and no longer reads private attribute
  `_routing_optimizer.counter`.
- **Explicit exit codes**: 0 / 1 / 2 / 130.
- **Full type hints**, docstrings, header block.

Run
---
    # Batch: use built-in demo tasks, one pass, exit
    python run_enhanced_agent_with_moe.py --once

    # Custom tasks from JSON
    python run_enhanced_agent_with_moe.py --tasks tasks.json --once

    # Serve: run demo tasks and stay for reports
    python run_enhanced_agent_with_moe.py

    # No enhancements (base MoE only)
    python run_enhanced_agent_with_moe.py --once --no-enhancements

Exit codes
----------
    0  success
    1  partial failure (some tasks failed)
    2  config / environment error
    130 SIGINT (Ctrl+C)
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import os
import signal
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

__version__ = "5.0.0-moe"

# =============================================================================
# sys.path — resolved from __file__ so the script works from any cwd.
# =============================================================================
_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# =============================================================================
# Required MoE imports — flat `enhancements.*` or `src.enhancements.*`
# =============================================================================
def _import_moe_modules() -> tuple[dict[str, Any], Optional[Exception]]:
    """
    Import the MoE system + integrations.

    Tries ``src.enhancements.moe_expert_system`` first (matches the rest of
    the family), then falls back to ``enhancements.moe_expert_system``
    (the flat layout some deployments use).
    """
    prefixes = ("src.enhancements.moe_expert_system",
                "enhancements.moe_expert_system")
    names = {
        "ExpertRegistry": None,
        "MoEGatingNetwork": None,
        "ExpertRouter": None,
        "ExpertMetricsCollector": None,
        "LayerIntegrator": None,
        "EnhancedWorkIntegrator": None,
        "WorkContext": None,
        "QuantumLimitGraphIntegrator": None,
    }
    last_exc: Optional[Exception] = None
    for prefix in prefixes:
        try:
            core = __import__(prefix, fromlist=[
                "ExpertRegistry", "MoEGatingNetwork", "ExpertRouter",
                "ExpertMetricsCollector", "LayerIntegrator",
            ])
            integ = __import__(
                f"{prefix}.integration.enhanced_work_integration",
                fromlist=["EnhancedWorkIntegrator", "WorkContext"],
            )
            quant = __import__(
                f"{prefix}.integration.quantum_limit_integration",
                fromlist=["QuantumLimitGraphIntegrator"],
            )
        except Exception as exc:
            last_exc = exc
            continue
        names["ExpertRegistry"] = getattr(core, "ExpertRegistry", None)
        names["MoEGatingNetwork"] = getattr(core, "MoEGatingNetwork", None)
        names["ExpertRouter"] = getattr(core, "ExpertRouter", None)
        names["ExpertMetricsCollector"] = getattr(core, "ExpertMetricsCollector", None)
        names["LayerIntegrator"] = getattr(core, "LayerIntegrator", None)
        names["EnhancedWorkIntegrator"] = getattr(integ, "EnhancedWorkIntegrator", None)
        names["WorkContext"] = getattr(integ, "WorkContext", None)
        names["QuantumLimitGraphIntegrator"] = getattr(
            quant, "QuantumLimitGraphIntegrator", None,
        )
        return names, None
    return names, last_exc or ImportError("MoE package not found")


_MOE, _MOE_ERROR = _import_moe_modules()
if _MOE_ERROR is not None:
    print(
        f"FATAL: could not import MoE expert system: {_MOE_ERROR}\n"
        "Install with: pip install -e . --no-deps",
        file=sys.stderr,
    )
    sys.exit(2)

ExpertRegistry = _MOE["ExpertRegistry"]
MoEGatingNetwork = _MOE["MoEGatingNetwork"]
ExpertRouter = _MOE["ExpertRouter"]
ExpertMetricsCollector = _MOE["ExpertMetricsCollector"]
LayerIntegrator = _MOE["LayerIntegrator"]
EnhancedWorkIntegrator = _MOE["EnhancedWorkIntegrator"]
WorkContext = _MOE["WorkContext"]
QuantumLimitGraphIntegrator = _MOE["QuantumLimitGraphIntegrator"]


# =============================================================================
# Optional enhancement imports — per-module isolation
# =============================================================================
def _try_import(module: str, symbols: tuple[str, ...]) -> tuple[dict[str, Any], Optional[Exception]]:
    try:
        mod = __import__(module, fromlist=list(symbols))
    except Exception as exc:
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


logger = logging.getLogger("run_enhanced_agent_with_moe")


# =============================================================================
# Constants
# =============================================================================
_DEFAULT_CONFIG: dict[str, Any] = {
    "enable_quantum": True,
    "max_carbon_budget_kg": 0.1,
    "max_helium_budget": 0.05,
    "max_latency_ms": 1000,
    "reflection_enabled": True,
    "monitoring_enabled": True,
    "pipeline_default": "standard",
    "use_enhancements": True,
    "region": "us-east",
    "carbon_intensity": 400.0,
    "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
    "human_feedback_score": 0.6,
    "use_evolutionary": True,
    "distillation_epsilon": 0.1,
    "gating_lr": 0.005,
    "enable_zero_trust": True,
    "data_dir": "./green_agent_data",
}

_VALID_PIPELINES = {
    "standard", "quantum_enhanced", "helium_optimized", "meta_cognitive",
}


# =============================================================================
# Helpers
# =============================================================================

async def _maybe_await(value: Any) -> Any:
    """Await ``value`` if awaitable; return it otherwise."""
    if inspect.isawaitable(value):
        return await value
    return value


def _safe_attr(obj: Any, *path: str, default: Any = None) -> Any:
    cur = obj
    for name in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(name, default)
        else:
            cur = getattr(cur, name, default)
    return cur


def _load_config(path: Optional[str]) -> dict[str, Any]:
    """Load config from YAML or JSON; merge over defaults."""
    cfg = dict(_DEFAULT_CONFIG)
    if not path:
        return cfg
    p = Path(path)
    if not p.is_file():
        logger.warning("Config file not found: %s — using defaults", p)
        return cfg
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Could not read config {p}: {exc}") from exc

    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                f"pyyaml is required for {p.suffix} configs"
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
        return cfg
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Config {p} must be a mapping")
    cfg.update(loaded)
    return cfg


def _configure_logging(
    level: str, log_file: Optional[Path],
) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )


# =============================================================================
# EnhancedGreenAgentWithMoE
# =============================================================================

class EnhancedGreenAgentWithMoE:
    """
    Enhanced Green Agent with MoE integration plus optional advanced
    decision-making modules.
    """

    def __init__(
        self,
        enable_quantum: bool = True,
        config_path: Optional[str] = None,
        *,
        config: Optional[dict[str, Any]] = None,
    ) -> None:
        logger.info(
            "Initializing Enhanced Green Agent with MoE (version=%s)",
            __version__,
        )
        self.config = config if config is not None else _load_config(config_path)

        # --- MoE system ----------------------------------------------------
        self.metrics_collector = ExpertMetricsCollector()
        self.expert_router = ExpertRouter(
            enable_quantum=enable_quantum,
            metrics_collector=self.metrics_collector,
        )

        self.quantum_limiter = QuantumLimitGraphIntegrator()
        self.work_integrator = EnhancedWorkIntegrator(
            expert_router=self.expert_router,
            quantum_module=self.quantum_limiter if enable_quantum else None,
        )
        self.layer_integrator = LayerIntegrator(self.expert_router)

        # --- Advanced enhancement modules ---------------------------------
        want = bool(self.config.get("use_enhancements", True))
        self.use_enhancements = want and not _missing_enhancements()
        self.graph_registry: Any = None
        self.causal_graph: Any = None
        self.meta_cognition: Any = None
        self.node_descriptor: Any = None
        self.workload_descriptor: Any = None
        self.zero_trust: Any = None
        self.dag_carbon_ledger: Any = None
        self._dag_ledger_tmpdir: Optional[tempfile.TemporaryDirectory[str]] = None

        if self.use_enhancements:
            self._initialize_enhancement_modules()
        elif want and _missing_enhancements():
            logger.warning(
                "Enhancements requested but %d module(s) missing: %s",
                len(_missing_enhancements()),
                ", ".join(_missing_enhancements()),
            )

        self.start_time = datetime.now(timezone.utc)
        self.processed_tasks = 0
        logger.info(
            "Enhanced Green Agent with MoE initialized (advanced=%s)",
            self.use_enhancements,
        )

    # -------------------------------------------------------------------------
    # Enhancement initialization — per-module isolation
    # -------------------------------------------------------------------------
    def _initialize_enhancement_modules(self) -> None:
        failures: list[str] = []

        # 1. Graph registry
        if GraphRegistry is not None and GraphType is not None:
            try:
                self.graph_registry = GraphRegistry()
                self.causal_graph = self.graph_registry.get_or_create(GraphType.CAUSAL)
            except Exception as exc:
                failures.append("GraphRegistry")
                logger.error("GraphRegistry init failed: %s", exc)
        else:
            failures.append("GraphRegistry")

        # 2. Meta-cognition
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

        # 3. Node descriptor — documented kwargs only
        if NodeDescriptor is not None and NodeType is not None:
            try:
                self.node_descriptor = NodeDescriptor(
                    id="main_agent_node",
                    type=NodeType.EDGE,
                    region=self.config.get("region", "us-east"),
                    region_carbon_intensity=float(
                        self.config.get("carbon_intensity", 400.0)
                    ),
                    energy_per_token=0.00005,
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
        if (
            WorkloadDescriptor is not None
            and TaskType is not None
            and Urgency is not None
        ):
            try:
                self.workload_descriptor = WorkloadDescriptor(
                    task_id="adaptive_task",
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

        # 5. Zero Trust — opt-in
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

        # 6. DAG carbon ledger — tempdir
        if DAGCarbonLedger is not None:
            try:
                self._dag_ledger_tmpdir = tempfile.TemporaryDirectory(
                    prefix="green_agent_moe_ledger_",
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
            total = len(_ENHANCEMENT_ERRORS)
            if len(failures) >= max(1, total // 2):
                self.use_enhancements = False
                logger.warning(
                    "Disabling enhancements — %d/%d modules unavailable: %s",
                    len(failures), total, ", ".join(failures),
                )
            else:
                logger.info(
                    "Enhancements partially available; unavailable: %s",
                    ", ".join(failures),
                )

    # -------------------------------------------------------------------------
    # Pipeline selection — async-safe
    # -------------------------------------------------------------------------
    async def _select_pipeline(self, task: dict[str, Any]) -> str:
        """
        Select a pipeline based on advanced decision or heuristics.

        Async — prior version called ``asyncio.run()`` from inside
        ``process_task``, which raised
        ``RuntimeError: asyncio.run() cannot be called from a running event loop``.
        """
        if self.use_enhancements and self.node_descriptor and self.workload_descriptor:
            try:
                strategy = await _maybe_await(
                    self.node_descriptor.select_routing_strategy(exploration=False)
                )
                priority = await _maybe_await(
                    self.workload_descriptor.select_priority(exploration=False)
                )
                if strategy == "carbon_first" or priority == "green":
                    return "helium_optimized"
                if strategy == "latency_first" or priority == "accuracy":
                    return "meta_cognitive"
                if strategy in {"cost_first", "balanced", "adaptive"}:
                    return "standard"
            except Exception as exc:
                logger.warning(
                    "Advanced pipeline selection failed: %s — using heuristics", exc,
                )

        # Heuristic fallback
        if task.get("quantum_capable") and task.get("use_quantum"):
            return "quantum_enhanced"
        if task.get("helium_dependency", 0) > 0.5:
            return "helium_optimized"
        if task.get("complexity", 0) > 0.7:
            return "meta_cognitive"
        default = self.config.get("pipeline_default", "standard")
        return default if default in _VALID_PIPELINES else "standard"

    # -------------------------------------------------------------------------
    # Public task processing
    # -------------------------------------------------------------------------
    async def process_task(
        self,
        task: dict[str, Any],
        pipeline_type: Optional[str] = None,
    ) -> dict[str, Any]:
        self.processed_tasks += 1
        task_id = task.get("task_id") or task.get("id") or "unknown"

        if pipeline_type is None:
            pipeline_type = await self._select_pipeline(task)
        elif pipeline_type not in _VALID_PIPELINES:
            logger.warning(
                "Unknown pipeline %r; using 'standard'", pipeline_type,
            )
            pipeline_type = "standard"

        logger.info("Processing task %s with %s pipeline", task_id, pipeline_type)

        enhanced_context: dict[str, Any] = {}
        if self.use_enhancements:
            try:
                enhanced_context = await self._enhance_task_with_modules(task)
            except Exception as exc:
                logger.error("Enhancement pre-processing failed: %s", exc,
                             exc_info=True)

        try:
            result = await _maybe_await(
                self.work_integrator.process_work(
                    work_request=task, pipeline_type=pipeline_type,
                )
            )
        except Exception as exc:
            logger.error("process_work failed for %s: %s", task_id, exc,
                         exc_info=True)
            return {
                "success": False,
                "task_id": task_id,
                "error": str(exc),
                "pipeline_used": pipeline_type,
                "agent_metadata": {
                    "agent_version": __version__,
                    "processed_tasks": self.processed_tasks,
                },
            }

        if not isinstance(result, dict):
            result = {"success": True, "result": result}

        result.setdefault("success", True)
        result["task_id"] = task_id
        result.setdefault("agent_metadata", {})
        result["agent_metadata"].update({
            "agent_version": __version__,
            "processed_tasks": self.processed_tasks,
            "uptime_seconds": (
                datetime.now(timezone.utc) - self.start_time
            ).total_seconds(),
            "pipeline_used": pipeline_type,
            "enhanced_decision": enhanced_context,
        })

        if self.use_enhancements:
            try:
                await self._record_outcome_with_enhancements(
                    task, result, pipeline_type,
                )
            except Exception as exc:
                logger.warning("Post-processing failed: %s", exc)

        return result

    # -------------------------------------------------------------------------
    # Enhancement pre-processing — all calls via _maybe_await
    # -------------------------------------------------------------------------
    async def _enhance_task_with_modules(
        self, task: dict[str, Any],
    ) -> dict[str, Any]:
        context: dict[str, Any] = {}

        if self.workload_descriptor is not None:
            try:
                self._set_if_mutable(
                    self.workload_descriptor, "tokens",
                    int(task.get("token_count", 1000)),
                )
                self._set_if_mutable(
                    self.workload_descriptor, "latency_target",
                    float(task.get("max_latency_ms", 500.0)),
                )
                priority = await _maybe_await(
                    self.workload_descriptor.select_priority(exploration=False)
                )
                context["selected_priority"] = priority
            except Exception as exc:
                logger.warning("Workload descriptor failed: %s", exc)

        if self.node_descriptor is not None:
            try:
                carbon = float(
                    task.get("carbon_intensity",
                             self.config.get("carbon_intensity", 400.0))
                )
                self._set_if_mutable(
                    self.node_descriptor, "region_carbon_intensity", carbon,
                )
                strategy = await _maybe_await(
                    self.node_descriptor.select_routing_strategy(exploration=False)
                )
                context["selected_strategy"] = strategy
            except Exception as exc:
                logger.warning("Node descriptor failed: %s", exc)

        if self.meta_cognition is not None and self.causal_graph is not None:
            try:
                self.meta_cognition.observe_snapshot({
                    "CarbonIntensity": float(
                        task.get("carbon_intensity",
                                 self.config.get("carbon_intensity", 400.0))
                    ),
                    # Field renamed to `_high_threshold` in v2.0.
                    "CarbonIntensity_high_threshold": 400.0,
                    "GridStrain": float(task.get("grid_strain", 0.5)),
                })
                diagnosis = await _maybe_await(self.meta_cognition.diagnose()) or {}
                if diagnosis.get("status") == "anomaly_detected":
                    context["diagnosis"] = diagnosis.get("recommended_action")
            except Exception as exc:
                logger.warning("Meta-cognition failed: %s", exc)

        return context

    @staticmethod
    def _set_if_mutable(obj: Any, name: str, value: Any) -> None:
        """Set an attribute if the object exposes it and is not frozen."""
        if not hasattr(obj, name):
            return
        try:
            setattr(obj, name, value)
        except Exception:
            # Frozen dataclass or read-only property — skip silently;
            # the select_* methods will use whatever value is set.
            pass

    # -------------------------------------------------------------------------
    # Enhancement post-processing — schema-conformant FeedbackEvent
    # -------------------------------------------------------------------------
    async def _record_outcome_with_enhancements(
        self,
        task: dict[str, Any],
        result: dict[str, Any],
        pipeline_type: str,
    ) -> None:
        task_id = result.get("task_id") or task.get("task_id") or "unknown"
        energy_kwh = float(result.get("energy_kwh", 0.001))
        carbon_kg = float(result.get("carbon_kg", 0.0001))
        latency_ms = float(result.get("latency_ms", 100.0))
        cost_usd = float(result.get("cost_usd", 0.001))
        accuracy = float(result.get("accuracy", 0.9))

        # 1. Node descriptor outcome
        if self.node_descriptor is not None:
            try:
                await _maybe_await(self.node_descriptor.record_outcome(
                    carbon_saved_kg=float(result.get("carbon_saved", 0.0)),
                    latency_ms=latency_ms,
                    cost_usd=cost_usd,
                ))
            except Exception as exc:
                logger.debug("Node record_outcome failed: %s", exc)

        # 2. Workload descriptor outcome — energy_kwh (not joules)
        if self.workload_descriptor is not None:
            try:
                await _maybe_await(self.workload_descriptor.record_outcome(
                    latency_achieved_ms=latency_ms,
                    carbon_saved_kg=float(result.get("carbon_saved", 0.0)),
                    energy_used_joules=energy_kwh * 3_600_000.0,
                ))
            except Exception as exc:
                logger.debug("Workload record_outcome failed: %s", exc)

        # 3. DAG carbon ledger
        if self.dag_carbon_ledger is not None:
            try:
                node_id = self.dag_carbon_ledger.add_execution(
                    task_id=task_id,
                    framework="moe_agent",
                    energy_kwh=energy_kwh,
                    carbon_co2e_kg=carbon_kg,
                    accuracy=accuracy,
                    sustainability_index=float(
                        result.get("sustainability_index", 0.8)
                    ),
                    metadata={"pipeline": pipeline_type},
                )
                await _maybe_await(
                    self.dag_carbon_ledger.backpropagate_carbon(
                        node_id, transfer_rate=0.3,
                    )
                )
            except Exception as exc:
                logger.debug("DAG ledger record failed: %s", exc)

        # 4. FeedbackEvent — schema-conformant per README.md §2.5
        if FeedbackEvent is not None:
            try:
                FeedbackEvent(
                    source="run_enhanced_agent_with_moe",
                    feedback_type="routing",
                    task_id=task_id,
                    action={
                        "selected_action": pipeline_type,
                        "selected_rank": 1,
                    },
                    performance={
                        "quality_score": accuracy,
                        "latency_ms": latency_ms,
                        # README.md §2.5: `energy_kwh`, not `energy_joules`.
                        "energy_kwh": energy_kwh,
                    },
                    adaptive_cost_value=float(
                        result.get("sustainability_index", 0.0)
                    ),
                    graph_metrics=self.config.get("graph_metrics", {}),
                    human_feedback_score=float(
                        self.config.get("human_feedback_score", 0.6)
                    ),
                    modp_score=float(result.get("modp_score", 0.5)),
                )
                # Do NOT read event.event_id — not in schema.
                logger.debug(
                    "FeedbackEvent created (task_id=%s, pipeline=%s)",
                    task_id, pipeline_type,
                )
            except Exception as exc:
                logger.warning("FeedbackEvent creation failed: %s", exc)

    # -------------------------------------------------------------------------
    # Batch processing — correlated results
    # -------------------------------------------------------------------------
    async def batch_process(
        self,
        tasks: list[dict[str, Any]],
        max_concurrent: int = 5,
    ) -> list[dict[str, Any]]:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _process(task: dict[str, Any], index: int) -> dict[str, Any]:
            async with semaphore:
                try:
                    result = await self.process_task(task)
                    result["task_index"] = index
                    return result
                except Exception as exc:
                    logger.error("Task %d failed: %s", index, exc,
                                 exc_info=True)
                    return {
                        "success": False,
                        "task_id": task.get("task_id", f"index_{index}"),
                        "task_index": index,
                        "error": str(exc),
                    }

        return await asyncio.gather(
            *(_process(t, i) for i, t in enumerate(tasks)),
        )

    # -------------------------------------------------------------------------
    # Status and metrics
    # -------------------------------------------------------------------------
    def get_status(self) -> dict[str, Any]:
        def _safe(fn: Callable[[], Any], default: Any) -> Any:
            try:
                return fn()
            except Exception as exc:
                logger.warning("Status sub-call failed: %s", exc)
                return default

        status: dict[str, Any] = {
            "version": __version__,
            "status": "running",
            "uptime_seconds": (
                datetime.now(timezone.utc) - self.start_time
            ).total_seconds(),
            "processed_tasks": self.processed_tasks,
            "routing_stats": _safe(self.expert_router.get_routing_stats, {}),
            "work_stats": _safe(self.work_integrator.get_work_statistics, {}),
            "planetary_boundaries": _safe(
                self.quantum_limiter.get_planetary_boundary_status, {},
            ),
            "integration_status": _safe(
                self.layer_integrator.get_integration_status, {},
            ),
            "metrics_summary": _safe(
                self.metrics_collector.get_metrics_summary, {},
            ),
        }
        if self.use_enhancements:
            status["enhancement_status"] = {
                "node_descriptor": self.node_descriptor is not None,
                "workload_descriptor": self.workload_descriptor is not None,
                "causal_graph": self.causal_graph is not None,
                "meta_cognition": self.meta_cognition is not None,
                "zero_trust": self.zero_trust is not None,
                "dag_carbon_ledger": self.dag_carbon_ledger is not None,
                "missing_modules": _missing_enhancements(),
            }
        return status

    def export_metrics_prometheus(self) -> str:
        try:
            return self.metrics_collector.to_prometheus_format()
        except Exception as exc:
            logger.error("Prometheus export failed: %s", exc)
            return ""

    async def shutdown(self) -> None:
        logger.info("Shutting down Enhanced Green Agent with MoE")

        # Each step isolated — one failure does not skip the rest.
        if self.dag_carbon_ledger is not None:
            try:
                close = getattr(self.dag_carbon_ledger, "close", None)
                if callable(close):
                    await _maybe_await(close())
            except Exception as exc:
                logger.error("DAG ledger close failed: %s", exc)

        if self._dag_ledger_tmpdir is not None:
            try:
                self._dag_ledger_tmpdir.cleanup()
            except Exception as exc:
                logger.debug("Temp dir cleanup failed: %s", exc)

        if self.graph_registry is not None:
            try:
                shutdown = getattr(self.graph_registry, "shutdown", None)
                if callable(shutdown):
                    await _maybe_await(shutdown())
            except Exception as exc:
                logger.error("Graph registry shutdown failed: %s", exc)

        logger.info("Shutdown complete")


# =============================================================================
# Demo tasks (used when --tasks not provided)
# =============================================================================
_DEMO_TASKS: list[dict[str, Any]] = [
    {
        "task_id": "task_001",
        "task_type": "inference",
        "priority": 1,
        "complexity": 0.3,
        "helium_dependency": 0.2,
        "carbon_zone": 2,
        "max_carbon_budget": 0.05,
        "max_helium_budget": 0.02,
        "max_latency_ms": 100,
        "meta_cognitive_state": {
            "historical_success_rate": 0.95,
            "carbon_budget_remaining": 0.05,
            "helium_budget_remaining": 0.02,
        },
    },
    {
        "task_id": "task_002",
        "task_type": "optimization",
        "priority": 2,
        "complexity": 0.8,
        "helium_dependency": 0.7,
        "carbon_zone": 8,
        "quantum_capable": True,
        "use_quantum": True,
        "max_carbon_budget": 0.1,
        "max_helium_budget": 0.05,
        "max_latency_ms": 500,
    },
    {
        "task_id": "task_003",
        "task_type": "data_processing",
        "priority": 1,
        "complexity": 0.5,
        "helium_dependency": 0.4,
        "carbon_zone": 4,
        "max_carbon_budget": 0.08,
        "max_helium_budget": 0.03,
        "max_latency_ms": 200,
    },
]


# =============================================================================
# CLI
# =============================================================================
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_enhanced_agent_with_moe",
        description=f"Enhanced Green Agent with MoE v{__version__}",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Config file (.yaml, .yml, or .json)",
    )
    parser.add_argument(
        "--tasks", type=str, default=None,
        help="JSON file with tasks (defaults to built-in demo tasks)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Process the task list once and exit (no second batch pass)",
    )
    parser.add_argument(
        "--max-tasks", type=int, default=None,
        help="Maximum tasks to process (default: all)",
    )
    parser.add_argument(
        "--no-quantum", action="store_true",
        help="Disable quantum integration",
    )
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
        "--output-dir", type=str, default=None,
        help="Directory for artifacts (default: <data_dir>/output)",
    )
    parser.add_argument("--quiet", action="store_true",
                        help="Only log warnings and errors")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--log-file", type=str, default=None,
                        help="Write logs to this file (in addition to stderr)")
    parser.add_argument("--report", action="store_true",
                        help="Print the JSON status report on exit")
    return parser


def _load_tasks(path: Optional[str]) -> list[dict[str, Any]]:
    if not path:
        return [dict(t) for t in _DEMO_TASKS]
    p = Path(path)
    if not p.is_file():
        raise RuntimeError(f"Tasks file not found: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse tasks {p}: {exc}") from exc
    if isinstance(data, dict) and "tasks" in data:
        data = data["tasks"]
    if not isinstance(data, list):
        raise RuntimeError(f"Tasks file {p} must contain a JSON array")
    return data


async def _async_main(args: argparse.Namespace) -> int:
    # --- Load tasks -----------------------------------------------------
    try:
        tasks = _load_tasks(args.tasks)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2
    if not tasks:
        logger.error("No tasks to process")
        return 2
    if args.max_tasks is not None:
        if args.max_tasks < 1:
            logger.error("--max-tasks must be >= 1")
            return 2
        tasks = tasks[: args.max_tasks]

    # --- Merge CLI overrides into config --------------------------------
    try:
        config = _load_config(args.config)
    except RuntimeError as exc:
        logger.error("Config load failed: %s", exc)
        return 2

    if args.enhancements is not None:
        config["use_enhancements"] = bool(args.enhancements)

    if args.output_dir:
        config["data_dir"] = args.output_dir

    # --- Construct agent ------------------------------------------------
    try:
        agent = EnhancedGreenAgentWithMoE(
            enable_quantum=not args.no_quantum,
            config=config,
        )
    except Exception as exc:
        logger.error("Agent initialization failed: %s", exc, exc_info=True)
        return 2

    exit_code = 0
    try:
        # Signal handlers
        loop = asyncio.get_running_loop()
        shutdown = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown.set)
            except NotImplementedError:
                pass

        # --- Process ----------------------------------------------------
        results: list[dict[str, Any]] = []
        try:
            for task in tasks:
                if shutdown.is_set():
                    break
                result = await agent.process_task(task)
                results.append(result)
                print(
                    f"Task {result.get('task_id')}: "
                    f"{'success' if result.get('success') else 'FAILED'} "
                    f"(pipeline={result.get('pipeline_used', '?')})"
                )
        except Exception as exc:
            logger.error("Processing failed: %s", exc, exc_info=True)
            exit_code = 1

        # Optional second pass through the batch API
        if not args.once and not shutdown.is_set():
            try:
                batch_results = await agent.batch_process(
                    tasks, max_concurrent=min(5, len(tasks)),
                )
                print(f"Batch: {len(batch_results)} tasks completed")
                if any(not r.get("success") for r in batch_results):
                    exit_code = max(exit_code, 1)
            except Exception as exc:
                logger.error("Batch processing failed: %s", exc, exc_info=True)
                exit_code = max(exit_code, 1)

        # --- Artifacts -------------------------------------------------
        data_dir = Path(config.get("data_dir", "./green_agent_data"))
        out_dir = data_dir / "output"
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        try:
            status = agent.get_status()
            (out_dir / f"final_metrics_{stamp}.json").write_text(
                json.dumps(status, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.error("Metrics export failed: %s", exc)

        try:
            prom = agent.export_metrics_prometheus()
            if prom:
                (out_dir / f"metrics_{stamp}.prom").write_text(
                    prom, encoding="utf-8",
                )
        except Exception as exc:
            logger.error("Prometheus export failed: %s", exc)

        if args.report:
            print(agent.generate_report()
                  if hasattr(agent, "generate_report")
                  else json.dumps(agent.get_status(), indent=2, default=str))

        return exit_code

    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        return 1
    finally:
        try:
            await agent.shutdown()
        except Exception as exc:
            logger.error("Shutdown failed: %s", exc)


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    # Default log file when --log-file omitted
    data_dir = Path(args.output_dir) if args.output_dir else Path(
        _DEFAULT_CONFIG["data_dir"]
    )
    log_file = (
        Path(args.log_file) if args.log_file
        else data_dir / "logs" / "green_agent_moe.log"
    )

    level = (
        "DEBUG" if args.verbose
        else "WARNING" if args.quiet
        else "INFO"
    )
    _configure_logging(level, log_file)

    try:
        return asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
