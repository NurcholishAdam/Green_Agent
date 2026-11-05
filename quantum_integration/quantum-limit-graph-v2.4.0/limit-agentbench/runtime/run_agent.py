#!/usr/bin/env python3
"""
run_agent.py — Green Agent v5.0.0 Main Entry Point (Enhanced v2)

Initializes all components, wires helium monitoring, starts metrics export,
and runs the unified orchestration loop.

---- Fixes vs v1 ---------------------------------------------------------------
  * Removed ``registry`` NameError in EnhancedOrchestrationLayer (was referring
    to the outer name instead of ``self.registry``).
  * ``pre_execute`` / ``post_execute`` are now ``async`` and awaited from
    ``main``. Removed the two ``asyncio.run(...)`` calls that raised inside the
    already-running event loop.
  * ``decision_context`` is forwarded into ``agent.execute_task`` (v1 logged it
    and discarded it).
  * Duplicate ``FeedbackEvent`` import removed.
  * ``FeedbackEvent`` is delivered to a pluggable ``feedback_sink`` (defaults
    to logging; override via ``config['enhancements']['feedback_sink']``).
  * Signal handler cancels the in-flight task, uses ``get_running_loop``, and
    falls back to ``signal.signal`` on Windows.
  * Shutdown order wrapped in ``try/except``; exporter start tracked so
    ``stop_http_server`` only runs if start succeeded.
  * Added ``--once``, ``--dry-run``, ``--log-level``, ``--seed`` flags.
  * Added per-process Prometheus counters for tasks, duration, failures,
    enhanced decisions, soft failures, and feedback events.
  * ``helium_config`` deep-copied before mutating; ``register_helium_monitor``
    guarded with ``hasattr``.
  * Config + task JSON errors reported cleanly (``SystemExit`` with message).
  * ``post_execute`` runs even when ``execute_task`` fails.

---- Module map ----------------------------------------------------------------
  1. Imports (with explicit None fallbacks for enhancements)
  2. Prometheus metrics definitions
  3. EnhancedOrchestrationLayer       ← core enhancement wrapper (all async)
  4. load_config / load_task          ← validated I/O
  5. main()                           ← orchestration + signal handling
  6. _parse_args / __main__           ← CLI entry point
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.graph_registry import GraphRegistry, GraphType  # noqa: F401
from carbon.helium_monitor import HeliumMonitor
from monitoring.graph_metrics_exporter import GraphMetricsExporter
from integration.unified_orchestrator import UnifiedGreenAgent

# ---------------------------------------------------------------------------
# MODULE 1 — Optional enhanced imports (no duplicate, explicit fallbacks)
# ---------------------------------------------------------------------------
try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType,
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency,
    )
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig,
    )
    ENHANCEMENTS_AVAILABLE = True
except ImportError as _exc:                              # noqa: F841
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None                                # type: ignore
    NodeType = None                                      # type: ignore
    CoolingType = None                                   # type: ignore
    WorkloadDescriptor = None                            # type: ignore
    TaskType = None                                      # type: ignore
    Urgency = None                                       # type: ignore
    FeedbackEvent = None                                 # type: ignore
    ZeroTrustArchitecture = None                         # type: ignore
    ZeroTrustConfig = None                               # type: ignore
    # Note: logger is not yet configured at import time; a warning is emitted
    # by main() if enhancements are requested but un-importable.


logger = logging.getLogger("green_agent.run_agent")


# ---------------------------------------------------------------------------
# MODULE 2 — Prometheus metrics for this process
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram
    _PROM = True
except ImportError:
    _PROM = False

if _PROM:
    TASKS_TOTAL = Counter(
        "green_agent_tasks_total", "Tasks executed",
        ["status", "mode"],
    )
    TASK_DURATION = Histogram(
        "green_agent_task_duration_seconds", "Task wall-clock duration",
        buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
    )
    ENHANCED_DECISIONS = Counter(
        "green_agent_enhanced_decisions_total", "Enhanced decisions emitted",
        ["strategy"],
    )
    SOFT_FAILURES = Counter(
        "green_agent_soft_failures_total", "Soft failures injected/observed",
    )
    FEEDBACK_EVENTS = Counter(
        "green_agent_feedback_events_total", "Feedback events emitted",
        ["type"],
    )
else:                                                    # pragma: no cover
    TASKS_TOTAL = None
    TASK_DURATION = None
    ENHANCED_DECISIONS = None
    SOFT_FAILURES = None
    FEEDBACK_EVENTS = None


# ---------------------------------------------------------------------------
# MODULE 3 — Enhanced orchestration layer
# ---------------------------------------------------------------------------
class EnhancedOrchestrationLayer:
    """
    Wraps the core orchestrator with advanced decision-making components:
      * NodeDescriptor / WorkloadDescriptor for context
      * ZeroTrustArchitecture (optional, for signing feedback)
      * FeedbackEvent delivery to a pluggable sink

    All task-lifecycle methods are ``async`` so they can safely ``await``
    descriptor updates without spawning a new event loop.
    """

    def __init__(self, registry: GraphRegistry, config: Dict[str, Any]):
        self.registry = registry
        self.config = config
        self.use_enhancements = bool(
            config.get("enhancements", {}).get("enabled", False)
            and ENHANCEMENTS_AVAILABLE
        )

        self.node_descriptor: Optional[Any] = None
        self.workload_descriptor: Optional[Any] = None
        self.zero_trust: Optional[Any] = None
        self._feedback_sink: Optional[Callable[[Any], None]] = None

        if self.use_enhancements:
            self._init_enhanced_components()
        elif config.get("enhancements", {}).get("enabled", False):
            logger.warning(
                "enhancements.enabled=True but enhanced modules are not "
                "importable; running in legacy mode."
            )

    # -- init --------------------------------------------------------------
    def _init_enhanced_components(self) -> None:
        cfg = self.config.get("enhancements", {})
        graph_metrics = cfg.get("graph_metrics", {})
        human_feedback = float(cfg.get("human_feedback_score", 0.5))
        graph_id = getattr(self.registry, "graph_id", None)

        # --- NodeDescriptor ---
        try:
            self.node_descriptor = NodeDescriptor(
                id="main_agent_node",
                type=NodeType.EDGE,
                region=cfg.get("region", "us-east"),
                region_carbon_intensity=cfg.get("carbon_intensity", 400.0),
                energy_per_token=0.00005,
                helium_connectivity_score=0.8,
                uptime=0.99,
                renewable_fraction=0.3,
                cooling_type=CoolingType.AIR,
                hardware_model="cpu",
                graph_id=graph_id,
                graph_metrics=graph_metrics,
                human_feedback_score=human_feedback,
                metadata={"role": "main_agent"},
            )
            logger.info("NodeDescriptor initialized")
        except Exception as exc:                         # noqa: BLE001
            logger.error("Failed to init NodeDescriptor: %s", exc)

        # --- WorkloadDescriptor ---
        try:
            self.workload_descriptor = WorkloadDescriptor(
                task_id="adaptive_task",
                task_type=TaskType.INFERENCE,
                tokens=1000,
                latency_target=500.0,
                urgency=Urgency.MEDIUM,
                estimated_energy_joules=0.001,
                estimated_carbon_kg=0.0002,
                user_id="orchestrator",
                metadata={"source": "run_agent"},
            )
            logger.info("WorkloadDescriptor initialized")
        except Exception as exc:                         # noqa: BLE001
            logger.error("Failed to init WorkloadDescriptor: %s", exc)

        # --- Zero Trust (optional) ---
        if cfg.get("enable_zero_trust", False):
            try:
                zt_cfg = ZeroTrustConfig(**cfg.get("zero_trust", {}))
                self.zero_trust = ZeroTrustArchitecture(zt_cfg)
                logger.info("ZeroTrustArchitecture initialized")
            except Exception as exc:                     # noqa: BLE001
                logger.error("Failed to init ZeroTrust: %s", exc)

        # --- Feedback sink ---
        self._feedback_sink = cfg.get("feedback_sink") or (
            lambda ev: logger.info(
                "feedback_event id=%s type=%s task=%s",
                getattr(ev, "event_id", "?"),
                getattr(ev, "feedback_type", "?"),
                getattr(ev, "task_id", "?"),
            )
        )

        logger.info("Enhanced orchestration layer initialized")

    # -- pre ---------------------------------------------------------------
    async def pre_execute(self, task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Adapt task context before execution. Returns a decision-context dict
        that is forwarded into ``agent.execute_task(..., decision_context=...)``.
        """
        if not self.use_enhancements or self.workload_descriptor is None:
            return None
        try:
            self.workload_descriptor.estimated_energy_joules = float(
                task.get("energy_estimate", 0.001)
            )
            self.workload_descriptor.estimated_carbon_kg = float(
                task.get("carbon_estimate", 0.0002)
            )
            hf = float(self.config.get("enhancements", {}).get(
                "human_feedback_score", 0.5
            ))
            # Prefer a documented setter; fall back to attribute assignment
            if hasattr(self.workload_descriptor, "update_human_feedback"):
                self.workload_descriptor.update_human_feedback(hf)
            elif hasattr(self.workload_descriptor, "human_feedback_score"):
                setattr(self.workload_descriptor, "human_feedback_score", hf)

            return {
                "enhanced": True,
                "strategy": "adaptive",
                "human_feedback": hf,
            }
        except Exception as exc:                         # noqa: BLE001
            logger.warning("pre_execute failed: %s", exc)
            return None

    # -- post --------------------------------------------------------------
    async def post_execute(
        self, task: Dict[str, Any], result: Any,
    ) -> None:
        """
        Feed execution outcomes back into descriptors, emit a FeedbackEvent,
        and (optionally) sign it with Zero Trust.
        """
        if not self.use_enhancements:
            return

        metrics: Dict[str, float] = {}
        if hasattr(result, "energy_consumed"):
            metrics["energy_kwh"] = float(result.energy_consumed)
        if hasattr(result, "carbon_emitted"):
            metrics["carbon_kg"] = float(result.carbon_emitted)
        if hasattr(result, "accuracy"):
            metrics["accuracy"] = float(result.accuracy)
        latency = float(getattr(result, "latency_ms", 100.0))

        # --- WorkloadDescriptor.record_outcome (awaited, no asyncio.run) ---
        if self.workload_descriptor is not None and hasattr(
            self.workload_descriptor, "record_outcome"
        ):
            try:
                await self.workload_descriptor.record_outcome(
                    latency_achieved_ms=latency,
                    carbon_saved_kg=max(
                        0.0, 0.01 - metrics.get("carbon_kg", 0.0)
                    ),
                    energy_used_joules=metrics.get("energy_kwh", 0.0) * 3600.0,
                )
            except Exception as exc:                     # noqa: BLE001
                logger.warning(
                    "workload_descriptor.record_outcome failed: %s", exc,
                )

        # --- NodeDescriptor.record_outcome ---
        if self.node_descriptor is not None and hasattr(
            self.node_descriptor, "record_outcome"
        ):
            try:
                await self.node_descriptor.record_outcome(
                    carbon_saved_kg=max(
                        0.0, 0.01 - metrics.get("carbon_kg", 0.0)
                    ),
                    latency_ms=latency,
                    cost_usd=0.001,
                )
            except Exception as exc:                     # noqa: BLE001
                logger.warning(
                    "node_descriptor.record_outcome failed: %s", exc,
                )

        # --- FeedbackEvent (delivered to sink, optionally signed) ---
        if FeedbackEvent is None:
            return
        try:
            event = FeedbackEvent(
                source="run_agent",
                feedback_type="routing",
                task_id=task.get("id", "unknown"),
                context={"strategy": "enhanced"},
                action={"selected_action": "execute", "selected_rank": 1},
                performance={
                    "quality_score": metrics.get("accuracy", 0.0),
                    "latency_ms": latency,
                    "energy_joules": metrics.get("energy_kwh", 0.0) * 3600.0,
                    "carbon_g": metrics.get("carbon_kg", 0.0) * 1000.0,
                    "helium_cost": 0.0,
                    "duration_ms": latency,
                },
                adaptive_cost_value=0.5,
                tags=["main_agent", "enhanced"],
            )

            if self.zero_trust is not None and hasattr(
                self.zero_trust, "sign_event"
            ):
                try:
                    event = self.zero_trust.sign_event(event)
                except Exception as exc:                 # noqa: BLE001
                    logger.warning("zero_trust.sign_event failed: %s", exc)

            if self._feedback_sink is not None:
                self._feedback_sink(event)
            if FEEDBACK_EVENTS is not None:
                FEEDBACK_EVENTS.labels(type="routing").inc()
        except Exception as exc:                         # noqa: BLE001
            logger.warning("FeedbackEvent creation failed: %s", exc)


# ---------------------------------------------------------------------------
# MODULE 4 — Config / task loading (validated)
# ---------------------------------------------------------------------------
def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML; clean error on malformed input."""
    path = Path(config_path)
    if not path.exists():
        logger.warning("Config not found: %s — using defaults", config_path)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise SystemExit(f"Invalid YAML in {config_path}: {exc}") from exc
    if not isinstance(config, dict):
        raise SystemExit(
            f"Config root must be a mapping, got {type(config).__name__}"
        )
    logger.info("Loaded configuration from %s", config_path)
    return config


def load_task(task_path: str) -> Dict[str, Any]:
    """Load a task JSON; refuses paths outside the current working directory."""
    path = Path(task_path).resolve()
    try:
        path.relative_to(Path.cwd())
    except ValueError:
        raise SystemExit(
            f"Refusing to read task outside CWD: {task_path}"
        ) from None
    try:
        with open(path, "r", encoding="utf-8") as f:
            task = json.load(f)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {task_path}: {exc}") from exc
    if not isinstance(task, dict):
        raise SystemExit("Task JSON root must be an object")
    return task


# ---------------------------------------------------------------------------
# MODULE 5 — Main orchestration
# ---------------------------------------------------------------------------
async def main(args: argparse.Namespace) -> int:
    """Initialize components, run the orchestration loop, return exit code."""

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    if args.seed is not None:
        random.seed(args.seed)
        try:
            import numpy as _np
            _np.random.seed(args.seed)
        except ImportError:
            pass

    logger.info("Starting Green Agent v5.0.0 (%s mode)", args.mode)
    config = load_config(args.config)
    if args.dry_run:
        logger.info("Dry run — config loaded successfully. Exiting.")
        return 0

    # --- Core registry ---------------------------------------------------
    registry = GraphRegistry()

    # --- Helium monitor (optional) --------------------------------------
    helium_monitor: Optional[HeliumMonitor] = None
    helium_cfg = deepcopy(config.get("helium", {}))
    if helium_cfg.get("enabled", False):
        try:
            api_key_env = helium_cfg.get("api_key_env_var", "HELIUM_API_KEY")
            helium_cfg["api_key"] = os.getenv(
                api_key_env, helium_cfg.get("api_key"),
            )
            helium_monitor = HeliumMonitor(
                config=helium_cfg,
                simulation_seed=helium_cfg.get("simulation_seed", args.seed),
            )
            if hasattr(registry, "register_helium_monitor"):
                registry.register_helium_monitor(helium_monitor)
            logger.info("HeliumMonitor initialized and registered")
        except Exception as exc:                         # noqa: BLE001
            logger.error("Failed to initialize HeliumMonitor: %s", exc)
            helium_monitor = None

    # --- Enhanced orchestration layer -----------------------------------
    enhancement_layer = EnhancedOrchestrationLayer(registry, config)
    if enhancement_layer.use_enhancements:
        logger.info("Enhanced orchestration layer ACTIVE")
    else:
        logger.info("Enhanced orchestration layer inactive (legacy mode)")

    # --- Unified orchestrator -------------------------------------------
    agent = UnifiedGreenAgent(config)
    await agent.initialize()

    # --- Metrics exporter ----------------------------------------------
    exporter: Optional[GraphMetricsExporter] = None
    exporter_started = False
    prom_cfg = config.get("monitoring", {}).get("prometheus", {})
    metrics_port = int(os.getenv(
        "PROMETHEUS_PORT", prom_cfg.get("port", 8000),
    ))
    try:
        exporter = GraphMetricsExporter(
            registry=registry,
            job_name="green_agent",
            max_edges_export=100,
            helium_monitor=helium_monitor,
        )
        exporter.start_http_server(port=metrics_port)
        exporter_started = True
        logger.info(
            "Prometheus metrics at http://0.0.0.0:%d/metrics", metrics_port,
        )
    except Exception as exc:                             # noqa: BLE001
        logger.error("Failed to start metrics exporter: %s", exc)

    # --- Signal handling ------------------------------------------------
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    in_flight: Dict[str, Optional[asyncio.Task]] = {"task": None}

    def _request_shutdown(reason: str) -> None:
        logger.info("Shutdown requested (%s)", reason)
        shutdown_event.set()
        t = in_flight.get("task")
        if t is not None and not t.done():
            t.cancel()

    def _signal_handler_sync(signum, _frame):
        _request_shutdown(f"signal {signum}")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                sig, _request_shutdown, sig.name,
            )
        except (NotImplementedError, RuntimeError):
            # Windows / unsupported loop
            try:
                signal.signal(sig, _signal_handler_sync)
            except Exception as exc:                     # noqa: BLE001
                logger.warning(
                    "Could not install handler for %s: %s", sig, exc,
                )

    # --- Execute --------------------------------------------------------
    exit_code = 0
    task_timeout = float(config.get("task_timeout_seconds", 600))
    try:
        task = load_task(args.task) if args.task else {
            "id": "demo_001",
            "type": "ml_inference",
            "priority": 5,
            "deferrable": True,
            "model": "llama-7b",
            "input_size_mb": 128,
        }

        # Enhanced pre-execution decision (awaited, not discarded)
        decision_context = await enhancement_layer.pre_execute(task)
        if decision_context:
            logger.info("Enhanced decision: %s", decision_context)
            if ENHANCED_DECISIONS is not None:
                ENHANCED_DECISIONS.labels(
                    strategy=decision_context.get("strategy", "unknown"),
                ).inc()

        logger.info("Executing task: %s", task.get("id"))
        t0 = time.monotonic()
        exec_coro = agent.execute_task(
            task,
            decision_context=decision_context,   # forwarded (v1 discarded this)
        )
        task_fut = asyncio.create_task(
            asyncio.wait_for(exec_coro, timeout=task_timeout)
        )
        in_flight["task"] = task_fut

        result = None
        try:
            result = await task_fut
        except asyncio.CancelledError:
            logger.warning("Task cancelled by shutdown signal")
            exit_code = 130
        except asyncio.TimeoutError:
            logger.error("Task exceeded %.1fs timeout", task_timeout)
            exit_code = 124
        except Exception as exc:                         # noqa: BLE001
            logger.exception("execute_task failed: %s", exc)
            exit_code = 1
        finally:
            duration = time.monotonic() - t0
            if TASK_DURATION is not None:
                TASK_DURATION.observe(duration)
            if TASKS_TOTAL is not None:
                TASKS_TOTAL.labels(
                    status="ok" if exit_code == 0 else "fail",
                    mode=args.mode,
                ).inc()
            # post_execute runs even on failure, if we have a result
            if result is not None:
                await enhancement_layer.post_execute(task, result)

        # --- Result reporting -------------------------------------------
        if result is not None:
            logger.info(
                "Task %s: %s | energy=%.4f kWh carbon=%.4f kg "
                "accuracy=%.2f negawatt=%.2f zone=%s",
                result.task_id,
                "success" if result.success else "failed",
                result.energy_consumed,
                result.carbon_emitted,
                result.accuracy,
                result.negawatt_reward,
                result.carbon_zone,
            )
            if getattr(result, "errors", None):
                logger.warning("Task errors: %s", result.errors)

        if not args.once and exit_code == 0:
            logger.info("Agent running. Press Ctrl+C to stop...")
            await shutdown_event.wait()

    except SystemExit:
        raise
    except Exception as exc:                             # noqa: BLE001
        logger.exception("Fatal error: %s", exc)
        exit_code = 1
    finally:
        # --- Shutdown, each step isolated -------------------------------
        logger.info("Shutting down components...")
        for name, coro in (
            ("agent", agent.shutdown()),
            ("registry", registry.shutdown()),
        ):
            try:
                await coro
            except Exception as exc:                     # noqa: BLE001
                logger.warning("%s.shutdown failed: %s", name, exc)
        if exporter is not None and exporter_started:
            try:
                exporter.stop_http_server()
            except Exception as exc:                     # noqa: BLE001
                logger.warning(
                    "exporter.stop_http_server failed: %s", exc,
                )
        logger.info("Green Agent shutdown complete")

    return exit_code


# ---------------------------------------------------------------------------
# MODULE 6 — CLI entry point
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Green Agent v5.0.0")
    p.add_argument(
        "--mode", choices=["legacy", "unified", "compare"], default="unified",
    )
    p.add_argument(
        "--config", default="config/base/green_agent_config.yaml",
    )
    p.add_argument("--task", type=str, help="Task JSON file to execute")
    p.add_argument(
        "--once", action="store_true",
        help="Execute one task and exit (no wait for Ctrl+C)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Load and validate config then exit",
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument(
        "--seed", type=int, default=None,
        help="Seed for random / numpy for reproducibility",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(_parse_args())))
