#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_agent.py — Green Agent v5.0.0 main entry point (Enhanced v2.0)

Wires together GraphRegistry, HeliumMonitor, GraphMetricsExporter,
UnifiedGreenAgent, and (optionally) TemporalLogicMonitor. Runs one task
and then optionally waits for a shutdown signal so Prometheus can scrape
the metrics endpoint.

Enhanced in v2.0
----------------
- **Import namespace unified**: all imports use the ``src.`` prefix that
  the rest of the family uses (``src.enhancements.*``, ``INSTALLATION.md``).
  Prior version had three different import styles that could not coexist.
- **`UnifiedGreenAgent` fallback**: tries ``UnifiedGreenAgent`` first,
  falls back to ``UnifiedOrchestrator`` (the documented class name) if
  the alias is missing.
- **No `sys.path` hack**: assumes `pip install -e .` per
  `INSTALLATION.md §3.3`. Fails loudly with install instructions if the
  package is not importable.
- **`--once` / `--timeout N` flags**: prevents infinite hang in CI. The
  prior version blocked forever after executing one task.
- **`--no-enhancements` counterpart**: cannot force-disable if the
  config has them on.
- **`--version`, `--quiet`, `--verbose`, `--validate-config`,
  `--metrics-bind`** flags.
- **Cross-platform signal handlers**: `NotImplementedError` on Windows
  falls back to `signal.signal`.
- **`asyncio.get_running_loop()`** replaces the deprecated
  `get_event_loop()`.
- **Safe result access**: every field read via `getattr(result, X, None)`
  with ``n/a`` fallback. Prior version crashed on missing attributes like
  `negawatt_reward` or `carbon_zone`.
- **Secrets via `$GREEN_AGENT_SECRETS_DIR`** (per `SECURITY.md`), with
  env-var fallback and a warning.
- **Metrics bound to `127.0.0.1` by default** (matching the family's
  security posture). `0.0.0.0` requires explicit `--metrics-bind 0.0.0.0`.
- **Config merge**: `green_agent_config.example.yaml` provides defaults,
  user config overrides them. Prior version silently returned `{}` on
  missing file.
- **Temporal rules from config** (`enhancements.temporal_logic.rules`),
  defaulting to the prior hardcoded list only when no config is present.
- **`--mode` deprecated**: logs a warning if `legacy` or `compare` is
  passed, defaults to `unified`.
- **Cleanup is resilient**: each shutdown step is wrapped so
  `exporter.stop_http_server()` runs even if `agent.shutdown()` raises.
- **`sys.exit`** replaces `exit()`.
- **`--validate-config`** for CI config gating.

Run
---
    # Batch: run one task, exit
    python run_agent.py --once --task task.json

    # Serve: run one task, stay up for Prometheus scraping
    python run_agent.py

    # Demo defaults + no wait
    python run_agent.py --once

    # Validate config only
    python run_agent.py --validate-config

Exit codes
----------
    0   success
    1   task execution failure
    2   configuration or environment error
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
from pathlib import Path
from typing import Any, Optional

try:
    import yaml
except ImportError:  # pragma: no cover
    print(
        "FATAL: pyyaml is not installed.\n"
        "Install with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(2)


# =============================================================================
# Core imports — expect `pip install -e . --no-deps`
# =============================================================================
try:
    from src.core.graph_registry import GraphRegistry, GraphType
    from src.carbon.helium_monitor import HeliumMonitor
    from src.monitoring.graph_metrics_exporter import GraphMetricsExporter
except ImportError as exc:  # pragma: no cover - environment error path
    print(
        f"FATAL: could not import required Green Agent module: {exc}\n"
        "Install the package with:\n"
        "    pip install -e . --no-deps\n"
        "Or ensure the repo root is on PYTHONPATH.",
        file=sys.stderr,
    )
    sys.exit(2)

# UnifiedGreenAgent — accept both class names (documented vs. legacy).
_AGENT_IMPORT_ERROR: Optional[Exception] = None
try:
    from src.integration.unified_orchestrator import UnifiedGreenAgent  # type: ignore[attr-defined]
except (ImportError, AttributeError) as _exc:
    _AGENT_IMPORT_ERROR = _exc
    try:
        # Documented name (per INSTALLATION.md §A.2 and ARCHITECTURE_PROPOSAL)
        from src.integration.unified_orchestrator import UnifiedOrchestrator as UnifiedGreenAgent  # type: ignore[assignment]
    except ImportError as _exc2:  # pragma: no cover
        print(
            f"FATAL: could not import UnifiedGreenAgent or "
            f"UnifiedOrchestrator: {_exc2}\n"
            "Install the package with:\n"
            "    pip install -e . --no-deps",
            file=sys.stderr,
        )
        sys.exit(2)


# =============================================================================
# Optional enhancement imports — per-module isolation
# =============================================================================
_TEMPORAL_MONITOR_AVAILABLE = False
TemporalLogicMonitor: Any = None
try:  # pragma: no branch
    from src.enhancements.symbolic.temporal_logic_monitor import (  # type: ignore[import-not-found]
        TemporalLogicMonitor as _TemporalLogicMonitor,
    )
    TemporalLogicMonitor = _TemporalLogicMonitor
    _TEMPORAL_MONITOR_AVAILABLE = True
except ImportError:
    pass

# =============================================================================
# Constants
# =============================================================================
_AGENT_VERSION = "5.0.0"
_DEFAULT_CONFIG_PATH = "config/base/green_agent_config.yaml"
_DEFAULT_EXAMPLE_PATH = "config/base/green_agent_config.example.yaml"
_DEFAULT_METRICS_PORT = 8000
_DEFAULT_METRICS_BIND = "127.0.0.1"
_DEFAULT_MAX_EDGES_EXPORT = 100
_SECRETS_DIR_ENV = "GREEN_AGENT_SECRETS_DIR"

# Prior hardcoded defaults — now the fallback when config does not declare
# enhancements.temporal_logic.rules.
_DEFAULT_TEMPORAL_RULES: list[dict[str, Any]] = [
    {
        "name": "CarbonAlwaysLow",
        "field": "carbon",
        "op": "<",
        "value": 400,
        "mode": "ALWAYS",
        "window": 5,
    },
    {
        "name": "EventuallyDefer",
        "field": "action",
        "op": "==",
        "value": "defer",
        "mode": "EVENTUALLY",
        "window": 10,
    },
]

logger = logging.getLogger("run_agent")


# =============================================================================
# Helpers
# =============================================================================

async def _maybe_await(value: Any) -> Any:
    """Await ``value`` if awaitable; return it otherwise."""
    if inspect.isawaitable(value):
        return await value
    return value


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` over ``base`` (dicts merge, scalars replace)."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _load_config(path: str, example_path: str) -> dict[str, Any]:
    """
    Load and merge configuration.

    1. If ``example_path`` exists, treat it as the defaults tree.
    2. If ``path`` exists, deep-merge its contents on top of the defaults.
    3. Return the merged dict.

    Raises:
        RuntimeError: if ``path`` exists but cannot be parsed as YAML.
    """
    defaults: dict[str, Any] = {}
    example = Path(example_path)
    if example.is_file():
        try:
            loaded = yaml.safe_load(example.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                defaults = loaded
                logger.debug("Loaded defaults from %s", example)
            else:
                logger.warning("Example config %s is not a dict; ignoring.", example)
        except Exception as exc:
            logger.warning("Could not parse %s: %s", example, exc)

    user = Path(path)
    if not user.is_file():
        logger.warning(
            "Config not found: %s. Using defaults%s.",
            path,
            f" from {example}" if defaults else " (empty)",
        )
        return defaults

    try:
        loaded = yaml.safe_load(user.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Could not parse config {path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Config {path} must be a YAML mapping")
    return _deep_merge(defaults, loaded)


def _read_secret(name: str, fallback_env: Optional[str] = None) -> Optional[str]:
    """
    Read a secret.

    Priority:
      1. ``$GREEN_AGENT_SECRETS_DIR/<name>`` (per SECURITY.md).
      2. ``$<fallback_env>`` with a warning.
    """
    secrets_dir = os.environ.get(_SECRETS_DIR_ENV)
    if secrets_dir:
        path = Path(secrets_dir) / name
        if path.is_file():
            text = path.read_text(encoding="utf-8").strip()
            if text:
                return text
    if fallback_env:
        value = os.environ.get(fallback_env)
        if value:
            logger.warning(
                "Secret %r loaded from env var %r. Prefer $%s/%s.",
                name, fallback_env, _SECRETS_DIR_ENV, name,
            )
            return value
    return None


def _fmt_num(value: Any, spec: str = ".4f", suffix: str = "") -> str:
    """Format a numeric value with a specific format spec, or ``n/a``."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return f"{value:{spec}}{suffix}"
        except (TypeError, ValueError):
            return str(value)
    return "n/a"


def _validate_task(task: Any) -> None:
    """Reject tasks that lack an id or are not mappings."""
    if not isinstance(task, dict):
        raise ValueError("Task must be a JSON object")
    if "id" not in task:
        raise ValueError("Task must contain an 'id' field")
    if not isinstance(task["id"], (str, int)):
        raise ValueError("Task 'id' must be a string or integer")


def _setup_signal_handlers(loop: asyncio.AbstractEventLoop,
                            event: asyncio.Event) -> None:
    """
    Register SIGTERM/SIGINT handlers on the event loop.

    Falls back to ``signal.signal`` on platforms that raise
    ``NotImplementedError`` (Windows).
    """
    def _handler() -> None:
        logger.info("Shutdown signal received")
        event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handler)
        except NotImplementedError:
            try:
                signal.signal(sig, lambda _s, _f: _handler())
            except (ValueError, OSError) as exc:
                logger.debug("Could not register %s: %s", sig, exc)


def _print_result(result: Any) -> None:
    """Print task result fields via safe ``getattr`` with ``n/a`` fallbacks."""
    task_id = getattr(result, "task_id", "?")
    success = getattr(result, "success", None)
    energy = getattr(result, "energy_consumed", None)
    carbon = getattr(result, "carbon_emitted", None)
    accuracy = getattr(result, "accuracy", None)
    reward = getattr(result, "negawatt_reward", None)
    zone = getattr(result, "carbon_zone", None)
    errors = getattr(result, "errors", None) or []

    if success is True:
        status = "Success"
    elif success is False:
        status = "Failed"
    else:
        status = "Unknown"

    print(f"\n{'='*60}")
    print(f"Task {task_id}: {status}")
    print(f"   Energy:          {_fmt_num(energy, '.4f', ' kWh')}")
    print(f"   Carbon:          {_fmt_num(carbon, '.4f', ' kg CO2')}")
    print(f"   Accuracy:        {_fmt_num(accuracy, '.2f')}")
    print(f"   Negawatt Reward: {_fmt_num(reward, '.2f')}")
    print(f"   Carbon Zone:     {zone if zone is not None else 'n/a'}")
    if errors:
        print(f"   Errors:          {errors}")
    print(f"{'='*60}\n")


def _start_metrics(exporter: GraphMetricsExporter, port: int,
                    bind: str) -> None:
    """
    Start the Prometheus HTTP server on ``bind:port``.

    Falls back to ``0.0.0.0`` if the installed ``prometheus_client`` version
    does not accept the ``addr`` kwarg, with a warning.
    """
    try:
        exporter.start_http_server(port=port, addr=bind)
    except TypeError:
        # Older prometheus_client does not support `addr`.
        exporter.start_http_server(port=port)
        if bind != "0.0.0.0":
            logger.warning(
                "Metrics endpoint bound to 0.0.0.0; the installed "
                "prometheus_client does not support addr=%r. "
                "Upgrade prometheus_client or use a reverse proxy.",
                bind,
            )


# =============================================================================
# Main async flow
# =============================================================================

async def _run(args: argparse.Namespace) -> int:
    # --- Load config --------------------------------------------------------
    try:
        config = _load_config(args.config, args.example_config)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2

    # --- Apply enhancements override ---------------------------------------
    if args.enhancements is not None:
        config.setdefault("enhancements", {})["enabled"] = bool(args.enhancements)
        logger.info(
            "Enhancements %s via CLI",
            "enabled" if args.enhancements else "disabled",
        )
    enhancements_enabled = bool(config.get("enhancements", {}).get("enabled", False))
    logger.info("Enhancements: %s", "on" if enhancements_enabled else "off")

    if args.mode != "unified":
        logger.warning("--mode %r is deprecated; running unified.", args.mode)

    # --- Initialize components with ordered cleanup ------------------------
    registry = GraphRegistry()
    agent: Any = None
    exporter: Optional[GraphMetricsExporter] = None
    temporal_monitor: Any = None

    try:
        # Helium monitor ---------------------------------------------------
        helium_monitor: Optional[HeliumMonitor] = None
        helium_cfg = dict(config.get("helium", {}) or {})
        if helium_cfg.get("enabled", False):
            api_key = _read_secret(
                "helium_api_key",
                helium_cfg.get("api_key_env_var", "HELIUM_API_KEY"),
            )
            if api_key:
                helium_cfg["api_key"] = api_key
            try:
                helium_monitor = HeliumMonitor(
                    config=helium_cfg,
                    simulation_seed=helium_cfg.get("simulation_seed"),
                )
                if hasattr(registry, "register_helium_monitor"):
                    await _maybe_await(registry.register_helium_monitor(helium_monitor))
                logger.info("HeliumMonitor initialized and registered")
            except Exception as exc:
                logger.error("HeliumMonitor failed: %s", exc)
                logger.warning("Continuing without helium monitoring")

        # Agent ------------------------------------------------------------
        logger.info("Initializing UnifiedGreenAgent...")
        try:
            agent = UnifiedGreenAgent(config)
            await _maybe_await(agent.initialize())
        except Exception as exc:
            logger.error("Agent initialization failed: %s", exc, exc_info=True)
            return 2

        # Metrics exporter -------------------------------------------------
        monitoring_cfg = (config.get("monitoring", {}) or {}).get("prometheus", {}) or {}
        port = int(monitoring_cfg.get("port", _DEFAULT_METRICS_PORT))
        bind = args.metrics_bind or str(monitoring_cfg.get("bind", _DEFAULT_METRICS_BIND))
        max_edges = int(monitoring_cfg.get("max_edges_export", _DEFAULT_MAX_EDGES_EXPORT))

        exporter = GraphMetricsExporter(
            registry=registry,
            job_name="green_agent",
            max_edges_export=max_edges,
            helium_monitor=helium_monitor,
        )
        try:
            _start_metrics(exporter, port, bind)
        except OSError as exc:
            logger.error("Metrics server could not bind to %s:%d: %s", bind, port, exc)
            return 2
        logger.info("Metrics endpoint: http://%s:%d/metrics (bound to %s)", bind, port, bind)
        if bind == "0.0.0.0":
            logger.warning(
                "Metrics are exposed on all interfaces with no authentication. "
                "Place a reverse proxy in front or bind to 127.0.0.1."
            )

        # Temporal monitor -------------------------------------------------
        if _TEMPORAL_MONITOR_AVAILABLE and enhancements_enabled:
            temporal_monitor = TemporalLogicMonitor(history_size=20)
            rules = (
                (config.get("enhancements", {}) or {})
                .get("temporal_logic", {})
                .get("rules", _DEFAULT_TEMPORAL_RULES)
            )
            for rule in rules:
                try:
                    temporal_monitor.add_rule(**rule)
                except Exception as exc:
                    logger.warning("Temporal rule %r rejected: %s", rule.get("name"), exc)
            logger.info("Temporal monitor initialized with %d rules", len(rules))
        elif enhancements_enabled and not _TEMPORAL_MONITOR_AVAILABLE:
            logger.warning("Temporal logic monitor not available; skipping")

        # Task -------------------------------------------------------------
        if args.task:
            try:
                with open(args.task, "r", encoding="utf-8") as f:
                    task = json.load(f)
            except (OSError, json.JSONDecodeError) as exc:
                logger.error("Could not read task file %s: %s", args.task, exc)
                return 2
        else:
            task = {
                "id": "demo_001",
                "type": "ml_inference",
                "priority": 5,
                "deferrable": True,
                "model": "llama-7b",
                "input_size_mb": 128,
            }
        try:
            _validate_task(task)
        except ValueError as exc:
            logger.error("Invalid task: %s", exc)
            return 2

        # Execute ----------------------------------------------------------
        logger.info("Executing task: %s", task["id"])
        try:
            result = await _maybe_await(agent.execute_task(task))
        except Exception as exc:
            logger.error("Task execution failed: %s", exc, exc_info=True)
            return 1
        _print_result(result)

        # Temporal observation --------------------------------------------
        if temporal_monitor is not None:
            # carbon_intensity_g_per_kwh is grid-level; carbon_emitted is
            # per-task kg. The default temporal rule uses grid intensity.
            carbon_value = getattr(result, "carbon_intensity_g_per_kwh", None)
            if carbon_value is None:
                carbon_value = float(
                    (config.get("carbon_context", {}) or {})
                    .get("grid_intensity_g_kwh", 400.0)
                )
            action = getattr(result, "action", "execute")
            temporal_monitor.add_observation(
                {"carbon": carbon_value, "action": action}, step=1,
            )
            violations = temporal_monitor.evaluate_rules()
            if violations:
                logger.warning("Temporal rule violations: %s", violations)
            else:
                logger.info("Temporal rules satisfied")

        # Shutdown wait ----------------------------------------------------
        if args.once or args.timeout == 0:
            logger.info("Exiting after one task (--once / --timeout 0)")
            return 0

        loop = asyncio.get_running_loop()
        shutdown_event = asyncio.Event()
        _setup_signal_handlers(loop, shutdown_event)

        logger.info("Agent running. Ctrl+C to stop.")
        if args.timeout is not None and args.timeout > 0:
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=args.timeout)
            except asyncio.TimeoutError:
                logger.info("Timeout (%ss) reached; shutting down.", args.timeout)
        else:
            await shutdown_event.wait()

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130

    except Exception as exc:  # pragma: no cover - unexpected
        logger.error("Unexpected error: %s", exc, exc_info=True)
        return 1

    finally:
        # Cleanup in reverse order — each step isolated so a failure in one
        # does not skip the rest. Prior version could leave the metrics
        # port open if `agent.shutdown()` raised.
        if agent is not None:
            try:
                await _maybe_await(agent.shutdown())
                logger.info("Agent shut down cleanly")
            except Exception as exc:
                logger.error("Agent shutdown failed: %s", exc)

        if exporter is not None:
            try:
                exporter.stop_http_server()
                logger.info("Metrics server stopped")
            except Exception as exc:
                logger.error("Exporter stop failed: %s", exc)

        if hasattr(registry, "shutdown"):
            try:
                await _maybe_await(registry.shutdown())
                logger.info("Registry shut down cleanly")
            except Exception as exc:
                logger.error("Registry shutdown failed: %s", exc)


# =============================================================================
# Sync main
# =============================================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_agent",
        description=f"Green Agent v{_AGENT_VERSION}",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {_AGENT_VERSION}",
    )
    parser.add_argument(
        "--mode", choices=["unified", "legacy", "compare"], default="unified",
        help="Execution mode. 'legacy' and 'compare' are deprecated.",
    )
    parser.add_argument(
        "--config", default=_DEFAULT_CONFIG_PATH,
        help=f"Config path (default: {_DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--example-config", default=_DEFAULT_EXAMPLE_PATH,
        help=f"Defaults from example config (default: {_DEFAULT_EXAMPLE_PATH})",
    )
    parser.add_argument(
        "--task", type=str, default=None,
        help="Task JSON file to execute (default: built-in demo task)",
    )
    parser.add_argument(
        "--enhancements", dest="enhancements",
        action="store_true", default=None,
        help="Force-enable enhancements (overrides config)",
    )
    parser.add_argument(
        "--no-enhancements", dest="enhancements",
        action="store_false",
        help="Force-disable enhancements (overrides config)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run one task and exit immediately (no shutdown wait)",
    )
    parser.add_argument(
        "--timeout", type=float, default=None,
        help="Max seconds to wait for shutdown after task (0 = exit immediately)",
    )
    parser.add_argument(
        "--metrics-bind", default=None,
        help=f"Metrics bind address (default: {_DEFAULT_METRICS_BIND})",
    )
    parser.add_argument(
        "--validate-config", action="store_true",
        help="Validate config and exit without running any task",
    )
    parser.add_argument("--quiet", action="store_true", help="Only log warnings and errors")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    level = (
        logging.DEBUG if args.verbose
        else logging.WARNING if args.quiet
        else logging.INFO
    )
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # --validate-config short-circuits before running anything.
    if args.validate_config:
        try:
            _load_config(args.config, args.example_config)
        except RuntimeError as exc:
            logger.error("Config validation failed: %s", exc)
            return 2
        logger.info("Config is valid: %s", args.config)
        return 0

    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
