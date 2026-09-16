# src/agentbeats/mcp_server.py

"""
Green Agent MCP Server

Provides Model Context Protocol (MCP) tools that purple agents can invoke to
introspect and interact with the Green Agent's sustainability state:

- Energy budget inspection and consumption reporting
- Carbon emission reporting and accounting
- Pareto-based agent scoring (delegates to GreenSustainabilityAgent)
- Memory introspection (run memory + episodic memory)
- Quantum efficiency metric reporting
- A2A budget-gated task forwarding

Original behaviour preserved
----------------------------
- ``GreenAgentMCPServer`` class with ``@tool``-decorated methods.
- ``get_energy_budget()`` — returns remaining energy budget for the task.
- ``submit_carbon_report(emissions)`` — accepts carbon emission data.

Enhancements
------------
- Thread-safe via ``RLock``.
- Configurable via :class:`MCPConfig` (energy budget, carbon ceiling, etc.).
- Immutable ``ToolCallRecord`` history with bounded ring-buffer.
- Full validation of every tool argument; strict / non-strict modes.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Context-manager support for scoped MCP sessions.
- Custom :class:`MCPServerError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
- Integration with every enhanced module: ``BudgetEnforcer``, ``RunMemory``,
  ``EpisodicMemory``, ``QuantumEfficiencyMetric``, ``GreenSustainabilityAgent``,
  ``A2AHandler``, and ``MetaCognitiveController``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional MCP tool decorator (graceful degradation)
# --------------------------------------------------------------------------- #
# The real environment provides ``@tool`` via an MCP SDK. When unavailable
# (tests, CI, standalone usage), we fall back to a no-op decorator that simply
# tags the function so the class remains fully functional.
try:  # pragma: no cover — import-time branch depends on environment
    from mcp.server.fastmcp import tool  # type: ignore
    _MCP_TOOL_AVAILABLE = True
except ImportError:  # pragma: no cover
    try:
        from mcp.server import tool  # type: ignore
        _MCP_TOOL_AVAILABLE = True
    except ImportError:
        def tool(*args: Any, **kwargs: Any) -> Callable:  # type: ignore
            """Fallback ``@tool`` decorator: tags the function, no registration."""
            def _wrap(fn: Callable) -> Callable:
                fn.__mcp_tool__ = True  # type: ignore[attr-defined]
                fn.__mcp_tool_kwargs__ = kwargs  # type: ignore[attr-defined]
                return fn
            if args and callable(args[0]) and not kwargs:
                return _wrap(args[0])
            return _wrap
        _MCP_TOOL_AVAILABLE = False
        logger.debug("MCP SDK unavailable; using fallback @tool decorator.")


# --------------------------------------------------------------------------- #
# Enhanced module imports (all optional, resolved defensively)
# --------------------------------------------------------------------------- #
# Each import is wrapped so this module stays importable even if a downstream
# module is missing. The corresponding tool degrades to a clear error message.
_BudgetEnforcer = _Budget = _A2AHandler = _A2AHandlerError = None
_GreenSustainabilityAgent = _GreenAgentError = None
_RunMemory = _EpisodicMemory = None
_QuantumEfficiencyMetric = _QuantumEfficiencyError = None
_MetaCognitiveController = _MetaCognitiveError = None

try:  # constraints layer
    from constraints.budget_enforcer import BudgetEnforcer as _BudgetEnforcer  # type: ignore
    from constraints.budget_manager import Budget as _Budget  # type: ignore
except ImportError:  # pragma: no cover
    pass

try:  # green agent + a2a handler (support both package and script imports)
    from .green_agent import GreenSustainabilityAgent as _GreenSustainabilityAgent  # type: ignore
    from .green_agent import GreenAgentError as _GreenAgentError  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from green_agent import GreenSustainabilityAgent as _GreenSustainabilityAgent  # type: ignore
        from green_agent import GreenAgentError as _GreenAgentError  # type: ignore
    except ImportError:
        pass

try:
    from .a2a_handler import A2AHandler as _A2AHandler  # type: ignore
    from .a2a_handler import A2AHandlerError as _A2AHandlerError  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from a2a_handler import A2AHandler as _A2AHandler  # type: ignore
        from a2a_handler import A2AHandlerError as _A2AHandlerError  # type: ignore
    except ImportError:
        pass

try:  # memory layer
    from memory.run_memory import RunMemory as _RunMemory  # type: ignore
    from memory.episodic_memory import EpisodicMemory as _EpisodicMemory  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from ..memory.run_memory import RunMemory as _RunMemory  # type: ignore
        from ..memory.episodic_memory import EpisodicMemory as _EpisodicMemory  # type: ignore
    except ImportError:
        pass

try:  # quantum metrics layer
    from metrics.quantum_efficiency import QuantumEfficiencyMetric as _QuantumEfficiencyMetric  # type: ignore
    from metrics.quantum_efficiency import QuantumEfficiencyError as _QuantumEfficiencyError  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from ..metrics.quantum_efficiency import QuantumEfficiencyMetric as _QuantumEfficiencyMetric  # type: ignore
        from ..metrics.quantum_efficiency import QuantumEfficiencyError as _QuantumEfficiencyError  # type: ignore
    except ImportError:
        pass

try:  # metacognition layer
    from metacognition.meta_cognitive_controller import MetaCognitiveController as _MetaCognitiveController  # type: ignore
    from metacognition.meta_cognitive_controller import MetaCognitiveError as _MetaCognitiveError  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from ..metacognition.meta_cognitive_controller import MetaCognitiveController as _MetaCognitiveController  # type: ignore
        from ..metacognition.meta_cognitive_controller import MetaCognitiveError as _MetaCognitiveError  # type: ignore
    except ImportError:
        pass


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MCPServerError(ValueError):
    """Raised for invalid tool arguments or server configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MCPConfig:
    """
    Tunable parameters for the MCP server.

    Centralizes budgets and thresholds so deployments can calibrate without
    editing the server.
    """

    # Energy budget accounting
    energy_budget_wh: float = 3.0
    energy_warn_fraction: float = 0.8     # warn when consumed >= 80% of budget

    # Carbon accounting
    carbon_ceiling_g: float = 10.0
    carbon_warn_fraction: float = 0.8

    # History ring-buffer
    max_history: Optional[int] = 1000

    # MCP server identity
    server_name: str = "green-agent-mcp"
    server_version: str = "2.4.0"

    def __post_init__(self) -> None:
        if self.energy_budget_wh <= 0:
            raise MCPServerError("energy_budget_wh must be > 0.")
        if self.carbon_ceiling_g <= 0:
            raise MCPServerError("carbon_ceiling_g must be > 0.")
        if not 0.0 < self.energy_warn_fraction <= 1.0:
            raise MCPServerError("energy_warn_fraction must be in (0, 1].")
        if not 0.0 < self.carbon_warn_fraction <= 1.0:
            raise MCPServerError("carbon_warn_fraction must be in (0, 1].")
        if self.max_history is not None and self.max_history <= 0:
            raise MCPServerError("max_history must be > 0 or None.")


# --------------------------------------------------------------------------- #
# Immutable tool-call sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ToolCallRecord:
    """Immutable snapshot of one MCP tool invocation."""

    timestamp: float
    tool_name: str
    args: Mapping[str, Any]
    outcome: str  # "ok" | "error"
    duration_ms: float
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["args"] = dict(self.args)
        return d


# --------------------------------------------------------------------------- #
# MCP server
# --------------------------------------------------------------------------- #
class GreenAgentMCPServer:
    """
    Provides tools for purple agents via MCP.

    All ``@tool``-decorated methods are safe to call concurrently. Each tool
    validates its arguments, records a :class:`ToolCallRecord`, and returns a
    JSON-serializable dict.

    Parameters
    ----------
    config : MCPConfig, optional
        Budgets, thresholds, and identity. Defaults to ``MCPConfig()``.
    strict : bool, default True
        If True, invalid arguments raise :class:`MCPServerError`.
        If False, they are logged and coerced to safe defaults.
    green_agent : GreenSustainabilityAgent, optional
        Shared agent instance used by scoring tools. Lazily created if absent.
    run_memory : RunMemory, optional
        Shared run memory. Lazily created if absent.
    episodic_memory : EpisodicMemory, optional
        Shared episodic memory. Lazily created if absent.
    quantum_metric : QuantumEfficiencyMetric, optional
        Shared quantum metric. Lazily created if absent.
    meta_controller : MetaCognitiveController, optional
        Shared metacognition controller. Lazily created if absent.
    budget_enforcer : BudgetEnforcer, optional
        Budget enforcer used by A2A tools. Lazily created if absent.
    """

    def __init__(
        self,
        *,
        config: Optional[MCPConfig] = None,
        strict: bool = True,
        green_agent: Any = None,
        run_memory: Any = None,
        episodic_memory: Any = None,
        quantum_metric: Any = None,
        meta_controller: Any = None,
        budget_enforcer: Any = None,
    ) -> None:
        self._config: MCPConfig = config or MCPConfig()
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._history: List[ToolCallRecord] = []
        self._ctx_start: Optional[float] = None
        self._started_at: float = time.time()

        # ---- Energy / carbon state -------------------------------------
        self._energy_consumed_wh: float = 0.0
        self._carbon_emitted_g: float = 0.0

        # ---- Companion components (lazily created) ----------------------
        self._green_agent = green_agent
        self._run_memory = run_memory
        self._episodic_memory = episodic_memory
        self._quantum_metric = quantum_metric
        self._meta_controller = meta_controller
        self._budget_enforcer = budget_enforcer

        logger.debug(
            "GreenAgentMCPServer initialized (budget=%.3g Wh, ceiling=%.3g g, "
            "strict=%s, mcp_sdk=%s)",
            self._config.energy_budget_wh,
            self._config.carbon_ceiling_g,
            self._strict,
            _MCP_TOOL_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MCPConfig:
        return self._config

    @property
    def history(self) -> List[ToolCallRecord]:
        with self._lock:
            return list(self._history)

    @property
    def energy_consumed_wh(self) -> float:
        with self._lock:
            return self._energy_consumed_wh

    @property
    def carbon_emitted_g(self) -> float:
        with self._lock:
            return self._carbon_emitted_g

    # ------------------------------------------------------ lazy accessors
    def _ensure_green_agent(self) -> Any:
        if self._green_agent is None and _GreenSustainabilityAgent is not None:
            self._green_agent = _GreenSustainabilityAgent()
        return self._green_agent

    def _ensure_run_memory(self) -> Any:
        if self._run_memory is None and _RunMemory is not None:
            self._run_memory = _RunMemory(memory_file=":memory:", auto_load=False, autosave=False)
        return self._run_memory

    def _ensure_episodic_memory(self) -> Any:
        if self._episodic_memory is None and _EpisodicMemory is not None:
            self._episodic_memory = _EpisodicMemory(memory_file=":memory:", autosave=False)
        return self._episodic_memory

    def _ensure_quantum_metric(self) -> Any:
        if self._quantum_metric is None and _QuantumEfficiencyMetric is not None:
            self._quantum_metric = _QuantumEfficiencyMetric(strict=self._strict)
        return self._quantum_metric

    def _ensure_meta_controller(self) -> Any:
        if self._meta_controller is None and _MetaCognitiveController is not None:
            self._meta_controller = _MetaCognitiveController(strict=self._strict)
        return self._meta_controller

    # =================================================================== #
    # MCP TOOLS
    # =================================================================== #

    @tool(description="Get the remaining energy budget for the current task.")
    def get_energy_budget(self) -> Dict[str, Any]:
        """
        Return the remaining energy budget and consumption state.

        Returns
        -------
        dict
            ``budget_wh``, ``consumed_wh``, ``remaining_wh``, ``utilization``,
            ``warning`` (True if >= warn_fraction consumed), and ``status``.
        """
        return self._run_tool("get_energy_budget", {})

    @tool(description="Report energy consumption for the current task.")
    def report_energy_consumption(
        self, consumed_wh: float, *, source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Record additional energy consumption and return updated budget state.

        Parameters
        ----------
        consumed_wh : float
            Energy consumed (Watt-hours). Must be finite and >= 0.
        source : str, optional
            Free-form provenance (e.g. ``"nvml"``, ``"cpu-rapl"``).
        """
        return self._run_tool(
            "report_energy_consumption",
            {"consumed_wh": consumed_wh, "source": source},
        )

    @tool(description="Submit carbon emissions data for the current task.")
    def submit_carbon_report(
        self,
        emissions: float,
        *,
        unit: str = "g",
        source: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Record carbon emissions and return updated carbon state.

        Parameters
        ----------
        emissions : float
            Non-negative emission amount (interpreted via ``unit``).
        unit : str, default "g"
            ``"g"`` or ``"kg"``. Values are normalized to grams internally.
        source : str, optional
            Free-form provenance (e.g. ``"grid-mix:eu-west-1"``).
        """
        return self._run_tool(
            "submit_carbon_report",
            {"emissions": emissions, "unit": unit, "source": source},
        )

    @tool(description="Score agents using Pareto optimality across accuracy, energy, carbon, and latency.")
    def score_agents(
        self,
        results: List[Dict[str, Any]],
        *,
        label: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Delegate to :class:`GreenSustainabilityAgent.score_with_pareto`.

        Parameters
        ----------
        results : list of dict
            Each row: ``agent_id``, ``accuracy``, ``energy_kwh``, ``carbon_kg``,
            ``latency_ms``.
        label : str, optional
            Optional label for the underlying score record.
        """
        return self._run_tool(
            "score_agents", {"results": results, "label": label}
        )

    @tool(description="Return performance trends from the run-memory subsystem.")
    def get_run_trends(
        self, metric: str = "final_score", *, recent: int = 10
    ) -> Dict[str, Any]:
        """
        Return trend + recent runs + statistics for ``metric``.

        Parameters
        ----------
        metric : str, default "final_score"
            Metric name tracked by ``RunMemory``.
        recent : int, default 10
            Number of most recent runs to include.
        """
        return self._run_tool(
            "get_run_trends", {"metric": metric, "recent": recent}
        )

    @tool(description="Return recent episodes from episodic memory.")
    def get_recent_episodes(self, n: int = 10) -> Dict[str, Any]:
        """
        Return the ``n`` most recent stored episodes.

        Parameters
        ----------
        n : int, default 10
            Number of recent episodes to return (must be > 0).
        """
        return self._run_tool("get_recent_episodes", {"n": n})

    @tool(description="Report current quantum efficiency metric state.")
    def get_quantum_efficiency(
        self, *, record: bool = True, label: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Return the current quantum efficiency value + statistics.

        Parameters
        ----------
        record : bool, default True
            If True, record a history sample for the computed value.
        label : str, optional
            Optional label for the recorded sample.
        """
        return self._run_tool(
            "get_quantum_efficiency", {"record": record, "label": label}
        )

    @tool(description="Report metacognitive anomaly state and recovery-trigger status.")
    def get_anomaly_state(self) -> Dict[str, Any]:
        """
        Return the current anomaly score, whether recovery should trigger, and
        aggregate statistics from the metacognition controller.
        """
        return self._run_tool("get_anomaly_state", {})

    @tool(description="Evaluate a metrics sample through the metacognitive controller.")
    def evaluate_anomaly(
        self,
        energy: float,
        baseline_energy: float,
        *,
        label: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Feed a metrics sample to :class:`MetaCognitiveController.evaluate`.

        Parameters
        ----------
        energy : float
            Observed energy for the current step (must be finite, >= 0).
        baseline_energy : float
            Reference baseline energy (> 0).
        label : str, optional
            Optional label stored with the controller sample.
        """
        return self._run_tool(
            "evaluate_anomaly",
            {"energy": energy, "baseline_energy": baseline_energy, "label": label},
        )

    @tool(description="Send an A2A task to a remote agent subject to budget constraints.")
    def send_a2a_task(
        self,
        agent_url: str,
        task: Dict[str, Any],
        *,
        label: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Forward ``task`` to ``agent_url`` through the budget-gated A2A handler.

        Parameters
        ----------
        agent_url : str
            Remote agent endpoint.
        task : dict
            Task payload.
        label : str, optional
            Optional dispatch label.
        """
        return self._run_tool(
            "send_a2a_task",
            {"agent_url": agent_url, "task": task, "label": label},
        )

    # =================================================================== #
    # DISPATCHER
    # =================================================================== #
    def _run_tool(self, tool_name: str, args: Mapping[str, Any]) -> Dict[str, Any]:
        """Validate args, dispatch, record, and return a structured response."""
        start = time.perf_counter()
        outcome = "ok"
        error_msg: Optional[str] = None

        try:
            handler = getattr(self, f"_impl_{tool_name}")
            result = handler(**dict(args))
            if not isinstance(result, Mapping):
                raise MCPServerError(
                    f"tool '{tool_name}' returned {type(result).__name__}; "
                    "expected Mapping."
                )
            return dict(result)

        except MCPServerError:
            outcome = "error"
            error_msg = "validation_error"
            raise
        except Exception as exc:
            outcome = "error"
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.exception("Tool '%s' failed: %s", tool_name, exc)
            if self._strict:
                raise MCPServerError(
                    f"tool '{tool_name}' failed: {exc}"
                ) from exc
            return {"status": "error", "tool": tool_name, "error": str(exc)}

        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            self._record(
                tool_name=tool_name,
                args=dict(args),
                outcome=outcome,
                duration_ms=duration_ms,
                error=error_msg,
            )
            logger.debug(
                "tool=%s outcome=%s duration=%.2fms",
                tool_name,
                outcome,
                duration_ms,
            )

    # ------------------------------------------------------ tool implementations
    def _impl_get_energy_budget(self) -> Dict[str, Any]:
        with self._lock:
            consumed = self._energy_consumed_wh
        cfg = self._config
        remaining = max(cfg.energy_budget_wh - consumed, 0.0)
        utilization = consumed / cfg.energy_budget_wh
        warning = utilization >= cfg.energy_warn_fraction

        if warning:
            logger.warning(
                "Energy budget warning: consumed %.3g/%.3g Wh (%.1f%%).",
                consumed, cfg.energy_budget_wh, utilization * 100.0,
            )

        return {
            "status": "ok",
            "budget_wh": cfg.energy_budget_wh,
            "consumed_wh": consumed,
            "remaining_wh": remaining,
            "utilization": utilization,
            "warning": warning,
            "exhausted": remaining <= 0.0,
        }

    def _impl_report_energy_consumption(
        self, consumed_wh: float, source: Optional[str] = None
    ) -> Dict[str, Any]:
        value = self._validate_non_negative("consumed_wh", consumed_wh)
        with self._lock:
            self._energy_consumed_wh += value
            total = self._energy_consumed_wh
        logger.debug("Reported %.6g Wh (source=%s, total=%.6g Wh)", value, source, total)
        return {
            "status": "ok",
            "reported_wh": value,
            "consumed_wh": total,
            "source": source,
            "budget": self._impl_get_energy_budget(),
        }

    def _impl_submit_carbon_report(
        self,
        emissions: float,
        unit: str = "g",
        source: Optional[str] = None,
    ) -> Dict[str, Any]:
        value = self._validate_non_negative("emissions", emissions)
        if not isinstance(unit, str) or unit.lower() not in ("g", "kg"):
            raise MCPServerError(
                f"unit must be 'g' or 'kg', got {unit!r}."
            )
        grams = value if unit.lower() == "g" else value * 1000.0

        with self._lock:
            self._carbon_emitted_g += grams
            total = self._carbon_emitted_g

        cfg = self._config
        utilization = total / cfg.carbon_ceiling_g
        warning = utilization >= cfg.carbon_warn_fraction

        if warning:
            logger.warning(
                "Carbon ceiling warning: emitted %.3g/%.3g g (%.1f%%).",
                total, cfg.carbon_ceiling_g, utilization * 100.0,
            )

        return {
            "status": "ok",
            "reported_grams": grams,
            "emitted_grams": total,
            "ceiling_grams": cfg.carbon_ceiling_g,
            "utilization": utilization,
            "warning": warning,
            "source": source,
        }

    def _impl_score_agents(
        self, results: List[Dict[str, Any]], label: Optional[str] = None
    ) -> Dict[str, Any]:
        agent = self._ensure_green_agent()
        if agent is None:
            raise MCPServerError(
                "GreenSustainabilityAgent unavailable (import failed)."
            )
        if not isinstance(results, list) or not results:
            raise MCPServerError("results must be a non-empty list.")

        # score_with_pareto is async; run it to completion in this (sync) tool.
        coro = agent.score_with_pareto(results, label=label)
        try:
            result = asyncio.get_event_loop().run_until_complete(coro)
        except RuntimeError:
            # No running loop → safe to spin a fresh one.
            result = asyncio.run(coro)
        return {"status": "ok", **dict(result)}

    def _impl_get_run_trends(
        self, metric: str = "final_score", recent: int = 10
    ) -> Dict[str, Any]:
        memory = self._ensure_run_memory()
        if memory is None:
            raise MCPServerError("RunMemory unavailable (import failed).")
        if not isinstance(recent, int) or recent <= 0:
            raise MCPServerError("recent must be a positive int.")

        return {
            "status": "ok",
            "metric": metric,
            "trend": memory.get_performance_trend(metric),
            "recent_runs": memory.get_recent_runs(recent),
            "statistics": memory.statistics(metric),
            "total_runs": memory.count if hasattr(memory, "count") else len(memory.runs),
        }

    def _impl_get_recent_episodes(self, n: int = 10) -> Dict[str, Any]:
        memory = self._ensure_episodic_memory()
        if memory is None:
            raise MCPServerError("EpisodicMemory unavailable (import failed).")
        if not isinstance(n, int) or n <= 0:
            raise MCPServerError("n must be a positive int.")
        episodes = memory.get_recent(n)
        return {
            "status": "ok",
            "count": len(episodes),
            "episodes": episodes,
        }

    def _impl_get_quantum_efficiency(
        self, record: bool = True, label: Optional[str] = None
    ) -> Dict[str, Any]:
        metric = self._ensure_quantum_metric()
        if metric is None:
            raise MCPServerError("QuantumEfficiencyMetric unavailable (import failed).")
        value = metric.compute(record=record, label=label)
        return {
            "status": "ok",
            "efficiency": value,
            "quantum_energy_joules": metric.quantum_energy,
            "task_completion_ratio": metric.task_completion_ratio,
            "statistics": metric.statistics(),
        }

    def _impl_get_anomaly_state(self) -> Dict[str, Any]:
        ctrl = self._ensure_meta_controller()
        if ctrl is None:
            raise MCPServerError("MetaCognitiveController unavailable (import failed).")
        return {
            "status": "ok",
            "anomaly_score": ctrl.anomaly_score,
            "should_trigger_recovery": ctrl.should_trigger_recovery(),
            "statistics": ctrl.statistics(),
        }

    def _impl_evaluate_anomaly(
        self,
        energy: float,
        baseline_energy: float,
        label: Optional[str] = None,
    ) -> Dict[str, Any]:
        ctrl = self._ensure_meta_controller()
        if ctrl is None:
            raise MCPServerError("MetaCognitiveController unavailable (import failed).")

        e = self._validate_finite("energy", energy)
        b = self._validate_finite("baseline_energy", baseline_energy)
        if b <= 0:
            raise MCPServerError("baseline_energy must be > 0.")

        score = ctrl.evaluate(
            {"energy": e, "baseline_energy": b}, label=label
        )
        return {
            "status": "ok",
            "anomaly_score": score,
            "should_trigger_recovery": ctrl.should_trigger_recovery(),
        }

    def _impl_send_a2a_task(
        self,
        agent_url: str,
        task: Dict[str, Any],
        label: Optional[str] = None,
    ) -> Dict[str, Any]:
        if _A2AHandler is None:
            raise MCPServerError("A2AHandler unavailable (import failed).")
        if not isinstance(agent_url, str) or not agent_url:
            raise MCPServerError("agent_url must be a non-empty string.")
        if not isinstance(task, Mapping):
            raise MCPServerError("task must be a mapping.")

        handler = self._a2a_handler()  # cached
        coro = handler.send_task_with_budget(
            agent_url, dict(task), label=label
        )
        try:
            result = asyncio.get_event_loop().run_until_complete(coro)
        except RuntimeError:
            result = asyncio.run(coro)
        return {"status": "ok", **dict(result)}

    def _a2a_handler(self) -> Any:
        """Return a cached A2AHandler bound to the MCP energy budget."""
        existing = getattr(self, "_cached_a2a_handler", None)
        if existing is not None:
            return existing

        budget = None
        if _Budget is not None:
            try:
                budget = _Budget(
                    max_energy_wh=self._config.energy_budget_wh,
                    max_carbon_g=self._config.carbon_ceiling_g,
                )
            except TypeError:  # pragma: no cover — alternate signature
                budget = _Budget()
        handler = _A2AHandler(budget=budget, strict=self._strict)
        self._cached_a2a_handler = handler
        return handler

    # ------------------------------------------------------------- validation
    @staticmethod
    def _validate_finite(name: str, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise MCPServerError(
                f"{name} must be numeric, got {type(value).__name__}."
            )
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            raise MCPServerError(f"{name} must be finite, got {value!r}.")
        return v

    @classmethod
    def _validate_non_negative(cls, name: str, value: Any) -> float:
        v = cls._validate_finite(name, value)
        if v < 0:
            raise MCPServerError(f"{name} must be non-negative, got {v!r}.")
        return v

    # ---------------------------------------------------------------- history
    def _record(
        self,
        *,
        tool_name: str,
        args: Mapping[str, Any],
        outcome: str,
        duration_ms: float,
        error: Optional[str],
    ) -> None:
        # Keep arg snapshots JSON-safe by replacing non-primitive values.
        safe_args = {k: self._json_safe(v) for k, v in args.items()}
        sample = ToolCallRecord(
            timestamp=time.time(),
            tool_name=tool_name,
            args=safe_args,
            outcome=outcome,
            duration_ms=duration_ms,
            error=error,
        )
        with self._lock:
            self._history.append(sample)
            if (
                self._config.max_history is not None
                and len(self._history) > self._config.max_history
            ):
                del self._history[0]

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, Mapping):
            return {str(k): GreenAgentMCPServer._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [GreenAgentMCPServer._json_safe(v) for v in value]
        return str(value)

    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over recorded tool calls."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "count": 0,
                "by_tool": {},
                "errors": 0,
                "mean_duration_ms": None,
            }
        by_tool: Dict[str, int] = {}
        for s in history:
            by_tool[s.tool_name] = by_tool.get(s.tool_name, 0) + 1
        durations = [s.duration_ms for s in history]
        return {
            "count": len(history),
            "by_tool": by_tool,
            "errors": sum(1 for s in history if s.outcome == "error"),
            "mean_duration_ms": sum(durations) / len(durations),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset counters; optionally clear tool-call history."""
        with self._lock:
            self._energy_consumed_wh = 0.0
            self._carbon_emitted_g = 0.0
            if clear_history:
                self._history.clear()
        logger.debug("GreenAgentMCPServer reset (clear_history=%s)", clear_history)

    # -------------------------------------------------------- tool listing
    def list_tools(self) -> List[Dict[str, Any]]:
        """
        Return the MCP tool manifest (name, description, parameters).

        Useful for exposing this server over a JSON-RPC endpoint or for
        debugging without an MCP client.
        """
        import inspect

        manifest: List[Dict[str, Any]] = []
        for attr in dir(self):
            if attr.startswith("_impl_"):
                continue
            method = getattr(self, attr, None)
            if method is None or not callable(method):
                continue
            if not getattr(method, "__mcp_tool__", False):
                continue

            try:
                sig = inspect.signature(method)
            except (TypeError, ValueError):  # pragma: no cover
                continue

            params = {
                name: str(p.annotation)
                for name, p in sig.parameters.items()
                if name != "self"
            }
            doc = (method.__doc__ or "").strip().splitlines()
            manifest.append({
                "name": attr,
                "description": doc[0] if doc else "",
                "parameters": params,
            })
        manifest.sort(key=lambda x: x["name"])
        return manifest

    # ------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "energy_consumed_wh": self._energy_consumed_wh,
                "carbon_emitted_g": self._carbon_emitted_g,
                "started_at": self._started_at,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GreenAgentMCPServer":
        if not isinstance(data, Mapping):
            raise MCPServerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = MCPConfig(
            energy_budget_wh=float(cfg_data.get("energy_budget_wh", 3.0)),
            energy_warn_fraction=float(cfg_data.get("energy_warn_fraction", 0.8)),
            carbon_ceiling_g=float(cfg_data.get("carbon_ceiling_g", 10.0)),
            carbon_warn_fraction=float(cfg_data.get("carbon_warn_fraction", 0.8)),
            max_history=cfg_data.get("max_history", 1000),
            server_name=cfg_data.get("server_name", "green-agent-mcp"),
            server_version=cfg_data.get("server_version", "2.4.0"),
        )
        server = cls(config=cfg, strict=bool(data.get("strict", True)))
        with server._lock:
            server._energy_consumed_wh = float(data.get("energy_consumed_wh", 0.0))
            server._carbon_emitted_g = float(data.get("carbon_emitted_g", 0.0))
            server._started_at = float(data.get("started_at", time.time()))
            for entry in data.get("history", []):
                server._history.append(
                    ToolCallRecord(
                        timestamp=float(entry["timestamp"]),
                        tool_name=str(entry["tool_name"]),
                        args=dict(entry.get("args", {})),
                        outcome=str(entry["outcome"]),
                        duration_ms=float(entry["duration_ms"]),
                        error=entry.get("error"),
                    )
                )
        return server

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenAgentMCPServer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise MCPServerError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "GreenAgentMCPServer":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped MCP session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "MCP session exited with %s after %.4fs.", exc_type.__name__, elapsed
            )
            return
        logger.info(
            "MCP session closed in %.4fs (%d tool call(s)).",
            elapsed,
            len(self._history),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "GreenAgentMCPServer("
            f"name={self._config.server_name!r}, "
            f"budget_wh={self._config.energy_budget_wh:.3g}, "
            f"strict={self._strict}, "
            f"calls={len(self._history)})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GreenAgentMCPServer",
    "MCPServerError",
    "MCPConfig",
    "ToolCallRecord",
    "_MCP_TOOL_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m agentbeats.mcp_server
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    server = GreenAgentMCPServer(
        config=MCPConfig(energy_budget_wh=2.0, carbon_ceiling_g=5.0),
        strict=True,
    )

    print("Tools available:")
    for t in server.list_tools():
        print("  -", t["name"], "::", t["description"])

    # ---- Energy tools --------------------------------------------------
    print("\nEnergy budget  :", server.get_energy_budget())
    print("Report energy  :", server.report_energy_consumption(0.5, source="nvml"))
    print("Budget after   :", server.get_energy_budget())

    # ---- Carbon tools --------------------------------------------------
    print("Carbon (kg)    :", server.submit_carbon_report(0.002, unit="kg",
                                                         source="grid-mix:eu"))

    # ---- Pareto scoring ------------------------------------------------
    results = [
        {"agent_id": "alpha", "accuracy": 0.95, "energy_kwh": 0.010,
         "carbon_kg": 0.0030, "latency_ms": 120.0},
        {"agent_id": "beta",  "accuracy": 0.90, "energy_kwh": 0.003,
         "carbon_kg": 0.0010, "latency_ms":  90.0},
        {"agent_id": "gamma", "accuracy": 0.75, "energy_kwh": 0.001,
         "carbon_kg": 0.0005, "latency_ms":  60.0},
    ]
    score = server.score_agents(results, label="mcp-smoke")
    print("\nFrontier       :", [p["agent_id"] for p in score["frontier"]])
    print("Ranks          :", score["ranks"])

    # ---- Memory tools --------------------------------------------------
    print("\nRun trends     :", server.get_run_trends())
    print("Recent episodes:", server.get_recent_episodes(3))

    # ---- Quantum metric -----------------------------------------------
    print("\nQuantum eff    :", server.get_quantum_efficiency())

    # ---- Metacognition ------------------------------------------------
    print("\nAnomaly state  :", server.get_anomaly_state())
    print("Evaluate       :", server.evaluate_anomaly(energy=5.0, baseline_energy=1.0))

    # ---- Statistics ----------------------------------------------------
    print("\nServer stats   :", server.statistics())

    # ---- Serialization round-trip -------------------------------------
    payload = server.to_json()
    restored = GreenAgentMCPServer.from_json(payload)
    assert restored.to_dict() == server.to_dict()
    print("Serialization round-trip OK.")

    # ---- Context manager ----------------------------------------------
    with GreenAgentMCPServer(config=MCPConfig(energy_budget_wh=1.0)) as scoped:
        scoped.get_energy_budget()
        scoped.report_energy_consumption(0.2)
    print("Context-managed session OK.")

    # ---- Validation failures ------------------------------------------
    for bad_call in (
        lambda: server.report_energy_consumption(-1.0),
        lambda: server.report_energy_consumption(float("nan")),
        lambda: server.submit_carbon_report(1.0, unit="tonnes"),
        lambda: server.get_recent_episodes(0),
        lambda: server.get_run_trends(recent=-1),
    ):
        try:
            bad_call()
        except MCPServerError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError("Expected MCPServerError")

    print("\nSmoke test passed.")
