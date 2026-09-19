# src/memory/mcp_tools.py

"""
Memory MCP Tools
================

Exposes the Supermemory memory layer as MCP tools that the existing
``agentbeats/mcp_server.py`` can register.

Tools
-----
- ``recall_similar_runs``
- ``remember_decision``
- ``remember_policy``
- ``record_outcome``
- ``list_spaces``
- ``list_memories``

Enhancements
------------
- ``MemoryMCPConfig`` — frozen, validated: server identity, tool name
  prefix, bounded history.
- ``MemoryMCPBridge`` — thread-safe wrapper around the memory layer.
- Each method decorated with the MCP ``@tool`` decorator (with fallback).
- ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple

from .bounded_recall import BoundedRecall, RecallBundle
from .memory_schemas import (
    DecisionRecord,
    OutcomeRecord,
    PolicyRecord,
    TruthLevel,
)
from .supermemory_adapter import SupermemoryAdapter, SupermemoryAdapterError
from .write_governor import WriteGovernor, WriteGovernorError

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Tool decorator (graceful fallback when MCP SDK is absent)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    from mcp.server.fastmcp import tool  # type: ignore

    _MCP_TOOL_AVAILABLE = True
except ImportError:  # pragma: no cover
    try:
        from mcp.server import tool  # type: ignore

        _MCP_TOOL_AVAILABLE = True
    except ImportError:
        def tool(*args: Any, **kwargs: Any) -> Callable:  # type: ignore
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
# Errors
# --------------------------------------------------------------------------- #
class MemoryMCPError(ValueError):
    """Raised for invalid MCP tool inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryMCPConfig:
    """Tunable parameters for the memory MCP bridge."""

    server_name: str = "green-agent-memory"
    server_version: str = "5.0.0"
    tool_prefix: str = "memory_"
    max_history: int = 1_000

    def __post_init__(self) -> None:
        for name in ("server_name", "server_version"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise MemoryMCPError(f"{name} must be a non-empty string.")
        if not isinstance(self.tool_prefix, str):
            raise MemoryMCPError("tool_prefix must be a string.")
        if not isinstance(self.max_history, int) or self.max_history <= 0:
            raise MemoryMCPError("max_history must be a positive int.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Tool call record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ToolCallRecord:
    """Immutable snapshot of one MCP tool invocation."""

    tool_name: str
    outcome: str
    duration_ms: float
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Bridge
# --------------------------------------------------------------------------- #
class MemoryMCPBridge:
    """Exposes the memory layer as MCP tools.

    Parameters
    ----------
    adapter : SupermemoryAdapter
    recall : BoundedRecall
    governor : WriteGovernor, optional
        Required for write tools.
    config : MemoryMCPConfig, optional
    strict : bool, default True
    """

    def __init__(
        self,
        *,
        adapter: SupermemoryAdapter,
        recall: BoundedRecall,
        governor: Optional[WriteGovernor] = None,
        config: Optional[MemoryMCPConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(adapter, SupermemoryAdapter):
            raise MemoryMCPError("adapter must be a SupermemoryAdapter.")
        if not isinstance(recall, BoundedRecall):
            raise MemoryMCPError("recall must be a BoundedRecall.")
        if governor is not None and not isinstance(governor, WriteGovernor):
            raise MemoryMCPError("governor must be a WriteGovernor.")

        self._adapter = adapter
        self._recall = recall
        self._governor = governor
        self._config = config or MemoryMCPConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ToolCallRecord] = deque(
            maxlen=self._config.max_history
        )
        self._started_at = time.time()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryMCPConfig:
        return self._config

    @property
    def history(self) -> List[ToolCallRecord]:
        with self._lock:
            return list(self._history)

    # =================================================================== #
    # MCP TOOLS
    # =================================================================== #
    @tool(description="Recall comparable historical runs from memory.")
    def recall_similar_runs(
        self,
        query: str,
        k: int = 5,
        container_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Recall up to ``k`` comparable runs matching ``query``."""
        return self._run_tool(
            "recall_similar_runs",
            lambda: self._recall_similar_runs_impl(query, k, container_tag),
        )

    @tool(description="Persist a decision outcome to memory.")
    def remember_decision(
        self,
        run_id: str,
        workload_type: str,
        device_class: str,
        route: str,
        policy_version: str,
        predicted_energy_wh: float,
        predicted_carbon_gco2e: float,
        predicted_latency_ms: float,
        reason: str = "",
        truth_level: str = "measured",
        container_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a decision record via the governed write path."""
        return self._run_tool(
            "remember_decision",
            lambda: self._remember_decision_impl(
                run_id=run_id,
                workload_type=workload_type,
                device_class=device_class,
                route=route,
                policy_version=policy_version,
                predicted_energy_wh=predicted_energy_wh,
                predicted_carbon_gco2e=predicted_carbon_gco2e,
                predicted_latency_ms=predicted_latency_ms,
                reason=reason,
                truth_level=truth_level,
                container_tag=container_tag,
            ),
        )

    @tool(description="Persist a policy version to memory.")
    def remember_policy(
        self,
        version: str,
        content: Dict[str, Any],
        approved_by: str = "system",
        container_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a policy record via the governed write path."""
        return self._run_tool(
            "remember_policy",
            lambda: self._remember_policy_impl(
                version=version, content=content,
                approved_by=approved_by, container_tag=container_tag,
            ),
        )

    @tool(description="Record the measured outcome of a run.")
    def record_outcome(
        self,
        run_id: str,
        predicted: Dict[str, float],
        measured: Dict[str, float],
        truth_level: str = "measured",
        container_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a predicted-vs-measured outcome record."""
        return self._run_tool(
            "record_outcome",
            lambda: self._record_outcome_impl(
                run_id=run_id, predicted=predicted, measured=measured,
                truth_level=truth_level, container_tag=container_tag,
            ),
        )

    @tool(description="List the spaces (container tags) known to Supermemory.")
    def list_spaces(self) -> Dict[str, Any]:
        """Return the available container tags."""
        return self._run_tool("list_spaces", self._list_spaces_impl)

    @tool(description="List memories in a given container tag.")
    def list_memories(
        self,
        container_tag: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Return memories in ``container_tag`` (best-effort)."""
        return self._run_tool(
            "list_memories",
            lambda: self._list_memories_impl(container_tag, limit),
        )

    # =================================================================== #
    # IMPLEMENTATIONS
    # =================================================================== #
    def _recall_similar_runs_impl(
        self, query: str, k: int, container_tag: Optional[str],
    ) -> Dict[str, Any]:
        bundle: RecallBundle = self._recall.query(
            query, k=k, container_tag=container_tag,
        )
        return {
            "status": "ok",
            "query": bundle.query,
            "count": len(bundle.memories),
            "citations": list(bundle.citations),
            "token_cost": bundle.token_cost,
            "latency_ms": bundle.latency_ms,
            "truth_level_mix": dict(bundle.truth_level_mix),
            "summary": self._recall.summarize(bundle),
            "memories": [dict(m) for m in bundle.memories],
        }

    def _remember_decision_impl(
        self,
        *,
        run_id: str,
        workload_type: str,
        device_class: str,
        route: str,
        policy_version: str,
        predicted_energy_wh: float,
        predicted_carbon_gco2e: float,
        predicted_latency_ms: float,
        reason: str,
        truth_level: str,
        container_tag: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPError("remember_decision requires a governor.")
        record = DecisionRecord(
            run_id=run_id,
            workload_type=workload_type,
            device_class=device_class,
            route=route,
            policy_version=policy_version,
            truth_level=truth_level,
            predicted_energy_wh=predicted_energy_wh,
            predicted_carbon_gco2e=predicted_carbon_gco2e,
            predicted_latency_ms=predicted_latency_ms,
            reason=reason,
            container_tag=container_tag or self._adapter.config.default_container_tag,
        )
        promoted = self._governor.promote(
            record, writer_id="mcp_client", kind="decision_outcome",
        )
        memory_id: Optional[str] = None
        if promoted:
            memory_id = self._adapter.remember_decision(record)
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "run_id": run_id,
        }

    def _remember_policy_impl(
        self,
        *,
        version: str,
        content: Mapping[str, Any],
        approved_by: str,
        container_tag: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPError("remember_policy requires a governor.")
        record = PolicyRecord(
            version=version,
            content=dict(content),
            approved_by=approved_by,
            container_tag=container_tag or "policy:current",
        )
        promoted = self._governor.promote(
            record, writer_id="mcp_client", kind="policy",
        )
        memory_id: Optional[str] = None
        if promoted:
            memory_id = self._adapter.remember_policy(record)
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "version": version,
        }

    def _record_outcome_impl(
        self,
        *,
        run_id: str,
        predicted: Mapping[str, float],
        measured: Mapping[str, float],
        truth_level: str,
        container_tag: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPError("record_outcome requires a governor.")
        record = OutcomeRecord(
            run_id=run_id,
            predicted=dict(predicted),
            measured=dict(measured),
            truth_level=truth_level,
            container_tag=container_tag or self._adapter.config.default_container_tag,
        )
        promoted = self._governor.promote(
            record, writer_id="mcp_client", kind="outcome",
        )
        memory_id: Optional[str] = None
        if promoted:
            memory_id = self._adapter.record_outcome(record)
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "delta": record.delta(),
        }

    def _list_spaces_impl(self) -> Dict[str, Any]:
        # Best-effort: enumerate spaces from the local mirror's tags.
        with self._adapter._lock:  # type: ignore[attr-defined]
            tags = sorted({
                str(m.get("container_tag"))
                for m in self._adapter._mirror  # type: ignore[attr-defined]
                if m.get("container_tag")
            })
        return {"status": "ok", "spaces": tags}

    def _list_memories_impl(
        self, container_tag: Optional[str], limit: int,
    ) -> Dict[str, Any]:
        if not isinstance(limit, int) or limit <= 0:
            raise MemoryMCPError("limit must be a positive int.")
        tag = container_tag or self._adapter.config.default_container_tag
        with self._adapter._lock:  # type: ignore[attr-defined]
            memories = [
                dict(m) for m in self._adapter._mirror  # type: ignore[attr-defined]
                if m.get("container_tag") == tag
            ][-limit:]
        return {"status": "ok", "container_tag": tag, "count": len(memories),
                "memories": memories}

    # =================================================================== #
    # Dispatcher
    # =================================================================== #
    def _run_tool(self, tool_name: str, impl: Callable[[], Any]) -> Dict[str, Any]:
        start = time.perf_counter()
        outcome = "ok"
        error: Optional[str] = None
        try:
            result = impl()
            if not isinstance(result, Mapping):
                raise MemoryMCPError(
                    f"tool {tool_name!r} returned "
                    f"{type(result).__name__}; expected Mapping."
                )
            return dict(result)
        except MemoryMCPError:
            outcome = "error"
            error = "validation_error"
            raise
        except Exception as exc:
            outcome = "error"
            error = f"{type(exc).__name__}: {exc}"
            logger.exception("MCP tool '%s' failed: %s", tool_name, exc)
            if self._strict:
                raise MemoryMCPError(
                    f"tool '{tool_name}' failed: {exc}"
                ) from exc
            return {"status": "error", "tool": tool_name, "error": str(exc)}
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            record = ToolCallRecord(
                tool_name=tool_name,
                outcome=outcome,
                duration_ms=duration_ms,
                error=error,
            )
            with self._lock:
                self._history.append(record)
            logger.debug(
                "tool=%s outcome=%s duration=%.2fms",
                tool_name, outcome, duration_ms,
            )

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
        by_tool: Dict[str, int] = {}
        for record in history:
            by_tool[record.tool_name] = by_tool.get(record.tool_name, 0) + 1
        durations = [r.duration_ms for r in history]
        return {
            "calls": len(history),
            "by_tool": by_tool,
            "errors": sum(1 for r in history if r.outcome == "error"),
            "mean_duration_ms": (
                sum(durations) / len(durations) if durations else 0.0
            ),
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    def list_tools(self) -> List[Dict[str, Any]]:
        """Return the MCP tool manifest."""
        import inspect

        manifest: List[Dict[str, Any]] = []
        for attr in dir(self):
            if attr.startswith("_"):
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
                "name": f"{self._config.tool_prefix}{attr}",
                "method": attr,
                "description": doc[0] if doc else "",
                "parameters": params,
            })
        manifest.sort(key=lambda x: x["name"])
        return manifest

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=str, **kw)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "MemoryMCPBridge("
                f"calls={len(self._history)}, "
                f"strict={self._strict}, "
                f"mcp_sdk={_MCP_TOOL_AVAILABLE})"
            )


__all__ = [
    "MemoryMCPBridge",
    "MemoryMCPConfig",
    "MemoryMCPError",
    "ToolCallRecord",
    "_MCP_TOOL_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.mcp_tools
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .bounded_recall import BoundedRecall
    from .supermemory_adapter import SupermemoryAdapter
    from .write_governor import WriteGovernor

    adapter = SupermemoryAdapter()
    recall = BoundedRecall(adapter)
    governor = WriteGovernor()
    governor.register_writer(
        "mcp_client",
        capabilities={"write:decision", "write:policy", "write:outcome"},
    )

    bridge = MemoryMCPBridge(
        adapter=adapter, recall=recall, governor=governor,
    )
    print("repr       :", bridge)

    # ---- Write a decision ----------------------------------------
    write = bridge.remember_decision(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        route="edge_int8",
        policy_version="v0.3",
        predicted_energy_wh=8.1,
        predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0,
        reason="SLA met; lower carbon.",
    )
    print("write      :", write["status"], write["memory_id"])

    # ---- Recall ---------------------------------------------------
    recalled = bridge.recall_similar_runs("vision_inference edge_int8", k=3)
    print("recall     :", recalled["count"], "memories")
    print("summary    :\n" + recalled["summary"])

    # ---- Policy ---------------------------------------------------
    policy = bridge.remember_policy(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0},
        approved_by="ops",
    )
    print("policy     :", policy["status"])

    # ---- Outcome --------------------------------------------------
    outcome = bridge.record_outcome(
        run_id="run-001",
        predicted={"energy_wh": 8.1, "carbon_g": 3.7},
        measured={"energy_wh": 8.6, "carbon_g": 4.0},
    )
    print("outcome    :", outcome["status"], outcome["delta"])

    # ---- List spaces / memories -----------------------------------
    print("spaces     :", bridge.list_spaces())
    print("memories   :", bridge.list_memories(limit=5)["count"])

    # ---- Tool manifest --------------------------------------------
    print("tools      :")
    for t in bridge.list_tools():
        print("  -", t["name"])

    print("statistics :", {
        k: v for k, v in bridge.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    print("\nSmoke test passed.")
