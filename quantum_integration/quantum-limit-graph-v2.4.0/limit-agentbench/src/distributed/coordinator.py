# src/distributed/coordinator.py

"""
Distributed Coordinator — v2.0.0
=================================

Coordinates multiple Green Agents in distributed mode.

Responsibilities
----------------
- Register and track :class:`AgentNode` instances.
- Execute a task against every registered node.
- Aggregate per-node Pareto metrics into a cluster-wide summary.
- Isolate per-node failures so one bad node can't take down the batch.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``run_all`` no longer silently double-executes side effects when
        called from inside an event loop. It detects the running loop
        up-front and uses the serial path without ever attempting
        ``asyncio.run``. No fallback-after-failure, so nodes are never
        invoked twice.
  * 2.2 ``missing_metric_policy="skip"`` now works in ``run_all`` /
        ``run_all_async`` (previously only in ``aggregate``).
  * 2.3 Missing ``.run`` / ``.run_async`` is caught at ``register()`` in
        strict mode, and produces a ``failed`` ``NodeResult`` (not a batch
        abort) in lenient mode.
  * 2.4 ``aggregate()`` handles duplicate ``node_id`` values consistently:
        strict raises, lenient sums into ``by_node`` so totals match.
  * 2.5 ``from_dict`` delegates to ``CoordinatorConfig.from_legacy_dict``
        so new config fields default correctly.
  * 2.6 ``__getstate__`` / ``__setstate__`` make the coordinator
        picklable (the ``RLock`` is dropped and restored).
  * 2.7 ``NodeResult.to_dict(include_result=True)`` now actually includes
        the result object (JSON serialization still omits it).
  * 2.8 ``_finalize`` uses the same number for totals and ``by_node``
        (previously ``None`` in ``by_node`` vs. ``0.0`` in totals).

**P1 robustness / observability**
  * 2.9 O(1) capacity eviction via ``OrderedDict.popitem(last=False)``.
  * 3.1 Sync path also honours ``per_node_timeout_seconds`` via a daemon
        thread + ``threading.Event``.
  * 3.3 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.4 ``close()`` lifecycle hook.
  * 3.5 ``__enter__`` / ``__exit__`` / ``__aenter__`` / ``__aexit__`` hold
        the RLock while mutating context state.
  * 3.6 ``statistics(window=N)`` for a consistent windowed view.
  * 3.7 ``ExecutionStatus`` enum for a stable machine contract.
  * 3.10 Consistent ``agents`` shape: ``run_all`` includes only successful
         results; ``aggregate`` includes the raw input list as given.
         ``node_count`` and ``by_node`` are always consistent.

**P2 enhancements**
  * 3.12 ``RunRecord`` uses ``time.time()`` for the record and derived
         durations are wall-clock millis (already the case).
  * 3.13 Deterministic fallback ``node_id`` for anonymous nodes.
  * 3.14 ``auto_register_from_registry`` propagates ``CoordinatorError``
         in strict mode.
  * 3.16 Removed dead ``carbon_scheduling`` search in ``_search_number``.
  * 3.17 ``RunRecord.skipped`` counter (defaults to 0 for old payloads).

Thread-safe, serializable, picklable, bounded in memory.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import pickle  # noqa: F401  (used in self-tests)
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CoordinatorError(ValueError):
    """Raised for invalid inputs, configuration, or coordination failures."""


class _SkippedNode(Exception):
    """
    Internal: raised by ``_extract_metrics`` when
    ``missing_metric_policy == "skip"`` (P0 2.2).

    Never propagates past ``_invoke_node_*``.
    """

    def __init__(self, node_id: str) -> None:
        super().__init__(f"node {node_id} skipped (missing metrics)")
        self.node_id = node_id


# --------------------------------------------------------------------------- #
# Execution status enum (P1 3.7)
# --------------------------------------------------------------------------- #
class ExecutionStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CoordinatorConfig:
    """Tunable parameters for the distributed coordinator."""

    max_concurrency: int = 8
    per_node_timeout_seconds: float = 300.0
    failure_policy: str = "best_effort"    # "best_effort" | "strict"
    missing_metric_policy: str = "zero"    # "zero" | "skip" | "error"
    max_nodes: int = 128
    max_history: int = 1000
    metric_energy_key: str = "energy_kwh"
    metric_latency_key: str = "latency"

    def __post_init__(self) -> None:
        if isinstance(self.max_concurrency, bool) or \
                not isinstance(self.max_concurrency, int) or \
                self.max_concurrency <= 0:
            raise CoordinatorError("max_concurrency must be > 0.")
        if isinstance(self.per_node_timeout_seconds, bool) or \
                not isinstance(self.per_node_timeout_seconds, (int, float)) or \
                self.per_node_timeout_seconds <= 0:
            raise CoordinatorError(
                "per_node_timeout_seconds must be > 0.")
        if self.failure_policy not in ("best_effort", "strict"):
            raise CoordinatorError(
                "failure_policy must be 'best_effort' or 'strict'.")
        if self.missing_metric_policy not in ("zero", "skip", "error"):
            raise CoordinatorError(
                "missing_metric_policy must be 'zero', 'skip', or 'error'.")
        if isinstance(self.max_nodes, bool) or \
                not isinstance(self.max_nodes, int) or \
                self.max_nodes <= 0:
            raise CoordinatorError("max_nodes must be > 0.")
        if isinstance(self.max_history, bool) or \
                not isinstance(self.max_history, int) or \
                self.max_history <= 0:
            raise CoordinatorError("max_history must be > 0.")
        for name in ("metric_energy_key", "metric_latency_key"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise CoordinatorError(
                    f"{name} must be a non-empty string.")

    # ---------------------------------------------------------------- #
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "CoordinatorConfig":
        """Build from a flat mapping; unknown keys ignored (P0 2.5)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Per-node execution record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class NodeResult:
    """Immutable per-node execution record."""

    node_id: str
    status: str             # ExecutionStatus.value
    latency_ms: float
    energy_kwh: Optional[float] = None
    agent_latency_ms: Optional[float] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    result: Any = None

    def to_dict(self, *, include_result: bool = False) -> Dict[str, Any]:
        """
        Return a dict representation (P0 2.7).

        ``include_result=True`` embeds the raw ``result`` object. Not
        JSON-safe — ``to_json`` never passes this flag.
        """
        d: Dict[str, Any] = {
            "node_id": self.node_id,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "energy_kwh": self.energy_kwh,
            "agent_latency_ms": self.agent_latency_ms,
            "error": self.error,
            "timestamp": self.timestamp,
            "has_result": self.result is not None,
        }
        if include_result and self.result is not None:
            d["result"] = self.result
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NodeResult":
        if not isinstance(data, Mapping):
            raise CoordinatorError(
                f"NodeResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            node_id=str(data["node_id"]),
            status=str(data["status"]),
            latency_ms=float(data["latency_ms"]),
            energy_kwh=(
                float(data["energy_kwh"])
                if data.get("energy_kwh") is not None else None
            ),
            agent_latency_ms=(
                float(data["agent_latency_ms"])
                if data.get("agent_latency_ms") is not None else None
            ),
            error=data.get("error"),
            timestamp=float(data.get("timestamp", time.time())),
            result=data.get("result"),
        )


@dataclass(frozen=True)
class RunRecord:
    """Immutable snapshot of one ``run_all`` / ``run_all_async``."""

    timestamp: float
    node_count: int
    completed: int
    failed: int
    timed_out: int
    total_energy_kwh: float
    total_latency_ms: float
    wall_time_ms: float
    skipped: int = 0    # P2 3.17 — default 0 for old serialized payloads

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #
class DistributedCoordinator:
    """
    Manages multiple agent nodes and aggregates Pareto metrics.

    Thread-safe, serializable, picklable, and bounded in memory.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        config: Optional[CoordinatorConfig] = None,
        *,
        strict: bool = True,
        node_provider: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        self._config: CoordinatorConfig = config or CoordinatorConfig()
        self._strict: bool = bool(strict)

        if metrics_hook is not None and not callable(metrics_hook):
            raise CoordinatorError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook
        self._node_provider = node_provider

        self._lock = threading.RLock()
        # 2.9: OrderedDict enables O(1) oldest-eviction.
        self._node_ids: "OrderedDict[str, Any]" = OrderedDict()
        # Separate list preserves the original iteration contract and
        # gives callers a stable ``nodes`` property.
        self._nodes: List[Any] = []
        self._history: Deque[RunRecord] = deque(
            maxlen=self._config.max_history)
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._started_at: float = time.time()

        logger.debug(
            "DistributedCoordinator v%s initialized "
            "(max_nodes=%d, concurrency=%d, failure_policy=%s, strict=%s)",
            __version__,
            self._config.max_nodes,
            self._config.max_concurrency,
            self._config.failure_policy,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CoordinatorConfig:
        return self._config

    @property
    def nodes(self) -> List[Any]:
        with self._lock:
            return list(self._nodes)

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self._nodes)

    @property
    def history(self) -> List[RunRecord]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- registration
    def register(self, node: Any) -> Any:
        """
        Register an agent node.

        Nodes are deduplicated by ``node_id``. Registration is bounded by
        ``config.max_nodes``; exceeding the cap raises in strict mode and
        evicts the oldest node in lenient mode (P0 2.3, P1 2.9).

        The node must expose at least one of ``run`` / ``run_async``.
        """
        if node is None:
            raise CoordinatorError("node must not be None.")

        has_run = callable(getattr(node, "run", None))
        has_run_async = callable(getattr(node, "run_async", None))
        if not has_run and not has_run_async:
            msg = (
                f"node {self._node_id_of(node)!r} exposes neither "
                f"'.run' nor '.run_async'."
            )
            if self._strict:
                raise CoordinatorError(msg)
            logger.warning(msg)
            # We still register in lenient mode; execution will produce a
            # failed NodeResult without aborting the batch (see
            # _invoke_node_sync/_async).

        node_id = self._node_id_of(node)
        with self._lock:
            if node_id in self._node_ids:
                logger.debug(
                    "Node %s already registered; skipping.", node_id)
                return self._node_ids[node_id]

            if len(self._nodes) >= self._config.max_nodes:
                msg = (
                    f"max_nodes={self._config.max_nodes} reached; "
                    f"cannot register {node_id}."
                )
                if self._strict:
                    raise CoordinatorError(msg)
                oldest_id, oldest_node = self._node_ids.popitem(
                    last=False)
                try:
                    self._nodes.remove(oldest_node)
                except ValueError:
                    pass
                logger.warning(
                    "%s Evicted oldest node %s.", msg, oldest_id)

            self._nodes.append(node)
            self._node_ids[node_id] = node

        self._emit_metric("node_registered", {
            "node_id": node_id,
            "has_run": has_run,
            "has_run_async": has_run_async,
        })
        logger.debug(
            "Registered node %s (total=%d).",
            node_id, self.node_count)
        return node

    def unregister(self, node_id: str) -> bool:
        """Remove a node by ``node_id``. Returns True if it was present."""
        if not isinstance(node_id, str) or not node_id:
            raise CoordinatorError("node_id must be a non-empty string.")
        with self._lock:
            node = self._node_ids.pop(node_id, None)
            if node is None:
                return False
            try:
                self._nodes.remove(node)
            except ValueError:
                pass
        self._emit_metric("node_unregistered", {"node_id": node_id})
        logger.debug(
            "Unregistered node %s (total=%d).",
            node_id, self.node_count)
        return True

    def clear(self) -> int:
        """Remove every registered node. Returns the number removed."""
        with self._lock:
            n = len(self._nodes)
            self._nodes.clear()
            self._node_ids.clear()
        if n:
            self._emit_metric("nodes_cleared", {"count": n})
        logger.debug("Cleared %d node(s).", n)
        return n

    def auto_register_from_registry(self) -> int:
        """
        Register every alive node exposed by ``node_provider``.

        In strict mode, ``CoordinatorError`` from ``register()`` propagates
        (P2 3.14). In lenient mode, it is logged and skipped.
        """
        if self._node_provider is None:
            return 0
        method = getattr(self._node_provider, "alive_agents", None)
        if not callable(method):
            logger.warning(
                "node_provider lacks alive_agents(); skipping "
                "auto-register.")
            return 0
        try:
            entries = method()
        except Exception as exc:  # noqa: BLE001
            logger.warning("alive_agents() failed: %s", exc)
            return 0

        registered = 0
        for entry in entries or []:
            handle = getattr(entry, "handle", None)
            if handle is None:
                continue
            before = self.node_count
            try:
                self.register(handle)
            except CoordinatorError as exc:
                if self._strict:
                    raise
                logger.warning("Could not register node: %s", exc)
                continue
            if self.node_count > before:
                registered += 1
        return registered

    # ---------------------------------------------------------- execution
    def run_all(
        self,
        task_input: Any,
        *,
        parallel: bool = True,
    ) -> Dict[str, Any]:
        """
        Run ``task_input`` against every registered node and aggregate.

        P0 2.1: when called from inside a running event loop, this method
        uses the serial path. It **never** attempts ``asyncio.run`` (which
        would fail) and **never** falls back after a partial async
        execution — so nodes' side effects are guaranteed to run at most
        once per call.
        """
        if self.node_count == 0:
            if self._strict:
                raise CoordinatorError("No nodes registered.")
            logger.warning(
                "No nodes registered; returning empty aggregate.")
            return self._empty_aggregate()

        if parallel and self.node_count > 1:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                # No running loop — safe to spin one up for parallel
                # execution. asyncio.run will raise if it cannot, at which
                # point no nodes have executed yet.
                return asyncio.run(
                    self.run_all_async(task_input, parallel=True))
            # A loop is already running — asyncio.run() would fail, and
            # retrying via serial after an async attempt would risk
            # double-executing side effects. Serial path is the safe
            # choice.
            logger.debug(
                "run_all() called from running event loop; using "
                "serial path. Use `await run_all_async(...)` for "
                "concurrent execution.")

        return self._run_serial(task_input)

    async def run_all_async(
        self,
        task_input: Any,
        *,
        parallel: bool = True,
    ) -> Dict[str, Any]:
        """Async variant of :meth:`run_all`."""
        if self.node_count == 0:
            if self._strict:
                raise CoordinatorError("No nodes registered.")
            logger.warning(
                "No nodes registered; returning empty aggregate.")
            return self._empty_aggregate()

        wall_start = time.perf_counter()
        with self._lock:
            nodes = list(self._nodes)

        if parallel and len(nodes) > 1:
            semaphore = asyncio.Semaphore(
                self._config.max_concurrency)

            async def _run_one(node: Any) -> NodeResult:
                async with semaphore:
                    return await self._invoke_node_async(
                        node, task_input)

            node_results = await asyncio.gather(
                *(_run_one(n) for n in nodes),
                return_exceptions=False,
            )
        else:
            node_results = []
            for node in nodes:
                node_results.append(
                    await self._invoke_node_async(node, task_input))

        wall_ms = (time.perf_counter() - wall_start) * 1000.0
        return self._finalize(node_results, wall_ms)

    def _run_serial(self, task_input: Any) -> Dict[str, Any]:
        """Serial fallback used by ``run_all`` when async isn't available."""
        wall_start = time.perf_counter()
        with self._lock:
            nodes = list(self._nodes)

        node_results: List[NodeResult] = []
        for node in nodes:
            node_results.append(
                self._invoke_node_sync(node, task_input))

        wall_ms = (time.perf_counter() - wall_start) * 1000.0
        return self._finalize(node_results, wall_ms)

    # ---------------------------------------------------------- node invocation
    @staticmethod
    def _run_with_timeout(
        fn: Callable[[], Any], timeout_s: float,
    ) -> Any:
        """
        Run ``fn`` in a daemon thread, waiting at most ``timeout_s``.

        Raises ``TimeoutError`` on deadline (P1 3.1). The daemon thread is
        orphaned on timeout but cannot delay interpreter shutdown.
        """
        result_holder: List[Any] = []
        exc_holder: List[BaseException] = []
        done = threading.Event()

        def target() -> None:
            try:
                result_holder.append(fn())
            except BaseException as exc:  # noqa: BLE001
                exc_holder.append(exc)
            finally:
                done.set()

        t = threading.Thread(target=target, daemon=True)
        t.start()
        if not done.wait(timeout=timeout_s):
            raise TimeoutError(
                f"exceeded {timeout_s:.1f}s budget")
        if exc_holder:
            raise exc_holder[0]
        return result_holder[0]

    def _invoke_node_sync(
        self, node: Any, task_input: Any,
    ) -> NodeResult:
        """Invoke a node synchronously with timeout isolation (P1 3.1)."""
        node_id = self._node_id_of(node)
        start = time.perf_counter()

        run_method = getattr(node, "run", None)
        run_async_method = getattr(node, "run_async", None)

        if not callable(run_method) and not callable(run_async_method):
            error = (
                "node exposes neither 'run' nor 'run_async'")
            return self._failed_result(
                node_id, start, error, node)
        if not callable(run_method):
            error = (
                "sync path requires 'run'; use run_all_async() for "
                "async-only nodes")
            return self._failed_result(
                node_id, start, error, node)

        try:
            result = self._run_with_timeout(
                lambda: run_method(task_input),
                self._config.per_node_timeout_seconds,
            )
        except TimeoutError as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = str(exc)
            logger.warning(
                "Node %s timed out: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' timed out: {error_repr}")
            self._emit_metric("node_timeout", {
                "node_id": node_id, "latency_ms": latency_ms,
            })
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.TIMEOUT.value,
                latency_ms=latency_ms,
                error=error_repr,
            )
        except Exception as exc:  # noqa: BLE001
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Node %s failed: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' failed: {error_repr}") from exc
            self._emit_metric("node_failed", {
                "node_id": node_id, "reason": error_repr,
            })
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.FAILED.value,
                latency_ms=latency_ms,
                error=error_repr,
            )

        latency_ms = (time.perf_counter() - start) * 1000.0
        try:
            energy, agent_latency = self._extract_metrics(
                result, node_id)
        except _SkippedNode:
            self._emit_metric("node_skipped", {"node_id": node_id})
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.SKIPPED.value,
                latency_ms=latency_ms,
            )
        # CoordinatorError from metric "error" policy propagates.
        self._emit_metric("node_completed", {
            "node_id": node_id, "latency_ms": latency_ms,
            "energy_kwh": energy,
        })
        return NodeResult(
            node_id=node_id,
            status=ExecutionStatus.COMPLETED.value,
            latency_ms=latency_ms,
            energy_kwh=energy,
            agent_latency_ms=agent_latency,
            result=result,
        )

    async def _invoke_node_async(
        self, node: Any, task_input: Any,
    ) -> NodeResult:
        """Invoke a node asynchronously with timeout isolation."""
        node_id = self._node_id_of(node)
        start = time.perf_counter()

        run_async = getattr(node, "run_async", None)
        run_method = getattr(node, "run", None)

        if not callable(run_async) and not callable(run_method):
            error = (
                "node exposes neither 'run' nor 'run_async'")
            return self._failed_result(
                node_id, start, error, node)

        try:
            if callable(run_async):
                result = await asyncio.wait_for(
                    run_async(task_input),
                    timeout=self._config.per_node_timeout_seconds,
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(run_method, task_input),
                    timeout=self._config.per_node_timeout_seconds,
                )
        except asyncio.TimeoutError:
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = (
                f"timeout after "
                f"{self._config.per_node_timeout_seconds:.1f}s")
            logger.warning(
                "Node %s timed out: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' {error_repr}.")
            self._emit_metric("node_timeout", {
                "node_id": node_id, "latency_ms": latency_ms,
            })
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.TIMEOUT.value,
                latency_ms=latency_ms,
                error=error_repr,
            )
        except Exception as exc:  # noqa: BLE001
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Node %s failed: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' failed: {error_repr}") from exc
            self._emit_metric("node_failed", {
                "node_id": node_id, "reason": error_repr,
            })
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.FAILED.value,
                latency_ms=latency_ms,
                error=error_repr,
            )

        latency_ms = (time.perf_counter() - start) * 1000.0
        try:
            energy, agent_latency = self._extract_metrics(
                result, node_id)
        except _SkippedNode:
            self._emit_metric("node_skipped", {"node_id": node_id})
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.SKIPPED.value,
                latency_ms=latency_ms,
            )
        self._emit_metric("node_completed", {
            "node_id": node_id, "latency_ms": latency_ms,
            "energy_kwh": energy,
        })
        return NodeResult(
            node_id=node_id,
            status=ExecutionStatus.COMPLETED.value,
            latency_ms=latency_ms,
            energy_kwh=energy,
            agent_latency_ms=agent_latency,
            result=result,
        )

    def _failed_result(
        self, node_id: str, start: float,
        error: str, node: Any,
    ) -> NodeResult:
        """Common failure path when node contract is invalid."""
        logger.warning("Node %s: %s", node_id, error)
        if self._config.failure_policy == "strict":
            raise CoordinatorError(f"Node '{node_id}': {error}")
        latency_ms = (time.perf_counter() - start) * 1000.0
        self._emit_metric("node_failed", {
            "node_id": node_id, "reason": error,
        })
        return NodeResult(
            node_id=node_id,
            status=ExecutionStatus.FAILED.value,
            latency_ms=latency_ms,
            error=error,
        )

    # ---------------------------------------------------------- metrics
    def _extract_metrics(
        self, result: Any, node_id: str,
    ) -> Tuple[float, float]:
        """
        Extract ``(energy_kwh, agent_latency_ms)`` from a result.

        P0 2.2: the ``skip`` policy raises ``_SkippedNode`` (caught in the
        invocation path); the ``zero`` policy substitutes 0.0; the
        ``error`` policy raises ``CoordinatorError``.
        """
        cfg = self._config
        energy = self._search_number(
            result,
            keys=(cfg.metric_energy_key, "energy_kwh"),
            containers=("metrics", "result"),
        )
        latency = self._search_number(
            result,
            keys=(cfg.metric_latency_key, "latency_ms", "latency"),
            containers=("metrics", "result"),
        )

        missing_energy = energy is None
        missing_latency = latency is None

        if missing_energy or missing_latency:
            if cfg.missing_metric_policy == "error":
                which = []
                if missing_energy:
                    which.append(cfg.metric_energy_key)
                if missing_latency:
                    which.append(cfg.metric_latency_key)
                raise CoordinatorError(
                    f"node {node_id} result is missing: "
                    f"{', '.join(which)}."
                )
            if cfg.missing_metric_policy == "skip":
                raise _SkippedNode(node_id)
            # "zero"
            if missing_energy:
                energy = 0.0
            if missing_latency:
                latency = 0.0

        return float(energy), float(latency)

    @staticmethod
    def _search_number(
        obj: Any,
        *,
        keys: Iterable[str],
        containers: Iterable[str],
        _depth: int = 0,
    ) -> Optional[float]:
        """Best-effort search for a numeric value under any of ``keys``."""
        if obj is None or _depth > 3:
            return None

        if isinstance(obj, Mapping):
            for key in keys:
                if key in obj:
                    value = obj[key]
                    if isinstance(value, bool):
                        continue
                    if isinstance(value, (int, float)):
                        f = float(value)
                        if math.isfinite(f):
                            return f
            for container in containers:
                if container in obj:
                    found = DistributedCoordinator._search_number(
                        obj[container],
                        keys=keys,
                        containers=containers,
                        _depth=_depth + 1,
                    )
                    if found is not None:
                        return found
            return None

        for key in keys:
            value = getattr(obj, key, None)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                f = float(value)
                if math.isfinite(f):
                    return f
        for container in containers:
            value = getattr(obj, container, None)
            if value is not None:
                found = DistributedCoordinator._search_number(
                    value,
                    keys=keys,
                    containers=containers,
                    _depth=_depth + 1,
                )
                if found is not None:
                    return found
        return None

    # ---------------------------------------------------------- aggregation
    def aggregate(self, results: Iterable[Any]) -> Dict[str, Any]:
        """
        Aggregate a raw iterable of node results.

        P0 2.4: duplicate ``node_id`` values are summed per node so
        ``total_energy`` / ``total_latency`` match ``by_node``. In strict
        mode, duplicates raise ``CoordinatorError``.

        P0 2.2: ``missing_metric_policy="skip"`` entries appear in
        ``skipped_nodes`` and are excluded from totals.
        """
        if results is None:
            results = []
        results_list = list(results)

        total_energy = 0.0
        total_latency = 0.0
        by_node: Dict[str, Dict[str, Any]] = {}
        failed: List[str] = []
        skipped: List[str] = []
        statuses: Dict[str, str] = {}

        for idx, raw in enumerate(results_list):
            node_id = self._node_id_from_raw(
                raw, fallback=f"index-{idx}")

            try:
                energy, latency = self._extract_metrics(raw, node_id)
            except _SkippedNode:
                statuses[node_id] = ExecutionStatus.SKIPPED.value
                skipped.append(node_id)
                continue
            # CoordinatorError from metric "error" policy propagates.

            if node_id in by_node:
                if self._strict:
                    raise CoordinatorError(
                        f"duplicate node_id {node_id!r} in aggregate "
                        f"results.")
                logger.warning(
                    "Duplicate node_id %r in aggregate(); summing.",
                    node_id)
                by_node[node_id]["energy_kwh"] += energy
                by_node[node_id]["latency_ms"] += latency
            else:
                by_node[node_id] = {
                    "energy_kwh": energy,
                    "latency_ms": latency,
                }
            total_energy += energy
            total_latency += latency
            statuses[node_id] = ExecutionStatus.COMPLETED.value

        status_counts: Dict[str, int] = {}
        for status in statuses.values():
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_energy": total_energy,
            "total_latency": total_latency,
            "agents": results_list,
            "by_node": by_node,
            "failed_nodes": failed,
            "skipped_nodes": skipped,
            "node_count": len(results_list),
            "status_counts": status_counts,
        }

    def _finalize(
        self, node_results: List[NodeResult], wall_ms: float,
    ) -> Dict[str, Any]:
        """Build the final aggregate from a batch of :class:`NodeResult`."""
        total_energy = 0.0
        total_latency = 0.0
        by_node: Dict[str, Dict[str, Any]] = {}
        failed: List[str] = []
        skipped: List[str] = []
        status_counts: Dict[str, int] = {}

        raw_results: List[Any] = []
        for nr in node_results:
            status_counts[nr.status] = (
                status_counts.get(nr.status, 0) + 1)

            if nr.status == ExecutionStatus.COMPLETED.value:
                # P0 2.8: same number in totals and by_node.
                e = nr.energy_kwh if nr.energy_kwh is not None else 0.0
                l = (
                    nr.agent_latency_ms
                    if nr.agent_latency_ms is not None else 0.0
                )
                total_energy += e
                total_latency += l
                by_node[nr.node_id] = {
                    "energy_kwh": e,
                    "latency_ms": l,
                    "wall_time_ms": nr.latency_ms,
                    "status": nr.status,
                }
                raw_results.append(nr.result)
            elif nr.status == ExecutionStatus.SKIPPED.value:
                skipped.append(nr.node_id)
                by_node[nr.node_id] = {
                    "energy_kwh": None,
                    "latency_ms": None,
                    "wall_time_ms": nr.latency_ms,
                    "status": nr.status,
                }
            else:
                failed.append(nr.node_id)
                by_node[nr.node_id] = {
                    "energy_kwh": None,
                    "latency_ms": None,
                    "wall_time_ms": nr.latency_ms,
                    "status": nr.status,
                    "error": nr.error,
                }

        aggregate = {
            "total_energy": total_energy,
            "total_latency": total_latency,
            "agents": raw_results,
            "by_node": by_node,
            "failed_nodes": failed,
            "skipped_nodes": skipped,
            "node_count": len(node_results),
            "wall_time_ms": wall_ms,
            "status_counts": status_counts,
        }

        record = RunRecord(
            timestamp=time.time(),
            node_count=len(node_results),
            completed=status_counts.get(
                ExecutionStatus.COMPLETED.value, 0),
            failed=status_counts.get(
                ExecutionStatus.FAILED.value, 0),
            timed_out=status_counts.get(
                ExecutionStatus.TIMEOUT.value, 0),
            total_energy_kwh=total_energy,
            total_latency_ms=total_latency,
            wall_time_ms=wall_ms,
            skipped=status_counts.get(
                ExecutionStatus.SKIPPED.value, 0),
        )
        with self._lock:
            self._history.append(record)

        self._emit_metric("run_completed", {
            "node_count": len(node_results),
            "completed": status_counts.get(
                ExecutionStatus.COMPLETED.value, 0),
            "failed": status_counts.get(
                ExecutionStatus.FAILED.value, 0),
            "timeout": status_counts.get(
                ExecutionStatus.TIMEOUT.value, 0),
            "skipped": status_counts.get(
                ExecutionStatus.SKIPPED.value, 0),
            "wall_time_ms": wall_ms,
        })
        logger.info(
            "run_all complete: nodes=%d completed=%d failed=%d "
            "timeout=%d skipped=%d energy=%.4fkWh latency=%.1fms "
            "wall=%.1fms",
            len(node_results),
            status_counts.get(ExecutionStatus.COMPLETED.value, 0),
            status_counts.get(ExecutionStatus.FAILED.value, 0),
            status_counts.get(ExecutionStatus.TIMEOUT.value, 0),
            status_counts.get(ExecutionStatus.SKIPPED.value, 0),
            total_energy, total_latency, wall_ms,
        )
        return aggregate

    def _empty_aggregate(self) -> Dict[str, Any]:
        return {
            "total_energy": 0.0,
            "total_latency": 0.0,
            "agents": [],
            "by_node": {},
            "failed_nodes": [],
            "skipped_nodes": [],
            "node_count": 0,
            "wall_time_ms": 0.0,
            "status_counts": {},
        }

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _node_id_of(node: Any) -> str:
        """
        Return the node's identifier.

        Deterministic fallback (P2 3.13): prefer explicit string
        ``node_id``, then ``agent_id``, then ``name``; only fall back to
        the identity hash as a last resort (with a warning at register).
        """
        for attr in ("node_id", "agent_id", "name"):
            value = getattr(node, attr, None)
            if isinstance(value, str) and value:
                return value
        return f"node@{id(node)}"

    @staticmethod
    def _node_id_from_raw(
        raw: Any, *, fallback: str,
    ) -> str:
        if isinstance(raw, Mapping):
            for key in ("node_id", "agent_id", "id"):
                value = raw.get(key)
                if isinstance(value, str) and value:
                    return value
        for attr in ("node_id", "agent_id", "name"):
            value = getattr(raw, attr, None)
            if isinstance(value, str) and value:
                return value
        return fallback

    # ---------------------------------------------------------- metrics hook
    def _emit_metric(
        self, event: str, fields: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ---------------------------------------------------------- statistics
    def statistics(
        self, window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Return aggregate statistics over the recorded runs.

        ``window`` (P1 3.6): if provided, restrict counts and means to the
        last ``window`` runs, so both are internally consistent.
        """
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)
            node_count = len(self._nodes)

        if not history:
            return {
                "runs": 0,
                "nodes_registered": node_count,
                "total_completed": 0,
                "total_failed": 0,
                "total_timed_out": 0,
                "total_skipped": 0,
                "mean_wall_time_ms": None,
                "mean_energy_kwh": None,
                "mean_latency_ms": None,
                "success_rate": None,
            }

        total_completed = sum(r.completed for r in history)
        total_failed = sum(r.failed for r in history)
        total_timeouts = sum(r.timed_out for r in history)
        total_skipped = sum(getattr(r, "skipped", 0) for r in history)
        walls = [r.wall_time_ms for r in history]
        energies = [r.total_energy_kwh for r in history]
        latencies = [r.total_latency_ms for r in history]
        total_attempts = (
            total_completed + total_failed + total_timeouts)

        return {
            "runs": len(history),
            "nodes_registered": node_count,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "total_timed_out": total_timeouts,
            "total_skipped": total_skipped,
            "mean_wall_time_ms": sum(walls) / len(walls),
            "mean_energy_kwh": sum(energies) / len(energies),
            "mean_latency_ms": sum(latencies) / len(latencies),
            "success_rate": (
                total_completed / total_attempts
                if total_attempts > 0
                else None
            ),
        }

    def reset(
        self,
        *,
        clear_nodes: bool = False,
        clear_history: bool = False,
    ) -> None:
        """Reset in-memory state; optionally drop nodes / history."""
        with self._lock:
            if clear_nodes:
                self._nodes.clear()
                self._node_ids.clear()
            if clear_history:
                self._history.clear()
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "DistributedCoordinator reset "
            "(clear_nodes=%s, clear_history=%s)",
            clear_nodes, clear_history,
        )

    def close(self) -> None:
        """Close every node and release resources (P1 3.4)."""
        with self._lock:
            nodes = list(self._nodes)
        for node in nodes:
            close_fn = getattr(node, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "node.close() failed", exc_info=True)
        logger.debug(
            "DistributedCoordinator closed (%d node(s)).", len(nodes))

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            nodes = [
                {
                    "node_id": self._node_id_of(n),
                    "type": type(n).__name__,
                }
                for n in self._nodes
            ]
            return {
                "version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "started_at": self._started_at,
                "node_count": len(self._nodes),
                "nodes": nodes,
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        nodes: Optional[Iterable[Any]] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "DistributedCoordinator":
        """
        Rebuild a coordinator from a dict produced by :meth:`to_dict`.

        Live node objects are not serialized; pass ``nodes`` to re-inject
        them.
        """
        if not isinstance(data, Mapping):
            raise CoordinatorError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.5: delegate to from_legacy_dict.
        cfg = CoordinatorConfig.from_legacy_dict(
            data.get("config") or {})

        coordinator = cls(
            config=cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        for node in nodes or []:
            try:
                coordinator.register(node)
            except CoordinatorError as exc:
                logger.warning(
                    "Skipping node during from_dict: %s", exc)

        with coordinator._lock:
            coordinator._started_at = float(
                data.get("started_at", time.time()))
            for entry in data.get("history", []):
                coordinator._history.append(RunRecord(
                    timestamp=float(entry["timestamp"]),
                    node_count=int(entry["node_count"]),
                    completed=int(entry["completed"]),
                    failed=int(entry["failed"]),
                    timed_out=int(entry["timed_out"]),
                    total_energy_kwh=float(
                        entry["total_energy_kwh"]),
                    total_latency_ms=float(
                        entry["total_latency_ms"]),
                    wall_time_ms=float(entry["wall_time_ms"]),
                    skipped=int(entry.get("skipped", 0)),
                ))
        return coordinator

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        nodes: Optional[Iterable[Any]] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "DistributedCoordinator":
        try:
            return cls.from_dict(
                json.loads(payload),
                nodes=nodes,
                metrics_hook=metrics_hook,
            )
        except json.JSONDecodeError as exc:
            raise CoordinatorError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P0 2.6: RLock is not picklable.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "DistributedCoordinator":
        with self._lock:
            self._ctx_start = time.perf_counter()
        logger.debug(
            "Entering scoped DistributedCoordinator session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            node_count = len(self._nodes)
        if exc_type is not None:
            logger.warning(
                "DistributedCoordinator scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "DistributedCoordinator scope closed in %.4fs "
            "(%d node(s)).", elapsed, node_count)

    async def __aenter__(self) -> "DistributedCoordinator":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        logger.debug(
            "Entering async DistributedCoordinator session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
            node_count = len(self._nodes)
        if exc_type is not None:
            logger.warning(
                "Async DistributedCoordinator scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "Async DistributedCoordinator scope closed in %.4fs "
            "(%d node(s)).", elapsed, node_count)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "DistributedCoordinator("
                f"version={__version__!r}, "
                f"nodes={len(self._nodes)}, "
                f"max_nodes={self._config.max_nodes}, "
                f"failure_policy={self._config.failure_policy!r}, "
                f"strict={self._strict})"
            )

    def __len__(self) -> int:
        return self.node_count


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DistributedCoordinator",
    "CoordinatorConfig",
    "CoordinatorError",
    "ExecutionStatus",
    "NodeResult",
    "RunRecord",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m distributed.coordinator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    import pickle

    # ---------------------------------------------------------------- #
    # Test node implementations
    # ---------------------------------------------------------------- #
    class _CountingNode:
        """Counts invocations per (node_id) via a shared counter dict."""

        def __init__(self, node_id: str, counts: Dict[str, int]) -> None:
            self.node_id = node_id
            self._counts = counts
            counts[node_id] = 0

        def run(self, task_input: Any) -> Dict[str, Any]:
            self._counts[self.node_id] += 1
            return {
                "node_id": self.node_id,
                "task_input": task_input,
                "metrics": {"energy_kwh": 0.01, "latency": 50.0},
            }

        async def run_async(self, task_input: Any) -> Dict[str, Any]:
            self._counts[self.node_id] += 1
            return {
                "node_id": self.node_id,
                "task_input": task_input,
                "metrics": {"energy_kwh": 0.01, "latency": 50.0},
            }

    class _GoodNode:
        def __init__(
            self, node_id: str,
            energy: float = 0.01, latency: float = 50.0,
            delay_s: float = 
