# src/distributed/coordinator.py

"""
Distributed Coordinator
=======================

Coordinates multiple Green Agents in distributed mode.

Responsibilities
----------------
- Register and track :class:`AgentNode` instances.
- Execute a task against every registered node.
- Aggregate per-node Pareto metrics into a cluster-wide summary.
- Isolate per-node failures so one bad node can't take down the batch.

Original behaviour preserved
----------------------------
- ``DistributedCoordinator()`` — constructs an empty coordinator.
- ``register(node)`` — appends a node.
- ``run_all(task_input)`` — runs every node with the same input and returns
  an aggregated summary ``{"total_energy", "total_latency", "agents"}``.
- ``aggregate(results)`` — sums energy and latency across results.

Enhancements
------------
- Configurable via :class:`CoordinatorConfig` (concurrency, timeout, capacity,
  failure policy, missing-metric policy).
- Thread-safe via ``RLock``; bounded node count and result history.
- Per-node result attribution and status (``completed`` / ``failed`` / ``timeout``).
- Concurrent execution when nodes expose ``run_async`` (falls back to serial).
- Per-node failure isolation with strict / best-effort policies.
- Robust metric extraction from arbitrary result shapes.
- Full validation; strict / non-strict modes.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Sync and async context managers; ``statistics()``; ``__repr__``.
- Custom :class:`CoordinatorError`; lazy ``%s`` logging.
- ``__main__`` smoke test covering happy path, partial failure, serialization,
  context managers, and validation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CoordinatorError(ValueError):
    """Raised for invalid inputs, configuration, or coordination failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CoordinatorConfig:
    """
    Tunable parameters for the distributed coordinator.

    Centralizes concurrency, timeout, capacity, and failure-handling policy so
    deployments can calibrate without editing the class.
    """

    # Execution policy
    max_concurrency: int = 8
    per_node_timeout_seconds: float = 300.0
    failure_policy: str = "best_effort"    # "best_effort" | "strict"
    missing_metric_policy: str = "zero"    # "zero" | "skip" | "error"

    # Capacity / history
    max_nodes: int = 128
    max_history: int = 1000

    # Metric field names (result → float mapping)
    metric_energy_key: str = "energy_kwh"
    metric_latency_key: str = "latency"

    def __post_init__(self) -> None:
        if self.max_concurrency <= 0:
            raise CoordinatorError("max_concurrency must be > 0.")
        if self.per_node_timeout_seconds <= 0:
            raise CoordinatorError("per_node_timeout_seconds must be > 0.")
        if self.failure_policy not in ("best_effort", "strict"):
            raise CoordinatorError(
                "failure_policy must be 'best_effort' or 'strict'."
            )
        if self.missing_metric_policy not in ("zero", "skip", "error"):
            raise CoordinatorError(
                "missing_metric_policy must be 'zero', 'skip', or 'error'."
            )
        if self.max_nodes <= 0:
            raise CoordinatorError("max_nodes must be > 0.")
        if self.max_history <= 0:
            raise CoordinatorError("max_history must be > 0.")
        for name in ("metric_energy_key", "metric_latency_key"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise CoordinatorError(f"{name} must be a non-empty string.")


# --------------------------------------------------------------------------- #
# Per-node execution record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class NodeResult:
    """
    Immutable per-node execution record.

    ``result`` holds whatever the underlying node returned; it is excluded
    from :meth:`to_dict` when non-serializable, but its extracted metrics are
    always preserved.
    """

    node_id: str
    status: str             # "completed" | "failed" | "timeout"
    latency_ms: float
    energy_kwh: Optional[float] = None
    agent_latency_ms: Optional[float] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    result: Any = None

    def to_dict(self, *, include_result: bool = False) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "energy_kwh": self.energy_kwh,
            "agent_latency_ms": self.agent_latency_ms,
            "error": self.error,
            "timestamp": self.timestamp,
            "result_included": bool(include_result and self.result is not None),
        }


@dataclass(frozen=True)
class RunRecord:
    """Immutable snapshot of one ``run_all`` / ``run_all_async`` invocation."""

    timestamp: float
    node_count: int
    completed: int
    failed: int
    timed_out: int
    total_energy_kwh: float
    total_latency_ms: float
    wall_time_ms: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #
class DistributedCoordinator:
    """
    Manages multiple agent nodes and aggregates Pareto metrics.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only with backward-compatible
    defaults.

    Parameters
    ----------
    config : CoordinatorConfig, optional
        Execution / capacity / failure policy. Defaults to
        ``CoordinatorConfig()``.
    strict : bool, default True
        If True, invalid inputs raise :class:`CoordinatorError`.
        If False, invalid inputs are logged and coerced.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        config: Optional[CoordinatorConfig] = None,
        *,
        strict: bool = True,
        node_provider: Optional[Any] = None,
    ) -> None:
        """
        Parameters
        ----------
        config : CoordinatorConfig, optional
            Coordinator configuration.
        strict : bool, default True
            Raise on invalid inputs when True.
        node_provider : Any, optional
            Optional :class:`AgentRegistry`-like provider used by
            :meth:`auto_register_from_registry`. Must expose
            ``alive_agents()`` returning entries with ``.handle`` attributes.
        """
        self._config: CoordinatorConfig = config or CoordinatorConfig()
        self._strict: bool = bool(strict)
        self._node_provider = node_provider

        self._lock = threading.RLock()
        self._nodes: List[Any] = []
        self._node_ids: Dict[str, Any] = {}       # node_id -> node (dedupe guard)
        self._history: Deque[RunRecord] = deque(
            maxlen=self._config.max_history
        )
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._started_at: float = time.time()

        logger.debug(
            "DistributedCoordinator initialized "
            "(max_nodes=%d, concurrency=%d, failure_policy=%s, strict=%s)",
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

        Nodes are deduplicated by their ``node_id`` attribute (or by identity
        if that attribute is absent). Re-registering the same node is a no-op.
        Registration is bounded by ``config.max_nodes``; exceeding the cap
        raises :class:`CoordinatorError` in strict mode and evicts the oldest
        node otherwise.

        Returns the registered node (for chaining).
        """
        if node is None:
            raise CoordinatorError("node must not be None.")

        node_id = self._node_id_of(node)
        with self._lock:
            if node_id in self._node_ids:
                logger.debug("Node %s already registered; skipping.", node_id)
                return self._node_ids[node_id]

            if len(self._nodes) >= self._config.max_nodes:
                msg = (
                    f"max_nodes={self._config.max_nodes} reached; "
                    f"cannot register {node_id}."
                )
                if self._strict:
                    raise CoordinatorError(msg)
                evicted = self._nodes.pop(0)
                evicted_id = self._node_id_of(evicted)
                self._node_ids.pop(evicted_id, None)
                logger.warning("%s Evicted oldest node %s.", msg, evicted_id)

            self._nodes.append(node)
            self._node_ids[node_id] = node

        logger.debug(
            "Registered node %s (total=%d).", node_id, self.node_count
        )
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
        logger.debug("Unregistered node %s (total=%d).", node_id, self.node_count)
        return True

    def clear(self) -> int:
        """Remove every registered node. Returns the number removed."""
        with self._lock:
            n = len(self._nodes)
            self._nodes.clear()
            self._node_ids.clear()
        logger.debug("Cleared %d node(s).", n)
        return n

    def auto_register_from_registry(self) -> int:
        """
        Register every alive node exposed by ``node_provider``.

        ``node_provider`` must expose ``alive_agents()`` returning entries
        with a ``.handle`` attribute. Returns the number of newly registered
        nodes.
        """
        if self._node_provider is None:
            return 0
        method = getattr(self._node_provider, "alive_agents", None)
        if not callable(method):
            logger.warning(
                "node_provider lacks alive_agents(); skipping auto-register."
            )
            return 0
        try:
            entries = method()
        except Exception as exc:
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

        Parameters
        ----------
        task_input : Any
            Input forwarded to each node's ``run`` / ``run_async``.
        parallel : bool, default True
            If True **and** an event loop is available **and** the coordinator
            has more than one node, run concurrently via ``run_all_async``.
            Otherwise run serially (matching the original behaviour).

        Returns
        -------
        dict
            ``{"total_energy", "total_latency", "agents", "by_node",
            "failed_nodes", "wall_time_ms"}``.

        Raises
        ------
        CoordinatorError
            If no nodes are registered (strict mode).
        """
        if self.node_count == 0:
            if self._strict:
                raise CoordinatorError("No nodes registered.")
            logger.warning("No nodes registered; returning empty aggregate.")
            return self._empty_aggregate()

        # Fast path: try concurrent execution if an event loop is reachable.
        if parallel and self.node_count > 1:
            try:
                return asyncio.run(
                    self.run_all_async(task_input, parallel=True)
                )
            except RuntimeError:
                # Already running inside an event loop — caller must use
                # ``run_all_async`` explicitly.
                logger.debug(
                    "Event loop already running; falling back to serial "
                    "run_all. Use `await run_all_async()` for concurrency."
                )
            except Exception as exc:
                logger.warning(
                    "Parallel execution failed (%s); falling back to serial.",
                    exc,
                )

        return self._run_serial(task_input)

    async def run_all_async(
        self,
        task_input: Any,
        *,
        parallel: bool = True,
    ) -> Dict[str, Any]:
        """
        Async variant of :meth:`run_all`.

        Runs every node's ``run_async`` concurrently when ``parallel=True`` and
        the node exposes an ``async def run_async`` method. Nodes that only
        expose a sync ``run`` are executed in a thread pool.
        """
        if self.node_count == 0:
            if self._strict:
                raise CoordinatorError("No nodes registered.")
            logger.warning("No nodes registered; returning empty aggregate.")
            return self._empty_aggregate()

        wall_start = time.perf_counter()
        with self._lock:
            nodes = list(self._nodes)

        if parallel and len(nodes) > 1:
            semaphore = asyncio.Semaphore(self._config.max_concurrency)

            async def _run_one(node: Any) -> NodeResult:
                async with semaphore:
                    return await self._invoke_node_async(node, task_input)

            node_results = await asyncio.gather(
                *(_run_one(n) for n in nodes),
                return_exceptions=False,
            )
        else:
            node_results = []
            for node in nodes:
                node_results.append(await self._invoke_node_async(node, task_input))

        wall_ms = (time.perf_counter() - wall_start) * 1000.0
        return self._finalize(node_results, wall_ms)

    def _run_serial(self, task_input: Any) -> Dict[str, Any]:
        """Serial fallback used by ``run_all`` when async isn't available."""
        wall_start = time.perf_counter()
        with self._lock:
            nodes = list(self._nodes)

        node_results: List[NodeResult] = []
        for node in nodes:
            node_results.append(self._invoke_node_sync(node, task_input))

        wall_ms = (time.perf_counter() - wall_start) * 1000.0
        return self._finalize(node_results, wall_ms)

    # ---------------------------------------------------------- node invocation
    def _invoke_node_sync(self, node: Any, task_input: Any) -> NodeResult:
        """Invoke a node synchronously, capturing failures and timing."""
        node_id = self._node_id_of(node)
        start = time.perf_counter()
        try:
            if not callable(getattr(node, "run", None)):
                raise CoordinatorError(
                    f"node {node_id} does not expose a callable 'run' method."
                )
            result = node.run(task_input)
            latency_ms = (time.perf_counter() - start) * 1000.0
            energy, agent_latency = self._extract_metrics(result, node_id)
            return NodeResult(
                node_id=node_id,
                status="completed",
                latency_ms=latency_ms,
                energy_kwh=energy,
                agent_latency_ms=agent_latency,
                result=result,
            )
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = f"{type(exc).__name__}: {exc}"
            logger.warning("Node %s failed: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' failed: {error_repr}"
                ) from exc
            return NodeResult(
                node_id=node_id,
                status="failed",
                latency_ms=latency_ms,
                error=error_repr,
                result=None,
            )

    async def _invoke_node_async(self, node: Any, task_input: Any) -> NodeResult:
        """Invoke a node asynchronously, capturing failures and timing."""
        node_id = self._node_id_of(node)
        start = time.perf_counter()

        try:
            run_async = getattr(node, "run_async", None)
            if callable(run_async):
                result = await asyncio.wait_for(
                    run_async(task_input),
                    timeout=self._config.per_node_timeout_seconds,
                )
            else:
                run_method = getattr(node, "run", None)
                if not callable(run_method):
                    raise CoordinatorError(
                        f"node {node_id} does not expose 'run' or "
                        f"'run_async'."
                    )
                result = await asyncio.wait_for(
                    asyncio.to_thread(run_method, task_input),
                    timeout=self._config.per_node_timeout_seconds,
                )

            latency_ms = (time.perf_counter() - start) * 1000.0
            energy, agent_latency = self._extract_metrics(result, node_id)
            return NodeResult(
                node_id=node_id,
                status="completed",
                latency_ms=latency_ms,
                energy_kwh=energy,
                agent_latency_ms=agent_latency,
                result=result,
            )

        except asyncio.TimeoutError:
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = (
                f"timeout after {self._config.per_node_timeout_seconds:.1f}s"
            )
            logger.warning("Node %s timed out: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' {error_repr}."
                )
            return NodeResult(
                node_id=node_id,
                status="timeout",
                latency_ms=latency_ms,
                error=error_repr,
                result=None,
            )

        except CoordinatorError:
            raise
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_repr = f"{type(exc).__name__}: {exc}"
            logger.warning("Node %s failed: %s", node_id, error_repr)
            if self._config.failure_policy == "strict":
                raise CoordinatorError(
                    f"Node '{node_id}' failed: {error_repr}"
                ) from exc
            return NodeResult(
                node_id=node_id,
                status="failed",
                latency_ms=latency_ms,
                error=error_repr,
                result=None,
            )

    # ---------------------------------------------------------- metrics
    def _extract_metrics(
        self, result: Any, node_id: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract ``(energy_kwh, agent_latency_ms)`` from an arbitrary result.

        The original aggregator assumed ``result["metrics"]["energy_kwh"]``.
        This helper accepts that shape and also flattens nested variants:

        - ``{"metrics": {"energy_kwh": ..., "latency": ...}}``
        - ``{"energy_kwh": ..., "latency_ms": ...}``
        - ``{"energy_kwh": ..., "latency": ...}``
        - any object with ``.energy_kwh`` / ``.latency`` attributes.
        """
        cfg = self._config
        energy = self._search_number(
            result,
            keys=(cfg.metric_energy_key, "energy_kwh"),
            containers=("metrics", "result", "carbon_scheduling"),
        )
        latency = self._search_number(
            result,
            keys=(cfg.metric_latency_key, "latency_ms", "latency"),
            containers=("metrics", "result"),
        )

        if energy is None and cfg.missing_metric_policy == "error":
            raise CoordinatorError(
                f"node {node_id} result has no '{cfg.metric_energy_key}'."
            )
        if latency is None and cfg.missing_metric_policy == "error":
            raise CoordinatorError(
                f"node {node_id} result has no '{cfg.metric_latency_key}'."
            )

        return energy, latency

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

        # Mapping search.
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

        # Attribute search.
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

        Preserves the original contract: returns a dict with at least
        ``total_energy``, ``total_latency``, and ``agents``. Missing metrics
        are treated according to ``config.missing_metric_policy``.
        """
        if results is None:
            results = []
        results_list = list(results)

        total_energy = 0.0
        total_latency = 0.0
        by_node: Dict[str, Dict[str, Any]] = {}
        failed: List[str] = []
        statuses: Dict[str, int] = {}

        for idx, raw in enumerate(results_list):
            node_id = self._node_id_from_raw(raw, fallback=f"index-{idx}")
            energy, latency = self._extract_metrics(raw, node_id)

            # Missing-metric policy.
            if energy is None:
                if self._config.missing_metric_policy == "skip":
                    continue
                if self._config.missing_metric_policy == "error":
                    raise CoordinatorError(
                        f"result at index {idx} is missing "
                        f"'{self._config.metric_energy_key}'."
                    )
                energy = 0.0
            if latency is None:
                if self._config.missing_metric_policy == "skip":
                    continue
                if self._config.missing_metric_policy == "error":
                    raise CoordinatorError(
                        f"result at index {idx} is missing "
                        f"'{self._config.metric_latency_key}'."
                    )
                latency = 0.0

            total_energy += energy
            total_latency += latency
            by_node[node_id] = {
                "energy_kwh": energy,
                "latency_ms": latency,
            }
            statuses[node_id] = "completed"

        return {
            "total_energy": total_energy,
            "total_latency": total_latency,
            "agents": results_list,
            "by_node": by_node,
            "failed_nodes": failed,
            "node_count": len(results_list),
            "status_counts": statuses and {
                s: sum(1 for v in statuses.values() if v == s)
                for s in set(statuses.values())
            } or {},
        }

    def _finalize(
        self, node_results: List[NodeResult], wall_ms: float
    ) -> Dict[str, Any]:
        """Build the final aggregate from a batch of :class:`NodeResult`."""
        total_energy = 0.0
        total_latency = 0.0
        by_node: Dict[str, Dict[str, Any]] = {}
        failed: List[str] = []
        status_counts: Dict[str, int] = {}

        raw_results: List[Any] = []
        for nr in node_results:
            status_counts[nr.status] = status_counts.get(nr.status, 0) + 1
            if nr.status == "completed":
                total_energy += nr.energy_kwh or 0.0
                total_latency += nr.agent_latency_ms or 0.0
                by_node[nr.node_id] = {
                    "energy_kwh": nr.energy_kwh,
                    "latency_ms": nr.agent_latency_ms,
                    "wall_time_ms": nr.latency_ms,
                    "status": nr.status,
                }
                raw_results.append(nr.result)
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
            "node_count": len(node_results),
            "wall_time_ms": wall_ms,
            "status_counts": status_counts,
        }

        record = RunRecord(
            timestamp=time.time(),
            node_count=len(node_results),
            completed=status_counts.get("completed", 0),
            failed=status_counts.get("failed", 0),
            timed_out=status_counts.get("timeout", 0),
            total_energy_kwh=total_energy,
            total_latency_ms=total_latency,
            wall_time_ms=wall_ms,
        )
        with self._lock:
            self._history.append(record)

        logger.info(
            "run_all complete: nodes=%d completed=%d failed=%d "
            "timeout=%d energy=%.4fkWh latency=%.1fms wall=%.1fms",
            len(node_results),
            status_counts.get("completed", 0),
            status_counts.get("failed", 0),
            status_counts.get("timeout", 0),
            total_energy,
            total_latency,
            wall_ms,
        )
        return aggregate

    def _empty_aggregate(self) -> Dict[str, Any]:
        return {
            "total_energy": 0.0,
            "total_latency": 0.0,
            "agents": [],
            "by_node": {},
            "failed_nodes": [],
            "node_count": 0,
            "wall_time_ms": 0.0,
            "status_counts": {},
        }

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _node_id_of(node: Any) -> str:
        """Return the node's identifier (``node_id`` attr or identity hash)."""
        node_id = getattr(node, "node_id", None)
        if isinstance(node_id, str) and node_id:
            return node_id
        return f"node@{id(node)}"

    @staticmethod
    def _node_id_from_raw(raw: Any, *, fallback: str) -> str:
        if isinstance(raw, Mapping):
            for key in ("node_id", "agent_id", "id"):
                value = raw.get(key)
                if isinstance(value, str) and value:
                    return value
        node_id = getattr(raw, "node_id", None)
        if isinstance(node_id, str) and node_id:
            return node_id
        return fallback

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded runs."""
        with self._lock:
            history = list(self._history)
            node_count = len(self._nodes)

        if not history:
            return {
                "runs": 0,
                "nodes_registered": node_count,
                "total_completed": 0,
                "total_failed": 0,
                "total_timed_out": 0,
                "mean_wall_time_ms": None,
                "mean_energy_kwh": None,
                "mean_latency_ms": None,
                "success_rate": None,
            }

        total_completed = sum(r.completed for r in history)
        total_failed = sum(r.failed for r in history)
        total_timeouts = sum(r.timed_out for r in history)
        walls = [r.wall_time_ms for r in history]
        energies = [r.total_energy_kwh for r in history]
        latencies = [r.total_latency_ms for r in history]
        total_attempts = total_completed + total_failed + total_timeouts

        return {
            "runs": len(history),
            "nodes_registered": node_count,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "total_timed_out": total_timeouts,
            "mean_wall_time_ms": sum(walls) / len(walls),
            "mean_energy_kwh": sum(energies) / len(energies),
            "mean_latency_ms": sum(latencies) / len(latencies),
            "success_rate": (
                total_completed / total_attempts
                if total_attempts > 0
                else None
            ),
        }

    def reset(self, *, clear_nodes: bool = False, clear_history: bool = False) -> None:
        """Reset in-memory state; optionally drop nodes / history."""
        with self._lock:
            if clear_nodes:
                self._nodes.clear()
                self._node_ids.clear()
            if clear_history:
                self._history.clear()
        logger.debug(
            "DistributedCoordinator reset "
            "(clear_nodes=%s, clear_history=%s)",
            clear_nodes, clear_history,
        )

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
    ) -> "DistributedCoordinator":
        """
        Rebuild a coordinator from a dict produced by :meth:`to_dict`.

        Live node objects are not serialized; pass ``nodes`` to re-inject
        them. History and configuration are restored exactly.
        """
        if not isinstance(data, Mapping):
            raise CoordinatorError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = CoordinatorConfig(
            max_concurrency=int(cfg_data.get("max_concurrency", 8)),
            per_node_timeout_seconds=float(
                cfg_data.get("per_node_timeout_seconds", 300.0)
            ),
            failure_policy=str(cfg_data.get("failure_policy", "best_effort")),
            missing_metric_policy=str(
                cfg_data.get("missing_metric_policy", "zero")
            ),
            max_nodes=int(cfg_data.get("max_nodes", 128)),
            max_history=int(cfg_data.get("max_history", 1000)),
            metric_energy_key=str(
                cfg_data.get("metric_energy_key", "energy_kwh")
            ),
            metric_latency_key=str(
                cfg_data.get("metric_latency_key", "latency")
            ),
        )

        coordinator = cls(config=cfg, strict=bool(data.get("strict", True)))

        for node in nodes or []:
            try:
                coordinator.register(node)
            except CoordinatorError as exc:
                logger.warning("Skipping node during from_dict: %s", exc)

        with coordinator._lock:
            coordinator._started_at = float(
                data.get("started_at", time.time())
            )
            for entry in data.get("history", []):
                coordinator._history.append(
                    RunRecord(
                        timestamp=float(entry["timestamp"]),
                        node_count=int(entry["node_count"]),
                        completed=int(entry["completed"]),
                        failed=int(entry["failed"]),
                        timed_out=int(entry["timed_out"]),
                        total_energy_kwh=float(entry["total_energy_kwh"]),
                        total_latency_ms=float(entry["total_latency_ms"]),
                        wall_time_ms=float(entry["wall_time_ms"]),
                    )
                )
        return coordinator

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        nodes: Optional[Iterable[Any]] = None,
    ) -> "DistributedCoordinator":
        try:
            return cls.from_dict(json.loads(payload), nodes=nodes)
        except json.JSONDecodeError as exc:
            raise CoordinatorError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "DistributedCoordinator":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped DistributedCoordinator session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "DistributedCoordinator scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "DistributedCoordinator scope closed in %.4fs (%d node(s)).",
            elapsed, self.node_count,
        )

    async def __aenter__(self) -> "DistributedCoordinator":
        self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async DistributedCoordinator session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._async_ctx_start
            if self._async_ctx_start is not None
            else time.perf_counter()
        )
        self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async DistributedCoordinator scope exited with %s after "
                "%.4fs.", exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Async DistributedCoordinator scope closed in %.4fs (%d node(s)).",
            elapsed, self.node_count,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "DistributedCoordinator("
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
    "NodeResult",
    "RunRecord",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m distributed.coordinator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---------------------------------------------------------------- #
    # Fake AgentNode implementations for the smoke test.
    # ---------------------------------------------------------------- #
    class _FakeNode:
        def __init__(
            self,
            node_id: str,
            energy_kwh: float = 0.01,
            latency_ms: float = 50.0,
            fail: bool = False,
            delay_s: float = 0.0,
        ) -> None:
            self.node_id = node_id
            self._energy = energy_kwh
            self._latency = latency_ms
            self._fail = fail
            self._delay = delay_s

        def run(self, task_input: Any) -> Dict[str, Any]:
            if self._fail:
                raise RuntimeError(f"node {self.node_id} failure")
            if self._delay > 0:
                time.sleep(self._delay)
            return {
                "node_id": self.node_id,
                "task_input": task_input,
                "metrics": {
                    "energy_kwh": self._energy,
                    "latency": self._latency,
                },
            }

        async def run_async(self, task_input: Any) -> Dict[str, Any]:
            if self._fail:
                raise RuntimeError(f"node {self.node_id} failure")
            if self._delay > 0:
                await asyncio.sleep(self._delay)
            return {
                "node_id": self.node_id,
                "task_input": task_input,
                "metrics": {
                    "energy_kwh": self._energy,
                    "latency": self._latency,
                },
            }

    async def main() -> None:
        # ------------------------------------------------------------ #
        # 1. Happy path (parallel async)
        # ------------------------------------------------------------ #
        coord = DistributedCoordinator()
        for i in range(4):
            coord.register(_FakeNode(f"node-{i}", energy_kwh=0.01 * (i + 1),
                                     latency_ms=40 + i * 5))

        print("coordinator  :", coord)
        print("node count   :", len(coord))

        agg = await coord.run_all_async("classify this")
        print("aggregate    :", {
            "total_energy": round(agg["total_energy"], 6),
            "total_latency": round(agg["total_latency"], 2),
            "node_count": agg["node_count"],
            "wall_time_ms": round(agg["wall_time_ms"], 2),
            "status_counts": agg["status_counts"],
        })

        # ------------------------------------------------------------ #
        # 2. Partial failure with best_effort
        # ------------------------------------------------------------ #
        coord2 = DistributedCoordinator()
        coord2.register(_FakeNode("ok-1"))
        coord2.register(_FakeNode("boom", fail=True))
        coord2.register(_FakeNode("ok-2"))

        agg2 = await coord2.run_all_async("task")
        print("best_effort  :", {
            "completed": agg2["status_counts"].get("completed", 0),
            "failed": agg2["status_counts"].get("failed", 0),
            "failed_nodes": agg2["failed_nodes"],
        })

        # ------------------------------------------------------------ #
        # 3. Partial failure with strict policy
        # ------------------------------------------------------------ #
        strict = DistributedCoordinator(
            config=CoordinatorConfig(failure_policy="strict"),
        )
        strict.register(_FakeNode("ok-1"))
        strict.register(_FakeNode("boom", fail=True))
        try:
            await strict.run_all_async("task")
        except CoordinatorError as exc:
            print("strict err   :", exc)

        # ------------------------------------------------------------ #
        # 4. Timeout handling
        # ------------------------------------------------------------ #
        to_coord = DistributedCoordinator(
            config=CoordinatorConfig(per_node_timeout_seconds=0.1),
        )
        to_coord.register(_FakeNode("fast"))
        to_coord.register(_FakeNode("slow", delay_s=0.5))
        agg_to = await to_coord.run_all_async("task")
        print("timeout      :", agg_to["status_counts"])

        # ------------------------------------------------------------ #
        # 5. Serial fallback (parallel=False)
        # ------------------------------------------------------------ #
        serial_agg = await coord.run_all_async("task", parallel=False)
        print("serial wall  :", round(serial_agg["wall_time_ms"], 2), "ms")

        # ------------------------------------------------------------ #
        # 6. Legacy aggregate() on raw dicts
        # ------------------------------------------------------------ #
        raw = [
            {"metrics": {"energy_kwh": 0.01, "latency": 50.0}},
            {"metrics": {"energy_kwh": 0.02, "latency": 60.0}},
        ]
        legacy = coord.aggregate(raw)
        print("legacy agg   :", {
            "total_energy": legacy["total_energy"],
            "total_latency": legacy["total_latency"],
        })

        # ------------------------------------------------------------ #
        # 7. Statistics
        # ------------------------------------------------------------ #
        print("statistics   :", {
            k: (round(v, 4) if isinstance(v, float) else v)
            for k, v in coord.statistics().items()
        })

        # ------------------------------------------------------------ #
        # 8. Serialization round-trip
        # ------------------------------------------------------------ #
        payload = coord.to_json()
        restored = DistributedCoordinator.from_json(
            payload,
            nodes=[_FakeNode(f"restored-{i}") for i in range(2)],
        )
        # Note: node identity differs after re-injection; compare config +
        # history only.
        assert restored.to_dict()["config"] == coord.to_dict()["config"]
        assert restored.to_dict()["history"] == coord.to_dict()["history"]
        print("Serialization round-trip OK.")

        # ------------------------------------------------------------ #
        # 9. Context managers
        # ------------------------------------------------------------ #
        with DistributedCoordinator() as scoped:
            scoped.register(_FakeNode("ctx-1"))
            scoped.register(_FakeNode("ctx-2"))
            assert len(scoped) == 2
        print("Sync context OK.")

        async with DistributedCoordinator() as scoped:
            scoped.register(_FakeNode("actx-1"))
            await scoped.run_all_async("task")
        print("Async context OK.")

        # ------------------------------------------------------------ #
        # 10. Dedup / unregister / capacity
        # ------------------------------------------------------------ #
        dedupe = DistributedCoordinator()
        n = _FakeNode("dup")
        dedupe.register(n)
        dedupe.register(n)  # duplicate
        assert len(dedupe) == 1
        assert dedupe.unregister("dup") is True
        assert dedupe.unregister("dup") is False
        print("dedup / unregister OK.")

        # ------------------------------------------------------------ #
        # 11. Validation failures
        # ------------------------------------------------------------ #
        for bad_cfg in (
            dict(max_concurrency=0),
            dict(per_node_timeout_seconds=0),
            dict(failure_policy="bogus"),
            dict(missing_metric_policy="bogus"),
            dict(max_nodes=0),
            dict(max_history=0),
        ):
            try:
                CoordinatorConfig(**bad_cfg)  # type: ignore[arg-type]
            except CoordinatorError as exc:
                print("Rejected cfg :", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected config rejection: {bad_cfg!r}")

        try:
            DistributedCoordinator().register(None)
        except CoordinatorError as exc:
            print("Rejected node:", exc)

        # Empty coordinator in strict mode.
        try:
            await DistributedCoordinator().run_all_async("task")
        except CoordinatorError as exc:
            print("Empty coord  :", exc)

        # Missing metric with error policy.
        strict_metrics = DistributedCoordinator(
            config=CoordinatorConfig(missing_metric_policy="error"),
        )
        strict_metrics.register(_FakeNode("bad-shape"))
        strict_metrics._nodes[0].run = lambda _inp: {"unrelated": True}  # type: ignore[assignment]
        try:
            await strict_metrics.run_all_async("task")
        except CoordinatorError as exc:
            print("Missing mtrc :", exc)

        print("\nSmoke test passed.")

    asyncio.run(main())
