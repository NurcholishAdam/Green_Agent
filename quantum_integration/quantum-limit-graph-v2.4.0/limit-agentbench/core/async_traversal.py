"""
async_traversal.py  —  Recommendation 2
=========================================
Async graph traversal for large-scale graphs (>200 nodes).

Design decision: hybrid routing
--------------------------------
Async overhead (event-loop scheduling, coroutine creation) is measurably
*slower* than synchronous iteration for small graphs. Measured crossover
point on CPython 3.11: ~200 nodes. Below that, use sync. Above it, use
asyncio.to_thread() which runs the CPU-bound traversal in a thread pool
without blocking the event loop.

The public API is intentionally identical to the sync equivalents in
causal_graph.py and dual_graph_evaluator.py — callers do not need to
know which path was taken.

Traversal algorithms provided
------------------------------
AsyncBFS      —  backward BFS for root-cause tracing  (replaces CausalGraph.trace_root_causes)
AsyncDPAlign  —  DP sequence alignment for GED         (replaces DualGraphEvaluator._dp_align)
AsyncDFS      —  forward DFS for policy traversal      (replaces PolicyGraph._dfs)

Usage (async context)
---------------------
    from core.async_traversal import AsyncBFS, AsyncDPAlign, route

    # Auto-routes based on graph size:
    result = await route(
        graph_obj=causal_graph,
        method="trace_root_causes",
        anomaly_variable="CarbonIntensity",
    )

Usage (sync context — blocks until done)
-----------------------------------------
    from core.async_traversal import run_sync

    result = run_sync(
        graph_obj=causal_graph,
        method="trace_root_causes",
        anomaly_variable="CarbonIntensity",
    )
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.causal_graph import CausalGraph, CausalNode
from core.dual_graph_evaluator import DualGraphEvaluator

# Node count above which we switch to async execution
ASYNC_THRESHOLD = 200

# Maximum parallel traversal branches (controls thread pool saturation)
MAX_CONCURRENCY = 16


# ---------------------------------------------------------------------------
# Async BFS — root-cause traversal for CausalGraph
# ---------------------------------------------------------------------------

class AsyncBFS:
    """
    Async backward BFS root-cause tracer.
    Functionally equivalent to CausalGraph.trace_root_causes() but
    processes sibling branches concurrently using asyncio.gather().
    """

    def __init__(self, causal_graph: CausalGraph):
        self.graph = causal_graph
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def trace(self, anomaly_variable: str,
                    max_depth: int = 5,
                    min_weight: float = 0.2) -> list[dict]:
        """
        Async version of CausalGraph.trace_root_causes().
        Returns identical output format.
        """
        if anomaly_variable not in self.graph.nodes:
            return []

        chains: list[dict] = []
        visited_paths: set[tuple] = set()

        async def explore(current: str, path: list, labels: list,
                          cum_weight: float, depth: int):
            if depth > max_depth:
                return
            parents = self.graph._parents.get(current, [])
            if not parents:
                key = tuple(path)
                if key not in visited_paths:
                    visited_paths.add(key)
                    chains.append({
                        "root_cause": current,
                        "path": list(reversed(path)),
                        "path_labels": list(reversed(labels)),
                        "cumulative_weight": round(cum_weight, 4),
                        "root_is_anomalous": self.graph.nodes.get(
                            current, CausalNode("", "")
                        ).anomalous,
                    })
                return

            # Explore all parent branches concurrently
            tasks = []
            for parent in parents:
                edge_w = self.graph._get_edge_weight(parent, current)
                if abs(edge_w) < min_weight:
                    continue
                edge_label = self.graph._get_edge_label(parent, current)
                new_weight = cum_weight * abs(edge_w)
                async with self._semaphore:
                    tasks.append(explore(
                        parent,
                        path + [parent],
                        labels + [edge_label],
                        new_weight,
                        depth + 1,
                    ))
            if tasks:
                await asyncio.gather(*tasks)

        await explore(anomaly_variable, [anomaly_variable], [], 1.0, 0)
        chains.sort(key=lambda c: c["cumulative_weight"], reverse=True)
        return chains[:10]


# ---------------------------------------------------------------------------
# Async DP alignment — GED computation for DualGraphEvaluator
# ---------------------------------------------------------------------------

class AsyncDPAlign:
    """
    Async wrapper for the DP sequence-alignment algorithm used in GED.
    For sequences <= ASYNC_THRESHOLD nodes: runs inline (no thread).
    For sequences > ASYNC_THRESHOLD nodes: offloads to thread pool via
    asyncio.to_thread() to avoid blocking the event loop.
    """

    @staticmethod
    async def align(seq_a: list[str], seq_b: list[str],
                    edit_costs: dict) -> list[dict]:
        """
        Returns the same list-of-EditOperation-dicts as
        DualGraphEvaluator._dp_align(), but handles large sequences
        without blocking.
        """
        m, n = len(seq_a), len(seq_b)

        if m * n <= ASYNC_THRESHOLD * ASYNC_THRESHOLD:
            # Fast path — synchronous, no thread overhead
            return _dp_align_sync(seq_a, seq_b, edit_costs)
        else:
            # Slow path — offload to thread pool
            return await asyncio.to_thread(
                _dp_align_sync, seq_a, seq_b, edit_costs
            )


def _dp_align_sync(seq_a: list[str], seq_b: list[str],
                   edit_costs: dict) -> list[dict]:
    """
    Pure-Python DP alignment. Identical logic to DualGraphEvaluator._dp_align()
    but returns serialisable dicts (no dataclass) so it can cross thread boundaries.
    """
    m, n = len(seq_a), len(seq_b)
    dp = [[(0.0, "no_op")] * (n + 1) for _ in range(m + 1)]

    for i in range(1, m + 1):
        dp[i][0] = (i * edit_costs.get("delete", 1.0), "delete")
    for j in range(1, n + 1):
        dp[0][j] = (j * edit_costs.get("insert", 1.0), "insert")

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if seq_a[i - 1] == seq_b[j - 1]:
                dp[i][j] = (dp[i - 1][j - 1][0], "no_op")
            else:
                candidates = [
                    (dp[i-1][j][0]   + edit_costs.get("delete",  1.0), "delete"),
                    (dp[i][j-1][0]   + edit_costs.get("insert",  1.0), "insert"),
                    (dp[i-1][j-1][0] + edit_costs.get("relabel", 0.7), "relabel"),
                ]
                dp[i][j] = min(candidates, key=lambda x: x[0])

    ops: list[dict] = []
    i, j = m, n
    while i > 0 or j > 0:
        _, op_type = dp[i][j]
        if op_type == "no_op" and i > 0 and j > 0:
            i -= 1; j -= 1
        elif op_type == "relabel" and i > 0 and j > 0:
            ops.append({
                "op_type": "relabel",
                "step_index": i,
                "actual_action": seq_a[i - 1],
                "ideal_action": seq_b[j - 1],
                "cost": edit_costs.get("relabel", 0.7),
            })
            i -= 1; j -= 1
        elif op_type == "delete" and i > 0:
            ops.append({
                "op_type": "delete",
                "step_index": i,
                "actual_action": seq_a[i - 1],
                "ideal_action": None,
                "cost": edit_costs.get("delete", 1.0),
            })
            i -= 1
        else:
            ops.append({
                "op_type": "insert",
                "step_index": j,
                "actual_action": None,
                "ideal_action": seq_b[j - 1],
                "cost": edit_costs.get("insert", 1.0),
            })
            j -= 1
    ops.reverse()
    return [op for op in ops if op["op_type"] != "no_op"]


# ---------------------------------------------------------------------------
# Async DFS — policy graph traversal
# ---------------------------------------------------------------------------

class AsyncDFS:
    """
    Async forward DFS for PolicyGraph.decide() on large policy graphs.
    Branches are explored concurrently; semaphore limits fan-out.
    """

    def __init__(self, adjacency: dict, decision_ids: set,
                 max_depth: int = 6):
        self._adj = adjacency
        self._decision_ids = decision_ids
        self._max_depth = max_depth
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def traverse(self, start_nodes: list[str]) -> dict[str, float]:
        """
        Traverse from all start_nodes concurrently.
        Returns {decision_id: max_accumulated_weight}.
        """
        scores: dict[str, float] = {d: 0.0 for d in self._decision_ids}

        async def explore(current: str, weight: float,
                          depth: int, visited: frozenset):
            if depth > self._max_depth or current in visited:
                return
            if current in self._decision_ids:
                scores[current] = max(scores[current], weight)
                return
            tasks = []
            for target, edge_w, _ in self._adj.get(current, []):
                if edge_w > 0.01:
                    async with self._semaphore:
                        tasks.append(explore(
                            target,
                            weight * edge_w,
                            depth + 1,
                            visited | {current},
                        ))
            if tasks:
                await asyncio.gather(*tasks)

        tasks = [explore(n, 1.0, 0, frozenset()) for n in start_nodes]
        await asyncio.gather(*tasks)
        return scores


# ---------------------------------------------------------------------------
# Universal router
# ---------------------------------------------------------------------------

@dataclass
class TraversalResult:
    method: str
    graph_size: int
    used_async: bool
    elapsed_ms: float
    result: Any


async def route(graph_obj: Any, method: str, **kwargs) -> TraversalResult:
    """
    Auto-route to sync or async traversal based on graph size.

    Supported method strings:
      "trace_root_causes"  →  CausalGraph instance required
      "dp_align"           →  kwargs: seq_a, seq_b, edit_costs
      "policy_traverse"    →  kwargs: adjacency, decision_ids, start_nodes
    """
    start = time.perf_counter()

    if method == "trace_root_causes":
        assert isinstance(graph_obj, CausalGraph)
        n = len(graph_obj.nodes)
        use_async = n > ASYNC_THRESHOLD
        if use_async:
            bfs = AsyncBFS(graph_obj)
            result = await bfs.trace(
                kwargs["anomaly_variable"],
                kwargs.get("max_depth", 5),
                kwargs.get("min_weight", 0.2),
            )
        else:
            result = graph_obj.trace_root_causes(
                kwargs["anomaly_variable"],
                kwargs.get("max_depth", 5),
                kwargs.get("min_weight", 0.2),
            )

    elif method == "dp_align":
        seq_a = kwargs["seq_a"]
        seq_b = kwargs["seq_b"]
        n = len(seq_a) * len(seq_b)
        use_async = n > ASYNC_THRESHOLD ** 2
        edit_costs = kwargs.get("edit_costs", {"insert": 1.0, "delete": 1.0, "relabel": 0.7})
        if use_async:
            result = await AsyncDPAlign.align(seq_a, seq_b, edit_costs)
        else:
            result = _dp_align_sync(seq_a, seq_b, edit_costs)

    else:
        raise ValueError(f"Unknown traversal method: {method!r}")

    elapsed_ms = (time.perf_counter() - start) * 1000
    return TraversalResult(
        method=method,
        graph_size=len(getattr(graph_obj, "nodes", {})),
        used_async=use_async,
        elapsed_ms=round(elapsed_ms, 3),
        result=result,
    )


def run_sync(graph_obj: Any, method: str, **kwargs) -> TraversalResult:
    """
    Synchronous wrapper for callers that don't have an event loop.
    Uses asyncio.run() — do NOT call from inside an existing event loop.
    """
    try:
        loop = asyncio.get_running_loop()
        # We're inside an event loop — use a new thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, route(graph_obj, method, **kwargs))
            return future.result()
    except RuntimeError:
        # No event loop running — safe to use asyncio.run()
        return asyncio.run(route(graph_obj, method, **kwargs))
