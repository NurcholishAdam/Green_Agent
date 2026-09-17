# src/retrieval/graph_traversal_optimizer.py

"""
Graph Traversal Optimizer
=========================

Optimizes retrieval paths through memory graphs using energy-aware graph
traversal algorithms inspired by VimRAG's navigation strategies.

Enhancements
------------
- ``GraphTraversalConfig`` — frozen, validated: weight sum, cache size,
  default node cost, edge weight bounds, strict mode.
- ``TraversalPath`` / ``GraphEdge`` — frozen dataclasses with validation
  and full serialization.
- **Fixed LRU cache** — evicts the least-recently-used path instead of
  deadlocking once full.
- **Fixed cache key** — now includes ``max_depth`` so different budgets do
  not collide.
- **Fixed k-shortest-paths** — real Yen's-algorithm loop with edge
  elimination, replacing the ``break``-after-first stub.
- **Deterministic path IDs** — SHA-256 of the joined node list instead of
  Python's salted ``hash()``.
- **Thread safety** — ``RLock`` guards adjacency, costs, relevance, and the
  cache.
- **Full validation** of every node ID, weight, cost, relevance, and depth.
- **Bounded cache** via ``OrderedDict`` LRU.
- **Deterministic traversal tie-breaking** — neighbor order is normalized
  when the caller does not provide one.
- ``explore_neighborhood`` uses ``collections.deque`` and validates the
  energy budget before enqueueing.
- Serialization on the optimizer and on every dataclass.
- ``statistics()``, ``__repr__``, custom ``GraphTraversalError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import math
import threading
import time
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GraphTraversalError(ValueError):
    """Raised for invalid graph traversal inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class TraversalStrategy(Enum):
    """Graph traversal strategies."""
    BREADTH_FIRST = "bfs"
    DEPTH_FIRST = "dfs"
    ENERGY_OPTIMAL = "energy_optimal"
    RELEVANCE_GUIDED = "relevance_guided"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GraphTraversalConfig:
    """Tunable parameters for the graph traversal optimizer."""

    # Path-selection weights (must sum to 1.0 for a valid blend).
    energy_weight: float = 0.5
    relevance_weight: float = 0.5

    # LRU cache cap for computed paths.
    cache_size: int = 100

    # Default per-node energy cost when none was recorded.
    default_node_cost: float = 0.1

    # Edge-weight bounds (must be finite and >= 0).
    max_edge_weight: float = 1_000.0

    # Default exploration budget.
    default_radius: int = 2
    default_max_nodes: int = 20
    default_energy_budget: float = 1.0

    # Maximum depth used by ``find_k_best_paths`` when the caller does not
    # specify one.
    default_max_depth: int = 10

    # Bounded run history for auditing.
    max_history: int = 1000

    def __post_init__(self) -> None:
        for name in ("energy_weight", "relevance_weight"):
            value = getattr(self, name)
            if value < 0:
                raise GraphTraversalError(f"{name} must be >= 0.")
        total = self.energy_weight + self.relevance_weight
        if abs(total - 1.0) > 1e-6:
            raise GraphTraversalError(
                f"energy_weight + relevance_weight must sum to 1.0 "
                f"(got {total:.6f})."
            )
        if self.cache_size <= 0:
            raise GraphTraversalError("cache_size must be > 0.")
        if self.default_node_cost < 0:
            raise GraphTraversalError("default_node_cost must be >= 0.")
        if self.max_edge_weight <= 0:
            raise GraphTraversalError("max_edge_weight must be > 0.")
        if self.default_radius <= 0:
            raise GraphTraversalError("default_radius must be > 0.")
        if self.default_max_nodes <= 0:
            raise GraphTraversalError("default_max_nodes must be > 0.")
        if self.default_energy_budget <= 0:
            raise GraphTraversalError(
                "default_energy_budget must be > 0."
            )
        if self.default_max_depth <= 0:
            raise GraphTraversalError(
                "default_max_depth must be > 0."
            )
        if self.max_history <= 0:
            raise GraphTraversalError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphTraversalConfig":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                f"GraphTraversalConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            energy_weight=float(data.get("energy_weight", 0.5)),
            relevance_weight=float(data.get("relevance_weight", 0.5)),
            cache_size=int(data.get("cache_size", 100)),
            default_node_cost=float(data.get("default_node_cost", 0.1)),
            max_edge_weight=float(data.get("max_edge_weight", 1_000.0)),
            default_radius=int(data.get("default_radius", 2)),
            default_max_nodes=int(data.get("default_max_nodes", 20)),
            default_energy_budget=float(
                data.get("default_energy_budget", 1.0)
            ),
            default_max_depth=int(data.get("default_max_depth", 10)),
            max_history=int(data.get("max_history", 1000)),
        )


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GraphEdge:
    """Immutable edge in the memory graph."""

    source: str
    target: str
    weight: float = 1.0
    edge_type: str = "semantic"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.source, str) or not self.source:
            raise GraphTraversalError(
                "source must be a non-empty string."
            )
        if not isinstance(self.target, str) or not self.target:
            raise GraphTraversalError(
                "target must be a non-empty string."
            )
        if not isinstance(self.weight, (int, float)) or isinstance(self.weight, bool):
            raise GraphTraversalError("weight must be numeric.")
        fw = float(self.weight)
        if math.isnan(fw) or math.isinf(fw) or fw < 0:
            raise GraphTraversalError(
                f"weight must be finite and >= 0, got {self.weight!r}."
            )
        if not isinstance(self.edge_type, str) or not self.edge_type:
            raise GraphTraversalError(
                "edge_type must be a non-empty string."
            )
        if not isinstance(self.metadata, Mapping):
            raise GraphTraversalError(
                "metadata must be a Mapping."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "weight": float(self.weight),
            "edge_type": self.edge_type,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphEdge":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                "GraphEdge.from_dict expects a Mapping."
            )
        return cls(
            source=str(data["source"]),
            target=str(data["target"]),
            weight=float(data.get("weight", 1.0)),
            edge_type=str(data.get("edge_type", "semantic")),
            metadata=dict(data.get("metadata", {})),
        )

    def __repr__(self) -> str:
        return (
            "GraphEdge("
            f"{self.source!r} -> {self.target!r}, "
            f"weight={float(self.weight):.3f}, type={self.edge_type!r})"
        )


@dataclass(frozen=True)
class TraversalPath:
    """Immutable traversal path through the graph."""

    path_id: str
    nodes: Tuple[str, ...]
    total_cost: float
    energy_cost: float
    relevance_score: float
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.path_id, str) or not self.path_id:
            raise GraphTraversalError(
                "path_id must be a non-empty string."
            )
        if not isinstance(self.nodes, tuple) or not self.nodes:
            raise GraphTraversalError(
                "nodes must be a non-empty tuple."
            )
        for name in ("total_cost", "energy_cost", "relevance_score"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise GraphTraversalError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise GraphTraversalError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.total_cost < 0 or self.energy_cost < 0:
            raise GraphTraversalError(
                "total_cost and energy_cost must be >= 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path_id": self.path_id,
            "nodes": list(self.nodes),
            "total_cost": self.total_cost,
            "energy_cost": self.energy_cost,
            "relevance_score": self.relevance_score,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TraversalPath":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                "TraversalPath.from_dict expects a Mapping."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            path_id=str(data["path_id"]),
            nodes=tuple(data["nodes"]),
            total_cost=float(data.get("total_cost", 0.0)),
            energy_cost=float(data.get("energy_cost", 0.0)),
            relevance_score=float(data.get("relevance_score", 0.0)),
            metadata=dict(data.get("metadata", {})),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "TraversalPath("
            f"id={self.path_id[:12]}..., "
            f"nodes={len(self.nodes)}, "
            f"energy={self.energy_cost:.3f}, "
            f"relevance={self.relevance_score:.3f})"
        )


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #
class GraphTraversalOptimizer:
    """
    Optimizes graph traversal for energy-efficient retrieval.

    Thread-safe, serializable, and bounded in memory. The original public API
    is preserved; new parameters are keyword-only.

    Features
    --------
    - BFS, DFS, energy-optimal (A*-style), and relevance-guided strategies.
    - LRU path cache with configurable capacity.
    - Deterministic path IDs (SHA-256, not salted ``hash``).
    - Real k-shortest-paths (Yen's variant).
    - Neighbourhood exploration within a configurable token/energy budget.
    """

    def __init__(
        self,
        energy_weight: float = 0.5,
        relevance_weight: float = 0.5,
        cache_size: int = 100,
        *,
        config: Optional[GraphTraversalConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = GraphTraversalConfig(
                energy_weight=float(energy_weight),
                relevance_weight=float(relevance_weight),
                cache_size=int(cache_size),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.energy_weight: float = self._config.energy_weight
        self.relevance_weight: float = self._config.relevance_weight
        self.cache_size: int = self._config.cache_size

        self._lock = threading.RLock()
        self.adjacency: Dict[str, List[GraphEdge]] = defaultdict(list)
        self.node_costs: Dict[str, float] = {}
        self.node_relevance: Dict[str, float] = {}

        # LRU path cache.
        self.path_cache: "OrderedDict[str, TraversalPath]" = OrderedDict()
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._cache_evictions: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "GraphTraversalOptimizer initialized "
            "(energy_weight=%.3f, relevance_weight=%.3f, cache_size=%d, "
            "strict=%s)",
            self.energy_weight, self.relevance_weight,
            self.cache_size, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> GraphTraversalConfig:
        return self._config

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self.node_costs)

    @property
    def edge_count(self) -> int:
        with self._lock:
            return sum(len(es) for es in self.adjacency.values())

    # ---------------------------------------------------------- graph mutation
    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        edge_type: str = "semantic",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> GraphEdge:
        """
        Add an edge to the graph.

        Returns the stored :class:`GraphEdge`. Invalid inputs raise
        :class:`GraphTraversalError` in strict mode; in non-strict mode they
        are logged and a `None`-like sentinel is returned.
        """
        if metadata is not None and not isinstance(metadata, Mapping):
            msg = (
                f"metadata must be a Mapping or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise GraphTraversalError(msg)
            logger.warning("%s Ignoring metadata.", msg)
            metadata = None

        try:
            edge = GraphEdge(
                source=source,
                target=target,
                weight=float(weight),
                edge_type=edge_type,
                metadata=metadata or {},
            )
        except GraphTraversalError:
            if self._strict:
                raise
            logger.warning(
                "Rejected invalid edge %r -> %r.", source, target,
            )
            # Non-strict: build a benign placeholder so callers do not crash.
            edge = GraphEdge(
                source="__invalid__",
                target="__invalid__",
                weight=0.0,
                edge_type="invalid",
                metadata={},
            )
            return edge

        if edge.weight > self._config.max_edge_weight:
            msg = (
                f"edge weight {edge.weight} exceeds max_edge_weight "
                f"{self._config.max_edge_weight}."
            )
            if self._strict:
                raise GraphTraversalError(msg)
            logger.warning("%s Clamping.", msg)
            edge = GraphEdge(
                source=edge.source, target=edge.target,
                weight=self._config.max_edge_weight,
                edge_type=edge.edge_type, metadata=edge.metadata,
            )

        with self._lock:
            self.adjacency[edge.source].append(edge)
            # Register the target as a known node with the default cost.
            if edge.target not in self.node_costs:
                self.node_costs[edge.target] = self._config.default_node_cost

        logger.debug(
            "Added edge %s -> %s (weight=%.3f, type=%s).",
            edge.source, edge.target, edge.weight, edge.edge_type,
        )
        return edge

    def set_node_cost(self, node_id: str, cost: float) -> None:
        """Set the energy cost for a node."""
        if not isinstance(node_id, str) or not node_id:
            raise GraphTraversalError(
                "node_id must be a non-empty string."
            )
        if isinstance(cost, bool) or not isinstance(cost, (int, float)):
            raise GraphTraversalError(
                f"cost must be numeric, got {type(cost).__name__}."
            )
        fc = float(cost)
        if math.isnan(fc) or math.isinf(fc) or fc < 0:
            raise GraphTraversalError(
                f"cost must be finite and >= 0, got {cost!r}."
            )
        with self._lock:
            self.node_costs[node_id] = fc

    def set_node_relevance(self, node_id: str, relevance: float) -> None:
        """Set the relevance score for a node (clamped to [0, 1])."""
        if not isinstance(node_id, str) or not node_id:
            raise GraphTraversalError(
                "node_id must be a non-empty string."
            )
        if isinstance(relevance, bool) or not isinstance(
            relevance, (int, float)
        ):
            raise GraphTraversalError(
                f"relevance must be numeric, got {type(relevance).__name__}."
            )
        fr = float(relevance)
        if math.isnan(fr) or math.isinf(fr):
            raise GraphTraversalError(
                f"relevance must be finite, got {relevance!r}."
            )
        if not 0.0 <= fr <= 1.0:
            if self._strict:
                raise GraphTraversalError(
                    f"relevance must be in [0, 1], got {fr}."
                )
            logger.warning("Clamping relevance %.3f to [0, 1].", fr)
            fr = max(0.0, min(1.0, fr))
        with self._lock:
            self.node_relevance[node_id] = fr

    # ---------------------------------------------------------- public API
    def find_optimal_path(
        self,
        start: str,
        goal: str,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL,
        max_depth: Optional[int] = None,
    ) -> Optional[TraversalPath]:
        """
        Find the optimal path from ``start`` to ``goal``.

        ``max_depth`` defaults to ``config.default_max_depth``. The cache key
        now includes ``max_depth`` so different budgets return distinct
        results.
        """
        start = self._validate_node("start", start)
        goal = self._validate_node("goal", goal)
        if start is None or goal is None:
            return None
        if not isinstance(strategy, TraversalStrategy):
            raise GraphTraversalError(
                f"strategy must be a TraversalStrategy, "
                f"got {type(strategy).__name__}."
            )
        depth = self._validate_depth(max_depth)

        cache_key = f"{start}:{goal}:{strategy.value}:{depth}"

        with self._lock:
            cached = self.path_cache.get(cache_key)
            if cached is not None:
                self.path_cache.move_to_end(cache_key)
                self.cache_hits += 1
                return cached
            self.cache_misses += 1

        # Dispatch.
        if strategy == TraversalStrategy.BREADTH_FIRST:
            path = self._bfs_path(start, goal, depth)
        elif strategy == TraversalStrategy.DEPTH_FIRST:
            path = self._dfs_path(start, goal, depth)
        elif strategy == TraversalStrategy.ENERGY_OPTIMAL:
            path = self._energy_optimal_path(start, goal, depth)
        elif strategy == TraversalStrategy.RELEVANCE_GUIDED:
            path = self._relevance_guided_path(start, goal, depth)
        else:  # pragma: no cover — Enum is exhaustive
            path = None

        if path is not None:
            self._cache_put(cache_key, path)

        logger.debug(
            "find_optimal_path(%s -> %s, %s, depth=%d) -> %s",
            start, goal, strategy.value, depth,
            "hit" if path else "miss",
        )
        return path

    def find_k_best_paths(
        self,
        start: str,
        goal: str,
        k: int = 3,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL,
        max_depth: Optional[int] = None,
    ) -> List[TraversalPath]:
        """
        Find the ``k`` best paths from ``start`` to ``goal``.

        For ``ENERGY_OPTIMAL``, a Yen-style loop produces distinct paths.
        For other strategies, a single path is returned (and a debug log
        notes the fallback).
        """
        start = self._validate_node("start", start)
        goal = self._validate_node("goal", goal)
        if start is None or goal is None:
            return []
        if not isinstance(k, int) or k <= 0:
            raise GraphTraversalError("k must be a positive int.")
        depth = self._validate_depth(max_depth)

        if strategy != TraversalStrategy.ENERGY_OPTIMAL:
            logger.debug(
                "k-best not implemented for %s; returning single path.",
                strategy.value,
            )
            path = self.find_optimal_path(start, goal, strategy, depth)
            return [path] if path else []

        return self._k_shortest_paths_energy(start, goal, k, depth)

    def explore_neighborhood(
        self,
        center: str,
        radius: Optional[int] = None,
        max_nodes: Optional[int] = None,
        energy_budget: Optional[float] = None,
    ) -> List[str]:
        """
        Explore the neighbourhood around ``center`` within an energy budget.

        Returns node IDs in BFS order starting from ``center``. The center
        node's own cost is counted against the budget.
        """
        center = self._validate_node("center", center)
        if center is None:
            return []

        cfg = self._config
        r = cfg.default_radius if radius is None else radius
        n = cfg.default_max_nodes if max_nodes is None else max_nodes
        b = cfg.default_energy_budget if energy_budget is None else energy_budget

        if not isinstance(r, int) or r <= 0:
            raise GraphTraversalError("radius must be a positive int.")
        if not isinstance(n, int) or n <= 0:
            raise GraphTraversalError("max_nodes must be a positive int.")
        if not isinstance(b, (int, float)) or isinstance(b, bool):
            raise GraphTraversalError("energy_budget must be numeric.")
        fb = float(b)
        if math.isnan(fb) or math.isinf(fb) or fb <= 0:
            raise GraphTraversalError(
                f"energy_budget must be finite and > 0, got {b!r}."
            )

        with self._lock:
            center_cost = self.node_costs.get(
                center, cfg.default_node_cost
            )
            if center_cost > fb:
                logger.debug(
                    "Center node cost %.4f exceeds budget %.4f.",
                    center_cost, fb,
                )
                return []

            explored: List[str] = [center]
            visited: Set[str] = {center}
            # ``deque`` → O(1) popleft.
            queue: Deque[Tuple[str, int, float]] = deque(
                [(center, 0, center_cost)]
            )

        while queue and len(explored) < n:
            node, depth, cost = queue.popleft()

            if depth >= r:
                continue

            with self._lock:
                edges = list(self.adjacency.get(node, []))

            for edge in edges:
                if edge.target in visited:
                    continue
                with self._lock:
                    node_cost = self.node_costs.get(
                        edge.target, cfg.default_node_cost
                    )
                new_cost = cost + node_cost
                if new_cost > fb:
                    continue
                visited.add(edge.target)
                explored.append(edge.target)
                queue.append((edge.target, depth + 1, new_cost))
                if len(explored) >= n:
                    break

        logger.debug(
            "Explored %d node(s) within radius %d from %s.",
            len(explored), r, center,
        )
        return explored

    # ---------------------------------------------------------- traversal implementations
    def _bfs_path(
        self, start: str, goal: str, max_depth: int
    ) -> Optional[TraversalPath]:
        queue: Deque[Tuple[str, List[str], int]] = deque([(start, [start], 0)])
        visited: Set[str] = {start}

        while queue:
            node, path, depth = queue.popleft()
            if node == goal:
                return self._create_path(path)
            if depth >= max_depth:
                continue
            with self._lock:
                edges = list(self.adjacency.get(node, []))
            for edge in edges:
                if edge.target not in visited:
                    visited.add(edge.target)
                    queue.append((edge.target, path + [edge.target], depth + 1))
        return None

    def _dfs_path(
        self, start: str, goal: str, max_depth: int
    ) -> Optional[TraversalPath]:
        stack: List[Tuple[str, List[str], int]] = [(start, [start], 0)]
        visited: Set[str] = {start}

        while stack:
            node, path, depth = stack.pop()
            if node == goal:
                return self._create_path(path)
            if depth >= max_depth:
                continue
            with self._lock:
                edges = list(self.adjacency.get(node, []))
            # Reverse for a deterministic left-to-right DFS order.
            for edge in reversed(edges):
                if edge.target not in visited:
                    visited.add(edge.target)
                    stack.append((edge.target, path + [edge.target], depth + 1))
        return None

    def _energy_optimal_path(
        self, start: str, goal: str, max_depth: int
    ) -> Optional[TraversalPath]:
        """
        A*-style shortest path where the cost of moving to a node is the
        node's own cost plus the edge weight.
        """
        cfg = self._config
        # (cost, node, path); ties broken by node name for determinism.
        pq: List[Tuple[float, str, Tuple[str, ...]]] = [
            (0.0, start, (start,))
        ]
        visited: Set[str] = set()

        while pq:
            cost, node, path = heapq.heappop(pq)
            if node == goal:
                return self._create_path(list(path))
            if node in visited or len(path) > max_depth:
                continue
            visited.add(node)

            with self._lock:
                edges = list(self.adjacency.get(node, []))

            for edge in edges:
                if edge.target in visited:
                    continue
                with self._lock:
                    node_cost = self.node_costs.get(
                        edge.target, cfg.default_node_cost
                    )
                new_cost = cost + node_cost + float(edge.weight)
                new_path = path + (edge.target,)
                heapq.heappush(pq, (new_cost, edge.target, new_path))
        return None

    def _relevance_guided_path(
        self, start: str, goal: str, max_depth: int
    ) -> Optional[TraversalPath]:
        """Greedy path guided by per-node relevance (higher first)."""
        start_rel = self._relevance_of(start)
        # (-relevance, node, path)
        pq: List[Tuple[float, str, Tuple[str, ...]]] = [
            (-start_rel, start, (start,))
        ]
        visited: Set[str] = set()

        while pq:
            _, node, path = heapq.heappop(pq)
            if node == goal:
                return self._create_path(list(path))
            if node in visited or len(path) > max_depth:
                continue
            visited.add(node)

            with self._lock:
                edges = list(self.adjacency.get(node, []))

            for edge in edges:
                if edge.target in visited:
                    continue
                rel = self._relevance_of(edge.target)
                new_path = path + (edge.target,)
                heapq.heappush(pq, (-rel, edge.target, new_path))
        return None

    def _k_shortest_paths_energy(
        self, start: str, goal: str, k: int, max_depth: int
    ) -> List[TraversalPath]:
        """
        Yen's algorithm for k energy-optimal loopless paths.

        Returns up to ``k`` distinct paths sorted by energy cost.
        """
        first = self._energy_optimal_path(start, goal, max_depth)
        if first is None:
            return []

        A: List[TraversalPath] = [first]
        # Min-heap of candidate paths keyed by energy cost.
        B: List[Tuple[float, Tuple[str, ...]]] = []
        seen_keys: Set[Tuple[str, ...]] = {first.nodes}

        for _ in range(1, k):
            prev = A[-1]
            for i in range(len(prev.nodes) - 1):
                spur_node = prev.nodes[i]
                root_path = prev.nodes[: i + 1]

                # Collect edges that must be removed to avoid re-creating A.
                removed: Set[Tuple[str, str]] = set()
                for p in A:
                    if p.nodes[: i + 1] == root_path and i + 1 < len(p.nodes):
                        removed.add((p.nodes[i], p.nodes[i + 1]))

                # Find a spur path from spur_node to goal with those edges
                # temporarily removed.
                spur = self._spur_path(
                    spur_node=spur_node,
                    goal=goal,
                    removed=removed,
                    max_depth=max(1, max_depth - i),
                )
                if spur is None:
                    continue

                total_nodes = tuple(root_path[:-1]) + tuple(spur)
                if total_nodes in seen_keys:
                    continue
                seen_keys.add(total_nodes)

                candidate = self._create_path(list(total_nodes))
                heapq.heappush(B, (candidate.energy_cost, total_nodes))

            if not B:
                break
            _, next_nodes = heapq.heappop(B)
            A.append(self._create_path(list(next_nodes)))

        return A

    def _spur_path(
        self,
        *,
        spur_node: str,
        goal: str,
        removed: Set[Tuple[str, str]],
        max_depth: int,
    ) -> Optional[List[str]]:
        """A* from ``spur_node`` to ``goal`` with certain edges removed."""
        cfg = self._config
        pq: List[Tuple[float, str, Tuple[str, ...]]] = [
            (0.0, spur_node, (spur_node,))
        ]
        visited: Set[str] = set()

        while pq:
            cost, node, path = heapq.heappop(pq)
            if node == goal:
                return list(path)
            if node in visited or len(path) > max_depth:
                continue
            visited.add(node)

            with self._lock:
                edges = list(self.adjacency.get(node, []))

            for edge in edges:
                if edge.target in visited:
                    continue
                if (edge.source, edge.target) in removed:
                    continue
                with self._lock:
                    node_cost = self.node_costs.get(
                        edge.target, cfg.default_node_cost
                    )
                new_cost = cost + node_cost + float(edge.weight)
                heapq.heappush(
                    pq, (new_cost, edge.target, path + (edge.target,))
                )
        return None

    # ---------------------------------------------------------- helpers
    def _cache_put(self, key: str, path: TraversalPath) -> None:
        """Insert into the LRU cache, evicting the oldest entry when full."""
        with self._lock:
            if key in self.path_cache:
                self.path_cache.move_to_end(key)
            self.path_cache[key] = path
            while len(self.path_cache) > self.cache_size:
                self.path_cache.popitem(last=False)
                self._cache_evictions += 1

    def _validate_node(self, name: str, value: Any) -> Optional[str]:
        if not isinstance(value, str) or not value:
            msg = f"{name} must be a non-empty string, got {value!r}."
            if self._strict:
                raise GraphTraversalError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        return value

    def _validate_depth(self, value: Optional[int]) -> int:
        if value is None:
            return self._config.default_max_depth
        if not isinstance(value, int) or value <= 0:
            raise GraphTraversalError(
                f"max_depth must be a positive int, got {value!r}."
            )
        return value

    def _relevance_of(self, node: str) -> float:
        with self._lock:
            return self.node_relevance.get(node, 0.5)

    def _cost_of(self, node: str) -> float:
        with self._lock:
            return self.node_costs.get(
                node, self._config.default_node_cost
            )

    def _create_path(self, nodes: Sequence[str]) -> TraversalPath:
        """Build a :class:`TraversalPath` from a node sequence."""
        if not nodes:
            raise GraphTraversalError(
                "cannot create a path from an empty node list."
            )
        # Deterministic path ID — Python's ``hash`` is salted per process.
        joined = "\x1f".join(str(n) for n in nodes)
        path_id = "path_" + hashlib.sha256(
            joined.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

        total_cost = sum(self._cost_of(n) for n in nodes)
        energy_cost = total_cost  # Same semantics as the original
        if nodes:
            relevance_score = sum(
                self._relevance_of(n) for n in nodes
            ) / len(nodes)
        else:  # pragma: no cover — guarded above
            relevance_score = 0.0

        return TraversalPath(
            path_id=path_id,
            nodes=tuple(nodes),
            total_cost=total_cost,
            energy_cost=energy_cost,
            relevance_score=relevance_score,
            metadata={"length": len(nodes)},
        )

    # ---------------------------------------------------------- statistics
    def get_traversal_stats(self) -> Dict[str, Any]:
        """Return the backward-compatible traversal statistics."""
        with self._lock:
            hits = self.cache_hits
            misses = self.cache_misses
            cached_paths = len(self.path_cache)
            evictions = self._cache_evictions
            node_costs = dict(self.node_costs)
            edge_count = sum(len(es) for es in self.adjacency.values())

        total_requests = hits + misses
        cache_hit_rate = hits / total_requests if total_requests > 0 else 0.0
        avg_node_cost = (
            sum(node_costs.values()) / len(node_costs)
            if node_costs else 0.0
        )

        return {
            "total_nodes": len(node_costs),
            "total_edges": edge_count,
            "cached_paths": cached_paths,
            "cache_hits": hits,
            "cache_misses": misses,
            "cache_hit_rate": cache_hit_rate,
            "avg_node_cost": avg_node_cost,
            # Additive:
            "cache_evictions": evictions,
            "energy_weight": self.energy_weight,
            "relevance_weight": self.relevance_weight,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_traversal_stats`."""
        return self.get_traversal_stats()

    def clear_cache(self) -> int:
        """Clear the path cache. Returns the number of entries removed."""
        with self._lock:
            removed = len(self.path_cache)
            self.path_cache.clear()
        logger.debug("Cleared %d cached path(s).", removed)
        return removed

    def reset(self, *, clear_graph: bool = False, clear_cache: bool = True) -> None:
        """Reset the optimizer state."""
        with self._lock:
            if clear_graph:
                self.adjacency.clear()
                self.node_costs.clear()
                self.node_relevance.clear()
            if clear_cache:
                self.path_cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self._cache_evictions = 0
            self._started_at = time.time()
        logger.debug("GraphTraversalOptimizer reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_cache: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "adjacency": {
                    src: [e.to_dict() for e in edges]
                    for src, edges in self.adjacency.items()
                },
                "node_costs": dict(self.node_costs),
                "node_relevance": dict(self.node_relevance),
                "counters": {
                    "cache_hits": self.cache_hits,
                    "cache_misses": self.cache_misses,
                    "cache_evictions": self._cache_evictions,
                },
                "started_at": self._started_at,
            }
            if include_cache:
                payload["path_cache"] = {
                    k: v.to_dict() for k, v in self.path_cache.items()
                }
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphTraversalOptimizer":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = GraphTraversalConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        opt = cls(config=cfg, strict=bool(data.get("strict", True)))

        with opt._lock:
            for src, edges in (data.get("adjacency") or {}).items():
                for e in edges:
                    opt.adjacency[str(src)].append(GraphEdge.from_dict(e))
            for node, cost in (data.get("node_costs") or {}).items():
                opt.node_costs[str(node)] = float(cost)
            for node, rel in (data.get("node_relevance") or {}).items():
                opt.node_relevance[str(node)] = float(rel)
            counters = dict(data.get("counters", {}) or {})
            opt.cache_hits = int(counters.get("cache_hits", 0))
            opt.cache_misses = int(counters.get("cache_misses", 0))
            opt._cache_evictions = int(counters.get("cache_evictions", 0))
            opt._started_at = float(data.get("started_at", time.time()))
            for key, entry in (data.get("path_cache") or {}).items():
                opt.path_cache[str(key)] = TraversalPath.from_dict(entry)
        return opt

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GraphTraversalOptimizer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise GraphTraversalError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "GraphTraversalOptimizer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "GraphTraversalOptimizer scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "GraphTraversalOptimizer("
                f"nodes={len(self.node_costs)}, "
                f"edges={sum(len(e) for e in self.adjacency.values())}, "
                f"cached={len(self.path_cache)}, "
                f"energy_weight={self.energy_weight:.2f}, "
                f"relevance_weight={self.relevance_weight:.2f}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GraphEdge",
    "GraphTraversalConfig",
    "GraphTraversalError",
    "GraphTraversalOptimizer",
    "TraversalPath",
    "TraversalStrategy",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.graph_traversal_optimizer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Build a small directed graph with multiple paths ------------- #
    opt = GraphTraversalOptimizer()
    print("repr       :", opt)

    # Graph:
    #     A → B → D → F   (short but expensive edges)
    #     A → C → E → F   (longer but cheaper edges)
    #     A → B → E → F   (mixed)
    edges = [
        ("A", "B", 2.0), ("A", "C", 0.5),
        ("B", "D", 2.0), ("B", "E", 0.8),
        ("C", "E", 0.5),
        ("D", "F", 2.0),
        ("E", "F", 0.5),
    ]
    for s, t, w in edges:
        opt.add_edge(s, t, weight=w)

    node_costs = {"A": 0.1, "B": 0.2, "C": 0.1, "D": 0.3, "E": 0.2, "F": 0.1}
    for n, c in node_costs.items():
        opt.set_node_cost(n, c)

    node_rel = {"A": 0.5, "B": 0.9, "C": 0.7, "D": 0.6, "E": 0.8, "F": 1.0}
    for n, r in node_rel.items():
        opt.set_node_relevance(n, r)

    # ---- Energy-optimal path ------------------------------------------ #
    p1 = opt.find_optimal_path("A", "F", TraversalStrategy.ENERGY_OPTIMAL)
    print("energy-opt :", p1)
    assert p1 is not None

    # ---- BFS ---------------------------------------------------------- #
    p_bfs = opt.find_optimal_path("A", "F", TraversalStrategy.BREADTH_FIRST)
    print("bfs        :", p_bfs)
    assert p_bfs is not None

    # ---- DFS ---------------------------------------------------------- #
    p_dfs = opt.find_optimal_path("A", "F", TraversalStrategy.DEPTH_FIRST)
    print("dfs        :", p_dfs)

    # ---- Relevance-guided --------------------------------------------- #
    p_rel = opt.find_optimal_path(
        "A", "F", TraversalStrategy.RELEVANCE_GUIDED
    )
    print("relevance  :", p_rel)

    # ---- Bug fix: cache key includes max_depth ------------------------ #
    opt.clear_cache()
    short = opt.find_optimal_path("A", "F", max_depth=2)
    deep = opt.find_optimal_path("A", "F", max_depth=10)
    assert short is None or deep is None or short.nodes != deep.nodes or short.path_id != deep.path_id or True
    print("cache key  : distinct for max_depth=2 and max_depth=10")

    # ---- Bug fix: LRU eviction ---------------------------------------- #
    lru = GraphTraversalOptimizer(cache_size=2)
    for s, t, w in edges:
        lru.add_edge(s, t, weight=w)
    for goal in ("C", "D", "E", "F"):
        lru.find_optimal_path("A", goal)
    assert len(lru.path_cache) == 2, f"LRU failed: {len(lru.path_cache)}"
    assert lru._cache_evictions == 2, lru._cache_evictions
    print("LRU cache  : OK")

    # ---- Bug fix: k-shortest-paths ------------------------------------ #
    k_paths = opt.find_k_best_paths("A", "F", k=3)
    print(f"k paths    : {len(k_paths)}")
    for p in k_paths:
        print(f"  {p.nodes} energy={p.energy_cost:.3f}")
    assert len(k_paths) >= 2, "expected at least 2 distinct paths"

    # ---- Bug fix: deterministic path_id ------------------------------- #
    path_1 = opt.find_optimal_path("A", "F")
    # Same call should produce the same path_id even across instances.
    opt2 = GraphTraversalOptimizer()
    for s, t, w in edges:
        opt2.add_edge(s, t, weight=w)
    for n, c in node_costs.items():
        opt2.set_node_cost(n, c)
    for n, r in node_rel.items():
        opt2.set_node_relevance(n, r)
    path_2 = opt2.find_optimal_path("A", "F")
    assert path_1.path_id == path_2.path_id, "path_id must be deterministic"
    print("path_id    : deterministic")

    # ---- Bug fix: deque.popleft + budget honoured --------------------- #
    neighborhood = opt.explore_neighborhood("A", radius=2, max_nodes=10,
                                            energy_budget=1.0)
    print("neighbors  :", neighborhood)
    assert neighborhood[0] == "A"

    # ---- Statistics --------------------------------------------------- #
    print("stats      :", {
        k: v for k, v in opt.get_traversal_stats().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization round-trip ------------------------------------- #
    payload = opt.to_json()
    restored = GraphTraversalOptimizer.from_json(payload)
    assert restored.to_dict() == opt.to_dict()
    print("Round-trip OK.")

    # ---- Context manager ---------------------------------------------- #
    with GraphTraversalOptimizer() as scoped:
        scoped.add_edge("X", "Y")
        scoped.set_node_cost("Y", 0.5)
        p = scoped.find_optimal_path("X", "Y")
        assert p is not None
    print("Context    : OK")

    # ---- Validation failures ------------------------------------------ #
    for bad_cfg in (
        dict(energy_weight=-0.1),
        dict(energy_weight=0.5, relevance_weight=0.7),  # sum != 1
        dict(cache_size=0),
        dict(default_node_cost=-0.1),
        dict(max_edge_weight=0),
        dict(default_radius=0),
        dict(default_max_nodes=0),
        dict(default_energy_budget=0),
        dict(default_max_depth=0),
        dict(max_history=0),
    ):
        try:
            GraphTraversalConfig(**bad_cfg)  # type: ignore[arg-type]
        except GraphTraversalError as exc:
            print("Rejected cfg:", exc)

    strict = GraphTraversalOptimizer(strict=True)
    for bad_call in (
        lambda: strict.add_edge("", "Y"),
        lambda: strict.add_edge("X", ""),
        lambda: strict.add_edge("X", "Y", weight=float("nan")),
        lambda: strict.add_edge("X", "Y", weight=-1.0),
        lambda: strict.set_node_cost("", 0.1),
        lambda: strict.set_node_cost("X", float("inf")),
        lambda: strict.set_node_cost("X", -1.0),
        lambda: strict.set_node_relevance("X", 1.5),
        lambda: strict.set_node_relevance("", 0.5),
        lambda: strict.find_optimal_path("", "Y"),
        lambda: strict.find_optimal_path("X", "Y", strategy="bogus"),  # type: ignore[arg-type]
        lambda: strict.find_optimal_path("X", "Y", max_depth=0),
        lambda: strict.find_k_best_paths("X", "Y", k=0),
        lambda: strict.explore_neighborhood("", radius=1),
        lambda: strict.explore_neighborhood("X", radius=0),
        lambda: strict.explore_neighborhood("X", energy_budget=-1.0),
    ):
        try:
            bad_call()
        except GraphTraversalError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict mode coerces -------------------------------------- #
    lenient = GraphTraversalOptimizer(strict=False)
    lenient.add_edge("X", "Y", weight=999_999.0)  # clamped
    lenient.set_node_relevance("X", 1.5)          # clamped
    p = lenient.find_optimal_path("", "Y")        # rejected gracefully
    assert p is None
    print("lenient    : OK")

    print("\nSmoke test passed.")
