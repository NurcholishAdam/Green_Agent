# src/retrieval/graph_traversal_optimizer.py

"""
Graph Traversal Optimizer
=========================

Energy-aware graph traversal for retrieval memory graphs, inspired by
VimRAG's navigation strategies.

Design
------
- Frozen, validated ``GraphTraversalConfig`` — weight sum, cache size,
  default node cost / relevance, edge weight bound, default exploration
  defaults, bounded history.
- ``TraversalPath`` / ``GraphEdge`` — frozen dataclasses with
  validation and full serialization.
- **Cost semantics unified**: ``TraversalPath.total_cost`` is
  ``sum(node_costs) + sum(edge_weights)`` — the same quantity every
  search strategy minimizes. ``metadata`` exposes the split.
- **Yen's k-shortest-paths is correct**: the spur-path search excludes
  the root-prefix nodes, so returned paths are always simple, even on
  cyclic graphs; candidate priority uses the same edge+node cost the
  primary search minimizes.
- **Non-strict rejection is clean**: invalid edges return ``None`` and
  do not touch ``adjacency``; ``node_costs`` registers *both* endpoints
  of every accepted edge.
- **LRU cache** honours ``config.cache_size`` at all times.
- **Deterministic path IDs** — SHA-256 of the joined node list.
- **Thread-safe** — ``RLock`` guards adjacency, costs, relevance,
  communities, cache, and the snapshots used by pathfinding and
  neighbourhood exploration.
- **Full validation** of every argument; strict / non-strict modes.
- **Strict serialization** — ``to_json`` uses ``allow_nan=False``;
  every ``from_dict`` wraps its errors in the module's error type.

Version: 2.0.0
"""

from __future__ import annotations

import dataclasses
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
from types import TracebackType
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "GraphEdge",
    "GraphTraversalConfig",
    "GraphTraversalError",
    "GraphTraversalOptimizer",
    "TraversalPath",
    "TraversalStrategy",
]

VERSION = "2.0.0"


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
# Helpers
# --------------------------------------------------------------------------- #
def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return (
            value if value.tzinfo
            else value.replace(tzinfo=timezone.utc)
        )
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise GraphTraversalError(
                f"invalid timestamp {value!r}: {exc}"
            ) from exc
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GraphTraversalConfig:
    """Tunable parameters for the graph traversal optimizer."""

    energy_weight: float = 0.5
    relevance_weight: float = 0.5

    cache_size: int = 100

    default_node_cost: float = 0.1
    default_node_relevance: float = 0.5

    max_edge_weight: float = 1_000.0

    default_radius: int = 2
    default_max_nodes: int = 20
    default_energy_budget: float = 1.0
    default_max_depth: int = 10

    max_history: int = 1000

    def __post_init__(self) -> None:
        for name in ("energy_weight", "relevance_weight"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise GraphTraversalError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv):
                raise GraphTraversalError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise GraphTraversalError(f"{name} must be >= 0.")
        total = float(self.energy_weight) + float(
            self.relevance_weight
        )
        if abs(total - 1.0) > 1e-6:
            raise GraphTraversalError(
                f"energy_weight + relevance_weight must sum to 1.0 "
                f"(got {total:.6f})."
            )
        if (
            isinstance(self.cache_size, bool)
            or not isinstance(self.cache_size, int)
            or self.cache_size <= 0
        ):
            raise GraphTraversalError(
                "cache_size must be a positive int."
            )
        if (
            isinstance(self.default_node_cost, bool)
            or not isinstance(self.default_node_cost, (int, float))
        ):
            raise GraphTraversalError(
                "default_node_cost must be numeric."
            )
        if not math.isfinite(float(self.default_node_cost)):
            raise GraphTraversalError(
                "default_node_cost must be finite."
            )
        if self.default_node_cost < 0:
            raise GraphTraversalError(
                "default_node_cost must be >= 0."
            )
        if (
            isinstance(self.default_node_relevance, bool)
            or not isinstance(
                self.default_node_relevance, (int, float)
            )
        ):
            raise GraphTraversalError(
                "default_node_relevance must be numeric."
            )
        if not math.isfinite(float(self.default_node_relevance)):
            raise GraphTraversalError(
                "default_node_relevance must be finite."
            )
        if not 0.0 <= float(self.default_node_relevance) <= 1.0:
            raise GraphTraversalError(
                "default_node_relevance must be in [0, 1]."
            )
        if (
            isinstance(self.max_edge_weight, bool)
            or not isinstance(self.max_edge_weight, (int, float))
        ):
            raise GraphTraversalError(
                "max_edge_weight must be numeric."
            )
        if not math.isfinite(float(self.max_edge_weight)):
            raise GraphTraversalError(
                "max_edge_weight must be finite."
            )
        if self.max_edge_weight <= 0:
            raise GraphTraversalError(
                "max_edge_weight must be > 0."
            )
        for name in (
            "default_radius",
            "default_max_nodes",
            "default_max_depth",
            "max_history",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise GraphTraversalError(f"{name} must be > 0.")
        if (
            isinstance(self.default_energy_budget, bool)
            or not isinstance(
                self.default_energy_budget, (int, float)
            )
        ):
            raise GraphTraversalError(
                "default_energy_budget must be numeric."
            )
        if not math.isfinite(float(self.default_energy_budget)):
            raise GraphTraversalError(
                "default_energy_budget must be finite."
            )
        if self.default_energy_budget <= 0:
            raise GraphTraversalError(
                "default_energy_budget must be > 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "GraphTraversalConfig":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                f"GraphTraversalConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                energy_weight=float(
                    data.get("energy_weight", 0.5)
                ),
                relevance_weight=float(
                    data.get("relevance_weight", 0.5)
                ),
                cache_size=int(data.get("cache_size", 100)),
                default_node_cost=float(
                    data.get("default_node_cost", 0.1)
                ),
                default_node_relevance=float(
                    data.get("default_node_relevance", 0.5)
                ),
                max_edge_weight=float(
                    data.get("max_edge_weight", 1_000.0)
                ),
                default_radius=int(
                    data.get("default_radius", 2)
                ),
                default_max_nodes=int(
                    data.get("default_max_nodes", 20)
                ),
                default_energy_budget=float(
                    data.get("default_energy_budget", 1.0)
                ),
                default_max_depth=int(
                    data.get("default_max_depth", 10)
                ),
                max_history=int(data.get("max_history", 1000)),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, GraphTraversalError):
                raise
            raise GraphTraversalError(
                f"invalid GraphTraversalConfig payload: {exc}"
            ) from exc


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
        if (
            isinstance(self.weight, bool)
            or not isinstance(self.weight, (int, float))
        ):
            raise GraphTraversalError("weight must be numeric.")
        fw = float(self.weight)
        if not math.isfinite(fw) or fw < 0:
            raise GraphTraversalError(
                f"weight must be finite and >= 0, got "
                f"{self.weight!r}."
            )
        if not isinstance(self.edge_type, str) or not self.edge_type:
            raise GraphTraversalError(
                "edge_type must be a non-empty string."
            )
        if not isinstance(self.metadata, Mapping):
            raise GraphTraversalError("metadata must be a Mapping.")

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
        if "source" not in data or "target" not in data:
            raise GraphTraversalError(
                "GraphEdge requires 'source' and 'target'."
            )
        try:
            return cls(
                source=str(data["source"]),
                target=str(data["target"]),
                weight=float(data.get("weight", 1.0)),
                edge_type=str(
                    data.get("edge_type", "semantic")
                ),
                metadata=dict(data.get("metadata", {}) or {}),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, GraphTraversalError):
                raise
            raise GraphTraversalError(
                f"invalid GraphEdge payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "GraphEdge("
            f"{self.source!r} -> {self.target!r}, "
            f"weight={float(self.weight):.3f}, "
            f"type={self.edge_type!r})"
        )


@dataclass(frozen=True)
class TraversalPath:
    """
    Immutable traversal path.

    ``total_cost`` == ``sum(node_costs) + sum(edge_weights)`` — the
    quantity every search strategy minimizes. The node-only and
    edge-only components are in ``metadata["node_cost"]`` and
    ``metadata["edge_cost"]``; the sum always equals ``total_cost``.
    ``energy_cost`` mirrors ``total_cost`` for backward compatibility.
    """

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
        for name in (
            "total_cost", "energy_cost", "relevance_score",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                raise GraphTraversalError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise GraphTraversalError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.total_cost < 0 or self.energy_cost < 0:
            raise GraphTraversalError(
                "total_cost and energy_cost must be >= 0."
            )
        if not 0.0 <= float(self.relevance_score) <= 1.0:
            raise GraphTraversalError(
                "relevance_score must be in [0, 1]."
            )
        if not isinstance(self.metadata, Mapping):
            raise GraphTraversalError(
                "metadata must be a Mapping."
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "TraversalPath":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                "TraversalPath.from_dict expects a Mapping."
            )
        if "path_id" not in data or "nodes" not in data:
            raise GraphTraversalError(
                "TraversalPath requires 'path_id' and 'nodes'."
            )
        try:
            return cls(
                path_id=str(data["path_id"]),
                nodes=tuple(data["nodes"]),
                total_cost=float(
                    data.get("total_cost", 0.0)
                ),
                energy_cost=float(
                    data.get("energy_cost", 0.0)
                ),
                relevance_score=float(
                    data.get("relevance_score", 0.0)
                ),
                metadata=dict(
                    data.get("metadata", {}) or {}
                ),
                timestamp=_coerce_datetime(
                    data.get("timestamp")
                ),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, GraphTraversalError):
                raise
            raise GraphTraversalError(
                f"invalid TraversalPath payload: {exc}"
            ) from exc

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

    Thread-safe, serializable, bounded in memory.

    Public API
    ----------
    - ``add_edge(source, target, weight, edge_type, metadata)`` →
      ``Optional[GraphEdge]``
    - ``set_node_cost(node_id, cost)``
    - ``set_node_relevance(node_id, relevance)``
    - ``find_optimal_path(start, goal, strategy, max_depth)``
    - ``find_k_best_paths(start, goal, k, strategy, max_depth)``
    - ``explore_neighborhood(center, radius, max_nodes, energy_budget)``
    - ``get_traversal_stats()`` / ``statistics()``
    - ``clear_cache()`` / ``reset(clear_graph, clear_cache)``
    - ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``
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

        self._lock = threading.RLock()
        self.adjacency: Dict[str, List[GraphEdge]] = defaultdict(list)
        self.node_costs: Dict[str, float] = {}
        self.node_relevance: Dict[str, float] = {}

        self.path_cache: "OrderedDict[str, TraversalPath]" = (
            OrderedDict()
        )
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._cache_evictions: int = 0
        self._started_at: float = time.time()

        logger.debug(
            "GraphTraversalOptimizer initialized "
            "(energy_weight=%.3f, relevance_weight=%.3f, "
            "cache_size=%d, strict=%s)",
            self.energy_weight, self.relevance_weight,
            self.cache_size, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> GraphTraversalConfig:
        return self._config

    @property
    def energy_weight(self) -> float:
        return self._config.energy_weight

    @energy_weight.setter
    def energy_weight(self, value: float) -> None:
        self._config = dataclasses.replace(
            self._config, energy_weight=float(value),
        )

    @property
    def relevance_weight(self) -> float:
        return self._config.relevance_weight

    @relevance_weight.setter
    def relevance_weight(self, value: float) -> None:
        self._config = dataclasses.replace(
            self._config, relevance_weight=float(value),
        )

    @property
    def cache_size(self) -> int:
        return self._config.cache_size

    @cache_size.setter
    def cache_size(self, value: int) -> None:
        self._config = dataclasses.replace(
            self._config, cache_size=int(value),
        )

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self.node_costs)

    @property
    def edge_count(self) -> int:
        with self._lock:
            return sum(len(es) for es in self.adjacency.values())

    # ---------------------------------------------------------- mutation
    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        edge_type: str = "semantic",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[GraphEdge]:
        """
        Add an edge to the graph.

        Returns the stored :class:`GraphEdge`, or ``None`` in non-strict
        mode when the input was rejected (state is **not** mutated).
        """
        if metadata is not None and not isinstance(
            metadata, Mapping
        ):
            msg = (
                f"metadata must be a Mapping or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise GraphTraversalError(msg)
            logger.warning("%s Rejecting edge.", msg)
            return None

        try:
            edge = GraphEdge(
                source=source,
                target=target,
                weight=float(weight),
                edge_type=edge_type,
                metadata=metadata or {},
            )
        except GraphTraversalError as exc:
            if self._strict:
                raise
            logger.warning(
                "Rejecting invalid edge %r -> %r: %s",
                source, target, exc,
            )
            return None

        if edge.weight > self._config.max_edge_weight:
            msg = (
                f"edge weight {edge.weight} exceeds "
                f"max_edge_weight {self._config.max_edge_weight}."
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
            # Register both endpoints so node_costs is complete.
            self.node_costs.setdefault(
                edge.source, self._config.default_node_cost
            )
            self.node_costs.setdefault(
                edge.target, self._config.default_node_cost
            )

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
        if (
            isinstance(cost, bool)
            or not isinstance(cost, (int, float))
        ):
            raise GraphTraversalError(
                f"cost must be numeric, got "
                f"{type(cost).__name__}."
            )
        fc = float(cost)
        if not math.isfinite(fc) or fc < 0:
            raise GraphTraversalError(
                f"cost must be finite and >= 0, got {cost!r}."
            )
        with self._lock:
            self.node_costs[node_id] = fc

    def set_node_relevance(
        self, node_id: str, relevance: float,
    ) -> None:
        """Set the relevance score for a node (clamped to [0, 1])."""
        if not isinstance(node_id, str) or not node_id:
            raise GraphTraversalError(
                "node_id must be a non-empty string."
            )
        if (
            isinstance(relevance, bool)
            or not isinstance(relevance, (int, float))
        ):
            raise GraphTraversalError(
                f"relevance must be numeric, got "
                f"{type(relevance).__name__}."
            )
        fr = float(relevance)
        if not math.isfinite(fr):
            raise GraphTraversalError(
                f"relevance must be finite, got {relevance!r}."
            )
        if not 0.0 <= fr <= 1.0:
            if self._strict:
                raise GraphTraversalError(
                    f"relevance must be in [0, 1], got {fr}."
                )
            logger.warning(
                "Clamping relevance %.3f to [0, 1].", fr,
            )
            fr = max(0.0, min(1.0, fr))
        with self._lock:
            self.node_relevance[node_id] = fr

    # ---------------------------------------------------------- pathfinding
    def find_optimal_path(
        self,
        start: str,
        goal: str,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL,
        max_depth: Optional[int] = None,
    ) -> Optional[TraversalPath]:
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

        if strategy == TraversalStrategy.BREADTH_FIRST:
            path = self._bfs_path(start, goal, depth)
        elif strategy == TraversalStrategy.DEPTH_FIRST:
            path = self._dfs_path(start, goal, depth)
        elif strategy == TraversalStrategy.ENERGY_OPTIMAL:
            path = self._energy_optimal_path(start, goal, depth)
        elif strategy == TraversalStrategy.RELEVANCE_GUIDED:
            path = self._relevance_guided_path(start, goal, depth)
        else:  # pragma: no cover — Enum exhaustive
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
        Find up to ``k`` best paths from ``start`` to ``goal``.

        Only ``ENERGY_OPTIMAL`` supports multi-path output (Yen's
        algorithm). Other strategies return a single path regardless of
        ``k``.
        """
        start = self._validate_node("start", start)
        goal = self._validate_node("goal", goal)
        if start is None or goal is None:
            return []
        if (
            isinstance(k, bool)
            or not isinstance(k, int)
            or k <= 0
        ):
            raise GraphTraversalError("k must be a positive int.")
        depth = self._validate_depth(max_depth)

        if strategy != TraversalStrategy.ENERGY_OPTIMAL:
            logger.debug(
                "k-best not implemented for %s; "
                "returning single path.", strategy.value,
            )
            path = self.find_optimal_path(
                start, goal, strategy, depth,
            )
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
        BFS around ``center`` within an energy budget.

        Returns node IDs in BFS order starting from ``center``. The
        center's own cost is charged against the budget. The BFS runs
        against a point-in-time snapshot of adjacency + costs, so
        concurrent mutations cannot produce duplicates or torn reads.
        """
        center = self._validate_node("center", center)
        if center is None:
            return []

        cfg = self._config
        r = cfg.default_radius if radius is None else radius
        n = (
            cfg.default_max_nodes
            if max_nodes is None else max_nodes
        )
        b = (
            cfg.default_energy_budget
            if energy_budget is None else energy_budget
        )

        if (
            isinstance(r, bool)
            or not isinstance(r, int)
            or r <= 0
        ):
            raise GraphTraversalError(
                "radius must be a positive int."
            )
        if (
            isinstance(n, bool)
            or not isinstance(n, int)
            or n <= 0
        ):
            raise GraphTraversalError(
                "max_nodes must be a positive int."
            )
        if (
            isinstance(b, bool)
            or not isinstance(b, (int, float))
        ):
            raise GraphTraversalError(
                "energy_budget must be numeric."
            )
        fb = float(b)
        if not math.isfinite(fb) or fb <= 0:
            raise GraphTraversalError(
                f"energy_budget must be finite and > 0, "
                f"got {b!r}."
            )

        # Snapshot state under the lock, then run BFS lock-free.
        with self._lock:
            center_cost = self.node_costs.get(
                center, cfg.default_node_cost
            )
            if center_cost > fb:
                logger.debug(
                    "Center cost %.4f exceeds budget %.4f.",
                    center_cost, fb,
                )
                return []
            adjacency_snapshot = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
            costs_snapshot = dict(self.node_costs)

        explored: List[str] = [center]
        visited: Set[str] = {center}
        queue: Deque[Tuple[str, int, float]] = deque(
            [(center, 0, center_cost)]
        )

        while queue and len(explored) < n:
            node, depth, cost = queue.popleft()
            if depth >= r:
                continue
            for edge in adjacency_snapshot.get(node, []):
                if edge.target in visited:
                    continue
                node_cost = costs_snapshot.get(
                    edge.target, cfg.default_node_cost
                )
                new_cost = cost + node_cost
                if new_cost > fb:
                    continue
                visited.add(edge.target)
                explored.append(edge.target)
                queue.append(
                    (edge.target, depth + 1, new_cost)
                )
                if len(explored) >= n:
                    break

        logger.debug(
            "Explored %d node(s) within radius %d from %s.",
            len(explored), r, center,
        )
        return explored

    # ---------------------------------------------------------- BFS / DFS
    def _bfs_path(
        self, start: str, goal: str, max_depth: int,
    ) -> Optional[TraversalPath]:
        queue: Deque[Tuple[str, List[str], int]] = deque(
            [(start, [start], 0)]
        )
        visited: Set[str] = {start}
        with self._lock:
            adj = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
        while queue:
            node, path, depth = queue.popleft()
            if node == goal:
                return self._create_path(path)
            if depth >= max_depth:
                continue
            for edge in adj.get(node, []):
                if edge.target not in visited:
                    visited.add(edge.target)
                    queue.append(
                        (edge.target, path + [edge.target], depth + 1)
                    )
        return None

    def _dfs_path(
        self, start: str, goal: str, max_depth: int,
    ) -> Optional[TraversalPath]:
        stack: List[Tuple[str, List[str], int]] = [
            (start, [start], 0)
        ]
        visited: Set[str] = {start}
        with self._lock:
            adj = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
        while stack:
            node, path, depth = stack.pop()
            if node == goal:
                return self._create_path(path)
            if depth >= max_depth:
                continue
            for edge in reversed(adj.get(node, [])):
                if edge.target not in visited:
                    visited.add(edge.target)
                    stack.append(
                        (edge.target, path + [edge.target], depth + 1)
                    )
        return None

    # ---------------------------------------------------------- Dijkstra
    def _energy_optimal_path(
        self, start: str, goal: str, max_depth: int,
    ) -> Optional[TraversalPath]:
        """
        Dijkstra minimizing ``node_cost(v) + edge_weight(u, v)`` — the
        same cost :attr:`TraversalPath.total_cost` reports.
        """
        cfg = self._config
        with self._lock:
            adj = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
            costs = dict(self.node_costs)

        pq: List[
            Tuple[float, str, Tuple[str, ...]]
        ] = [(0.0, start, (start,))]
        visited: Set[str] = set()

        while pq:
            cost, node, path = heapq.heappop(pq)
            if node == goal:
                return self._create_path(list(path))
            if node in visited or len(path) > max_depth:
                continue
            visited.add(node)

            for edge in adj.get(node, []):
                if edge.target in visited:
                    continue
                node_cost = costs.get(
                    edge.target, cfg.default_node_cost
                )
                new_cost = cost + node_cost + float(edge.weight)
                heapq.heappush(
                    pq,
                    (new_cost, edge.target, path + (edge.target,)),
                )
        return None

    def _relevance_guided_path(
        self, start: str, goal: str, max_depth: int,
    ) -> Optional[TraversalPath]:
        """
        Greedy: prioritise high-relevance neighbours. Returns *a* path,
        not the optimal one — this strategy ignores edge weights.
        """
        start_rel = self._relevance_of(start)
        pq: List[
            Tuple[float, str, Tuple[str, ...]]
        ] = [(-start_rel, start, (start,))]
        visited: Set[str] = set()
        with self._lock:
            adj = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
            rel_map = dict(self.node_relevance)
        default_rel = self._config.default_node_relevance

        while pq:
            _, node, path = heapq.heappop(pq)
            if node == goal:
                return self._create_path(list(path))
            if node in visited or len(path) > max_depth:
                continue
            visited.add(node)

            for edge in adj.get(node, []):
                if edge.target in visited:
                    continue
                rel = rel_map.get(edge.target, default_rel)
                heapq.heappush(
                    pq,
                    (-rel, edge.target, path + (edge.target,)),
                )
        return None

    # ---------------------------------------------------------- k-shortest
    def _k_shortest_paths_energy(
        self,
        start: str,
        goal: str,
        k: int,
        max_depth: int,
    ) -> List[TraversalPath]:
        """
        Yen's algorithm for k energy-optimal simple paths.

        Returned paths are sorted by :attr:`TraversalPath.total_cost`
        (edge+node), matching the primary search's cost model.
        """
        first = self._energy_optimal_path(start, goal, max_depth)
        if first is None:
            return []

        A: List[TraversalPath] = [first]
        # Heap entries: (total_cost, nodes_tuple). Sorted by the same
        # cost the search minimizes.
        B: List[Tuple[float, Tuple[str, ...]]] = []
        seen_keys: Set[Tuple[str, ...]] = {first.nodes}

        for _ in range(1, k):
            prev = A[-1]
            for i in range(len(prev.nodes) - 1):
                spur_node = prev.nodes[i]
                root_path = prev.nodes[: i + 1]
                # Nodes from the root (exclusive of spur_node) must be
                # excluded from the spur search — otherwise cycles can
                # reappear in the combined path.
                removed_nodes = set(root_path[:-1])

                removed_edges: Set[Tuple[str, str]] = set()
                for p in A:
                    if (
                        p.nodes[: i + 1] == root_path
                        and i + 1 < len(p.nodes)
                    ):
                        removed_edges.add(
                            (p.nodes[i], p.nodes[i + 1])
                        )

                spur = self._spur_path(
                    spur_node=spur_node,
                    goal=goal,
                    removed_edges=removed_edges,
                    removed_nodes=removed_nodes,
                    max_depth=max(1, max_depth - i),
                )
                if spur is None:
                    continue

                total_nodes = (
                    tuple(root_path[:-1]) + tuple(spur)
                )
                if total_nodes in seen_keys:
                    continue
                seen_keys.add(total_nodes)

                candidate = self._create_path(
                    list(total_nodes)
                )
                heapq.heappush(
                    B, (candidate.total_cost, total_nodes)
                )

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
        removed_edges: Set[Tuple[str, str]],
        removed_nodes: Set[str],
        max_depth: int,
    ) -> Optional[List[str]]:
        """
        Dijkstra from ``spur_node`` to ``goal`` with certain nodes and
        edges removed. Excluding ``removed_nodes`` guarantees the
        combined (root + spur) path is simple.
        """
        cfg = self._config
        with self._lock:
            adj = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
            costs = dict(self.node_costs)

        pq: List[
            Tuple[float, str, Tuple[str, ...]]
        ] = [(0.0, spur_node, (spur_node,))]
        visited: Set[str] = {spur_node} | removed_nodes

        while pq:
            cost, node, path = heapq.heappop(pq)
            if node == goal:
                return list(path)
            if node in visited or len(path) > max_depth:
                continue
            visited.add(node)

            for edge in adj.get(node, []):
                if edge.target in visited:
                    continue
                if (edge.source, edge.target) in removed_edges:
                    continue
                node_cost = costs.get(
                    edge.target, cfg.default_node_cost
                )
                new_cost = cost + node_cost + float(edge.weight)
                heapq.heappush(
                    pq,
                    (new_cost, edge.target, path + (edge.target,)),
                )
        return None

    # ---------------------------------------------------------- helpers
    def _cache_put(
        self, key: str, path: TraversalPath,
    ) -> None:
        """Insert into the LRU cache, evicting the oldest when full."""
        with self._lock:
            if key in self.path_cache:
                self.path_cache.move_to_end(key)
            self.path_cache[key] = path
            while len(self.path_cache) > self._config.cache_size:
                self.path_cache.popitem(last=False)
                self._cache_evictions += 1

    def _validate_node(
        self, name: str, value: Any,
    ) -> Optional[str]:
        if not isinstance(value, str) or not value:
            msg = (
                f"{name} must be a non-empty string, got "
                f"{value!r}."
            )
            if self._strict:
                raise GraphTraversalError(msg)
            logger.warning("%s Returning None.", msg)
            return None
        return value

    def _validate_depth(self, value: Optional[int]) -> int:
        if value is None:
            return self._config.default_max_depth
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
        ):
            msg = (
                f"max_depth must be a positive int, got {value!r}."
            )
            if self._strict:
                raise GraphTraversalError(msg)
            logger.warning(
                "%s Using default %d.",
                msg, self._config.default_max_depth,
            )
            return self._config.default_max_depth
        return value

    def _relevance_of(self, node: str) -> float:
        with self._lock:
            return self.node_relevance.get(
                node, self._config.default_node_relevance
            )

    def _create_path(
        self, nodes: Sequence[str],
    ) -> TraversalPath:
        """
        Build a :class:`TraversalPath` with ``total_cost =
        sum(node_costs) + sum(edge_weights)`` — the cost every search
        minimizes.
        """
        if not nodes:
            raise GraphTraversalError(
                "cannot create a path from an empty node list."
            )

        joined = "\x1f".join(str(n) for n in nodes)
        path_id = "path_" + hashlib.sha256(
            joined.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

        with self._lock:
            adj = {
                src: list(es)
                for src, es in self.adjacency.items()
            }
            costs = dict(self.node_costs)
            rel_map = dict(self.node_relevance)
        default_cost = self._config.default_node_cost
        default_rel = self._config.default_node_relevance

        node_cost = sum(
            costs.get(n, default_cost) for n in nodes
        )
        edge_cost = 0.0
        for i in range(len(nodes) - 1):
            u, v = nodes[i], nodes[i + 1]
            # Cheapest parallel edge — matches the search's edge
            # selection (min of parallel weights).
            best = math.inf
            for edge in adj.get(u, []):
                if edge.target == v and float(edge.weight) < best:
                    best = float(edge.weight)
            if best is not math.inf:
                edge_cost += best

        total_cost = node_cost + edge_cost

        relevance_score = sum(
            rel_map.get(n, default_rel) for n in nodes
        ) / len(nodes)
        relevance_score = max(0.0, min(1.0, relevance_score))

        return TraversalPath(
            path_id=path_id,
            nodes=tuple(nodes),
            total_cost=total_cost,
            energy_cost=total_cost,
            relevance_score=relevance_score,
            metadata={
                "length": len(nodes),
                "node_cost": node_cost,
                "edge_cost": edge_cost,
            },
        )

    # ---------------------------------------------------------- stats
    def get_traversal_stats(self) -> Dict[str, Any]:
        with self._lock:
            hits = self.cache_hits
            misses = self.cache_misses
            cached = len(self.path_cache)
            evictions = self._cache_evictions
            costs = dict(self.node_costs)
            edges = sum(
                len(es) for es in self.adjacency.values()
            )

        total_requests = hits + misses
        hit_rate = (
            hits / total_requests if total_requests > 0 else 0.0
        )
        avg_node_cost = (
            sum(costs.values()) / len(costs)
            if costs else 0.0
        )

        return {
            "total_nodes": len(costs),
            "total_edges": edges,
            "cached_paths": cached,
            "cache_hits": hits,
            "cache_misses": misses,
            "cache_hit_rate": hit_rate,
            "avg_node_cost": avg_node_cost,
            "cache_evictions": evictions,
            "energy_weight": self._config.energy_weight,
            "relevance_weight": self._config.relevance_weight,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_traversal_stats()

    def clear_cache(self) -> int:
        with self._lock:
            removed = len(self.path_cache)
            self.path_cache.clear()
        logger.debug("Cleared %d cached path(s).", removed)
        return removed

    def reset(
        self,
        *,
        clear_graph: bool = False,
        clear_cache: bool = True,
        reset_counters: bool = True,
    ) -> None:
        """
        Reset the optimizer state.

        ``reset_counters=False`` keeps hit/miss counters even when the
        cache is cleared — useful for aggregating across resets.
        """
        with self._lock:
            if clear_graph:
                self.adjacency.clear()
                self.node_costs.clear()
                self.node_relevance.clear()
            if clear_cache:
                self.path_cache.clear()
            if reset_counters:
                self.cache_hits = 0
                self.cache_misses = 0
                self._cache_evictions = 0
            self._started_at = time.time()
        logger.debug("GraphTraversalOptimizer reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_cache: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "adjacency": {
                    src: [e.to_dict() for e in edges]
                    for src, edges in self.adjacency.items()
                },
                "node_costs": dict(self.node_costs),
                "node_relevance": dict(
                    self.node_relevance
                ),
                "counters": {
                    "cache_hits": self.cache_hits,
                    "cache_misses": self.cache_misses,
                    "cache_evictions": self._cache_evictions,
                },
                "started_at": self._started_at,
            }
            if include_cache:
                payload["path_cache"] = {
                    k: v.to_dict()
                    for k, v in self.path_cache.items()
                }
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "GraphTraversalOptimizer":
        if not isinstance(data, Mapping):
            raise GraphTraversalError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            cfg = GraphTraversalConfig.from_dict(
                dict(data.get("config", {}) or {})
            )
            opt = cls(
                config=cfg,
                strict=bool(data.get("strict", True)),
            )

            raw_adjacency = data.get("adjacency") or {}
            if not isinstance(raw_adjacency, Mapping):
                raise GraphTraversalError(
                    "adjacency must be a Mapping."
                )
            raw_costs = data.get("node_costs") or {}
            if not isinstance(raw_costs, Mapping):
                raise GraphTraversalError(
                    "node_costs must be a Mapping."
                )
            raw_relevance = (
                data.get("node_relevance") or {}
            )
            if not isinstance(raw_relevance, Mapping):
                raise GraphTraversalError(
                    "node_relevance must be a Mapping."
                )

            with opt._lock:
                for src, edges in raw_adjacency.items():
                    if not isinstance(edges, list):
                        raise GraphTraversalError(
                            f"adjacency[{src!r}] must be a list, "
                            f"got {type(edges).__name__}."
                        )
                    for e in edges:
                        if not isinstance(e, Mapping):
                            raise GraphTraversalError(
                                f"adjacency[{src!r}] entries "
                                f"must be Mappings."
                            )
                        opt.adjacency[str(src)].append(
                            GraphEdge.from_dict(e)
                        )
                for node, cost in raw_costs.items():
                    opt.node_costs[str(node)] = float(cost)
                for node, rel in raw_relevance.items():
                    opt.node_relevance[str(node)] = float(rel)

                counters = dict(
                    data.get("counters", {}) or {}
                )
                opt.cache_hits = int(
                    counters.get("cache_hits", 0)
                )
                opt.cache_misses = int(
                    counters.get("cache_misses", 0)
                )
                opt._cache_evictions = int(
                    counters.get("cache_evictions", 0)
                )
                opt._started_at = float(
                    data.get("started_at", time.time())
                )

                raw_cache = data.get("path_cache") or {}
                if not isinstance(raw_cache, Mapping):
                    raise GraphTraversalError(
                        "path_cache must be a Mapping."
                    )
                for key, entry in raw_cache.items():
                    if not isinstance(entry, Mapping):
                        raise GraphTraversalError(
                            f"path_cache[{key!r}] must be a "
                            f"Mapping."
                        )
                    # Honour cache_size eviction.
                    opt._cache_put(
                        str(key),
                        TraversalPath.from_dict(entry),
                    )
            return opt
        except GraphTraversalError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise GraphTraversalError(
                f"invalid optimizer payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_cache: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_cache=include_cache),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "GraphTraversalOptimizer":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GraphTraversalError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "GraphTraversalOptimizer":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "GraphTraversalOptimizer scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            nodes = len(self.node_costs)
            edges = sum(
                len(e) for e in self.adjacency.values()
            )
            cached = len(self.path_cache)
        return (
            "GraphTraversalOptimizer("
            f"nodes={nodes}, edges={edges}, "
            f"cached={cached}, "
            f"energy_weight={self._config.energy_weight:.2f}, "
            f"relevance_weight={self._config.relevance_weight:.2f}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    # ---- Build graph with multiple paths of varied costs ---------- #
    opt = GraphTraversalOptimizer()
    print("repr       :", opt)

    edges = [
        ("A", "B", 2.0), ("A", "C", 0.5),
        ("B", "D", 2.0), ("B", "E", 0.8),
        ("C", "E", 0.5),
        ("D", "F", 2.0),
        ("E", "F", 0.5),
    ]
    for s, t, w in edges:
        opt.add_edge(s, t, weight=w)

    node_costs = {
        "A": 0.1, "B": 0.2, "C": 0.1,
        "D": 0.3, "E": 0.2, "F": 0.1,
    }
    for n, c in node_costs.items():
        opt.set_node_cost(n, c)

    node_rel = {
        "A": 0.5, "B": 0.9, "C": 0.7,
        "D": 0.6, "E": 0.8, "F": 1.0,
    }
    for n, r in node_rel.items():
        opt.set_node_relevance(n, r)

    # ---- Energy-optimal path with edge+node cost ------------------ #
    p1 = opt.find_optimal_path(
        "A", "F", TraversalStrategy.ENERGY_OPTIMAL,
    )
    print("energy-opt :", p1)
    assert p1 is not None
    # Cheapest path is A→C→E→F: edges 0.5+0.5+0.5=1.5,
    # nodes 0.1+0.1+0.2+0.1=0.5, total=2.0
    assert p1.nodes == ("A", "C", "E", "F"), p1.nodes
    assert abs(p1.total_cost - 2.0) < 1e-6, p1.total_cost
    assert abs(p1.metadata["node_cost"] - 0.5) < 1e-6
    assert abs(p1.metadata["edge_cost"] - 1.5) < 1e-6
    print(f"  total_cost = {p1.total_cost:.3f} "
          f"(node={p1.metadata['node_cost']:.3f}, "
          f"edge={p1.metadata['edge_cost']:.3f})")

    # ---- BFS / DFS / relevance ------------------------------------ #
    p_bfs = opt.find_optimal_path(
        "A", "F", TraversalStrategy.BREADTH_FIRST,
    )
    p_dfs = opt.find_optimal_path(
        "A", "F", TraversalStrategy.DEPTH_FIRST,
    )
    p_rel = opt.find_optimal_path(
        "A", "F", TraversalStrategy.RELEVANCE_GUIDED,
    )
    print("bfs        :", p_bfs)
    print("dfs        :", p_dfs)
    print("relevance  :", p_rel)

    # ---- Cache key includes max_depth (real assertion) ------------ #
    opt.clear_cache()
    short = opt.find_optimal_path("A", "F", max_depth=2)
    deep = opt.find_optimal_path("A", "F", max_depth=10)
    # Shortest A→F path is 3 hops, so max_depth=2 must reject it.
    assert short is None, (
        f"max_depth=2 should reject 3-hop path, got "
        f"{short.nodes if short else None}"
    )
    assert deep is not None, "max_depth=10 should succeed"
    # Only the successful lookup should be cached.
    assert len(opt.path_cache) == 1, len(opt.path_cache)
    print("cache key  : OK (max_depth distinct)")

    # ---- LRU cache ------------------------------------------------ #
    lru = GraphTraversalOptimizer(cache_size=2)
    for s, t, w in edges:
        lru.add_edge(s, t, weight=w)
    for goal in ("C", "D", "E", "F"):
        lru.find_optimal_path("A", goal)
    assert len(lru.path_cache) == 2, len(lru.path_cache)
    assert lru._cache_evictions == 2, lru._cache_evictions
    print("LRU cache  : OK")

    # ---- k-shortest-paths sorted by edge+node cost --------------- #
    k_paths = opt.find_k_best_paths("A", "F", k=3)
    print(f"k paths    : {len(k_paths)}")
    costs = []
    for p in k_paths:
        costs.append(p.total_cost)
        print(
            f"  {p.nodes} total={p.total_cost:.3f} "
            f"(edge={p.metadata['edge_cost']:.3f}, "
            f"node={p.metadata['node_cost']:.3f})"
        )
    assert len(k_paths) >= 2, len(k_paths)
    assert costs == sorted(costs), (
        f"k-shortest paths must be sorted by total_cost, got {costs}"
    )
    assert k_paths[0].nodes == ("A", "C", "E", "F"), (
        f"cheapest path should be A→C→E→F, got "
        f"{k_paths[0].nodes}"
    )

    # ---- k-shortest on a cyclic graph (spur path fix) ------------ #
    cyclic = GraphTraversalOptimizer()
    cyclic.add_edge("A", "B", 1.0)
    cyclic.add_edge("B", "A", 1.0)   # cycle
    cyclic.add_edge("B", "C", 1.0)
    cyclic.add_edge("A", "C", 2.0)
    for n in ("A", "B", "C"):
        cyclic.set_node_cost(n, 0.1)
    cyc_paths = cyclic.find_k_best_paths("A", "C", k=3)
    for p in cyc_paths:
        assert len(p.nodes) == len(set(p.nodes)), (
            f"non-simple path: {p.nodes}"
        )
    print(f"cyclic     : {len(cyc_paths)} simple path(s)")

    # ---- Deterministic path_id ----------------------------------- #
    opt.clear_cache()
    path_1 = opt.find_optimal_path("A", "F")
    opt2 = GraphTraversalOptimizer()
    for s, t, w in edges:
        opt2.add_edge(s, t, weight=w)
    for n, c in node_costs.items():
        opt2.set_node_cost(n, c)
    for n, r in node_rel.items():
        opt2.set_node_relevance(n, r)
    path_2 = opt2.find_optimal_path("A", "F")
    assert path_1 is not None and path_2 is not None
    assert path_1.path_id == path_2.path_id, (
        "path_id must be deterministic across instances"
    )
    print("path_id    : deterministic")

    # ---- explore_neighborhood (race-free) ------------------------ #
    neighborhood = opt.explore_neighborhood(
        "A", radius=2, max_nodes=10, energy_budget=1.0,
    )
    print("neighbors  :", neighborhood)
    assert neighborhood[0] == "A"
    assert len(neighborhood) == len(set(neighborhood)), (
        "explore_neighborhood returned duplicates"
    )

    # ---- Statistics --------------------------------------------- #
    print("stats      :", {
        k: v for k, v in opt.get_traversal_stats().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization round-trip ------------------------------- #
    payload = opt.to_json(include_cache=True)
    restored = GraphTraversalOptimizer.from_json(payload)
    assert restored.to_dict() == opt.to_dict()
    print("Round-trip OK.")

    # ---- Context manager ---------------------------------------- #
    with GraphTraversalOptimizer() as scoped:
        scoped.add_edge("X", "Y")
        scoped.set_node_cost("Y", 0.5)
        p = scoped.find_optimal_path("X", "Y")
        assert p is not None
    print("Context    : OK")

    # ---- Config validation -------------------------------------- #
    for bad_cfg in (
        dict(energy_weight=-0.1),
        dict(energy_weight=0.5, relevance_weight=0.7),
        dict(cache_size=0),
        dict(default_node_cost=-0.1),
        dict(default_node_relevance=1.5),
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

    # ---- Strict-mode input validation --------------------------- #
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
        lambda: strict.find_optimal_path(
            "X", "Y", strategy="bogus",  # type: ignore[arg-type]
        ),
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

    # ---- Non-strict: no state pollution ------------------------- #
    lenient = GraphTraversalOptimizer(strict=False)
    lenient.add_edge("X", "Y", weight=999_999.0)  # clamped
    before_edges = lenient.edge_count
    before_nodes = lenient.node_count
    assert lenient.add_edge("", "Y") is None
    assert lenient.edge_count == before_edges
    assert lenient.node_count == before_nodes
    lenient.set_node_relevance("X", 1.5)  # clamped
    p = lenient.find_optimal_path("", "Y")  # graceful skip
    assert p is None
    print("lenient    : OK (no state pollution)")

    # ---- from_dict error wrapping ------------------------------- #
    for bad_payload in (
        {"adjacency": "abc"},
        {"node_costs": 42},
        {"node_relevance": "not-a-mapping"},
        {"adjacency": {"A": "not-a-list"}},
        {"path_cache": {"k": "not-a-mapping"}},
    ):
        try:
            GraphTraversalOptimizer.from_dict(bad_payload)
        except GraphTraversalError as exc:
            print("Rejected payload:", str(exc)[:60], "...")

    # ---- to_json rejects NaN (defensive) ------------------------ #
    tainted = GraphTraversalOptimizer()
    tainted.add_edge("A", "B")
    object.__setattr__(
        tainted._config, "default_node_cost", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN in to_json rejected:", str(exc)[:60])

    print("\nSmoke test passed.")
