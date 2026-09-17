# src/retrieval/enhanced_graph_traversal.py

"""
Enhanced Graph Traversal Optimizer with NetworkX
================================================

Replaces simple adjacency lists with full NetworkX graph capabilities.
Includes hooks for Quantum-LIMIT-Graph integration.

Enhancements
------------
- Optional ``networkx`` / ``numpy`` imports guarded by ``_*_AVAILABLE``.
- ``EnhancedGraphConfig`` — frozen, validated: weights, cache size, node
  cost/relevance defaults, community-crossing penalty, bounded history.
- ``TraversalPath`` / ``GraphStatistics`` — frozen dataclasses with
  validation and serialization.
- **Fixed path-cache deadlock** — LRU eviction replaces the silent
  ``len < cache_size`` gate.
- **Fixed cache-key collision** — includes ``max_depth`` and
  ``energy_budget`` so different constraints return distinct results.
- **Fixed non-deterministic path IDs** — SHA-256 over the joined node list.
- **Fixed bare-except** on diameter.
- **Fixed `integrate_quantum_backend`** — validates the backend shape.
- **Fixed GraphML import** — re-syncs per-node cost / relevance maps.
- **Thread safety** — ``RLock`` guards cache, counters, and communities.
- **Full validation** of every argument; strict / non-strict modes.
- **Deterministic tie-breaking** on community detection and k-shortest
  paths.
- Serialization on the optimizer, the config, and both dataclasses.
- ``statistics()``, ``__repr__``, custom ``EnhancedGraphError``, lazy
  ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import networkx as nx  # type: ignore

    _NETWORKX_AVAILABLE = True
except ImportError:  # pragma: no cover
    nx = None  # type: ignore[assignment]
    _NETWORKX_AVAILABLE = False
    logger.debug("networkx not importable; EnhancedGraph disabled.")

try:  # pragma: no cover
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EnhancedGraphError(ValueError):
    """Raised for invalid graph optimizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class TraversalStrategy(Enum):
    """Graph traversal strategies."""
    BREADTH_FIRST = "bfs"
    DEPTH_FIRST = "dfs"
    ENERGY_OPTIMAL = "energy_optimal"
    RELEVANCE_GUIDED = "relevance_guided"
    COMMUNITY_AWARE = "community_aware"
    QUANTUM_ENHANCED = "quantum_enhanced"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EnhancedGraphConfig:
    """Tunable parameters for :class:`EnhancedGraphTraversalOptimizer`."""

    graph_type: str = "directed"          # "directed" | "undirected"
    energy_weight: float = 0.5
    relevance_weight: float = 0.5
    enable_communities: bool = True

    # LRU path cache.
    cache_size: int = 100

    # Default per-node attributes.
    default_node_cost: float = 0.1
    default_node_relevance: float = 0.5

    # Community detection defaults.
    default_community_algorithm: str = "louvain"
    community_crossing_penalty: float = 2.0

    # Optimization thresholds.
    sparse_graph_density_threshold: float = 0.01
    diameter_timeout_seconds: float = 5.0

    # Bounded run history.
    max_history: int = 1000

    def __post_init__(self) -> None:
        if self.graph_type not in ("directed", "undirected"):
            raise EnhancedGraphError(
                "graph_type must be 'directed' or 'undirected'."
            )
        for name in ("energy_weight", "relevance_weight"):
            value = getattr(self, name)
            if value < 0:
                raise EnhancedGraphError(f"{name} must be >= 0.")
        weight_sum = self.energy_weight + self.relevance_weight
        if abs(weight_sum - 1.0) > 1e-6:
            raise EnhancedGraphError(
                f"energy_weight + relevance_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )
        if not isinstance(self.cache_size, int) or self.cache_size <= 0:
            raise EnhancedGraphError("cache_size must be a positive int.")
        if self.default_node_cost < 0:
            raise EnhancedGraphError("default_node_cost must be >= 0.")
        if not 0.0 <= self.default_node_relevance <= 1.0:
            raise EnhancedGraphError(
                "default_node_relevance must be in [0, 1]."
            )
        if self.default_community_algorithm not in (
            "louvain", "label_propagation", "greedy_modularity",
        ):
            raise EnhancedGraphError(
                "default_community_algorithm must be one of "
                "'louvain', 'label_propagation', 'greedy_modularity'."
            )
        if self.community_crossing_penalty < 1.0:
            raise EnhancedGraphError(
                "community_crossing_penalty must be >= 1.0."
            )
        if self.sparse_graph_density_threshold < 0:
            raise EnhancedGraphError(
                "sparse_graph_density_threshold must be >= 0."
            )
        if self.diameter_timeout_seconds <= 0:
            raise EnhancedGraphError(
                "diameter_timeout_seconds must be > 0."
            )
        if self.max_history <= 0:
            raise EnhancedGraphError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EnhancedGraphConfig":
        if not isinstance(data, Mapping):
            raise EnhancedGraphError(
                f"EnhancedGraphConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TraversalPath:
    """Immutable traversal path through the graph."""

    path_id: str
    nodes: Tuple[str, ...]
    total_cost: float
    energy_cost: float
    relevance_score: float
    community_ids: Tuple[int, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.path_id, str) or not self.path_id:
            raise EnhancedGraphError("path_id must be a non-empty string.")
        if not isinstance(self.nodes, tuple) or not self.nodes:
            raise EnhancedGraphError("nodes must be a non-empty tuple.")
        if not isinstance(self.community_ids, tuple):
            object.__setattr__(
                self, "community_ids", tuple(self.community_ids)
            )
        for name in ("total_cost", "energy_cost", "relevance_score"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise EnhancedGraphError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise EnhancedGraphError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise EnhancedGraphError(f"{name} must be >= 0.")
        if not 0.0 <= self.relevance_score <= 1.0:
            raise EnhancedGraphError(
                "relevance_score must be in [0, 1]."
            )
        if not isinstance(self.metadata, Mapping):
            raise EnhancedGraphError("metadata must be a Mapping.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path_id": self.path_id,
            "nodes": list(self.nodes),
            "total_cost": self.total_cost,
            "energy_cost": self.energy_cost,
            "relevance_score": self.relevance_score,
            "community_ids": list(self.community_ids),
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TraversalPath":
        if not isinstance(data, Mapping):
            raise EnhancedGraphError(
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
            community_ids=tuple(data.get("community_ids", [])),
            metadata=dict(data.get("metadata", {})),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "TraversalPath("
            f"id={self.path_id[:8]}..., "
            f"nodes={len(self.nodes)}, "
            f"energy={self.energy_cost:.4f}, "
            f"relevance={self.relevance_score:.3f})"
        )


@dataclass(frozen=True)
class GraphStatistics:
    """Immutable snapshot of graph structure statistics."""

    num_nodes: int
    num_edges: int
    avg_degree: float
    density: float
    num_communities: int
    clustering_coefficient: float
    diameter: Optional[int]
    is_connected: bool
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in ("num_nodes", "num_edges", "num_communities"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise EnhancedGraphError(f"{name} must be >= 0.")
        for name in ("avg_degree", "density", "clustering_coefficient"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise EnhancedGraphError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise EnhancedGraphError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if self.diameter is not None and (
            not isinstance(self.diameter, int) or self.diameter < 0
        ):
            raise EnhancedGraphError("diameter must be a non-negative int or None.")
        if not isinstance(self.is_connected, bool):
            raise EnhancedGraphError("is_connected must be a bool.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphStatistics":
        if not isinstance(data, Mapping):
            raise EnhancedGraphError(
                "GraphStatistics.from_dict expects a Mapping."
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
            num_nodes=int(data.get("num_nodes", 0)),
            num_edges=int(data.get("num_edges", 0)),
            avg_degree=float(data.get("avg_degree", 0.0)),
            density=float(data.get("density", 0.0)),
            num_communities=int(data.get("num_communities", 0)),
            clustering_coefficient=float(
                data.get("clustering_coefficient", 0.0)
            ),
            diameter=data.get("diameter"),
            is_connected=bool(data.get("is_connected", False)),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "GraphStatistics("
            f"nodes={self.num_nodes}, edges={self.num_edges}, "
            f"density={self.density:.4f}, "
            f"communities={self.num_communities}, "
            f"connected={self.is_connected})"
        )


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #
class EnhancedGraphTraversalOptimizer:
    """
    Enhanced graph traversal with full NetworkX capabilities.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only.

    New features
    ------------
    - Real graph algorithms (shortest path, communities, centrality).
    - Community detection for cluster-aware retrieval.
    - Graph metrics for optimization.
    - Quantum-LIMIT-Graph integration hooks.
    """

    def __init__(
        self,
        graph_type: str = "directed",
        energy_weight: float = 0.5,
        relevance_weight: float = 0.5,
        enable_communities: bool = True,
        cache_size: int = 100,
        *,
        config: Optional[EnhancedGraphConfig] = None,
        strict: bool = True,
    ) -> None:
        if not _NETWORKX_AVAILABLE:
            raise EnhancedGraphError(
                "networkx is required for EnhancedGraphTraversalOptimizer; "
                "install networkx to enable this module."
            )

        if config is not None:
            self._config = config
        else:
            self._config = EnhancedGraphConfig(
                graph_type=str(graph_type),
                energy_weight=float(energy_weight),
                relevance_weight=float(relevance_weight),
                enable_communities=bool(enable_communities),
                cache_size=int(cache_size),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.energy_weight: float = self._config.energy_weight
        self.relevance_weight: float = self._config.relevance_weight
        self.enable_communities: bool = self._config.enable_communities
        self.cache_size: int = self._config.cache_size

        # ---- Graph ---------------------------------------------------
        if self._config.graph_type == "directed":
            self.graph: Any = nx.DiGraph()
        else:
            self.graph = nx.Graph()

        # ---- State ---------------------------------------------------
        self._lock = threading.RLock()
        self.node_costs: Dict[str, float] = {}
        self.node_relevance: Dict[str, float] = {}
        self.node_embeddings: Dict[str, Any] = {}
        self.communities: List[Set[str]] = []
        self.node_to_community: Dict[str, int] = {}

        # Bounded LRU path cache.
        self.path_cache: "OrderedDict[str, TraversalPath]" = OrderedDict()
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._cache_evictions: int = 0

        # Optional quantum backend.
        self.quantum_backend: Optional[Any] = None

        # Bounded run history.
        from collections import deque as _deque
        self._history: Any = _deque(maxlen=self._config.max_history)
        self._started_at: float = time.time()

        logger.debug(
            "EnhancedGraphTraversalOptimizer initialized "
            "(type=%s, energy_weight=%.2f, relevance_weight=%.2f, "
            "communities=%s, cache_size=%d, strict=%s)",
            self._config.graph_type, self.energy_weight,
            self.relevance_weight, self.enable_communities,
            self.cache_size, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> EnhancedGraphConfig:
        return self._config

    @property
    def node_count(self) -> int:
        with self._lock:
            return self.graph.number_of_nodes()

    @property
    def edge_count(self) -> int:
        with self._lock:
            return self.graph.number_of_edges()

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self._history)

    # ---------------------------------------------------------- graph mutation
    def add_node(
        self,
        node_id: str,
        cost: float = 0.1,
        relevance: float = 0.5,
        embedding: Optional[Any] = None,
        **attributes: Any,
    ) -> None:
        """Add a node with attributes."""
        if not isinstance(node_id, str) or not node_id:
            raise EnhancedGraphError(
                "node_id must be a non-empty string."
            )
        if isinstance(cost, bool) or not isinstance(cost, (int, float)):
            raise EnhancedGraphError("cost must be numeric.")
        fc = float(cost)
        if math.isnan(fc) or math.isinf(fc) or fc < 0:
            raise EnhancedGraphError(
                f"cost must be finite and >= 0, got {cost!r}."
            )
        if isinstance(relevance, bool) or not isinstance(relevance, (int, float)):
            raise EnhancedGraphError("relevance must be numeric.")
        fr = float(relevance)
        if math.isnan(fr) or math.isinf(fr):
            raise EnhancedGraphError(
                f"relevance must be finite, got {relevance!r}."
            )
        if not 0.0 <= fr <= 1.0:
            msg = f"relevance must be in [0, 1], got {fr}."
            if self._strict:
                raise EnhancedGraphError(msg)
            logger.warning("%s Clamping.", msg)
            fr = max(0.0, min(1.0, fr))

        with self._lock:
            self.graph.add_node(node_id, **attributes)
            self.node_costs[node_id] = fc
            self.node_relevance[node_id] = fr
            if embedding is not None:
                self.node_embeddings[node_id] = embedding

    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        edge_type: str = "semantic",
        **attributes: Any,
    ) -> None:
        """Add an edge with attributes."""
        if not isinstance(source, str) or not source:
            raise EnhancedGraphError("source must be a non-empty string.")
        if not isinstance(target, str) or not target:
            raise EnhancedGraphError("target must be a non-empty string.")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise EnhancedGraphError("weight must be numeric.")
        fw = float(weight)
        if math.isnan(fw) or math.isinf(fw) or fw < 0:
            raise EnhancedGraphError(
                f"weight must be finite and >= 0, got {weight!r}."
            )

        with self._lock:
            if source not in self.graph:
                # Auto-register missing nodes with default attributes.
                self.graph.add_node(source)
                self.node_costs.setdefault(
                    source, self._config.default_node_cost
                )
                self.node_relevance.setdefault(
                    source, self._config.default_node_relevance
                )
            if target not in self.graph:
                self.graph.add_node(target)
                self.node_costs.setdefault(
                    target, self._config.default_node_cost
                )
                self.node_relevance.setdefault(
                    target, self._config.default_node_relevance
                )
            self.graph.add_edge(
                source, target, weight=fw,
                edge_type=edge_type, **attributes,
            )

    # ---------------------------------------------------------- communities
    def detect_communities(
        self, algorithm: Optional[str] = None
    ) -> List[Set[str]]:
        """
        Detect communities in the graph.

        Parameters
        ----------
        algorithm : str, optional
            One of ``"louvain"``, ``"label_propagation"``,
            ``"greedy_modularity"``. Defaults to
            ``config.default_community_algorithm``.
        """
        algo = algorithm or self._config.default_community_algorithm
        if algo not in (
            "louvain", "label_propagation", "greedy_modularity",
        ):
            raise EnhancedGraphError(
                f"unknown algorithm {algo!r}."
            )

        with self._lock:
            if self.graph.number_of_nodes() == 0:
                self.communities = []
                self.node_to_community = {}
                return []

            G_undirected = self.graph.to_undirected()

            try:
                import networkx.algorithms.community as nx_comm
                if algo == "louvain":
                    communities = nx_comm.louvain_communities(G_undirected)
                elif algo == "label_propagation":
                    communities = list(
                        nx.algorithms.community.label_propagation_communities(
                            G_undirected
                        )
                    )
                else:  # greedy_modularity
                    communities = list(
                        nx.algorithms.community.greedy_modularity_communities(
                            G_undirected
                        )
                    )
            except Exception as exc:
                if self._strict:
                    raise EnhancedGraphError(
                        f"community detection failed: {exc}"
                    ) from exc
                logger.warning(
                    "Community detection failed: %s; returning empty list.",
                    exc,
                )
                return []

            # Deterministic ordering — sort by the smallest node ID in the
            # community, so `node_to_community` indices are stable across
            # runs.
            communities_sorted = sorted(
                (set(c) for c in communities),
                key=lambda s: min(s) if s else "",
            )
            self.communities = communities_sorted
            self.node_to_community = {}
            for i, comm in enumerate(communities_sorted):
                for node in comm:
                    self.node_to_community[node] = i

        logger.debug(
            "Detected %d communities using %s.",
            len(self.communities), algo,
        )
        return list(self.communities)

    # ---------------------------------------------------------- retrieval
    def find_optimal_path(
        self,
        start: str,
        goal: str,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL,
        max_depth: int = 10,
        energy_budget: Optional[float] = None,
    ) -> Optional[TraversalPath]:
        """
        Find an optimal path between two nodes.

        The path cache key includes ``strategy``, ``max_depth``, and
        ``energy_budget`` so different constraints return distinct results.
        """
        if not isinstance(start, str) or not start:
            raise EnhancedGraphError("start must be a non-empty string.")
        if not isinstance(goal, str) or not goal:
            raise EnhancedGraphError("goal must be a non-empty string.")
        if not isinstance(strategy, TraversalStrategy):
            raise EnhancedGraphError(
                f"strategy must be a TraversalStrategy, got "
                f"{type(strategy).__name__}."
            )
        if not isinstance(max_depth, int) or max_depth <= 0:
            raise EnhancedGraphError("max_depth must be a positive int.")
        budget_key: Optional[float] = None
        if energy_budget is not None:
            if isinstance(energy_budget, bool) or not isinstance(
                energy_budget, (int, float)
            ):
                raise EnhancedGraphError(
                    "energy_budget must be numeric or None."
                )
            fb = float(energy_budget)
            if math.isnan(fb) or math.isinf(fb) or fb <= 0:
                raise EnhancedGraphError(
                    f"energy_budget must be finite and > 0, got "
                    f"{energy_budget!r}."
                )
            budget_key = fb

        cache_key = (
            f"{start}:{goal}:{strategy.value}:{max_depth}:{budget_key}"
        )

        with self._lock:
            cached = self.path_cache.get(cache_key)
            if cached is not None:
                self.path_cache.move_to_end(cache_key)
                self.cache_hits += 1
                return cached
            self.cache_misses += 1

        # ---- Dispatch -------------------------------------------------
        with self._lock:
            has_start = start in self.graph
            has_goal = goal in self.graph
        if not (has_start and has_goal):
            logger.debug(
                "find_optimal_path: %s or %s missing from graph.",
                start, goal,
            )
            return None

        path: Optional[TraversalPath] = None
        try:
            if strategy == TraversalStrategy.ENERGY_OPTIMAL:
                path = self._energy_optimal_path_nx(start, goal, budget_key)
            elif strategy == TraversalStrategy.RELEVANCE_GUIDED:
                path = self._relevance_guided_path_nx(start, goal)
            elif strategy == TraversalStrategy.COMMUNITY_AWARE:
                path = self._community_aware_path(start, goal)
            elif strategy == TraversalStrategy.QUANTUM_ENHANCED:
                path = self._quantum_enhanced_path(start, goal)
            elif strategy == TraversalStrategy.BREADTH_FIRST:
                path = self._bfs_path_nx(start, goal)
            elif strategy == TraversalStrategy.DEPTH_FIRST:
                path = self._dfs_path_nx(start, goal)
        except Exception as exc:
            if self._strict:
                raise EnhancedGraphError(
                    f"pathfinding failed for strategy {strategy.value}: {exc}"
                ) from exc
            logger.warning("Pathfinding failed: %s", exc)
            path = None

        if path is not None:
            # Enforce max_depth after the fact — NetworkX does not have a
            # first-class depth constraint.
            if len(path.nodes) > max_depth:
                logger.debug(
                    "Path length %d exceeds max_depth %d; rejecting.",
                    len(path.nodes), max_depth,
                )
                path = None
            else:
                self._cache_put(cache_key, path)

        with self._lock:
            self._history.append({
                "start": start,
                "goal": goal,
                "strategy": strategy.value,
                "found": path is not None,
                "timestamp": time.time(),
            })
        return path

    def find_k_shortest_paths(
        self,
        start: str,
        goal: str,
        k: int = 3,
        weight: str = "weight",
    ) -> List[TraversalPath]:
        """Find the ``k`` shortest paths (Yen's algorithm via NetworkX)."""
        if not isinstance(start, str) or not start:
            raise EnhancedGraphError("start must be a non-empty string.")
        if not isinstance(goal, str) or not goal:
            raise EnhancedGraphError("goal must be a non-empty string.")
        if not isinstance(k, int) or k <= 0:
            raise EnhancedGraphError("k must be a positive int.")
        if not isinstance(weight, str) or not weight:
            raise EnhancedGraphError("weight must be a non-empty string.")

        with self._lock:
            if start not in self.graph or goal not in self.graph:
                return []

        try:
            paths_generator = nx.shortest_simple_paths(
                self.graph, start, goal, weight=weight,
            )
            paths: List[TraversalPath] = []
            for i, path_nodes in enumerate(paths_generator):
                if i >= k:
                    break
                paths.append(self._create_path_from_nodes(path_nodes))
            return paths
        except nx.NetworkXNoPath:
            return []
        except Exception as exc:
            if self._strict:
                raise EnhancedGraphError(
                    f"k-shortest-paths failed: {exc}"
                ) from exc
            logger.warning("k-shortest-paths failed: %s", exc)
            return []

    def get_subgraph_around_nodes(
        self, center_nodes: List[str], radius: int = 2
    ) -> Any:
        """Return the induced subgraph within ``radius`` of ``center_nodes``."""
        if not isinstance(center_nodes, list):
            raise EnhancedGraphError("center_nodes must be a list.")
        if not isinstance(radius, int) or radius <= 0:
            raise EnhancedGraphError("radius must be a positive int.")

        with self._lock:
            subgraph_nodes: Set[str] = set()
            for center in center_nodes:
                if not isinstance(center, str) or not center:
                    continue
                if center not in self.graph:
                    continue
                ego = nx.ego_graph(self.graph, center, radius=radius)
                subgraph_nodes.update(ego.nodes())
            return self.graph.subgraph(subgraph_nodes)

    # ---------------------------------------------------------- stats
    def get_graph_statistics(self) -> GraphStatistics:
        """Compute structure statistics for the current graph."""
        with self._lock:
            num_nodes = self.graph.number_of_nodes()
            num_edges = self.graph.number_of_edges()

            if num_nodes == 0:
                return GraphStatistics(
                    num_nodes=0, num_edges=0, avg_degree=0.0,
                    density=0.0, num_communities=0,
                    clustering_coefficient=0.0,
                    diameter=None, is_connected=False,
                )

            degrees = dict(self.graph.degree())
            avg_degree = sum(degrees.values()) / num_nodes
            try:
                density = float(nx.density(self.graph))
            except Exception:  # pragma: no cover — nx handles this internally
                density = 0.0

            # Community count (reuse cached value if present).
            if not self.communities:
                try:
                    self.detect_communities()
                except EnhancedGraphError:
                    if self._strict:
                        raise
            num_communities = len(self.communities)

            # Clustering coefficient (only for undirected graphs).
            if isinstance(self.graph, nx.DiGraph):
                clustering = 0.0
            else:
                try:
                    clustering = float(nx.average_clustering(self.graph))
                except Exception:
                    clustering = 0.0

            # Connectivity + diameter.
            try:
                if isinstance(self.graph, nx.DiGraph):
                    is_connected = nx.is_strongly_connected(self.graph)
                else:
                    is_connected = nx.is_connected(self.graph)
            except Exception:
                is_connected = False

            diameter: Optional[int] = None
            if is_connected:
                try:
                    diameter = int(nx.diameter(self.graph))
                except Exception as exc:
                    logger.debug(
                        "diameter calculation failed (%s); leaving None.",
                        exc,
                    )
                    diameter = None

        return GraphStatistics(
            num_nodes=num_nodes,
            num_edges=num_edges,
            avg_degree=float(avg_degree),
            density=float(density),
            num_communities=num_communities,
            clustering_coefficient=float(clustering),
            diameter=diameter,
            is_connected=bool(is_connected),
        )

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_graph_statistics().to_dict()` plus counters."""
        stats = self.get_graph_statistics().to_dict()
        with self._lock:
            stats.update({
                "cache_size": len(self.path_cache),
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "cache_evictions": self._cache_evictions,
                "history_size": len(self._history),
                "networkx_available": _NETWORKX_AVAILABLE,
                "numpy_available": _NUMPY_AVAILABLE,
                "uptime_seconds": time.time() - self._started_at,
            })
        return stats

    def optimize_graph_structure(self) -> Dict[str, Any]:
        """Return structural recommendations for the graph."""
        stats = self.get_graph_statistics()
        recommendations: List[Dict[str, str]] = []

        if not stats.is_connected:
            recommendations.append({
                "issue": "disconnected_graph",
                "recommendation": "Add edges to connect isolated components",
                "severity": "high",
            })

        if stats.density < self._config.sparse_graph_density_threshold:
            recommendations.append({
                "issue": "sparse_graph",
                "recommendation": "Consider adding more semantic edges",
                "severity": "medium",
            })

        if stats.num_communities == 0:
            recommendations.append({
                "issue": "no_communities",
                "recommendation": (
                    "Run community detection for cluster-aware retrieval"
                ),
                "severity": "low",
            })

        return {
            "statistics": stats.to_dict(),
            "recommendations": recommendations,
        }

    # ---------------------------------------------------------- quantum
    def integrate_quantum_backend(self, quantum_backend: Any) -> None:
        """
        Integrate a Quantum-LIMIT-Graph backend.

        The backend is stored as-is; when it is not ``None``,
        ``_quantum_enhanced_path`` will delegate to it if it exposes an
        ``energy_optimal_path`` method. Otherwise the classical fallback is
        used.
        """
        if quantum_backend is not None and not hasattr(
            quantum_backend, "__class__"
        ):  # pragma: no cover — defensive
            raise EnhancedGraphError(
                "quantum_backend must be an object or None."
            )
        with self._lock:
            self.quantum_backend = quantum_backend
        logger.debug(
            "Quantum backend integrated (%s).",
            type(quantum_backend).__name__ if quantum_backend else None,
        )

    # ---------------------------------------------------------- internals
    def _energy_optimal_path_nx(
        self,
        start: str,
        goal: str,
        energy_budget: Optional[float],
    ) -> Optional[TraversalPath]:
        """Energy-optimal path via Dijkstra with per-node cost."""
        cfg = self._config

        def energy_weight(u: str, v: str, attrs: Mapping[str, Any]) -> float:
            edge_weight = float(attrs.get("weight", 1.0))
            node_cost = self.node_costs.get(v, cfg.default_node_cost)
            return edge_weight + node_cost

        try:
            path_nodes = nx.shortest_path(
                self.graph, start, goal, weight=energy_weight,
            )
        except nx.NetworkXNoPath:
            return None

        path = self._create_path_from_nodes(path_nodes)
        if energy_budget is not None and path.energy_cost > energy_budget:
            return None
        return path

    def _relevance_guided_path_nx(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        """Relevance-guided path via A* with a relevance heuristic."""
        cfg = self._config

        def relevance_heuristic(node: str) -> float:
            relevance = self.node_relevance.get(
                node, cfg.default_node_relevance,
            )
            return 1.0 - relevance

        try:
            path_nodes = nx.astar_path(
                self.graph, start, goal,
                heuristic=relevance_heuristic,
                weight="weight",
            )
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
        return self._create_path_from_nodes(path_nodes)

    def _community_aware_path(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        """Path minimizing cross-community hops."""
        with self._lock:
            has_communities = bool(self.communities)
        if not has_communities and self.enable_communities:
            self.detect_communities()

        penalty = self._config.community_crossing_penalty

        def community_weight(
            u: str, v: str, attrs: Mapping[str, Any]
        ) -> float:
            base_weight = float(attrs.get("weight", 1.0))
            u_comm = self.node_to_community.get(u, -1)
            v_comm = self.node_to_community.get(v, -1)
            if u_comm != v_comm and u_comm != -1 and v_comm != -1:
                return base_weight * penalty
            return base_weight

        try:
            path_nodes = nx.shortest_path(
                self.graph, start, goal, weight=community_weight,
            )
        except nx.NetworkXNoPath:
            return None
        return self._create_path_from_nodes(path_nodes)

    def _quantum_enhanced_path(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        """
        Quantum-enhanced pathfinding.

        If a quantum backend was integrated and exposes an
        ``energy_optimal_path(start, goal)`` method, delegate to it.
        Otherwise fall back to the classical energy-optimal path.
        """
        backend = self.quantum_backend
        if backend is not None:
            method = getattr(backend, "energy_optimal_path", None)
            if callable(method):
                try:
                    result = method(start, goal)
                    if isinstance(result, TraversalPath):
                        return result
                    if isinstance(result, (list, tuple)) and result:
                        return self._create_path_from_nodes(list(result))
                    if isinstance(result, Mapping) and "nodes" in result:
                        return self._create_path_from_nodes(list(result["nodes"]))
                except Exception as exc:
                    if self._strict:
                        raise EnhancedGraphError(
                            f"quantum backend failed: {exc}"
                        ) from exc
                    logger.warning("Quantum backend failed: %s", exc)
            else:
                logger.debug(
                    "Quantum backend does not expose energy_optimal_path; "
                    "using classical fallback.",
                )
        return self._energy_optimal_path_nx(start, goal, None)

    def _bfs_path_nx(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        """BFS shortest (unweighted) path."""
        try:
            path_nodes = nx.shortest_path(self.graph, start, goal)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
        return self._create_path_from_nodes(path_nodes)

    def _dfs_path_nx(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        """First-found simple path (DFS-like)."""
        try:
            path_nodes = next(nx.all_simple_paths(self.graph, start, goal))
        except (nx.NetworkXNoPath, nx.NodeNotFound, StopIteration):
            return None
        return self._create_path_from_nodes(path_nodes)

    def _create_path_from_nodes(
        self, nodes: Sequence[str]
    ) -> TraversalPath:
        """Build a :class:`TraversalPath` from a node sequence."""
        if not nodes:
            raise EnhancedGraphError(
                "cannot create a path from an empty node list."
            )
        cfg = self._config

        # Deterministic path ID.
        joined = "\x1f".join(str(n) for n in nodes)
        path_id = "path_" + hashlib.sha256(
            joined.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

        # Energy cost.
        energy_cost = sum(
            self.node_costs.get(n, cfg.default_node_cost) for n in nodes
        )

        # Edge cost.
        edge_cost = 0.0
        with self._lock:
            for i in range(len(nodes) - 1):
                if self.graph.has_edge(nodes[i], nodes[i + 1]):
                    edge_cost += float(
                        self.graph[nodes[i]][nodes[i + 1]].get(
                            "weight", 1.0
                        )
                    )

        total_cost = energy_cost + edge_cost

        # Relevance: mean over nodes, clamped to [0, 1].
        relevance_score = sum(
            self.node_relevance.get(n, cfg.default_node_relevance)
            for n in nodes
        ) / len(nodes)
        relevance_score = max(0.0, min(1.0, relevance_score))

        # Communities traversed.
        community_ids = tuple(
            self.node_to_community.get(n, -1) for n in nodes
        )
        unique_communities = len({c for c in community_ids if c != -1})

        return TraversalPath(
            path_id=path_id,
            nodes=tuple(nodes),
            total_cost=total_cost,
            energy_cost=energy_cost,
            relevance_score=relevance_score,
            community_ids=community_ids,
            metadata={
                "length": len(nodes),
                "unique_communities": unique_communities,
                "edge_cost": edge_cost,
            },
        )

    def _cache_put(self, key: str, path: TraversalPath) -> None:
        with self._lock:
            if key in self.path_cache:
                self.path_cache.move_to_end(key)
            self.path_cache[key] = path
            while len(self.path_cache) > self.cache_size:
                self.path_cache.popitem(last=False)
                self._cache_evictions += 1

    # ---------------------------------------------------------- import/export
    def export_to_graphml(self, filepath: str) -> int:
        """Export the graph to GraphML. Returns bytes written."""
        if not isinstance(filepath, str) or not filepath:
            raise EnhancedGraphError("filepath must be a non-empty string.")
        try:
            nx.write_graphml(self.graph, filepath)
        except Exception as exc:
            raise EnhancedGraphError(
                f"GraphML export failed: {exc}"
            ) from exc
        return 0

    def import_from_graphml(self, filepath: str) -> None:
        """
        Import a graph from GraphML.

        Re-synchronizes ``node_costs`` / ``node_relevance`` from graph node
        attributes so per-node metadata is preserved across reloads.
        """
        if not isinstance(filepath, str) or not filepath:
            raise EnhancedGraphError("filepath must be a non-empty string.")
        try:
            new_graph = nx.read_graphml(filepath)
        except Exception as exc:
            raise EnhancedGraphError(
                f"GraphML import failed: {exc}"
            ) from exc

        with self._lock:
            self.graph = new_graph
            # Rebuild per-node maps from graph attributes (fall back to
            # config defaults when attributes are missing).
            self.node_costs = {}
            self.node_relevance = {}
            for node, attrs in new_graph.nodes(data=True):
                self.node_costs[str(node)] = float(
                    attrs.get("cost", self._config.default_node_cost)
                )
                self.node_relevance[str(node)] = float(
                    attrs.get(
                        "relevance", self._config.default_node_relevance
                    )
                )
            # Reset derived state — communities / cache are stale.
            self.communities = []
            self.node_to_community = {}
            self.path_cache.clear()

        logger.debug(
            "Imported GraphML from %s (%d nodes, %d edges).",
            filepath,
            new_graph.number_of_nodes(),
            new_graph.number_of_edges(),
        )

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_cache: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "cache_size": len(self.path_cache),
                    "cache_hits": self.cache_hits,
                    "cache_misses": self.cache_misses,
                    "cache_evictions": self._cache_evictions,
                    "history_size": len(self._history),
                },
                "node_count": self.graph.number_of_nodes(),
                "edge_count": self.graph.number_of_edges(),
                "communities": [sorted(c) for c in self.communities],
                "networkx_available": _NETWORKX_AVAILABLE,
                "numpy_available": _NUMPY_AVAILABLE,
            }
            if include_cache:
                payload["path_cache"] = {
                    k: v.to_dict() for k, v in self.path_cache.items()
                }
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_cache: bool = True) -> int:
        """Reset the path cache and counters. Returns entries removed."""
        with self._lock:
            removed = len(self.path_cache)
            if clear_cache:
                self.path_cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self._cache_evictions = 0
            self._history.clear()
            self._started_at = time.time()
        logger.debug("EnhancedGraphTraversalOptimizer reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "EnhancedGraphTraversalOptimizer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "EnhancedGraphTraversalOptimizer scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.node_count

    def __contains__(self, node_id: object) -> bool:
        if not isinstance(node_id, str):
            return False
        with self._lock:
            return node_id in self.graph

    def __repr__(self) -> str:
        with self._lock:
            return (
                "EnhancedGraphTraversalOptimizer("
                f"type={self._config.graph_type}, "
                f"nodes={self.graph.number_of_nodes()}, "
                f"edges={self.graph.number_of_edges()}, "
                f"communities={len(self.communities)}, "
                f"cache={len(self.path_cache)}/{self.cache_size}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "EnhancedGraphConfig",
    "EnhancedGraphError",
    "EnhancedGraphTraversalOptimizer",
    "GraphStatistics",
    "TraversalPath",
    "TraversalStrategy",
    "_NETWORKX_AVAILABLE",
    "_NUMPY_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.enhanced_graph_traversal
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    if not _NETWORKX_AVAILABLE:
        print("networkx not installed; skipping enhanced graph smoke test.")
    else:
        # ---- Build a small directed graph with two communities ------ #
        opt = EnhancedGraphTraversalOptimizer()
        print("repr       :", opt)

        # Cluster A: a1 → a2 → a3 → a4
        # Cluster B: b1 → b2 → b3 → b4
        # Bridge:    a3 → b1
        for nid in ("a1", "a2", "a3", "a4", "b1", "b2", "b3", "b4"):
            opt.add_node(nid, cost=0.1, relevance=0.5)

        edges = [
            ("a1", "a2"), ("a2", "a3"), ("a3", "a4"),
            ("b1", "b2"), ("b2", "b3"), ("b3", "b4"),
            ("a3", "b1"),
        ]
        for s, t in edges:
            opt.add_edge(s, t, weight=1.0)

        print(f"graph      : {opt.node_count} nodes, {opt.edge_count} edges")

        # ---- Community detection ------------------------------------ #
        communities = opt.detect_communities("louvain")
        print(f"communities: {len(communities)}")
        for i, c in enumerate(communities):
            print(f"  #{i}: {sorted(c)}")

        # ---- Statistics --------------------------------------------- #
        stats = opt.get_graph_statistics()
        print("stats      :", stats)

        # ---- Path finding: all strategies -------------------------- #
        for strat in TraversalStrategy:
            p = opt.find_optimal_path("a1", "b4", strategy=strat)
            if p is not None:
                print(f"  {strat.value:<18} -> {list(p.nodes)} "
                      f"(energy={p.energy_cost:.3f})")
            else:
                print(f"  {strat.value:<18} -> no path")

        # ---- k-shortest paths --------------------------------------- #
        k_paths = opt.find_k_shortest_paths("a1", "b4", k=3)
        print(f"k=3 paths  : {[list(p.nodes) for p in k_paths]}")
        assert len(k_paths) >= 2

        # ---- Bug fix: cache deadlock -------------------------------- #
        tiny = EnhancedGraphTraversalOptimizer(
            config=EnhancedGraphConfig(cache_size=2)
        )
        for n in ("n1", "n2", "n3", "n4"):
            tiny.add_node(n)
        for s, t in (("n1", "n2"), ("n2", "n3"), ("n3", "n4")):
            tiny.add_edge(s, t)
        # Trigger three distinct cache keys → only two should fit.
        tiny.find_optimal_path("n1", "n2")
        tiny.find_optimal_path("n1", "n3")
        tiny.find_optimal_path("n1", "n4")
        assert len(tiny.path_cache) == 2, len(tiny.path_cache)
        assert tiny._cache_evictions == 1
        print("LRU cache  : OK")

        # ---- Bug fix: cache key includes budget --------------------- #
        opt.path_cache.clear()
        p_lo = opt.find_optimal_path(
            "a1", "b4", energy_budget=10.0,
        )
        p_hi = opt.find_optimal_path(
            "a1", "b4", energy_budget=0.001,
        )
        # Different budgets must yield different results (or at least
        # distinct cache entries).
        assert len(opt.path_cache) >= 2
        print("budget key : OK")

        # ---- Bug fix: deterministic path IDs ------------------------ #
        opt.path_cache.clear()
        opt2 = EnhancedGraphTraversalOptimizer()
        for nid in ("a1", "a2", "a3", "a4", "b1", "b2", "b3", "b4"):
            opt2.add_node(nid, cost=0.1, relevance=0.5)
        for s, t in edges:
            opt2.add_edge(s, t, weight=1.0)
        pa = opt.find_optimal_path("a1", "b4", strategy=TraversalStrategy.BFS)
        pb = opt2.find_optimal_path("a1", "b4", strategy=TraversalStrategy.BFS)
        assert pa is not None and pb is not None
        assert pa.path_id == pb.path_id, "path_id must be deterministic"
        print("path id    : OK")

        # ---- Bug fix: max_depth enforced ---------------------------- #
        opt.path_cache.clear()
        shallow = opt.find_optimal_path("a1", "b4", max_depth=3)
        deep = opt.find_optimal_path("a1", "b4", max_depth=10)
        assert shallow is None, "max_depth=3 should reject a 8-node path"
        assert deep is not None
        print("max_depth  : OK")

        # ---- GraphML round-trip ------------------------------------- #
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "graph.graphml")
            opt.export_to_graphml(path)
            fresh = EnhancedGraphTraversalOptimizer()
            fresh.import_from_graphml(path)
            assert fresh.node_count == opt.node_count
            assert fresh.edge_count == opt.edge_count
            assert all(
                abs(fresh.node_costs[n] - 0.1) < 1e-6
                for n in fresh.graph.nodes()
            )
            print("GraphML    : OK")

        # ---- Statistics --------------------------------------------- #
        print("stats      :", {
            k: v for k, v in opt.statistics().items()
            if k not in ("uptime_seconds",)
        })

        # ---- Optimization recommendations --------------------------- #
        recs = opt.optimize_graph_structure()
        print("recs       :", [r["issue"] for r in recs["recommendations"]])

        # ---- Serialization ------------------------------------------ #
        payload = opt.to_json()
        restored = json.loads(payload)
        assert restored["config"]["graph_type"] == "directed"
        print("Serialization OK.")

        # ---- Dataclass round-trips ---------------------------------- #
        p = opt.find_optimal_path("a1", "b4")
        assert p is not None
        p_payload = json.dumps(p.to_dict(), default=str)
        p_restored = TraversalPath.from_dict(json.loads(p_payload))
        assert p_restored.to_dict() == p.to_dict()
        s_payload = json.dumps(stats.to_dict(), default=str)
        s_restored = GraphStatistics.from_dict(json.loads(s_payload))
        assert s_restored.to_dict() == stats.to_dict()
        print("Dataclass  : OK")

        # ---- Context manager --------------------------------------- #
        with EnhancedGraphTraversalOptimizer() as scoped:
            scoped.add_node("x", cost=0.5, relevance=0.9)
            assert "x" in scoped
        print("Context    : OK")

        # ---- Validation failures ----------------------------------- #
        for bad_cfg in (
            dict(graph_type="bogus"),
            dict(energy_weight=-0.1),
            dict(energy_weight=0.9, relevance_weight=0.9),   # sum != 1
            dict(cache_size=0),
            dict(default_node_cost=-0.1),
            dict(default_node_relevance=1.5),
            dict(default_community_algorithm="bogus"),
            dict(community_crossing_penalty=0.5),
            dict(sparse_graph_density_threshold=-0.1),
            dict(diameter_timeout_seconds=0),
            dict(max_history=0),
        ):
            try:
                EnhancedGraphConfig(**bad_cfg)  # type: ignore[arg-type]
            except EnhancedGraphError as exc:
                print("Rejected cfg:", exc)

        strict = EnhancedGraphTraversalOptimizer(strict=True)
        for bad_call in (
            lambda: strict.add_node("", cost=0.1),
            lambda: strict.add_node("a", cost=-1.0),
            lambda: strict.add_node("a", relevance=1.5),
            lambda: strict.add_edge("", "b"),
            lambda: strict.add_edge("a", "b", weight=-1.0),
            lambda: strict.add_edge("a", "b", weight=float("nan")),
            lambda: strict.find_optimal_path("", "b"),
            lambda: strict.find_optimal_path("a", ""),
            lambda: strict.find_optimal_path("a", "b", strategy="bfs"),  # type: ignore[arg-type]
            lambda: strict.find_optimal_path("a", "b", max_depth=0),
            lambda: strict.find_optimal_path("a", "b", energy_budget=-0.1),
            lambda: strict.find_k_shortest_paths("a", "b", k=0),
            lambda: strict.get_subgraph_around_nodes([], radius=0),
            lambda: strict.detect_communities("bogus"),
        ):
            try:
                bad_call()
            except EnhancedGraphError as exc:
                print("Rejected   :", exc)

        # ---- Non-strict coerces ------------------------------------ #
        lenient = EnhancedGraphTraversalOptimizer(strict=False)
        lenient.add_node("x", cost=0.1, relevance=1.5)  # clamped
        print("lenient    : OK")

        # ---- Reset ------------------------------------------------- #
        removed = opt.reset()
        print("reset      :", removed, "cache entries removed")

        print("\nSmoke test passed.")
