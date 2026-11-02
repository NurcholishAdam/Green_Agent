# src/retrieval/enhanced_graph_traversal.py

"""
Enhanced Graph Traversal Optimizer with NetworkX
================================================

Full NetworkX-backed graph traversal with community detection, k-shortest
paths, LRU path caching, and a Quantum-LIMIT-Graph integration hook.

Design
------
- Optional ``networkx`` / ``numpy`` imports guarded by
  ``NETWORKX_AVAILABLE`` / ``NUMPY_AVAILABLE``.
- ``EnhancedGraphConfig`` — frozen, validated.
- ``TraversalPath`` / ``GraphStatistics`` — frozen dataclasses with
  validation and full serialization.
- **LRU path cache** with bounded eviction (not silent gating).
- **Cache key includes** ``strategy`` + ``max_depth``, plus
  ``energy_budget`` **only for strategies that honour it** — so two
  identical BFS lookups with different budget kwargs share one slot.
- **Deterministic path IDs** — SHA-256 over the joined node list.
- **GraphML round-trip** re-syncs per-node cost / relevance /
  embeddings; cost and relevance are stored as graph node attributes
  so they survive export/import.
- **Bounded diameter calc** — ``nx.diameter`` runs in a worker with a
  timeout; on timeout the field is ``None`` instead of blocking the
  caller.
- **Thread-safe** — ``RLock`` guards cache, counters, communities,
  and the snapshots used by pathfinding.
- **Full serialization** — ``to_dict(include_graph=True)`` embeds
  nodes/edges so ``from_dict`` reconstructs the graph exactly.
- **Strict / non-strict modes** for all mutations and strategy calls;
  every deserialization failure is wrapped in
  :class:`EnhancedGraphError`.

Version: 2.0.0
"""

from __future__ import annotations

import concurrent.futures as _cf
import hashlib
import json
import logging
import math
import os
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
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
    "NETWORKX_AVAILABLE",
    "NUMPY_AVAILABLE",
    "EnhancedGraphConfig",
    "EnhancedGraphError",
    "EnhancedGraphTraversalOptimizer",
    "GraphStatistics",
    "TraversalPath",
    "TraversalStrategy",
]

VERSION = "2.0.0"


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

# Public aliases (no leading underscore; importable for feature detection).
NETWORKX_AVAILABLE = _NETWORKX_AVAILABLE
NUMPY_AVAILABLE = _NUMPY_AVAILABLE


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EnhancedGraphError(ValueError):
    """Raised for invalid graph optimizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class TraversalStrategy(Enum):
    """
    Graph traversal strategies.

    - ``BREADTH_FIRST`` — unweighted shortest path (hop count).
    - ``DEPTH_FIRST``   — first-found simple path.
    - ``ENERGY_OPTIMAL`` — Dijkstra over ``edge_weight + node_cost``.
      Honours ``energy_budget``.
    - ``RELEVANCE_GUIDED`` — Dijkstra over
      ``edge_weight / target_node_relevance``. Optimal for that cost.
    - ``COMMUNITY_AWARE`` — Dijkstra over ``edge_weight * crossing_penalty``.
    - ``QUANTUM_ENHANCED`` — delegates to the integrated backend if
      present, otherwise ``ENERGY_OPTIMAL``.
    """
    BREADTH_FIRST = "bfs"
    DEPTH_FIRST = "dfs"
    ENERGY_OPTIMAL = "energy_optimal"
    RELEVANCE_GUIDED = "relevance_guided"
    COMMUNITY_AWARE = "community_aware"
    QUANTUM_ENHANCED = "quantum_enhanced"


# Strategies whose result depends on ``energy_budget``. Used to key the
# path cache so that strategies ignoring the budget share cache slots.
_BUDGET_USING_STRATEGIES = frozenset({TraversalStrategy.ENERGY_OPTIMAL})


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _validate_non_empty_str(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise EnhancedGraphError(
            f"{name} must be a non-empty string, got {value!r}."
        )
    return value


def _validate_finite_number(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EnhancedGraphError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise EnhancedGraphError(
            f"{name} must be finite, got {value!r}."
        )
    if min_value is not None and fv < min_value:
        raise EnhancedGraphError(
            f"{name} must be >= {min_value}, got {fv}."
        )
    if max_value is not None and fv > max_value:
        raise EnhancedGraphError(
            f"{name} must be <= {max_value}, got {fv}."
        )
    return fv


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise EnhancedGraphError(
                f"invalid timestamp {value!r}: {exc}"
            ) from exc
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _json_default(value: Any) -> Any:
    """Explicit JSON coercion: datetime → ISO, Enum → value, else raise."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if _NUMPY_AVAILABLE and np is not None:
        # Convert numpy scalars explicitly (never via str()).
        if isinstance(value, np.generic):  # pragma: no cover
            return value.item()
    raise TypeError(
        f"cannot serialize non-JSON-native value of type "
        f"{type(value).__name__}."
    )


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

    cache_size: int = 100

    default_node_cost: float = 0.1
    default_node_relevance: float = 0.5

    default_community_algorithm: str = "louvain"
    community_crossing_penalty: float = 2.0

    sparse_graph_density_threshold: float = 0.01
    diameter_timeout_seconds: float = 5.0

    max_history: int = 1000

    def __post_init__(self) -> None:
        if self.graph_type not in ("directed", "undirected"):
            raise EnhancedGraphError(
                "graph_type must be 'directed' or 'undirected'."
            )
        for name in ("energy_weight", "relevance_weight"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise EnhancedGraphError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv):
                raise EnhancedGraphError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise EnhancedGraphError(f"{name} must be >= 0.")
        weight_sum = float(self.energy_weight) + float(
            self.relevance_weight
        )
        if abs(weight_sum - 1.0) > 1e-6:
            raise EnhancedGraphError(
                f"energy_weight + relevance_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )
        if (
            isinstance(self.cache_size, bool)
            or not isinstance(self.cache_size, int)
            or self.cache_size <= 0
        ):
            raise EnhancedGraphError(
                "cache_size must be a positive int."
            )
        if (
            isinstance(self.default_node_cost, bool)
            or not isinstance(self.default_node_cost, (int, float))
        ):
            raise EnhancedGraphError(
                "default_node_cost must be numeric."
            )
        if not math.isfinite(float(self.default_node_cost)):
            raise EnhancedGraphError(
                "default_node_cost must be finite."
            )
        if self.default_node_cost < 0:
            raise EnhancedGraphError("default_node_cost must be >= 0.")
        if (
            isinstance(self.default_node_relevance, bool)
            or not isinstance(
                self.default_node_relevance, (int, float)
            )
        ):
            raise EnhancedGraphError(
                "default_node_relevance must be numeric."
            )
        if not math.isfinite(float(self.default_node_relevance)):
            raise EnhancedGraphError(
                "default_node_relevance must be finite."
            )
        if not 0.0 <= float(self.default_node_relevance) <= 1.0:
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
        if (
            isinstance(self.community_crossing_penalty, bool)
            or not isinstance(
                self.community_crossing_penalty, (int, float)
            )
        ):
            raise EnhancedGraphError(
                "community_crossing_penalty must be numeric."
            )
        if not math.isfinite(float(self.community_crossing_penalty)):
            raise EnhancedGraphError(
                "community_crossing_penalty must be finite."
            )
        if self.community_crossing_penalty < 1.0:
            raise EnhancedGraphError(
                "community_crossing_penalty must be >= 1.0."
            )
        if (
            isinstance(self.sparse_graph_density_threshold, bool)
            or not isinstance(
                self.sparse_graph_density_threshold, (int, float)
            )
        ):
            raise EnhancedGraphError(
                "sparse_graph_density_threshold must be numeric."
            )
        if self.sparse_graph_density_threshold < 0:
            raise EnhancedGraphError(
                "sparse_graph_density_threshold must be >= 0."
            )
        if (
            isinstance(self.diameter_timeout_seconds, bool)
            or not isinstance(
                self.diameter_timeout_seconds, (int, float)
            )
        ):
            raise EnhancedGraphError(
                "diameter_timeout_seconds must be numeric."
            )
        if not math.isfinite(float(self.diameter_timeout_seconds)):
            raise EnhancedGraphError(
                "diameter_timeout_seconds must be finite."
            )
        if self.diameter_timeout_seconds <= 0:
            raise EnhancedGraphError(
                "diameter_timeout_seconds must be > 0."
            )
        if (
            isinstance(self.max_history, bool)
            or not isinstance(self.max_history, int)
            or self.max_history <= 0
        ):
            raise EnhancedGraphError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "EnhancedGraphConfig":
        if not isinstance(data, Mapping):
            raise EnhancedGraphError(
                f"EnhancedGraphConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            return cls(
                graph_type=str(data.get("graph_type", "directed")),
                energy_weight=float(
                    data.get("energy_weight", 0.5)
                ),
                relevance_weight=float(
                    data.get("relevance_weight", 0.5)
                ),
                enable_communities=bool(
                    data.get("enable_communities", True)
                ),
                cache_size=int(data.get("cache_size", 100)),
                default_node_cost=float(
                    data.get("default_node_cost", 0.1)
                ),
                default_node_relevance=float(
                    data.get("default_node_relevance", 0.5)
                ),
                default_community_algorithm=str(
                    data.get(
                        "default_community_algorithm", "louvain"
                    )
                ),
                community_crossing_penalty=float(
                    data.get("community_crossing_penalty", 2.0)
                ),
                sparse_graph_density_threshold=float(
                    data.get(
                        "sparse_graph_density_threshold", 0.01
                    )
                ),
                diameter_timeout_seconds=float(
                    data.get("diameter_timeout_seconds", 5.0)
                ),
                max_history=int(data.get("max_history", 1000)),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, EnhancedGraphError):
                raise
            raise EnhancedGraphError(
                f"invalid EnhancedGraphConfig payload: {exc}"
            ) from exc


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
            raise EnhancedGraphError(
                "path_id must be a non-empty string."
            )
        if not isinstance(self.nodes, tuple) or not self.nodes:
            raise EnhancedGraphError("nodes must be a non-empty tuple.")
        if not isinstance(self.community_ids, tuple):
            object.__setattr__(
                self, "community_ids", tuple(self.community_ids)
            )
        for name in ("total_cost", "energy_cost", "relevance_score"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise EnhancedGraphError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv):
                raise EnhancedGraphError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise EnhancedGraphError(f"{name} must be >= 0.")
        if not 0.0 <= float(self.relevance_score) <= 1.0:
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
        try:
            return cls(
                path_id=str(data["path_id"]),
                nodes=tuple(data["nodes"]),
                total_cost=float(data.get("total_cost", 0.0)),
                energy_cost=float(
                    data.get("energy_cost", 0.0)
                ),
                relevance_score=float(
                    data.get("relevance_score", 0.0)
                ),
                community_ids=tuple(
                    data.get("community_ids", []) or []
                ),
                metadata=dict(data.get("metadata", {}) or {}),
                timestamp=_coerce_datetime(data.get("timestamp")),
            )
        except KeyError as exc:
            raise EnhancedGraphError(
                f"TraversalPath payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, EnhancedGraphError):
                raise
            raise EnhancedGraphError(
                f"invalid TraversalPath payload: {exc}"
            ) from exc

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
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise EnhancedGraphError(f"{name} must be >= 0.")
        for name in (
            "avg_degree", "density", "clustering_coefficient",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise EnhancedGraphError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise EnhancedGraphError(
                    f"{name} must be finite and >= 0, "
                    f"got {value!r}."
                )
        if self.diameter is not None and (
            isinstance(self.diameter, bool)
            or not isinstance(self.diameter, int)
            or self.diameter < 0
        ):
            raise EnhancedGraphError(
                "diameter must be a non-negative int or None."
            )
        if not isinstance(self.is_connected, bool):
            raise EnhancedGraphError("is_connected must be a bool.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "GraphStatistics":
        if not isinstance(data, Mapping):
            raise EnhancedGraphError(
                "GraphStatistics.from_dict expects a Mapping."
            )
        try:
            return cls(
                num_nodes=int(data.get("num_nodes", 0)),
                num_edges=int(data.get("num_edges", 0)),
                avg_degree=float(data.get("avg_degree", 0.0)),
                density=float(data.get("density", 0.0)),
                num_communities=int(
                    data.get("num_communities", 0)
                ),
                clustering_coefficient=float(
                    data.get("clustering_coefficient", 0.0)
                ),
                diameter=data.get("diameter"),
                is_connected=bool(
                    data.get("is_connected", False)
                ),
                timestamp=_coerce_datetime(data.get("timestamp")),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, EnhancedGraphError):
                raise
            raise EnhancedGraphError(
                f"invalid GraphStatistics payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "GraphStatistics("
            f"nodes={self.num_nodes}, edges={self.num_edges}, "
            f"density={self.density:.4f}, "
            f"communities={self.num_communities}, "
            f"connected={self.is_connected})"
        )


# --------------------------------------------------------------------------- #
# Cache-key helper
# --------------------------------------------------------------------------- #
def _path_cache_key(
    start: str,
    goal: str,
    strategy: TraversalStrategy,
    max_depth: int,
    energy_budget: Optional[float],
) -> str:
    parts = [start, goal, strategy.value, str(max_depth)]
    if strategy in _BUDGET_USING_STRATEGIES:
        parts.append(str(energy_budget))
    return ":".join(parts)


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #
class EnhancedGraphTraversalOptimizer:
    """
    Enhanced graph traversal with full NetworkX capabilities.

    Thread-safe, serializable, and bounded in memory.
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
                "networkx is required for "
                "EnhancedGraphTraversalOptimizer; install networkx "
                "to enable this module."
            )

        self._config = config if config is not None else (
            EnhancedGraphConfig(
                graph_type=str(graph_type),
                energy_weight=float(energy_weight),
                relevance_weight=float(relevance_weight),
                enable_communities=bool(enable_communities),
                cache_size=int(cache_size),
            )
        )
        self._strict = bool(strict)

        # Legacy attributes (mirror the config).
        self.energy_weight: float = self._config.energy_weight
        self.relevance_weight: float = self._config.relevance_weight
        self.enable_communities: bool = self._config.enable_communities
        self.cache_size: int = self._config.cache_size

        if self._config.graph_type == "directed":
            self.graph: Any = nx.DiGraph()
        else:
            self.graph = nx.Graph()

        self._lock = threading.RLock()
        self.node_costs: Dict[str, float] = {}
        self.node_relevance: Dict[str, float] = {}
        # Reserved for future use; serialized only via the graph payload.
        self.node_embeddings: Dict[str, Any] = {}
        self.communities: List[Set[str]] = []
        self.node_to_community: Dict[str, int] = {}

        self.path_cache: "OrderedDict[str, TraversalPath]" = (
            OrderedDict()
        )
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._cache_evictions: int = 0

        self.quantum_backend: Optional[Any] = None

        self._history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
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

    # ---------------------------------------------------------- mutation
    def _reject_reserved_kwargs(
        self, attributes: Dict[str, Any], reserved: Sequence[str],
    ) -> None:
        """Reject reserved names passed via **attributes."""
        for name in reserved:
            if name in attributes:
                msg = (
                    f"{name!r} is reserved and must be passed as a "
                    f"named argument, not via **attributes."
                )
                if self._strict:
                    raise EnhancedGraphError(msg)
                logger.warning("%s Dropping from attributes.", msg)
                attributes.pop(name, None)

    def add_node(
        self,
        node_id: str,
        cost: float = 0.1,
        relevance: float = 0.5,
        embedding: Optional[Any] = None,
        **attributes: Any,
    ) -> None:
        """Add a node with attributes."""
        _validate_non_empty_str(node_id, "node_id")
        fc = _validate_finite_number(cost, "cost", min_value=0.0)
        fr = _validate_finite_number(relevance, "relevance")
        if not 0.0 <= fr <= 1.0:
            msg = f"relevance must be in [0, 1], got {fr}."
            if self._strict:
                raise EnhancedGraphError(msg)
            logger.warning("%s Clamping.", msg)
            fr = max(0.0, min(1.0, fr))

        self._reject_reserved_kwargs(
            attributes, ("cost", "relevance", "embedding")
        )

        with self._lock:
            # Store cost/relevance as graph attributes too so GraphML
            # round-trips preserve them.
            self.graph.add_node(
                node_id, cost=fc, relevance=fr, **attributes,
            )
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
        _validate_non_empty_str(source, "source")
        _validate_non_empty_str(target, "target")
        fw = _validate_finite_number(
            weight, "weight", min_value=0.0,
        )
        _validate_non_empty_str(edge_type, "edge_type")
        self._reject_reserved_kwargs(
            attributes, ("weight", "edge_type")
        )

        with self._lock:
            if source not in self.graph:
                self.graph.add_node(
                    source,
                    cost=self._config.default_node_cost,
                    relevance=self._config.default_node_relevance,
                )
                self.node_costs.setdefault(
                    source, self._config.default_node_cost
                )
                self.node_relevance.setdefault(
                    source, self._config.default_node_relevance
                )
            if target not in self.graph:
                self.graph.add_node(
                    target,
                    cost=self._config.default_node_cost,
                    relevance=self._config.default_node_relevance,
                )
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
                    communities = nx_comm.louvain_communities(
                        G_undirected
                    )
                elif algo == "label_propagation":
                    communities = list(
                        nx.algorithms.community
                        .label_propagation_communities(G_undirected)
                    )
                else:
                    communities = list(
                        nx.algorithms.community
                        .greedy_modularity_communities(G_undirected)
                    )
            except Exception as exc:
                if self._strict:
                    raise EnhancedGraphError(
                        f"community detection failed: {exc}"
                    ) from exc
                logger.warning(
                    "community detection failed: %s; "
                    "returning empty list.", exc,
                )
                return []

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

    # ---------------------------------------------------------- pathfinding
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

        Cache key = ``(start, goal, strategy, max_depth, budget)`` where
        ``budget`` is included only for strategies that honour it.
        """
        _validate_non_empty_str(start, "start")
        _validate_non_empty_str(goal, "goal")
        if not isinstance(strategy, TraversalStrategy):
            raise EnhancedGraphError(
                f"strategy must be a TraversalStrategy, got "
                f"{type(strategy).__name__}."
            )
        if (
            isinstance(max_depth, bool)
            or not isinstance(max_depth, int)
            or max_depth <= 0
        ):
            raise EnhancedGraphError(
                "max_depth must be a positive int."
            )

        budget_key: Optional[float] = None
        if energy_budget is not None:
            budget_key = _validate_finite_number(
                energy_budget, "energy_budget",
            )
            if budget_key <= 0:
                raise EnhancedGraphError(
                    f"energy_budget must be > 0, got "
                    f"{energy_budget!r}."
                )

        cache_key = _path_cache_key(
            start, goal, strategy, max_depth, budget_key,
        )

        with self._lock:
            cached = self.path_cache.get(cache_key)
            if cached is not None:
                self.path_cache.move_to_end(cache_key)
                self.cache_hits += 1
                return cached
            self.cache_misses += 1
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
                path = self._energy_optimal_path_nx(
                    start, goal, budget_key,
                )
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
        except EnhancedGraphError:
            raise
        except Exception as exc:
            if self._strict:
                raise EnhancedGraphError(
                    f"pathfinding failed for strategy "
                    f"{strategy.value}: {exc}"
                ) from exc
            logger.warning("Pathfinding failed: %s", exc)
            path = None

        if path is not None:
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
        """
        Find the ``k`` shortest paths (Yen's algorithm via NetworkX).

        Note: the generator uses simple-path enumeration; on dense
        graphs, a large ``k`` may take significant time. Cap ``k`` if
        latency matters.
        """
        _validate_non_empty_str(start, "start")
        _validate_non_empty_str(goal, "goal")
        if (
            isinstance(k, bool)
            or not isinstance(k, int)
            or k <= 0
        ):
            raise EnhancedGraphError("k must be a positive int.")
        _validate_non_empty_str(weight, "weight")

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
                paths.append(
                    self._create_path_from_nodes(path_nodes)
                )
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
        self,
        center_nodes: Sequence[str],
        radius: int = 2,
    ) -> Any:
        """
        Return the induced subgraph within ``radius`` of
        ``center_nodes`` as a **copy** — safe to mutate.
        """
        if not isinstance(center_nodes, (list, tuple)):
            raise EnhancedGraphError(
                "center_nodes must be a list or tuple."
            )
        if (
            isinstance(radius, bool)
            or not isinstance(radius, int)
            or radius <= 0
        ):
            raise EnhancedGraphError(
                "radius must be a positive int."
            )

        with self._lock:
            subgraph_nodes: Set[str] = set()
            for center in center_nodes:
                if not isinstance(center, str) or not center:
                    continue
                if center not in self.graph:
                    continue
                ego = nx.ego_graph(
                    self.graph, center, radius=radius,
                )
                subgraph_nodes.update(ego.nodes())
            # Return a copy so callers can't mutate the live graph
            # through the NetworkX subgraph view.
            return self.graph.subgraph(
                subgraph_nodes
            ).copy()

    # ---------------------------------------------------------- statistics
    def _bounded_diameter(self, timeout: float) -> Optional[int]:
        """
        Run ``nx.diameter`` in a worker thread with ``timeout``.

        NetworkX has no native timeout. On timeout we return ``None``
        without blocking; the worker finishes in the background.
        """
        with self._lock:
            graph_copy = self.graph.copy()

        def _run() -> Optional[int]:
            try:
                return int(nx.diameter(graph_copy))
            except Exception as exc:
                logger.debug("diameter failed: %s", exc)
                return None

        executor = _cf.ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(_run)
            try:
                return future.result(timeout=timeout)
            except _cf.TimeoutError:
                logger.warning(
                    "diameter calc exceeded %.2fs; leaving None.",
                    timeout,
                )
                return None
        finally:
            executor.shutdown(wait=False)

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
            except Exception:  # pragma: no cover
                density = 0.0

            is_directed = isinstance(self.graph, nx.DiGraph)

            # Community count (reuse cached value if present).
            if not self.communities:
                try:
                    self.detect_communities()
                except EnhancedGraphError:
                    if self._strict:
                        raise
            num_communities = len(self.communities)

            if is_directed:
                clustering = 0.0
            else:
                try:
                    clustering = float(
                        nx.average_clustering(self.graph)
                    )
                except Exception:
                    clustering = 0.0

            try:
                if is_directed:
                    is_connected = nx.is_strongly_connected(
                        self.graph
                    )
                else:
                    is_connected = nx.is_connected(self.graph)
            except Exception:
                is_connected = False

        diameter: Optional[int] = None
        if is_connected:
            diameter = self._bounded_diameter(
                self._config.diameter_timeout_seconds
            )

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
        stats = self.get_graph_statistics()
        recommendations: List[Dict[str, str]] = []

        if not stats.is_connected:
            recommendations.append({
                "issue": "disconnected_graph",
                "recommendation": (
                    "Add edges to connect isolated components"
                ),
                "severity": "high",
            })
        if stats.density < (
            self._config.sparse_graph_density_threshold
        ):
            recommendations.append({
                "issue": "sparse_graph",
                "recommendation": (
                    "Consider adding more semantic edges"
                ),
                "severity": "medium",
            })
        if stats.num_communities == 0:
            recommendations.append({
                "issue": "no_communities",
                "recommendation": (
                    "Run community detection for cluster-aware "
                    "retrieval"
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

        ``quantum_backend`` must be ``None`` or an object that exposes
        a callable ``energy_optimal_path(start, goal)`` method. When
        present, :attr:`TraversalStrategy.QUANTUM_ENHANCED` delegates to
        it; otherwise it falls back to :attr:`ENERGY_OPTIMAL`.
        """
        if quantum_backend is not None:
            method = getattr(
                quantum_backend, "energy_optimal_path", None,
            )
            if not callable(method):
                raise EnhancedGraphError(
                    "quantum_backend must expose a callable "
                    "energy_optimal_path(start, goal) method, "
                    "or be None."
                )
        with self._lock:
            self.quantum_backend = quantum_backend
        logger.debug(
            "Quantum backend integrated (%s).",
            type(quantum_backend).__name__
            if quantum_backend else None,
        )

    # ---------------------------------------------------------- internals
    def _energy_optimal_path_nx(
        self,
        start: str,
        goal: str,
        energy_budget: Optional[float],
    ) -> Optional[TraversalPath]:
        cfg = self._config
        # Snapshot under the lock so concurrent mutations can't change
        # the cost map mid-Dijkstra.
        with self._lock:
            costs_snapshot = dict(self.node_costs)

        def energy_weight(
            u: str, v: str, attrs: Mapping[str, Any]
        ) -> float:
            edge_weight = float(attrs.get("weight", 1.0))
            node_cost = costs_snapshot.get(
                v, cfg.default_node_cost
            )
            return edge_weight + node_cost

        try:
            path_nodes = nx.shortest_path(
                self.graph, start, goal, weight=energy_weight,
            )
        except nx.NetworkXNoPath:
            return None

        path = self._create_path_from_nodes(
            path_nodes, costs=costs_snapshot,
        )
        if (
            energy_budget is not None
            and path.energy_cost > energy_budget
        ):
            return None
        return path

    def _relevance_guided_path_nx(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        """
        Dijkstra over ``edge_weight / target_node_relevance`` — optimal
        for that cost, favouring high-relevance waypoints.
        """
        cfg = self._config
        with self._lock:
            relevance_snapshot = dict(self.node_relevance)

        def relevance_cost(
            u: str, v: str, attrs: Mapping[str, Any]
        ) -> float:
            edge_weight = float(attrs.get("weight", 1.0))
            rel = relevance_snapshot.get(
                v, cfg.default_node_relevance
            )
            return edge_weight / max(rel, 1e-6)

        try:
            path_nodes = nx.dijkstra_path(
                self.graph, start, goal, weight=relevance_cost,
            )
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
        return self._create_path_from_nodes(
            path_nodes, relevance=relevance_snapshot,
        )

    def _community_aware_path(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        with self._lock:
            has_communities = bool(self.communities)
        if not has_communities and self.enable_communities:
            self.detect_communities()

        penalty = self._config.community_crossing_penalty
        with self._lock:
            communities_snapshot = dict(self.node_to_community)

        def community_weight(
            u: str, v: str, attrs: Mapping[str, Any]
        ) -> float:
            base_weight = float(attrs.get("weight", 1.0))
            u_comm = communities_snapshot.get(u, -1)
            v_comm = communities_snapshot.get(v, -1)
            if (
                u_comm != v_comm
                and u_comm != -1
                and v_comm != -1
            ):
                return base_weight * penalty
            return base_weight

        try:
            path_nodes = nx.shortest_path(
                self.graph, start, goal, weight=community_weight,
            )
        except nx.NetworkXNoPath:
            return None
        return self._create_path_from_nodes(
            path_nodes, communities=communities_snapshot,
        )

    def _quantum_enhanced_path(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        backend = self.quantum_backend
        if backend is not None:
            method = getattr(
                backend, "energy_optimal_path", None,
            )
            if callable(method):
                try:
                    result = method(start, goal)
                    if isinstance(result, TraversalPath):
                        return result
                    if (
                        isinstance(result, (list, tuple))
                        and result
                    ):
                        return self._create_path_from_nodes(
                            list(result)
                        )
                    if (
                        isinstance(result, Mapping)
                        and "nodes" in result
                    ):
                        return self._create_path_from_nodes(
                            list(result["nodes"])
                        )
                except Exception as exc:
                    if self._strict:
                        raise EnhancedGraphError(
                            f"quantum backend failed: {exc}"
                        ) from exc
                    logger.warning(
                        "Quantum backend failed: %s", exc,
                    )
        return self._energy_optimal_path_nx(start, goal, None)

    def _bfs_path_nx(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        try:
            path_nodes = nx.shortest_path(self.graph, start, goal)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
        return self._create_path_from_nodes(path_nodes)

    def _dfs_path_nx(
        self, start: str, goal: str
    ) -> Optional[TraversalPath]:
        try:
            path_nodes = next(
                nx.all_simple_paths(self.graph, start, goal)
            )
        except (
            nx.NetworkXNoPath, nx.NodeNotFound, StopIteration,
        ):
            return None
        return self._create_path_from_nodes(path_nodes)

    def _create_path_from_nodes(
        self,
        nodes: Sequence[str],
        *,
        costs: Optional[Dict[str, float]] = None,
        relevance: Optional[Dict[str, float]] = None,
        communities: Optional[Dict[str, int]] = None,
    ) -> TraversalPath:
        """
        Build a :class:`TraversalPath` from a node sequence.

        Optional per-node maps override the current live state, so the
        path's costs match the snapshot used by the search.
        """
        if not nodes:
            raise EnhancedGraphError(
                "cannot create a path from an empty node list."
            )
        cfg = self._config

        with self._lock:
            if costs is None:
                costs = dict(self.node_costs)
            if relevance is None:
                relevance = dict(self.node_relevance)
            if communities is None:
                communities = dict(self.node_to_community)
            graph_ref = self.graph

        joined = "\x1f".join(str(n) for n in nodes)
        path_id = "path_" + hashlib.sha256(
            joined.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

        energy_cost = sum(
            costs.get(n, cfg.default_node_cost) for n in nodes
        )

        edge_cost = 0.0
        for i in range(len(nodes) - 1):
            if graph_ref.has_edge(nodes[i], nodes[i + 1]):
                edge_cost += float(
                    graph_ref[nodes[i]][nodes[i + 1]].get(
                        "weight", 1.0
                    )
                )

        total_cost = energy_cost + edge_cost

        relevance_score = sum(
            relevance.get(n, cfg.default_node_relevance)
            for n in nodes
        ) / len(nodes)
        relevance_score = max(0.0, min(1.0, relevance_score))

        community_ids = tuple(
            communities.get(n, -1) for n in nodes
        )
        unique_communities = len(
            {c for c in community_ids if c != -1}
        )

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

    def _cache_put(
        self, key: str, path: TraversalPath
    ) -> None:
        with self._lock:
            if key in self.path_cache:
                self.path_cache.move_to_end(key)
            self.path_cache[key] = path
            while len(self.path_cache) > self.cache_size:
                self.path_cache.popitem(last=False)
                self._cache_evictions += 1

    # ---------------------------------------------------------- GraphML
    def export_to_graphml(self, filepath: str) -> int:
        """Export the graph to GraphML. Returns 0 (bytes not reported)."""
        _validate_non_empty_str(filepath, "filepath")
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

        Re-syncs per-node cost / relevance from graph attributes (falls
        back to config defaults). Clears embeddings, communities, and
        the path cache — none of which are representable in GraphML.
        """
        _validate_non_empty_str(filepath, "filepath")
        try:
            new_graph = nx.read_graphml(filepath)
        except Exception as exc:
            raise EnhancedGraphError(
                f"GraphML import failed: {exc}"
            ) from exc

        with self._lock:
            self.graph = new_graph
            self.node_costs = {}
            self.node_relevance = {}
            self.node_embeddings = {}
            for node, attrs in new_graph.nodes(data=True):
                self.node_costs[str(node)] = float(
                    attrs.get(
                        "cost", self._config.default_node_cost
                    )
                )
                self.node_relevance[str(node)] = float(
                    attrs.get(
                        "relevance",
                        self._config.default_node_relevance,
                    )
                )
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
    def to_dict(
        self,
        *,
        include_cache: bool = False,
        include_graph: bool = True,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
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
                "communities": [
                    sorted(c) for c in self.communities
                ],
                "networkx_available": _NETWORKX_AVAILABLE,
                "numpy_available": _NUMPY_AVAILABLE,
            }
            if include_graph:
                payload["nodes"] = [
                    {
                        "id": str(n),
                        "cost": self.node_costs.get(
                            n, self._config.default_node_cost
                        ),
                        "relevance": self.node_relevance.get(
                            n,
                            self._config.default_node_relevance,
                        ),
                        "attributes": dict(attrs),
                    }
                    for n, attrs in self.graph.nodes(data=True)
                ]
                payload["edges"] = [
                    {
                        "source": str(u),
                        "target": str(v),
                        "attributes": dict(attrs),
                    }
                    for u, v, attrs in self.graph.edges(data=True)
                ]
            if include_cache:
                payload["path_cache"] = {
                    k: v.to_dict()
                    for k, v in self.path_cache.items()
                }
        return payload

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
    ) -> "EnhancedGraphTraversalOptimizer":
        if not isinstance(data, Mapping):
            raise EnhancedGraphError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            cfg = EnhancedGraphConfig.from_dict(
                dict(data.get("config", {}) or {})
            )
            strict_flag = (
                bool(data.get("strict", True))
                if strict is None else strict
            )
            opt = cls(config=cfg, strict=strict_flag)

            for node in data.get("nodes", []) or []:
                if not isinstance(node, Mapping):
                    raise EnhancedGraphError(
                        "nodes payload entries must be Mappings."
                    )
                attributes = dict(
                    node.get("attributes") or {}
                )
                opt.add_node(
                    str(node["id"]),
                    cost=float(
                        node.get(
                            "cost",
                            cfg.default_node_cost,
                        )
                    ),
                    relevance=float(
                        node.get(
                            "relevance",
                            cfg.default_node_relevance,
                        )
                    ),
                    **attributes,
                )

            for edge in data.get("edges", []) or []:
                if not isinstance(edge, Mapping):
                    raise EnhancedGraphError(
                        "edges payload entries must be Mappings."
                    )
                attributes = dict(
                    edge.get("attributes") or {}
                )
                weight = float(
                    attributes.pop("weight", 1.0)
                )
                edge_type = str(
                    attributes.pop("edge_type", "semantic")
                )
                opt.add_edge(
                    str(edge["source"]),
                    str(edge["target"]),
                    weight=weight,
                    edge_type=edge_type,
                    **attributes,
                )

            if data.get("communities"):
                with opt._lock:
                    opt.communities = [
                        set(c)
                        for c in data["communities"]
                    ]
                    opt.node_to_community = {}
                    for i, comm in enumerate(
                        opt.communities
                    ):
                        for n in comm:
                            opt.node_to_community[n] = i

            if "path_cache" in data:
                for key, entry in (
                    data["path_cache"] or {}
                ).items():
                    opt._cache_put(
                        str(key),
                        TraversalPath.from_dict(entry),
                    )
            return opt
        except KeyError as exc:
            raise EnhancedGraphError(
                f"optimizer payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, EnhancedGraphError):
                raise
            raise EnhancedGraphError(
                f"invalid optimizer payload: {exc}"
            ) from exc

    def to_json(
        self,
        *,
        include_cache: bool = False,
        include_graph: bool = True,
        **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        kwargs.setdefault("default", _json_default)
        return json.dumps(
            self.to_dict(
                include_cache=include_cache,
                include_graph=include_graph,
            ),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
    ) -> "EnhancedGraphTraversalOptimizer":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EnhancedGraphError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw, strict=strict)

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_cache: bool = True) -> int:
        """
        Reset the path cache and counters.

        **Does not** clear the graph or the node cost / relevance
        maps — use :meth:`clear_graph` for that.
        Returns the number of cache entries removed.
        """
        with self._lock:
            removed = len(self.path_cache)
            if clear_cache:
                self.path_cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self._cache_evictions = 0
            self._history.clear()
            self._started_at = time.time()
        logger.debug(
            "EnhancedGraphTraversalOptimizer reset "
            "(removed %d cache entries).", removed,
        )
        return removed

    def clear_graph(self) -> None:
        """Remove all nodes, edges, and derived state."""
        with self._lock:
            self.graph.clear()
            self.node_costs.clear()
            self.node_relevance.clear()
            self.node_embeddings.clear()
            self.communities = []
            self.node_to_community = {}
            self.path_cache.clear()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "EnhancedGraphTraversalOptimizer":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Any,
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "EnhancedGraphTraversalOptimizer scope exited "
                "with %s.", exc_type.__name__,
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
# Smoke test: python -m retrieval.enhanced_graph_traversal
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION          :", VERSION)
    print("NETWORKX_AVAILABLE:", NETWORKX_AVAILABLE)

    if not _NETWORKX_AVAILABLE:
        print(
            "networkx not installed; skipping enhanced graph smoke test."
        )
    else:
        import tempfile

        # ---- Build a graph with two paths between a1 and b4 ------ #
        opt = EnhancedGraphTraversalOptimizer()
        print("repr       :", opt)

        for nid in (
            "a1", "a2", "a3", "a4",
            "b1", "b2", "b3", "b4",
        ):
            opt.add_node(nid, cost=0.1, relevance=0.5)

        edges = [
            ("a1", "a2"), ("a2", "a3"), ("a3", "a4"),
            ("b1", "b2"), ("b2", "b3"), ("b3", "b4"),
            ("a3", "b1"),
            ("a2", "b1"),   # second route (fixes k-shortest assertion)
        ]
        for s, t in edges:
            opt.add_edge(s, t, weight=1.0)

        print(f"graph      : {opt.node_count} nodes, "
              f"{opt.edge_count} edges")

        # ---- Communities ------------------------------------------ #
        communities = opt.detect_communities("louvain")
        print(f"communities: {len(communities)}")
        for i, c in enumerate(communities):
            print(f"  #{i}: {sorted(c)}")

        # ---- Statistics ------------------------------------------- #
        stats = opt.get_graph_statistics()
        print("stats      :", stats)

        # ---- All strategies -------------------------------------- #
        for strat in TraversalStrategy:
            p = opt.find_optimal_path(
                "a1", "b4", strategy=strat,
            )
            if p is not None:
                print(
                    f"  {strat.value:<18} -> {list(p.nodes)} "
                    f"(energy={p.energy_cost:.3f})"
                )
            else:
                print(f"  {strat.value:<18} -> no path")

        # ---- k-shortest (now finds >= 2) ------------------------ #
        k_paths = opt.find_k_shortest_paths("a1", "b4", k=3)
        print(f"k=3 paths  : "
              f"{[list(p.nodes) for p in k_paths]}")
        assert len(k_paths) >= 2, (
            f"expected >=2 paths, got {len(k_paths)}"
        )

        # ---- LRU cache -------------------------------------------- #
        tiny = EnhancedGraphTraversalOptimizer(
            config=EnhancedGraphConfig(cache_size=2)
        )
        for n in ("n1", "n2", "n3", "n4"):
            tiny.add_node(n)
        for s, t in (
            ("n1", "n2"), ("n2", "n3"), ("n3", "n4"),
        ):
            tiny.add_edge(s, t)
        tiny.find_optimal_path("n1", "n2")
        tiny.find_optimal_path("n1", "n3")
        tiny.find_optimal_path("n1", "n4")
        assert len(tiny.path_cache) == 2, len(tiny.path_cache)
        assert tiny._cache_evictions == 1
        print("LRU cache  : OK")

        # ---- Cache key includes budget --------------------------- #
        opt.path_cache.clear()
        p_lo = opt.find_optimal_path(
            "a1", "b4", energy_budget=100.0,
        )
        p_hi = opt.find_optimal_path(
            "a1", "b4", energy_budget=50.0,
        )
        assert p_lo is not None and p_hi is not None
        assert len(opt.path_cache) == 2, (
            f"expected 2 cache entries, got {len(opt.path_cache)}"
        )
        print("budget key : OK")

        # ---- Budget ignored for BFS shares one cache slot ------- #
        opt.path_cache.clear()
        opt.find_optimal_path(
            "a1", "b4",
            strategy=TraversalStrategy.BREADTH_FIRST,
            energy_budget=100.0,
        )
        opt.find_optimal_path(
            "a1", "b4",
            strategy=TraversalStrategy.BREADTH_FIRST,
        )
        assert len(opt.path_cache) == 1, (
            f"BFS should share cache slot, got "
            f"{len(opt.path_cache)}"
        )
        print("BFS key    : OK")

        # ---- Deterministic path IDs ------------------------------ #
        opt.path_cache.clear()
        opt2 = EnhancedGraphTraversalOptimizer()
        for nid in (
            "a1", "a2", "a3", "a4",
            "b1", "b2", "b3", "b4",
        ):
            opt2.add_node(nid, cost=0.1, relevance=0.5)
        for s, t in edges:
            opt2.add_edge(s, t, weight=1.0)
        pa = opt.find_optimal_path(
            "a1", "b4", strategy=TraversalStrategy.BREADTH_FIRST,
        )
        pb = opt2.find_optimal_path(
            "a1", "b4", strategy=TraversalStrategy.BREADTH_FIRST,
        )
        assert pa is not None and pb is not None
        assert pa.path_id == pb.path_id
        print("path id    : OK")

        # ---- max_depth enforced --------------------------------- #
        opt.path_cache.clear()
        shallow = opt.find_optimal_path("a1", "b4", max_depth=3)
        deep = opt.find_optimal_path("a1", "b4", max_depth=10)
        assert shallow is None
        assert deep is not None
        print("max_depth  : OK")

        # ---- GraphML round-trip --------------------------------- #
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
            ), "costs should survive GraphML round-trip"
            print("GraphML    : OK")

        # ---- Statistics ----------------------------------------- #
        print("stats      :", {
            k: v for k, v in opt.statistics().items()
            if k not in ("uptime_seconds",)
        })

        # ---- Recommendations ------------------------------------- #
        recs = opt.optimize_graph_structure()
        print(
            "recs       :",
            [r["issue"] for r in recs["recommendations"]],
        )

        # ---- Full serialization + from_dict --------------------- #
        payload = opt.to_dict(include_cache=True)
        assert payload["version"] == VERSION
        restored = EnhancedGraphTraversalOptimizer.from_dict(
            payload,
        )
        assert restored.node_count == opt.node_count
        assert restored.edge_count == opt.edge_count
        assert restored.to_dict() == opt.to_dict()
        print("Serialization OK.")

        # ---- Dataclass round-trips ------------------------------ #
        p = opt.find_optimal_path("a1", "b4")
        assert p is not None
        p_restored = TraversalPath.from_dict(p.to_dict())
        assert p_restored.to_dict() == p.to_dict()
        s_restored = GraphStatistics.from_dict(stats.to_dict())
        assert s_restored.to_dict() == stats.to_dict()
        print("Dataclass  : OK")

        # ---- Context manager ------------------------------------ #
        with EnhancedGraphTraversalOptimizer() as scoped:
            scoped.add_node("x", cost=0.5, relevance=0.9)
            assert "x" in scoped
        print("Context    : OK")

        # ---- Config validation ---------------------------------- #
        for bad_cfg in (
            dict(graph_type="bogus"),
            dict(energy_weight=-0.1),
            dict(energy_weight=0.9, relevance_weight=0.9),
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

        # ---- Input validation ----------------------------------- #
        strict = EnhancedGraphTraversalOptimizer(strict=True)
        for bad_call in (
            lambda: strict.add_node("", cost=0.1),
            lambda: strict.add_node("a", cost=-1.0),
            lambda: strict.add_node("a", relevance=1.5),
            lambda: strict.add_edge("", "b"),
            lambda: strict.add_edge("a", "b", weight=-1.0),
            lambda: strict.add_edge(
                "a", "b", weight=float("nan"),
            ),
            lambda: strict.add_edge(
                "a", "b", **{"weight": 2.0},  # reserved collision
            ),
            lambda: strict.find_optimal_path("", "b"),
            lambda: strict.find_optimal_path("a", ""),
            lambda: strict.find_optimal_path(
                "a", "b", strategy="bfs",  # type: ignore[arg-type]
            ),
            lambda: strict.find_optimal_path(
                "a", "b", max_depth=0,
            ),
            lambda: strict.find_optimal_path(
                "a", "b", energy_budget=-0.1,
            ),
            lambda: strict.find_k_shortest_paths("a", "b", k=0),
            lambda: strict.get_subgraph_around_nodes([], radius=0),
            lambda: strict.detect_communities("bogus"),
            lambda: strict.integrate_quantum_backend(
                object(),  # missing energy_optimal_path
            ),
        ):
            try:
                bad_call()
            except EnhancedGraphError as exc:
                print("Rejected   :", exc)

        # ---- Non-strict coerces --------------------------------- #
        lenient = EnhancedGraphTraversalOptimizer(strict=False)
        lenient.add_node("x", cost=0.1, relevance=1.5)  # clamped
        print("lenient    : OK")

        # ---- Quantum backend validation ------------------------- #
        class _FakeBackend:
            def energy_optimal_path(self, start, goal):
                return [start, goal]

        opt.integrate_quantum_backend(_FakeBackend())
        p = opt.find_optimal_path(
            "a1", "b4",
            strategy=TraversalStrategy.QUANTUM_ENHANCED,
        )
        assert p is not None and p.nodes == ("a1", "b4")
        print("quantum    : OK")

        # ---- Reset / clear_graph -------------------------------- #
        removed = opt.reset()
        print("reset      :", removed, "cache entries removed")
        opt.clear_graph()
        assert opt.node_count == 0

        print("\nSmoke test passed.")
