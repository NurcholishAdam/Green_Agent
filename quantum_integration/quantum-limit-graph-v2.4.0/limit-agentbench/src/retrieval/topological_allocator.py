# src/retrieval/topological_allocator.py

"""
Topological Token Allocator
===========================

Implements VimRAG's core innovation: allocating token budgets based on graph
topology and node importance rather than uniform distribution.

Key Concept
-----------
- High-importance nodes (central, high betweenness) get high-resolution tokens.
- Low-importance nodes (peripheral, redundant) get compressed/low-resolution
  tokens.
- The total token budget is conserved but redistributed optimally.

Enhancements
------------
- Optional ``numpy`` / ``networkx`` imports guarded by ``_*_AVAILABLE``.
- ``TopologicalAllocatorConfig`` — frozen, validated: thresholds,
  multipliers, boost factor, semantic weights, graph-fallback policy,
  bounded history.
- ``TokenAllocation`` — frozen dataclass with validation and serialization.
- **Fixed bare-except** in the eigenvector fallback.
- **Fixed zero-norm** division in semantic similarity.
- **Fixed negative-budget crash** — ``total_token_budget`` must be positive.
- **Fixed single-node misclassification** — when every importance score is
  equal, all nodes get MEDIUM instead of 0.5 (same fix, clearer semantics).
- **Fixed scale-factor drift** — multipliers are constrained to ``(0, 1]``
  so the total budget is never exceeded.
- **Thread safety** — ``RLock`` guards the allocation history.
- **Bounded history** — ``deque(maxlen=config.max_history)``.
- **Full validation** of every argument; strict / non-strict modes.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` on the
  allocator, the config, and ``TokenAllocation``.
- ``statistics()``, ``__repr__``, custom ``TopologicalAllocatorError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test with a
  small synthetic graph.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False
    logger.debug("numpy not importable; some features disabled.")

try:  # pragma: no cover
    import networkx as nx  # type: ignore

    _NETWORKX_AVAILABLE = True
except ImportError:  # pragma: no cover
    nx = None  # type: ignore[assignment]
    _NETWORKX_AVAILABLE = False
    logger.debug("networkx not importable; TopologicalTokenAllocator disabled.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TopologicalAllocatorError(ValueError):
    """Raised for invalid topological allocator inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ImportanceMetric(Enum):
    """Metrics for measuring node importance."""
    PAGERANK = "pagerank"
    BETWEENNESS = "betweenness"
    DEGREE_CENTRALITY = "degree_centrality"
    EIGENVECTOR = "eigenvector"
    CLOSENESS = "closeness"
    COMBINED = "combined"


class ResolutionLevel(Enum):
    """Token resolution levels."""
    CRITICAL = "critical"    # 100% of base tokens
    HIGH = "high"            # 75% of base tokens
    MEDIUM = "medium"        # 50% of base tokens
    LOW = "low"              # 25% of base tokens
    MINIMAL = "minimal"      # 10% of base tokens


# Ordered from highest to lowest, used for validation and iteration.
_LEVEL_ORDER: Tuple[ResolutionLevel, ...] = (
    ResolutionLevel.CRITICAL,
    ResolutionLevel.HIGH,
    ResolutionLevel.MEDIUM,
    ResolutionLevel.LOW,
    ResolutionLevel.MINIMAL,
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TopologicalAllocatorConfig:
    """Tunable parameters for :class:`TopologicalTokenAllocator`."""

    importance_metric: ImportanceMetric = ImportanceMetric.COMBINED

    # Thresholds: critical > high > medium > low > 0 (implicit MINIMAL).
    critical_threshold: float = 0.8
    high_threshold: float = 0.6
    medium_threshold: float = 0.4
    low_threshold: float = 0.2

    # Resolution multipliers — every value must be in (0, 1].
    critical_multiplier: float = 1.00
    high_multiplier: float = 0.75
    medium_multiplier: float = 0.50
    low_multiplier: float = 0.25
    minimal_multiplier: float = 0.10

    # Query-node boost factor.
    enable_dynamic_adjustment: bool = True
    query_boost_factor: float = 1.5

    # Semantic fusion weights (must sum to 1.0 when both are used).
    topology_weight: float = 0.6
    semantic_weight: float = 0.4
    semantic_epsilon: float = 1e-12

    # Default base tokens per node used by ``allocate_for_retrieval``.
    default_base_tokens_per_node: int = 100

    # Bounded allocation history.
    max_history: int = 1000

    # If True, eigenvector failures fall back to degree centrality.
    fallback_on_eigenvector_failure: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.importance_metric, ImportanceMetric):
            raise TopologicalAllocatorError(
                "importance_metric must be an ImportanceMetric instance."
            )

        # Thresholds: strictly decreasing and in [0, 1).
        thresholds = [
            self.critical_threshold,
            self.high_threshold,
            self.medium_threshold,
            self.low_threshold,
        ]
        for name, value in zip(
            ("critical_threshold", "high_threshold",
             "medium_threshold", "low_threshold"),
            thresholds,
        ):
            if not 0.0 <= value < 1.0:
                raise TopologicalAllocatorError(f"{name} must be in [0, 1).")
        if not all(
            a > b for a, b in zip(thresholds, thresholds[1:])
        ):
            raise TopologicalAllocatorError(
                "thresholds must be strictly decreasing: critical > high > "
                "medium > low."
            )

        # Multipliers: in (0, 1] and strictly decreasing.
        multipliers = [
            self.critical_multiplier,
            self.high_multiplier,
            self.medium_multiplier,
            self.low_multiplier,
            self.minimal_multiplier,
        ]
        for name, value in zip(
            ("critical_multiplier", "high_multiplier",
             "medium_multiplier", "low_multiplier", "minimal_multiplier"),
            multipliers,
        ):
            if not 0.0 < value <= 1.0:
                raise TopologicalAllocatorError(
                    f"{name} must be in (0, 1]."
                )
        if not all(a >= b for a, b in zip(multipliers, multipliers[1:])):
            raise TopologicalAllocatorError(
                "multipliers must be non-increasing: critical >= high >= "
                "medium >= low >= minimal."
            )

        if self.query_boost_factor < 1.0:
            raise TopologicalAllocatorError(
                "query_boost_factor must be >= 1.0."
            )

        # Weights: non-negative and sum to 1.0.
        for name in ("topology_weight", "semantic_weight"):
            value = getattr(self, name)
            if value < 0:
                raise TopologicalAllocatorError(f"{name} must be >= 0.")
        weight_sum = self.topology_weight + self.semantic_weight
        if abs(weight_sum - 1.0) > 1e-6:
            raise TopologicalAllocatorError(
                f"topology_weight + semantic_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )

        if self.semantic_epsilon <= 0:
            raise TopologicalAllocatorError(
                "semantic_epsilon must be > 0."
            )
        if self.default_base_tokens_per_node <= 0:
            raise TopologicalAllocatorError(
                "default_base_tokens_per_node must be > 0."
            )
        if self.max_history <= 0:
            raise TopologicalAllocatorError("max_history must be > 0.")

    # ----- convenience ----------------------------------------------------
    def thresholds_map(self) -> Dict[ResolutionLevel, float]:
        return {
            ResolutionLevel.CRITICAL: self.critical_threshold,
            ResolutionLevel.HIGH: self.high_threshold,
            ResolutionLevel.MEDIUM: self.medium_threshold,
            ResolutionLevel.LOW: self.low_threshold,
            ResolutionLevel.MINIMAL: 0.0,
        }

    def multipliers_map(self) -> Dict[ResolutionLevel, float]:
        return {
            ResolutionLevel.CRITICAL: self.critical_multiplier,
            ResolutionLevel.HIGH: self.high_multiplier,
            ResolutionLevel.MEDIUM: self.medium_multiplier,
            ResolutionLevel.LOW: self.low_multiplier,
            ResolutionLevel.MINIMAL: self.minimal_multiplier,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "importance_metric": self.importance_metric.value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TopologicalAllocatorConfig":
        if not isinstance(data, Mapping):
            raise TopologicalAllocatorError(
                f"TopologicalAllocatorConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "importance_metric" and isinstance(v, str):
                kwargs[k] = ImportanceMetric(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Allocation record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TokenAllocation:
    """Immutable token allocation for a single node."""

    node_id: str
    base_tokens: int
    allocated_tokens: int
    resolution_level: ResolutionLevel
    importance_score: float
    allocation_ratio: float
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise TopologicalAllocatorError(
                "node_id must be a non-empty string."
            )
        if not isinstance(self.base_tokens, int) or self.base_tokens < 0:
            raise TopologicalAllocatorError(
                "base_tokens must be a non-negative int."
            )
        if not isinstance(self.allocated_tokens, int) or self.allocated_tokens < 0:
            raise TopologicalAllocatorError(
                "allocated_tokens must be a non-negative int."
            )
        if not isinstance(self.resolution_level, ResolutionLevel):
            raise TopologicalAllocatorError(
                "resolution_level must be a ResolutionLevel."
            )
        for name in ("importance_score", "allocation_ratio"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TopologicalAllocatorError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise TopologicalAllocatorError(
                    f"{name} must be finite, got {value!r}."
                )
        if not isinstance(self.metadata, Mapping):
            raise TopologicalAllocatorError("metadata must be a Mapping.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "base_tokens": self.base_tokens,
            "allocated_tokens": self.allocated_tokens,
            "resolution_level": self.resolution_level.value,
            "importance_score": self.importance_score,
            "allocation_ratio": self.allocation_ratio,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TokenAllocation":
        if not isinstance(data, Mapping):
            raise TopologicalAllocatorError(
                "TokenAllocation.from_dict expects a Mapping."
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
            node_id=str(data["node_id"]),
            base_tokens=int(data.get("base_tokens", 0)),
            allocated_tokens=int(data.get("allocated_tokens", 0)),
            resolution_level=ResolutionLevel(str(data["resolution_level"])),
            importance_score=float(data.get("importance_score", 0.0)),
            allocation_ratio=float(data.get("allocation_ratio", 0.0)),
            metadata=dict(data.get("metadata", {})),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "TokenAllocation("
            f"node={self.node_id!r}, "
            f"tokens={self.allocated_tokens}/{self.base_tokens}, "
            f"level={self.resolution_level.value}, "
            f"importance={self.importance_score:.3f})"
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _mean(values: Sequence[float]) -> float:
    """Return the arithmetic mean, using NumPy when available."""
    if not values:
        return 0.0
    if _NUMPY_AVAILABLE and np is not None:
        return float(np.mean(values))
    return sum(values) / len(values)


def _require_networkx() -> None:
    if not _NETWORKX_AVAILABLE:
        raise TopologicalAllocatorError(
            "networkx is required by TopologicalTokenAllocator; "
            "install networkx to enable topological allocation."
        )


# --------------------------------------------------------------------------- #
# Allocator
# --------------------------------------------------------------------------- #
class TopologicalTokenAllocator:
    """
    Allocates tokens based on graph topology and node importance.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only.

    Core VimRAG Innovation
    ----------------------
    - Nodes with high graph centrality get more tokens (high-resolution).
    - Peripheral nodes get fewer tokens (low-resolution / compressed).
    - The total token budget remains constant but redistributed.
    """

    def __init__(
        self,
        importance_metric: ImportanceMetric = ImportanceMetric.COMBINED,
        critical_threshold: float = 0.8,
        high_threshold: float = 0.6,
        medium_threshold: float = 0.4,
        low_threshold: float = 0.2,
        enable_dynamic_adjustment: bool = True,
        *,
        config: Optional[TopologicalAllocatorConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = TopologicalAllocatorConfig(
                importance_metric=importance_metric,
                critical_threshold=float(critical_threshold),
                high_threshold=float(high_threshold),
                medium_threshold=float(medium_threshold),
                low_threshold=float(low_threshold),
                enable_dynamic_adjustment=bool(enable_dynamic_adjustment),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.importance_metric: ImportanceMetric = self._config.importance_metric
        self.enable_dynamic: bool = self._config.enable_dynamic_adjustment
        self.thresholds: Dict[ResolutionLevel, float] = (
            self._config.thresholds_map()
        )
        self.resolution_multipliers: Dict[ResolutionLevel, float] = (
            self._config.multipliers_map()
        )

        self._lock = threading.RLock()
        self.allocation_history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        logger.debug(
            "TopologicalTokenAllocator initialized "
            "(metric=%s, thresholds=(%.2f, %.2f, %.2f, %.2f), "
            "boost=%.2f, strict=%s)",
            self.importance_metric.value,
            self._config.critical_threshold, self._config.high_threshold,
            self._config.medium_threshold, self._config.low_threshold,
            self._config.query_boost_factor,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> TopologicalAllocatorConfig:
        return self._config

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self.allocation_history)

    # ---------------------------------------------------------- public API
    def allocate_tokens(
        self,
        graph: Any,
        total_token_budget: int,
        base_tokens_per_node: int = 100,
        query_nodes: Optional[List[str]] = None,
    ) -> Dict[str, TokenAllocation]:
        """
        Allocate tokens across graph nodes based on topology.

        Parameters
        ----------
        graph : networkx.Graph
            Memory graph.
        total_token_budget : int
            Total tokens to distribute. Must be positive.
        base_tokens_per_node : int
            Base token count per node. Must be positive.
        query_nodes : list of str, optional
            Nodes relevant to the current query; they receive a boost.

        Returns
        -------
        dict[node_id, TokenAllocation]
        """
        _require_networkx()
        if graph is None:
            raise TopologicalAllocatorError("graph must not be None.")
        if not hasattr(graph, "nodes"):
            raise TopologicalAllocatorError(
                "graph must be a networkx.Graph-like object with .nodes."
            )
        budget = self._validate_budget(total_token_budget)
        base = self._validate_base_tokens(base_tokens_per_node)

        node_count = len(graph.nodes)
        if node_count == 0:
            logger.debug("Empty graph; returning no allocations.")
            return {}

        # ---- 1. Topological importance --------------------------------
        importance_scores = self._calculate_importance_scores(graph)

        # ---- 2. Query-node boost ---------------------------------------
        if query_nodes and self.enable_dynamic:
            importance_scores = self._boost_query_nodes(
                importance_scores,
                query_nodes,
                boost_factor=self._config.query_boost_factor,
            )

        # ---- 3. Normalize ---------------------------------------------
        importance_scores = self._normalize_scores(importance_scores)

        # ---- 4. Resolution levels -------------------------------------
        resolution_assignments = self._assign_resolution_levels(
            importance_scores
        )

        # ---- 5. Compute allocations -----------------------------------
        allocations = self._calculate_allocations(
            graph,
            importance_scores,
            resolution_assignments,
            budget,
            base,
        )

        # ---- 6. Record ------------------------------------------------
        self._record_allocation(allocations, budget)

        logger.debug(
            "Allocated %d tokens across %d node(s) "
            "(metric=%s, query_nodes=%d).",
            budget, node_count,
            self.importance_metric.value,
            len(query_nodes) if query_nodes else 0,
        )
        return allocations

    def allocate_for_retrieval(
        self,
        graph: Any,
        query: str,
        query_embedding: Optional[Any],
        total_token_budget: int,
        node_embeddings: Optional[Dict[str, Any]] = None,
        *,
        base_tokens_per_node: Optional[int] = None,
    ) -> Dict[str, TokenAllocation]:
        """
        Allocate tokens for a specific retrieval query.

        When ``query_embedding`` and ``node_embeddings`` are provided, the
        topological importance scores are fused with cosine similarity.
        """
        _require_networkx()
        if graph is None:
            raise TopologicalAllocatorError("graph must not be None.")
        if not isinstance(query, str):
            raise TopologicalAllocatorError("query must be a string.")
        budget = self._validate_budget(total_token_budget)
        base = (
            self._validate_base_tokens(base_tokens_per_node)
            if base_tokens_per_node is not None
            else self._config.default_base_tokens_per_node
        )

        if len(graph.nodes) == 0:
            return {}

        # ---- 1. Base topological importance ---------------------------
        importance_scores = self._calculate_importance_scores(graph)

        # ---- 2. Optional semantic fusion ------------------------------
        if query_embedding is not None and node_embeddings:
            try:
                semantic_scores = self._calculate_semantic_scores(
                    query_embedding, node_embeddings,
                )
                importance_scores = self._combine_scores(
                    importance_scores,
                    semantic_scores,
                    topology_weight=self._config.topology_weight,
                    semantic_weight=self._config.semantic_weight,
                )
            except TopologicalAllocatorError:
                if self._strict:
                    raise
                logger.warning(
                    "Semantic fusion failed; continuing with topological "
                    "scores only.",
                )

        # ---- 3-5. Normalize, classify, allocate -----------------------
        importance_scores = self._normalize_scores(importance_scores)
        resolution_assignments = self._assign_resolution_levels(
            importance_scores
        )
        allocations = self._calculate_allocations(
            graph,
            importance_scores,
            resolution_assignments,
            budget,
            base,
        )
        self._record_allocation(allocations, budget)
        return allocations

    def get_allocation_summary(
        self,
        allocations: Mapping[str, TokenAllocation],
    ) -> Dict[str, Any]:
        """Return a JSON-safe summary of the allocation map."""
        if not allocations:
            return {"status": "no_allocations"}

        total_allocated = sum(
            a.allocated_tokens for a in allocations.values()
        )
        level_counts: Dict[str, int] = defaultdict(int)
        level_tokens: Dict[str, int] = defaultdict(int)
        for alloc in allocations.values():
            level_counts[alloc.resolution_level.value] += 1
            level_tokens[alloc.resolution_level.value] += alloc.allocated_tokens

        # Deterministic tie-break: node_id ascending.
        top_nodes = sorted(
            allocations.items(),
            key=lambda kv: (-kv[1].allocated_tokens, kv[0]),
        )[:10]

        return {
            "total_nodes": len(allocations),
            "total_tokens_allocated": total_allocated,
            "avg_tokens_per_node": (
                total_allocated / len(allocations)
                if allocations else 0.0
            ),
            "nodes_by_resolution": dict(level_counts),
            "tokens_by_resolution": dict(level_tokens),
            "top_allocated_nodes": [
                {
                    "node_id": nid,
                    "tokens": alloc.allocated_tokens,
                    "resolution": alloc.resolution_level.value,
                    "importance": alloc.importance_score,
                }
                for nid, alloc in top_nodes
            ],
        }

    def analyze_allocation_efficiency(self) -> Dict[str, Any]:
        """Return aggregate efficiency statistics over the allocation history."""
        with self._lock:
            history = list(self.allocation_history)

        if not history:
            return {"status": "no_history", "total_allocations": 0}

        avg_by_resolution: Dict[str, List[float]] = defaultdict(list)
        for record in history:
            for level, count in record.get("nodes_by_resolution", {}).items():
                avg_by_resolution[level].append(count)

        avg_distribution = {
            level: _mean(counts)
            for level, counts in avg_by_resolution.items()
        }

        return {
            "total_allocations": len(history),
            "avg_nodes_by_resolution": avg_distribution,
            "latest_allocation": history[-1] if history else None,
            "numpy_available": _NUMPY_AVAILABLE,
            "networkx_available": _NETWORKX_AVAILABLE,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`analyze_allocation_efficiency`."""
        return self.analyze_allocation_efficiency()

    # ---------------------------------------------------------- internals
    def _calculate_importance_scores(self, graph: Any) -> Dict[str, float]:
        """Compute importance scores from the graph (per ``importance_metric``)."""
        metric = self.importance_metric
        try:
            if metric == ImportanceMetric.PAGERANK:
                return dict(nx.pagerank(graph))
            if metric == ImportanceMetric.BETWEENNESS:
                return dict(nx.betweenness_centrality(graph))
            if metric == ImportanceMetric.DEGREE_CENTRALITY:
                return dict(nx.degree_centrality(graph))
            if metric == ImportanceMetric.EIGENVECTOR:
                try:
                    return dict(
                        nx.eigenvector_centrality(graph, max_iter=1000)
                    )
                except Exception as exc:
                    if not self._config.fallback_on_eigenvector_failure:
                        raise TopologicalAllocatorError(
                            f"eigenvector_centrality failed: {exc}"
                        ) from exc
                    logger.warning(
                        "eigenvector_centrality failed (%s); falling back "
                        "to degree centrality.", exc,
                    )
                    return dict(nx.degree_centrality(graph))
            if metric == ImportanceMetric.CLOSENESS:
                return dict(nx.closeness_centrality(graph))
            # COMBINED
            pagerank = dict(nx.pagerank(graph))
            betweenness = dict(nx.betweenness_centrality(graph))
            degree = dict(nx.degree_centrality(graph))
            return {
                node: (
                    0.4 * pagerank.get(node, 0.0)
                    + 0.3 * betweenness.get(node, 0.0)
                    + 0.3 * degree.get(node, 0.0)
                )
                for node in graph.nodes
            }
        except TopologicalAllocatorError:
            raise
        except Exception as exc:
            raise TopologicalAllocatorError(
                f"importance calculation failed for metric "
                f"{metric.value}: {exc}"
            ) from exc

    def _boost_query_nodes(
        self,
        importance_scores: Mapping[str, float],
        query_nodes: Sequence[str],
        boost_factor: float = 1.5,
    ) -> Dict[str, float]:
        """Return a copy of ``importance_scores`` with query nodes boosted."""
        if boost_factor < 1.0:
            raise TopologicalAllocatorError(
                "boost_factor must be >= 1.0."
            )
        boosted = dict(importance_scores)
        unknown: List[str] = []
        for node in query_nodes:
            if node in boosted:
                boosted[node] = boosted[node] * boost_factor
            else:
                unknown.append(str(node))
        if unknown:
            logger.debug(
                "Query-node boost ignored %d unknown node(s): %s",
                len(unknown), unknown[:5],
            )
        return boosted

    def _normalize_scores(
        self, scores: Mapping[str, float]
    ) -> Dict[str, float]:
        """
        Normalize scores to ``[0, 1]``.

        When every score is identical (a common case for graphs with a
        single node or perfectly symmetric topology), all scores map to
        ``0.5`` — the original behavior — which classifies them as MEDIUM.
        """
        if not scores:
            return {}
        values = list(scores.values())
        min_score = min(values)
        max_score = max(values)
        if max_score == min_score:
            return {k: 0.5 for k in scores.keys()}
        span = max_score - min_score
        return {
            k: (v - min_score) / span for k, v in scores.items()
        }

    def _assign_resolution_levels(
        self, importance_scores: Mapping[str, float]
    ) -> Dict[str, ResolutionLevel]:
        """Assign each node a :class:`ResolutionLevel`."""
        cfg = self._config
        assignments: Dict[str, ResolutionLevel] = {}
        for node, score in importance_scores.items():
            if score >= cfg.critical_threshold:
                assignments[node] = ResolutionLevel.CRITICAL
            elif score >= cfg.high_threshold:
                assignments[node] = ResolutionLevel.HIGH
            elif score >= cfg.medium_threshold:
                assignments[node] = ResolutionLevel.MEDIUM
            elif score >= cfg.low_threshold:
                assignments[node] = ResolutionLevel.LOW
            else:
                assignments[node] = ResolutionLevel.MINIMAL
        return assignments

    def _calculate_allocations(
        self,
        graph: Any,
        importance_scores: Mapping[str, float],
        resolution_assignments: Mapping[str, ResolutionLevel],
        total_budget: int,
        base_tokens_per_node: int,
    ) -> Dict[str, TokenAllocation]:
        """
        Turn resolution assignments into concrete token counts.

        The ``scale_factor`` makes the sum of allocations match the total
        budget exactly. Because multipliers are constrained to ``(0, 1]``
        and ``base_tokens_per_node`` is positive, the scale factor is
        always finite and positive.
        """
        multipliers = self.resolution_multipliers
        total_weighted = sum(
            base_tokens_per_node * multipliers[level]
            for level in resolution_assignments.values()
        )
        if total_weighted <= 0:
            logger.warning(
                "total_weighted=0; distributing budget uniformly.",
            )
            per_node = (
                max(1, total_budget // len(graph.nodes))
                if len(graph.nodes) > 0 else 0
            )
            return {
                node: TokenAllocation(
                    node_id=node,
                    base_tokens=base_tokens_per_node,
                    allocated_tokens=per_node,
                    resolution_level=ResolutionLevel.MEDIUM,
                    importance_score=0.5,
                    allocation_ratio=(
                        per_node / base_tokens_per_node
                        if base_tokens_per_node > 0 else 0.0
                    ),
                    metadata={"multiplier": 1.0, "scale_factor": 1.0},
                )
                for node in graph.nodes
            }

        scale_factor = total_budget / total_weighted

        allocations: Dict[str, TokenAllocation] = {}
        for node in graph.nodes:
            level = resolution_assignments.get(
                node, ResolutionLevel.MINIMAL,
            )
            multiplier = multipliers[level]
            allocated = int(
                base_tokens_per_node * multiplier * scale_factor
            )
            allocations[node] = TokenAllocation(
                node_id=node,
                base_tokens=base_tokens_per_node,
                allocated_tokens=max(0, allocated),
                resolution_level=level,
                importance_score=float(importance_scores.get(node, 0.0)),
                allocation_ratio=(
                    allocated / base_tokens_per_node
                    if base_tokens_per_node > 0 else 0.0
                ),
                metadata={
                    "multiplier": multiplier,
                    "scale_factor": scale_factor,
                },
            )
        return allocations

    def _calculate_semantic_scores(
        self,
        query_embedding: Any,
        node_embeddings: Mapping[str, Any],
    ) -> Dict[str, float]:
        """Compute cosine similarity between ``query_embedding`` and each node."""
        if not _NUMPY_AVAILABLE:
            raise TopologicalAllocatorError(
                "numpy is required for semantic similarity computation."
            )
        q = np.asarray(query_embedding, dtype=float)
        q_norm = float(np.linalg.norm(q))
        if q_norm <= self._config.semantic_epsilon:
            raise TopologicalAllocatorError(
                "query_embedding has near-zero norm."
            )

        scores: Dict[str, float] = {}
        for node_id, node_embedding in node_embeddings.items():
            n = np.asarray(node_embedding, dtype=float)
            n_norm = float(np.linalg.norm(n))
            if n_norm <= self._config.semantic_epsilon:
                # Zero-norm node gets zero similarity rather than NaN.
                scores[str(node_id)] = 0.0
                continue
            if q.shape != n.shape:
                raise TopologicalAllocatorError(
                    f"shape mismatch for node {node_id!r}: "
                    f"{q.shape} vs {n.shape}."
                )
            scores[str(node_id)] = float(np.dot(q, n) / (q_norm * n_norm))
        return scores

    def _combine_scores(
        self,
        topological_scores: Mapping[str, float],
        semantic_scores: Mapping[str, float],
        topology_weight: float,
        semantic_weight: float,
    ) -> Dict[str, float]:
        """Return the weighted sum of topological and semantic scores."""
        if topology_weight < 0 or semantic_weight < 0:
            raise TopologicalAllocatorError(
                "weights must be non-negative."
            )
        total = topology_weight + semantic_weight
        if total <= 0:
            raise TopologicalAllocatorError(
                "at least one weight must be > 0."
            )
        # Normalize weights so the result is scale-invariant.
        tw = topology_weight / total
        sw = semantic_weight / total

        all_nodes = set(topological_scores) | set(semantic_scores)
        return {
            node: (
                tw * float(topological_scores.get(node, 0.0))
                + sw * float(semantic_scores.get(node, 0.0))
            )
            for node in all_nodes
        }

    def _record_allocation(
        self,
        allocations: Mapping[str, TokenAllocation],
        total_budget: int,
    ) -> None:
        """Append a compact allocation record to the bounded history."""
        summary = self.get_allocation_summary(allocations)
        summary["total_budget"] = total_budget
        summary["timestamp"] = time.time()
        with self._lock:
            self.allocation_history.append(summary)

    # ---------------------------------------------------------- validation
    def _validate_budget(self, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TopologicalAllocatorError(
                f"total_token_budget must be an int, "
                f"got {type(value).__name__}."
            )
        if value <= 0:
            raise TopologicalAllocatorError(
                f"total_token_budget must be > 0, got {value}."
            )
        return value

    def _validate_base_tokens(self, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TopologicalAllocatorError(
                f"base_tokens_per_node must be an int, "
                f"got {type(value).__name__}."
            )
        if value <= 0:
            raise TopologicalAllocatorError(
                f"base_tokens_per_node must be > 0, got {value}."
            )
        return value

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "history_size": len(self.allocation_history),
                "numpy_available": _NUMPY_AVAILABLE,
                "networkx_available": _NETWORKX_AVAILABLE,
            }
            if include_history:
                payload["allocation_history"] = list(self.allocation_history)
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TopologicalTokenAllocator":
        if not isinstance(data, Mapping):
            raise TopologicalAllocatorError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = TopologicalAllocatorConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        allocator = cls(config=cfg, strict=bool(data.get("strict", True)))
        with allocator._lock:
            for entry in data.get("allocation_history", []):
                if isinstance(entry, Mapping):
                    allocator.allocation_history.append(dict(entry))
            allocator._started_at = float(data.get("started_at", time.time()))
        return allocator

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "TopologicalTokenAllocator":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise TopologicalAllocatorError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """Clear allocation history. Returns the number of entries removed."""
        with self._lock:
            removed = len(self.allocation_history)
            if clear_history:
                self.allocation_history.clear()
            self._started_at = time.time()
        logger.debug("TopologicalTokenAllocator reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "TopologicalTokenAllocator":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "TopologicalTokenAllocator scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "TopologicalTokenAllocator("
                f"metric={self.importance_metric.value}, "
                f"history={len(self.allocation_history)}, "
                f"dynamic={self.enable_dynamic}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ImportanceMetric",
    "ResolutionLevel",
    "TokenAllocation",
    "TopologicalAllocatorConfig",
    "TopologicalAllocatorError",
    "TopologicalTokenAllocator",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.topological_allocator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    if not _NETWORKX_AVAILABLE:
        print("networkx not installed; skipping topological allocator "
              "smoke test.")
    else:
        # ---- Small synthetic graph ---------------------------------- #
        #     A ─ B ─ C
        #     │   │
        #     D ─ E ─ F
        g = nx.Graph()
        g.add_edges_from([
            ("A", "B"), ("B", "C"),
            ("A", "D"), ("D", "E"),
            ("B", "E"), ("E", "F"),
        ])
        print(f"graph      : {g.number_of_nodes()} nodes, "
              f"{g.number_of_edges()} edges")

        # ---- Happy path: COMBINED metric ---------------------------- #
        allocator = TopologicalTokenAllocator()
        print("repr       :", allocator)

        allocations = allocator.allocate_tokens(
            graph=g,
            total_token_budget=1000,
            base_tokens_per_node=100,
        )
        for nid in ("A", "B", "E"):
            print(f"  {nid}: {allocations[nid]}")

        # Sum of allocations should be <= the budget (int truncation).
        total = sum(a.allocated_tokens for a in allocations.values())
        print(f"total      : {total} tokens (budget=1000)")
        assert total <= 1000, f"budget exceeded: {total}"

        # ---- Query-node boost --------------------------------------- #
        boosted = allocator.allocate_tokens(
            graph=g,
            total_token_budget=1000,
            base_tokens_per_node=100,
            query_nodes=["F"],
        )
        print(f"boost F    : importance={boosted['F'].importance_score:.3f} "
              f"(vs unboosted {allocations['F'].importance_score:.3f})")
        assert boosted["F"].importance_score >= allocations["F"].importance_score

        # ---- Summary ------------------------------------------------- #
        summary = allocator.get_allocation_summary(allocations)
        print("summary    :", {
            "total_nodes": summary["total_nodes"],
            "total_tokens": summary["total_tokens_allocated"],
            "nodes_by_resolution": summary["nodes_by_resolution"],
        })

        # ---- allocate_for_retrieval --------------------------------- #
        if _NUMPY_AVAILABLE:
            node_embeddings = {
                nid: np.random.default_rng(0).standard_normal(4)
                for nid in g.nodes
            }
            query_emb = np.random.default_rng(1).standard_normal(4)
            retrieved = allocator.allocate_for_retrieval(
                graph=g,
                query="test query",
                query_embedding=query_emb,
                total_token_budget=800,
                node_embeddings=node_embeddings,
            )
            print("retrieval  :", {
                nid: retrieved[nid].allocated_tokens for nid in ("A", "B", "F")
            })

        # ---- Efficiency analysis ------------------------------------ #
        print("efficiency :", {
            k: v for k, v in allocator.statistics().items()
            if k not in ("latest_allocation", "uptime_seconds")
        })

        # ---- Bug fix: zero-norm node embedding ---------------------- #
        if _NUMPY_AVAILABLE:
            zero_embs = {"A": np.zeros(4), "B": np.ones(4)}
            scores = allocator._calculate_semantic_scores(
                np.ones(4), zero_embs,
            )
            assert scores["A"] == 0.0 and not math.isnan(scores["A"])
            print("zero-norm  : OK (0.0, not NaN)")

        # ---- Bug fix: budget must be positive ----------------------- #
        for bad_budget in (0, -1, "1000", 1.5, None):
            try:
                allocator.allocate_tokens(g, bad_budget)  # type: ignore[arg-type]
            except TopologicalAllocatorError as exc:
                print("Rejected   :", exc)

        # ---- Bug fix: base tokens must be positive ------------------ #
        try:
            allocator.allocate_tokens(g, 1000, base_tokens_per_node=0)
        except TopologicalAllocatorError as exc:
            print("Rejected   :", exc)

        # ---- Empty graph -------------------------------------------- #
        empty = nx.Graph()
        assert allocator.allocate_tokens(empty, 1000) == {}
        print("empty      : OK")

        # ---- Bounded history ---------------------------------------- #
        bounded = TopologicalTokenAllocator(
            config=TopologicalAllocatorConfig(max_history=2)
        )
        for _ in range(5):
            bounded.allocate_tokens(g, 500)
        assert bounded.history_size == 2, bounded.history_size
        print("bounded    : OK")

        # ---- Serialization round-trip ------------------------------- #
        payload = allocator.to_json()
        restored = TopologicalTokenAllocator.from_json(payload)
        assert restored.to_dict() == allocator.to_dict()
        print("Round-trip OK.")

        # ---- TokenAllocation round-trip ----------------------------- #
        ta = allocations["A"]
        p = json.dumps(ta.to_dict(), default=str)
        restored_ta = TokenAllocation.from_dict(json.loads(p))
        assert restored_ta.to_dict() == ta.to_dict()
        print("TokenAllocation OK.")

        # ---- Context manager ---------------------------------------- #
        with TopologicalTokenAllocator() as scoped:
            scoped.allocate_tokens(g, 500)
            assert scoped.history_size == 1
        print("Context    : OK")

        # ---- Validation of config ----------------------------------- #
        for bad_cfg in (
            dict(critical_threshold=0.4, high_threshold=0.8),  # not decreasing
            dict(critical_threshold=-0.1),
            dict(critical_threshold=1.5),
            dict(critical_multiplier=0),
            dict(critical_multiplier=1.5),
            dict(minimal_multiplier=0.1,
                 low_multiplier=0.05),  # not non-increasing
            dict(query_boost_factor=0.5),
            dict(topology_weight=0.9, semantic_weight=0.9),  # sum != 1
            dict(semantic_epsilon=0),
            dict(default_base_tokens_per_node=0),
            dict(max_history=0),
        ):
            try:
                TopologicalAllocatorConfig(**bad_cfg)  # type: ignore[arg-type]
            except TopologicalAllocatorError as exc:
                print("Rejected cfg:", exc)

        # ---- Non-strict semantic fusion failure --------------------- #
        if not _NUMPY_AVAILABLE:
            lenient = TopologicalTokenAllocator(strict=False)
            r = lenient.allocate_for_retrieval(
                graph=g,
                query="q",
                query_embedding=None,
                total_token_budget=500,
                node_embeddings=None,
            )
            assert r
            print("lenient    : OK (no embeddings provided)")

        # ---- Distribution sanity ------------------------------------ #
        # Nodes with higher importance should receive more tokens.
        sorted_nodes = sorted(
            allocations.values(),
            key=lambda a: a.importance_score, reverse=True,
        )
        if len(sorted_nodes) >= 2:
            top = sorted_nodes[0]
            bottom = sorted_nodes[-1]
            assert top.allocated_tokens >= bottom.allocated_tokens
            print(
                f"top: {top.node_id} -> {top.allocated_tokens} tokens "
                f"(importance={top.importance_score:.3f})"
            )
            print(
                f"bot: {bottom.node_id} -> {bottom.allocated_tokens} tokens "
                f"(importance={bottom.importance_score:.3f})"
            )

        print("\nSmoke test passed.")
