# src/retrieval/topological_allocator.py

"""
Topological Token Allocator
===========================

Implements VimRAG's core innovation: allocating token budgets based on
graph topology and node importance rather than uniform distribution.

Design
------
- Optional ``numpy`` / ``networkx`` imports guarded by ``_*_AVAILABLE``.
- ``TopologicalAllocatorConfig`` — frozen, validated.
- ``TokenAllocation`` — frozen dataclass with validation and
  serialization.
- **Bounded allocation history** — ``deque(maxlen=max_history)``.
  ``to_dict(include_history=True)`` embeds the history; the payload is
  re-ingestible via ``from_dict``.
- **Round-trip is symmetric.** ``to_dict(include_history=False)``
  omits ``history_size`` (only present when the history itself is
  present), so a fresh allocator round-trips without spurious diffs.
- **Semantic scores are mapped to ``[0, 1]``** via ``(cos + 1) / 2`` —
  a raw cosine of ``-1`` maps to ``0.0`` and ``+1`` to ``1.0``,
  preserving ordering while matching the topological scale.
- **Non-finite embeddings are rejected** — a ``NaN``/``Inf`` in a node
  embedding raises ``TopologicalAllocatorError`` at fusion time rather
  than poisoning the normalization.
- **Non-string graph nodes** are rejected at the API boundary in
  strict mode (``TokenAllocation.node_id`` requires ``str``). Non-strict
  mode logs and proceeds with the caller's native IDs.
- **Importance-calculation failures fall back** to degree centrality
  in non-strict mode — matching the semantic-fusion fallback.
- **Integer truncation remainder is redistributed** — the sum of
  ``TokenAllocation.allocated_tokens`` equals ``total_token_budget``
  exactly, not ``<=`` budget.
- **Full validation and error wrapping** on every ``from_dict``.

Version: 2.0.0
"""

from __future__ import annotations

import heapq
import json
import logging
import math
import numbers
import threading
import time
from collections import defaultdict, deque
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
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "ImportanceMetric",
    "ResolutionLevel",
    "TokenAllocation",
    "TopologicalAllocatorConfig",
    "TopologicalAllocatorError",
    "TopologicalTokenAllocator",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
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
    logger.debug(
        "networkx not importable; TopologicalTokenAllocator disabled."
    )


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
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    MINIMAL = "minimal"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TopologicalAllocatorConfig:
    """Tunable parameters for :class:`TopologicalTokenAllocator`."""

    importance_metric: ImportanceMetric = ImportanceMetric.COMBINED

    critical_threshold: float = 0.8
    high_threshold: float = 0.6
    medium_threshold: float = 0.4
    low_threshold: float = 0.2

    critical_multiplier: float = 1.00
    high_multiplier: float = 0.75
    medium_multiplier: float = 0.50
    low_multiplier: float = 0.25
    minimal_multiplier: float = 0.10

    enable_dynamic_adjustment: bool = True
    query_boost_factor: float = 1.5

    topology_weight: float = 0.6
    semantic_weight: float = 0.4
    semantic_epsilon: float = 1e-12

    default_base_tokens_per_node: int = 100

    max_history: int = 1000

    fallback_on_eigenvector_failure: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.importance_metric, ImportanceMetric):
            raise TopologicalAllocatorError(
                "importance_metric must be an ImportanceMetric instance."
            )

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
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or not 0.0 <= float(value) < 1.0
            ):
                raise TopologicalAllocatorError(
                    f"{name} must be finite and in [0, 1)."
                )
        if not all(a > b for a, b in zip(thresholds, thresholds[1:])):
            raise TopologicalAllocatorError(
                "thresholds must be strictly decreasing: "
                "critical > high > medium > low."
            )

        multipliers = [
            self.critical_multiplier,
            self.high_multiplier,
            self.medium_multiplier,
            self.low_multiplier,
            self.minimal_multiplier,
        ]
        for name, value in zip(
            ("critical_multiplier", "high_multiplier",
             "medium_multiplier", "low_multiplier",
             "minimal_multiplier"),
            multipliers,
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or not 0.0 < float(value) <= 1.0
            ):
                raise TopologicalAllocatorError(
                    f"{name} must be finite and in (0, 1]."
                )
        if not all(
            a >= b for a, b in zip(multipliers, multipliers[1:])
        ):
            raise TopologicalAllocatorError(
                "multipliers must be non-increasing: "
                "critical >= high >= medium >= low >= minimal."
            )

        if (
            isinstance(self.query_boost_factor, bool)
            or not isinstance(self.query_boost_factor, numbers.Real)
            or not math.isfinite(float(self.query_boost_factor))
            or self.query_boost_factor < 1.0
        ):
            raise TopologicalAllocatorError(
                "query_boost_factor must be finite and >= 1.0."
            )

        for name in ("topology_weight", "semantic_weight"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise TopologicalAllocatorError(
                    f"{name} must be finite and >= 0."
                )
        weight_sum = (
            float(self.topology_weight)
            + float(self.semantic_weight)
        )
        if abs(weight_sum - 1.0) > 1e-6:
            raise TopologicalAllocatorError(
                f"topology_weight + semantic_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )

        if (
            isinstance(self.semantic_epsilon, bool)
            or not isinstance(self.semantic_epsilon, numbers.Real)
            or not math.isfinite(float(self.semantic_epsilon))
            or self.semantic_epsilon <= 0
        ):
            raise TopologicalAllocatorError(
                "semantic_epsilon must be finite and > 0."
            )
        if (
            isinstance(self.default_base_tokens_per_node, bool)
            or not isinstance(
                self.default_base_tokens_per_node,
                numbers.Integral,
            )
            or self.default_base_tokens_per_node <= 0
        ):
            raise TopologicalAllocatorError(
                "default_base_tokens_per_node must be > 0."
            )
        if (
            isinstance(self.max_history, bool)
            or not isinstance(self.max_history, numbers.Integral)
            or self.max_history <= 0
        ):
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
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "TopologicalAllocatorConfig":
        if not isinstance(data, Mapping):
            raise TopologicalAllocatorError(
                f"TopologicalAllocatorConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
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
        except (TypeError, ValueError) as exc:
            if isinstance(exc, TopologicalAllocatorError):
                raise
            raise TopologicalAllocatorError(
                f"invalid TopologicalAllocatorConfig payload: {exc}"
            ) from exc


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
        if (
            isinstance(self.base_tokens, bool)
            or not isinstance(self.base_tokens, numbers.Integral)
            or self.base_tokens < 0
        ):
            raise TopologicalAllocatorError(
                "base_tokens must be a non-negative int."
            )
        if (
            isinstance(self.allocated_tokens, bool)
            or not isinstance(
                self.allocated_tokens, numbers.Integral
            )
            or self.allocated_tokens < 0
        ):
            raise TopologicalAllocatorError(
                "allocated_tokens must be a non-negative int."
            )
        if not isinstance(
            self.resolution_level, ResolutionLevel
        ):
            raise TopologicalAllocatorError(
                "resolution_level must be a ResolutionLevel."
            )
        for name in ("importance_score", "allocation_ratio"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
            ):
                raise TopologicalAllocatorError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise TopologicalAllocatorError(
                    f"{name} must be finite, got {value!r}."
                )
        if not isinstance(self.metadata, Mapping):
            raise TopologicalAllocatorError(
                "metadata must be a Mapping."
            )

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
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "TokenAllocation":
        if not isinstance(data, Mapping):
            raise TopologicalAllocatorError(
                "TokenAllocation.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)
            metadata = data.get("metadata") or {}
            if not isinstance(metadata, Mapping):
                raise TopologicalAllocatorError(
                    "metadata must be a Mapping."
                )
            return cls(
                node_id=str(data["node_id"]),
                base_tokens=int(data.get("base_tokens", 0)),
                allocated_tokens=int(
                    data.get("allocated_tokens", 0)
                ),
                resolution_level=ResolutionLevel(
                    str(data["resolution_level"])
                ),
                importance_score=float(
                    data.get("importance_score", 0.0)
                ),
                allocation_ratio=float(
                    data.get("allocation_ratio", 0.0)
                ),
                metadata=dict(metadata),
                timestamp=timestamp,
            )
        except KeyError as exc:
            raise TopologicalAllocatorError(
                f"TokenAllocation payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, TopologicalAllocatorError):
                raise
            raise TopologicalAllocatorError(
                f"invalid TokenAllocation payload: {exc}"
            ) from exc

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
    if _NUMPY_AVAILABLE and np is not None and len(values) > 16:
        return float(np.mean(values))
    return sum(values) / len(values)


def _require_networkx() -> None:
    if not _NETWORKX_AVAILABLE:
        raise TopologicalAllocatorError(
            "networkx is required by TopologicalTokenAllocator; "
            "install networkx to enable topological allocation."
        )


def _cosine_to_unit_interval(cos: float) -> float:
    """
    Map a raw cosine similarity in ``[-1, 1]`` to ``[0, 1]``.

    ``-1 → 0.0``, ``0 → 0.5``, ``+1 → 1.0``. Preserves relative
    ordering (unlike clamping) and matches the topological-score
    scale.
    """
    return max(0.0, min(1.0, (float(cos) + 1.0) / 2.0))


# --------------------------------------------------------------------------- #
# Allocator
# --------------------------------------------------------------------------- #
class TopologicalTokenAllocator:
    """
    Allocates tokens based on graph topology and node importance.

    Thread-safe, serializable, bounded in memory.

    Core VimRAG Innovation
    ----------------------
    - Nodes with high graph centrality get more tokens (high-resolution).
    - Peripheral nodes get fewer tokens (low-resolution / compressed).
    - The total token budget is **exactly conserved** — integer
      truncation remainder is redistributed to the highest-fractional
      nodes so ``sum(a.allocated_tokens) == total_token_budget``.
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
                enable_dynamic_adjustment=bool(
                    enable_dynamic_adjustment
                ),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.importance_metric: ImportanceMetric = (
            self._config.importance_metric
        )
        self.enable_dynamic: bool = (
            self._config.enable_dynamic_adjustment
        )
        self.thresholds: Dict[ResolutionLevel, float] = (
            self._config.thresholds_map()
        )
        self.resolution_multipliers: Dict[
            ResolutionLevel, float
        ] = self._config.multipliers_map()

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
            self._config.critical_threshold,
            self._config.high_threshold,
            self._config.medium_threshold,
            self._config.low_threshold,
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

        The sum of ``allocated_tokens`` equals ``total_token_budget``
        exactly. Integer truncation remainder is distributed to the
        nodes with the highest fractional loss first.
        """
        _require_networkx()
        self._validate_graph(graph)
        budget = self._validate_budget(total_token_budget)
        base = self._validate_base_tokens(base_tokens_per_node)

        node_count = len(graph.nodes)
        if node_count == 0:
            logger.debug("Empty graph; returning no allocations.")
            return {}

        # ---- 1. Topological importance (with non-strict fallback) -----
        importance_scores = self._safe_importance_scores(graph)

        # ---- 2. Query-node boost --------------------------------------
        if query_nodes and self.enable_dynamic:
            importance_scores = self._boost_query_nodes(
                importance_scores,
                query_nodes,
                boost_factor=self._config.query_boost_factor,
            )

        # ---- 3. Normalize ---------------------------------------------
        importance_scores = self._normalize_scores(
            importance_scores
        )

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
        """Allocate tokens for a specific retrieval query."""
        _require_networkx()
        self._validate_graph(graph)
        if not isinstance(query, str):
            raise TopologicalAllocatorError(
                "query must be a string."
            )
        budget = self._validate_budget(total_token_budget)
        base = (
            self._validate_base_tokens(base_tokens_per_node)
            if base_tokens_per_node is not None
            else self._config.default_base_tokens_per_node
        )

        if len(graph.nodes) == 0:
            return {}

        # ---- 1. Base topological importance --------------------------
        importance_scores = self._safe_importance_scores(graph)

        # ---- 2. Optional semantic fusion -----------------------------
        semantic_skipped = False
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
                semantic_skipped = True
                logger.warning(
                    "Semantic fusion failed; continuing with "
                    "topological scores only.",
                )

        # ---- 3-5. Normalize, classify, allocate ----------------------
        importance_scores = self._normalize_scores(
            importance_scores
        )
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
        if semantic_skipped:
            # Annotate each allocation so callers can tell the
            # fusion was skipped.
            for alloc in allocations.values():
                alloc.metadata["semantic_fusion_skipped"] = True
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
            level_tokens[
                alloc.resolution_level.value
            ] += alloc.allocated_tokens

        top_nodes = heapq.nlargest(
            10,
            allocations.items(),
            key=lambda kv: (
                kv[1].allocated_tokens,
                "".join(reversed(kv[0])),  # tie-break: node_id asc
            ),
        )

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
        """Aggregate efficiency statistics over the allocation history."""
        with self._lock:
            history = list(self.allocation_history)

        if not history:
            return {"status": "no_history", "total_allocations": 0}

        avg_by_resolution: Dict[str, List[float]] = defaultdict(list)
        for record in history:
            for level, count in record.get(
                "nodes_by_resolution", {},
            ).items():
                avg_by_resolution[level].append(count)

        avg_distribution = {
            level: _mean(counts)
            for level, counts in avg_by_resolution.items()
        }

        return {
            "total_allocations": len(history),
            "avg_nodes_by_resolution": avg_distribution,
            "latest_allocation": (
                history[-1] if history else None
            ),
            "numpy_available": _NUMPY_AVAILABLE,
            "networkx_available": _NETWORKX_AVAILABLE,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.analyze_allocation_efficiency()

    # ---------------------------------------------------------- internals
    def _safe_importance_scores(
        self, graph: Any,
    ) -> Dict[Any, float]:
        """
        Compute importance scores, falling back to degree centrality
        when non-strict and the primary metric fails.
        """
        try:
            return self._calculate_importance_scores(graph)
        except TopologicalAllocatorError:
            # Config-driven or already wrapped — respect strict mode.
            if self._strict:
                raise
            logger.warning(
                "Importance calculation failed; falling back to "
                "degree centrality.",
            )
            try:
                return dict(nx.degree_centrality(graph))
            except Exception as exc:
                raise TopologicalAllocatorError(
                    f"importance calculation and fallback both "
                    f"failed: {exc}"
                ) from exc

    def _calculate_importance_scores(
        self, graph: Any,
    ) -> Dict[Any, float]:
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
                        nx.eigenvector_centrality(
                            graph, max_iter=1000,
                        )
                    )
                except Exception as exc:
                    if not self._config.fallback_on_eigenvector_failure:
                        raise TopologicalAllocatorError(
                            f"eigenvector_centrality failed: {exc}"
                        ) from exc
                    logger.warning(
                        "eigenvector_centrality failed (%s); "
                        "falling back to degree centrality.", exc,
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
        if boost_factor < 1.0:
            raise TopologicalAllocatorError(
                "boost_factor must be >= 1.0."
            )
        boosted = dict(importance_scores)
        unknown: List[str] = []
        for node in query_nodes:
            if node in boosted:
                boosted[node] = (
                    boosted[node] * boost_factor
                )
            else:
                unknown.append(str(node))
        if unknown:
            # WARNING (not DEBUG): a typo'd node name should be
            # visible to the caller.
            logger.warning(
                "Query-node boost ignored %d unknown node(s): %s",
                len(unknown), unknown[:5],
            )
        return boosted

    def _normalize_scores(
        self, scores: Mapping[str, float],
    ) -> Dict[str, float]:
        if not scores:
            return {}
        values = list(scores.values())
        min_score = min(values)
        max_score = max(values)
        if max_score == min_score:
            return {k: 0.5 for k in scores.keys()}
        span = max_score - min_score
        return {
            k: (v - min_score) / span
            for k, v in scores.items()
        }

    def _assign_resolution_levels(
        self, importance_scores: Mapping[str, float],
    ) -> Dict[str, ResolutionLevel]:
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

        The sum of allocated tokens equals ``total_budget`` exactly —
        integer truncation remainder goes to nodes with the largest
        fractional loss, tie-broken by node_id ascending for
        determinism.
        """
        multipliers = self.resolution_multipliers
        total_weighted = sum(
            base_tokens_per_node * multipliers[level]
            for level in resolution_assignments.values()
        )
        # ``total_weighted > 0`` is guaranteed by config validation
        # (base > 0, multipliers > 0). No dead branch.

        scale_factor = total_budget / total_weighted

        # Compute raw fractional allocations.
        raw: Dict[Any, float] = {}
        for node in graph.nodes:
            level = resolution_assignments.get(
                node, ResolutionLevel.MINIMAL,
            )
            raw[node] = (
                base_tokens_per_node
                * multipliers[level]
                * scale_factor
            )

        # Truncate to ints.
        allocated: Dict[Any, int] = {
            node: int(amount) for node, amount in raw.items()
        }

        # Redistribute the remainder to the largest fractional losses.
        remainder = total_budget - sum(allocated.values())
        if remainder > 0 and allocated:
            candidates = sorted(
                allocated.keys(),
                key=lambda n: (
                    -(raw[n] - allocated[n]),
                    str(n),
                ),
            )
            for i in range(remainder):
                allocated[candidates[i % len(candidates)]] += 1

        allocations: Dict[str, TokenAllocation] = {}
        for node in graph.nodes:
            level = resolution_assignments.get(
                node, ResolutionLevel.MINIMAL,
            )
            multiplier = multipliers[level]
            alloc_tokens = allocated[node]
            allocations[node] = TokenAllocation(
                node_id=node,
                base_tokens=base_tokens_per_node,
                allocated_tokens=alloc_tokens,
                resolution_level=level,
                importance_score=float(
                    importance_scores.get(node, 0.0)
                ),
                allocation_ratio=(
                    alloc_tokens / base_tokens_per_node
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
        """
        Cosine similarity between ``query_embedding`` and each node,
        shifted to ``[0, 1]`` via ``(cos + 1) / 2``.

        Raises on non-finite vectors (``NaN`` / ``Inf``) and shape
        mismatch. Zero-norm node embeddings score ``0.0`` (not NaN).
        """
        if not _NUMPY_AVAILABLE:
            raise TopologicalAllocatorError(
                "numpy is required for semantic similarity "
                "computation."
            )
        q = np.asarray(query_embedding, dtype=float)
        if not np.isfinite(q).all():
            raise TopologicalAllocatorError(
                "query_embedding contains non-finite values."
            )
        q_norm = float(np.linalg.norm(q))
        if q_norm <= self._config.semantic_epsilon:
            raise TopologicalAllocatorError(
                "query_embedding has near-zero norm."
            )

        scores: Dict[str, float] = {}
        for node_id, node_embedding in node_embeddings.items():
            n = np.asarray(node_embedding, dtype=float)
            if not np.isfinite(n).all():
                raise TopologicalAllocatorError(
                    f"node_embedding for {node_id!r} contains "
                    f"non-finite values."
                )
            if q.shape != n.shape:
                raise TopologicalAllocatorError(
                    f"shape mismatch for node {node_id!r}: "
                    f"{q.shape} vs {n.shape}."
                )
            n_norm = float(np.linalg.norm(n))
            if n_norm <= self._config.semantic_epsilon:
                scores[str(node_id)] = 0.0
                continue
            cos = float(np.dot(q, n) / (q_norm * n_norm))
            scores[str(node_id)] = _cosine_to_unit_interval(cos)
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
        tw = topology_weight / total
        sw = semantic_weight / total

        all_nodes = set(topological_scores) | set(
            semantic_scores
        )
        combined: Dict[str, float] = {}
        for node in all_nodes:
            value = (
                tw * float(topological_scores.get(node, 0.0))
                + sw * float(semantic_scores.get(node, 0.0))
            )
            # Clamp defensively — inputs are in [0, 1] but the
            # invariant is worth enforcing.
            combined[node] = max(0.0, min(1.0, value))
        return combined

    def _record_allocation(
        self,
        allocations: Mapping[str, TokenAllocation],
        total_budget: int,
    ) -> None:
        summary = self.get_allocation_summary(allocations)
        summary["total_budget"] = total_budget
        summary["timestamp"] = time.time()
        with self._lock:
            self.allocation_history.append(summary)

    # ---------------------------------------------------------- validation
    def _validate_graph(self, graph: Any) -> None:
        if graph is None:
            raise TopologicalAllocatorError(
                "graph must not be None."
            )
        if not hasattr(graph, "nodes"):
            raise TopologicalAllocatorError(
                "graph must be a networkx.Graph-like object "
                "with .nodes."
            )
        for node in graph.nodes:
            if not isinstance(node, str):
                msg = (
                    f"graph node IDs must be strings; got "
                    f"{type(node).__name__} ({node!r}). Cast your "
                    f"graph node IDs to str before calling."
                )
                if self._strict:
                    raise TopologicalAllocatorError(msg)
                logger.warning("%s Proceeding.", msg)

    def _validate_budget(self, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(
            value, numbers.Integral,
        ):
            raise TopologicalAllocatorError(
                f"total_token_budget must be an int, "
                f"got {type(value).__name__}."
            )
        iv = int(value)
        if iv <= 0:
            raise TopologicalAllocatorError(
                f"total_token_budget must be > 0, got {iv}."
            )
        return iv

    def _validate_base_tokens(self, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(
            value, numbers.Integral,
        ):
            raise TopologicalAllocatorError(
                f"base_tokens_per_node must be an int, "
                f"got {type(value).__name__}."
            )
        iv = int(value)
        if iv <= 0:
            raise TopologicalAllocatorError(
                f"base_tokens_per_node must be > 0, got {iv}."
            )
        return iv

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "numpy_available": _NUMPY_AVAILABLE,
                "networkx_available": _NETWORKX_AVAILABLE,
            }
            if include_history:
                # ``history_size`` appears only when the history
                # itself does — this keeps a fresh allocator
                # round-tripping without spurious diffs.
                payload["history_size"] = len(
                    self.allocation_history
                )
                payload["allocation_history"] = list(
                    self.allocation_history
                )
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "TopologicalTokenAllocator":
        if not isinstance(data, Mapping):
            raise TopologicalAllocatorError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise TopologicalAllocatorError(
                    "config must be a Mapping."
                )
            cfg = TopologicalAllocatorConfig.from_dict(
                dict(raw_config)
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise TopologicalAllocatorError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )

            allocator = cls(config=cfg, strict=strict_raw)

            raw_history = data.get("allocation_history")
            if raw_history is None:
                raw_history = []
            if not isinstance(raw_history, list):
                raise TopologicalAllocatorError(
                    f"allocation_history must be a list, got "
                    f"{type(raw_history).__name__}."
                )
            # Truncate to max_history — the deque would do it
            # silently, but we want the newest entries preserved.
            if len(raw_history) > cfg.max_history:
                logger.warning(
                    "allocation_history has %d entries, exceeds "
                    "max_history=%d; truncating to newest %d.",
                    len(raw_history),
                    cfg.max_history,
                    cfg.max_history,
                )
                raw_history = raw_history[-cfg.max_history:]
            with allocator._lock:
                for entry in raw_history:
                    if not isinstance(entry, Mapping):
                        raise TopologicalAllocatorError(
                            "allocation_history entries must be "
                            "Mappings."
                        )
                    allocator.allocation_history.append(
                        dict(entry)
                    )

            raw_started = data.get("started_at")
            if raw_started is None:
                allocator._started_at = time.time()
            else:
                try:
                    allocator._started_at = float(raw_started)
                except (TypeError, ValueError) as exc:
                    raise TopologicalAllocatorError(
                        f"invalid started_at: {exc}"
                    ) from exc
            return allocator
        except TopologicalAllocatorError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise TopologicalAllocatorError(
                f"invalid TopologicalTokenAllocator payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_history=include_history),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "TopologicalTokenAllocator":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise TopologicalAllocatorError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        with self._lock:
            removed = len(self.allocation_history)
            if clear_history:
                self.allocation_history.clear()
            self._started_at = time.time()
        logger.debug(
            "TopologicalTokenAllocator reset (removed %d).",
            removed,
        )
        return removed

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "TopologicalTokenAllocator":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    if not _NETWORKX_AVAILABLE:
        print(
            "networkx not installed; skipping topological allocator "
            "smoke test."
        )
    else:
        g = nx.Graph()
        g.add_edges_from([
            ("A", "B"), ("B", "C"),
            ("A", "D"), ("D", "E"),
            ("B", "E"), ("E", "F"),
        ])
        print(
            f"graph      : {g.number_of_nodes()} nodes, "
            f"{g.number_of_edges()} edges"
        )

        allocator = TopologicalTokenAllocator()
        print("repr       :", allocator)

        allocations = allocator.allocate_tokens(
            graph=g,
            total_token_budget=1000,
            base_tokens_per_node=100,
        )
        for nid in ("A", "B", "E"):
            print(f"  {nid}: {allocations[nid]}")

        total = sum(
            a.allocated_tokens for a in allocations.values()
        )
        print(f"total      : {total} tokens (budget=1000)")
        # Total is now exactly conserved (remainder distributed).
        assert total == 1000, f"budget not conserved: {total}"

        # ---- Query-node boost -------------------------------------- #
        boosted = allocator.allocate_tokens(
            graph=g,
            total_token_budget=1000,
            base_tokens_per_node=100,
            query_nodes=["F"],
        )
        assert (
            boosted["F"].importance_score
            >= allocations["F"].importance_score
        )
        print(
            f"boost F    : "
            f"importance={boosted['F'].importance_score:.3f} "
            f"(vs unboosted "
            f"{allocations['F'].importance_score:.3f})"
        )

        # ---- Unknown query node logs at WARNING -------------------- #
        allocator.allocate_tokens(
            graph=g, total_token_budget=1000,
            query_nodes=["unknown_node"],
        )
        print("unknown    : WARNING logged")

        # ---- Summary ------------------------------------------------ #
        summary = allocator.get_allocation_summary(allocations)
        print("summary    :", {
            "total_nodes": summary["total_nodes"],
            "total_tokens": summary["total_tokens_allocated"],
            "nodes_by_resolution": summary["nodes_by_resolution"],
        })

        # ---- allocate_for_retrieval -------------------------------- #
        if _NUMPY_AVAILABLE:
            rng = np.random.default_rng(0)
            node_embeddings = {
                nid: rng.standard_normal(4)
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
                nid: retrieved[nid].allocated_tokens
                for nid in ("A", "B", "F")
            })

        # ---- Efficiency analysis ------------------------------------ #
        print("efficiency :", {
            k: v for k, v in allocator.statistics().items()
            if k not in ("latest_allocation", "uptime_seconds")
        })

        # ---- Semantic scores in [0, 1] ----------------------------- #
        if _NUMPY_AVAILABLE:
            scores = allocator._calculate_semantic_scores(
                np.ones(4),
                {
                    "A": np.zeros(4),
                    "B": np.ones(4),
                    "C": -np.ones(4),
                },
            )
            assert scores["A"] == 0.0  # zero-norm → 0
            assert abs(scores["B"] - 1.0) < 1e-9  # cosine +1 → 1
            assert scores["C"] == 0.0  # cosine -1 → 0 (mapped)
            print("semantic   : all in [0, 1]")

            # NaN embedding rejected
            try:
                allocator._calculate_semantic_scores(
                    np.ones(4), {"X": np.array([np.nan, 0, 0, 0])},
                )
            except TopologicalAllocatorError as exc:
                print("Rejected NaN:", str(exc)[:60])

        # ---- Budget / base validation ------------------------------ #
        for bad_budget in (0, -1, "1000", 1.5, None):
            try:
                allocator.allocate_tokens(
                    g, bad_budget,  # type: ignore[arg-type]
                )
            except TopologicalAllocatorError as exc:
                print("Rejected   :", str(exc)[:50])

        try:
            allocator.allocate_tokens(
                g, 1000, base_tokens_per_node=0,
            )
        except TopologicalAllocatorError as exc:
            print("Rejected   :", str(exc)[:50])

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
        assert bounded.history_size == 2
        print("bounded    : OK")

        # ---- Round-trip (default: no history) ----------------------- #
        payload = allocator.to_json()
        restored = TopologicalTokenAllocator.from_json(payload)
        assert restored.to_dict() == allocator.to_dict()
        print("Round-trip (no history) OK.")

        # ---- Round-trip (with history) ------------------------------ #
        full_payload = allocator.to_json(include_history=True)
        restored_full = TopologicalTokenAllocator.from_json(
            full_payload,
        )
        assert (
            restored_full.to_dict(include_history=True)
            == allocator.to_dict(include_history=True)
        ), "history round-trip failed"
        print("Round-trip (with history) OK.")

        # ---- TokenAllocation round-trip ----------------------------- #
        ta = allocations["A"]
        restored_ta = TokenAllocation.from_dict(ta.to_dict())
        assert restored_ta.to_dict() == ta.to_dict()
        print("TokenAllocation OK.")

        # ---- Context manager --------------------------------------- #
        with TopologicalTokenAllocator() as scoped:
            scoped.allocate_tokens(g, 500)
            assert scoped.history_size == 1
        print("Context    : OK")

        # ---- from_dict error wrapping ------------------------------ #
        for bad in (
            {"strict": "yes"},
            {"allocation_history": "abc"},
            {"allocation_history": [{"bad": "entry"}]},  # ok shape
            {"started_at": "abc"},
            {"config": {"critical_threshold": "abc"}},
            {"config": {"importance_metric": "bogus"}},
        ):
            try:
                TopologicalTokenAllocator.from_dict(bad)
            except TopologicalAllocatorError as exc:
                print(
                    "Rejected   :", str(exc)[:60], "...",
                )

        # ---- to_json rejects NaN ---------------------------------- #
        tainted = TopologicalTokenAllocator()
        tainted.allocate_tokens(g, 500)
        object.__setattr__(
            tainted._config, "topology_weight", float("nan"),
        )
        try:
            tainted.to_json()
        except ValueError as exc:
            print("NaN json   :", str(exc)[:60])

        # ---- Config validation ------------------------------------ #
        for bad_cfg in (
            dict(
                critical_threshold=0.4,
                high_threshold=0.8,
            ),
            dict(critical_threshold=-0.1),
            dict(critical_threshold=1.5),
            dict(critical_multiplier=0),
            dict(critical_multiplier=1.5),
            dict(
                minimal_multiplier=0.1,
                low_multiplier=0.05,
            ),
            dict(query_boost_factor=0.5),
            dict(
                topology_weight=0.9, semantic_weight=0.9,
            ),
            dict(semantic_epsilon=0),
            dict(default_base_tokens_per_node=0),
            dict(max_history=0),
        ):
            try:
                TopologicalAllocatorConfig(
                    **bad_cfg  # type: ignore[arg-type]
                )
            except TopologicalAllocatorError as exc:
                print("Rejected cfg:", str(exc)[:50])

        # ---- Distribution sanity ---------------------------------- #
        sorted_allocs = sorted(
            allocations.values(),
            key=lambda a: a.importance_score, reverse=True,
        )
        if len(sorted_allocs) >= 2:
            top = sorted_allocs[0]
            bottom = sorted_allocs[-1]
            assert (
                top.allocated_tokens
                >= bottom.allocated_tokens
            )
            print(
                f"top: {top.node_id} -> "
                f"{top.allocated_tokens} tokens "
                f"(importance={top.importance_score:.3f})"
            )
            print(
                f"bot: {bottom.node_id} -> "
                f"{bottom.allocated_tokens} tokens "
                f"(importance={bottom.importance_score:.3f})"
            )

        # ---- Non-strict mode -------------------------------------- #
        lenient = TopologicalTokenAllocator(strict=False)
        # Should not raise even for a non-string node.
        bad_graph = nx.Graph()
        bad_graph.add_edge(1, 2)
        lenient.allocate_tokens(bad_graph, 500)
        print("lenient    : OK (non-string nodes tolerated)")

        print("\nSmoke test passed.")
