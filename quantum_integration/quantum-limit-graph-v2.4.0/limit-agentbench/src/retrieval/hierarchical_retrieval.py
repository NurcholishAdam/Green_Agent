# src/retrieval/hierarchical_retrieval.py

"""
Hierarchical Retrieval Module
=============================

Multi-level retrieval with coarse-to-fine filtering inspired by VimRAG's
hierarchical memory navigation.

Enhancements
------------
- ``HierarchicalRetrievalConfig`` — frozen, validated: per-level token
  budgets, top-k, pruning thresholds, max depth, bounded history.
- ``HierarchicalNode`` — frozen dataclass with validation, cached token set,
  and full serialization.
- **Fixed index-duplication bug** — re-adding a node removes the old level
  entry and old parent link.
- **Fixed infinite-loop risk** — the traversal is bounded by ``max_depth``.
- **Fixed empty-candidates exit** — the loop bails when no children remain.
- **Fixed stale children** — ``_get_children`` filters against ``self.nodes``.
- **Optional NumPy** — module imports cleanly without numpy; falls back to a
  pure-Python mean.
- **Thread safety** — ``RLock`` guards every state mutation.
- **Bounded pruning history** — ``deque(maxlen=config.max_history)``.
- **Cached tokenization** — each node's lowercased word set is computed once.
- **Full validation** of every argument; strict / non-strict modes.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` on the
  hierarchy and every dataclass.
- ``statistics()``, ``__repr__``, custom ``HierarchicalRetrievalError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional NumPy
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False
    logger.debug("numpy not importable; using pure-Python mean.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class HierarchicalRetrievalError(ValueError):
    """Raised for invalid hierarchical retrieval inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class RetrievalLevel(Enum):
    """Hierarchical retrieval levels."""

    COARSE = "coarse"  # High-level semantic clusters
    MEDIUM = "medium"  # Topic-level groups
    FINE = "fine"      # Individual nodes


# Ordered hierarchy — used for traversal and index rebuild.
_LEVEL_ORDER: Tuple[RetrievalLevel, ...] = (
    RetrievalLevel.COARSE,
    RetrievalLevel.MEDIUM,
    RetrievalLevel.FINE,
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HierarchicalRetrievalConfig:
    """Tunable parameters for :class:`HierarchicalRetrieval`."""

    # Per-level token budgets.
    coarse_budget_tokens: int = 500
    medium_budget_tokens: int = 1500
    fine_budget_tokens: int = 4096

    # How many top candidates to keep at each non-fine level.
    top_k_per_level: int = 5

    # Traversal guards.
    max_depth: int = 8
    max_results_cap: int = 10_000

    # Energy coefficient: microWh per token.
    micro_wh_per_token: float = 1e-6

    # Recommendation thresholds.
    coarse_to_medium_ratio: float = 0.5
    min_pruning_efficiency: float = 0.5

    # Bounded history of pruning efficiencies.
    max_history: int = 1000

    def __post_init__(self) -> None:
        for name in (
            "coarse_budget_tokens",
            "medium_budget_tokens",
            "fine_budget_tokens",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise HierarchicalRetrievalError(f"{name} must be > 0.")
        if not (
            self.coarse_budget_tokens
            <= self.medium_budget_tokens
            <= self.fine_budget_tokens
        ):
            raise HierarchicalRetrievalError(
                "token budgets must satisfy coarse <= medium <= fine."
            )
        if self.top_k_per_level <= 0:
            raise HierarchicalRetrievalError(
                "top_k_per_level must be > 0."
            )
        if self.max_depth <= 0:
            raise HierarchicalRetrievalError("max_depth must be > 0.")
        if self.max_results_cap <= 0:
            raise HierarchicalRetrievalError(
                "max_results_cap must be > 0."
            )
        if self.micro_wh_per_token <= 0:
            raise HierarchicalRetrievalError(
                "micro_wh_per_token must be > 0."
            )
        if not 0.0 < self.coarse_to_medium_ratio < 1.0:
            raise HierarchicalRetrievalError(
                "coarse_to_medium_ratio must be in (0, 1)."
            )
        if not 0.0 <= self.min_pruning_efficiency <= 1.0:
            raise HierarchicalRetrievalError(
                "min_pruning_efficiency must be in [0, 1]."
            )
        if self.max_history <= 0:
            raise HierarchicalRetrievalError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HierarchicalRetrievalConfig":
        if not isinstance(data, Mapping):
            raise HierarchicalRetrievalError(
                f"HierarchicalRetrievalConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            coarse_budget_tokens=int(
                data.get("coarse_budget_tokens", 500)
            ),
            medium_budget_tokens=int(
                data.get("medium_budget_tokens", 1500)
            ),
            fine_budget_tokens=int(data.get("fine_budget_tokens", 4096)),
            top_k_per_level=int(data.get("top_k_per_level", 5)),
            max_depth=int(data.get("max_depth", 8)),
            max_results_cap=int(data.get("max_results_cap", 10_000)),
            micro_wh_per_token=float(
                data.get("micro_wh_per_token", 1e-6)
            ),
            coarse_to_medium_ratio=float(
                data.get("coarse_to_medium_ratio", 0.5)
            ),
            min_pruning_efficiency=float(
                data.get("min_pruning_efficiency", 0.5)
            ),
            max_history=int(data.get("max_history", 1000)),
        )


# --------------------------------------------------------------------------- #
# Node
# --------------------------------------------------------------------------- #
@dataclass
class HierarchicalNode:
    """
    Node in the hierarchical retrieval structure.

    Kept **mutable** for backward compatibility (external code may append to
    ``children``). All numeric fields are validated in ``__post_init__``.
    """

    node_id: str
    level: RetrievalLevel
    content: Any
    children: List[str] = field(default_factory=list)
    parent: Optional[str] = None
    token_budget: int = 0
    energy_budget: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Cached token set — recomputed lazily by ``_score_nodes``.
    _content_tokens: Optional[FrozenSet[str]] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise HierarchicalRetrievalError(
                "node_id must be a non-empty string."
            )
        if not isinstance(self.level, RetrievalLevel):
            raise HierarchicalRetrievalError(
                f"level must be a RetrievalLevel, got "
                f"{type(self.level).__name__}."
            )
        if not isinstance(self.children, list):
            raise HierarchicalRetrievalError(
                "children must be a list."
            )
        if self.parent is not None and (
            not isinstance(self.parent, str) or not self.parent
        ):
            raise HierarchicalRetrievalError(
                "parent must be a non-empty string or None."
            )
        if not isinstance(self.token_budget, int) or self.token_budget < 0:
            raise HierarchicalRetrievalError(
                "token_budget must be a non-negative int."
            )
        if not isinstance(self.energy_budget, (int, float)):
            raise HierarchicalRetrievalError(
                "energy_budget must be numeric."
            )
        fe = float(self.energy_budget)
        if math.isnan(fe) or math.isinf(fe) or fe < 0:
            raise HierarchicalRetrievalError(
                f"energy_budget must be finite and >= 0, got "
                f"{self.energy_budget!r}."
            )
        if not isinstance(self.metadata, dict):
            raise HierarchicalRetrievalError("metadata must be a dict.")

    def content_tokens(self) -> FrozenSet[str]:
        """Return the cached lower-cased token set of ``content``."""
        if self._content_tokens is None:
            self._content_tokens = frozenset(
                str(self.content).lower().split()
            )
        return self._content_tokens

    def invalidate_tokens(self) -> None:
        """Force a recompute on the next ``content_tokens()`` call."""
        self._content_tokens = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "level": self.level.value,
            "content": self.content,
            "children": list(self.children),
            "parent": self.parent,
            "token_budget": self.token_budget,
            "energy_budget": self.energy_budget,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HierarchicalNode":
        if not isinstance(data, Mapping):
            raise HierarchicalRetrievalError(
                "HierarchicalNode.from_dict expects a Mapping."
            )
        return cls(
            node_id=str(data["node_id"]),
            level=RetrievalLevel(str(data["level"])),
            content=data.get("content"),
            children=list(data.get("children", [])),
            parent=data.get("parent"),
            token_budget=int(data.get("token_budget", 0)),
            energy_budget=float(data.get("energy_budget", 0.0)),
            metadata=dict(data.get("metadata", {})),
        )

    def __repr__(self) -> str:
        return (
            "HierarchicalNode("
            f"node_id={self.node_id!r}, "
            f"level={self.level.value}, "
            f"children={len(self.children)}, "
            f"parent={self.parent!r}, "
            f"token_budget={self.token_budget})"
        )


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class HierarchicalRetrieval:
    """
    Hierarchical retrieval system for efficient multi-level navigation.

    Implements a coarse-to-fine retrieval strategy:

    1. **Coarse**: identify relevant semantic clusters.
    2. **Medium**: navigate to topic groups.
    3. **Fine**: retrieve specific nodes.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        coarse_budget_tokens: int = 500,
        medium_budget_tokens: int = 1500,
        fine_budget_tokens: int = 4096,
        *,
        config: Optional[HierarchicalRetrievalConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = HierarchicalRetrievalConfig(
                coarse_budget_tokens=int(coarse_budget_tokens),
                medium_budget_tokens=int(medium_budget_tokens),
                fine_budget_tokens=int(fine_budget_tokens),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.budgets: Dict[RetrievalLevel, int] = {
            RetrievalLevel.COARSE: self._config.coarse_budget_tokens,
            RetrievalLevel.MEDIUM: self._config.medium_budget_tokens,
            RetrievalLevel.FINE: self._config.fine_budget_tokens,
        }

        self._lock = threading.RLock()
        self.nodes: Dict[str, HierarchicalNode] = {}
        self.level_index: Dict[RetrievalLevel, List[str]] = {
            level: [] for level in RetrievalLevel
        }
        self.level_access_counts: Dict[RetrievalLevel, int] = {
            level: 0 for level in RetrievalLevel
        }
        self.pruning_efficiency: Deque[float] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        logger.debug(
            "HierarchicalRetrieval initialized "
            "(budgets=%s, top_k=%d, max_depth=%d, strict=%s)",
            {k.value: v for k, v in self.budgets.items()},
            self._config.top_k_per_level,
            self._config.max_depth,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> HierarchicalRetrievalConfig:
        return self._config

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self.nodes)

    # ---------------------------------------------------------- node mutation
    def add_hierarchical_node(
        self,
        node_id: str,
        level: RetrievalLevel,
        content: Any,
        parent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> HierarchicalNode:
        """
        Add a node to the hierarchical structure.

        Re-adding an existing ``node_id`` **replaces** the previous node: its
        old level-index entry and its old parent link are removed. The
        original implementation appended to the new level without cleaning up
        the old one, leaving duplicate index entries.

        Returns the stored node (or a placeholder when inputs were rejected
        in non-strict mode).
        """
        if not isinstance(node_id, str) or not node_id:
            msg = f"node_id must be a non-empty string, got {node_id!r}."
            if self._strict:
                raise HierarchicalRetrievalError(msg)
            logger.warning("%s Skipping.", msg)
            return self._placeholder_node()

        if not isinstance(level, RetrievalLevel):
            msg = (
                f"level must be a RetrievalLevel, got "
                f"{type(level).__name__}."
            )
            if self._strict:
                raise HierarchicalRetrievalError(msg)
            logger.warning("%s Skipping.", msg)
            return self._placeholder_node()

        if parent is not None:
            if not isinstance(parent, str) or not parent:
                msg = (
                    f"parent must be a non-empty string or None, got "
                    f"{parent!r}."
                )
                if self._strict:
                    raise HierarchicalRetrievalError(msg)
                logger.warning("%s Ignoring parent.", msg)
                parent = None

        if metadata is not None and not isinstance(metadata, dict):
            msg = (
                f"metadata must be a dict or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise HierarchicalRetrievalError(msg)
            logger.warning("%s Ignoring metadata.", msg)
            metadata = None

        # Parent must exist and be one level up (if strict).
        if parent is not None:
            with self._lock:
                parent_node = self.nodes.get(parent)
            if parent_node is None:
                msg = f"parent {parent!r} not found."
                if self._strict:
                    raise HierarchicalRetrievalError(msg)
                logger.warning("%s Ignoring parent.", msg)
                parent = None
            elif not self._is_valid_parent_level(parent_node.level, level):
                msg = (
                    f"parent level {parent_node.level.value} is not a valid "
                    f"parent for {level.value}."
                )
                if self._strict:
                    raise HierarchicalRetrievalError(msg)
                logger.warning("%s Ignoring parent.", msg)
                parent = None

        node = HierarchicalNode(
            node_id=node_id,
            level=level,
            content=content,
            children=[],
            parent=parent,
            token_budget=self.budgets[level],
            energy_budget=self._estimate_energy_budget(level),
            metadata=metadata or {},
        )

        with self._lock:
            # ---- Clean up previous entry (fixes index-duplication bug) ----
            previous = self.nodes.get(node_id)
            if previous is not None:
                # Remove from the old level index.
                old_level_list = self.level_index.get(previous.level, [])
                if node_id in old_level_list:
                    old_level_list.remove(node_id)
                # Remove from the old parent's children.
                if previous.parent:
                    old_parent = self.nodes.get(previous.parent)
                    if old_parent is not None and node_id in old_parent.children:
                        old_parent.children.remove(node_id)

            # ---- Store the new node ---------------------------------------
            self.nodes[node_id] = node
            self.level_index[level].append(node_id)

            # ---- Link to the parent ----------------------------------------
            if parent is not None:
                parent_node = self.nodes.get(parent)
                if parent_node is not None and node_id not in parent_node.children:
                    parent_node.children.append(node_id)

        logger.debug(
            "Added hierarchical node %s at %s (parent=%s, budget=%d).",
            node_id, level.value, parent, node.token_budget,
        )
        return node

    @staticmethod
    def _is_valid_parent_level(
        parent_level: RetrievalLevel, child_level: RetrievalLevel
    ) -> bool:
        """Return True when ``parent_level`` strictly precedes ``child_level``."""
        try:
            return _LEVEL_ORDER.index(parent_level) < _LEVEL_ORDER.index(child_level)
        except ValueError:  # pragma: no cover — Enum is fixed
            return False

    @staticmethod
    def _placeholder_node() -> HierarchicalNode:
        return HierarchicalNode(
            node_id="__invalid__",
            level=RetrievalLevel.FINE,
            content=None,
            token_budget=0,
            energy_budget=0.0,
        )

    # ---------------------------------------------------------- retrieval
    def hierarchical_retrieve(
        self,
        query: str,
        start_level: RetrievalLevel = RetrievalLevel.COARSE,
        max_results: int = 10,
    ) -> Tuple[List[HierarchicalNode], Dict[str, Any]]:
        """
        Perform a coarse-to-fine retrieval.

        Parameters
        ----------
        query : str
            Retrieval query.
        start_level : RetrievalLevel, default COARSE
            Starting level of the traversal.
        max_results : int, default 10
            Maximum results returned at the fine level.

        Returns
        -------
        (nodes, stats)
            ``nodes`` is the list of retrieved :class:`HierarchicalNode`
            instances; ``stats`` includes traversal counters and resource
            usage.
        """
        # ---- Validate ------------------------------------------------
        if not isinstance(query, str) or not query:
            raise HierarchicalRetrievalError(
                "query must be a non-empty string."
            )
        if not isinstance(start_level, RetrievalLevel):
            raise HierarchicalRetrievalError(
                f"start_level must be a RetrievalLevel, got "
                f"{type(start_level).__name__}."
            )
        if not isinstance(max_results, int) or max_results <= 0:
            raise HierarchicalRetrievalError(
                "max_results must be a positive int."
            )
        if max_results > self._config.max_results_cap:
            max_results = self._config.max_results_cap

        stats: Dict[str, Any] = {
            "levels_traversed": [],
            "nodes_evaluated": 0,
            "nodes_pruned": 0,
            "total_tokens": 0,
            "total_energy": 0.0,
            "exhausted": False,
            "depth_reached": 0,
        }

        # ---- Traverse coarse-to-fine ---------------------------------
        with self._lock:
            current_level = start_level
            candidate_ids = list(self.level_index.get(current_level, []))

        depth = 0
        while current_level != RetrievalLevel.FINE:
            depth += 1
            if depth > self._config.max_depth:
                logger.warning(
                    "Traversal depth %d exceeded max_depth %d; "
                    "forcing FINE level.",
                    depth, self._config.max_depth,
                )
                current_level = RetrievalLevel.FINE
                break

            # ---- Empty candidates → bail early ------------------------
            if not candidate_ids:
                stats["exhausted"] = True
                logger.debug(
                    "No candidates at %s; forcing FINE level.",
                    current_level.value,
                )
                current_level = RetrievalLevel.FINE
                break

            scored = self._score_nodes(query, candidate_ids)
            top_k = min(self._config.top_k_per_level, len(scored))
            selected = scored[:top_k]

            stats["levels_traversed"].append(current_level.value)
            stats["nodes_evaluated"] += len(candidate_ids)
            stats["nodes_pruned"] += len(candidate_ids) - len(selected)
            stats["depth_reached"] = depth

            with self._lock:
                self.level_access_counts[current_level] += 1

            # Move to the next level.
            next_level = self._next_level(current_level)
            children = self._get_children(selected)
            current_level = next_level
            candidate_ids = children

        # ---- Final fine-level retrieval ------------------------------
        if current_level == RetrievalLevel.FINE:
            # Avoid double counting the FINE level if the loop already
            # terminated at FINE without iterating.
            if RetrievalLevel.FINE.value not in stats["levels_traversed"]:
                stats["levels_traversed"].append(RetrievalLevel.FINE.value)
            if not candidate_ids:
                # Fall back to every FINE node.
                with self._lock:
                    candidate_ids = list(
                        self.level_index.get(RetrievalLevel.FINE, [])
                    )

            final_scored = self._score_nodes(query, candidate_ids)
            final_results = final_scored[:max_results]
            stats["nodes_evaluated"] += len(candidate_ids)
            stats["nodes_pruned"] += max(0, len(candidate_ids) - len(final_results))

        else:  # pragma: no cover — defensive
            final_results: List[Tuple[str, float]] = []

        # ---- Assemble result nodes ----------------------------------
        with self._lock:
            result_nodes = [
                self.nodes[nid]
                for nid, _ in final_results
                if nid in self.nodes
            ]

        stats["total_tokens"] = sum(n.token_budget for n in result_nodes)
        stats["total_energy"] = sum(n.energy_budget for n in result_nodes)

        pruning_eff = (
            stats["nodes_pruned"] / stats["nodes_evaluated"]
            if stats["nodes_evaluated"] > 0 else 0.0
        )
        with self._lock:
            self.pruning_efficiency.append(pruning_eff)
        stats["pruning_efficiency"] = pruning_eff
        stats["results_returned"] = len(result_nodes)

        logger.debug(
            "Retrieved %d node(s) for query=%r (pruning=%.3f, "
            "tokens=%d, energy=%.6f).",
            len(result_nodes), query, pruning_eff,
            stats["total_tokens"], stats["total_energy"],
        )
        return result_nodes, stats

    # ---------------------------------------------------------- statistics
    def get_hierarchical_stats(self) -> Dict[str, Any]:
        """Return aggregate statistics about the hierarchy."""
        with self._lock:
            node_count = len(self.nodes)
            per_level = {
                level.value: len(ids)
                for level, ids in self.level_index.items()
            }
            access = {
                level.value: count
                for level, count in self.level_access_counts.items()
            }
            pruning = list(self.pruning_efficiency)
            budgets = {level.value: b for level, b in self.budgets.items()}

        avg_pruning = (
            (sum(pruning) / len(pruning))
            if pruning else 0.0
        )

        return {
            "total_nodes": node_count,
            "nodes_per_level": per_level,
            "level_access_counts": access,
            "avg_pruning_efficiency": avg_pruning,
            "token_budgets": budgets,
            # Additive:
            "pruning_samples": len(pruning),
            "numpy_available": _NUMPY_AVAILABLE,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_hierarchical_stats`."""
        return self.get_hierarchical_stats()

    # ---------------------------------------------------------- optimization
    def optimize_hierarchy(self) -> Dict[str, Any]:
        """
        Analyze the hierarchy and return optimization recommendations.

        Notes
        -----
        The recommendations are *heuristics* based on density balance and
        pruning efficiency. They do not modify the hierarchy.
        """
        stats = self.get_hierarchical_stats()
        recommendations: List[Dict[str, str]] = []

        level_counts = stats["nodes_per_level"]
        coarse = level_counts.get("coarse", 0)
        medium = level_counts.get("medium", 0)

        # Coarse level too dense relative to medium.
        if medium > 0 and coarse > medium * self._config.coarse_to_medium_ratio:
            recommendations.append({
                "type": "rebalance",
                "action": "increase_medium_level_nodes",
                "reason": (
                    f"Coarse level is too dense relative to medium "
                    f"(coarse={coarse}, medium={medium})."
                ),
            })

        # Low pruning efficiency.
        if stats["avg_pruning_efficiency"] < self._config.min_pruning_efficiency:
            recommendations.append({
                "type": "pruning",
                "action": "improve_coarse_level_discrimination",
                "reason": (
                    f"Average pruning efficiency "
                    f"{stats['avg_pruning_efficiency']:.3f} is below "
                    f"threshold {self._config.min_pruning_efficiency:.3f}."
                ),
            })

        # Empty hierarchy.
        if stats["total_nodes"] == 0:
            recommendations.append({
                "type": "bootstrap",
                "action": "add_nodes",
                "reason": "Hierarchy is empty.",
            })

        return {
            "recommendations": recommendations,
            "current_stats": stats,
        }

    # ---------------------------------------------------------- traversal helpers
    def _score_nodes(
        self, query: str, node_ids: Sequence[str]
    ) -> List[Tuple[str, float]]:
        """
        Score and rank nodes by query overlap.

        Uses each node's cached token set — the original re-tokenized every
        node's content on every call.
        """
        query_words = frozenset(query.lower().split())
        q_size = max(len(query_words), 1)

        with self._lock:
            nodes = {
                nid: self.nodes[nid]
                for nid in node_ids
                if nid in self.nodes
            }

        scored: List[Tuple[str, float]] = []
        for nid, node in nodes.items():
            content_tokens = node.content_tokens()
            overlap = len(query_words & content_tokens)
            score = overlap / q_size
            scored.append((nid, score))

        # Deterministic tie-break: alphabetical node_id.
        scored.sort(key=lambda t: (-t[1], t[0]))
        return scored

    def _next_level(self, current: RetrievalLevel) -> RetrievalLevel:
        """Return the next level in the hierarchy (FINE is terminal)."""
        try:
            idx = _LEVEL_ORDER.index(current)
        except ValueError:  # pragma: no cover — Enum is fixed
            return RetrievalLevel.FINE
        if idx + 1 < len(_LEVEL_ORDER):
            return _LEVEL_ORDER[idx + 1]
        return RetrievalLevel.FINE

    def _get_children(
        self, parent_nodes: Sequence[Tuple[str, float]]
    ) -> List[str]:
        """
        Return the children of the top-k parent nodes.

        Filters against the live ``self.nodes`` map so stale child IDs do not
        pollute later scoring. Deduplicates the result to keep the traversal
        bounded.
        """
        with self._lock:
            children: List[str] = []
            seen: set = set()
            for nid, _ in parent_nodes:
                node = self.nodes.get(nid)
                if node is None:
                    continue
                for child in node.children:
                    if child in seen or child not in self.nodes:
                        continue
                    seen.add(child)
                    children.append(child)
        return children

    def _estimate_energy_budget(self, level: RetrievalLevel) -> float:
        """Return the estimated energy budget (Wh) for ``level``."""
        token_budget = self.budgets[level]
        return token_budget * self._config.micro_wh_per_token

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_pruning_history: bool = True) -> int:
        """Clear counters; optionally clear the pruning history."""
        with self._lock:
            for level in RetrievalLevel:
                self.level_access_counts[level] = 0
            removed = len(self.pruning_efficiency)
            if clear_pruning_history:
                self.pruning_efficiency.clear()
            self._started_at = time.time()
        logger.debug("HierarchicalRetrieval reset (removed %d samples).", removed)
        return removed

    def clear_hierarchy(self) -> int:
        """Remove every node. Returns the number of nodes removed."""
        with self._lock:
            removed = len(self.nodes)
            self.nodes.clear()
            for level in RetrievalLevel:
                self.level_index[level].clear()
        logger.debug("Cleared %d hierarchical node(s).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_pruning: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "nodes": [n.to_dict() for n in self.nodes.values()],
                "level_index": {
                    level.value: list(ids)
                    for level, ids in self.level_index.items()
                },
                "level_access_counts": {
                    level.value: count
                    for level, count in self.level_access_counts.items()
                },
                "started_at": self._started_at,
            }
            if include_pruning:
                payload["pruning_efficiency"] = list(self.pruning_efficiency)
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "HierarchicalRetrieval":
        if not isinstance(data, Mapping):
            raise HierarchicalRetrievalError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = HierarchicalRetrievalConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        retrieval = cls(config=cfg, strict=bool(data.get("strict", True)))

        with retrieval._lock:
            for entry in data.get("nodes", []):
                node = HierarchicalNode.from_dict(entry)
                retrieval.nodes[node.node_id] = node
            for level_value, ids in (data.get("level_index") or {}).items():
                try:
                    level = RetrievalLevel(str(level_value))
                except ValueError:
                    continue
                retrieval.level_index[level] = [str(i) for i in ids]
            for level_value, count in (
                data.get("level_access_counts") or {}
            ).items():
                try:
                    level = RetrievalLevel(str(level_value))
                except ValueError:
                    continue
                retrieval.level_access_counts[level] = int(count)
            for v in data.get("pruning_efficiency", []):
                retrieval.pruning_efficiency.append(float(v))
            retrieval._started_at = float(
                data.get("started_at", time.time())
            )
        return retrieval

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "HierarchicalRetrieval":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise HierarchicalRetrievalError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "HierarchicalRetrieval":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "HierarchicalRetrieval scope exited with %s.",
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
            return node_id in self.nodes

    def __repr__(self) -> str:
        with self._lock:
            return (
                "HierarchicalRetrieval("
                f"nodes={len(self.nodes)}, "
                f"per_level="
                f"{{c:{len(self.level_index[RetrievalLevel.COARSE])}, "
                f"m:{len(self.level_index[RetrievalLevel.MEDIUM])}, "
                f"f:{len(self.level_index[RetrievalLevel.FINE])}}}, "
                f"top_k={self._config.top_k_per_level}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "HierarchicalNode",
    "HierarchicalRetrieval",
    "HierarchicalRetrievalConfig",
    "HierarchicalRetrievalError",
    "RetrievalLevel",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.hierarchical_retrieval
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Build a 3-level hierarchy -------------------------------- #
    hr = HierarchicalRetrieval()
    print("repr       :", hr)

    # Coarse clusters.
    hr.add_hierarchical_node("c_energy", RetrievalLevel.COARSE,
                             "energy carbon sustainability green")
    hr.add_hierarchical_node("c_vision", RetrievalLevel.COARSE,
                             "vision image detection classification")

    # Medium topics.
    hr.add_hierarchical_node("m_renewable", RetrievalLevel.MEDIUM,
                             "renewable solar wind hydro energy",
                             parent="c_energy")
    hr.add_hierarchical_node("m_emissions", RetrievalLevel.MEDIUM,
                             "emissions carbon footprint offsets",
                             parent="c_energy")
    hr.add_hierarchical_node("m_cnn", RetrievalLevel.MEDIUM,
                             "CNN ResNet convolution neural network",
                             parent="c_vision")

    # Fine nodes.
    hr.add_hierarchical_node("f_solar", RetrievalLevel.FINE,
                             "solar panel efficiency has improved",
                             parent="m_renewable")
    hr.add_hierarchical_node("f_wind", RetrievalLevel.FINE,
                             "wind turbine energy generation",
                             parent="m_renewable")
    hr.add_hierarchical_node("f_carbon", RetrievalLevel.FINE,
                             "carbon offsets neutralize emissions",
                             parent="m_emissions")
    hr.add_hierarchical_node("f_cnn", RetrievalLevel.FINE,
                             "ResNet achieves high image accuracy",
                             parent="m_cnn")

    print("node count :", len(hr))
    assert "c_energy" in hr and "f_solar" in hr

    # ---- Retrieve: energy-oriented query -------------------------- #
    nodes, stats = hr.hierarchical_retrieve(
        "renewable solar energy", max_results=3,
    )
    print("query 1 nodes:", [n.node_id for n in nodes])
    print("query 1 stats:", {k: stats[k] for k in (
        "levels_traversed", "nodes_evaluated", "nodes_pruned",
        "results_returned", "pruning_efficiency",
    )})
    assert any(n.node_id == "f_solar" for n in nodes), (
        "energy query should reach solar node"
    )

    # ---- Retrieve: vision-oriented query -------------------------- #
    nodes2, stats2 = hr.hierarchical_retrieve("image classification cnn")
    print("query 2 nodes:", [n.node_id for n in nodes2])

    # ---- Bug fix: re-adding a node moves it correctly ------------- #
    hr.add_hierarchical_node(
        "c_vision", RetrievalLevel.COARSE,
        "vision image detection (updated)",
        # note: no parent — this is a COARSE node
    )
    # The old index entry must be gone; the new one is present exactly once.
    assert hr.level_index[RetrievalLevel.COARSE].count("c_vision") == 1
    print("re-add     : OK (no duplicate index entry)")

    # ---- Bug fix: re-parenting cleans up the old parent ----------- #
    hr.add_hierarchical_node(
        "f_carbon", RetrievalLevel.FINE,
        "carbon offsets neutralize emissions",
        parent="m_renewable",  # moved from m_emissions
    )
    assert "f_carbon" not in hr.nodes["m_emissions"].children
    assert "f_carbon" in hr.nodes["m_renewable"].children
    print("re-parent  : OK (old parent cleaned up)")

    # ---- Bug fix: stale child IDs are filtered -------------------- #
    # Simulate external corruption: put a ghost child on a node.
    hr.nodes["m_renewable"].children.append("ghost_node")
    children = hr._get_children([("m_renewable", 1.0)])
    assert "ghost_node" not in children
    print("stale child: OK (filtered)")

    # ---- Bug fix: empty hierarchy exits cleanly ------------------- #
    empty = HierarchicalRetrieval()
    empty_nodes, empty_stats = empty.hierarchical_retrieve("anything")
    assert empty_nodes == []
    assert empty_stats.get("exhausted") is True
    print("empty      : OK (exhausted flag)")

    # ---- Bug fix: top_k_per_level is configurable ----------------- #
    hr_small = HierarchicalRetrieval(
        config=HierarchicalRetrievalConfig(
            top_k_per_level=1, max_depth=4,
        )
    )
    for i in range(3):
        hr_small.add_hierarchical_node(
            f"c{i}", RetrievalLevel.COARSE, f"cluster {i}",
        )
    for i in range(3):
        hr_small.add_hierarchical_node(
            f"m{i}", RetrievalLevel.MEDIUM, f"topic {i}", parent=f"c{i}",
        )
        hr_small.add_hierarchical_node(
            f"f{i}", RetrievalLevel.FINE, f"leaf {i}", parent=f"m{i}",
        )
    _, small_stats = hr_small.hierarchical_retrieve("cluster")
    print("top_k=1    : traversed", small_stats["levels_traversed"])
    # Coarse level prunes 2 of 3; medium prunes 0 of 1 (only 1 child).

    # ---- Statistics ------------------------------------------------ #
    print("stats      :", {
        k: v for k, v in hr.get_hierarchical_stats().items()
        if k != "uptime_seconds"
    })

    # ---- Optimization recommendations ----------------------------- #
    recs = hr.optimize_hierarchy()
    print("recs       :", [r["action"] for r in recs["recommendations"]])

    # ---- Serialization round-trip --------------------------------- #
    payload = hr.to_json()
    restored = HierarchicalRetrieval.from_json(payload)
    assert restored.to_dict() == hr.to_dict()
    print("Round-trip OK.")

    # ---- Context manager ------------------------------------------ #
    with HierarchicalRetrieval() as scoped:
        scoped.add_hierarchical_node(
            "only", RetrievalLevel.FINE, "solo node",
        )
        nodes, _ = scoped.hierarchical_retrieve("solo")
        assert any(n.node_id == "only" for n in nodes)
    print("Context    : OK")

    # ---- Validation failures -------------------------------------- #
    for bad_cfg in (
        dict(coarse_budget_tokens=0),
        dict(coarse_budget_tokens=2000, medium_budget_tokens=1000),
        dict(medium_budget_tokens=6000, fine_budget_tokens=4096),
        dict(top_k_per_level=0),
        dict(max_depth=0),
        dict(max_results_cap=0),
        dict(micro_wh_per_token=0),
        dict(coarse_to_medium_ratio=0.0),
        dict(min_pruning_efficiency=1.5),
        dict(max_history=0),
    ):
        try:
            HierarchicalRetrievalConfig(**bad_cfg)  # type: ignore[arg-type]
        except HierarchicalRetrievalError as exc:
            print("Rejected cfg:", exc)

    strict = HierarchicalRetrieval(strict=True)
    for bad_call in (
        lambda: strict.add_hierarchical_node("", RetrievalLevel.FINE, "x"),
        lambda: strict.add_hierarchical_node("a", "not-a-level", "x"),
        lambda: strict.add_hierarchical_node("a", RetrievalLevel.FINE, "x",
                                             parent="missing"),
        lambda: strict.add_hierarchical_node("a", RetrievalLevel.FINE, "x",
                                             metadata="not-a-dict"),
        lambda: strict.hierarchical_retrieve(""),
        lambda: strict.hierarchical_retrieve("q", start_level="bogus"),
        lambda: strict.hierarchical_retrieve("q", max_results=0),
        lambda: strict.hierarchical_retrieve("q", max_results=-5),
    ):
        try:
            bad_call()
        except HierarchicalRetrievalError as exc:
            print("Rejected   :", exc)

    # ---- Parent level mismatch (strict) --------------------------- #
    try:
        # A FINE node cannot parent another FINE node.
        strict.add_hierarchical_node("p", RetrievalLevel.FINE, "parent")
        strict.add_hierarchical_node("q", RetrievalLevel.FINE, "child",
                                     parent="p")
    except HierarchicalRetrievalError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict ignores invalid parents ----------------------- #
    lenient = HierarchicalRetrieval(strict=False)
    lenient.add_hierarchical_node("a", RetrievalLevel.COARSE, "x",
                                  parent="missing")
    assert lenient.nodes["a"].parent is None
    print("lenient    : OK")

    print("\nSmoke test passed.")
