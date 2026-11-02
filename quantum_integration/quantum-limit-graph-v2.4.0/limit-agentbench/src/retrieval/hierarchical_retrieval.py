# src/retrieval/hierarchical_retrieval.py

"""
Hierarchical Retrieval Module
=============================

Multi-level retrieval with coarse-to-fine filtering inspired by VimRAG's
hierarchical memory navigation.

Design
------
- Frozen, validated ``HierarchicalRetrievalConfig``.
- ``HierarchicalNode`` — mutable (for BC) but with token-cache
  auto-invalidation on ``content`` reassignment.
- **Re-add replaces cleanly** — the previous node's level-index entry
  and parent-child link are removed; no duplicate index entries.
- **Traversal is bounded** by ``config.max_depth`` (interpreted as the
  maximum number of level *transitions*; a full COARSE→MEDIUM→FINE
  traversal needs ``max_depth >= 2``).
- **`exhausted` flag is set** whenever the traversal has no candidates
  to work with, including a FINE-start on an empty hierarchy.
- **`add_hierarchical_node` returns ``None``** on non-strict rejection.
  No ``"__invalid__"`` sentinel; state is not mutated.
- **`remove_node(node_id)`** recursively removes the node, its
  descendants, its index entry, and its parent-child link.
- **Serialization round-trip is lossless** — ``from_dict`` validates
  every nested structure and reconstructs ``level_index`` from
  ``nodes`` when the payload omits it. ``to_json`` uses
  ``allow_nan=False``.

Version: 2.0.0
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
from types import TracebackType
from typing import (
    Any,
    Deque,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "HierarchicalNode",
    "HierarchicalRetrieval",
    "HierarchicalRetrievalConfig",
    "HierarchicalRetrievalError",
    "RetrievalLevel",
]

VERSION = "2.0.0"


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

    COARSE = "coarse"
    MEDIUM = "medium"
    FINE = "fine"


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
    """
    Tunable parameters for :class:`HierarchicalRetrieval`.

    Note on ``max_depth``
    ---------------------
    ``max_depth`` is the maximum number of level **transitions** the
    traversal may make before being forced to FINE. A full
    COARSE → MEDIUM → FINE traversal requires ``max_depth >= 2``.
    ``max_depth=1`` performs a single transition (typically
    COARSE → MEDIUM) then drops to FINE. ``max_depth=0`` disables
    level transitions entirely.
    """

    coarse_budget_tokens: int = 500
    medium_budget_tokens: int = 1500
    fine_budget_tokens: int = 4096

    top_k_per_level: int = 5

    max_depth: int = 8
    max_results_cap: int = 10_000

    micro_wh_per_token: float = 1e-6

    coarse_to_medium_ratio: float = 0.5
    min_pruning_efficiency: float = 0.5

    max_history: int = 1000

    def __post_init__(self) -> None:
        for name in (
            "coarse_budget_tokens",
            "medium_budget_tokens",
            "fine_budget_tokens",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise HierarchicalRetrievalError(
                    f"{name} must be > 0."
                )
        if not (
            self.coarse_budget_tokens
            <= self.medium_budget_tokens
            <= self.fine_budget_tokens
        ):
            raise HierarchicalRetrievalError(
                "token budgets must satisfy coarse <= medium <= fine."
            )
        for name in (
            "top_k_per_level",
            "max_depth",
            "max_results_cap",
            "max_history",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise HierarchicalRetrievalError(
                    f"{name} must be > 0."
                )
        if (
            isinstance(self.micro_wh_per_token, bool)
            or not isinstance(
                self.micro_wh_per_token, (int, float)
            )
            or not math.isfinite(float(self.micro_wh_per_token))
            or self.micro_wh_per_token <= 0
        ):
            raise HierarchicalRetrievalError(
                "micro_wh_per_token must be finite and > 0."
            )
        if not 0.0 < self.coarse_to_medium_ratio < 1.0:
            raise HierarchicalRetrievalError(
                "coarse_to_medium_ratio must be in (0, 1)."
            )
        if not 0.0 <= self.min_pruning_efficiency <= 1.0:
            raise HierarchicalRetrievalError(
                "min_pruning_efficiency must be in [0, 1]."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HierarchicalRetrievalConfig":
        if not isinstance(data, Mapping):
            raise HierarchicalRetrievalError(
                f"HierarchicalRetrievalConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            return cls(
                coarse_budget_tokens=int(
                    data.get("coarse_budget_tokens", 500)
                ),
                medium_budget_tokens=int(
                    data.get("medium_budget_tokens", 1500)
                ),
                fine_budget_tokens=int(
                    data.get("fine_budget_tokens", 4096)
                ),
                top_k_per_level=int(
                    data.get("top_k_per_level", 5)
                ),
                max_depth=int(data.get("max_depth", 8)),
                max_results_cap=int(
                    data.get("max_results_cap", 10_000)
                ),
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
        except (TypeError, ValueError) as exc:
            if isinstance(exc, HierarchicalRetrievalError):
                raise
            raise HierarchicalRetrievalError(
                f"invalid HierarchicalRetrievalConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Node
# --------------------------------------------------------------------------- #
@dataclass
class HierarchicalNode:
    """
    Node in the hierarchical retrieval structure.

    Kept **mutable** for backward compatibility (external code may append
    to ``children``). The ``content_tokens`` cache auto-invalidates when
    ``content`` is reassigned; in-place mutation of a mutable ``content``
    requires an explicit :meth:`invalidate_tokens` call.
    """

    node_id: str
    level: RetrievalLevel
    content: Any
    children: List[str] = field(default_factory=list)
    parent: Optional[str] = None
    token_budget: int = 0
    energy_budget: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    _content_tokens: Optional[FrozenSet[str]] = field(
        default=None, repr=False, compare=False
    )

    def __setattr__(self, name: str, value: Any) -> None:
        # Invalidate token cache when ``content`` is reassigned.
        if name == "content" and hasattr(self, "_content_tokens"):
            object.__setattr__(self, "_content_tokens", None)
        object.__setattr__(self, name, value)

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
        if (
            isinstance(self.token_budget, bool)
            or not isinstance(self.token_budget, int)
            or self.token_budget < 0
        ):
            raise HierarchicalRetrievalError(
                "token_budget must be a non-negative int."
            )
        if (
            isinstance(self.energy_budget, bool)
            or not isinstance(self.energy_budget, (int, float))
        ):
            raise HierarchicalRetrievalError(
                "energy_budget must be numeric."
            )
        fe = float(self.energy_budget)
        if not math.isfinite(fe) or fe < 0:
            raise HierarchicalRetrievalError(
                f"energy_budget must be finite and >= 0, got "
                f"{self.energy_budget!r}."
            )
        if not isinstance(self.metadata, dict):
            raise HierarchicalRetrievalError(
                "metadata must be a dict."
            )

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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HierarchicalNode":
        if not isinstance(data, Mapping):
            raise HierarchicalRetrievalError(
                "HierarchicalNode.from_dict expects a Mapping."
            )
        if "node_id" not in data or "level" not in data:
            raise HierarchicalRetrievalError(
                "HierarchicalNode requires 'node_id' and 'level'."
            )
        try:
            return cls(
                node_id=str(data["node_id"]),
                level=RetrievalLevel(str(data["level"])),
                content=data.get("content"),
                children=list(data.get("children") or []),
                parent=data.get("parent"),
                token_budget=int(
                    data.get("token_budget", 0)
                ),
                energy_budget=float(
                    data.get("energy_budget", 0.0)
                ),
                metadata=dict(data.get("metadata") or {}),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, HierarchicalRetrievalError):
                raise
            raise HierarchicalRetrievalError(
                f"invalid HierarchicalNode payload: {exc}"
            ) from exc

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

    Thread-safe, serializable, bounded in memory.
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
    ) -> Optional[HierarchicalNode]:
        """
        Add a node to the hierarchical structure.

        Re-adding an existing ``node_id`` **replaces** the previous node:
        its old level-index entry and parent-child link are removed.

        Returns
        -------
        Optional[HierarchicalNode]
            The stored node, or ``None`` in non-strict mode when the
            input was rejected (nothing is mutated in that case).
        """
        if not isinstance(node_id, str) or not node_id:
            msg = (
                f"node_id must be a non-empty string, got "
                f"{node_id!r}."
            )
            if self._strict:
                raise HierarchicalRetrievalError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        if not isinstance(level, RetrievalLevel):
            msg = (
                f"level must be a RetrievalLevel, got "
                f"{type(level).__name__}."
            )
            if self._strict:
                raise HierarchicalRetrievalError(msg)
            logger.warning("%s Skipping.", msg)
            return None

        if parent is not None:
            if not isinstance(parent, str) or not parent:
                msg = (
                    f"parent must be a non-empty string or None, "
                    f"got {parent!r}."
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

        if parent is not None:
            with self._lock:
                parent_node = self.nodes.get(parent)
            if parent_node is None:
                msg = f"parent {parent!r} not found."
                if self._strict:
                    raise HierarchicalRetrievalError(msg)
                logger.warning("%s Ignoring parent.", msg)
                parent = None
            elif not self._is_valid_parent_level(
                parent_node.level, level
            ):
                msg = (
                    f"parent level {parent_node.level.value} is "
                    f"not a valid parent for {level.value}."
                )
                if self._strict:
                    raise HierarchicalRetrievalError(msg)
                logger.warning("%s Ignoring parent.", msg)
                parent = None

        try:
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
        except HierarchicalRetrievalError as exc:
            if self._strict:
                raise
            logger.warning(
                "Rejecting invalid node %r: %s", node_id, exc,
            )
            return None

        with self._lock:
            previous = self.nodes.get(node_id)
            if previous is not None:
                old_level_list = self.level_index.get(
                    previous.level, []
                )
                if node_id in old_level_list:
                    old_level_list.remove(node_id)
                if previous.parent:
                    old_parent = self.nodes.get(previous.parent)
                    if (
                        old_parent is not None
                        and node_id in old_parent.children
                    ):
                        old_parent.children.remove(node_id)

            self.nodes[node_id] = node
            self.level_index[level].append(node_id)

            if parent is not None:
                parent_node = self.nodes.get(parent)
                if (
                    parent_node is not None
                    and node_id not in parent_node.children
                ):
                    parent_node.children.append(node_id)

        logger.debug(
            "Added hierarchical node %s at %s (parent=%s, "
            "budget=%d).",
            node_id, level.value, parent, node.token_budget,
        )
        return node

    def remove_node(self, node_id: str) -> bool:
        """
        Remove ``node_id`` and (recursively) all its descendants.

        Cleans up:
        - ``nodes[node_id]``
        - ``level_index[node.level]`` entry
        - the old parent's ``children`` list

        Returns ``True`` if the node existed.
        """
        if not isinstance(node_id, str) or not node_id:
            raise HierarchicalRetrievalError(
                f"node_id must be a non-empty string, got "
                f"{node_id!r}."
            )
        with self._lock:
            node = self.nodes.get(node_id)
            if node is None:
                return False

            # Recursively remove children first — RLock re-enters safely.
            for child_id in list(node.children):
                self.remove_node(child_id)

            level_list = self.level_index.get(node.level, [])
            if node_id in level_list:
                level_list.remove(node_id)

            if node.parent is not None:
                parent = self.nodes.get(node.parent)
                if (
                    parent is not None
                    and node_id in parent.children
                ):
                    parent.children.remove(node_id)

            del self.nodes[node_id]
        logger.debug(
            "Removed node %s and its descendants.", node_id,
        )
        return True

    @staticmethod
    def _is_valid_parent_level(
        parent_level: RetrievalLevel,
        child_level: RetrievalLevel,
    ) -> bool:
        try:
            return _LEVEL_ORDER.index(
                parent_level
            ) < _LEVEL_ORDER.index(child_level)
        except ValueError:  # pragma: no cover — Enum is fixed
            return False

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
            Maximum results returned at the fine level. Silently capped
            to ``config.max_results_cap``; the truncation is signalled
            in ``stats["max_results_capped"]``.

        Returns
        -------
        (nodes, stats)
        """
        if not isinstance(query, str) or not query:
            raise HierarchicalRetrievalError(
                "query must be a non-empty string."
            )
        if not isinstance(start_level, RetrievalLevel):
            raise HierarchicalRetrievalError(
                f"start_level must be a RetrievalLevel, got "
                f"{type(start_level).__name__}."
            )
        if (
            isinstance(max_results, bool)
            or not isinstance(max_results, int)
            or max_results <= 0
        ):
            raise HierarchicalRetrievalError(
                "max_results must be a positive int."
            )

        max_results_capped = False
        if max_results > self._config.max_results_cap:
            logger.info(
                "max_results=%d exceeded cap %d; capping.",
                max_results, self._config.max_results_cap,
            )
            max_results = self._config.max_results_cap
            max_results_capped = True

        stats: Dict[str, Any] = {
            "levels_traversed": [],
            "nodes_evaluated": 0,
            "nodes_pruned": 0,
            "total_tokens": 0,
            "total_energy": 0.0,
            "exhausted": False,
            "depth_reached": 0,
            "max_results_capped": max_results_capped,
        }

        with self._lock:
            current_level = start_level
            candidate_ids = list(
                self.level_index.get(current_level, [])
            )

        # Starting directly at FINE with no FINE nodes counts as exhausted.
        if (
            start_level == RetrievalLevel.FINE
            and not candidate_ids
        ):
            stats["exhausted"] = True

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

            if not candidate_ids:
                stats["exhausted"] = True
                logger.debug(
                    "No candidates at %s; forcing FINE level.",
                    current_level.value,
                )
                current_level = RetrievalLevel.FINE
                break

            scored = self._score_nodes(query, candidate_ids)
            top_k = min(
                self._config.top_k_per_level, len(scored),
            )
            selected = scored[:top_k]

            stats["levels_traversed"].append(
                current_level.value
            )
            stats["nodes_evaluated"] += len(candidate_ids)
            stats["nodes_pruned"] += (
                len(candidate_ids) - len(selected)
            )
            stats["depth_reached"] = depth

            with self._lock:
                self.level_access_counts[current_level] += 1

            next_level = self._next_level(current_level)
            children = self._get_children(selected)
            current_level = next_level
            candidate_ids = children

        if current_level == RetrievalLevel.FINE:
            if (
                RetrievalLevel.FINE.value
                not in stats["levels_traversed"]
            ):
                stats["levels_traversed"].append(
                    RetrievalLevel.FINE.value
                )
            if not candidate_ids:
                with self._lock:
                    candidate_ids = list(
                        self.level_index.get(
                            RetrievalLevel.FINE, []
                        )
                    )
                if not candidate_ids:
                    stats["exhausted"] = True

            final_scored = self._score_nodes(
                query, candidate_ids,
            )
            final_results = final_scored[:max_results]
            stats["nodes_evaluated"] += len(candidate_ids)
            stats["nodes_pruned"] += max(
                0, len(candidate_ids) - len(final_results)
            )
        else:  # pragma: no cover — defensive
            final_results: List[Tuple[str, float]] = []

        with self._lock:
            result_nodes = [
                self.nodes[nid]
                for nid, _ in final_results
                if nid in self.nodes
            ]

        stats["total_tokens"] = sum(
            n.token_budget for n in result_nodes
        )
        stats["total_energy"] = sum(
            n.energy_budget for n in result_nodes
        )

        pruning_eff = (
            stats["nodes_pruned"] / stats["nodes_evaluated"]
            if stats["nodes_evaluated"] > 0 else 0.0
        )
        with self._lock:
            self.pruning_efficiency.append(pruning_eff)
        stats["pruning_efficiency"] = pruning_eff
        stats["results_returned"] = len(result_nodes)

        logger.debug(
            "Retrieved %d node(s) for query=%r "
            "(pruning=%.3f, tokens=%d, energy=%.6f, "
            "exhausted=%s).",
            len(result_nodes), query, pruning_eff,
            stats["total_tokens"], stats["total_energy"],
            stats["exhausted"],
        )
        return result_nodes, stats

    # ---------------------------------------------------------- statistics
    def get_hierarchical_stats(self) -> Dict[str, Any]:
        with self._lock:
            node_count = len(self.nodes)
            per_level = {
                level.value: len(ids)
                for level, ids in self.level_index.items()
            }
            access = {
                level.value: count
                for level, count in
                self.level_access_counts.items()
            }
            pruning = list(self.pruning_efficiency)
            budgets = {
                level.value: b
                for level, b in self.budgets.items()
            }

        avg_pruning = (
            sum(pruning) / len(pruning)
            if pruning else 0.0
        )

        return {
            "total_nodes": node_count,
            "nodes_per_level": per_level,
            "level_access_counts": access,
            "avg_pruning_efficiency": avg_pruning,
            "token_budgets": budgets,
            "pruning_samples": len(pruning),
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_hierarchical_stats()

    # ---------------------------------------------------------- optimization
    def optimize_hierarchy(self) -> Dict[str, Any]:
        """
        Analyze the hierarchy and return optimization recommendations.
        """
        stats = self.get_hierarchical_stats()
        recommendations: List[Dict[str, str]] = []

        level_counts = stats["nodes_per_level"]
        coarse = level_counts.get("coarse", 0)
        medium = level_counts.get("medium", 0)

        if (
            medium > 0
            and coarse
            > medium * self._config.coarse_to_medium_ratio
        ):
            recommendations.append({
                "type": "rebalance",
                "action": "increase_medium_level_nodes",
                "reason": (
                    f"Coarse level is too dense relative to medium "
                    f"(coarse={coarse}, medium={medium})."
                ),
            })

        if (
            stats["avg_pruning_efficiency"]
            < self._config.min_pruning_efficiency
        ):
            recommendations.append({
                "type": "pruning",
                "action": "improve_coarse_level_discrimination",
                "reason": (
                    f"Average pruning efficiency "
                    f"{stats['avg_pruning_efficiency']:.3f} is below "
                    f"threshold "
                    f"{self._config.min_pruning_efficiency:.3f}."
                ),
            })

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
        self, query: str, node_ids: Sequence[str],
    ) -> List[Tuple[str, float]]:
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

        scored.sort(key=lambda t: (-t[1], t[0]))
        return scored

    def _next_level(
        self, current: RetrievalLevel
    ) -> RetrievalLevel:
        try:
            idx = _LEVEL_ORDER.index(current)
        except ValueError:  # pragma: no cover
            return RetrievalLevel.FINE
        if idx + 1 < len(_LEVEL_ORDER):
            return _LEVEL_ORDER[idx + 1]
        return RetrievalLevel.FINE

    def _get_children(
        self, parent_nodes: Sequence[Tuple[str, float]]
    ) -> List[str]:
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

    def _estimate_energy_budget(
        self, level: RetrievalLevel
    ) -> float:
        token_budget = self.budgets[level]
        return token_budget * self._config.micro_wh_per_token

    # ---------------------------------------------------------- lifecycle
    def reset(
        self, *, clear_pruning_history: bool = True
    ) -> int:
        with self._lock:
            for level in RetrievalLevel:
                self.level_access_counts[level] = 0
            removed = len(self.pruning_efficiency)
            if clear_pruning_history:
                self.pruning_efficiency.clear()
            self._started_at = time.time()
        logger.debug(
            "HierarchicalRetrieval reset (removed %d samples).",
            removed,
        )
        return removed

    def clear_hierarchy(self) -> int:
        with self._lock:
            removed = len(self.nodes)
            self.nodes.clear()
            for level in RetrievalLevel:
                self.level_index[level].clear()
        logger.debug(
            "Cleared %d hierarchical node(s).", removed,
        )
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_pruning: bool = False
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "nodes": [
                    n.to_dict() for n in self.nodes.values()
                ],
                "level_index": {
                    level.value: list(ids)
                    for level, ids in self.level_index.items()
                },
                "level_access_counts": {
                    level.value: count
                    for level, count in
                    self.level_access_counts.items()
                },
                "started_at": self._started_at,
            }
            if include_pruning:
                payload["pruning_efficiency"] = list(
                    self.pruning_efficiency
                )
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HierarchicalRetrieval":
        if not isinstance(data, Mapping):
            raise HierarchicalRetrievalError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            cfg = HierarchicalRetrievalConfig.from_dict(
                dict(data.get("config", {}) or {})
            )
            retrieval = cls(
                config=cfg,
                strict=bool(data.get("strict", True)),
            )

            raw_nodes = data.get("nodes") or []
            if not isinstance(raw_nodes, list):
                raise HierarchicalRetrievalError(
                    f"nodes must be a list, got "
                    f"{type(raw_nodes).__name__}."
                )

            seen_ids: set = set()
            with retrieval._lock:
                for entry in raw_nodes:
                    if not isinstance(entry, Mapping):
                        raise HierarchicalRetrievalError(
                            "each node entry must be a Mapping."
                        )
                    node = HierarchicalNode.from_dict(entry)
                    if node.node_id in seen_ids:
                        raise HierarchicalRetrievalError(
                            f"duplicate node_id {node.node_id!r} "
                            f"in payload."
                        )
                    seen_ids.add(node.node_id)
                    retrieval.nodes[node.node_id] = node

                # Reset level_index; validate or rebuild below.
                for level in RetrievalLevel:
                    retrieval.level_index[level] = []

                raw_index = data.get("level_index") or {}
                if not isinstance(raw_index, Mapping):
                    raise HierarchicalRetrievalError(
                        f"level_index must be a Mapping, got "
                        f"{type(raw_index).__name__}."
                    )

                if raw_index:
                    for level_value, ids in raw_index.items():
                        try:
                            level = RetrievalLevel(
                                str(level_value)
                            )
                        except ValueError as exc:
                            raise HierarchicalRetrievalError(
                                f"unknown level {level_value!r} "
                                f"in level_index: {exc}"
                            ) from exc
                        if not isinstance(ids, list):
                            raise HierarchicalRetrievalError(
                                f"level_index[{level_value!r}] "
                                f"must be a list."
                            )
                        for node_id in ids:
                            sid = str(node_id)
                            if sid not in retrieval.nodes:
                                raise HierarchicalRetrievalError(
                                    f"level_index["
                                    f"{level_value!r}] references "
                                    f"unknown node {sid!r}."
                                )
                            retrieval.level_index[
                                level
                            ].append(sid)
                else:
                    # Rebuild from nodes.
                    for node in retrieval.nodes.values():
                        retrieval.level_index[
                            node.level
                        ].append(node.node_id)

                raw_counts = (
                    data.get("level_access_counts") or {}
                )
                if isinstance(raw_counts, Mapping):
                    for level_value, count in (
                        raw_counts.items()
                    ):
                        try:
                            level = RetrievalLevel(
                                str(level_value)
                            )
                        except ValueError:
                            continue
                        try:
                            retrieval.level_access_counts[
                                level
                            ] = int(count)
                        except (TypeError, ValueError):
                            pass

                raw_pruning = (
                    data.get("pruning_efficiency") or []
                )
                if not isinstance(raw_pruning, list):
                    raise HierarchicalRetrievalError(
                        f"pruning_efficiency must be a list, got "
                        f"{type(raw_pruning).__name__}."
                    )
                for v in raw_pruning:
                    try:
                        fv = float(v)
                    except (TypeError, ValueError) as exc:
                        raise HierarchicalRetrievalError(
                            f"invalid pruning_efficiency "
                            f"value {v!r}: {exc}"
                        ) from exc
                    if not 0.0 <= fv <= 1.0:
                        raise HierarchicalRetrievalError(
                            f"pruning_efficiency value {fv} "
                            f"not in [0, 1]."
                        )
                    retrieval.pruning_efficiency.append(fv)

                try:
                    retrieval._started_at = float(
                        data.get("started_at", time.time())
                    )
                except (TypeError, ValueError):
                    retrieval._started_at = time.time()

            return retrieval
        except HierarchicalRetrievalError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise HierarchicalRetrievalError(
                f"invalid HierarchicalRetrieval payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_pruning: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_pruning=include_pruning),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str
    ) -> "HierarchicalRetrieval":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise HierarchicalRetrievalError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "HierarchicalRetrieval":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    # ---- Build a 3-level hierarchy -------------------------------- #
    hr = HierarchicalRetrieval()
    print("repr       :", hr)

    hr.add_hierarchical_node(
        "c_energy", RetrievalLevel.COARSE,
        "energy carbon sustainability green",
    )
    hr.add_hierarchical_node(
        "c_vision", RetrievalLevel.COARSE,
        "vision image detection classification",
    )
    hr.add_hierarchical_node(
        "m_renewable", RetrievalLevel.MEDIUM,
        "renewable solar wind hydro energy",
        parent="c_energy",
    )
    hr.add_hierarchical_node(
        "m_emissions", RetrievalLevel.MEDIUM,
        "emissions carbon footprint offsets",
        parent="c_energy",
    )
    hr.add_hierarchical_node(
        "m_cnn", RetrievalLevel.MEDIUM,
        "CNN ResNet convolution neural network",
        parent="c_vision",
    )
    hr.add_hierarchical_node(
        "f_solar", RetrievalLevel.FINE,
        "solar panel efficiency has improved",
        parent="m_renewable",
    )
    hr.add_hierarchical_node(
        "f_wind", RetrievalLevel.FINE,
        "wind turbine energy generation",
        parent="m_renewable",
    )
    hr.add_hierarchical_node(
        "f_carbon", RetrievalLevel.FINE,
        "carbon offsets neutralize emissions",
        parent="m_emissions",
    )
    hr.add_hierarchical_node(
        "f_cnn", RetrievalLevel.FINE,
        "ResNet achieves high image accuracy",
        parent="m_cnn",
    )

    assert len(hr) == 9
    assert "c_energy" in hr and "f_solar" in hr

    # ---- Retrieval: energy-oriented query ------------------------ #
    nodes, stats = hr.hierarchical_retrieve(
        "renewable solar energy", max_results=3,
    )
    print("query 1    :", [n.node_id for n in nodes])
    print("query 1 st :", {
        k: stats[k] for k in (
            "levels_traversed", "nodes_evaluated",
            "nodes_pruned", "results_returned",
            "pruning_efficiency", "max_results_capped",
            "exhausted",
        )
    })
    assert any(
        n.node_id == "f_solar" for n in nodes
    ), "energy query should reach solar node"
    assert stats["max_results_capped"] is False

    # ---- Retrieval: vision-oriented query ------------------------ #
    nodes2, stats2 = hr.hierarchical_retrieve(
        "image classification cnn",
    )
    print("query 2    :", [n.node_id for n in nodes2])

    # ---- Re-add preserves index / parent cleanliness ------------- #
    hr.add_hierarchical_node(
        "c_vision", RetrievalLevel.COARSE,
        "vision image detection (updated)",
    )
    assert (
        hr.level_index[RetrievalLevel.COARSE].count("c_vision")
        == 1
    )
    print("re-add     : OK")

    # ---- Re-parent cleans up old parent -------------------------- #
    hr.add_hierarchical_node(
        "f_carbon", RetrievalLevel.FINE,
        "carbon offsets neutralize emissions",
        parent="m_renewable",
    )
    assert "f_carbon" not in hr.nodes["m_emissions"].children
    assert "f_carbon" in hr.nodes["m_renewable"].children
    print("re-parent  : OK")

    # ---- Stale children are filtered ----------------------------- #
    hr.nodes["m_renewable"].children.append("ghost_node")
    children = hr._get_children([("m_renewable", 1.0)])
    assert "ghost_node" not in children
    print("stale      : OK")

    # ---- Empty hierarchy: exhausted flag ------------------------- #
    empty = HierarchicalRetrieval()
    empty_nodes, empty_stats = empty.hierarchical_retrieve(
        "anything",
    )
    assert empty_nodes == []
    assert empty_stats["exhausted"] is True
    print("empty       : exhausted=True")

    # ---- FINE start on empty hierarchy: exhausted flag ----------- #
    _, fine_empty_stats = empty.hierarchical_retrieve(
        "anything", start_level=RetrievalLevel.FINE,
    )
    assert fine_empty_stats["exhausted"] is True, fine_empty_stats
    print("FINE empty  : exhausted=True")

    # ---- max_results_capped surfaced ---------------------------- #
    capped_hr = HierarchicalRetrieval(
        config=HierarchicalRetrievalConfig(
            max_results_cap=2, top_k_per_level=2,
        )
    )
    for i in range(4):
        capped_hr.add_hierarchical_node(
            f"f{i}", RetrievalLevel.FINE, f"content {i}",
        )
    _, capped_stats = capped_hr.hierarchical_retrieve(
        "content", max_results=100,
    )
    assert capped_stats["max_results_capped"] is True
    assert (
        capped_stats["results_returned"]
        <= 2
    )
    print("cap         : signalled in stats")

    # ---- add_hierarchical_node returns None on rejection --------- #
    lenient = HierarchicalRetrieval(strict=False)
    assert lenient.add_hierarchical_node(
        "", RetrievalLevel.FINE, "x",
    ) is None
    assert lenient.node_count == 0
    print("non-strict  : rejected → None, no mutation")

    # ---- remove_node removes recursively ------------------------ #
    rec_hr = HierarchicalRetrieval()
    rec_hr.add_hierarchical_node(
        "c1", RetrievalLevel.COARSE, "cluster one",
    )
    rec_hr.add_hierarchical_node(
        "m1", RetrievalLevel.MEDIUM, "topic one",
        parent="c1",
    )
    rec_hr.add_hierarchical_node(
        "f1", RetrievalLevel.FINE, "leaf one",
        parent="m1",
    )
    rec_hr.add_hierarchical_node(
        "f2", RetrievalLevel.FINE, "leaf two",
        parent="m1",
    )
    assert rec_hr.remove_node("m1") is True
    assert rec_hr.node_count == 1
    assert "c1" in rec_hr
    assert "m1" not in rec_hr
    assert "f1" not in rec_hr and "f2" not in rec_hr
    assert "m1" not in rec_hr.level_index[RetrievalLevel.MEDIUM]
    assert (
        rec_hr.remove_node("does_not_exist") is False
    )
    print("remove      : recursive OK")

    # ---- Content change invalidates token cache ------------------ #
    cache_hr = HierarchicalRetrieval()
    n = cache_hr.add_hierarchical_node(
        "n1", RetrievalLevel.FINE, "old words here",
    )
    assert n is not None
    tokens_before = n.content_tokens()
    assert "old" in tokens_before
    n.content = "brand new words"
    tokens_after = n.content_tokens()
    assert "brand" in tokens_after
    assert "old" not in tokens_after
    print("cache       : auto-invalidates")

    # ---- from_dict round-trip + validation ---------------------- #
    payload = hr.to_json(include_pruning=True)
    restored = HierarchicalRetrieval.from_json(payload)
    assert restored.to_dict(include_pruning=True) == hr.to_dict(
        include_pruning=True
    )
    print("round-trip  : OK")

    # Rebuild level_index from nodes when payload omits it.
    bare = hr.to_dict()
    del bare["level_index"]
    rebuilt = HierarchicalRetrieval.from_dict(bare)
    assert rebuilt.node_count == hr.node_count
    for level in RetrievalLevel:
        assert (
            len(rebuilt.level_index[level])
            == len(hr.level_index[level])
        )
    print("rebuild     : OK (missing level_index)")

    # ---- from_dict validation ----------------------------------- #
    for bad_payload in (
        {"nodes": "abc"},
        {"nodes": [{"node_id": "x"}]},  # missing level
        {"nodes": [
            {"node_id": "x", "level": "bogus"},
        ]},
        {"nodes": [
            {"node_id": "x", "level": "fine"},
            {"node_id": "x", "level": "fine"},
        ]},  # duplicate
        {"nodes": [
            {"node_id": "x", "level": "fine"},
        ], "level_index": {"fine": ["nonexistent"]}},
        {"pruning_efficiency": [1.5]},
        {"pruning_efficiency": "abc"},
    ):
        try:
            HierarchicalRetrieval.from_dict(bad_payload)
        except HierarchicalRetrievalError as exc:
            print(
                "Rejected payload:", str(exc)[:60], "...",
            )

    # ---- to_json rejects NaN ------------------------------------ #
    tainted = HierarchicalRetrieval()
    tainted.add_hierarchical_node(
        "x", RetrievalLevel.FINE, "hi",
    )
    object.__setattr__(
        tainted._config, "micro_wh_per_token", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN rejected:", str(exc)[:60])

    # ---- Config validation -------------------------------------- #
    for bad_cfg in (
        dict(coarse_budget_tokens=0),
        dict(coarse_budget_tokens=2000, medium_budget_tokens=1000),
        dict(medium_budget_tokens=6000, fine_budget_tokens=4096),
        dict(top_k_per_level=0),
        dict(max_depth=0),
        dict(max_results_cap=0),
        dict(micro_wh_per_token=0),
        dict(micro_wh_per_token=float("nan")),
        dict(coarse_to_medium_ratio=0.0),
        dict(min_pruning_efficiency=1.5),
        dict(max_history=0),
    ):
        try:
            HierarchicalRetrievalConfig(**bad_cfg)  # type: ignore[arg-type]
        except HierarchicalRetrievalError as exc:
            print("Rejected cfg:", exc)

    # ---- Strict-mode input validation --------------------------- #
    strict = HierarchicalRetrieval(strict=True)
    for bad_call in (
        lambda: strict.add_hierarchical_node(
            "", RetrievalLevel.FINE, "x",
        ),
        lambda: strict.add_hierarchical_node(
            "a", "not-a-level", "x",  # type: ignore[arg-type]
        ),
        lambda: strict.add_hierarchical_node(
            "a", RetrievalLevel.FINE, "x", parent="missing",
        ),
        lambda: strict.add_hierarchical_node(
            "a", RetrievalLevel.FINE, "x",
            metadata="not-a-dict",  # type: ignore[arg-type]
        ),
        lambda: strict.hierarchical_retrieve(""),
        lambda: strict.hierarchical_retrieve(
            "q", start_level="bogus",  # type: ignore[arg-type]
        ),
        lambda: strict.hierarchical_retrieve(
            "q", max_results=0,
        ),
        lambda: strict.hierarchical_retrieve(
            "q", max_results=-5,
        ),
    ):
        try:
            bad_call()
        except HierarchicalRetrievalError as exc:
            print("Rejected   :", exc)

    # ---- Parent level mismatch (strict) ------------------------- #
    try:
        strict.add_hierarchical_node(
            "p", RetrievalLevel.FINE, "parent",
        )
        strict.add_hierarchical_node(
            "q", RetrievalLevel.FINE, "child", parent="p",
        )
    except HierarchicalRetrievalError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict ignores invalid parents --------------------- #
    lenient2 = HierarchicalRetrieval(strict=False)
    n2 = lenient2.add_hierarchical_node(
        "a", RetrievalLevel.COARSE, "x", parent="missing",
    )
    assert n2 is not None
    assert lenient2.nodes["a"].parent is None
    print("lenient     : OK")

    # ---- Statistics --------------------------------------------- #
    print("stats       :", {
        k: v for k, v in hr.get_hierarchical_stats().items()
        if k != "uptime_seconds"
    })

    # ---- Optimization recommendations --------------------------- #
    recs = hr.optimize_hierarchy()
    print(
        "recs        :",
        [r["action"] for r in recs["recommendations"]],
    )

    # ---- Context manager ---------------------------------------- #
    with HierarchicalRetrieval() as scoped:
        scoped.add_hierarchical_node(
            "only", RetrievalLevel.FINE, "solo node",
        )
        nodes, _ = scoped.hierarchical_retrieve("solo")
        assert any(n.node_id == "only" for n in nodes)
    print("context     : OK")

    print("\nSmoke test passed.")
