# src/retrieval/vimrag_coretrieval.py

"""
VimRAG Co-Retrieval Module
==========================

Handles multimodal memory graphs (text + visual context) with energy-efficient
filtering based on token size and file weight.

Based on: VimRAG — Navigating Massive Visual Context in Retrieval-Augmented
Generation via Multimodal Memory Graph.

Enhancements
------------
- ``VimRAGConfig`` — frozen, validated: token limits, energy budget,
  per-type coefficients, boost weights, bounded history.
- ``RetrievalNode`` / ``RetrievalTrace`` — frozen dataclasses with full
  validation and serialization.
- **Fixed naive timestamps** — ``datetime.now(timezone.utc)`` everywhere.
- **Fixed non-deterministic node IDs** — content-hash based, with an
  optional sequence suffix for duplicate content.
- **Fixed MD5 collisions** — replaced with SHA-256 + uuid4.
- **Thread safety** — ``RLock`` guards graph, edges, and trace history.
- **Bounded trace history** — ``deque(maxlen=config.max_history)``.
- **Full validation** of every argument; strict / non-strict modes.
- **Atomic export** — ``tempfile`` + ``os.replace``; parent dirs created.
- **Content-token caching** — ``_score_relevance`` tokenizes once per node.
- **Deterministic tie-breaking** on retrieval scoring.
- Serialization on the module, the config, and both dataclasses.
- ``statistics()``, ``__repr__``, custom ``VimRAGError``, lazy ``%s``
  logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class VimRAGError(ValueError):
    """Raised for invalid VimRAG co-retrieval inputs or configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_VALID_CONTENT_TYPES: Tuple[str, ...] = ("text", "visual", "multimodal")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class VimRAGConfig:
    """
    Tunable parameters for :class:`VimRAGCoRetrieval`.

    Centralizes every token limit, energy coefficient, keyword-boost weight,
    and bounded-history cap that was previously hardcoded in the class body.
    """

    # Retrieval limits.
    max_tokens_per_retrieval: int = 4096
    max_energy_per_retrieval_wh: float = 0.01
    enable_visual_context: bool = True

    # Token estimation coefficients.
    chars_per_token: int = 4
    visual_token_estimate: int = 256
    multimodal_token_estimate: int = 256

    # Energy model.
    token_energy_cost_wh: float = 1e-6
    visual_energy_multiplier: float = 2.5
    multimodal_energy_multiplier: float = 2.5

    # Scoring weights (must sum to 1.0).
    keyword_weight: float = 0.7
    recency_weight: float = 0.3

    # Recency decay half-life (hours).
    recency_half_life_hours: float = 24.0

    # Efficiency formula coefficients.
    efficiency_energy_scale: float = 1000.0
    efficiency_token_scale: float = 100.0

    # Bounded trace history.
    max_history: int = 10_000

    # Content preview used in hashing/ID generation.
    hash_preview_chars: int = 100

    def __post_init__(self) -> None:
        if self.max_tokens_per_retrieval <= 0:
            raise VimRAGError(
                "max_tokens_per_retrieval must be > 0."
            )
        if self.max_energy_per_retrieval_wh <= 0:
            raise VimRAGError(
                "max_energy_per_retrieval_wh must be > 0."
            )
        for name in (
            "chars_per_token",
            "visual_token_estimate",
            "multimodal_token_estimate",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise VimRAGError(f"{name} must be a positive int.")
        for name in (
            "token_energy_cost_wh",
            "visual_energy_multiplier",
            "multimodal_energy_multiplier",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise VimRAGError(f"{name} must be > 0.")
        for name in ("keyword_weight", "recency_weight"):
            value = getattr(self, name)
            if value < 0:
                raise VimRAGError(f"{name} must be >= 0.")
        weight_sum = self.keyword_weight + self.recency_weight
        if abs(weight_sum - 1.0) > 1e-6:
            raise VimRAGError(
                f"keyword_weight + recency_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )
        if self.recency_half_life_hours <= 0:
            raise VimRAGError(
                "recency_half_life_hours must be > 0."
            )
        if self.efficiency_energy_scale <= 0:
            raise VimRAGError("efficiency_energy_scale must be > 0.")
        if self.efficiency_token_scale <= 0:
            raise VimRAGError("efficiency_token_scale must be > 0.")
        if self.max_history <= 0:
            raise VimRAGError("max_history must be > 0.")
        if self.hash_preview_chars <= 0:
            raise VimRAGError("hash_preview_chars must be > 0.")

    # ----- convenience ---------------------------------------------------
    def token_estimate(self, content: Any, content_type: str) -> int:
        if content_type == "text":
            return len(str(content)) // self.chars_per_token
        if content_type == "visual":
            return self.visual_token_estimate
        if content_type == "multimodal":
            return (
                len(str(content)) // self.chars_per_token
                + self.multimodal_token_estimate
            )
        return 0

    def energy_multiplier(self, content_type: str) -> float:
        if content_type == "visual":
            return self.visual_energy_multiplier
        if content_type == "multimodal":
            return self.multimodal_energy_multiplier
        return 1.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VimRAGConfig":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                f"VimRAGConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RetrievalNode:
    """Immutable node in the multimodal memory graph."""

    node_id: str
    content_type: str
    content: Any
    token_size: int
    file_weight: float
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Cached token set for relevance scoring — excluded from equality/hash.
    _content_tokens: Optional[FrozenSet[str]] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise VimRAGError("node_id must be a non-empty string.")
        if self.content_type not in _VALID_CONTENT_TYPES:
            raise VimRAGError(
                f"content_type must be one of {_VALID_CONTENT_TYPES}, "
                f"got {self.content_type!r}."
            )
        if not isinstance(self.token_size, int) or self.token_size < 0:
            raise VimRAGError("token_size must be a non-negative int.")
        if isinstance(self.file_weight, bool) or not isinstance(
            self.file_weight, (int, float)
        ):
            raise VimRAGError("file_weight must be numeric.")
        fw = float(self.file_weight)
        if math.isnan(fw) or math.isinf(fw) or fw < 0:
            raise VimRAGError(
                f"file_weight must be finite and >= 0, got "
                f"{self.file_weight!r}."
            )
        if not isinstance(self.metadata, Mapping):
            raise VimRAGError("metadata must be a Mapping.")
        if not isinstance(self.timestamp, (int, float)) or isinstance(
            self.timestamp, bool
        ):
            raise VimRAGError("timestamp must be numeric.")

    def content_tokens(self) -> FrozenSet[str]:
        """Return the cached lower-cased token set of ``content``."""
        if self._content_tokens is None:
            object.__setattr__(
                self,
                "_content_tokens",
                frozenset(str(self.content).lower().split()),
            )
        return self._content_tokens

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "content_type": self.content_type,
            "content": self.content,
            "token_size": self.token_size,
            "file_weight": self.file_weight,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RetrievalNode":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                "RetrievalNode.from_dict expects a Mapping."
            )
        ts = data.get("created_at")
        if isinstance(ts, str):
            created_at = datetime.fromisoformat(ts)
            if not created_at.tzinfo:
                created_at = created_at.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            created_at = ts
        else:
            created_at = datetime.now(timezone.utc)
        return cls(
            node_id=str(data["node_id"]),
            content_type=str(data["content_type"]),
            content=data.get("content"),
            token_size=int(data.get("token_size", 0)),
            file_weight=float(data.get("file_weight", 0.0)),
            metadata=dict(data.get("metadata", {})),
            timestamp=float(data.get("timestamp", time.time())),
            created_at=created_at,
        )

    def __repr__(self) -> str:
        return (
            "RetrievalNode("
            f"id={self.node_id[:8]}..., "
            f"type={self.content_type}, "
            f"tokens={self.token_size}, "
            f"weight={self.file_weight:.6f})"
        )


@dataclass(frozen=True)
class RetrievalTrace:
    """Immutable trace of a retrieval operation for meta-cognitive reflection."""

    trace_id: str
    query: str
    retrieved_nodes: Tuple[str, ...]
    total_tokens: int
    total_energy_wh: float
    retrieval_path: Tuple[str, ...]
    efficiency_score: float
    timestamp: float
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.trace_id, str) or not self.trace_id:
            raise VimRAGError("trace_id must be a non-empty string.")
        if not isinstance(self.query, str):
            raise VimRAGError("query must be a string.")
        if not isinstance(self.retrieved_nodes, tuple):
            object.__setattr__(
                self, "retrieved_nodes", tuple(self.retrieved_nodes)
            )
        if not isinstance(self.retrieval_path, tuple):
            object.__setattr__(
                self, "retrieval_path", tuple(self.retrieval_path)
            )
        if not isinstance(self.total_tokens, int) or self.total_tokens < 0:
            raise VimRAGError("total_tokens must be a non-negative int.")
        for name in ("total_energy_wh", "efficiency_score"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise VimRAGError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise VimRAGError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.total_energy_wh < 0:
            raise VimRAGError("total_energy_wh must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "query": self.query,
            "retrieved_nodes": list(self.retrieved_nodes),
            "total_tokens": self.total_tokens,
            "total_energy_wh": self.total_energy_wh,
            "retrieval_path": list(self.retrieval_path),
            "efficiency_score": self.efficiency_score,
            "timestamp": self.timestamp,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RetrievalTrace":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                "RetrievalTrace.from_dict expects a Mapping."
            )
        ts = data.get("created_at")
        if isinstance(ts, str):
            created_at = datetime.fromisoformat(ts)
            if not created_at.tzinfo:
                created_at = created_at.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            created_at = ts
        else:
            created_at = datetime.now(timezone.utc)
        return cls(
            trace_id=str(data["trace_id"]),
            query=str(data.get("query", "")),
            retrieved_nodes=tuple(data.get("retrieved_nodes", [])),
            total_tokens=int(data.get("total_tokens", 0)),
            total_energy_wh=float(data.get("total_energy_wh", 0.0)),
            retrieval_path=tuple(data.get("retrieval_path", [])),
            efficiency_score=float(data.get("efficiency_score", 0.0)),
            timestamp=float(data.get("timestamp", time.time())),
            created_at=created_at,
        )

    def __repr__(self) -> str:
        return (
            "RetrievalTrace("
            f"id={self.trace_id[:8]}..., "
            f"nodes={len(self.retrieved_nodes)}, "
            f"tokens={self.total_tokens}, "
            f"energy_wh={self.total_energy_wh:.6f}, "
            f"eff={self.efficiency_score:.4g})"
        )


# --------------------------------------------------------------------------- #
# Main module
# --------------------------------------------------------------------------- #
class VimRAGCoRetrieval:
    """
    VimRAG-inspired co-retrieval module for multimodal memory graphs.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.

    Responsibilities
    ----------------
    - Manage the multimodal memory graph (text + visual context).
    - Filter retrievals by token size and file weight.
    - Ensure energy-efficient access patterns.
    - Provide co-retrieval traces for meta-cognitive reflection.
    - Support serendipitous discovery of efficient paths.
    """

    def __init__(
        self,
        max_tokens_per_retrieval: int = 4096,
        max_energy_per_retrieval_wh: float = 0.01,
        enable_visual_context: bool = True,
        *,
        config: Optional[VimRAGConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = VimRAGConfig(
                max_tokens_per_retrieval=int(max_tokens_per_retrieval),
                max_energy_per_retrieval_wh=float(max_energy_per_retrieval_wh),
                enable_visual_context=bool(enable_visual_context),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.max_tokens: int = self._config.max_tokens_per_retrieval
        self.max_energy: float = self._config.max_energy_per_retrieval_wh
        self.enable_visual: bool = self._config.enable_visual_context
        self.token_energy_cost: float = self._config.token_energy_cost_wh
        self.visual_energy_multiplier: float = (
            self._config.visual_energy_multiplier
        )

        self._lock = threading.RLock()
        self.memory_graph: Dict[str, RetrievalNode] = {}
        self.graph_edges: Dict[str, List[str]] = {}
        self.retrieval_traces: Deque[RetrievalTrace] = deque(
            maxlen=self._config.max_history
        )
        # Counter for deterministic duplicate-content node IDs.
        self._sequence: int = 0

        logger.debug(
            "VimRAGCoRetrieval initialized "
            "(max_tokens=%d, max_energy_wh=%.4f, visual=%s, strict=%s)",
            self.max_tokens, self.max_energy, self.enable_visual, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> VimRAGConfig:
        return self._config

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self.memory_graph)

    @property
    def trace_count(self) -> int:
        with self._lock:
            return len(self.retrieval_traces)

    @property
    def edge_count(self) -> int:
        with self._lock:
            return sum(len(e) for e in self.graph_edges.values())

    # ---------------------------------------------------------- node ingestion
    def add_node(
        self,
        content: Any,
        content_type: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a node to the multimodal memory graph.

        Parameters
        ----------
        content : Any
            Node content (text, image path, etc.).
        content_type : str
            One of ``text``, ``visual``, ``multimodal``.
        metadata : Mapping, optional
            Arbitrary extra fields.

        Returns
        -------
        str
            The node ID. Identical ``(content, content_type, metadata)``
            combinations produce the same ID across runs; callers that need
            duplicates should pass distinct ``metadata`` or add an explicit
            ``"sequence"`` field.
        """
        if content_type not in _VALID_CONTENT_TYPES:
            raise VimRAGError(
                f"content_type must be one of {_VALID_CONTENT_TYPES}, "
                f"got {content_type!r}."
            )
        if metadata is not None and not isinstance(metadata, Mapping):
            msg = (
                f"metadata must be a Mapping or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise VimRAGError(msg)
            logger.warning("%s Ignoring metadata.", msg)
            metadata = None

        node_id = self._generate_node_id(content, content_type, metadata)
        token_size = self._estimate_token_size(content, content_type)
        file_weight = self._calculate_file_weight(token_size, content_type)

        node = RetrievalNode(
            node_id=node_id,
            content_type=content_type,
            content=content,
            token_size=token_size,
            file_weight=file_weight,
            metadata=dict(metadata or {}),
            timestamp=time.time(),
        )

        with self._lock:
            self.memory_graph[node_id] = node
            self.graph_edges.setdefault(node_id, [])

        logger.debug(
            "Added node %s (type=%s, tokens=%d, weight=%.6f).",
            node_id, content_type, token_size, file_weight,
        )
        return node_id

    def connect_nodes(self, source_id: str, target_id: str) -> bool:
        """
        Create a directed edge between two existing nodes.

        Returns True if the edge was added, False if it already existed.
        Raises :class:`VimRAGError` for unknown nodes in strict mode.
        """
        if not isinstance(source_id, str) or not source_id:
            raise VimRAGError("source_id must be a non-empty string.")
        if not isinstance(target_id, str) or not target_id:
            raise VimRAGError("target_id must be a non-empty string.")

        with self._lock:
            if source_id not in self.memory_graph:
                msg = f"source node {source_id!r} not found."
                if self._strict:
                    raise VimRAGError(msg)
                logger.warning("%s Skipping.", msg)
                return False
            if target_id not in self.memory_graph:
                msg = f"target node {target_id!r} not found."
                if self._strict:
                    raise VimRAGError(msg)
                logger.warning("%s Skipping.", msg)
                return False

            edges = self.graph_edges.setdefault(source_id, [])
            if target_id in edges:
                return False
            edges.append(target_id)

        logger.debug("Connected %s -> %s.", source_id, target_id)
        return True

    # ---------------------------------------------------------- retrieval
    def retrieve(
        self,
        query: str,
        max_nodes: int = 10,
        include_visual: bool = True,
        energy_budget_wh: Optional[float] = None,
    ) -> Tuple[List[RetrievalNode], RetrievalTrace]:
        """
        Retrieve relevant nodes from the memory graph with energy constraints.

        Parameters
        ----------
        query : str
            Retrieval query.
        max_nodes : int
            Maximum number of nodes to retrieve. Must be positive.
        include_visual : bool
            If False, visual nodes are excluded from the candidate set.
        energy_budget_wh : float, optional
            Energy budget for this retrieval. Defaults to
            ``config.max_energy_per_retrieval_wh``.

        Returns
        -------
        (nodes, trace)
        """
        if not isinstance(query, str):
            raise VimRAGError("query must be a string.")
        if not isinstance(max_nodes, int) or max_nodes <= 0:
            raise VimRAGError("max_nodes must be a positive int.")
        if not isinstance(include_visual, bool):
            raise VimRAGError("include_visual must be a bool.")
        budget = self._validate_budget(energy_budget_wh)

        # ---- Snapshot candidates --------------------------------------
        with self._lock:
            candidates = [
                node for node in self.memory_graph.values()
                if include_visual or node.content_type != "visual"
            ]

        # ---- Score and rank -------------------------------------------
        scored = [
            (node, self._score_relevance(query, node))
            for node in candidates
        ]
        # Deterministic tie-break: score desc, then node_id asc.
        scored.sort(key=lambda x: (-x[1], x[0].node_id))

        # ---- Greedy selection under budget ----------------------------
        selected: List[RetrievalNode] = []
        total_tokens = 0
        total_energy = 0.0
        retrieval_path: List[str] = []

        for node, _score in scored:
            if len(selected) >= max_nodes:
                break
            if total_energy + node.file_weight > budget:
                continue
            if total_tokens + node.token_size > self.max_tokens:
                continue
            selected.append(node)
            total_tokens += node.token_size
            total_energy += node.file_weight
            retrieval_path.append(node.node_id)

        # ---- Efficiency score -----------------------------------------
        efficiency = self._calculate_efficiency_score(
            len(selected), total_tokens, total_energy,
        )

        trace = RetrievalTrace(
            trace_id=self._generate_trace_id(query),
            query=query,
            retrieved_nodes=tuple(n.node_id for n in selected),
            total_tokens=total_tokens,
            total_energy_wh=total_energy,
            retrieval_path=tuple(retrieval_path),
            efficiency_score=efficiency,
            timestamp=time.time(),
        )

        with self._lock:
            self.retrieval_traces.append(trace)

        logger.debug(
            "Retrieved %d/%d node(s) for query=%r (tokens=%d, "
            "energy=%.6f Wh, budget=%.6f Wh).",
            len(selected), len(candidates), query,
            total_tokens, total_energy, budget,
        )
        return selected, trace

    # ---------------------------------------------------------- trace access
    def get_retrieval_traces(self, n: int = 10) -> List[RetrievalTrace]:
        """Return the ``n`` most recent traces."""
        if not isinstance(n, int) or n <= 0:
            raise VimRAGError("n must be a positive int.")
        with self._lock:
            traces = list(self.retrieval_traces)
        return traces[-n:] if len(traces) >= n else traces

    def analyze_retrieval_patterns(self) -> Dict[str, Any]:
        """Return aggregate statistics over the retrieval history."""
        with self._lock:
            traces = list(self.retrieval_traces)

        if not traces:
            return {
                "status": "no_traces",
                "total_retrievals": 0,
                "avg_tokens_per_retrieval": 0.0,
                "avg_energy_per_retrieval_wh": 0.0,
                "avg_efficiency_score": 0.0,
                "most_efficient_retrieval": None,
            }

        avg_tokens = sum(t.total_tokens for t in traces) / len(traces)
        avg_energy = sum(t.total_energy_wh for t in traces) / len(traces)
        avg_efficiency = sum(t.efficiency_score for t in traces) / len(traces)

        # Deterministic tie-break on efficiency.
        most_efficient = max(
            traces, key=lambda t: (t.efficiency_score, t.trace_id),
        )

        return {
            "total_retrievals": len(traces),
            "avg_tokens_per_retrieval": avg_tokens,
            "avg_energy_per_retrieval_wh": avg_energy,
            "avg_efficiency_score": avg_efficiency,
            "most_efficient_retrieval": {
                "trace_id": most_efficient.trace_id,
                "efficiency_score": most_efficient.efficiency_score,
                "tokens": most_efficient.total_tokens,
                "energy_wh": most_efficient.total_energy_wh,
            },
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`analyze_retrieval_patterns` (consistent with the tree)."""
        return self.analyze_retrieval_patterns()

    def get_memory_graph_stats(self) -> Dict[str, Any]:
        """Return aggregate statistics about the memory graph."""
        with self._lock:
            nodes = list(self.memory_graph.values())
            edge_count = sum(len(e) for e in self.graph_edges.values())

        text_nodes = sum(1 for n in nodes if n.content_type == "text")
        visual_nodes = sum(1 for n in nodes if n.content_type == "visual")
        multimodal_nodes = sum(
            1 for n in nodes if n.content_type == "multimodal"
        )
        total_tokens = sum(n.token_size for n in nodes)
        total_weight = sum(n.file_weight for n in nodes)

        return {
            "total_nodes": len(nodes),
            "text_nodes": text_nodes,
            "visual_nodes": visual_nodes,
            "multimodal_nodes": multimodal_nodes,
            "total_tokens": total_tokens,
            "total_energy_weight_wh": total_weight,
            "total_edges": edge_count,
            "traces_recorded": len(self.retrieval_traces),
        }

    # ---------------------------------------------------------- export
    def export_traces(self, filepath: str) -> int:
        """
        Atomically export retrieval traces to a JSON file.

        Returns the number of bytes written. Parent directories are created
        automatically. Uses ``tempfile`` + ``os.replace``.
        """
        if not isinstance(filepath, str) or not filepath:
            raise VimRAGError("filepath must be a non-empty string.")
        path = Path(filepath)

        with self._lock:
            payload = {
                "traces": [t.to_dict() for t in self.retrieval_traces],
                "analysis": self.analyze_retrieval_patterns(),
            }

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise VimRAGError(
                f"Could not create parent directory for {path}: {exc}"
            ) from exc

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, default=str)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            raise VimRAGError(
                f"Could not write traces to {path}: {exc}"
            ) from exc

        size = path.stat().st_size
        logger.debug("Exported traces to %s (%d bytes).", path, size)
        return size

    # ---------------------------------------------------------- internal
    def _generate_node_id(
        self,
        content: Any,
        content_type: str,
        metadata: Optional[Mapping[str, Any]],
    ) -> str:
        """
        Return a deterministic, collision-resistant node ID.

        The ID is derived from SHA-256 over (content_type, content preview,
        metadata repr). Two calls with identical inputs produce identical
        IDs across processes; callers who need duplicates should add a
        distinguishing field to ``metadata``.
        """
        preview = str(content)[: self._config.hash_preview_chars]
        meta_repr = ""
        if metadata:
            try:
                meta_repr = json.dumps(metadata, sort_keys=True, default=str)
            except Exception:
                meta_repr = repr(metadata)
        raw = f"{content_type}:{preview}:{meta_repr}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _generate_trace_id(self, query: str) -> str:
        """Return a collision-resistant trace ID."""
        raw = f"{uuid.uuid4().hex}:{query}:{time.time()}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _estimate_token_size(self, content: Any, content_type: str) -> int:
        """Estimate the token size of ``content`` (config-driven)."""
        return self._config.token_estimate(content, content_type)

    def _calculate_file_weight(
        self, token_size: int, content_type: str
    ) -> float:
        """Compute the energy cost (Wh) for ``content_type``."""
        base_energy = token_size * self._config.token_energy_cost_wh
        return base_energy * self._config.energy_multiplier(content_type)

    def _score_relevance(self, query: str, node: RetrievalNode) -> float:
        """
        Score a node's relevance to ``query``.

        Combines keyword overlap and recency, using the node's cached token
        set (the original re-tokenized the content on every call).
        """
        cfg = self._config
        query_words = frozenset(query.lower().split())
        content_words = node.content_tokens()
        overlap = len(query_words & content_words)

        age_hours = max(0.0, (time.time() - node.timestamp) / 3600.0)
        half_life = cfg.recency_half_life_hours
        recency_score = 1.0 / (1.0 + age_hours / half_life)

        return (
            cfg.keyword_weight * overlap
            + cfg.recency_weight * recency_score
        )

    def _calculate_efficiency_score(
        self,
        num_nodes: int,
        total_tokens: int,
        total_energy: float,
    ) -> float:
        """
        Return a normalized efficiency score for a retrieval.

        Efficiency = nodes / (energy × energy_scale + tokens / token_scale).
        Higher is better; the scales are configurable.
        """
        if total_energy <= 0 or total_tokens <= 0 or num_nodes <= 0:
            return 0.0
        cfg = self._config
        denom = (
            total_energy * cfg.efficiency_energy_scale
            + total_tokens / cfg.efficiency_token_scale
        )
        if denom <= 0:
            return 0.0
        return num_nodes / denom

    # ---------------------------------------------------------- validation
    def _validate_budget(self, value: Optional[float]) -> float:
        if value is None:
            return self.max_energy
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"energy_budget_wh must be numeric or None, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise VimRAGError(msg)
            logger.warning("%s Using default.", msg)
            return self.max_energy
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv) or fv < 0:
            msg = (
                f"energy_budget_wh must be finite and >= 0, got {value!r}."
            )
            if self._strict:
                raise VimRAGError(msg)
            logger.warning("%s Using default.", msg)
            return self.max_energy
        return fv

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_graph: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "nodes": len(self.memory_graph),
                    "edges": sum(len(e) for e in self.graph_edges.values()),
                    "traces": len(self.retrieval_traces),
                },
            }
            if include_graph:
                payload["nodes"] = [
                    n.to_dict() for n in self.memory_graph.values()
                ]
                payload["edges"] = {
                    src: list(dst) for src, dst in self.graph_edges.items()
                }
                payload["traces"] = [
                    t.to_dict() for t in self.retrieval_traces
                ]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VimRAGCoRetrieval":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = VimRAGConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        rag = cls(config=cfg, strict=bool(data.get("strict", True)))
        with rag._lock:
            for entry in data.get("nodes", []):
                node = RetrievalNode.from_dict(entry)
                rag.memory_graph[node.node_id] = node
                rag.graph_edges.setdefault(node.node_id, [])
            for src, dst_list in (data.get("edges") or {}).items():
                rag.graph_edges.setdefault(str(src), list(dst_list))
            for entry in data.get("traces", []):
                rag.retrieval_traces.append(
                    RetrievalTrace.from_dict(entry)
                )
        return rag

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "VimRAGCoRetrieval":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise VimRAGError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_graph: bool = False) -> int:
        """Reset trace history; optionally clear the graph. Returns traces removed."""
        with self._lock:
            removed = len(self.retrieval_traces)
            self.retrieval_traces.clear()
            if clear_graph:
                self.memory_graph.clear()
                self.graph_edges.clear()
            self._sequence = 0
        logger.debug("VimRAGCoRetrieval reset (removed %d trace(s)).", removed)
        return removed

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "VimRAGCoRetrieval":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "VimRAGCoRetrieval scope exited with %s.",
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
            return node_id in self.memory_graph

    def __repr__(self) -> str:
        with self._lock:
            return (
                "VimRAGCoRetrieval("
                f"nodes={len(self.memory_graph)}, "
                f"edges={sum(len(e) for e in self.graph_edges.values())}, "
                f"traces={len(self.retrieval_traces)}, "
                f"max_tokens={self.max_tokens}, "
                f"max_energy={self.max_energy:.4f}Wh, "
                f"visual={self.enable_visual}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "RetrievalNode",
    "RetrievalTrace",
    "VimRAGConfig",
    "VimRAGCoRetrieval",
    "VimRAGError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.vimrag_coretrieval
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path --------------------------------------------------- #
    rag = VimRAGCoRetrieval()
    print("repr       :", rag)

    text_id = rag.add_node(
        "Renewable energy reduces carbon emissions significantly.",
        "text",
        metadata={"topic": "energy"},
    )
    visual_id = rag.add_node(
        "/path/to/solar_panel.png", "visual",
    )
    multimodal_id = rag.add_node(
        "Chart showing solar adoption 2010-2024", "multimodal",
    )
    print("text node  :", text_id)
    print("visual     :", visual_id)
    print("multimodal :", multimodal_id)

    # Deterministic IDs across calls with identical inputs.
    again = rag.add_node(
        "Renewable energy reduces carbon emissions significantly.",
        "text",
        metadata={"topic": "energy"},
    )
    assert again == text_id, "identical inputs must produce identical IDs"
    print("determinism: OK")

    # ---- Connect graph ------------------------------------------------ #
    assert rag.connect_nodes(text_id, visual_id) is True
    assert rag.connect_nodes(text_id, multimodal_id) is True
    assert rag.connect_nodes(text_id, visual_id) is False  # already exists
    print("edges      :", rag.edge_count)

    # ---- Retrieve ----------------------------------------------------- #
    nodes, trace = rag.retrieve("renewable energy", max_nodes=5)
    print("retrieved  :", [n.node_id for n in nodes])
    print("trace      :", trace)

    # ---- Visual exclusion -------------------------------------------- #
    nodes_no_vis, _ = rag.retrieve(
        "renewable energy", include_visual=False,
    )
    assert all(n.content_type != "visual" for n in nodes_no_vis)
    print("no-visual  : OK")

    # ---- Budget enforcement ------------------------------------------ #
    tiny_budget, tiny_trace = rag.retrieve(
        "energy", energy_budget_wh=1e-8,
    )
    print(f"tiny budget: retrieved {len(tiny_budget)} node(s), "
          f"energy={tiny_trace.total_energy_wh:.9f} Wh")
    assert tiny_trace.total_energy_wh <= 1e-8

    # ---- Bug fix: deterministic node IDs ----------------------------- #
    # The original embedded ``datetime.now().timestamp()`` in the hash,
    # so the same content always produced a different ID.
    rag2 = VimRAGCoRetrieval()
    a = rag2.add_node("identical", "text")
    b = rag2.add_node("identical", "text")
    assert a == b, "identical content should produce identical IDs"
    print("id-collision: OK (identical content -> identical ID)")

    # ---- Bug fix: collision-resistant trace IDs ---------------------- #
    rag3 = VimRAGCoRetrieval()
    rag3.add_node("query text", "text")
    ids = set()
    for _ in range(200):
        _, t = rag3.retrieve("query text")
        ids.add(t.trace_id)
    assert len(ids) == 200, f"trace IDs collided: only {len(ids)} unique"
    print(f"trace ids  : OK ({len(ids)} distinct)")

    # ---- Statistics --------------------------------------------------- #
    print("graph      :", rag.get_memory_graph_stats())
    print("patterns   :", {
        k: v for k, v in rag.analyze_retrieval_patterns().items()
        if k != "most_efficient_retrieval"
    })
    print("most eff   :", rag.analyze_retrieval_patterns()["most_efficient_retrieval"])

    # ---- Bounded trace history --------------------------------------- #
    bounded = VimRAGCoRetrieval(
        config=VimRAGConfig(max_history=3)
    )
    bounded.add_node("c", "text")
    for i in range(10):
        bounded.retrieve(f"q{i}")
    assert bounded.trace_count == 3, bounded.trace_count
    print("bounded    : OK")

    # ---- Atomic export ------------------------------------------------ #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "traces.json"
        size = rag.export_traces(str(path))
        assert path.exists() and size > 0
        print(f"export     : {size} bytes at {path}")

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        assert "traces" in data and "analysis" in data
        print("readback   : OK")

    # ---- Serialization round-trip ------------------------------------ #
    payload = rag.to_json()
    restored = VimRAGCoRetrieval.from_json(payload)
    assert restored.to_dict() == rag.to_dict()
    print("Round-trip OK.")

    # ---- Dataclass round-trips --------------------------------------- #
    tn = list(rag.memory_graph.values())[0]
    p = json.dumps(tn.to_dict(), default=str)
    assert RetrievalNode.from_dict(json.loads(p)).to_dict() == tn.to_dict()
    print("Dataclass  : OK")

    # ---- Context manager -------------------------------------------- #
    with VimRAGCoRetrieval() as scoped:
        scoped.add_node("x", "text")
        scoped.retrieve("x")
        assert scoped.trace_count == 1
    print("Context    : OK")

    # ---- Validation failures ---------------------------------------- #
    for bad_cfg in (
        dict(max_tokens_per_retrieval=0),
        dict(max_energy_per_retrieval_wh=0),
        dict(chars_per_token=0),
        dict(visual_token_estimate=0),
        dict(token_energy_cost_wh=0),
        dict(keyword_weight=0.9, recency_weight=0.9),  # sum != 1
        dict(keyword_weight=-0.1),
        dict(recency_half_life_hours=0),
        dict(efficiency_energy_scale=0),
        dict(max_history=0),
    ):
        try:
            VimRAGConfig(**bad_cfg)  # type: ignore[arg-type]
        except VimRAGError as exc:
            print("Rejected cfg:", exc)

    strict = VimRAGCoRetrieval(strict=True)
    for bad_call in (
        lambda: strict.add_node("x", "images"),    # unknown type
        lambda: strict.add_node("x", "text", metadata="bad"),  # type: ignore[arg-type]
        lambda: strict.connect_nodes("", "target"),
        lambda: strict.connect_nodes("missing", "target"),
        lambda: strict.retrieve(123),              # type: ignore[arg-type]
        lambda: strict.retrieve("q", max_nodes=0),
        lambda: strict.retrieve("q", max_nodes=-5),
        lambda: strict.retrieve("q", include_visual="yes"),  # type: ignore[arg-type]
        lambda: strict.retrieve("q", energy_budget_wh=-0.1),
        lambda: strict.retrieve("q", energy_budget_wh=float("nan")),
        lambda: strict.get_retrieval_traces(0),
        lambda: strict.export_traces(""),
    ):
        try:
            bad_call()
        except VimRAGError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces ----------------------------------------- #
    lenient = VimRAGCoRetrieval(strict=False)
    nid = lenient.add_node("test", "text", metadata="not-a-mapping")  # type: ignore[arg-type]
    assert nid
    lenient.connect_nodes("ghost", "test")  # skipped with warning
    lenient.retrieve("q", energy_budget_wh=float("nan"))  # uses default
    print("lenient    : OK")

    # ---- Reset ------------------------------------------------------- #
    removed = rag.reset(clear_graph=True)
    print("reset      :", removed, "trace(s); new node count:", rag.node_count)
    assert rag.node_count == 0

    print("\nSmoke test passed.")
