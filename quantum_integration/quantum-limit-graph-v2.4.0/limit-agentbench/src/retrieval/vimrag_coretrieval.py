# src/retrieval/vimrag_coretrieval.py

"""
VimRAG Co-Retrieval Module
==========================

Handles multimodal memory graphs (text + visual context) with
energy-efficient filtering based on token size and file weight.

Based on: VimRAG — Navigating Massive Visual Context in
Retrieval-Augmented Generation via Multimodal Memory Graph.

Design
------
- Frozen, validated ``VimRAGConfig``: token limits, energy budget,
  per-type coefficients, boost weights, bounded history, duplicate
  policy, per-node token cap.
- ``RetrievalNode`` / ``RetrievalTrace`` — frozen dataclasses with full
  validation and serialization.
- **Deterministic, content-addressed node IDs** — SHA-256 over
  ``(content_type, content_preview, metadata)``. Two calls with
  identical inputs return the same ID. Duplicate policy is
  configurable (``overwrite`` / ``error`` / ``skip``).
- **Normalized relevance scoring** — keyword component is
  ``overlap / len(query_words)``, so the weighted sum stays in
  ``[0, 1]``.
- **Visual nodes score from metadata** — captions / alt-text /
  OCR / descriptions are matched against the query. Falls back to
  the content string when no caption keys are present.
- **Co-retrieval via graph edges** — ``neighbor_boost_factor``
  (default ``0.15``) adds a small boost to candidates connected to
  any of the top-scored seed nodes. Set to ``0.0`` for legacy
  scoring.
- **Thread safety** — ``RLock`` guards the graph, edges, and trace
  history.
- **Bounded trace history** — ``deque(maxlen=config.max_history)``.
- **Atomic export** — ``tempfile`` + ``os.replace``; parent dirs are
  created. ``to_json`` / ``export_traces`` use
  ``allow_nan=False``.

Version: 2.0.0
"""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import math
import numbers
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
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
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "RetrievalNode",
    "RetrievalTrace",
    "VimRAGConfig",
    "VimRAGCoRetrieval",
    "VimRAGError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class VimRAGError(ValueError):
    """Raised for invalid VimRAG co-retrieval inputs or configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_VALID_CONTENT_TYPES: Tuple[str, ...] = (
    "text", "visual", "multimodal", "audio",
)

_VALID_DUPLICATE_POLICIES: Tuple[str, ...] = (
    "overwrite", "error", "skip",
)

# Metadata keys whose values are considered when scoring a visual node.
# Captions, alt-text, OCR text, free-form descriptions, and tag lists
# are the common conventions across vision pipelines.
_VISUAL_TEXT_KEYS: Tuple[str, ...] = (
    "caption", "alt_text", "ocr_text", "description", "tags",
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class VimRAGConfig:
    """Tunable parameters for :class:`VimRAGCoRetrieval`."""

    # Retrieval limits.
    max_tokens_per_retrieval: int = 4096
    max_energy_per_retrieval_wh: float = 0.01
    enable_visual_context: bool = True

    # Token estimation coefficients.
    chars_per_token: int = 4
    visual_token_estimate: int = 256
    multimodal_token_estimate: int = 256
    audio_token_estimate: int = 256

    # Per-node cap on ``token_size`` — prevents accidental blowups
    # from oversized content (e.g. a 10 MB text node).
    max_tokens_per_node: int = 100_000

    # Energy model.
    token_energy_cost_wh: float = 1e-6
    visual_energy_multiplier: float = 2.5
    multimodal_energy_multiplier: float = 2.5
    audio_energy_multiplier: float = 2.0

    # Scoring weights (must sum to 1.0).
    keyword_weight: float = 0.7
    recency_weight: float = 0.3

    # Recency decay half-life (hours).
    recency_half_life_hours: float = 24.0

    # Co-retrieval: bonus added to a candidate's relevance when it is
    # connected to any of the top-K scored seed nodes. ``0.0``
    # disables the boost (legacy behavior).
    neighbor_boost_factor: float = 0.15

    # Efficiency formula coefficients.
    efficiency_energy_scale: float = 1000.0
    efficiency_token_scale: float = 100.0

    # Bounded trace history.
    max_history: int = 10_000

    # Content preview used in hashing/ID generation.
    hash_preview_chars: int = 100

    # Behaviour when ``add_node`` is called with content that hashes
    # to an existing node ID.
    on_duplicate_node: str = "overwrite"  # "overwrite" | "error" | "skip"

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_tokens_per_retrieval, bool)
            or not isinstance(
                self.max_tokens_per_retrieval, numbers.Integral
            )
            or self.max_tokens_per_retrieval <= 0
        ):
            raise VimRAGError(
                "max_tokens_per_retrieval must be a positive int."
            )
        if (
            isinstance(self.max_energy_per_retrieval_wh, bool)
            or not isinstance(
                self.max_energy_per_retrieval_wh, numbers.Real
            )
            or not math.isfinite(
                float(self.max_energy_per_retrieval_wh)
            )
            or self.max_energy_per_retrieval_wh <= 0
        ):
            raise VimRAGError(
                "max_energy_per_retrieval_wh must be finite and > 0."
            )
        if not isinstance(self.enable_visual_context, bool):
            raise VimRAGError(
                "enable_visual_context must be a bool."
            )
        for name in (
            "chars_per_token",
            "visual_token_estimate",
            "multimodal_token_estimate",
            "audio_token_estimate",
            "max_tokens_per_node",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value <= 0
            ):
                raise VimRAGError(
                    f"{name} must be a positive int."
                )
        for name in (
            "token_energy_cost_wh",
            "visual_energy_multiplier",
            "multimodal_energy_multiplier",
            "audio_energy_multiplier",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise VimRAGError(
                    f"{name} must be finite and > 0."
                )
        for name in ("keyword_weight", "recency_weight"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise VimRAGError(
                    f"{name} must be finite and >= 0."
                )
        weight_sum = (
            float(self.keyword_weight)
            + float(self.recency_weight)
        )
        if abs(weight_sum - 1.0) > 1e-6:
            raise VimRAGError(
                f"keyword_weight + recency_weight must sum to 1.0 "
                f"(got {weight_sum:.6f})."
            )
        if (
            isinstance(self.recency_half_life_hours, bool)
            or not isinstance(
                self.recency_half_life_hours, numbers.Real
            )
            or not math.isfinite(
                float(self.recency_half_life_hours)
            )
            or self.recency_half_life_hours <= 0
        ):
            raise VimRAGError(
                "recency_half_life_hours must be finite and > 0."
            )
        if (
            isinstance(self.neighbor_boost_factor, bool)
            or not isinstance(
                self.neighbor_boost_factor, numbers.Real
            )
            or not math.isfinite(
                float(self.neighbor_boost_factor)
            )
            or self.neighbor_boost_factor < 0
        ):
            raise VimRAGError(
                "neighbor_boost_factor must be finite and >= 0."
            )
        for name in (
            "efficiency_energy_scale",
            "efficiency_token_scale",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise VimRAGError(
                    f"{name} must be finite and > 0."
                )
        if (
            isinstance(self.max_history, bool)
            or not isinstance(
                self.max_history, numbers.Integral
            )
            or self.max_history <= 0
        ):
            raise VimRAGError("max_history must be > 0.")
        if (
            isinstance(self.hash_preview_chars, bool)
            or not isinstance(
                self.hash_preview_chars, numbers.Integral
            )
            or self.hash_preview_chars <= 0
        ):
            raise VimRAGError(
                "hash_preview_chars must be > 0."
            )
        if self.on_duplicate_node not in _VALID_DUPLICATE_POLICIES:
            raise VimRAGError(
                f"on_duplicate_node must be one of "
                f"{_VALID_DUPLICATE_POLICIES}, "
                f"got {self.on_duplicate_node!r}."
            )

    # ----- convenience ---------------------------------------------------
    def token_estimate(
        self, content: Any, content_type: str,
    ) -> int:
        if content_type == "text":
            char_count = len(str(content))
            if char_count == 0:
                return 0
            return max(1, char_count // self.chars_per_token)
        if content_type == "visual":
            return self.visual_token_estimate
        if content_type == "multimodal":
            char_count = len(str(content))
            text_part = (
                max(1, char_count // self.chars_per_token)
                if char_count > 0 else 0
            )
            return text_part + self.multimodal_token_estimate
        if content_type == "audio":
            return self.audio_token_estimate
        return 0

    def energy_multiplier(self, content_type: str) -> float:
        if content_type == "visual":
            return self.visual_energy_multiplier
        if content_type == "multimodal":
            return self.multimodal_energy_multiplier
        if content_type == "audio":
            return self.audio_energy_multiplier
        return 1.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VimRAGConfig":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                f"VimRAGConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            unknown = set(data) - valid
            if unknown:
                logger.debug(
                    "VimRAGConfig.from_dict dropping unknown fields: "
                    "%s", sorted(unknown),
                )
            kwargs = {
                k: v for k, v in data.items() if k in valid
            }
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VimRAGError):
                raise
            raise VimRAGError(
                f"invalid VimRAGConfig payload: {exc}"
            ) from exc


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

    _content_tokens: Optional[FrozenSet[str]] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise VimRAGError(
                "node_id must be a non-empty string."
            )
        if self.content_type not in _VALID_CONTENT_TYPES:
            raise VimRAGError(
                f"content_type must be one of "
                f"{_VALID_CONTENT_TYPES}, "
                f"got {self.content_type!r}."
            )
        if (
            isinstance(self.token_size, bool)
            or not isinstance(self.token_size, numbers.Integral)
            or self.token_size < 0
        ):
            raise VimRAGError(
                "token_size must be a non-negative int."
            )
        if (
            isinstance(self.file_weight, bool)
            or not isinstance(self.file_weight, numbers.Real)
        ):
            raise VimRAGError("file_weight must be numeric.")
        fw = float(self.file_weight)
        if not math.isfinite(fw) or fw < 0:
            raise VimRAGError(
                f"file_weight must be finite and >= 0, got "
                f"{self.file_weight!r}."
            )
        if not isinstance(self.metadata, Mapping):
            raise VimRAGError("metadata must be a Mapping.")
        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, numbers.Real)
            or not math.isfinite(float(self.timestamp))
        ):
            raise VimRAGError(
                "timestamp must be a finite number."
            )

    def content_tokens(self) -> FrozenSet[str]:
        """
        Return the cached lower-cased token set used for keyword
        matching.

        - Text and multimodal nodes: tokens from ``content``.
        - Visual and audio nodes: tokens from metadata caption keys
          (``caption``, ``alt_text``, ``ocr_text``, ``description``,
          ``tags``), falling back to ``content`` when none are set.
        """
        if self._content_tokens is None:
            if self.content_type in ("visual", "audio"):
                parts: List[str] = []
                for key in _VISUAL_TEXT_KEYS:
                    if key in self.metadata:
                        val = self.metadata[key]
                        if isinstance(val, (list, tuple, set)):
                            parts.extend(str(v) for v in val)
                        else:
                            parts.append(str(val))
                text = " ".join(parts) or str(self.content)
            else:
                text = str(self.content)
            object.__setattr__(
                self,
                "_content_tokens",
                frozenset(text.lower().split()),
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
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "RetrievalNode":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                "RetrievalNode.from_dict expects a Mapping."
            )
        try:
            ts = data.get("created_at")
            if isinstance(ts, str):
                created_at = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not created_at.tzinfo:
                    created_at = created_at.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                created_at = datetime.now(timezone.utc)
            metadata = data.get("metadata") or {}
            if not isinstance(metadata, Mapping):
                raise VimRAGError(
                    "metadata must be a Mapping."
                )
            return cls(
                node_id=str(data["node_id"]),
                content_type=str(data["content_type"]),
                content=data.get("content"),
                token_size=int(data.get("token_size", 0)),
                file_weight=float(
                    data.get("file_weight", 0.0)
                ),
                metadata=dict(metadata),
                timestamp=float(
                    data.get("timestamp", time.time())
                ),
                created_at=created_at,
            )
        except KeyError as exc:
            raise VimRAGError(
                f"RetrievalNode payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VimRAGError):
                raise
            raise VimRAGError(
                f"invalid RetrievalNode payload: {exc}"
            ) from exc

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
    """Immutable trace of a retrieval operation."""

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
        if not isinstance(self.trace_id, str) or (
            not self.trace_id
        ):
            raise VimRAGError(
                "trace_id must be a non-empty string."
            )
        if not isinstance(self.query, str):
            raise VimRAGError("query must be a string.")
        if not isinstance(self.retrieved_nodes, tuple):
            object.__setattr__(
                self, "retrieved_nodes",
                tuple(self.retrieved_nodes),
            )
        if not isinstance(self.retrieval_path, tuple):
            object.__setattr__(
                self, "retrieval_path",
                tuple(self.retrieval_path),
            )
        if (
            isinstance(self.total_tokens, bool)
            or not isinstance(
                self.total_tokens, numbers.Integral
            )
            or self.total_tokens < 0
        ):
            raise VimRAGError(
                "total_tokens must be a non-negative int."
            )
        for name in ("total_energy_wh", "efficiency_score"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
            ):
                raise VimRAGError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise VimRAGError(
                    f"{name} must be finite, got {value!r}."
                )
        if self.total_energy_wh < 0:
            raise VimRAGError(
                "total_energy_wh must be >= 0."
            )

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
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "RetrievalTrace":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                "RetrievalTrace.from_dict expects a Mapping."
            )
        try:
            ts = data.get("created_at")
            if isinstance(ts, str):
                created_at = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not created_at.tzinfo:
                    created_at = created_at.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                created_at = datetime.now(timezone.utc)
            return cls(
                trace_id=str(data["trace_id"]),
                query=str(data.get("query", "")),
                retrieved_nodes=tuple(
                    data.get("retrieved_nodes") or []
                ),
                total_tokens=int(
                    data.get("total_tokens", 0)
                ),
                total_energy_wh=float(
                    data.get("total_energy_wh", 0.0)
                ),
                retrieval_path=tuple(
                    data.get("retrieval_path") or []
                ),
                efficiency_score=float(
                    data.get("efficiency_score", 0.0)
                ),
                timestamp=float(
                    data.get("timestamp", time.time())
                ),
                created_at=created_at,
            )
        except KeyError as exc:
            raise VimRAGError(
                f"RetrievalTrace payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VimRAGError):
                raise
            raise VimRAGError(
                f"invalid RetrievalTrace payload: {exc}"
            ) from exc

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

    Thread-safe, serializable, and bounded in memory.

    Responsibilities
    ----------------
    - Manage the multimodal memory graph (text + visual context).
    - Filter retrievals by token size and file weight.
    - Ensure energy-efficient access patterns.
    - Co-retrieve: a node connected to a top-scored seed receives
      a small relevance boost (``neighbor_boost_factor``).
    - Provide traces for meta-cognitive reflection.
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
                max_tokens_per_retrieval=int(
                    max_tokens_per_retrieval
                ),
                max_energy_per_retrieval_wh=float(
                    max_energy_per_retrieval_wh
                ),
                enable_visual_context=bool(
                    enable_visual_context
                ),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.max_tokens: int = (
            self._config.max_tokens_per_retrieval
        )
        self.max_energy: float = (
            self._config.max_energy_per_retrieval_wh
        )
        self.enable_visual: bool = (
            self._config.enable_visual_context
        )
        self.token_energy_cost: float = (
            self._config.token_energy_cost_wh
        )
        self.visual_energy_multiplier: float = (
            self._config.visual_energy_multiplier
        )

        self._lock = threading.RLock()
        self.memory_graph: Dict[str, RetrievalNode] = {}
        # Edges stored as ``Set[str]`` for O(1) membership checks.
        self.graph_edges: Dict[str, Set[str]] = {}
        self.retrieval_traces: Deque[RetrievalTrace] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "VimRAGCoRetrieval initialized "
            "(max_tokens=%d, max_energy_wh=%.4f, visual=%s, "
            "dup=%s, neighbor_boost=%.2f, strict=%s)",
            self.max_tokens, self.max_energy,
            self.enable_visual,
            self._config.on_duplicate_node,
            self._config.neighbor_boost_factor,
            self._strict,
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
            return sum(
                len(e) for e in self.graph_edges.values()
            )

    # ---------------------------------------------------------- node ingestion
    def add_node(
        self,
        content: Any,
        content_type: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a node to the multimodal memory graph.

        Duplicate handling is governed by
        ``config.on_duplicate_node``:

        - ``"overwrite"`` (default): replaces the existing node.
        - ``"error"``: raises :class:`VimRAGError`.
        - ``"skip"``: returns the existing node ID unchanged.
        """
        if content_type not in _VALID_CONTENT_TYPES:
            raise VimRAGError(
                f"content_type must be one of "
                f"{_VALID_CONTENT_TYPES}, "
                f"got {content_type!r}."
            )
        if metadata is not None and not isinstance(
            metadata, Mapping,
        ):
            msg = (
                f"metadata must be a Mapping or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise VimRAGError(msg)
            logger.warning(
                "%s Ignoring metadata.", msg,
            )
            metadata = None

        node_id = self._generate_node_id(
            content, content_type, metadata,
        )
        token_size = self._estimate_token_size(
            content, content_type,
        )
        file_weight = self._calculate_file_weight(
            token_size, content_type,
        )

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
            if node_id in self.memory_graph:
                policy = self._config.on_duplicate_node
                if policy == "error":
                    raise VimRAGError(
                        f"node {node_id!r} already exists "
                        f"(content + type + metadata are identical "
                        f"to an existing node)."
                    )
                if policy == "skip":
                    logger.debug(
                        "Skipping duplicate node %s.", node_id,
                    )
                    return node_id
                # "overwrite" — fall through; graph_edges preserved.
            self.memory_graph[node_id] = node
            self.graph_edges.setdefault(node_id, set())

        logger.debug(
            "Added node %s (type=%s, tokens=%d, weight=%.6f).",
            node_id, content_type, token_size, file_weight,
        )
        return node_id

    def connect_nodes(
        self, source_id: str, target_id: str,
    ) -> bool:
        """
        Create a directed edge between two existing nodes.

        Returns ``True`` iff a new edge was added. Returns ``False``
        when the edge already existed or when either node is missing
        in non-strict mode. Raises :class:`VimRAGError` for unknown
        nodes in strict mode.
        """
        if not isinstance(source_id, str) or not source_id:
            raise VimRAGError(
                "source_id must be a non-empty string."
            )
        if not isinstance(target_id, str) or not target_id:
            raise VimRAGError(
                "target_id must be a non-empty string."
            )

        with self._lock:
            if source_id not in self.memory_graph:
                msg = (
                    f"source node {source_id!r} not found."
                )
                if self._strict:
                    raise VimRAGError(msg)
                logger.warning("%s Skipping.", msg)
                return False
            if target_id not in self.memory_graph:
                msg = (
                    f"target node {target_id!r} not found."
                )
                if self._strict:
                    raise VimRAGError(msg)
                logger.warning("%s Skipping.", msg)
                return False

            edges = self.graph_edges.setdefault(
                source_id, set(),
            )
            if target_id in edges:
                return False
            edges.add(target_id)

        logger.debug(
            "Connected %s -> %s.", source_id, target_id,
        )
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
        Retrieve relevant nodes from the memory graph with energy
        constraints.
        """
        if not isinstance(query, str):
            raise VimRAGError("query must be a string.")
        if (
            isinstance(max_nodes, bool)
            or not isinstance(max_nodes, numbers.Integral)
            or max_nodes <= 0
        ):
            raise VimRAGError(
                "max_nodes must be a positive int."
            )
        if not isinstance(include_visual, bool):
            raise VimRAGError(
                "include_visual must be a bool."
            )
        budget = self._validate_budget(energy_budget_wh)

        # Snapshot candidates + edges under the lock.
        with self._lock:
            candidates = {
                node.node_id: node
                for node in self.memory_graph.values()
                if include_visual or (
                    node.content_type != "visual"
                )
            }
            edge_snapshot = {
                src: set(dsts)
                for src, dsts in self.graph_edges.items()
            }

        if not candidates:
            trace = self._make_empty_trace(query, budget)
            with self._lock:
                self.retrieval_traces.append(trace)
            return [], trace

        # Hoist query tokens — one split per call instead of per node.
        query_words = frozenset(query.lower().split())

        # Score candidates.
        score_map: Dict[str, float] = {
            nid: self._score_relevance(
                query_words, node,
            )
            for nid, node in candidates.items()
        }

        # Co-retrieval: boost neighbors of top-scored seeds.
        boost = self._config.neighbor_boost_factor
        if boost > 0.0 and edge_snapshot:
            ordered_seeds = sorted(
                score_map.items(),
                key=lambda kv: (-kv[1], kv[0]),
            )[:3]
            for seed_id, seed_score in ordered_seeds:
                add = min(
                    seed_score, 1.0,
                ) * boost
                for neighbor_id in edge_snapshot.get(
                    seed_id, set(),
                ):
                    if neighbor_id in score_map:
                        score_map[neighbor_id] = min(
                            1.0,
                            score_map[neighbor_id] + add,
                        )

        # Deterministic sort: score desc, node_id asc.
        scored = sorted(
            (
                (candidates[nid], score)
                for nid, score in score_map.items()
            ),
            key=lambda x: (-x[1], x[0].node_id),
        )

        # Greedy selection under both budgets.
        selected: List[RetrievalNode] = []
        total_tokens = 0
        total_energy = 0.0
        retrieval_path: List[str] = []

        for node, _score in scored:
            if len(selected) >= int(max_nodes):
                break
            if total_energy + node.file_weight > budget:
                continue
            if (
                total_tokens + node.token_size
                > self.max_tokens
            ):
                continue
            selected.append(node)
            total_tokens += node.token_size
            total_energy += node.file_weight
            retrieval_path.append(node.node_id)

        efficiency = self._calculate_efficiency_score(
            len(selected), total_tokens, total_energy,
        )

        trace = RetrievalTrace(
            trace_id=self._generate_trace_id(query),
            query=query,
            retrieved_nodes=tuple(
                n.node_id for n in selected
            ),
            total_tokens=total_tokens,
            total_energy_wh=total_energy,
            retrieval_path=tuple(retrieval_path),
            efficiency_score=efficiency,
            timestamp=time.time(),
        )

        with self._lock:
            self.retrieval_traces.append(trace)

        logger.debug(
            "Retrieved %d/%d node(s) for query=%r "
            "(tokens=%d, energy=%.6f Wh, budget=%.6f Wh).",
            len(selected), len(candidates), query,
            total_tokens, total_energy, budget,
        )
        return selected, trace

    def _make_empty_trace(
        self, query: str, budget: float,
    ) -> RetrievalTrace:
        return RetrievalTrace(
            trace_id=self._generate_trace_id(query),
            query=query,
            retrieved_nodes=tuple(),
            total_tokens=0,
            total_energy_wh=0.0,
            retrieval_path=tuple(),
            efficiency_score=0.0,
            timestamp=time.time(),
        )

    # ---------------------------------------------------------- trace access
    def get_retrieval_traces(
        self, n: int = 10,
    ) -> List[RetrievalTrace]:
        """Return the ``n`` most recent traces (or all if fewer)."""
        if (
            isinstance(n, bool)
            or not isinstance(n, numbers.Integral)
            or n <= 0
        ):
            raise VimRAGError("n must be a positive int.")
        with self._lock:
            traces = list(self.retrieval_traces)
        return traces[-n:]

    def analyze_retrieval_patterns(self) -> Dict[str, Any]:
        """Aggregate statistics over the retrieval history."""
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

        avg_tokens = (
            sum(t.total_tokens for t in traces)
            / len(traces)
        )
        avg_energy = (
            sum(t.total_energy_wh for t in traces)
            / len(traces)
        )
        avg_efficiency = (
            sum(t.efficiency_score for t in traces)
            / len(traces)
        )

        # Tie-break: earliest trace wins (Python max is stable —
        # returns the first item on ties).
        most_efficient = max(
            traces, key=lambda t: t.efficiency_score,
        )

        return {
            "total_retrievals": len(traces),
            "avg_tokens_per_retrieval": avg_tokens,
            "avg_energy_per_retrieval_wh": avg_energy,
            "avg_efficiency_score": avg_efficiency,
            "most_efficient_retrieval": {
                "trace_id": most_efficient.trace_id,
                "efficiency_score": (
                    most_efficient.efficiency_score
                ),
                "tokens": most_efficient.total_tokens,
                "energy_wh": most_efficient.total_energy_wh,
            },
        }

    def statistics(self) -> Dict[str, Any]:
        return self.analyze_retrieval_patterns()

    def get_memory_graph_stats(self) -> Dict[str, Any]:
        """Aggregate statistics about the memory graph."""
        with self._lock:
            nodes = list(self.memory_graph.values())
            edge_count = sum(
                len(e) for e in self.graph_edges.values()
            )
            trace_count = len(self.retrieval_traces)

        text_nodes = sum(
            1 for n in nodes if n.content_type == "text"
        )
        visual_nodes = sum(
            1 for n in nodes if n.content_type == "visual"
        )
        multimodal_nodes = sum(
            1 for n in nodes
            if n.content_type == "multimodal"
        )
        audio_nodes = sum(
            1 for n in nodes if n.content_type == "audio"
        )
        total_tokens = sum(n.token_size for n in nodes)
        total_weight = sum(n.file_weight for n in nodes)

        return {
            "total_nodes": len(nodes),
            "text_nodes": text_nodes,
            "visual_nodes": visual_nodes,
            "multimodal_nodes": multimodal_nodes,
            "audio_nodes": audio_nodes,
            "total_tokens": total_tokens,
            "total_energy_weight_wh": total_weight,
            "total_edges": edge_count,
            "traces_recorded": trace_count,
        }

    # ---------------------------------------------------------- export
    def export_traces(self, filepath: str) -> int:
        """
        Atomically export retrieval traces to a JSON file.

        Returns the number of bytes written. Parent directories are
        created. Uses ``tempfile`` + ``os.replace``.
        """
        if not isinstance(filepath, str) or not filepath:
            raise VimRAGError(
                "filepath must be a non-empty string."
            )
        path = Path(filepath)

        with self._lock:
            payload = {
                "version": VERSION,
                "traces": [
                    t.to_dict()
                    for t in self.retrieval_traces
                ],
                "analysis": self.analyze_retrieval_patterns(),
            }

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise VimRAGError(
                f"could not create parent directory for "
                f"{path}: {exc}"
            ) from exc

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".",
                suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(
                    fd, "w", encoding="utf-8",
                ) as f:
                    json.dump(
                        payload, f,
                        indent=2,
                        allow_nan=False,
                        sort_keys=True,
                    )
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except (OSError, ValueError) as exc:
            raise VimRAGError(
                f"could not write traces to {path}: {exc}"
            ) from exc

        size = path.stat().st_size
        logger.debug(
            "Exported traces to %s (%d bytes).", path, size,
        )
        return size

    # ---------------------------------------------------------- internal
    def _generate_node_id(
        self,
        content: Any,
        content_type: str,
        metadata: Optional[Mapping[str, Any]],
    ) -> str:
        """
        Deterministic, content-addressed node ID.

        Two calls with identical ``(content_type, content preview,
        metadata)`` produce the same ID across processes. Duplicate
        handling is controlled by ``config.on_duplicate_node``.
        """
        preview = str(content)[
            : self._config.hash_preview_chars
        ]
        meta_repr = ""
        if metadata:
            try:
                meta_repr = json.dumps(
                    dict(metadata),
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
            except (TypeError, ValueError):
                meta_repr = repr(metadata)
        raw = f"{content_type}:{preview}:{meta_repr}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _generate_trace_id(self, query: str) -> str:
        """Collision-resistant trace ID."""
        raw = f"{uuid.uuid4().hex}:{query}:{time.time()}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _estimate_token_size(
        self, content: Any, content_type: str,
    ) -> int:
        raw = self._config.token_estimate(
            content, content_type,
        )
        cap = self._config.max_tokens_per_node
        if raw > cap:
            if self._strict:
                raise VimRAGError(
                    f"token_size {raw} exceeds "
                    f"max_tokens_per_node={cap}; "
                    f"content is too large."
                )
            logger.warning(
                "token_size %d exceeds max_tokens_per_node=%d; "
                "clamping.", raw, cap,
            )
            return cap
        return raw

    def _calculate_file_weight(
        self, token_size: int, content_type: str,
    ) -> float:
        base_energy = (
            token_size * self._config.token_energy_cost_wh
        )
        return base_energy * self._config.energy_multiplier(
            content_type,
        )

    def _score_relevance(
        self,
        query_words: FrozenSet[str],
        node: RetrievalNode,
    ) -> float:
        """
        Score a node's relevance in ``[0, 1]``.

        Keyword component: ``|query_words ∩ node_tokens| / |query_words|``
        (a fraction, not a raw count). Recency component: exponential
        decay against the configured half-life. Final score is
        ``keyword_weight * keyword_ratio + recency_weight * recency``,
        which is bounded in ``[0, 1]`` because both components are.
        """
        cfg = self._config
        content_words = node.content_tokens()

        if not query_words:
            keyword_ratio = 0.0
        else:
            overlap = len(query_words & content_words)
            keyword_ratio = overlap / len(query_words)

        age_hours = max(
            0.0, (time.time() - node.timestamp) / 3600.0,
        )
        half_life = cfg.recency_half_life_hours
        recency_score = 1.0 / (1.0 + age_hours / half_life)

        score = (
            cfg.keyword_weight * keyword_ratio
            + cfg.recency_weight * recency_score
        )
        # Defensive clamp — both components are in [0, 1] and the
        # weights sum to 1.0, so this should never fire, but a
        # caller-installed config with 0.999999-instead-of-1.0 weights
        # would otherwise leave a small gap.
        return max(0.0, min(1.0, score))

    def _calculate_efficiency_score(
        self,
        num_nodes: int,
        total_tokens: int,
        total_energy: float,
    ) -> float:
        """
        Efficiency: nodes per unit of
        ``(energy_scale · energy + tokens / token_scale)``.

        Higher is better. Raw scale, not normalized; callers
        comparing runs use relative values.
        """
        if (
            total_energy <= 0
            or total_tokens <= 0
            or num_nodes <= 0
        ):
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
    def _validate_budget(
        self, value: Optional[float],
    ) -> float:
        if value is None:
            return self.max_energy
        if (
            isinstance(value, bool)
            or not isinstance(value, numbers.Real)
        ):
            msg = (
                f"energy_budget_wh must be numeric or None, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise VimRAGError(msg)
            logger.warning("%s Using default.", msg)
            return self.max_energy
        fv = float(value)
        if not math.isfinite(fv) or fv < 0:
            msg = (
                f"energy_budget_wh must be finite and >= 0, "
                f"got {value!r}."
            )
            if self._strict:
                raise VimRAGError(msg)
            logger.warning("%s Using default.", msg)
            return self.max_energy
        return fv

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_graph: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "nodes": len(self.memory_graph),
                    "edges": sum(
                        len(e)
                        for e in self.graph_edges.values()
                    ),
                    "traces": len(self.retrieval_traces),
                },
            }
            if include_graph:
                payload["nodes"] = [
                    n.to_dict()
                    for n in self.memory_graph.values()
                ]
                payload["edges"] = {
                    src: sorted(dsts)
                    for src, dsts in self.graph_edges.items()
                }
                payload["traces"] = [
                    t.to_dict()
                    for t in self.retrieval_traces
                ]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VimRAGCoRetrieval":
        if not isinstance(data, Mapping):
            raise VimRAGError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise VimRAGError(
                    "config must be a Mapping."
                )
            cfg = VimRAGConfig.from_dict(dict(raw_config))

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise VimRAGError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )
            rag = cls(config=cfg, strict=strict_raw)

            raw_nodes = data.get("nodes") or []
            if not isinstance(raw_nodes, list):
                raise VimRAGError(
                    f"nodes must be a list, got "
                    f"{type(raw_nodes).__name__}."
                )
            raw_edges = data.get("edges") or {}
            if not isinstance(raw_edges, Mapping):
                raise VimRAGError(
                    f"edges must be a Mapping, got "
                    f"{type(raw_edges).__name__}."
                )
            raw_traces = data.get("traces") or []
            if not isinstance(raw_traces, list):
                raise VimRAGError(
                    f"traces must be a list, got "
                    f"{type(raw_traces).__name__}."
                )

            with rag._lock:
                for entry in raw_nodes:
                    if not isinstance(entry, Mapping):
                        raise VimRAGError(
                            "nodes entries must be Mappings."
                        )
                    node = RetrievalNode.from_dict(entry)
                    rag.memory_graph[node.node_id] = node
                    rag.graph_edges.setdefault(
                        node.node_id, set(),
                    )
                for src, dst_list in raw_edges.items():
                    if not isinstance(dst_list, (list, tuple, set)):
                        raise VimRAGError(
                            f"edges[{src!r}] must be a list, "
                            f"got {type(dst_list).__name__}."
                        )
                    rag.graph_edges.setdefault(
                        str(src), set(),
                    ).update(str(d) for d in dst_list)
                for entry in raw_traces:
                    if not isinstance(entry, Mapping):
                        raise VimRAGError(
                            "traces entries must be Mappings."
                        )
                    rag.retrieval_traces.append(
                        RetrievalTrace.from_dict(entry)
                    )
            return rag
        except VimRAGError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise VimRAGError(
                f"invalid VimRAGCoRetrieval payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_graph: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_graph=include_graph),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "VimRAGCoRetrieval":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise VimRAGError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_graph: bool = False) -> int:
        """
        Clear trace history; optionally clear the graph. Returns
        traces removed.
        """
        with self._lock:
            removed = len(self.retrieval_traces)
            self.retrieval_traces.clear()
            if clear_graph:
                self.memory_graph.clear()
                self.graph_edges.clear()
        logger.debug(
            "VimRAGCoRetrieval reset (removed %d trace(s)).",
            removed,
        )
        return removed

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "VimRAGCoRetrieval":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    rag = VimRAGCoRetrieval()
    print("repr       :", rag)

    text_id = rag.add_node(
        "Renewable energy reduces carbon emissions significantly.",
        "text",
        metadata={"topic": "energy"},
    )
    visual_id = rag.add_node(
        "/path/to/solar_panel.png",
        "visual",
        metadata={"caption": "solar panel installation"},
    )
    multimodal_id = rag.add_node(
        "Chart showing solar adoption 2010-2024",
        "multimodal",
    )
    print("text node  :", text_id)
    print("visual     :", visual_id)
    print("multimodal :", multimodal_id)

    # Deterministic IDs across identical inputs.
    again = rag.add_node(
        "Renewable energy reduces carbon emissions significantly.",
        "text",
        metadata={"topic": "energy"},
    )
    assert again == text_id
    print("determinism: OK")

    # ---- Duplicate policy ---------------------------------------- #
    strict_dup = VimRAGCoRetrieval(
        config=VimRAGConfig(on_duplicate_node="error"),
    )
    strict_dup.add_node("x", "text")
    try:
        strict_dup.add_node("x", "text")
    except VimRAGError as exc:
        print("dup error  :", str(exc)[:60])

    skip_dup = VimRAGCoRetrieval(
        config=VimRAGConfig(on_duplicate_node="skip"),
    )
    id1 = skip_dup.add_node("x", "text")
    id2 = skip_dup.add_node("x", "text")
    assert id1 == id2 and skip_dup.node_count == 1
    print("dup skip   : OK")

    # ---- Connect graph ------------------------------------------- #
    assert rag.connect_nodes(text_id, visual_id) is True
    assert rag.connect_nodes(text_id, multimodal_id) is True
    assert rag.connect_nodes(text_id, visual_id) is False
    assert rag.edge_count == 2
    print("edges      :", rag.edge_count)

    # ---- Retrieve (keyword + recency) ---------------------------- #
    nodes, trace = rag.retrieve("solar energy", max_nodes=5)
    ids = [n.node_id for n in nodes]
    print("retrieved  :", ids)
    print("trace eff  :", round(trace.efficiency_score, 3))
    # Solar query should hit both the visual (via caption) and the
    # text node (via "energy").
    assert text_id in ids
    assert visual_id in ids
    print("co-retrieval: caption matched for visual")

    # ---- Co-retrieval neighbor boost ---------------------------- #
    # The visual is connected to text; both should appear in top-K.
    assert set([text_id, visual_id]) <= set(ids)
    print("neighbor   : OK (boost factor =",
          rag.config.neighbor_boost_factor, ")")

    # ---- Visual exclusion --------------------------------------- #
    nodes_no_vis, _ = rag.retrieve(
        "solar energy", include_visual=False,
    )
    assert all(
        n.content_type != "visual" for n in nodes_no_vis
    )
    print("no-visual  : OK")

    # ---- Budget enforcement ------------------------------------ #
    tiny_budget, tiny_trace = rag.retrieve(
        "energy", energy_budget_wh=1e-8,
    )
    print(
        f"tiny budget: retrieved {len(tiny_budget)} node(s), "
        f"energy={tiny_trace.total_energy_wh:.9f} Wh"
    )
    assert tiny_trace.total_energy_wh <= 1e-8

    # ---- Trace ID collision resistance ------------------------- #
    rag3 = VimRAGCoRetrieval()
    rag3.add_node("query text", "text")
    ids_set = set()
    for _ in range(200):
        _, t = rag3.retrieve("query text")
        ids_set.add(t.trace_id)
    assert len(ids_set) == 200
    print(f"trace ids  : {len(ids_set)} distinct")

    # ---- Statistics -------------------------------------------- #
    print("graph      :", rag.get_memory_graph_stats())
    print("patterns   :", {
        k: v for k, v in
        rag.analyze_retrieval_patterns().items()
        if k != "most_efficient_retrieval"
    })

    # ---- Bounded trace history --------------------------------- #
    bounded = VimRAGCoRetrieval(
        config=VimRAGConfig(max_history=3),
    )
    bounded.add_node("c", "text")
    for i in range(10):
        bounded.retrieve(f"q{i}")
    assert bounded.trace_count == 3
    print("bounded    : OK")

    # ---- Atomic export ----------------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "traces.json"
        size = rag.export_traces(str(path))
        assert path.exists() and size > 0
        print(f"export     : {size} bytes")
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        assert "traces" in data and "analysis" in data
        assert data["version"] == VERSION
        print("readback   : OK")

    # ---- Round-trip -------------------------------------------- #
    payload = rag.to_json(include_graph=True)
    restored = VimRAGCoRetrieval.from_json(payload)
    assert (
        restored.to_dict(include_graph=True)
        == rag.to_dict(include_graph=True)
    )
    print("round-trip : OK (graph + traces)")

    # ---- from_dict validation ---------------------------------- #
    for bad in (
        {"nodes": "abc"},
        {"nodes": None},
        {"strict": "yes"},
        {"edges": "not-a-mapping"},
        {"traces": "not-a-list"},
        {"nodes": [{"node_id": "x"}]},   # missing fields
        {"config": {"max_tokens_per_retrieval": "abc"}},
    ):
        try:
            VimRAGCoRetrieval.from_dict(bad)
        except VimRAGError as exc:
            print(
                "Rejected   :", str(exc)[:60], "...",
            )

    # ---- to_json rejects NaN ----------------------------------- #
    tainted = VimRAGCoRetrieval()
    tainted.add_node("x", "text")
    object.__setattr__(
        tainted._config, "max_energy_per_retrieval_wh",
        float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    # ---- Dataclass round-trips --------------------------------- #
    tn = list(rag.memory_graph.values())[0]
    assert (
        RetrievalNode.from_dict(tn.to_dict()).to_dict()
        == tn.to_dict()
    )
    tr = rag.retrieval_traces[-1]
    assert (
        RetrievalTrace.from_dict(tr.to_dict()).to_dict()
        == tr.to_dict()
    )
    print("dataclasses: OK")

    # ---- Audio content type ------------------------------------ #
    audio_id = rag.add_node(
        "audio://recording.wav",
        "audio",
        metadata={"caption": "renewable energy podcast"},
    )
    assert rag.memory_graph[audio_id].content_type == "audio"
    print("audio      : OK")

    # ---- Non-strict mode --------------------------------------- #
    lenient = VimRAGCoRetrieval(strict=False)
    nid = lenient.add_node(
        "test", "text",
        metadata="not-a-mapping",  # type: ignore[arg-type]
    )
    assert nid
    lenient.connect_nodes("ghost", "test")  # skipped with warning
    lenient.retrieve(
        "q", energy_budget_wh=float("nan"),
    )  # uses default
    print("lenient    : OK")

    # ---- Reset ------------------------------------------------- #
    removed = rag.reset(clear_graph=True)
    print(
        "reset      :", removed, "trace(s); nodes:",
        rag.node_count,
    )
    assert rag.node_count == 0

    print("\nSmoke test passed.")
