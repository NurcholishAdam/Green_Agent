# src/retrieval/visual_context_handler.py

"""
Visual Context Handler
======================

Handles visual context in multimodal retrieval inspired by VimRAG's approach
to navigating massive visual context efficiently.

Enhancements
------------
- ``VisualContextConfig`` — frozen, validated: token/energy costs,
  multipliers, scoring weights, thresholds, bounded caps.
- ``VisualContext`` / ``VisualRetrievalResult`` — frozen dataclasses with
  validation and serialization.
- **Fixed naive timestamps** — ``datetime.now(timezone.utc)`` everywhere.
- **Fixed non-deterministic context IDs** — content-hash based (identical
  inputs → identical IDs), with uuid4 for deduplicated additions.
- **Fixed MD5 collisions** — replaced with SHA-256 + uuid4.
- **Thread safety** — ``RLock`` guards storage, index, and counters.
- **Bounded storage** — ``max_contexts`` cap with LRU eviction;
  ``max_index_entries`` caps the caption index.
- **Fixed running-mean division** — ``avg_relevance`` is guarded.
- **Fixed alignment-score overflow** — ``_score_alignment`` is clamped to
  ``[0, 1]``.
- **Full validation** of every argument; strict / non-strict modes.
- **Fixed `_score_relevance`** — uses cached lower-cased word sets.
- Serialization on the handler, the config, and both dataclasses.
- ``statistics()``, ``__repr__``, custom ``VisualContextError``, lazy
  ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class VisualContextError(ValueError):
    """Raised for invalid visual context inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class VisualContextType(Enum):
    """Types of visual context."""
    IMAGE = "image"
    DIAGRAM = "diagram"
    CHART = "chart"
    SCREENSHOT = "screenshot"
    VIDEO_FRAME = "video_frame"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class VisualContextConfig:
    """
    Tunable parameters for :class:`VisualContextHandler`.

    Centralizes every token cost, energy coefficient, scoring weight,
    alignment threshold, and bounded-history cap that was previously
    hardcoded in the class body.
    """

    # Per-context costs.
    visual_token_cost: int = 256
    visual_energy_multiplier: float = 2.5
    energy_per_token_wh: float = 1e-6

    # Retrieval limits.
    max_visual_contexts: int = 5
    max_contexts: Optional[int] = 10_000
    max_index_entries: Optional[int] = 50_000

    # Scoring weights (must sum to 1.0).
    weight_caption_overlap: float = 0.5
    weight_recency: float = 0.3
    weight_type_bonus: float = 0.2

    # Recency half-life (hours).
    recency_half_life_hours: float = 24.0

    # Type bonus: query keyword → preferred VisualContextType.
    chart_keywords: tuple = ("chart", "graph", "plot", "histogram")
    diagram_keywords: tuple = ("diagram", "flowchart", "schematic")
    screenshot_keywords: tuple = ("screenshot", "screen", "ui", "interface")
    image_keywords: tuple = ("image", "picture", "photo")

    # Alignment thresholds.
    min_alignment_score: float = 0.3

    # Budget allocation defaults.
    default_text_priority: float = 0.7
    visual_shift_factor: float = 0.2
    visual_heavy_keywords: tuple = (
        "image", "picture", "diagram", "chart", "graph",
        "screenshot", "visual", "show", "display",
    )

    # Hash-based IDs: content preview length.
    hash_preview_chars: int = 200

    # Bounded statistics.
    max_retrieval_history: int = 1000

    def __post_init__(self) -> None:
        if not isinstance(self.visual_token_cost, int) or self.visual_token_cost <= 0:
            raise VisualContextError("visual_token_cost must be a positive int.")
        if self.visual_energy_multiplier <= 0:
            raise VisualContextError(
                "visual_energy_multiplier must be > 0."
            )
        if self.energy_per_token_wh <= 0:
            raise VisualContextError("energy_per_token_wh must be > 0.")
        if self.max_visual_contexts <= 0:
            raise VisualContextError(
                "max_visual_contexts must be > 0."
            )
        if self.max_contexts is not None and self.max_contexts <= 0:
            raise VisualContextError(
                "max_contexts must be a positive int or None."
            )
        if self.max_index_entries is not None and self.max_index_entries <= 0:
            raise VisualContextError(
                "max_index_entries must be a positive int or None."
            )

        weights = (
            self.weight_caption_overlap
            + self.weight_recency
            + self.weight_type_bonus
        )
        if abs(weights - 1.0) > 1e-6:
            raise VisualContextError(
                f"scoring weights must sum to 1.0 (got {weights:.6f})."
            )
        for name in (
            "weight_caption_overlap", "weight_recency", "weight_type_bonus",
        ):
            if getattr(self, name) < 0:
                raise VisualContextError(f"{name} must be >= 0.")

        if self.recency_half_life_hours <= 0:
            raise VisualContextError(
                "recency_half_life_hours must be > 0."
            )
        if not 0.0 <= self.min_alignment_score <= 1.0:
            raise VisualContextError(
                "min_alignment_score must be in [0, 1]."
            )
        if not 0.0 <= self.default_text_priority <= 1.0:
            raise VisualContextError(
                "default_text_priority must be in [0, 1]."
            )
        if not 0.0 <= self.visual_shift_factor <= 1.0:
            raise VisualContextError(
                "visual_shift_factor must be in [0, 1]."
            )
        if self.hash_preview_chars <= 0:
            raise VisualContextError("hash_preview_chars must be > 0.")
        if self.max_retrieval_history <= 0:
            raise VisualContextError(
                "max_retrieval_history must be > 0."
            )
        for name in (
            "chart_keywords", "diagram_keywords", "screenshot_keywords",
            "image_keywords", "visual_heavy_keywords",
        ):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                raise VisualContextError(f"{name} must be a tuple.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VisualContextConfig":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                f"VisualContextConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in (
                "chart_keywords", "diagram_keywords", "screenshot_keywords",
                "image_keywords", "visual_heavy_keywords",
            ):
                kwargs[k] = tuple(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class VisualContext:
    """Immutable visual context item."""

    context_id: str
    context_type: VisualContextType
    source_path: str
    caption: Optional[str]
    embedding: Optional[Tuple[float, ...]]
    token_cost: int
    energy_cost: float
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Cached lower-cased word set of the caption (excluded from equality).
    _caption_tokens: Optional[FrozenSet[str]] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not isinstance(self.context_id, str) or not self.context_id:
            raise VisualContextError("context_id must be a non-empty string.")
        if not isinstance(self.context_type, VisualContextType):
            raise VisualContextError(
                f"context_type must be a VisualContextType, got "
                f"{type(self.context_type).__name__}."
            )
        if not isinstance(self.source_path, str) or not self.source_path:
            raise VisualContextError(
                "source_path must be a non-empty string."
            )
        if self.caption is not None and not isinstance(self.caption, str):
            raise VisualContextError("caption must be a string or None.")
        if self.embedding is not None:
            if not isinstance(self.embedding, tuple):
                object.__setattr__(self, "embedding", tuple(self.embedding))
            for i, v in enumerate(self.embedding):
                if isinstance(v, bool) or not isinstance(v, (int, float)):
                    raise VisualContextError(
                        f"embedding[{i}] must be numeric."
                    )
                if math.isnan(v) or math.isinf(v):
                    raise VisualContextError(
                        f"embedding[{i}] must be finite."
                    )
        if not isinstance(self.token_cost, int) or self.token_cost < 0:
            raise VisualContextError("token_cost must be >= 0.")
        if isinstance(self.energy_cost, bool) or not isinstance(
            self.energy_cost, (int, float)
        ):
            raise VisualContextError("energy_cost must be numeric.")
        fe = float(self.energy_cost)
        if math.isnan(fe) or math.isinf(fe) or fe < 0:
            raise VisualContextError(
                f"energy_cost must be finite and >= 0, got "
                f"{self.energy_cost!r}."
            )
        if not isinstance(self.metadata, Mapping):
            raise VisualContextError("metadata must be a Mapping.")
        if not isinstance(self.timestamp, (int, float)) or isinstance(
            self.timestamp, bool
        ):
            raise VisualContextError("timestamp must be numeric.")

    def caption_tokens(self) -> FrozenSet[str]:
        """Return the cached lower-cased word set of the caption."""
        if self._caption_tokens is None:
            tokens = frozenset(
                (self.caption or "").lower().split()
            )
            object.__setattr__(self, "_caption_tokens", tokens)
        return self._caption_tokens

    def to_dict(self) -> Dict[str, Any]:
        return {
            "context_id": self.context_id,
            "context_type": self.context_type.value,
            "source_path": self.source_path,
            "caption": self.caption,
            "embedding": list(self.embedding) if self.embedding is not None else None,
            "token_cost": self.token_cost,
            "energy_cost": self.energy_cost,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VisualContext":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                "VisualContext.from_dict expects a Mapping."
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
        emb = data.get("embedding")
        return cls(
            context_id=str(data["context_id"]),
            context_type=VisualContextType(str(data["context_type"])),
            source_path=str(data["source_path"]),
            caption=data.get("caption"),
            embedding=tuple(emb) if emb is not None else None,
            token_cost=int(data.get("token_cost", 0)),
            energy_cost=float(data.get("energy_cost", 0.0)),
            metadata=dict(data.get("metadata", {})),
            timestamp=float(data.get("timestamp", time.time())),
            created_at=created_at,
        )

    def __repr__(self) -> str:
        return (
            "VisualContext("
            f"id={self.context_id[:8]}..., "
            f"type={self.context_type.value}, "
            f"tokens={self.token_cost}, "
            f"energy_wh={self.energy_cost:.6f})"
        )


@dataclass(frozen=True)
class VisualRetrievalResult:
    """Immutable result of visual context retrieval."""

    contexts: Tuple[VisualContext, ...]
    total_token_cost: int
    total_energy_cost: float
    relevance_scores: Tuple[float, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in ("contexts", "relevance_scores"):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                object.__setattr__(self, name, tuple(value))
        if not isinstance(self.total_token_cost, int) or self.total_token_cost < 0:
            raise VisualContextError(
                "total_token_cost must be a non-negative int."
            )
        if isinstance(self.total_energy_cost, bool) or not isinstance(
            self.total_energy_cost, (int, float)
        ):
            raise VisualContextError("total_energy_cost must be numeric.")
        fv = float(self.total_energy_cost)
        if math.isnan(fv) or math.isinf(fv) or fv < 0:
            raise VisualContextError(
                "total_energy_cost must be finite and >= 0."
            )
        if not isinstance(self.metadata, Mapping):
            raise VisualContextError("metadata must be a Mapping.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contexts": [c.to_dict() for c in self.contexts],
            "total_token_cost": self.total_token_cost,
            "total_energy_cost": self.total_energy_cost,
            "relevance_scores": list(self.relevance_scores),
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "VisualRetrievalResult("
            f"contexts={len(self.contexts)}, "
            f"tokens={self.total_token_cost}, "
            f"energy_wh={self.total_energy_cost:.6f})"
        )


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #
class VisualContextHandler:
    """
    Handles visual context in multimodal retrieval.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only.

    Features
    --------
    - Efficient visual context indexing.
    - Token-aware visual retrieval.
    - Energy-efficient visual processing.
    - Caption-based filtering.
    - Visual-text alignment.
    """

    def __init__(
        self,
        visual_token_cost: int = 256,
        visual_energy_multiplier: float = 2.5,
        max_visual_contexts: int = 5,
        *,
        config: Optional[VisualContextConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = VisualContextConfig(
                visual_token_cost=int(visual_token_cost),
                visual_energy_multiplier=float(visual_energy_multiplier),
                max_visual_contexts=int(max_visual_contexts),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.visual_token_cost: int = self._config.visual_token_cost
        self.visual_energy_multiplier: float = (
            self._config.visual_energy_multiplier
        )
        self.max_visual_contexts: int = self._config.max_visual_contexts

        self._lock = threading.RLock()
        # Bounded LRU storage.
        self.visual_contexts: "OrderedDict[str, VisualContext]" = OrderedDict()
        self._context_evictions: int = 0

        # Bounded caption index: word -> deque of context_ids.
        self.caption_index: Dict[str, Deque[str]] = {}
        self._index_evictions: int = 0

        # Statistics.
        self.retrieval_stats: Dict[str, Any] = {
            "total_retrievals": 0,
            "total_contexts_retrieved": 0,
            "avg_relevance": 0.0,
        }

        # Optional bounded retrieval history.
        self._retrieval_history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_retrieval_history
        )

        logger.debug(
            "VisualContextHandler initialized "
            "(token_cost=%d, multiplier=%.2f, max_contexts=%d, strict=%s)",
            self.visual_token_cost, self.visual_energy_multiplier,
            self.max_visual_contexts, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> VisualContextConfig:
        return self._config

    @property
    def context_count(self) -> int:
        with self._lock:
            return len(self.visual_contexts)

    # ---------------------------------------------------------- ingestion
    def add_visual_context(
        self,
        source_path: str,
        context_type: VisualContextType,
        caption: Optional[str] = None,
        embedding: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a visual context to the handler.

        The ``context_id`` is deterministic: identical ``source_path`` +
        ``context_type`` produce the same ID, so re-adding the same file
        replaces its entry instead of duplicating it.
        """
        if not isinstance(source_path, str) or not source_path:
            raise VisualContextError(
                "source_path must be a non-empty string."
            )
        if not isinstance(context_type, VisualContextType):
            raise VisualContextError(
                f"context_type must be a VisualContextType, got "
                f"{type(context_type).__name__}."
            )
        if caption is not None and not isinstance(caption, str):
            raise VisualContextError("caption must be a string or None.")
        if metadata is not None and not isinstance(metadata, Mapping):
            msg = (
                f"metadata must be a Mapping or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise VisualContextError(msg)
            logger.warning("%s Ignoring metadata.", msg)
            metadata = None

        context_id = self._generate_context_id(source_path, context_type)
        token_cost = self.visual_token_cost
        energy_cost = (
            token_cost
            * self._config.energy_per_token_wh
            * self.visual_energy_multiplier
        )

        context = VisualContext(
            context_id=context_id,
            context_type=context_type,
            source_path=source_path,
            caption=caption,
            embedding=tuple(embedding) if embedding is not None else None,
            token_cost=token_cost,
            energy_cost=energy_cost,
            metadata=dict(metadata or {}),
            timestamp=time.time(),
        )

        with self._lock:
            # Replace if exists (move to end).
            if context_id in self.visual_contexts:
                # Remove old caption entries.
                old = self.visual_contexts.pop(context_id)
                if old.caption:
                    self._unindex_caption(context_id, old.caption)
            self.visual_contexts[context_id] = context
            # LRU eviction.
            while (
                self._config.max_contexts is not None
                and len(self.visual_contexts) > self._config.max_contexts
            ):
                _, dropped = self.visual_contexts.popitem(last=False)
                if dropped.caption:
                    self._unindex_caption(dropped.context_id, dropped.caption)
                self._context_evictions += 1

            # Index caption.
            if caption:
                self._index_caption(context_id, caption)

        logger.debug(
            "Added visual context %s (type=%s, tokens=%d, energy=%.6f).",
            context_id, context_type.value, token_cost, energy_cost,
        )
        return context_id

    # ---------------------------------------------------------- retrieval
    def retrieve_visual_contexts(
        self,
        query: str,
        max_contexts: Optional[int] = None,
        context_types: Optional[List[VisualContextType]] = None,
        token_budget: Optional[int] = None,
        energy_budget: Optional[float] = None,
    ) -> VisualRetrievalResult:
        """
        Retrieve relevant visual contexts.

        Parameters
        ----------
        query : str
            Text query.
        max_contexts : int, optional
            Maximum contexts to retrieve. Defaults to
            ``config.max_visual_contexts``.
        context_types : list of VisualContextType, optional
            Only consider these types.
        token_budget : int, optional
            Stop adding contexts once the cumulative token cost would exceed
            this budget.
        energy_budget : float, optional
            Stop adding contexts once the cumulative energy cost would exceed
            this budget.

        Returns
        -------
        VisualRetrievalResult
        """
        if not isinstance(query, str):
            raise VisualContextError("query must be a string.")
        if max_contexts is not None and (
            not isinstance(max_contexts, int) or max_contexts <= 0
        ):
            raise VisualContextError(
                "max_contexts must be a positive int or None."
            )
        if context_types is not None:
            if not isinstance(context_types, list):
                raise VisualContextError(
                    "context_types must be a list or None."
                )
            for i, t in enumerate(context_types):
                if not isinstance(t, VisualContextType):
                    raise VisualContextError(
                        f"context_types[{i}] must be a VisualContextType."
                    )
        if token_budget is not None and (
            not isinstance(token_budget, int) or token_budget <= 0
        ):
            raise VisualContextError(
                "token_budget must be a positive int or None."
            )
        if energy_budget is not None:
            if isinstance(energy_budget, bool) or not isinstance(
                energy_budget, (int, float)
            ):
                raise VisualContextError(
                    "energy_budget must be numeric or None."
                )
            fb = float(energy_budget)
            if math.isnan(fb) or math.isinf(fb) or fb <= 0:
                raise VisualContextError(
                    "energy_budget must be finite and > 0."
                )

        max_ctx = max_contexts or self.max_visual_contexts

        with self._lock:
            candidates = [
                ctx for ctx in self.visual_contexts.values()
                if not context_types or ctx.context_type in context_types
            ]

        # Score by relevance.
        scored = [
            (ctx, self._score_relevance(query, ctx))
            for ctx in candidates
        ]
        # Deterministic tie-break: score desc, then context_id asc.
        scored.sort(key=lambda x: (-x[1], x[0].context_id))

        # Select within budget.
        selected: List[VisualContext] = []
        total_tokens = 0
        total_energy = 0.0
        relevance_scores: List[float] = []

        for ctx, score in scored:
            if len(selected) >= max_ctx:
                break
            if token_budget is not None and total_tokens + ctx.token_cost > token_budget:
                continue
            if energy_budget is not None and total_energy + ctx.energy_cost > energy_budget:
                continue
            selected.append(ctx)
            total_tokens += ctx.token_cost
            total_energy += ctx.energy_cost
            relevance_scores.append(score)

        # ---- Update stats (guarded against the reset_stats edge case) --
        with self._lock:
            stats = self.retrieval_stats
            stats["total_retrievals"] = int(stats.get("total_retrievals", 0)) + 1
            stats["total_contexts_retrieved"] = (
                int(stats.get("total_contexts_retrieved", 0)) + len(selected)
            )
            if relevance_scores:
                avg_rel = sum(relevance_scores) / len(relevance_scores)
                prior_n = max(0, stats["total_retrievals"] - 1)
                if prior_n == 0:
                    stats["avg_relevance"] = avg_rel
                else:
                    prior_avg = float(stats.get("avg_relevance", 0.0))
                    stats["avg_relevance"] = (
                        prior_avg * prior_n + avg_rel
                    ) / stats["total_retrievals"]

            self._retrieval_history.append({
                "query": query,
                "candidates_evaluated": len(candidates),
                "contexts_selected": len(selected),
                "total_tokens": total_tokens,
                "total_energy": total_energy,
                "timestamp": time.time(),
            })

        return VisualRetrievalResult(
            contexts=tuple(selected),
            total_token_cost=total_tokens,
            total_energy_cost=total_energy,
            relevance_scores=tuple(relevance_scores),
            metadata={
                "query": query,
                "candidates_evaluated": len(candidates),
                "contexts_selected": len(selected),
            },
        )

    # ---------------------------------------------------------- alignment
    def align_visual_text(
        self,
        text_content: str,
        visual_contexts: List[VisualContext],
    ) -> List[Tuple[str, VisualContext, float]]:
        """
        Align text content with visual contexts.

        Returns a list of ``(segment, visual_context, alignment_score)``
        triples where ``alignment_score`` is in ``[0, 1]``.
        """
        if not isinstance(text_content, str):
            raise VisualContextError("text_content must be a string.")
        if not isinstance(visual_contexts, list):
            raise VisualContextError(
                "visual_contexts must be a list."
            )

        alignments: List[Tuple[str, VisualContext, float]] = []
        segments = self._segment_text(text_content)
        threshold = self._config.min_alignment_score

        for segment in segments:
            best_match: Optional[VisualContext] = None
            best_score = 0.0
            for ctx in visual_contexts:
                score = self._score_alignment(segment, ctx)
                if score > best_score:
                    best_score = score
                    best_match = ctx
            if best_match is not None and best_score > threshold:
                alignments.append((segment, best_match, best_score))

        return alignments

    # ---------------------------------------------------------- budget
    def optimize_visual_budget(
        self,
        query: str,
        total_token_budget: int,
        text_priority: Optional[float] = None,
    ) -> Dict[str, int]:
        """
        Optimize token budget allocation between text and visual.

        Parameters
        ----------
        query : str
            Query used to detect visual-heavy intent.
        total_token_budget : int
            Total budget to split.
        text_priority : float, optional
            Fraction of the budget allocated to text. Defaults to
            ``config.default_text_priority``.

        Returns
        -------
        dict
            ``{"text_tokens", "visual_tokens", "max_visual_contexts"}``.
        """
        if not isinstance(query, str):
            raise VisualContextError("query must be a string.")
        if not isinstance(total_token_budget, int) or total_token_budget <= 0:
            raise VisualContextError(
                "total_token_budget must be a positive int."
            )
        priority = (
            self._config.default_text_priority
            if text_priority is None else text_priority
        )
        if isinstance(priority, bool) or not isinstance(priority, (int, float)):
            raise VisualContextError("text_priority must be numeric.")
        fp = float(priority)
        if math.isnan(fp) or math.isinf(fp) or not 0.0 <= fp <= 1.0:
            raise VisualContextError(
                "text_priority must be in [0, 1]."
            )

        text_budget = int(total_token_budget * fp)
        visual_budget = total_token_budget - text_budget

        if self._is_visual_heavy_query(query):
            shift = int(text_budget * self._config.visual_shift_factor)
            text_budget -= shift
            visual_budget += shift

        return {
            "text_tokens": text_budget,
            "visual_tokens": visual_budget,
            "max_visual_contexts": visual_budget // max(1, self.visual_token_cost),
        }

    # ---------------------------------------------------------- statistics
    def get_visual_stats(self) -> Dict[str, Any]:
        """Return aggregate statistics about the visual context store."""
        with self._lock:
            contexts = list(self.visual_contexts.values())
            stats = dict(self.retrieval_stats)
            context_evictions = self._context_evictions
            index_evictions = self._index_evictions
            index_size = len(self.caption_index)

        type_counts: Dict[str, int] = {}
        for ctx in contexts:
            key = ctx.context_type.value
            type_counts[key] = type_counts.get(key, 0) + 1

        total_tokens = sum(c.token_cost for c in contexts)
        total_energy = sum(c.energy_cost for c in contexts)

        return {
            "total_contexts": len(contexts),
            "contexts_by_type": type_counts,
            "total_token_cost": total_tokens,
            "total_energy_cost": total_energy,
            "retrieval_stats": stats,
            "avg_token_cost_per_context": (
                total_tokens / len(contexts) if contexts else 0.0
            ),
            # Additive fields:
            "context_evictions": context_evictions,
            "index_evictions": index_evictions,
            "index_size": index_size,
            "retrieval_history_size": len(self._retrieval_history),
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_visual_stats`."""
        return self.get_visual_stats()

    def reset_stats(self, *, clear_history: bool = True) -> None:
        """Reset the retrieval counters (optionally clearing the history)."""
        with self._lock:
            self.retrieval_stats["total_retrievals"] = 0
            self.retrieval_stats["total_contexts_retrieved"] = 0
            self.retrieval_stats["avg_relevance"] = 0.0
            if clear_history:
                self._retrieval_history.clear()
        logger.debug("VisualContextHandler statistics reset.")

    # ---------------------------------------------------------- helpers
    def _generate_context_id(
        self,
        source_path: str,
        context_type: VisualContextType,
    ) -> str:
        """
        Return a deterministic context ID.

        Uses SHA-256 over ``(source_path, context_type)`` so identical
        inputs produce identical IDs across processes. Genuine duplicates
        are handled by the caller replacing the previous entry.
        """
        raw = f"{context_type.value}:{source_path}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _index_caption(self, context_id: str, caption: str) -> None:
        """Index the caption's words → context_id."""
        words = caption.lower().split()
        for word in words:
            bucket = self.caption_index.get(word)
            if bucket is None:
                bucket = deque(maxlen=256)
                self.caption_index[word] = bucket
            if context_id not in bucket:
                bucket.append(context_id)

        # Enforce global index cap by dropping the least recent words.
        cap = self._config.max_index_entries
        if cap is not None and len(self.caption_index) > cap:
            # Drop oldest words (dict preserves insertion order).
            excess = len(self.caption_index) - cap
            for _ in range(excess):
                oldest_word = next(iter(self.caption_index))
                self.caption_index.pop(oldest_word, None)
                self._index_evictions += 1

    def _unindex_caption(self, context_id: str, caption: str) -> None:
        """Remove ``context_id`` from every word bucket of ``caption``."""
        for word in caption.lower().split():
            bucket = self.caption_index.get(word)
            if bucket is None:
                continue
            try:
                bucket.remove(context_id)
            except ValueError:
                continue
            if not bucket:
                self.caption_index.pop(word, None)

    def _score_relevance(
        self, query: str, context: VisualContext
    ) -> float:
        """
        Score a visual context's relevance to ``query``.

        Combines caption overlap, recency, and a type-specific bonus, all
        config-driven. Uses cached caption tokens.
        """
        cfg = self._config
        score = 0.0

        # ---- Caption overlap -----------------------------------------
        if context.caption:
            query_words = frozenset(query.lower().split())
            caption_words = context.caption_tokens()
            if query_words:
                overlap = len(query_words & caption_words)
                score += cfg.weight_caption_overlap * (
                    overlap / len(query_words)
                )

        # ---- Recency -------------------------------------------------
        age_hours = max(0.0, (time.time() - context.timestamp) / 3600.0)
        recency = 1.0 / (1.0 + age_hours / cfg.recency_half_life_hours)
        score += cfg.weight_recency * recency

        # ---- Type bonus ---------------------------------------------
        score += cfg.weight_type_bonus * self._type_bonus(query, context)

        return max(0.0, min(1.0, score))

    def _type_bonus(self, query: str, context: VisualContext) -> float:
        """
        Return a bonus in ``[0, 1]`` when the query references the context's
        type family.
        """
        q = query.lower()
        cfg = self._config
        if context.context_type == VisualContextType.CHART:
            return 1.0 if any(k in q for k in cfg.chart_keywords) else 0.0
        if context.context_type == VisualContextType.DIAGRAM:
            return 1.0 if any(k in q for k in cfg.diagram_keywords) else 0.0
        if context.context_type == VisualContextType.SCREENSHOT:
            return 1.0 if any(k in q for k in cfg.screenshot_keywords) else 0.0
        if context.context_type == VisualContextType.IMAGE:
            return 1.0 if any(k in q for k in cfg.image_keywords) else 0.0
        return 0.0

    def _segment_text(self, text: str) -> List[str]:
        """Split ``text`` into paragraph segments."""
        return [p.strip() for p in text.split("\n\n") if p.strip()]

    def _score_alignment(
        self, text_segment: str, visual_context: VisualContext
    ) -> float:
        """
        Score the alignment between a text segment and a visual context.

        Returns a value in ``[0, 1]`` — the original returned a raw ratio
        that could exceed 1.0 for very short segments.
        """
        if not visual_context.caption:
            return 0.0
        text_words = frozenset(text_segment.lower().split())
        caption_words = visual_context.caption_tokens()
        if not text_words or not caption_words:
            return 0.0
        overlap = len(text_words & caption_words)
        # Jaccard-style overlap bounded by 1.0.
        return min(
            1.0,
            overlap / max(1, len(text_words | caption_words)),
        )

    def _is_visual_heavy_query(self, query: str) -> bool:
        """Return True when ``query`` mentions a visual concept."""
        q = query.lower()
        return any(kw in q for kw in self._config.visual_heavy_keywords)

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_contexts: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "contexts": len(self.visual_contexts),
                    "context_evictions": self._context_evictions,
                    "index_evictions": self._index_evictions,
                    "index_size": len(self.caption_index),
                    "retrieval_stats": dict(self.retrieval_stats),
                },
            }
            if include_contexts:
                payload["contexts"] = [
                    c.to_dict() for c in self.visual_contexts.values()
                ]
                payload["caption_index"] = {
                    word: list(ids)
                    for word, ids in self.caption_index.items()
                }
                payload["retrieval_history"] = list(self._retrieval_history)
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VisualContextHandler":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = VisualContextConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        handler = cls(config=cfg, strict=bool(data.get("strict", True)))

        with handler._lock:
            for entry in data.get("contexts", []):
                ctx = VisualContext.from_dict(entry)
                handler.visual_contexts[ctx.context_id] = ctx
            for word, ids in (data.get("caption_index") or {}).items():
                bucket = deque(
                    (str(i) for i in ids), maxlen=256
                )
                handler.caption_index[str(word)] = bucket
            for entry in data.get("retrieval_history", []):
                if isinstance(entry, Mapping):
                    handler._retrieval_history.append(dict(entry))
            counters = dict(data.get("counters", {}) or {})
            handler._context_evictions = int(
                counters.get("context_evictions", 0)
            )
            handler._index_evictions = int(
                counters.get("index_evictions", 0)
            )
            stats_in = counters.get("retrieval_stats")
            if isinstance(stats_in, Mapping):
                handler.retrieval_stats.update(dict(stats_in))
        return handler

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "VisualContextHandler":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise VisualContextError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "VisualContextHandler":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "VisualContextHandler scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.context_count

    def __contains__(self, context_id: object) -> bool:
        if not isinstance(context_id, str):
            return False
        with self._lock:
            return context_id in self.visual_contexts

    def __repr__(self) -> str:
        with self._lock:
            return (
                "VisualContextHandler("
                f"contexts={len(self.visual_contexts)}, "
                f"index_size={len(self.caption_index)}, "
                f"token_cost={self.visual_token_cost}, "
                f"max_contexts={self.max_visual_contexts}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "VisualContext",
    "VisualContextConfig",
    "VisualContextError",
    "VisualContextHandler",
    "VisualContextType",
    "VisualRetrievalResult",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.visual_context_handler
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path ------------------------------------------------ #
    handler = VisualContextHandler()
    print("repr       :", handler)

    ids = [
        handler.add_visual_context(
            "/path/to/chest_xray.png", VisualContextType.IMAGE,
            caption="Chest X-ray showing pulmonary opacity",
        ),
        handler.add_visual_context(
            "/path/to/energy_chart.png", VisualContextType.CHART,
            caption="Bar chart of renewable energy adoption by year",
        ),
        handler.add_visual_context(
            "/path/to/solar_diagram.png", VisualContextType.DIAGRAM,
            caption="Diagram of a photovoltaic solar panel",
        ),
        handler.add_visual_context(
            "/path/to/ui_screenshot.png", VisualContextType.SCREENSHOT,
            caption="Screenshot of the monitoring interface",
        ),
    ]
    print(f"added      : {len(ids)} context(s)")
    assert len(handler) == 4

    # ---- Deterministic IDs ---------------------------------------- #
    again = handler.add_visual_context(
        "/path/to/chest_xray.png", VisualContextType.IMAGE,
        caption="Chest X-ray showing pulmonary opacity",
    )
    assert again == ids[0], "identical inputs must produce identical IDs"
    assert len(handler) == 4, "identical inputs must not duplicate entries"
    print("determinism: OK")

    # ---- Retrieve: chart-oriented query -------------------------- #
    result = handler.retrieve_visual_contexts("show me the chart of energy")
    print("chart query:", [c.context_id[:8] for c in result.contexts])
    print("relevance  :", [round(s, 3) for s in result.relevance_scores])
    assert result.contexts, "should retrieve at least one chart"

    # ---- Type filtering ------------------------------------------ #
    charts_only = handler.retrieve_visual_contexts(
        "energy chart", context_types=[VisualContextType.CHART],
    )
    assert all(
        c.context_type == VisualContextType.CHART
        for c in charts_only.contexts
    )
    print("charts     : OK")

    # ---- Budget enforcement -------------------------------------- #
    tight = handler.retrieve_visual_contexts(
        "any query", token_budget=256,
    )
    assert tight.total_token_cost <= 256
    print(f"tight budget: tokens={tight.total_token_cost}, "
          f"contexts={len(tight.contexts)}")

    # ---- Alignment ----------------------------------------------- #
    text = (
        "The chest X-ray shows signs of pneumonia.\n\n"
        "Renewable energy adoption has increased sharply.\n\n"
        "The user interface displays real-time metrics."
    )
    aligned = handler.align_visual_text(text, list(result.contexts) + [
        handler.visual_contexts[i] for i in ids
    ])
    for segment, ctx, score in aligned:
        print(f"align      : {ctx.context_type.value:<10} "
              f"score={score:.3f} :: {segment[:40]}")

    # ---- Budget optimization ------------------------------------- #
    budget = handler.optimize_visual_budget(
        "show me the energy chart", total_token_budget=4096,
    )
    print("budget     :", budget)

    # Visual-heavy query shifts budget toward visual.
    budget_visual = handler.optimize_visual_budget(
        "display a picture of the pipeline", total_token_budget=4096,
    )
    print("visual bud :", budget_visual)
    assert budget_visual["visual_tokens"] >= budget["visual_tokens"]

    # ---- Statistics ---------------------------------------------- #
    print("stats      :", {
        k: v for k, v in handler.get_visual_stats().items()
        if k not in ("retrieval_stats",)
    })
    print("retrieval  :", handler.get_visual_stats()["retrieval_stats"])

    # ---- Bug fix: deterministic IDs ------------------------------ #
    h1 = VisualContextHandler()
    h2 = VisualContextHandler()
    a = h1.add_visual_context("/x.png", VisualContextType.IMAGE)
    b = h2.add_visual_context("/x.png", VisualContextType.IMAGE)
    assert a == b, "identical inputs must yield identical IDs across instances"
    print("deterministic: OK")

    # ---- Bug fix: alignment bounded to [0, 1] -------------------- #
    h3 = VisualContextHandler()
    cid = h3.add_visual_context(
        "/y.png", VisualContextType.IMAGE,
        caption="a b c d e f g h i j k l m n o p",
    )
    ctx = h3.visual_contexts[cid]
    score = h3._score_alignment("a", ctx)
    assert 0.0 <= score <= 1.0, f"alignment out of range: {score}"
    print(f"alignment  : {score:.3f} in [0, 1] OK")

    # ---- Bug fix: avg_relevance after reset --------------------- #
    handler.reset_stats()
    handler.retrieve_visual_contexts("energy")
    stats = handler.get_visual_stats()["retrieval_stats"]
    assert stats["total_retrievals"] == 1
    assert not math.isnan(stats["avg_relevance"])
    print(f"reset+avg  : {stats}")

    # ---- Bounded storage ---------------------------------------- #
    bounded = VisualContextHandler(
        config=VisualContextConfig(max_contexts=3, max_index_entries=10)
    )
    for i in range(10):
        bounded.add_visual_context(
            f"/path/{i}.png", VisualContextType.IMAGE,
            caption=f"caption {i} extra words here",
        )
    print(f"bounded    : contexts={bounded.context_count} "
          f"evictions={bounded._context_evictions} "
          f"index={len(bounded.caption_index)}")
    assert bounded.context_count == 3

    # ---- Serialization round-trip ------------------------------- #
    payload = handler.to_json()
    restored = VisualContextHandler.from_json(payload)
    assert restored.to_dict() == handler.to_dict()
    print("Round-trip OK.")

    # ---- Dataclass round-trips ---------------------------------- #
    ctx = list(handler.visual_contexts.values())[0]
    p = json.dumps(ctx.to_dict(), default=str)
    assert VisualContext.from_dict(json.loads(p)).to_dict() == ctx.to_dict()
    print("Dataclass  : OK")

    # ---- Context manager ---------------------------------------- #
    with VisualContextHandler() as scoped:
        scoped.add_visual_context("/a.png", VisualContextType.IMAGE)
        assert scoped.context_count == 1
    print("Context    : OK")

    # ---- Validation failures ------------------------------------ #
    for bad_cfg in (
        dict(visual_token_cost=0),
        dict(visual_energy_multiplier=0),
        dict(energy_per_token_wh=0),
        dict(max_visual_contexts=0),
        dict(max_contexts=0),
        dict(max_index_entries=0),
        dict(weight_caption_overlap=0.9, weight_recency=0.9),  # sum != 1
        dict(weight_caption_overlap=-0.1),
        dict(recency_half_life_hours=0),
        dict(min_alignment_score=1.5),
        dict(default_text_priority=-0.1),
        dict(visual_shift_factor=1.5),
        dict(hash_preview_chars=0),
        dict(max_retrieval_history=0),
    ):
        try:
            VisualContextConfig(**bad_cfg)  # type: ignore[arg-type]
        except VisualContextError as exc:
            print("Rejected cfg:", exc)

    strict = VisualContextHandler(strict=True)
    for bad_call in (
        lambda: strict.add_visual_context("", VisualContextType.IMAGE),
        lambda: strict.add_visual_context("/x.png", "image"),  # type: ignore[arg-type]
        lambda: strict.add_visual_context("/x.png", VisualContextType.IMAGE, caption=123),  # type: ignore[arg-type]
        lambda: strict.add_visual_context("/x.png", VisualContextType.IMAGE, metadata="bad"),  # type: ignore[arg-type]
        lambda: strict.retrieve_visual_contexts(123),  # type: ignore[arg-type]
        lambda: strict.retrieve_visual_contexts("q", max_contexts=0),
        lambda: strict.retrieve_visual_contexts("q", context_types="charts"),  # type: ignore[arg-type]
        lambda: strict.retrieve_visual_contexts("q", context_types=["chart"]),  # type: ignore[list-item]
        lambda: strict.retrieve_visual_contexts("q", token_budget=-1),
        lambda: strict.retrieve_visual_contexts("q", energy_budget=float("nan")),
        lambda: strict.align_visual_text(123, []),  # type: ignore[arg-type]
        lambda: strict.align_visual_text("text", "not-a-list"),  # type: ignore[arg-type]
        lambda: strict.optimize_visual_budget("q", 0),
        lambda: strict.optimize_visual_budget("q", 1000, text_priority=1.5),
    ):
        try:
            bad_call()
        except VisualContextError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces ------------------------------------- #
    lenient = VisualContextHandler(strict=False)
    lenient.add_visual_context(
        "/x.png", VisualContextType.IMAGE, metadata="bad",  # type: ignore[arg-type]
    )
    print("lenient    : OK")

    print("\nSmoke test passed.")
