# src/retrieval/visual_context_handler.py

"""
Visual Context Handler
======================

Handles visual context in multimodal retrieval inspired by VimRAG's
approach to navigating massive visual context efficiently.

Design
------
- Frozen, validated ``VisualContextConfig``: token/energy costs,
  multipliers, scoring weights, thresholds, bounded caps.
- ``VisualContext`` / ``VisualRetrievalResult`` — frozen dataclasses
  with validation and serialization; both round-trip via
  ``from_dict``.
- **Deterministic context IDs** — SHA-256 over
  ``(context_type, source_path[:hash_preview_chars])``. Two
  ``add_visual_context`` calls with identical arguments **replace**
  the existing entry rather than duplicating it. Callers who need
  multiple entries for the same file may pass an explicit
  ``context_id`` argument.
- **Index-backed retrieval** — candidate selection uses the
  ``caption_index`` (word → context IDs) so a query touching K words
  touches only the contexts mentioning those words. Falls back to a
  full scan when the index yields nothing (e.g., queries with no
  indexed words, or contexts without captions).
- **Correct running-mean** — ``avg_relevance`` averages only over
  retrievals that contributed at least one score
  (``scored_retrievals`` counter).
- **Bounded storage** — ``max_contexts`` LRU eviction,
  ``max_index_entries`` word cap, ``max_retrieval_history`` for the
  audit trail; ``from_dict`` honours all three.
- **Thread safety** — ``RLock`` guards storage, index, and counters.
- **Strict serialization** — ``to_json(allow_nan=False)`` on both
  the handler and the result. ``from_dict`` wraps all failures in
  ``VisualContextError``.

Version: 2.0.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import numbers
import threading
import time
from collections import OrderedDict, deque
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
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "VisualContext",
    "VisualContextConfig",
    "VisualContextError",
    "VisualContextHandler",
    "VisualContextType",
    "VisualRetrievalResult",
]

VERSION = "2.0.0"


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
    """Tunable parameters for :class:`VisualContextHandler`."""

    visual_token_cost: int = 256
    visual_energy_multiplier: float = 2.5
    energy_per_token_wh: float = 1e-6

    max_visual_contexts: int = 5
    max_contexts: Optional[int] = 10_000
    max_index_entries: Optional[int] = 50_000

    weight_caption_overlap: float = 0.5
    weight_recency: float = 0.3
    weight_type_bonus: float = 0.2

    recency_half_life_hours: float = 24.0

    chart_keywords: tuple = (
        "chart", "graph", "plot", "histogram",
    )
    diagram_keywords: tuple = (
        "diagram", "flowchart", "schematic",
    )
    screenshot_keywords: tuple = (
        "screenshot", "screen", "ui", "interface",
    )
    image_keywords: tuple = ("image", "picture", "photo")

    min_alignment_score: float = 0.3

    default_text_priority: float = 0.7
    visual_shift_factor: float = 0.2
    visual_heavy_keywords: tuple = (
        "image", "picture", "diagram", "chart", "graph",
        "screenshot", "visual", "show", "display",
    )

    # Truncation length for context IDs hashed from source_path.
    hash_preview_chars: int = 200

    max_retrieval_history: int = 1000

    def __post_init__(self) -> None:
        for name in (
            "visual_token_cost",
            "max_visual_contexts",
            "hash_preview_chars",
            "max_retrieval_history",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value <= 0
            ):
                raise VisualContextError(
                    f"{name} must be a positive int."
                )
        for name in ("max_contexts", "max_index_entries"):
            value = getattr(self, name)
            if value is not None:
                if (
                    isinstance(value, bool)
                    or not isinstance(value, numbers.Integral)
                    or value <= 0
                ):
                    raise VisualContextError(
                        f"{name} must be a positive int or None."
                    )
        for name in (
            "visual_energy_multiplier",
            "energy_per_token_wh",
            "recency_half_life_hours",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise VisualContextError(
                    f"{name} must be finite and > 0."
                )
        weights = (
            float(self.weight_caption_overlap)
            + float(self.weight_recency)
            + float(self.weight_type_bonus)
        )
        if abs(weights - 1.0) > 1e-6:
            raise VisualContextError(
                f"scoring weights must sum to 1.0 "
                f"(got {weights:.6f})."
            )
        for name in (
            "weight_caption_overlap",
            "weight_recency",
            "weight_type_bonus",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise VisualContextError(
                    f"{name} must be finite and >= 0."
                )
        for name in (
            "min_alignment_score",
            "default_text_priority",
            "visual_shift_factor",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise VisualContextError(
                    f"{name} must be in [0, 1]."
                )
        for name in (
            "chart_keywords",
            "diagram_keywords",
            "screenshot_keywords",
            "image_keywords",
            "visual_heavy_keywords",
        ):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                raise VisualContextError(
                    f"{name} must be a tuple."
                )
            for kw in value:
                if not isinstance(kw, str) or not kw:
                    raise VisualContextError(
                        f"{name} entries must be non-empty strings."
                    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VisualContextConfig":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                f"VisualContextConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            int_fields = {
                "visual_token_cost", "max_visual_contexts",
                "hash_preview_chars", "max_retrieval_history",
            }
            optional_int_fields = {
                "max_contexts", "max_index_entries",
            }
            tuple_fields = {
                "chart_keywords", "diagram_keywords",
                "screenshot_keywords", "image_keywords",
                "visual_heavy_keywords",
            }
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    logger.debug(
                        "VisualContextConfig.from_dict dropping "
                        "unknown field %r.", k,
                    )
                    continue
                if k in int_fields:
                    if isinstance(v, bool) or not isinstance(v, int):
                        raise VisualContextError(
                            f"{k} must be an int, got "
                            f"{type(v).__name__}."
                        )
                if k in optional_int_fields and v is not None:
                    if isinstance(v, bool) or not isinstance(v, int):
                        raise VisualContextError(
                            f"{k} must be an int or None, got "
                            f"{type(v).__name__}."
                        )
                if k in tuple_fields:
                    if not isinstance(v, (list, tuple)):
                        raise VisualContextError(
                            f"{k} must be a list or tuple, got "
                            f"{type(v).__name__}."
                        )
                    v = tuple(v)
                kwargs[k] = v
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VisualContextError):
                raise
            raise VisualContextError(
                f"invalid VisualContextConfig payload: {exc}"
            ) from exc


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

    _caption_tokens: Optional[FrozenSet[str]] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.context_id, str)
            or not self.context_id
        ):
            raise VisualContextError(
                "context_id must be a non-empty string."
            )
        if not isinstance(
            self.context_type, VisualContextType,
        ):
            raise VisualContextError(
                f"context_type must be a VisualContextType, got "
                f"{type(self.context_type).__name__}."
            )
        if (
            not isinstance(self.source_path, str)
            or not self.source_path
        ):
            raise VisualContextError(
                "source_path must be a non-empty string."
            )
        if self.caption is not None and not isinstance(
            self.caption, str,
        ):
            raise VisualContextError(
                "caption must be a string or None."
            )
        if self.embedding is not None:
            if not isinstance(self.embedding, tuple):
                object.__setattr__(
                    self, "embedding", tuple(self.embedding),
                )
            for i, v in enumerate(self.embedding):
                if (
                    isinstance(v, bool)
                    or not isinstance(v, numbers.Real)
                ):
                    raise VisualContextError(
                        f"embedding[{i}] must be numeric."
                    )
                if not math.isfinite(float(v)):
                    raise VisualContextError(
                        f"embedding[{i}] must be finite."
                    )
        if (
            isinstance(self.token_cost, bool)
            or not isinstance(self.token_cost, numbers.Integral)
            or self.token_cost < 0
        ):
            raise VisualContextError(
                "token_cost must be a non-negative int."
            )
        if (
            isinstance(self.energy_cost, bool)
            or not isinstance(self.energy_cost, numbers.Real)
        ):
            raise VisualContextError(
                "energy_cost must be numeric."
            )
        fe = float(self.energy_cost)
        if not math.isfinite(fe) or fe < 0:
            raise VisualContextError(
                f"energy_cost must be finite and >= 0, got "
                f"{self.energy_cost!r}."
            )
        if not isinstance(self.metadata, Mapping):
            raise VisualContextError(
                "metadata must be a Mapping."
            )
        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, numbers.Real)
            or not math.isfinite(float(self.timestamp))
        ):
            raise VisualContextError(
                "timestamp must be a finite number."
            )

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
            "embedding": (
                list(self.embedding)
                if self.embedding is not None else None
            ),
            "token_cost": self.token_cost,
            "energy_cost": self.energy_cost,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VisualContext":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                "VisualContext.from_dict expects a Mapping."
            )
        try:
            ts = data.get("created_at")
            if isinstance(ts, str):
                created_at = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not created_at.tzinfo:
                    created_at = created_at.replace(
                        tzinfo=timezone.utc,
                    )
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                created_at = datetime.now(timezone.utc)
            emb = data.get("embedding")
            metadata = data.get("metadata") or {}
            if not isinstance(metadata, Mapping):
                raise VisualContextError(
                    "metadata must be a Mapping."
                )
            return cls(
                context_id=str(data["context_id"]),
                context_type=VisualContextType(
                    str(data["context_type"])
                ),
                source_path=str(data["source_path"]),
                caption=data.get("caption"),
                embedding=(
                    tuple(emb) if emb is not None else None
                ),
                token_cost=int(data.get("token_cost", 0)),
                energy_cost=float(
                    data.get("energy_cost", 0.0)
                ),
                metadata=dict(metadata),
                timestamp=float(
                    data.get("timestamp", time.time())
                ),
                created_at=created_at,
            )
        except KeyError as exc:
            raise VisualContextError(
                f"VisualContext payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VisualContextError):
                raise
            raise VisualContextError(
                f"invalid VisualContext payload: {exc}"
            ) from exc

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
        if (
            isinstance(self.total_token_cost, bool)
            or not isinstance(
                self.total_token_cost, numbers.Integral,
            )
            or self.total_token_cost < 0
        ):
            raise VisualContextError(
                "total_token_cost must be a non-negative int."
            )
        if (
            isinstance(self.total_energy_cost, bool)
            or not isinstance(
                self.total_energy_cost, numbers.Real,
            )
        ):
            raise VisualContextError(
                "total_energy_cost must be numeric."
            )
        fv = float(self.total_energy_cost)
        if not math.isfinite(fv) or fv < 0:
            raise VisualContextError(
                "total_energy_cost must be finite and >= 0."
            )
        if not isinstance(self.metadata, Mapping):
            raise VisualContextError(
                "metadata must be a Mapping."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contexts": [c.to_dict() for c in self.contexts],
            "total_token_cost": self.total_token_cost,
            "total_energy_cost": self.total_energy_cost,
            "relevance_scores": list(self.relevance_scores),
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VisualRetrievalResult":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                "VisualRetrievalResult.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc,
                    )
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)

            raw_contexts = data.get("contexts") or []
            if not isinstance(raw_contexts, list):
                raise VisualContextError(
                    "contexts must be a list."
                )
            raw_scores = data.get("relevance_scores") or []
            if not isinstance(raw_scores, (list, tuple)):
                raise VisualContextError(
                    "relevance_scores must be a list."
                )
            metadata = data.get("metadata") or {}
            if not isinstance(metadata, Mapping):
                raise VisualContextError(
                    "metadata must be a Mapping."
                )

            return cls(
                contexts=tuple(
                    VisualContext.from_dict(entry)
                    for entry in raw_contexts
                ),
                total_token_cost=int(
                    data.get("total_token_cost", 0)
                ),
                total_energy_cost=float(
                    data.get("total_energy_cost", 0.0)
                ),
                relevance_scores=tuple(
                    float(s) for s in raw_scores
                ),
                metadata=dict(metadata),
                timestamp=timestamp,
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VisualContextError):
                raise
            raise VisualContextError(
                f"invalid VisualRetrievalResult payload: {exc}"
            ) from exc

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

    Thread-safe, serializable, bounded in memory.
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
                visual_energy_multiplier=float(
                    visual_energy_multiplier
                ),
                max_visual_contexts=int(max_visual_contexts),
            )
        self._strict = bool(strict)

        # Legacy attributes.
        self.visual_token_cost: int = (
            self._config.visual_token_cost
        )
        self.visual_energy_multiplier: float = (
            self._config.visual_energy_multiplier
        )
        self.max_visual_contexts: int = (
            self._config.max_visual_contexts
        )

        self._lock = threading.RLock()
        self.visual_contexts: "OrderedDict[str, VisualContext]" = (
            OrderedDict()
        )

        # Word → deque of context IDs.
        self.caption_index: Dict[str, Deque[str]] = {}

        # Counters.
        self._additions: int = 0
        self._replacements: int = 0
        self._removals: int = 0
        self._context_evictions: int = 0
        self._index_evictions: int = 0

        # Retrieval statistics — ``scored_retrievals`` avoids the
        # "zero-result drags the average down" bug.
        self.retrieval_stats: Dict[str, Any] = {
            "total_retrievals": 0,
            "total_contexts_retrieved": 0,
            "scored_retrievals": 0,
            "avg_relevance": 0.0,
        }

        self._retrieval_history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_retrieval_history
        )

        logger.debug(
            "VisualContextHandler initialized "
            "(token_cost=%d, multiplier=%.2f, max_contexts=%d, "
            "strict=%s)",
            self.visual_token_cost,
            self.visual_energy_multiplier,
            self.max_visual_contexts,
            self._strict,
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
        *,
        context_id: Optional[str] = None,
    ) -> str:
        """
        Add a visual context to the handler.

        The default ``context_id`` is deterministic: identical
        ``source_path`` + ``context_type`` produce the same ID, so
        re-adding the same file replaces its entry. Callers who need
        multiple entries for the same file may pass an explicit
        ``context_id``.
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
            raise VisualContextError(
                "caption must be a string or None."
            )
        if metadata is not None and not isinstance(
            metadata, Mapping,
        ):
            msg = (
                f"metadata must be a Mapping or None, got "
                f"{type(metadata).__name__}."
            )
            if self._strict:
                raise VisualContextError(msg)
            logger.warning("%s Ignoring metadata.", msg)
            metadata = None

        if context_id is None:
            context_id = self._generate_context_id(
                source_path, context_type,
            )
        elif not isinstance(context_id, str) or not context_id:
            raise VisualContextError(
                "context_id must be a non-empty string or None."
            )

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
            embedding=(
                tuple(embedding) if embedding is not None else None
            ),
            token_cost=token_cost,
            energy_cost=energy_cost,
            metadata=dict(metadata or {}),
            timestamp=time.time(),
        )

        with self._lock:
            if context_id in self.visual_contexts:
                old = self.visual_contexts.pop(context_id)
                if old.caption:
                    self._unindex_caption(
                        context_id, old.caption,
                    )
                self._replacements += 1
            else:
                self._additions += 1
            self.visual_contexts[context_id] = context

            while (
                self._config.max_contexts is not None
                and len(self.visual_contexts)
                > self._config.max_contexts
            ):
                _, dropped = self.visual_contexts.popitem(
                    last=False,
                )
                if dropped.caption:
                    self._unindex_caption(
                        dropped.context_id, dropped.caption,
                    )
                self._context_evictions += 1

            if caption:
                self._index_caption(context_id, caption)

        logger.debug(
            "Added visual context %s (type=%s, tokens=%d, "
            "energy=%.6f).",
            context_id, context_type.value,
            token_cost, energy_cost,
        )
        return context_id

    def remove_context(self, context_id: str) -> bool:
        """
        Remove a single context. Returns ``True`` iff it existed.
        """
        if not isinstance(context_id, str) or not context_id:
            raise VisualContextError(
                "context_id must be a non-empty string."
            )
        with self._lock:
            ctx = self.visual_contexts.pop(context_id, None)
            if ctx is None:
                return False
            if ctx.caption:
                self._unindex_caption(
                    context_id, ctx.caption,
                )
            self._removals += 1
        logger.debug("Removed visual context %s.", context_id)
        return True

    def clear(self) -> int:
        """Remove every context. Returns the number removed."""
        with self._lock:
            n = len(self.visual_contexts)
            self.visual_contexts.clear()
            self.caption_index.clear()
            self._removals += n
        logger.debug("Cleared %d visual context(s).", n)
        return n

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

        Candidate selection uses the caption index: the union of
        contexts indexed under any query word is scored; a full scan
        is used only when the index yields nothing (e.g., the query
        contains no indexed words, or no contexts have captions).

        ``context_types=[]`` returns an empty result — use ``None``
        (the default) to disable type filtering.
        """
        if not isinstance(query, str):
            raise VisualContextError("query must be a string.")
        if max_contexts is not None and (
            isinstance(max_contexts, bool)
            or not isinstance(max_contexts, numbers.Integral)
            or max_contexts <= 0
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
                        f"context_types[{i}] must be a "
                        f"VisualContextType."
                    )
        if token_budget is not None and (
            isinstance(token_budget, bool)
            or not isinstance(token_budget, numbers.Integral)
            or token_budget <= 0
        ):
            raise VisualContextError(
                "token_budget must be a positive int or None."
            )
        if energy_budget is not None:
            if (
                isinstance(energy_budget, bool)
                or not isinstance(energy_budget, numbers.Real)
            ):
                raise VisualContextError(
                    "energy_budget must be numeric or None."
                )
            fb = float(energy_budget)
            if not math.isfinite(fb) or fb <= 0:
                raise VisualContextError(
                    "energy_budget must be finite and > 0."
                )

        max_ctx = max_contexts or self.max_visual_contexts
        allowed_types: Optional[Set[VisualContextType]] = None
        if context_types is not None:
            allowed_types = set(context_types)
            if not allowed_types:
                # Explicit empty list means "no types allowed".
                empty = VisualRetrievalResult(
                    contexts=tuple(),
                    total_token_cost=0,
                    total_energy_cost=0.0,
                    relevance_scores=tuple(),
                    metadata={
                        "query": query,
                        "candidates_evaluated": 0,
                        "contexts_selected": 0,
                        "token_budget": token_budget,
                        "energy_budget": energy_budget,
                        "max_contexts": max_ctx,
                        "context_types": [],
                    },
                )
                self._record_empty_retrieval(
                    query, token_budget, energy_budget, max_ctx,
                )
                return empty

        with self._lock:
            # ---- Index-backed candidate selection -------------------
            query_words = query.lower().split()
            candidate_ids: Set[str] = set()
            if query_words and self.caption_index:
                for word in query_words:
                    bucket = self.caption_index.get(word)
                    if bucket:
                        candidate_ids.update(bucket)

            if candidate_ids:
                candidates = [
                    self.visual_contexts[cid]
                    for cid in candidate_ids
                    if cid in self.visual_contexts
                ]
                if allowed_types is not None:
                    candidates = [
                        c for c in candidates
                        if c.context_type in allowed_types
                    ]
            else:
                # Fall back to full scan.
                candidates = [
                    ctx for ctx in self.visual_contexts.values()
                    if allowed_types is None
                    or ctx.context_type in allowed_types
                ]

        # Score candidates.
        scored = [
            (ctx, self._score_relevance(query, ctx))
            for ctx in candidates
        ]
        scored.sort(key=lambda x: (-x[1], x[0].context_id))

        # Greedy selection within both budgets.
        selected: List[VisualContext] = []
        total_tokens = 0
        total_energy = 0.0
        relevance_scores: List[float] = []

        for ctx, score in scored:
            if len(selected) >= max_ctx:
                break
            if (
                token_budget is not None
                and total_tokens + ctx.token_cost > token_budget
            ):
                continue
            if (
                energy_budget is not None
                and total_energy + ctx.energy_cost > energy_budget
            ):
                continue
            selected.append(ctx)
            total_tokens += ctx.token_cost
            total_energy += ctx.energy_cost
            relevance_scores.append(score)

        # ---- Update stats with correct running mean ----------------
        with self._lock:
            stats = self.retrieval_stats
            stats["total_retrievals"] = int(
                stats.get("total_retrievals", 0)
            ) + 1
            stats["total_contexts_retrieved"] = int(
                stats.get("total_contexts_retrieved", 0)
            ) + len(selected)
            if relevance_scores:
                avg_rel = sum(relevance_scores) / len(
                    relevance_scores
                )
                prior_n = int(
                    stats.get("scored_retrievals", 0)
                )
                if prior_n == 0:
                    stats["avg_relevance"] = avg_rel
                else:
                    prior_avg = float(
                        stats.get("avg_relevance", 0.0)
                    )
                    stats["avg_relevance"] = (
                        prior_avg * prior_n + avg_rel
                    ) / (prior_n + 1)
                stats["scored_retrievals"] = prior_n + 1

            self._retrieval_history.append({
                "query": query,
                "candidates_evaluated": len(candidates),
                "contexts_selected": len(selected),
                "total_tokens": total_tokens,
                "total_energy": total_energy,
                "index_used": bool(candidate_ids),
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
                "token_budget": token_budget,
                "energy_budget": energy_budget,
                "max_contexts": max_ctx,
                "context_types": (
                    [t.value for t in context_types]
                    if context_types is not None else None
                ),
                "index_used": bool(candidate_ids),
            },
        )

    def _record_empty_retrieval(
        self,
        query: str,
        token_budget: Optional[int],
        energy_budget: Optional[float],
        max_ctx: int,
    ) -> None:
        with self._lock:
            stats = self.retrieval_stats
            stats["total_retrievals"] = int(
                stats.get("total_retrievals", 0)
            ) + 1
            self._retrieval_history.append({
                "query": query,
                "candidates_evaluated": 0,
                "contexts_selected": 0,
                "total_tokens": 0,
                "total_energy": 0.0,
                "index_used": False,
                "timestamp": time.time(),
            })

    # ---------------------------------------------------------- alignment
    def align_visual_text(
        self,
        text_content: str,
        visual_contexts: List[VisualContext],
    ) -> List[Tuple[str, VisualContext, float]]:
        """Align text segments with visual contexts."""
        if not isinstance(text_content, str):
            raise VisualContextError(
                "text_content must be a string."
            )
        if not isinstance(visual_contexts, list):
            raise VisualContextError(
                "visual_contexts must be a list."
            )
        for i, ctx in enumerate(visual_contexts):
            if not isinstance(ctx, VisualContext):
                raise VisualContextError(
                    f"visual_contexts[{i}] must be a "
                    f"VisualContext, got {type(ctx).__name__}."
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
                alignments.append(
                    (segment, best_match, best_score)
                )

        return alignments

    # ---------------------------------------------------------- budget
    def optimize_visual_budget(
        self,
        query: str,
        total_token_budget: int,
        text_priority: Optional[float] = None,
    ) -> Dict[str, int]:
        """Optimize token budget allocation between text and visual."""
        if not isinstance(query, str):
            raise VisualContextError("query must be a string.")
        if (
            isinstance(total_token_budget, bool)
            or not isinstance(
                total_token_budget, numbers.Integral,
            )
            or total_token_budget <= 0
        ):
            raise VisualContextError(
                "total_token_budget must be a positive int."
            )
        priority = (
            self._config.default_text_priority
            if text_priority is None else text_priority
        )
        if (
            isinstance(priority, bool)
            or not isinstance(priority, numbers.Real)
        ):
            raise VisualContextError(
                "text_priority must be numeric."
            )
        fp = float(priority)
        if not math.isfinite(fp) or not 0.0 <= fp <= 1.0:
            raise VisualContextError(
                "text_priority must be in [0, 1]."
            )

        text_budget = int(total_token_budget * fp)
        visual_budget = int(total_token_budget) - text_budget

        if self._is_visual_heavy_query(query):
            shift = int(
                text_budget * self._config.visual_shift_factor
            )
            text_budget -= shift
            visual_budget += shift

        max_by_budget = (
            visual_budget // max(1, self.visual_token_cost)
        )
        # Never suggest more contexts than the store's own cap.
        max_visual = min(
            max_by_budget, self._config.max_visual_contexts,
        )

        return {
            "text_tokens": text_budget,
            "visual_tokens": visual_budget,
            "max_visual_contexts": max_visual,
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
            additions = self._additions
            replacements = self._replacements
            removals = self._removals
            history_size = len(self._retrieval_history)

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
                total_tokens / len(contexts)
                if contexts else 0.0
            ),
            "context_evictions": context_evictions,
            "index_evictions": index_evictions,
            "index_size": index_size,
            "retrieval_history_size": history_size,
            "additions": additions,
            "replacements": replacements,
            "removals": removals,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_visual_stats()

    def reset_stats(self, *, clear_history: bool = True) -> None:
        with self._lock:
            self.retrieval_stats["total_retrievals"] = 0
            self.retrieval_stats[
                "total_contexts_retrieved"
            ] = 0
            self.retrieval_stats["scored_retrievals"] = 0
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
        Deterministic context ID: SHA-256 over
        ``(context_type, source_path[:hash_preview_chars])``.
        """
        preview = source_path[: self._config.hash_preview_chars]
        raw = f"{context_type.value}:{preview}"
        return hashlib.sha256(
            raw.encode("utf-8", errors="replace")
        ).hexdigest()[:16]

    def _index_caption(
        self, context_id: str, caption: str,
    ) -> None:
        """Index the caption's words → context_id."""
        words = caption.lower().split()
        for word in words:
            bucket = self.caption_index.get(word)
            if bucket is None:
                bucket = deque(maxlen=256)
                self.caption_index[word] = bucket
            if context_id not in bucket:
                bucket.append(context_id)

        cap = self._config.max_index_entries
        if cap is not None and len(self.caption_index) > cap:
            excess = len(self.caption_index) - cap
            for _ in range(excess):
                oldest_word = next(iter(self.caption_index))
                self.caption_index.pop(oldest_word, None)
                self._index_evictions += 1

    def _unindex_caption(
        self, context_id: str, caption: str,
    ) -> None:
        """Remove ``context_id`` from every word bucket."""
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
        self, query: str, context: VisualContext,
    ) -> float:
        """Score a visual context's relevance in ``[0, 1]``."""
        cfg = self._config
        score = 0.0

        if context.caption:
            query_words = frozenset(query.lower().split())
            caption_words = context.caption_tokens()
            if query_words:
                overlap = len(query_words & caption_words)
                score += cfg.weight_caption_overlap * (
                    overlap / len(query_words)
                )

        age_hours = max(
            0.0, (time.time() - context.timestamp) / 3600.0,
        )
        recency = 1.0 / (
            1.0 + age_hours / cfg.recency_half_life_hours
        )
        score += cfg.weight_recency * recency
        score += cfg.weight_type_bonus * self._type_bonus(
            query, context,
        )

        return max(0.0, min(1.0, score))

    def _type_bonus(
        self, query: str, context: VisualContext,
    ) -> float:
        """Word-boundary match, not substring."""
        cfg = self._config
        q_words = set(query.lower().split())

        def _any(keywords: Sequence[str]) -> bool:
            return any(kw in q_words for kw in keywords)

        if context.context_type == VisualContextType.CHART:
            return 1.0 if _any(cfg.chart_keywords) else 0.0
        if context.context_type == VisualContextType.DIAGRAM:
            return 1.0 if _any(cfg.diagram_keywords) else 0.0
        if context.context_type == VisualContextType.SCREENSHOT:
            return 1.0 if _any(
                cfg.screenshot_keywords
            ) else 0.0
        if context.context_type == VisualContextType.IMAGE:
            return 1.0 if _any(cfg.image_keywords) else 0.0
        return 0.0

    def _segment_text(self, text: str) -> List[str]:
        return [
            p.strip()
            for p in text.split("\n\n") if p.strip()
        ]

    def _score_alignment(
        self, text_segment: str,
        visual_context: VisualContext,
    ) -> float:
        """Alignment score in ``[0, 1]`` (Jaccard overlap)."""
        if not visual_context.caption:
            return 0.0
        text_words = frozenset(
            text_segment.lower().split()
        )
        caption_words = visual_context.caption_tokens()
        if not text_words or not caption_words:
            return 0.0
        overlap = len(text_words & caption_words)
        return min(
            1.0,
            overlap / max(
                1, len(text_words | caption_words),
            ),
        )

    def _is_visual_heavy_query(self, query: str) -> bool:
        q = query.lower()
        return any(
            kw in q
            for kw in self._config.visual_heavy_keywords
        )

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_contexts: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "counters": {
                    "contexts": len(self.visual_contexts),
                    "context_evictions": self._context_evictions,
                    "index_evictions": self._index_evictions,
                    "index_size": len(self.caption_index),
                    "additions": self._additions,
                    "replacements": self._replacements,
                    "removals": self._removals,
                    "retrieval_stats": dict(
                        self.retrieval_stats
                    ),
                },
            }
            if include_contexts:
                payload["contexts"] = [
                    c.to_dict()
                    for c in self.visual_contexts.values()
                ]
                payload["caption_index"] = {
                    word: list(ids)
                    for word, ids in
                    self.caption_index.items()
                }
                payload["retrieval_history"] = list(
                    self._retrieval_history
                )
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VisualContextHandler":
        if not isinstance(data, Mapping):
            raise VisualContextError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise VisualContextError(
                    "config must be a Mapping."
                )
            cfg = VisualContextConfig.from_dict(
                dict(raw_config)
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise VisualContextError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )
            handler = cls(config=cfg, strict=strict_raw)

            raw_contexts = data.get("contexts") or []
            if not isinstance(raw_contexts, list):
                raise VisualContextError(
                    f"contexts must be a list, got "
                    f"{type(raw_contexts).__name__}."
                )
            raw_index = data.get("caption_index") or {}
            if not isinstance(raw_index, Mapping):
                raise VisualContextError(
                    f"caption_index must be a Mapping, got "
                    f"{type(raw_index).__name__}."
                )
            raw_history = data.get("retrieval_history") or []
            if not isinstance(raw_history, list):
                raise VisualContextError(
                    f"retrieval_history must be a list, got "
                    f"{type(raw_history).__name__}."
                )
            raw_counters = data.get("counters") or {}
            if not isinstance(raw_counters, Mapping):
                raise VisualContextError(
                    "counters must be a Mapping."
                )

            # Enforce LRU cap on reload — keep the newest entries.
            cap = cfg.max_contexts
            if cap is not None and len(raw_contexts) > cap:
                logger.warning(
                    "contexts payload has %d entries, exceeds "
                    "max_contexts=%d; truncating to newest %d.",
                    len(raw_contexts), cap, cap,
                )
                raw_contexts = raw_contexts[-cap:]

            with handler._lock:
                for entry in raw_contexts:
                    if not isinstance(entry, Mapping):
                        raise VisualContextError(
                            "contexts entries must be Mappings."
                        )
                    ctx = VisualContext.from_dict(entry)
                    handler.visual_contexts[
                        ctx.context_id
                    ] = ctx

                # Validate caption_index against the live context set.
                live_ids = set(
                    handler.visual_contexts.keys()
                )
                max_words = cfg.max_index_entries
                loaded_words = 0
                for word, ids in raw_index.items():
                    if not isinstance(
                        ids, (list, tuple, set, deque)
                    ):
                        raise VisualContextError(
                            f"caption_index[{word!r}] must "
                            f"be a list of IDs."
                        )
                    if (
                        max_words is not None
                        and loaded_words >= max_words
                    ):
                        logger.warning(
                            "caption_index payload exceeds "
                            "max_index_entries=%d; "
                            "truncating.",
                            max_words,
                        )
                        break
                    bucket = deque(maxlen=256)
                    for cid in ids:
                        sid = str(cid)
                        if sid not in live_ids:
                            continue
                        bucket.append(sid)
                    if bucket:
                        handler.caption_index[
                            str(word)
                        ] = bucket
                        loaded_words += 1

                for entry in raw_history:
                    if not isinstance(entry, Mapping):
                        raise VisualContextError(
                            "retrieval_history entries must "
                            "be Mappings."
                        )
                    handler._retrieval_history.append(
                        dict(entry)
                    )

                try:
                    handler._context_evictions = int(
                        raw_counters.get(
                            "context_evictions", 0,
                        )
                    )
                    handler._index_evictions = int(
                        raw_counters.get(
                            "index_evictions", 0,
                        )
                    )
                    handler._additions = int(
                        raw_counters.get("additions", 0)
                    )
                    handler._replacements = int(
                        raw_counters.get(
                            "replacements", 0,
                        )
                    )
                    handler._removals = int(
                        raw_counters.get("removals", 0)
                    )
                except (TypeError, ValueError) as exc:
                    raise VisualContextError(
                        f"invalid counter: {exc}"
                    ) from exc
                for name, value in (
                    (
                        "context_evictions",
                        handler._context_evictions,
                    ),
                    (
                        "index_evictions",
                        handler._index_evictions,
                    ),
                    ("additions", handler._additions),
                    (
                        "replacements",
                        handler._replacements,
                    ),
                    ("removals", handler._removals),
                ):
                    if value < 0:
                        raise VisualContextError(
                            f"counter {name} must be >= 0, "
                            f"got {value}."
                        )
                stats_in = raw_counters.get(
                    "retrieval_stats"
                )
                if isinstance(stats_in, Mapping):
                    for k in handler.retrieval_stats:
                        if k not in stats_in:
                            continue
                        v = stats_in[k]
                        if isinstance(
                            v, (bool, int, float, str)
                        ):
                            handler.retrieval_stats[k] = v
            return handler
        except VisualContextError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise VisualContextError(
                f"invalid VisualContextHandler payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_contexts: bool = False,
        **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_contexts=include_contexts),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "VisualContextHandler":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise VisualContextError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "VisualContextHandler":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    handler = VisualContextHandler()
    print("repr       :", handler)

    ids = [
        handler.add_visual_context(
            "/path/to/chest_xray.png",
            VisualContextType.IMAGE,
            caption="Chest X-ray showing pulmonary opacity",
        ),
        handler.add_visual_context(
            "/path/to/energy_chart.png",
            VisualContextType.CHART,
            caption="Bar chart of renewable energy adoption by year",
        ),
        handler.add_visual_context(
            "/path/to/solar_diagram.png",
            VisualContextType.DIAGRAM,
            caption="Diagram of a photovoltaic solar panel",
        ),
        handler.add_visual_context(
            "/path/to/ui_screenshot.png",
            VisualContextType.SCREENSHOT,
            caption="Screenshot of the monitoring interface",
        ),
    ]
    print(f"added      : {len(ids)} context(s)")
    assert len(handler) == 4

    # ---- Deterministic IDs / replacement counting -------------- #
    again = handler.add_visual_context(
        "/path/to/chest_xray.png",
        VisualContextType.IMAGE,
        caption="Chest X-ray showing pulmonary opacity",
    )
    assert again == ids[0]
    assert len(handler) == 4
    stats = handler.get_visual_stats()
    assert stats["additions"] == 4
    assert stats["replacements"] == 1
    print("determinism: OK (replacements counted)")

    # ---- Index-backed retrieval -------------------------------- #
    result = handler.retrieve_visual_contexts(
        "show me the chart of energy"
    )
    print(
        "chart query:",
        [c.context_id[:8] for c in result.contexts],
    )
    assert result.metadata["index_used"] is True
    assert result.metadata["token_budget"] is None
    assert result.contexts

    # ---- Type filtering ---------------------------------------- #
    charts_only = handler.retrieve_visual_contexts(
        "energy chart",
        context_types=[VisualContextType.CHART],
    )
    assert all(
        c.context_type == VisualContextType.CHART
        for c in charts_only.contexts
    )
    print("charts     : OK")

    # Empty context_types list means "no types allowed".
    empty = handler.retrieve_visual_contexts(
        "energy chart", context_types=[],
    )
    assert len(empty.contexts) == 0
    print("empty types: OK")

    # ---- Budget enforcement ------------------------------------ #
    tight = handler.retrieve_visual_contexts(
        "any query", token_budget=256,
    )
    assert tight.total_token_cost <= 256
    print(
        f"tight budget: tokens={tight.total_token_cost}, "
        f"contexts={len(tight.contexts)}"
    )

    # ---- Alignment --------------------------------------------- #
    text = (
        "The chest X-ray shows signs of pneumonia.\n\n"
        "Renewable energy adoption has increased sharply.\n\n"
        "The user interface displays real-time metrics."
    )
    aligned = handler.align_visual_text(
        text,
        list(result.contexts)
        + [handler.visual_contexts[i] for i in ids],
    )
    for segment, ctx, score in aligned:
        print(
            f"align      : {ctx.context_type.value:<10} "
            f"score={score:.3f} :: {segment[:40]}"
        )

    # ---- Budget optimization with cap ------------------------- #
    budget = handler.optimize_visual_budget(
        "show me the energy chart", total_token_budget=4096,
    )
    print("budget     :", budget)
    assert budget["max_visual_contexts"] <= 5

    budget_visual = handler.optimize_visual_budget(
        "display a picture of the pipeline",
        total_token_budget=4096,
    )
    print("visual bud :", budget_visual)
    assert (
        budget_visual["visual_tokens"]
        >= budget["visual_tokens"]
    )
    # Even with a huge budget, cap holds.
    huge = handler.optimize_visual_budget(
        "picture", total_token_budget=100_000,
    )
    assert huge["max_visual_contexts"] <= 5
    print("cap        : OK")

    # ---- Statistics -------------------------------------------- #
    print("stats      :", {
        k: v for k, v in handler.get_visual_stats().items()
        if k not in ("retrieval_stats",)
    })
    print(
        "retrieval  :",
        handler.get_visual_stats()["retrieval_stats"],
    )

    # ---- avg_relevance running mean is correct ----------------- #
    h1 = VisualContextHandler()
    # First retrieval returns nothing (empty store) — should not
    # drag the average down.
    h1.retrieve_visual_contexts("nothing")
    # Second retrieval: add one context and retrieve it.
    h1.add_visual_context(
        "/x.png", VisualContextType.IMAGE,
        caption="x target",
    )
    h1.retrieve_visual_contexts("target")
    s = h1.get_visual_stats()["retrieval_stats"]
    assert s["total_retrievals"] == 2
    assert s["scored_retrievals"] == 1
    assert s["avg_relevance"] > 0.5  # not dragged down by first
    print(
        f"avg rel    : {s['avg_relevance']:.3f} "
        f"(scored={s['scored_retrievals']})"
    )

    # ---- reset_stats resets scored counter --------------------- #
    h1.reset_stats()
    s = h1.get_visual_stats()["retrieval_stats"]
    assert s["scored_retrievals"] == 0
    assert s["avg_relevance"] == 0.0
    print("reset+avg  : OK")

    # ---- Bounded storage --------------------------------------- #
    bounded = VisualContextHandler(
        config=VisualContextConfig(
            max_contexts=3, max_index_entries=10,
        )
    )
    for i in range(10):
        bounded.add_visual_context(
            f"/path/{i}.png", VisualContextType.IMAGE,
            caption=f"caption {i} extra words here",
        )
    print(
        f"bounded    : contexts={bounded.context_count} "
        f"evictions={bounded._context_evictions} "
        f"index={len(bounded.caption_index)}"
    )
    assert bounded.context_count == 3

    # ---- remove_context / clear ------------------------------- #
    rc = VisualContextHandler()
    c1 = rc.add_visual_context(
        "/a.png", VisualContextType.IMAGE, caption="a",
    )
    c2 = rc.add_visual_context(
        "/b.png", VisualContextType.IMAGE, caption="b",
    )
    assert rc.remove_context(c1) is True
    assert rc.context_count == 1
    assert rc.remove_context(c1) is False
    assert rc.clear() == 1
    assert rc.context_count == 0
    assert rc.get_visual_stats()["removals"] == 2
    print("remove     : OK")

    # ---- Explicit context_id ---------------------------------- #
    explicit = VisualContextHandler()
    eid = explicit.add_visual_context(
        "/x.png", VisualContextType.IMAGE,
        caption="my image", context_id="my_db_key_42",
    )
    assert eid == "my_db_key_42"
    print("explicit id: OK")

    # ---- Word-boundary type bonus ----------------------------- #
    tb = VisualContextHandler()
    tb.add_visual_context(
        "/c.png", VisualContextType.CHART, caption="data",
    )
    chart_ctx = list(tb.visual_contexts.values())[0]
    score_match = tb._type_bonus("chart data", chart_ctx)
    score_false = tb._type_bonus("chartreuse color", chart_ctx)
    assert score_match == 1.0
    assert score_false == 0.0
    print("word bound : OK")

    # ---- Serialization round-trip with contexts --------------- #
    payload = handler.to_json(include_contexts=True)
    restored = VisualContextHandler.from_json(payload)
    assert (
        restored.to_dict(include_contexts=True)
        == handler.to_dict(include_contexts=True)
    )
    print("round-trip : OK (with contexts)")

    # ---- from_dict respects max_contexts ---------------------- #
    many_payload = handler.to_dict(include_contexts=True)
    many_payload["config"] = VisualContextConfig(
        max_contexts=2,
    ).to_dict()
    capped = VisualContextHandler.from_dict(many_payload)
    assert capped.context_count <= 2
    print(
        f"from_dict  : capped to {capped.context_count} "
        f"context(s)"
    )

    # ---- from_dict error wrapping ---------------------------- #
    for bad in (
        {"strict": "yes"},
        {"contexts": "abc"},
        {"caption_index": "not-a-mapping"},
        {"retrieval_history": "not-a-list"},
        {"counters": {"context_evictions": "abc"}},
        {"counters": {"context_evictions": -1}},
        {"contexts": [{"context_id": "x"}]},  # missing fields
        {"config": {"visual_token_cost": True}},
        {"config": {"visual_token_cost": "abc"}},
    ):
        try:
            VisualContextHandler.from_dict(bad)
        except VisualContextError as exc:
            print(
                "Rejected p :", str(exc)[:60], "...",
            )

    # ---- to_json rejects NaN --------------------------------- #
    tainted = VisualContextHandler()
    tainted.add_visual_context(
        "/x.png", VisualContextType.IMAGE,
    )
    object.__setattr__(
        tainted._config,
        "energy_per_token_wh",
        float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    # ---- Dataclass round-trips -------------------------------- #
    ctx = list(handler.visual_contexts.values())[0]
    assert (
        VisualContext.from_dict(ctx.to_dict()).to_dict()
        == ctx.to_dict()
    )
    r = handler.retrieve_visual_contexts("xray")
    assert (
        VisualRetrievalResult.from_dict(r.to_dict()).to_dict()
        == r.to_dict()
    )
    print("dataclasses: OK")

    # ---- align_visual_text validates items -------------------- #
    try:
        handler.align_visual_text(
            "text", [{"not": "a VisualContext"}],  # type: ignore[list-item]
        )
    except VisualContextError as exc:
        print("align valid:", str(exc)[:60])

    # ---- Context manager -------------------------------------- #
    with VisualContextHandler() as scoped:
        scoped.add_visual_context(
            "/a.png", VisualContextType.IMAGE,
        )
        assert scoped.context_count == 1
    print("context    : OK")

    # ---- Config validation ------------------------------------ #
    for bad_cfg in (
        dict(visual_token_cost=0),
        dict(visual_token_cost=True),
        dict(visual_energy_multiplier=0),
        dict(energy_per_token_wh=0),
        dict(max_visual_contexts=0),
        dict(max_contexts=0),
        dict(max_index_entries=0),
        dict(
            weight_caption_overlap=0.9, weight_recency=0.9,
        ),
        dict(weight_caption_overlap=-0.1),
        dict(recency_half_life_hours=0),
        dict(min_alignment_score=1.5),
        dict(default_text_priority=-0.1),
        dict(visual_shift_factor=1.5),
        dict(hash_preview_chars=0),
        dict(max_retrieval_history=0),
    ):
        try:
            VisualContextConfig(
                **bad_cfg  # type: ignore[arg-type]
            )
        except VisualContextError as exc:
            print("Rejected c :", str(exc)[:50])

    # ---- Strict-mode input validation ------------------------- #
    strict = VisualContextHandler(strict=True)
    for bad_call in (
        lambda: strict.add_visual_context(
            "", VisualContextType.IMAGE,
        ),
        lambda: strict.add_visual_context(
            "/x.png", "image",  # type: ignore[arg-type]
        ),
        lambda: strict.add_visual_context(
            "/x.png", VisualContextType.IMAGE,
            caption=123,  # type: ignore[arg-type]
        ),
        lambda: strict.add_visual_context(
            "/x.png", VisualContextType.IMAGE,
            metadata="bad",  # type: ignore[arg-type]
        ),
        lambda: strict.add_visual_context(
            "/x.png", VisualContextType.IMAGE,
            context_id="",
        ),
        lambda: strict.retrieve_visual_contexts(123),  # type: ignore[arg-type]
        lambda: strict.retrieve_visual_contexts(
            "q", max_contexts=0,
        ),
        lambda: strict.retrieve_visual_contexts(
            "q", context_types="charts",  # type: ignore[arg-type]
        ),
        lambda: strict.retrieve_visual_contexts(
            "q", context_types=["chart"],  # type: ignore[list-item]
        ),
        lambda: strict.retrieve_visual_contexts(
            "q", token_budget=-1,
        ),
        lambda: strict.retrieve_visual_contexts(
            "q", energy_budget=float("nan"),
        ),
        lambda: strict.align_visual_text(123, []),  # type: ignore[arg-type]
        lambda: strict.align_visual_text(
            "text", "not-a-list",  # type: ignore[arg-type]
        ),
        lambda: strict.optimize_visual_budget("q", 0),
        lambda: strict.optimize_visual_budget(
            "q", 1000, text_priority=1.5,
        ),
        lambda: strict.remove_context(""),
    ):
        try:
            bad_call()
        except VisualContextError as exc:
            print("Rejected   :", str(exc)[:50])

    # ---- Non-strict coerces ----------------------------------- #
    lenient = VisualContextHandler(strict=False)
    lenient.add_visual_context(
        "/x.png", VisualContextType.IMAGE,
        metadata="bad",  # type: ignore[arg-type]
    )
    print("lenient    : OK")

    print("\nSmoke test passed.")
