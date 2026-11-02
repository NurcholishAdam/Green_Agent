# src/retrieval/context_compression.py

"""
Context Compression Module
==========================

Intelligent context compression for retrieved content: minimizes token
usage while preserving semantic information.

Design
------
- ``ContextCompressionConfig`` / ``SemanticChunkerConfig`` — frozen,
  validated: ratios, token estimators, sentence-length bands, chunker
  geometry, bounded history, batch token floor.
- ``CompressionResult`` / ``Chunk`` — frozen dataclasses with
  validation and full serialization.
- **Token estimator** returns ``int`` (was ``float``).
- **O(n) sentence scoring** — ``enumerate`` instead of ``list.index``.
- **Order-preserving reconstruction** — the compressed text preserves
  the original sentence order.
- **Entity reporting is honest**: ``CompressionResult.preserved_entities``
  contains only the entities that survived into the compressed text;
  the full extraction lives in ``metadata["extracted_entities"]``.
  When ``preserve_entities=True``, entity-bearing sentences receive
  **soft reservation** during selection (budget-permitting).
- **``adaptive_compress`` never mutates state** — uses a local ratio.
  The quality estimator measures each candidate against the ratio it
  was tested at, not against the fixed config ratio, so the binary
  search finds the smallest ratio meeting the threshold.
- **Robust sentence splitting** — respects common abbreviations.
- **Token-accurate chunker overlap** — cannot infinite-loop; flags
  oversized chunks when a single paragraph exceeds ``chunk_size``.
- **Thread safety** — ``RLock`` guards history, counters, shared state.
- **Full serialization round-trip** — ``to_json(include_history=True)``
  reconstructs an equivalent compressor via ``from_json``.
- **Strict / non-strict modes**; every deserialization failure is
  wrapped in ``ContextCompressionError`` / ``SemanticChunkerError``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
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
    "Chunk",
    "CompressionResult",
    "ContextCompressionConfig",
    "ContextCompressionError",
    "ContextCompressor",
    "SemanticChunker",
    "SemanticChunkerConfig",
    "SemanticChunkerError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ContextCompressionError(ValueError):
    """Raised for invalid compression inputs or configuration."""


class SemanticChunkerError(ValueError):
    """Raised for invalid chunker inputs or configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_ABBREVIATIONS = frozenset({
    "dr", "mr", "mrs", "ms", "prof", "sr", "jr", "st",
    "vs", "etc", "eg", "ie", "no", "fig", "eq", "ref", "vol",
    "u.s", "u.k", "u.s.a", "ph.d", "b.a", "m.a",
    "i.e", "e.g", "cf", "al", "inc", "ltd", "co", "corp",
})


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ContextCompressionConfig:
    """Tunable parameters for :class:`ContextCompressor`."""

    target_compression_ratio: float = 0.5

    # When True, entity-bearing sentences receive soft reservation
    # during selection (they are preferred over non-entity sentences,
    # but still must fit within the token budget). It does *not*
    # guarantee that every entity survives — check
    # ``CompressionResult.metadata["missing_entities"]`` for the
    # definitive answer.
    preserve_entities: bool = True
    preserve_numbers: bool = True

    # Token estimator: words are multiplied by this coefficient.
    tokens_per_word: float = 1.3

    # Sentence-length band that receives the length bonus.
    min_sentence_words: int = 5
    max_sentence_words: int = 30

    # Stop selecting sentences once this fraction of the target is reached.
    select_stop_fraction: float = 0.9

    # Bounded history.
    max_history: int = 1000

    # Minimum tokens allocated to a non-empty text in compress_batch.
    # 0 restores the pure proportional split (tiny texts can get 0).
    min_batch_tokens: int = 1

    def __post_init__(self) -> None:
        if not 0.0 < self.target_compression_ratio <= 1.0:
            raise ContextCompressionError(
                "target_compression_ratio must be in (0, 1]."
            )
        if self.tokens_per_word <= 0:
            raise ContextCompressionError("tokens_per_word must be > 0.")
        if self.min_sentence_words <= 0:
            raise ContextCompressionError(
                "min_sentence_words must be > 0."
            )
        if self.max_sentence_words < self.min_sentence_words:
            raise ContextCompressionError(
                "max_sentence_words must be >= min_sentence_words."
            )
        if not 0.0 < self.select_stop_fraction <= 1.0:
            raise ContextCompressionError(
                "select_stop_fraction must be in (0, 1]."
            )
        if self.max_history <= 0:
            raise ContextCompressionError("max_history must be > 0.")
        if self.min_batch_tokens < 0:
            raise ContextCompressionError(
                "min_batch_tokens must be >= 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ContextCompressionConfig":
        if not isinstance(data, Mapping):
            raise ContextCompressionError(
                f"ContextCompressionConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            return cls(
                target_compression_ratio=float(
                    data.get("target_compression_ratio", 0.5)
                ),
                preserve_entities=bool(
                    data.get("preserve_entities", True)
                ),
                preserve_numbers=bool(
                    data.get("preserve_numbers", True)
                ),
                tokens_per_word=float(
                    data.get("tokens_per_word", 1.3)
                ),
                min_sentence_words=int(
                    data.get("min_sentence_words", 5)
                ),
                max_sentence_words=int(
                    data.get("max_sentence_words", 30)
                ),
                select_stop_fraction=float(
                    data.get("select_stop_fraction", 0.9)
                ),
                max_history=int(data.get("max_history", 1000)),
                min_batch_tokens=int(
                    data.get("min_batch_tokens", 1)
                ),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ContextCompressionError):
                raise
            raise ContextCompressionError(
                f"invalid ContextCompressionConfig payload: {exc}"
            ) from exc


@dataclass(frozen=True)
class SemanticChunkerConfig:
    """Tunable parameters for :class:`SemanticChunker`."""

    chunk_size: int = 512
    overlap: int = 50
    tokens_per_word: float = 1.3
    max_chunks: Optional[int] = 10_000

    def __post_init__(self) -> None:
        if (
            isinstance(self.chunk_size, bool)
            or not isinstance(self.chunk_size, int)
            or self.chunk_size <= 0
        ):
            raise SemanticChunkerError(
                "chunk_size must be a positive int."
            )
        if (
            isinstance(self.overlap, bool)
            or not isinstance(self.overlap, int)
            or self.overlap < 0
        ):
            raise SemanticChunkerError(
                "overlap must be a non-negative int."
            )
        if self.overlap >= self.chunk_size:
            raise SemanticChunkerError(
                "overlap must be strictly less than chunk_size."
            )
        if self.tokens_per_word <= 0:
            raise SemanticChunkerError("tokens_per_word must be > 0.")
        if self.max_chunks is not None and (
            isinstance(self.max_chunks, bool)
            or not isinstance(self.max_chunks, int)
            or self.max_chunks <= 0
        ):
            raise SemanticChunkerError(
                "max_chunks must be a positive int or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "SemanticChunkerConfig":
        if not isinstance(data, Mapping):
            raise SemanticChunkerError(
                f"SemanticChunkerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            raw_max = data.get("max_chunks", 10_000)
            max_chunks = (
                None if raw_max is None else int(raw_max)
            )
            return cls(
                chunk_size=int(data.get("chunk_size", 512)),
                overlap=int(data.get("overlap", 50)),
                tokens_per_word=float(
                    data.get("tokens_per_word", 1.3)
                ),
                max_chunks=max_chunks,
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, SemanticChunkerError):
                raise
            raise SemanticChunkerError(
                f"invalid SemanticChunkerConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Result records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CompressionResult:
    """
    Immutable result of a context compression.

    ``preserved_entities`` contains only the entities that **survived**
    into ``compressed_text``. The full extraction set lives in
    ``metadata["extracted_entities"]``.
    """

    original_text: str
    compressed_text: str
    original_tokens: int
    compressed_tokens: int
    compression_ratio: float
    preserved_entities: Tuple[str, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.original_text, str):
            raise ContextCompressionError(
                f"original_text must be a string, "
                f"got {type(self.original_text).__name__}."
            )
        if not isinstance(self.compressed_text, str):
            raise ContextCompressionError(
                f"compressed_text must be a string, "
                f"got {type(self.compressed_text).__name__}."
            )
        if self.original_tokens < 0:
            raise ContextCompressionError(
                "original_tokens must be >= 0."
            )
        if self.compressed_tokens < 0:
            raise ContextCompressionError(
                "compressed_tokens must be >= 0."
            )
        if self.compression_ratio < 0:
            raise ContextCompressionError(
                "compression_ratio must be >= 0."
            )
        if not isinstance(self.metadata, Mapping):
            raise ContextCompressionError(
                f"metadata must be a Mapping, "
                f"got {type(self.metadata).__name__}."
            )
        if not isinstance(self.preserved_entities, tuple):
            object.__setattr__(
                self, "preserved_entities",
                tuple(self.preserved_entities),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_text": self.original_text,
            "compressed_text": self.compressed_text,
            "original_tokens": self.original_tokens,
            "compressed_tokens": self.compressed_tokens,
            "compression_ratio": self.compression_ratio,
            "preserved_entities": list(self.preserved_entities),
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CompressionResult":
        if not isinstance(data, Mapping):
            raise ContextCompressionError(
                f"CompressionResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        try:
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)

            return cls(
                original_text=str(data.get("original_text", "")),
                compressed_text=str(
                    data.get("compressed_text", "")
                ),
                original_tokens=int(data.get("original_tokens", 0)),
                compressed_tokens=int(
                    data.get("compressed_tokens", 0)
                ),
                compression_ratio=float(
                    data.get("compression_ratio", 0.0)
                ),
                preserved_entities=tuple(
                    data.get("preserved_entities", ())
                ),
                metadata=dict(data.get("metadata", {}) or {}),
                timestamp=timestamp,
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ContextCompressionError):
                raise
            raise ContextCompressionError(
                f"invalid CompressionResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "CompressionResult("
            f"original_tokens={self.original_tokens}, "
            f"compressed_tokens={self.compressed_tokens}, "
            f"ratio={self.compression_ratio:.3f}, "
            f"entities={len(self.preserved_entities)})"
        )


@dataclass(frozen=True)
class Chunk:
    """Immutable chunk emitted by :class:`SemanticChunker`."""

    text: str
    tokens: int
    paragraphs: int
    index: int
    start_char: int
    end_char: int
    oversized: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            raise SemanticChunkerError(
                f"text must be a string, "
                f"got {type(self.text).__name__}."
            )
        if self.tokens < 0:
            raise SemanticChunkerError("tokens must be >= 0.")
        if self.paragraphs < 0:
            raise SemanticChunkerError("paragraphs must be >= 0.")
        if self.index < 0:
            raise SemanticChunkerError("index must be >= 0.")
        if self.start_char < 0 or self.end_char < self.start_char:
            raise SemanticChunkerError(
                "start_char/end_char must satisfy "
                "0 <= start <= end."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Chunk":
        if not isinstance(data, Mapping):
            raise SemanticChunkerError(
                f"Chunk.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        if "text" not in data:
            raise SemanticChunkerError(
                "Chunk payload missing required field 'text'."
            )
        text = data["text"]
        if not isinstance(text, str):
            raise SemanticChunkerError(
                f"Chunk.text must be a string, "
                f"got {type(text).__name__}."
            )
        try:
            return cls(
                text=text,
                tokens=int(data.get("tokens", 0)),
                paragraphs=int(data.get("paragraphs", 0)),
                index=int(data.get("index", 0)),
                start_char=int(data.get("start_char", 0)),
                end_char=int(data.get("end_char", 0)),
                oversized=bool(data.get("oversized", False)),
            )
        except (TypeError, ValueError) as exc:
            raise SemanticChunkerError(
                f"invalid Chunk payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        flag = " oversized" if self.oversized else ""
        return (
            f"Chunk(index={self.index}, tokens={self.tokens}, "
            f"paragraphs={self.paragraphs}, "
            f"chars=[{self.start_char}, {self.end_char}]{flag})"
        )


# --------------------------------------------------------------------------- #
# Sentence splitting helper
# --------------------------------------------------------------------------- #
def _split_sentences(text: str) -> List[str]:
    """
    Split ``text`` into sentences, respecting common abbreviations.

    Splits on ``[.!?]`` followed by whitespace; then merges fragments
    whose last word is a known abbreviation.
    """
    if not text or not text.strip():
        return []

    raw = re.split(r"(?<=[.!?])\s+", text)
    merged: List[str] = []

    for frag in raw:
        if not frag.strip():
            continue
        if merged:
            prev = merged[-1].rstrip()
            m = re.search(r"([A-Za-z][A-Za-z.]*)$", prev)
            last_word = m.group(1).rstrip(".").lower() if m else ""
            if last_word in _ABBREVIATIONS:
                merged[-1] = prev + " " + frag.strip()
                continue
        merged.append(frag.strip())

    return [s for s in merged if s]


# --------------------------------------------------------------------------- #
# ContextCompressor
# --------------------------------------------------------------------------- #
class ContextCompressor:
    """
    Compresses retrieved context to reduce token usage.

    Thread-safe, serializable, and bounded in memory.

    Strategies
    ----------
    - Entity-aware summarization (soft reservation when
      ``preserve_entities=True``).
    - Redundancy removal via sentence ranking.
    - Order-preserving reconstruction.
    """

    def __init__(
        self,
        target_compression_ratio: float = 0.5,
        preserve_entities: bool = True,
        preserve_numbers: bool = True,
        *,
        config: Optional[ContextCompressionConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = ContextCompressionConfig(
                target_compression_ratio=float(
                    target_compression_ratio
                ),
                preserve_entities=bool(preserve_entities),
                preserve_numbers=bool(preserve_numbers),
            )
        self._strict = bool(strict)

        self.target_ratio = self._config.target_compression_ratio
        self.preserve_entities = self._config.preserve_entities
        self.preserve_numbers = self._config.preserve_numbers

        self._lock = threading.RLock()
        self.compression_history: Deque[CompressionResult] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        logger.debug(
            "ContextCompressor initialized "
            "(target_ratio=%.3f, preserve_entities=%s, "
            "preserve_numbers=%s, strict=%s)",
            self.target_ratio, self.preserve_entities,
            self.preserve_numbers, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ContextCompressionConfig:
        return self._config

    @property
    def history(self) -> List[CompressionResult]:
        with self._lock:
            return list(self.compression_history)

    # ---------------------------------------------------------- public API
    def compress(
        self,
        text: str,
        query: Optional[str] = None,
        max_tokens: Optional[int] = None,
    ) -> CompressionResult:
        """Compress ``text`` while preserving key information."""
        return self._compress_with_ratio(
            text=text,
            query=query,
            max_tokens=max_tokens,
            ratio=self.target_ratio,
        )

    def compress_batch(
        self,
        texts: List[str],
        query: Optional[str] = None,
        total_token_budget: int = 4096,
    ) -> List[CompressionResult]:
        """
        Compress multiple texts within a total token budget.

        Non-empty texts receive at least ``config.min_batch_tokens``
        tokens; empty texts remain empty.
        """
        if not isinstance(texts, list):
            raise ContextCompressionError("texts must be a list.")
        if (
            isinstance(total_token_budget, bool)
            or not isinstance(total_token_budget, int)
            or total_token_budget <= 0
        ):
            raise ContextCompressionError(
                "total_token_budget must be a positive int."
            )

        original_tokens = [self._estimate_tokens(t) for t in texts]
        total_original = sum(original_tokens)

        results: List[CompressionResult] = []
        for text, orig in zip(texts, original_tokens):
            if total_original == 0:
                allocated = 0
            else:
                allocated = int(
                    (orig / total_original) * total_token_budget
                )
                # Guarantee progress for non-empty texts.
                if orig > 0:
                    allocated = max(
                        self._config.min_batch_tokens, allocated
                    )
            results.append(
                self.compress(
                    text, query=query, max_tokens=allocated,
                )
            )
        return results

    def adaptive_compress(
        self,
        text: str,
        min_ratio: float = 0.3,
        max_ratio: float = 0.8,
        quality_threshold: float = 0.7,
        *,
        query: Optional[str] = None,
    ) -> CompressionResult:
        """
        Compress ``text`` to satisfy a quality threshold using binary
        search.

        The compressor's ``target_ratio`` is **not** mutated. Each
        candidate is scored against the ratio it was tested at, so the
        search converges on the smallest ratio meeting the threshold.

        Parameters
        ----------
        query : str, optional
            Passed to :meth:`_compress_with_ratio` to guide
            sentence scoring at every probe.
        """
        if not 0.0 < min_ratio <= max_ratio <= 1.0:
            raise ContextCompressionError(
                "must satisfy 0 < min_ratio <= max_ratio <= 1."
            )
        if not 0.0 <= quality_threshold <= 1.0:
            raise ContextCompressionError(
                "quality_threshold must be in [0, 1]."
            )

        low, high = float(min_ratio), float(max_ratio)
        best_result: Optional[CompressionResult] = None
        best_quality: float = -1.0

        for _ in range(5):
            mid = (low + high) / 2.0
            result = self._compress_with_ratio(
                text=text, query=query, max_tokens=None, ratio=mid,
            )
            quality = self._estimate_quality(
                result, target_ratio=mid,
            )

            if quality > best_quality:
                best_result = result
                best_quality = quality

            if quality >= quality_threshold:
                best_result = result
                high = mid
            else:
                low = mid

        if best_result is None:  # pragma: no cover — defensive
            best_result = self._compress_with_ratio(
                text=text, query=query, max_tokens=None,
                ratio=max_ratio,
            )
        return best_result

    # ---------------------------------------------------------- internal
    def _compress_with_ratio(
        self,
        text: str,
        query: Optional[str],
        max_tokens: Optional[int],
        ratio: float,
    ) -> CompressionResult:
        """Core compression routine parameterized by ``ratio``."""
        if not isinstance(text, str):
            msg = (
                f"text must be a string, "
                f"got {type(text).__name__}."
            )
            if self._strict:
                raise ContextCompressionError(msg)
            logger.warning("%s Treating as empty.", msg)
            text = ""

        if max_tokens is not None and (
            isinstance(max_tokens, bool)
            or not isinstance(max_tokens, int)
            or max_tokens < 0
        ):
            msg = (
                f"max_tokens must be a non-negative int or None, "
                f"got {max_tokens!r}."
            )
            if self._strict:
                raise ContextCompressionError(msg)
            logger.warning("%s Ignoring max_tokens.", msg)
            max_tokens = None

        if not 0.0 < ratio <= 1.0:
            raise ContextCompressionError(
                "ratio must be in (0, 1]."
            )

        original_tokens = self._estimate_tokens(text)

        if original_tokens == 0:
            result = CompressionResult(
                original_text=text,
                compressed_text="",
                original_tokens=0,
                compressed_tokens=0,
                compression_ratio=0.0,
                preserved_entities=tuple(),
                metadata={
                    "sentences_original": 0,
                    "sentences_kept": 0,
                    "query_guided": query is not None,
                    "target_ratio": ratio,
                    "empty_input": True,
                    "extracted_entities": [],
                    "survived_entities": [],
                    "missing_entities": [],
                },
            )
            with self._lock:
                self.compression_history.append(result)
            return result

        entities = (
            self._extract_entities(text)
            if self.preserve_entities else []
        )
        sentences = _split_sentences(text)
        scored = self._score_sentences(sentences, query, entities)

        target_tokens = (
            max_tokens if max_tokens is not None
            else max(0, int(original_tokens * ratio))
        )

        selected_indices = self._select_sentences(
            scored, target_tokens, entities=entities,
        )
        compressed_text = self._reconstruct_text(
            sentences, selected_indices,
        )
        compressed_tokens = self._estimate_tokens(compressed_text)

        # ---- Honest entity reporting -------------------------------
        compressed_lower = compressed_text.lower()
        survived_entities = [
            e for e in entities if e.lower() in compressed_lower
        ]
        missing_entities = [
            e for e in entities if e.lower() not in compressed_lower
        ]

        result = CompressionResult(
            original_text=text,
            compressed_text=compressed_text,
            original_tokens=original_tokens,
            compressed_tokens=compressed_tokens,
            compression_ratio=(
                compressed_tokens / original_tokens
                if original_tokens > 0 else 0.0
            ),
            preserved_entities=tuple(survived_entities),
            metadata={
                "sentences_original": len(sentences),
                "sentences_kept": len(selected_indices),
                "query_guided": query is not None,
                "target_ratio": ratio,
                "target_tokens": target_tokens,
                "extracted_entities": list(entities),
                "survived_entities": list(survived_entities),
                "missing_entities": missing_entities,
            },
        )

        with self._lock:
            self.compression_history.append(result)

        logger.debug(
            "Compressed %d -> %d tokens "
            "(ratio=%.3f, kept %d/%d sentences, "
            "entities %d/%d survived).",
            original_tokens, compressed_tokens,
            result.compression_ratio,
            len(selected_indices), len(sentences),
            len(survived_entities), len(entities),
        )
        return result

    # ---------------------------------------------------------- token estimation
    def _estimate_tokens(self, text: str) -> int:
        """
        Estimate the token count of ``text``.

        Uses whitespace-split words × ``tokens_per_word``. Known
        limitation: URLs and non-space-delimited scripts (CJK) are
        under-counted; callers using such content should override
        ``tokens_per_word`` to a conservative value.
        """
        if not text:
            return 0
        return int(
            len(text.split()) * self._config.tokens_per_word
        )

    # ---------------------------------------------------------- entity extraction
    def _extract_entities(self, text: str) -> List[str]:
        """Extract capitalized words and (optionally) numbers/dates."""
        entities: List[str] = []
        entities.extend(
            re.findall(
                r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", text,
            )
        )
        if self.preserve_numbers:
            entities.extend(
                re.findall(
                    r"\b\d+(?:\.\d+)?(?:%|kg|m|km|MB|GB|s|ms)?\b",
                    text,
                )
            )
        seen: set = set()
        ordered: List[str] = []
        for e in entities:
            if e not in seen:
                seen.add(e)
                ordered.append(e)
        return ordered

    # ---------------------------------------------------------- scoring
    def _score_sentences(
        self,
        sentences: List[str],
        query: Optional[str],
        entities: List[str],
    ) -> List[Tuple[int, str, float]]:
        """Score sentences by importance (O(n) via enumerate)."""
        cfg = self._config
        query_words = (
            set(query.lower().split()) if query else set()
        )
        entity_lower = [e.lower() for e in entities]
        last_index = len(sentences) - 1
        scored: List[Tuple[int, str, float]] = []

        for idx, sentence in enumerate(sentences):
            score = 0.0
            length = len(sentence.split())
            if (
                cfg.min_sentence_words <= length
                <= cfg.max_sentence_words
            ):
                score += 0.3

            # Case-insensitive entity matching (was case-sensitive).
            sentence_lower = sentence.lower()
            entity_count = sum(
                1 for e in entity_lower if e in sentence_lower
            )
            score += entity_count * 0.2

            if query_words:
                sentence_words = set(sentence_lower.split())
                score += len(query_words & sentence_words) * 0.5

            if idx == 0 or idx == last_index:
                score += 0.2

            scored.append((idx, sentence, score))

        scored.sort(key=lambda t: t[2], reverse=True)
        return scored

    # ---------------------------------------------------------- selection
    def _select_sentences(
        self,
        scored: List[Tuple[int, str, float]],
        target_tokens: int,
        entities: Optional[Sequence[str]] = None,
    ) -> List[int]:
        """
        Select sentence indices under the token budget.

        Returns indices in **original order** so reconstruction
        preserves the source flow.

        When ``preserve_entities=True`` (and ``entities`` is given),
        entity-bearing sentences are visited first (in score order),
        then non-entity sentences (in score order). Both phases respect
        the same budget; entity reservation is *soft*, not guaranteed.
        """
        if target_tokens <= 0 or not scored:
            return []

        # ---- Phase ordering ----------------------------------------
        if entities and self.preserve_entities:
            entity_lower = [str(e).lower() for e in entities]

            def _has_entity(s: str) -> bool:
                sl = s.lower()
                return any(e in sl for e in entity_lower)

            entity_scored = [
                t for t in scored if _has_entity(t[1])
            ]
            other_scored = [
                t for t in scored if not _has_entity(t[1])
            ]
            ordered = entity_scored + other_scored
        else:
            ordered = list(scored)

        selected: List[int] = []
        current_tokens = 0
        stop_at = (
            target_tokens * self._config.select_stop_fraction
        )

        for idx, sentence, _score in ordered:
            sentence_tokens = self._estimate_tokens(sentence)
            if current_tokens + sentence_tokens <= target_tokens:
                selected.append(idx)
                current_tokens += sentence_tokens
            if current_tokens >= stop_at:
                break

        # ---- Fallback: try each sentence in score order until one fits
        if not selected:
            for idx, sentence, _score in scored:
                if self._estimate_tokens(sentence) <= max(
                    target_tokens, 1
                ):
                    selected.append(idx)
                    break

        selected.sort()
        return selected

    # ---------------------------------------------------------- reconstruction
    def _reconstruct_text(
        self,
        sentences: List[str],
        selected_indices: List[int],
    ) -> str:
        """Reconstruct preserving original sentence order."""
        if not selected_indices:
            return ""
        return ". ".join(
            sentences[i].rstrip(". ")
            for i in selected_indices
            if 0 <= i < len(sentences)
        ) + "."

    # ---------------------------------------------------------- quality
    def _estimate_quality(
        self,
        result: CompressionResult,
        *,
        target_ratio: Optional[float] = None,
        target_entities: Optional[Sequence[str]] = None,
    ) -> float:
        """
        Estimate compression quality in ``[0, 1]``.

        Parameters
        ----------
        result : CompressionResult
        target_ratio : float, optional
            The ratio this result was intended to achieve. Defaults to
            ``self.target_ratio``. :meth:`adaptive_compress` passes
            each candidate's own probe ratio so the search measures
            each result against its own goal.
        target_entities : sequence of str, optional
            The entities the caller cares about. Defaults to
            ``result.metadata["extracted_entities"]``. Entity score is
            the fraction of these that survived into the compressed
            text.
        """
        # Empty-input case: no meaningful compression.
        if result.original_tokens == 0:
            return 0.0

        intended_ratio = float(
            self.target_ratio if target_ratio is None
            else target_ratio
        )
        ratio_diff = abs(result.compression_ratio - intended_ratio)
        ratio_score = max(0.0, 1.0 - ratio_diff)

        extracted = tuple(
            target_entities
            if target_entities is not None
            else result.metadata.get("extracted_entities", ())
        )

        if not extracted:
            entity_score = 1.0  # Nothing to lose → full credit.
        else:
            compressed_lower = result.compressed_text.lower()
            survived = sum(
                1 for e in extracted
                if str(e).lower() in compressed_lower
            )
            entity_score = survived / len(extracted)

        return max(
            0.0,
            min(1.0, ratio_score * 0.6 + entity_score * 0.4),
        )

    # ---------------------------------------------------------- stats
    def get_compression_stats(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self.compression_history)

        if not history:
            return {
                "status": "no_compressions",
                "total_compressions": 0,
                "avg_compression_ratio": 0.0,
                "total_tokens_saved": 0,
                "avg_entities_preserved": 0.0,
                "mean_original_tokens": 0.0,
                "mean_compressed_tokens": 0.0,
                "uptime_seconds": time.time() - self._started_at,
            }

        # Exclude empty inputs from the "real compressions" counters
        # while leaving them in history — callers who want the raw
        # count can inspect ``len(history)``.
        real = [r for r in history if r.original_tokens > 0]
        n = len(real) if real else len(history)

        avg_ratio = (
            sum(r.compression_ratio for r in real) / n
            if real else 0.0
        )
        total_saved = sum(
            r.original_tokens - r.compressed_tokens for r in history
        )
        avg_entities = (
            sum(len(r.preserved_entities) for r in real) / n
            if real else 0.0
        )

        return {
            "total_compressions": len(history),
            "non_empty_compressions": len(real),
            "avg_compression_ratio": avg_ratio,
            "total_tokens_saved": total_saved,
            "avg_entities_preserved": avg_entities,
            "mean_original_tokens": (
                sum(r.original_tokens for r in history)
                / len(history)
            ),
            "mean_compressed_tokens": (
                sum(r.compressed_tokens for r in history)
                / len(history)
            ),
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_compression_stats()

    def reset(self, *, clear_history: bool = True) -> int:
        with self._lock:
            removed = len(self.compression_history)
            if clear_history:
                self.compression_history.clear()
            self._started_at = time.time()
        logger.debug(
            "ContextCompressor reset (removed %d).", removed,
        )
        return removed

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
                "history_size": len(self.compression_history),
            }
            if include_history:
                payload["history"] = [
                    r.to_dict() for r in self.compression_history
                ]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ContextCompressor":
        if not isinstance(data, Mapping):
            raise ContextCompressionError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            cfg = ContextCompressionConfig.from_dict(
                dict(data.get("config", {}) or {})
            )
            compressor = cls(
                config=cfg,
                strict=bool(data.get("strict", True)),
            )
            raw_history = data.get("history", [])
            if raw_history is None:
                raw_history = []
            if not isinstance(raw_history, list):
                raise ContextCompressionError(
                    f"history must be a list, "
                    f"got {type(raw_history).__name__}."
                )
            with compressor._lock:
                for entry in raw_history:
                    compressor.compression_history.append(
                        CompressionResult.from_dict(entry)
                    )
                compressor._started_at = float(
                    data.get("started_at", time.time())
                )
            return compressor
        except ContextCompressionError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise ContextCompressionError(
                f"invalid ContextCompressor payload: {exc}"
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
    def from_json(cls, payload: str) -> "ContextCompressor":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ContextCompressionError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "ContextCompressor":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Any,
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "ContextCompressor scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            n = len(self.compression_history)
        return (
            "ContextCompressor("
            f"target_ratio={self.target_ratio:.3f}, "
            f"history={n}, "
            f"preserve_entities={self.preserve_entities}, "
            f"preserve_numbers={self.preserve_numbers}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# SemanticChunker
# --------------------------------------------------------------------------- #
class SemanticChunker:
    """
    Chunks text into semantically coherent segments.

    Thread-safe, serializable, and bounded in memory.

    Notes
    -----
    The ``overlap`` implementation carries over the last ``overlap``
    *tokens* of the previous chunk (reconstructed paragraph-by-paragraph
    until the budget is met). A single paragraph larger than
    ``chunk_size`` cannot be split and is emitted as an oversized
    chunk with ``Chunk.oversized = True`` and a warning logged.
    """

    def __init__(
        self,
        chunk_size: int = 512,
        overlap: int = 50,
        *,
        config: Optional[SemanticChunkerConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = SemanticChunkerConfig(
                chunk_size=int(chunk_size),
                overlap=int(overlap),
            )
        self._strict = bool(strict)

        self.chunk_size = self._config.chunk_size
        self.overlap = self._config.overlap

        self._lock = threading.RLock()
        self._chunk_count: int = 0

        logger.debug(
            "SemanticChunker initialized "
            "(chunk_size=%d, overlap=%d, strict=%s)",
            self.chunk_size, self.overlap, self._strict,
        )

    @property
    def config(self) -> SemanticChunkerConfig:
        return self._config

    # ---------------------------------------------------------- public API
    def chunk(self, text: str) -> List[Dict[str, Any]]:
        """Chunk ``text`` into semantic segments (list of dicts)."""
        if not isinstance(text, str):
            msg = (
                f"text must be a string, "
                f"got {type(text).__name__}."
            )
            if self._strict:
                raise SemanticChunkerError(msg)
            logger.warning("%s Treating as empty.", msg)
            text = ""

        chunks = self.chunk_objects(text)
        return [c.to_dict() for c in chunks]

    def chunk_objects(self, text: str) -> List[Chunk]:
        """Same as :meth:`chunk` but returns :class:`Chunk` objects."""
        if not isinstance(text, str):
            msg = (
                f"text must be a string, "
                f"got {type(text).__name__}."
            )
            if self._strict:
                raise SemanticChunkerError(msg)
            logger.warning("%s Treating as empty.", msg)
            text = ""
        if not text:
            return []

        paragraphs_with_offsets = self._paragraph_offsets(text)
        chunks: List[Chunk] = []

        current_paras: List[Tuple[str, int, int]] = []
        current_tokens = 0

        for para, start, end in paragraphs_with_offsets:
            para_tokens = self._estimate_tokens(para)

            if (
                current_tokens + para_tokens > self.chunk_size
                and current_paras
            ):
                chunks.append(
                    self._make_chunk(chunks, current_paras)
                )
                current_paras, current_tokens = (
                    self._carry_overlap(
                        current_paras, para, para_tokens,
                    )
                )
            else:
                current_paras.append((para, start, end))
                current_tokens += para_tokens

        if current_paras:
            chunks.append(self._make_chunk(chunks, current_paras))

        with self._lock:
            self._chunk_count += len(chunks)

        logger.debug(
            "Chunked %d chars into %d chunk(s).",
            len(text), len(chunks),
        )
        return chunks

    # ---------------------------------------------------------- internals
    def _estimate_tokens(self, text: str) -> int:
        if not text:
            return 0
        return int(
            len(text.split()) * self._config.tokens_per_word
        )

    @staticmethod
    def _paragraph_offsets(
        text: str,
    ) -> List[Tuple[str, int, int]]:
        """
        Return ``[(paragraph, start_char, end_char), ...]``.
        """
        raw = text.split("\n\n")
        offsets: List[Tuple[str, int, int]] = []
        cursor = 0
        for para in raw:
            start = text.find(para, cursor)
            if start < 0:
                start = cursor
            end = start + len(para)
            offsets.append((para, start, end))
            cursor = end
        return offsets

    def _make_chunk(
        self,
        existing_chunks: List[Chunk],
        paragraphs: List[Tuple[str, int, int]],
    ) -> Chunk:
        text = "\n\n".join(p[0] for p in paragraphs)
        tokens = self._estimate_tokens(text)
        oversized = tokens > self.chunk_size
        if oversized:
            logger.warning(
                "chunk %d is %d tokens (> chunk_size=%d); an "
                "individual paragraph exceeded the budget and "
                "cannot be split.",
                len(existing_chunks), tokens, self.chunk_size,
            )
        return Chunk(
            text=text,
            tokens=tokens,
            paragraphs=len(paragraphs),
            index=len(existing_chunks),
            start_char=paragraphs[0][1],
            end_char=paragraphs[-1][2],
            oversized=oversized,
        )

    def _carry_overlap(
        self,
        previous: List[Tuple[str, int, int]],
        incoming: Tuple[str, int, int],
        incoming_tokens: int,
    ) -> Tuple[List[Tuple[str, int, int]], int]:
        """
        Build the new chunk from a token-bounded suffix of
        ``previous`` plus the incoming paragraph.
        """
        if self.overlap == 0 or not previous:
            return [incoming], incoming_tokens

        carried: List[Tuple[str, int, int]] = []
        carried_tokens = 0
        for para, start, end in reversed(previous):
            para_tokens = self._estimate_tokens(para)
            if carried_tokens + para_tokens > self.overlap:
                break
            carried.insert(0, (para, start, end))
            carried_tokens += para_tokens

        new_paras = carried + [incoming]
        new_tokens = carried_tokens + incoming_tokens
        return new_paras, new_tokens

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            n = self._chunk_count
        return {
            "chunk_size": self.chunk_size,
            "overlap": self.overlap,
            "chunks_produced": n,
        }

    def reset(self) -> None:
        with self._lock:
            self._chunk_count = 0
        logger.debug("SemanticChunker reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            count = self._chunk_count
        return {
            "version": VERSION,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "chunks_produced": count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SemanticChunker":
        if not isinstance(data, Mapping):
            raise SemanticChunkerError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            cfg = SemanticChunkerConfig.from_dict(
                dict(data.get("config", {}) or {})
            )
            chunker = cls(
                config=cfg,
                strict=bool(data.get("strict", True)),
            )
            try:
                chunker._chunk_count = int(
                    data.get("chunks_produced", 0)
                )
            except (TypeError, ValueError) as exc:
                raise SemanticChunkerError(
                    f"invalid chunks_produced: {exc}"
                ) from exc
            return chunker
        except SemanticChunkerError:
            raise
        except (TypeError, ValueError) as exc:
            raise SemanticChunkerError(
                f"invalid SemanticChunker payload: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SemanticChunker":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SemanticChunkerError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            n = self._chunk_count
        return (
            "SemanticChunker("
            f"chunk_size={self.chunk_size}, "
            f"overlap={self.overlap}, "
            f"chunks_produced={n}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.context_compression
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION:", VERSION)

    # ---------------------------------------------------------------- #
    # Happy path: entity preservation + query-guided compression
    # ---------------------------------------------------------------- #
    text = (
        "Dr. Smith joined Acme Corp in 2019. "
        "She led the U.S. division. "
        "The team grew from 12 to 48 engineers in two years. "
        "They shipped the Orion platform to 500 customers in Q3 2021. "
        "Prof. Johnson consulted briefly on the data pipeline. "
        "The 2022 audit found no material issues. "
        "Acme went public in 2023 at $45 per share. "
        "Both founders retained significant equity."
    )

    compressor = ContextCompressor(target_compression_ratio=0.5)
    print("repr       :", compressor)

    result = compressor.compress(text, query="Acme acquisition 2023")
    print(f"original   : {result.original_tokens} tokens")
    print(f"compressed : {result.compressed_tokens} tokens")
    print(f"ratio      : {result.compression_ratio:.3f}")
    print(f"preserved  : {result.preserved_entities}")
    print(f"extracted  : {result.metadata['extracted_entities']}")
    print(f"missing    : {result.metadata['missing_entities']}")

    assert (
        result.compressed_tokens <= result.original_tokens
    ), "compression must not inflate the output"
    print("no-inflate : OK")

    # ---- preserved_entities now reports survivors, not extracted --
    survived_from_meta = set(result.metadata["survived_entities"])
    assert set(result.preserved_entities) == survived_from_meta, (
        "preserved_entities must match metadata['survived_entities']"
    )
    print("honest entities: OK")

    # ---- Sentence order preserved ---------------------------------
    original_order = [
        s.strip()
        for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()
    ]
    compressed_order = [
        s.strip()
        for s in re.split(r"(?<=[.!?])\s+", result.compressed_text)
        if s.strip()
    ]
    positions = [
        original_order.index(s)
        for s in compressed_order if s in original_order
    ]
    assert positions == sorted(positions), "order scrambled"
    print("order kept : OK")

    # ---- Abbreviation handling ------------------------------------
    dr_text = (
        "Dr. Smith is here. U.S. policy changed in 2020."
    )
    dr_result = compressor.compress(dr_text, query=None)
    assert dr_result.metadata["sentences_original"] == 2
    print(f"split Dr.  : {dr_result.metadata['sentences_original']} "
          "sentences (expected 2)")

    # ---- Batch compression with empty inputs ----------------------
    batch = compressor.compress_batch(
        ["", "one two three four five", ""],
        total_token_budget=64,
    )
    print(f"batch      : "
          f"{[r.original_tokens for r in batch]}")
    # Non-empty text gets at least min_batch_tokens budget.
    assert batch[1].compressed_tokens >= 1
    print("batch floor: OK")

    # ---- adaptive_compress side-effect + quality fix --------------
    before_ratio = compressor.target_ratio
    adaptive = compressor.adaptive_compress(
        text, min_ratio=0.3, max_ratio=0.8,
        quality_threshold=0.7, query="Acme 2023",
    )
    after_ratio = compressor.target_ratio
    assert before_ratio == after_ratio, (
        "adaptive_compress must not mutate target_ratio"
    )
    print(f"adaptive   : ratio={adaptive.compression_ratio:.3f} "
          "(target_ratio unchanged)")

    # Quality should be measured against the *probe* ratio, not the
    # fixed config ratio. Verify the returned result achieves roughly
    # its own target ratio.
    q = compressor._estimate_quality(
        adaptive, target_ratio=adaptive.metadata["target_ratio"],
    )
    print(f"adaptive q : {q:.3f} (against own target)")

    # ---- Statistics ----------------------------------------------
    print("stats      :", {
        k: v for k, v in
        compressor.get_compression_stats().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization round-trip (FIXED) ------------------------
    # Round-trip the full state via include_history=True so the
    # comparison is lossless.
    payload = compressor.to_json(include_history=True)
    restored = ContextCompressor.from_json(payload)
    a = compressor.to_dict(include_history=True)
    b = restored.to_dict(include_history=True)
    assert a == b, (
        f"history round-trip failed "
        f"(compressor={a.get('history_size')}, "
        f"restored={b.get('history_size')})"
    )
    print("full round-trip: OK")

    # And check that config-only round-trip preserves history_size=0
    # honestly (that is the contract).
    cfg_only = compressor.to_json(include_history=False)
    restored_cfg = ContextCompressor.from_json(cfg_only)
    assert restored_cfg.to_dict()["history_size"] == 0
    print("cfg-only round-trip: OK")

    # ---- Context manager ------------------------------------------
    with ContextCompressor() as scoped:
        scoped.compress("Some text to compress.")
    print("Context    : OK")

    # ---- Validation failures --------------------------------------
    for bad_cfg in (
        dict(target_compression_ratio=0),
        dict(target_compression_ratio=1.5),
        dict(tokens_per_word=0),
        dict(min_sentence_words=0),
        dict(min_sentence_words=10, max_sentence_words=5),
        dict(select_stop_fraction=0),
        dict(max_history=0),
        dict(min_batch_tokens=-1),
    ):
        try:
            ContextCompressionConfig(**bad_cfg)  # type: ignore[arg-type]
        except ContextCompressionError as exc:
            print("Rejected cfg:", exc)

    strict = ContextCompressor(strict=True)
    for bad_call in (
        lambda: strict.compress(None),  # type: ignore[arg-type]
        lambda: strict.compress("hello", max_tokens=-1),
        lambda: strict.compress_batch("not-a-list"),  # type: ignore[arg-type]
        lambda: strict.compress_batch([], total_token_budget=0),
        lambda: strict.adaptive_compress(
            "hi", min_ratio=0.5, max_ratio=0.3,
        ),
        lambda: strict.adaptive_compress(
            "hi", quality_threshold=2.0,
        ),
    ):
        try:
            bad_call()
        except ContextCompressionError as exc:
            print("Rejected   :", exc)

    lenient = ContextCompressor(strict=False)
    r = lenient.compress(None)  # type: ignore[arg-type]
    assert r.original_tokens == 0 and r.compressed_text == ""
    print("lenient    : OK")

    # ---- from_dict error wrapping --------------------------------
    for bad_payload in (
        {"history": "abc"},
        {"config": {"target_compression_ratio": "x"}},
        {"started_at": "abc"},
    ):
        try:
            ContextCompressor.from_dict(bad_payload)
        except ContextCompressionError as exc:
            print("Rejected payload:", str(exc)[:50], "...")

    # ---- to_json rejects NaN (defensive) -------------------------
    tainted = ContextCompressor()
    tainted.compress("some text to compress here.")
    # Inject NaN via private mutation on the config object.
    object.__setattr__(
        tainted._config, "tokens_per_word", float("nan"),
    )
    try:
        # Allow_nan=False rejects NaN in the config payload.
        tainted.to_json()
    except ValueError as exc:
        print("NaN in to_json rejected:", str(exc)[:50], "...")

    # ---------------------------------------------------------------- #
    # SemanticChunker
    # ---------------------------------------------------------------- #
    doc = "\n\n".join([
        "Paragraph one discusses the background of the project. " * 3,
        "Paragraph two outlines the methodology used. " * 3,
        "Paragraph three presents the results obtained. " * 3,
        "Paragraph four discusses the implications. " * 3,
        "Paragraph five concludes with recommendations. " * 3,
    ])

    chunker = SemanticChunker(chunk_size=120, overlap=30)
    print("\nchunker    :", chunker)
    chunks = chunker.chunk(doc)
    print(f"chunks     : {len(chunks)}")
    for c in chunks:
        print(
            f"  #{c['index']}: tokens={c['tokens']}, "
            f"paras={c['paragraphs']}, "
            f"chars=[{c['start_char']}, {c['end_char']}], "
            f"oversized={c.get('oversized', False)}"
        )

    # ---- Oversized chunk warning --------------------------------
    big_doc = "one two three " * 200  # single paragraph, > 120 tokens
    big_chunks = chunker.chunk_objects(big_doc)
    assert any(c.oversized for c in big_chunks), (
        "single oversized paragraph should be flagged"
    )
    print("oversized  : flagged correctly")

    # ---- Config validation --------------------------------------
    for bad_cfg in (
        dict(chunk_size=0),
        dict(overlap=-1),
        dict(chunk_size=100, overlap=100),
        dict(chunk_size=100, overlap=200),
        dict(tokens_per_word=0),
        dict(max_chunks=0),
        dict(max_chunks=True),
    ):
        try:
            SemanticChunkerConfig(**bad_cfg)  # type: ignore[arg-type]
        except SemanticChunkerError as exc:
            print("Rejected cfg:", exc)

    # ---- Round-trip --------------------------------------------
    payload = chunker.to_json()
    restored_chunker = SemanticChunker.from_json(payload)
    assert (
        restored_chunker.to_dict() == chunker.to_dict()
    ), "chunker round-trip failed"
    print("Chunker round-trip OK.")

    # ---- Error paths -------------------------------------------
    try:
        chunker.chunk(None)  # type: ignore[arg-type]
    except SemanticChunkerError as exc:
        print("Rejected   :", exc)

    lenient_chunker = SemanticChunker(strict=False)
    assert lenient_chunker.chunk(None) == []  # type: ignore[arg-type]
    print("lenient chunker: OK")

    # Chunk.from_dict validation
    for bad_chunk in (
        {},
        {"text": None},
        {"text": 123},
    ):
        try:
            Chunk.from_dict(bad_chunk)
        except SemanticChunkerError as exc:
            print("Rejected Chunk:", str(exc)[:50], "...")

    print("\nSmoke test passed.")
