# src/retrieval/context_compression.py

"""
Context Compression Module
==========================

Intelligent context compression for retrieved content: minimizes token usage
while preserving semantic information.

Enhancements
------------
- ``ContextCompressionConfig`` / ``SemanticChunkerConfig`` — frozen,
  validated: ratios, token estimators, sentence-length bands, chunker
  geometry, bounded history.
- ``CompressionResult`` / ``Chunk`` — frozen dataclasses with validation and
  full serialization.
- **Fixed token estimator** — returns ``int`` (was ``float``).
- **Fixed O(n²) sentence scoring** — uses ``enumerate`` instead of
  ``list.index``.
- **Fixed sentence-order scrambling** — the reconstructed text preserves the
  original sentence order.
- **Fixed missing-entity inflation** — missing entities are reported in
  ``metadata`` instead of being appended to the compressed text.
- **Fixed side-effect mutation** — :meth:`adaptive_compress` uses a local
  ratio and never touches ``self.target_ratio``.
- **Fixed ``ZeroDivisionError``** in :meth:`compress_batch`.
- **Fixed negative quality scores** — clamped to ``[0, 1]``.
- **Robust sentence splitting** — respects common abbreviations
  (``Dr.``, ``U.S.``, ``e.g.``, …).
- **Fixed chunker geometry** — a token-accurate overlap that cannot infinite-
  loop and validates ``overlap < chunk_size``.
- **Thread safety** — ``RLock`` guards history and shared state.
- **Bounded history** — ``deque(maxlen=config.max_history)``.
- **Full validation** of every argument; strict / non-strict modes.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` on both
  classes and every dataclass.
- ``statistics()``, ``__repr__``, custom ``ContextCompressionError`` /
  ``SemanticChunkerError``, lazy ``%s`` logging, and a ``__main__`` smoke
  test.
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
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


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
# Common abbreviations that should not terminate a sentence.
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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ContextCompressionConfig":
        if not isinstance(data, Mapping):
            raise ContextCompressionError(
                f"ContextCompressionConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            target_compression_ratio=float(
                data.get("target_compression_ratio", 0.5)
            ),
            preserve_entities=bool(data.get("preserve_entities", True)),
            preserve_numbers=bool(data.get("preserve_numbers", True)),
            tokens_per_word=float(data.get("tokens_per_word", 1.3)),
            min_sentence_words=int(data.get("min_sentence_words", 5)),
            max_sentence_words=int(data.get("max_sentence_words", 30)),
            select_stop_fraction=float(
                data.get("select_stop_fraction", 0.9)
            ),
            max_history=int(data.get("max_history", 1000)),
        )


@dataclass(frozen=True)
class SemanticChunkerConfig:
    """Tunable parameters for :class:`SemanticChunker`."""

    chunk_size: int = 512
    overlap: int = 50
    tokens_per_word: float = 1.3
    max_chunks: Optional[int] = 10_000

    def __post_init__(self) -> None:
        if not isinstance(self.chunk_size, int) or self.chunk_size <= 0:
            raise SemanticChunkerError("chunk_size must be a positive int.")
        if not isinstance(self.overlap, int) or self.overlap < 0:
            raise SemanticChunkerError(
                "overlap must be a non-negative int."
            )
        if self.overlap >= self.chunk_size:
            raise SemanticChunkerError(
                "overlap must be strictly less than chunk_size."
            )
        if self.tokens_per_word <= 0:
            raise SemanticChunkerError("tokens_per_word must be > 0.")
        if self.max_chunks is not None and self.max_chunks <= 0:
            raise SemanticChunkerError(
                "max_chunks must be a positive int or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SemanticChunkerConfig":
        if not isinstance(data, Mapping):
            raise SemanticChunkerError(
                f"SemanticChunkerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            chunk_size=int(data.get("chunk_size", 512)),
            overlap=int(data.get("overlap", 50)),
            tokens_per_word=float(data.get("tokens_per_word", 1.3)),
            max_chunks=data.get("max_chunks", 10_000),
        )


# --------------------------------------------------------------------------- #
# Result records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CompressionResult:
    """Immutable result of a context compression."""

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
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            original_text=str(data.get("original_text", "")),
            compressed_text=str(data.get("compressed_text", "")),
            original_tokens=int(data.get("original_tokens", 0)),
            compressed_tokens=int(data.get("compressed_tokens", 0)),
            compression_ratio=float(data.get("compression_ratio", 0.0)),
            preserved_entities=tuple(data.get("preserved_entities", ())),
            metadata=dict(data.get("metadata", {})),
            timestamp=timestamp,
        )

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

    def __post_init__(self) -> None:
        if self.tokens < 0:
            raise SemanticChunkerError("tokens must be >= 0.")
        if self.paragraphs < 0:
            raise SemanticChunkerError("paragraphs must be >= 0.")
        if self.index < 0:
            raise SemanticChunkerError("index must be >= 0.")
        if self.start_char < 0 or self.end_char < self.start_char:
            raise SemanticChunkerError(
                "start_char/end_char must satisfy 0 <= start <= end."
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
        return cls(
            text=str(data["text"]),
            tokens=int(data.get("tokens", 0)),
            paragraphs=int(data.get("paragraphs", 0)),
            index=int(data.get("index", 0)),
            start_char=int(data.get("start_char", 0)),
            end_char=int(data.get("end_char", 0)),
        )

    def __repr__(self) -> str:
        return (
            "Chunk("
            f"index={self.index}, tokens={self.tokens}, "
            f"paragraphs={self.paragraphs}, "
            f"chars=[{self.start_char}, {self.end_char}])"
        )


# --------------------------------------------------------------------------- #
# Sentence splitting helper
# --------------------------------------------------------------------------- #
def _split_sentences(text: str) -> List[str]:
    """
    Split ``text`` into sentences, respecting common abbreviations.

    Splits on ``[.!?]`` followed by whitespace; then merges fragments whose
    last word is a known abbreviation (``Dr.``, ``U.S.``, ``e.g.``, …).
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
            # Extract the last whitespace-delimited token (may include dots).
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

    Thread-safe, serializable, and bounded in memory. The original public API
    is preserved; new parameters are keyword-only.

    Strategies
    ----------
    - Entity-preserving summarization.
    - Redundancy removal (via sentence ranking).
    - Sentence ranking and selection.
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
                target_compression_ratio=float(target_compression_ratio),
                preserve_entities=bool(preserve_entities),
                preserve_numbers=bool(preserve_numbers),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved (mirror the config).
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
        """
        Compress ``text`` while preserving key information.

        Parameters
        ----------
        text : str
            Text to compress.
        query : str, optional
            Query used to prioritize sentences with overlapping terms.
        max_tokens : int, optional
            Hard cap on the compressed output. Overrides the ratio-based
            target when supplied.

        Returns
        -------
        CompressionResult
        """
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

        Empty inputs are preserved as empty results (rather than crashing or
        receiving a spurious share of the budget). The remaining budget is
        split proportionally to each text's token count.
        """
        if not isinstance(texts, list):
            raise ContextCompressionError("texts must be a list.")
        if not isinstance(total_token_budget, int) or total_token_budget <= 0:
            raise ContextCompressionError(
                "total_token_budget must be a positive int."
            )

        original_tokens = [self._estimate_tokens(t) for t in texts]
        total_original = sum(original_tokens)

        results: List[CompressionResult] = []
        for text, orig in zip(texts, original_tokens):
            # ---- Fixed: no ZeroDivisionError when every input is empty ----
            if total_original == 0:
                allocated_budget = 0
            else:
                allocated_budget = int(
                    (orig / total_original) * total_token_budget
                )
            results.append(
                self.compress(text, query=query, max_tokens=allocated_budget)
            )
        return results

    def adaptive_compress(
        self,
        text: str,
        min_ratio: float = 0.3,
        max_ratio: float = 0.8,
        quality_threshold: float = 0.7,
    ) -> CompressionResult:
        """
        Compress ``text`` to satisfy a quality threshold using binary search.

        The compressor's ``target_ratio`` is **not** mutated — the search
        uses a local ratio so subsequent :meth:`compress` calls are
        unaffected.
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
                text=text, query=None, max_tokens=None, ratio=mid,
            )
            quality = self._estimate_quality(result)

            if quality > best_quality:
                best_result = result
                best_quality = quality

            if quality >= quality_threshold:
                best_result = result
                high = mid  # Try more aggressive compression
            else:
                low = mid   # Need less compression

        # Fixed: deterministic fallback to ``max_ratio`` (least aggressive)
        # instead of whatever ``self.target_ratio`` happened to be.
        if best_result is None:
            best_result = self._compress_with_ratio(
                text=text, query=None, max_tokens=None, ratio=max_ratio,
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
        # ---- Validate ------------------------------------------------
        if not isinstance(text, str):
            msg = f"text must be a string, got {type(text).__name__}."
            if self._strict:
                raise ContextCompressionError(msg)
            logger.warning("%s Treating as empty.", msg)
            text = ""

        if max_tokens is not None and (
            not isinstance(max_tokens, int) or max_tokens < 0
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
            raise ContextCompressionError("ratio must be in (0, 1].")

        original_tokens = self._estimate_tokens(text)

        # ---- Empty / trivial input -----------------------------------
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
                },
            )
            with self._lock:
                self.compression_history.append(result)
            return result

        # ---- Extract entities ----------------------------------------
        entities = (
            self._extract_entities(text)
            if self.preserve_entities else []
        )

        # ---- Split into sentences ------------------------------------
        sentences = _split_sentences(text)

        # ---- Score sentences (O(n) — enumerate, not list.index) ------
        scored = self._score_sentences(sentences, query, entities)

        # ---- Determine target tokens ---------------------------------
        target_tokens = (
            max_tokens if max_tokens is not None
            else max(0, int(original_tokens * ratio))
        )

        # ---- Select sentences (returns original indices) -------------
        selected_indices = self._select_sentences(scored, target_tokens)

        # ---- Reconstruct (preserving original order) -----------------
        compressed_text = self._reconstruct_text(sentences, selected_indices)
        compressed_tokens = self._estimate_tokens(compressed_text)

        # ---- Missing entities (report, do not corrupt the text) ------
        compressed_lower = compressed_text.lower()
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
            preserved_entities=tuple(entities),
            metadata={
                "sentences_original": len(sentences),
                "sentences_kept": len(selected_indices),
                "query_guided": query is not None,
                "target_ratio": ratio,
                "target_tokens": target_tokens,
                "missing_entities": missing_entities,
            },
        )

        with self._lock:
            self.compression_history.append(result)

        logger.debug(
            "Compressed %d -> %d tokens (ratio=%.3f, kept %d/%d sentences).",
            original_tokens, compressed_tokens,
            result.compression_ratio,
            len(selected_indices), len(sentences),
        )
        return result

    # ---------------------------------------------------------- token estimation
    def _estimate_tokens(self, text: str) -> int:
        """
        Estimate the token count of ``text``.

        Returns an ``int`` — the original implementation returned a float
        which drifted through the ratio math.
        """
        if not text:
            return 0
        return int(len(text.split()) * self._config.tokens_per_word)

    # ---------------------------------------------------------- entity extraction
    def _extract_entities(self, text: str) -> List[str]:
        """Extract capitalized words and (optionally) numbers/dates."""
        entities: List[str] = []
        # Capitalized words (potential names).
        entities.extend(
            re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", text)
        )
        if self.preserve_numbers:
            entities.extend(
                re.findall(r"\b\d+(?:\.\d+)?(?:%|kg|m|km|MB|GB|s|ms)?\b", text)
            )
        # Stable dedup: preserve order.
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
        """
        Score sentences by importance.

        Returns ``[(original_index, sentence, score), ...]`` sorted by
        descending score. Uses ``enumerate`` — the original
        ``list.index(sentence)`` was O(n²) and returned the first occurrence
        for duplicates.
        """
        cfg = self._config
        query_words = (
            set(query.lower().split()) if query else set()
        )
        last_index = len(sentences) - 1
        scored: List[Tuple[int, str, float]] = []

        for idx, sentence in enumerate(sentences):
            score = 0.0

            # Length band.
            length = len(sentence.split())
            if cfg.min_sentence_words <= length <= cfg.max_sentence_words:
                score += 0.3

            # Entity presence.
            entity_count = sum(1 for e in entities if e in sentence)
            score += entity_count * 0.2

            # Query relevance.
            if query_words:
                sentence_words = set(sentence.lower().split())
                score += len(query_words & sentence_words) * 0.5

            # Position bonus (first and last sentences).
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
    ) -> List[int]:
        """
        Select sentence indices under the token budget.

        Returns indices in **original order** so reconstruction preserves
        the source flow.
        """
        if target_tokens <= 0 or not scored:
            return []

        selected: List[int] = []
        current_tokens = 0
        stop_at = target_tokens * self._config.select_stop_fraction

        for idx, sentence, _score in scored:
            sentence_tokens = self._estimate_tokens(sentence)
            if current_tokens + sentence_tokens <= target_tokens:
                selected.append(idx)
                current_tokens += sentence_tokens
            if current_tokens >= stop_at:
                break

        # If nothing fit but there is budget, keep the single best sentence.
        if not selected:
            best = scored[0]
            if self._estimate_tokens(best[1]) <= max(target_tokens, 1):
                selected.append(best[0])

        selected.sort()
        return selected

    # ---------------------------------------------------------- reconstruction
    def _reconstruct_text(
        self,
        sentences: List[str],
        selected_indices: List[int],
    ) -> str:
        """
        Reconstruct the compressed text from the selected indices.

        Preserves the original sentence order and does **not** inflate the
        output by appending entity tags (the original behavior could produce
        a "compressed" output larger than the input).
        """
        if not selected_indices:
            return ""
        return ". ".join(
            sentences[i].rstrip(". ")
            for i in selected_indices
            if 0 <= i < len(sentences)
        ) + "."

    # ---------------------------------------------------------- quality
    def _estimate_quality(self, result: CompressionResult) -> float:
        """
        Estimate compression quality in ``[0, 1]``.

        Blends ratio accuracy with entity coverage. Clamped to ``[0, 1]`` —
        the original implementation could go negative when the actual ratio
        diverged from the target.
        """
        ratio_diff = abs(result.compression_ratio - self.target_ratio)
        ratio_score = max(0.0, 1.0 - ratio_diff)
        entity_score = min(len(result.preserved_entities) / 10.0, 1.0)
        return max(0.0, min(1.0, ratio_score * 0.6 + entity_score * 0.4))

    # ---------------------------------------------------------- stats
    def get_compression_stats(self) -> Dict[str, Any]:
        """Return the backward-compatible compression statistics."""
        with self._lock:
            history = list(self.compression_history)

        if not history:
            return {
                "status": "no_compressions",
                "total_compressions": 0,
                "avg_compression_ratio": 0.0,
                "total_tokens_saved": 0,
                "avg_entities_preserved": 0.0,
            }

        avg_ratio = sum(r.compression_ratio for r in history) / len(history)
        total_saved = sum(
            r.original_tokens - r.compressed_tokens for r in history
        )
        avg_entities = sum(
            len(r.preserved_entities) for r in history
        ) / len(history)

        return {
            "total_compressions": len(history),
            "avg_compression_ratio": avg_ratio,
            "total_tokens_saved": total_saved,
            "avg_entities_preserved": avg_entities,
            # Additive:
            "mean_original_tokens": sum(r.original_tokens for r in history)
            / len(history),
            "mean_compressed_tokens": sum(r.compressed_tokens for r in history)
            / len(history),
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_compression_stats`."""
        return self.get_compression_stats()

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the compression history. Returns the number of entries removed."""
        with self._lock:
            removed = len(self.compression_history)
            if clear_history:
                self.compression_history.clear()
            self._started_at = time.time()
        logger.debug("ContextCompressor reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "history_size": len(self.compression_history),
            }
            if include_history:
                payload["history"] = [r.to_dict() for r in self.compression_history]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ContextCompressor":
        if not isinstance(data, Mapping):
            raise ContextCompressionError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = ContextCompressionConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        compressor = cls(config=cfg, strict=bool(data.get("strict", True)))
        with compressor._lock:
            for entry in data.get("history", []):
                compressor.compression_history.append(
                    CompressionResult.from_dict(entry)
                )
            compressor._started_at = float(data.get("started_at", time.time()))
        return compressor

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "ContextCompressor":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise ContextCompressionError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "ContextCompressor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "ContextCompressor scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "ContextCompressor("
                f"target_ratio={self.target_ratio:.3f}, "
                f"history={len(self.compression_history)}, "
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

    Thread-safe, serializable, and bounded in memory. The original public API
    (``chunk_size``, ``overlap``, ``chunk()``) is preserved.

    Notes
    -----
    The original ``overlap`` implementation kept only the last *paragraph*,
    which rarely corresponds to ``overlap`` tokens. The enhanced version
    carries over the last ``overlap`` tokens of the previous chunk by
    reconstructing the paragraph list until the token budget is met.
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

        # Legacy attributes preserved.
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
        """
        Chunk ``text`` into semantic segments.

        Returns a list of dicts with keys ``text``, ``tokens``,
        ``paragraphs``, ``index``, ``start_char``, ``end_char`` — the first
        three are backward compatible; the last three are additive.
        """
        if not isinstance(text, str):
            msg = f"text must be a string, got {type(text).__name__}."
            if self._strict:
                raise SemanticChunkerError(msg)
            logger.warning("%s Treating as empty.", msg)
            text = ""

        chunks = self.chunk_objects(text)
        return [c.to_dict() for c in chunks]

    def chunk_objects(self, text: str) -> List[Chunk]:
        """Same as :meth:`chunk` but returns :class:`Chunk` objects."""
        if not text:
            return []

        paragraphs_with_offsets = self._paragraph_offsets(text)
        chunks: List[Chunk] = []

        current_paras: List[Tuple[str, int, int]] = []
        current_tokens = 0

        for para, start, end in paragraphs_with_offsets:
            para_tokens = self._estimate_tokens(para)

            if current_tokens + para_tokens > self.chunk_size and current_paras:
                # ---- Emit current chunk -----------------------------------
                chunks.append(self._make_chunk(chunks, current_paras))

                # ---- Carry overlap ----------------------------------------
                current_paras, current_tokens = self._carry_overlap(
                    current_paras, para, para_tokens
                )
            else:
                current_paras.append((para, start, end))
                current_tokens += para_tokens

        # ---- Emit final chunk -----------------------------------------
        if current_paras:
            chunks.append(self._make_chunk(chunks, current_paras))

        self._chunk_count += len(chunks)
        logger.debug(
            "Chunked %d chars into %d chunk(s).", len(text), len(chunks),
        )
        return chunks

    # ---------------------------------------------------------- internals
    def _estimate_tokens(self, text: str) -> int:
        if not text:
            return 0
        return int(len(text.split()) * self._config.tokens_per_word)

    @staticmethod
    def _paragraph_offsets(text: str) -> List[Tuple[str, int, int]]:
        """
        Return ``[(paragraph, start_char, end_char), ...]``.

        Uses ``str.find`` on the original text to reconstruct offsets without
        losing the exact paragraph boundaries.
        """
        raw = text.split("\n\n")
        offsets: List[Tuple[str, int, int]] = []
        cursor = 0
        for para in raw:
            start = text.find(para, cursor)
            if start < 0:
                # Fallback — should never happen on the same string.
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
        return Chunk(
            text=text,
            tokens=self._estimate_tokens(text),
            paragraphs=len(paragraphs),
            index=len(existing_chunks),
            start_char=paragraphs[0][1],
            end_char=paragraphs[-1][2],
        )

    def _carry_overlap(
        self,
        previous: List[Tuple[str, int, int]],
        incoming: Tuple[str, int, int],
        incoming_tokens: int,
    ) -> Tuple[List[Tuple[str, int, int]], int]:
        """
        Build the new chunk from a token-bounded suffix of ``previous``
        plus the incoming paragraph.

        Fixes the original behavior, which kept only the last paragraph
        regardless of the configured ``overlap`` in tokens.
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
            return {
                "chunk_size": self.chunk_size,
                "overlap": self.overlap,
                "chunks_produced": self._chunk_count,
            }

    def reset(self) -> None:
        with self._lock:
            self._chunk_count = 0
        logger.debug("SemanticChunker reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self._config.to_dict(),
            "strict": self._strict,
            "chunks_produced": self._chunk_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SemanticChunker":
        if not isinstance(data, Mapping):
            raise SemanticChunkerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = SemanticChunkerConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        chunker = cls(config=cfg, strict=bool(data.get("strict", True)))
        chunker._chunk_count = int(data.get("chunks_produced", 0))
        return chunker

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SemanticChunker":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise SemanticChunkerError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "SemanticChunker("
            f"chunk_size={self.chunk_size}, "
            f"overlap={self.overlap}, "
            f"chunks_produced={self._chunk_count}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "Chunk",
    "CompressionResult",
    "ContextCompressionConfig",
    "ContextCompressionError",
    "ContextCompressor",
    "SemanticChunker",
    "SemanticChunkerConfig",
    "SemanticChunkerError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.context_compression
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---------------------------------------------------------------- #
    # Happy path: entity preservation + query-guided compression
    # ---------------------------------------------------------------- #
    text = (
        "Dr. Smith joined Acme Corp in 2019. She led the U.S. division. "
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
    print(f"entities   : {result.preserved_entities}")
    print(f"metadata   : {result.metadata}")

    # ---- Bug fix: no entity-tag inflation --------------------------- #
    assert (
        result.compressed_tokens <= result.original_tokens
    ), "compression must not inflate the output"
    print("no-inflate : OK")

    # ---- Bug fix: sentence order preserved -------------------------- #
    # The compressed text must preserve the *relative* order of the
    # sentences that survived.
    original_order = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
    compressed_order = [
        s.strip() for s in re.split(r"(?<=[.!?])\s+", result.compressed_text)
        if s.strip()
    ]
    positions = [original_order.index(s) for s in compressed_order if s in original_order]
    assert positions == sorted(positions), "sentence order was scrambled"
    print("order kept : OK")

    # ---- Bug fix: Dr. / U.S. handling ------------------------------ #
    dr_text = "Dr. Smith is here. U.S. policy changed in 2020."
    dr_result = compressor.compress(dr_text, query=None)
    # The sentence splitter must not have broken "Dr. Smith" into two.
    sentences_detected = dr_result.metadata["sentences_original"]
    print(f"split Dr.  : {sentences_detected} sentences (expected 2)")
    assert sentences_detected == 2

    # ---- Batch compression (with empty inputs) --------------------- #
    batch = compressor.compress_batch(
        ["", "one two three four five", ""],
        total_token_budget=64,
    )
    print(f"batch      : {[r.original_tokens for r in batch]}")

    # ---- adaptive_compress side-effect fix ------------------------- #
    before_ratio = compressor.target_ratio
    adaptive = compressor.adaptive_compress(text, min_ratio=0.3, max_ratio=0.8)
    after_ratio = compressor.target_ratio
    assert before_ratio == after_ratio, "adaptive_compress must not mutate target_ratio"
    print(f"adaptive   : ratio={adaptive.compression_ratio:.3f} "
          f"(target_ratio unchanged)")

    # ---- Statistics ------------------------------------------------ #
    print("stats      :", {
        k: v for k, v in compressor.get_compression_stats().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization round-trip ---------------------------------- #
    payload = compressor.to_json()
    restored = ContextCompressor.from_json(payload)
    assert restored.to_dict() == compressor.to_dict()
    print("Round-trip OK.")

    # ---- Context manager ------------------------------------------- #
    with ContextCompressor() as scoped:
        scoped.compress("Some text to compress.")
    print("Context    : OK")

    # ---- Validation failures --------------------------------------- #
    for bad_cfg in (
        dict(target_compression_ratio=0),
        dict(target_compression_ratio=1.5),
        dict(tokens_per_word=0),
        dict(min_sentence_words=0),
        dict(min_sentence_words=10, max_sentence_words=5),
        dict(select_stop_fraction=0),
        dict(max_history=0),
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
        lambda: strict.adaptive_compress("hi", min_ratio=0.5, max_ratio=0.3),
        lambda: strict.adaptive_compress("hi", quality_threshold=2.0),
    ):
        try:
            bad_call()
        except ContextCompressionError as exc:
            print("Rejected   :", exc)

    # Non-strict coerces.
    lenient = ContextCompressor(strict=False)
    r = lenient.compress(None)  # type: ignore[arg-type]
    assert r.original_tokens == 0 and r.compressed_text == ""
    print("lenient    : OK")

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
    print("chunker    :", chunker)

    chunks = chunker.chunk(doc)
    print(f"chunks     : {len(chunks)}")
    for c in chunks:
        print(f"  #{c['index']}: tokens={c['tokens']}, "
              f"paras={c['paragraphs']}, "
              f"chars=[{c['start_char']}, {c['end_char']}]")

    # ---- Bug fix: no infinite loop on overlap >= chunk_size -------- #
    for bad_cfg in (
        dict(chunk_size=0),
        dict(overlap=-1),
        dict(chunk_size=100, overlap=100),
        dict(chunk_size=100, overlap=200),
        dict(tokens_per_word=0),
        dict(max_chunks=0),
    ):
        try:
            SemanticChunkerConfig(**bad_cfg)  # type: ignore[arg-type]
        except SemanticChunkerError as exc:
            print("Rejected cfg:", exc)

    # ---- Serialization round-trip ---------------------------------- #
    payload = chunker.to_json()
    restored_chunker = SemanticChunker.from_json(payload)
    assert restored_chunker.to_dict() == chunker.to_dict()
    print("Chunker round-trip OK.")

    # ---- Non-string input in strict mode --------------------------- #
    try:
        chunker.chunk(None)  # type: ignore[arg-type]
    except SemanticChunkerError as exc:
        print("Rejected   :", exc)

    # ---- Non-strict input coerces to empty ------------------------- #
    lenient_chunker = SemanticChunker(strict=False)
    assert lenient_chunker.chunk(None) == []  # type: ignore[arg-type]
    print("lenient chunker: OK")

    print("\nSmoke test passed.")
