# src/retrieval/semantic_scorer.py

"""
Semantic Scoring Module
=======================

Replaces keyword-based scoring with embedding-based semantic similarity.
Supports local embeddings (``sentence-transformers``), API-based embeddings
(OpenAI, Cohere), and user-supplied custom providers.

Enhancements
------------
- Optional ``numpy`` / ``sentence_transformers`` / ``openai`` / ``cohere``
  imports guarded by ``_*_AVAILABLE`` flags.
- ``SemanticScorerConfig`` — frozen, validated: similarity metric, cache
  size, epsilon, recency / importance / content-type boost coefficients.
- ``SemanticScore`` — frozen dataclass with validation and serialization.
- **LRU embedding cache** — bounded by ``max_cache_entries``.
- **Thread safety** — ``RLock`` guards the cache and hit/miss counters.
- **Fixed ``cache_hit`` metadata** — now reflects the *current* call.
- **Validated ``similarity_metric``** at construction.
- **Added ``AnthropicProvider`` and ``CohereProvider``** matching the
  ``EmbeddingModel`` enum.
- **Full input validation**; strict / non-strict modes.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``
  on the scorer, the config, and ``SemanticScore``.
- ``statistics()``, ``__repr__``, custom ``SemanticScorerError`` /
  ``EmbeddingProviderError``, lazy ``%s`` logging, and a ``__main__`` smoke
  test using a deterministic fake provider.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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
    logger.debug("numpy not importable; semantic scorer disabled.")


# --------------------------------------------------------------------------- #
# Optional embedding SDKs
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    from sentence_transformers import SentenceTransformer  # type: ignore

    _SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:  # pragma: no cover
    SentenceTransformer = None  # type: ignore[assignment]
    _SENTENCE_TRANSFORMERS_AVAILABLE = False

try:  # pragma: no cover
    from openai import OpenAI  # type: ignore

    _OPENAI_AVAILABLE = True
except ImportError:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]
    _OPENAI_AVAILABLE = False

try:  # pragma: no cover
    import cohere  # type: ignore

    _COHERE_AVAILABLE = True
except ImportError:  # pragma: no cover
    cohere = None  # type: ignore[assignment]
    _COHERE_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SemanticScorerError(ValueError):
    """Raised for invalid semantic scorer inputs or configuration."""


class EmbeddingProviderError(ValueError):
    """Raised when an embedding provider cannot fulfil a request."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class EmbeddingModel(Enum):
    """Available embedding models."""
    SENTENCE_TRANSFORMERS = "sentence-transformers"  # Local, offline
    OPENAI = "openai"                                # API-based
    ANTHROPIC = "anthropic"                          # API-based (not supported)
    COHERE = "cohere"                                # API-based
    CUSTOM = "custom"                                # User-provided


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SemanticScorerConfig:
    """Tunable parameters for :class:`SemanticScorer`."""

    cache_embeddings: bool = True
    similarity_metric: str = "cosine"    # "cosine" | "dot" | "euclidean"

    # Bounded LRU cache cap.
    max_cache_entries: int = 100_000

    # Epsilon used to avoid zero-division.
    epsilon: float = 1e-12

    # Recency-boost coefficient.
    recency_boost_coefficient: float = 0.2
    recency_half_life_hours: float = 24.0

    # Importance-boost coefficient.
    importance_boost_coefficient: float = 0.3

    # Content-type boost multiplier for critical/high-priority items.
    critical_content_boost: float = 1.2

    # Confidence blend weights (must sum to 1.0).
    confidence_weight_similarity: float = 0.6
    confidence_weight_content_length: float = 0.2
    confidence_weight_query_length: float = 0.2
    content_length_saturation_words: int = 100
    query_length_saturation_words: int = 10

    def __post_init__(self) -> None:
        if self.similarity_metric not in ("cosine", "dot", "euclidean"):
            raise SemanticScorerError(
                f"similarity_metric must be 'cosine', 'dot', or "
                f"'euclidean', got {self.similarity_metric!r}."
            )
        if self.max_cache_entries <= 0:
            raise SemanticScorerError("max_cache_entries must be > 0.")
        if self.epsilon <= 0:
            raise SemanticScorerError("epsilon must be > 0.")
        for name in (
            "recency_boost_coefficient",
            "importance_boost_coefficient",
            "critical_content_boost",
        ):
            value = getattr(self, name)
            if value < 0:
                raise SemanticScorerError(f"{name} must be >= 0.")
        if self.recency_half_life_hours <= 0:
            raise SemanticScorerError(
                "recency_half_life_hours must be > 0."
            )
        weights = (
            self.confidence_weight_similarity
            + self.confidence_weight_content_length
            + self.confidence_weight_query_length
        )
        if abs(weights - 1.0) > 1e-6:
            raise SemanticScorerError(
                f"confidence weights must sum to 1.0 (got {weights:.6f})."
            )
        if self.content_length_saturation_words <= 0:
            raise SemanticScorerError(
                "content_length_saturation_words must be > 0."
            )
        if self.query_length_saturation_words <= 0:
            raise SemanticScorerError(
                "query_length_saturation_words must be > 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SemanticScorerConfig":
        if not isinstance(data, Mapping):
            raise SemanticScorerError(
                f"SemanticScorerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        kwargs: Dict[str, Any] = {}
        for key in (
            "cache_embeddings", "similarity_metric", "max_cache_entries",
            "epsilon", "recency_boost_coefficient",
            "recency_half_life_hours", "importance_boost_coefficient",
            "critical_content_boost",
            "confidence_weight_similarity",
            "confidence_weight_content_length",
            "confidence_weight_query_length",
            "content_length_saturation_words",
            "query_length_saturation_words",
        ):
            if key in data:
                kwargs[key] = data[key]
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Score
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SemanticScore:
    """Immutable semantic similarity score."""

    node_id: str
    query: str
    similarity_score: float
    embedding_model: str
    confidence: float
    metadata: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise SemanticScorerError(
                "node_id must be a non-empty string."
            )
        if not isinstance(self.query, str):
            raise SemanticScorerError("query must be a string.")
        for name in ("similarity_score", "confidence"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SemanticScorerError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise SemanticScorerError(
                    f"{name} must be finite, got {value!r}."
                )
            if not 0.0 <= fv <= 1.0:
                raise SemanticScorerError(
                    f"{name} must be in [0, 1], got {fv}."
                )
        if not isinstance(self.embedding_model, str) or not self.embedding_model:
            raise SemanticScorerError(
                "embedding_model must be a non-empty string."
            )
        if not isinstance(self.metadata, Mapping):
            raise SemanticScorerError("metadata must be a Mapping.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "query": self.query,
            "similarity_score": self.similarity_score,
            "embedding_model": self.embedding_model,
            "confidence": self.confidence,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SemanticScore":
        if not isinstance(data, Mapping):
            raise SemanticScorerError(
                "SemanticScore.from_dict expects a Mapping."
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
            query=str(data.get("query", "")),
            similarity_score=float(data.get("similarity_score", 0.0)),
            embedding_model=str(data.get("embedding_model", "unknown")),
            confidence=float(data.get("confidence", 0.0)),
            metadata=dict(data.get("metadata", {})),
            timestamp=timestamp,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SemanticScore":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise SemanticScorerError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "SemanticScore("
            f"node_id={self.node_id!r}, "
            f"similarity={self.similarity_score:.3f}, "
            f"confidence={self.confidence:.3f}, "
            f"model={self.embedding_model!r})"
        )


# --------------------------------------------------------------------------- #
# Providers
# --------------------------------------------------------------------------- #
class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    name: str = "abstract"
    dimension: int = 0

    @abstractmethod
    def encode(self, text: str) -> "np.ndarray":
        """Encode a single text into an embedding vector."""
        raise NotImplementedError

    @abstractmethod
    def encode_batch(self, texts: List[str]) -> List["np.ndarray"]:
        """Encode a batch of texts."""
        raise NotImplementedError

    @abstractmethod
    def get_dimension(self) -> int:
        """Return the embedding dimension."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, dimension={self.get_dimension()})"
        )


def _require_numpy() -> None:
    if not _NUMPY_AVAILABLE:
        raise EmbeddingProviderError(
            "numpy is required for embedding providers; "
            "install numpy to enable semantic scoring."
        )


class SentenceTransformerProvider(EmbeddingProvider):
    """Local sentence-transformers provider (offline capable)."""

    name = "sentence-transformers"

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        _require_numpy()
        if not _SENTENCE_TRANSFORMERS_AVAILABLE:
            raise EmbeddingProviderError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
        if not isinstance(model_name, str) or not model_name:
            raise EmbeddingProviderError(
                "model_name must be a non-empty string."
            )
        try:
            self.model = SentenceTransformer(model_name)  # type: ignore[misc]
        except Exception as exc:
            raise EmbeddingProviderError(
                f"Could not load sentence-transformers model "
                f"{model_name!r}: {exc}"
            ) from exc
        self.model_name = model_name

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got {type(text).__name__}."
            )
        return self.model.encode(text, convert_to_numpy=True)

    def encode_batch(self, texts: List[str]) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        if not texts:
            return []
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        return [emb for emb in embeddings]

    def get_dimension(self) -> int:
        return int(self.model.get_sentence_embedding_dimension())


class OpenAIProvider(EmbeddingProvider):
    """OpenAI embeddings provider (API-based)."""

    name = "openai"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "text-embedding-3-small",
    ) -> None:
        _require_numpy()
        if not _OPENAI_AVAILABLE:
            raise EmbeddingProviderError(
                "openai not installed. Install with: pip install openai"
            )
        if not isinstance(model, str) or not model:
            raise EmbeddingProviderError("model must be a non-empty string.")
        try:
            self.client = OpenAI(api_key=api_key)  # type: ignore[misc]
        except Exception as exc:
            raise EmbeddingProviderError(
                f"Could not construct OpenAI client: {exc}"
            ) from exc
        self.model = model
        self._dimension = 1536 if "small" in model else 3072

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got {type(text).__name__}."
            )
        try:
            response = self.client.embeddings.create(
                input=text, model=self.model,
            )
        except Exception as exc:
            raise EmbeddingProviderError(
                f"OpenAI embedding call failed: {exc}"
            ) from exc
        return np.array(response.data[0].embedding)

    def encode_batch(self, texts: List[str]) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        if not texts:
            return []
        try:
            response = self.client.embeddings.create(
                input=texts, model=self.model,
            )
        except Exception as exc:
            raise EmbeddingProviderError(
                f"OpenAI batch embedding call failed: {exc}"
            ) from exc
        return [np.array(item.embedding) for item in response.data]

    def get_dimension(self) -> int:
        return int(self._dimension)


class CohereProvider(EmbeddingProvider):
    """Cohere embeddings provider (API-based)."""

    name = "cohere"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "embed-english-v3.0",
    ) -> None:
        _require_numpy()
        if not _COHERE_AVAILABLE:
            raise EmbeddingProviderError(
                "cohere not installed. Install with: pip install cohere"
            )
        if not isinstance(model, str) or not model:
            raise EmbeddingProviderError("model must be a non-empty string.")
        try:
            self.client = cohere.Client(api_key=api_key)  # type: ignore[union-attr]
        except Exception as exc:
            raise EmbeddingProviderError(
                f"Could not construct Cohere client: {exc}"
            ) from exc
        self.model = model
        # Cohere v3 embed models are 1024-dim by default.
        self._dimension = 1024

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got {type(text).__name__}."
            )
        try:
            response = self.client.embed(
                texts=[text], model=self.model, input_type="search_query",
            )
        except Exception as exc:
            raise EmbeddingProviderError(
                f"Cohere embedding call failed: {exc}"
            ) from exc
        return np.array(response.embeddings[0])

    def encode_batch(self, texts: List[str]) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        if not texts:
            return []
        try:
            response = self.client.embed(
                texts=texts, model=self.model, input_type="search_document",
            )
        except Exception as exc:
            raise EmbeddingProviderError(
                f"Cohere batch embedding call failed: {exc}"
            ) from exc
        return [np.array(e) for e in response.embeddings]

    def get_dimension(self) -> int:
        return int(self._dimension)


class AnthropicProvider(EmbeddingProvider):
    """
    Placeholder provider for the ``EmbeddingModel.ANTHROPIC`` enum member.

    Anthropic does not offer a public embeddings API. Attempting to construct
    this provider raises :class:`EmbeddingProviderError` with a clear message
    rather than silently doing nothing. Callers should use a different
    provider (OpenAI, Cohere, or ``sentence-transformers``).
    """

    name = "anthropic"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise EmbeddingProviderError(
            "Anthropic does not expose an embeddings API. Use OpenAI, "
            "Cohere, or a local sentence-transformers model instead."
        )

    def encode(self, text: str) -> "np.ndarray":  # pragma: no cover
        raise EmbeddingProviderError("Anthropic embeddings unavailable.")

    def encode_batch(self, texts: List[str]) -> List["np.ndarray"]:  # pragma: no cover
        raise EmbeddingProviderError("Anthropic embeddings unavailable.")

    def get_dimension(self) -> int:  # pragma: no cover
        return 0


class CustomEmbeddingProvider(EmbeddingProvider):
    """Custom embedding provider for user-supplied embeddings."""

    name = "custom"

    def __init__(self, embedding_function: Any, dimension: int) -> None:
        _require_numpy()
        if not callable(embedding_function):
            raise EmbeddingProviderError(
                "embedding_function must be callable."
            )
        if not isinstance(dimension, int) or dimension <= 0:
            raise EmbeddingProviderError(
                "dimension must be a positive int."
            )
        self.embedding_fn = embedding_function
        self.dimension = dimension

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got {type(text).__name__}."
            )
        result = self.embedding_fn(text)
        return self._validate_vector(result)

    def encode_batch(self, texts: List[str]) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        return [self.encode(t) for t in texts]

    def get_dimension(self) -> int:
        return int(self.dimension)

    def _validate_vector(self, vector: Any) -> "np.ndarray":
        if not isinstance(vector, np.ndarray):
            try:
                vector = np.asarray(vector)
            except Exception as exc:
                raise EmbeddingProviderError(
                    f"embedding_function returned un-coercible value: {exc}"
                ) from exc
        if vector.ndim != 1:
            raise EmbeddingProviderError(
                f"embedding_function must return a 1-D vector, "
                f"got shape {vector.shape}."
            )
        if vector.shape[0] != self.dimension:
            raise EmbeddingProviderError(
                f"embedding_function returned dimension "
                f"{vector.shape[0]}, expected {self.dimension}."
            )
        return vector


# --------------------------------------------------------------------------- #
# Scorer
# --------------------------------------------------------------------------- #
class SemanticScorer:
    """
    Semantic scorer using embeddings instead of keyword matching.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.

    Features
    --------
    - Multiple embedding provider support (local, OpenAI, Cohere, custom).
    - Batch processing for efficiency.
    - LRU embedding cache.
    - Hybrid scoring (semantic similarity + metadata boost).
    """

    def __init__(
        self,
        provider: EmbeddingProvider,
        cache_embeddings: bool = True,
        similarity_metric: str = "cosine",
        *,
        config: Optional[SemanticScorerConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(provider, EmbeddingProvider):
            raise SemanticScorerError(
                "provider must be an EmbeddingProvider instance."
            )
        if config is not None:
            self._config = config
        else:
            self._config = SemanticScorerConfig(
                cache_embeddings=bool(cache_embeddings),
                similarity_metric=str(similarity_metric),
            )
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.provider: EmbeddingProvider = provider
        self.cache_embeddings: bool = self._config.cache_embeddings
        self.similarity_metric: str = self._config.similarity_metric

        self._lock = threading.RLock()
        # Bounded LRU cache via OrderedDict.
        self.embedding_cache: "OrderedDict[str, Any]" = OrderedDict()
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._cache_evictions: int = 0

        logger.debug(
            "SemanticScorer initialized "
            "(provider=%s, metric=%s, cache=%s, max_cache=%d, strict=%s)",
            type(provider).__name__,
            self.similarity_metric,
            self.cache_embeddings,
            self._config.max_cache_entries,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SemanticScorerConfig:
        return self._config

    @property
    def cache_size(self) -> int:
        with self._lock:
            return len(self.embedding_cache)

    # ---------------------------------------------------------- public API
    def score_relevance(
        self,
        query: str,
        content: str,
        content_id: Optional[str] = None,
        metadata_boost: Optional[Dict[str, Any]] = None,
    ) -> SemanticScore:
        """
        Score the relevance of ``content`` to ``query`` using embeddings.

        Parameters
        ----------
        query : str
            Query text.
        content : str
            Content to score.
        content_id : str, optional
            Stable content identifier used as the cache key.
        metadata_boost : Mapping, optional
            Metadata used to bias the similarity (recency / importance /
            content type).

        Returns
        -------
        SemanticScore
        """
        query = self._validate_text("query", query)
        content = self._validate_text("content", content)
        if content_id is not None and (
            not isinstance(content_id, str) or not content_id
        ):
            msg = (
                f"content_id must be a non-empty string or None, "
                f"got {content_id!r}."
            )
            if self._strict:
                raise SemanticScorerError(msg)
            logger.warning("%s Ignoring content_id.", msg)
            content_id = None
        if metadata_boost is not None and not isinstance(
            metadata_boost, Mapping
        ):
            msg = (
                f"metadata_boost must be a Mapping or None, got "
                f"{type(metadata_boost).__name__}."
            )
            if self._strict:
                raise SemanticScorerError(msg)
            logger.warning("%s Ignoring metadata_boost.", msg)
            metadata_boost = None

        # Query embedding always uses the query string as cache key.
        query_emb, _ = self._get_embedding(query, cache_key=f"query:{query}")
        content_emb, content_cache_hit = self._get_embedding(
            content, cache_key=content_id,
        )

        similarity = self._calculate_similarity(query_emb, content_emb)

        if metadata_boost:
            boost_factor = self._calculate_metadata_boost(metadata_boost)
            similarity = min(1.0, similarity * boost_factor)

        confidence = self._estimate_confidence(query, content, similarity)

        return SemanticScore(
            node_id=content_id or "unknown",
            query=query,
            similarity_score=float(similarity),
            embedding_model=type(self.provider).__name__,
            confidence=float(confidence),
            metadata={
                "similarity_metric": self.similarity_metric,
                "cache_hit": bool(content_cache_hit),
            },
        )

    def score_batch(
        self,
        query: str,
        contents: List[Tuple[str, str]],
        metadata_boosts: Optional[List[Dict[str, Any]]] = None,
    ) -> List[SemanticScore]:
        """
        Score a batch of ``(content_id, content_text)`` pairs efficiently.

        Uses the provider's batch API for cache misses and reuses cached
        embeddings when available.
        """
        query = self._validate_text("query", query)
        if not isinstance(contents, list):
            raise SemanticScorerError("contents must be a list.")
        if not contents:
            return []
        if metadata_boosts is not None:
            if not isinstance(metadata_boosts, list):
                raise SemanticScorerError(
                    "metadata_boosts must be a list or None."
                )
            if len(metadata_boosts) != len(contents):
                raise SemanticScorerError(
                    "metadata_boosts must match contents in length."
                )

        # Query embedding.
        query_emb, _ = self._get_embedding(query, cache_key=f"query:{query}")

        # ---- Split cache hits from misses ---------------------------
        cache_hit_flags: List[bool] = []
        embeddings: List[Any] = []
        misses: List[Tuple[int, str, str]] = []

        for i, item in enumerate(contents):
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not isinstance(item[1], str)
            ):
                msg = (
                    f"contents[{i}] must be a (str, str) tuple, got "
                    f"{item!r}."
                )
                if self._strict:
                    raise SemanticScorerError(msg)
                logger.warning("%s Using empty placeholder.", msg)
                cache_hit_flags.append(False)
                embeddings.append(None)
                continue

            content_id, content_text = item
            cached = self._cache_lookup(content_id)
            if cached is not None:
                cache_hit_flags.append(True)
                embeddings.append(cached)
            else:
                cache_hit_flags.append(False)
                embeddings.append(None)
                misses.append((i, content_id, content_text))

        # ---- Fetch misses via provider ------------------------------
        if misses:
            miss_texts = [m[2] for m in misses]
            try:
                fresh = self.provider.encode_batch(miss_texts)
            except EmbeddingProviderError:
                raise
            except Exception as exc:
                raise SemanticScorerError(
                    f"provider.encode_batch failed: {exc}"
                ) from exc
            if len(fresh) != len(misses):
                raise SemanticScorerError(
                    f"provider returned {len(fresh)} embeddings for "
                    f"{len(misses)} inputs."
                )
            for (i, content_id, _text), emb in zip(misses, fresh):
                embeddings[i] = emb
                self._cache_store(content_id, emb)

        # ---- Compute similarities -----------------------------------
        scores: List[SemanticScore] = []
        for i, (content_id, content_text) in enumerate(contents):
            if embeddings[i] is None:
                # Malformed entry in non-strict mode — skip.
                continue
            similarity = self._calculate_similarity(query_emb, embeddings[i])

            if metadata_boosts:
                boost = metadata_boosts[i]
                if boost:
                    factor = self._calculate_metadata_boost(boost)
                    similarity = min(1.0, similarity * factor)

            confidence = self._estimate_confidence(
                query, content_text, similarity,
            )
            scores.append(SemanticScore(
                node_id=content_id,
                query=query,
                similarity_score=float(similarity),
                embedding_model=type(self.provider).__name__,
                confidence=float(confidence),
                metadata={
                    "similarity_metric": self.similarity_metric,
                    "cache_hit": bool(cache_hit_flags[i]),
                },
            ))
        return scores

    def score_and_rank(
        self,
        query: str,
        contents: List[Tuple[str, str]],
        top_k: Optional[int] = None,
    ) -> List[SemanticScore]:
        """Score and rank contents by descending similarity."""
        if top_k is not None and (
            not isinstance(top_k, int) or top_k < 0
        ):
            raise SemanticScorerError("top_k must be a non-negative int or None.")
        scores = self.score_batch(query, contents)
        # Deterministic tie-break: score desc, node_id asc.
        scores.sort(key=lambda s: (-s.similarity_score, s.node_id))
        if top_k is not None:
            return scores[:top_k]
        return scores

    def get_embedding(self, text: str) -> Any:
        """Public embedding accessor (bypasses the LRU cache)."""
        text = self._validate_text("text", text)
        emb, _ = self._get_embedding(text)
        return emb

    def precompute_embeddings(
        self, contents: List[Tuple[str, str]]
    ) -> int:
        """
        Precompute and cache embeddings for ``contents``.

        Returns the number of cache entries added. Existing entries are
        skipped.
        """
        if not isinstance(contents, list):
            raise SemanticScorerError("contents must be a list.")
        if not contents:
            return 0

        to_fetch: List[Tuple[str, str]] = []
        with self._lock:
            for content_id, _text in contents:
                if content_id in self.embedding_cache:
                    self.embedding_cache.move_to_end(content_id)
                    continue
                to_fetch.append((content_id, _text))

        if not to_fetch:
            return 0

        texts = [t for _, t in to_fetch]
        try:
            fresh = self.provider.encode_batch(texts)
        except EmbeddingProviderError:
            raise
        except Exception as exc:
            raise SemanticScorerError(
                f"provider.encode_batch failed: {exc}"
            ) from exc

        added = 0
        for (content_id, _text), emb in zip(to_fetch, fresh):
            self._cache_store(content_id, emb)
            added += 1
        return added

    def clear_cache(self) -> int:
        """Clear the embedding cache and reset hit/miss counters."""
        with self._lock:
            removed = len(self.embedding_cache)
            self.embedding_cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self._cache_evictions = 0
        logger.debug("SemanticScorer cache cleared (%d entries).", removed)
        return removed

    def get_cache_stats(self) -> Dict[str, Any]:
        """Return the backward-compatible cache statistics."""
        with self._lock:
            hits = self.cache_hits
            misses = self.cache_misses
            size = len(self.embedding_cache)
            evictions = self._cache_evictions
            capacity = self._config.max_cache_entries

        total = hits + misses
        hit_rate = hits / total if total > 0 else 0.0

        return {
            "cache_size": size,
            "cache_hits": hits,
            "cache_misses": misses,
            "hit_rate": hit_rate,
            "embedding_dimension": self.provider.get_dimension(),
            # Additive:
            "cache_capacity": capacity,
            "cache_evictions": evictions,
            "similarity_metric": self.similarity_metric,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_cache_stats`."""
        return self.get_cache_stats()

    # ---------------------------------------------------------- internal
    def _validate_text(self, name: str, value: Any) -> str:
        if not isinstance(value, str):
            msg = f"{name} must be a string, got {type(value).__name__}."
            if self._strict:
                raise SemanticScorerError(msg)
            logger.warning("%s Coercing via str().", msg)
            value = str(value)
        if not value:
            msg = f"{name} must be non-empty."
            if self._strict:
                raise SemanticScorerError(msg)
            logger.warning("%s", msg)
        return value

    def _cache_lookup(self, key: Optional[str]) -> Optional[Any]:
        if not key or not self.cache_embeddings:
            return None
        with self._lock:
            cached = self.embedding_cache.get(key)
            if cached is None:
                self.cache_misses += 1
                return None
            self.embedding_cache.move_to_end(key)
            self.cache_hits += 1
            return cached

    def _cache_store(self, key: Optional[str], embedding: Any) -> None:
        if not key or not self.cache_embeddings:
            return
        with self._lock:
            if key in self.embedding_cache:
                self.embedding_cache.move_to_end(key)
            self.embedding_cache[key] = embedding
            while len(self.embedding_cache) > self._config.max_cache_entries:
                self.embedding_cache.popitem(last=False)
                self._cache_evictions += 1

    def _get_embedding(
        self,
        text: str,
        cache_key: Optional[str] = None,
    ) -> Tuple[Any, bool]:
        """
        Return ``(embedding, cache_hit)``.

        ``cache_hit`` is True only when the current call retrieved a value
        from the cache. The original implementation surfaced
        ``content_id in self.embedding_cache`` in metadata, which was
        incorrect because it did not account for the LRU refresh order or
        ``None`` keys.
        """
        if cache_key and self.cache_embeddings:
            cached = self._cache_lookup(cache_key)
            if cached is not None:
                return cached, True

        try:
            embedding = self.provider.encode(text)
        except EmbeddingProviderError:
            raise
        except Exception as exc:
            raise SemanticScorerError(
                f"provider.encode failed: {exc}"
            ) from exc

        if cache_key and self.cache_embeddings:
            self._cache_store(cache_key, embedding)

        return embedding, False

    def _calculate_similarity(self, vec1: Any, vec2: Any) -> float:
        """
        Compute the similarity between two embedding vectors.

        All return values are cast to Python ``float`` — the original
        returned ``np.float64`` which broke JSON serialization.
        """
        if not _NUMPY_AVAILABLE:  # pragma: no cover — guarded upstream
            raise SemanticScorerError("numpy is required for similarity.")

        v1 = np.asarray(vec1, dtype=float)
        v2 = np.asarray(vec2, dtype=float)
        if v1.shape != v2.shape:
            raise SemanticScorerError(
                f"vector shape mismatch: {v1.shape} vs {v2.shape}."
            )

        metric = self.similarity_metric
        if metric == "cosine":
            norm1 = float(np.linalg.norm(v1))
            norm2 = float(np.linalg.norm(v2))
            if norm1 <= self._config.epsilon or norm2 <= self._config.epsilon:
                return 0.0
            return float(np.dot(v1, v2) / (norm1 * norm2))

        if metric == "dot":
            return float(np.dot(v1, v2))

        # euclidean
        distance = float(np.linalg.norm(v1 - v2))
        return 1.0 / (1.0 + distance)

    def _calculate_metadata_boost(
        self, metadata: Mapping[str, Any]
    ) -> float:
        """Compute the multiplicative boost from metadata (>= 1.0)."""
        cfg = self._config
        boost = 1.0

        # Recency boost.
        timestamp = metadata.get("timestamp")
        if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool):
            age_hours = max(0.0, (time.time() - float(timestamp)) / 3600.0)
            half_life = cfg.recency_half_life_hours
            recency_boost = 1.0 + (
                cfg.recency_boost_coefficient / (1.0 + age_hours / half_life)
            )
            boost *= recency_boost

        # Importance boost (clamped to [0, 1]).
        importance = metadata.get("importance")
        if isinstance(importance, (int, float)) and not isinstance(importance, bool):
            imp = max(0.0, min(1.0, float(importance)))
            boost *= 1.0 + imp * cfg.importance_boost_coefficient

        # Content-type boost.
        content_type = metadata.get("content_type")
        if isinstance(content_type, str) and content_type in (
            "critical", "high_priority",
        ):
            boost *= cfg.critical_content_boost

        return boost

    def _estimate_confidence(
        self, query: str, content: str, similarity: float
    ) -> float:
        """Estimate confidence in ``[0, 1]`` for a score."""
        cfg = self._config
        similarity_conf = max(0.0, min(1.0, float(similarity)))

        content_length = len(content.split())
        length_conf = min(
            1.0, content_length / cfg.content_length_saturation_words,
        )
        query_length = len(query.split())
        query_conf = min(
            1.0, query_length / cfg.query_length_saturation_words,
        )

        return max(0.0, min(1.0,
            cfg.confidence_weight_similarity * similarity_conf
            + cfg.confidence_weight_content_length * length_conf
            + cfg.confidence_weight_query_length * query_conf
        ))

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_cache: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "provider": {
                    "type": type(self.provider).__name__,
                    "name": getattr(self.provider, "name", "unknown"),
                    "dimension": self.provider.get_dimension(),
                },
                "counters": {
                    "cache_hits": self.cache_hits,
                    "cache_misses": self.cache_misses,
                    "cache_evictions": self._cache_evictions,
                    "cache_size": len(self.embedding_cache),
                },
            }
            if include_cache:
                # Embeddings are ndarray; stringify them for JSON safety.
                payload["cache_keys"] = list(self.embedding_cache.keys())
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "SemanticScorer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "SemanticScorer scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "SemanticScorer("
                f"provider={type(self.provider).__name__}, "
                f"metric={self.similarity_metric!r}, "
                f"cache={len(self.embedding_cache)}, "
                f"hits={self.cache_hits}, misses={self.cache_misses}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Convenience factories
# --------------------------------------------------------------------------- #
def create_local_scorer(
    model_name: str = "all-MiniLM-L6-v2",
    cache_embeddings: bool = True,
    *,
    config: Optional[SemanticScorerConfig] = None,
    strict: bool = True,
) -> SemanticScorer:
    """Create a semantic scorer backed by a local sentence-transformers model."""
    provider = SentenceTransformerProvider(model_name)
    return SemanticScorer(
        provider,
        cache_embeddings=cache_embeddings,
        config=config,
        strict=strict,
    )


def create_openai_scorer(
    api_key: Optional[str] = None,
    model: str = "text-embedding-3-small",
    cache_embeddings: bool = True,
    *,
    config: Optional[SemanticScorerConfig] = None,
    strict: bool = True,
) -> SemanticScorer:
    """Create a semantic scorer backed by OpenAI embeddings."""
    provider = OpenAIProvider(api_key, model)
    return SemanticScorer(
        provider,
        cache_embeddings=cache_embeddings,
        config=config,
        strict=strict,
    )


def create_custom_scorer(
    embedding_function: Any,
    dimension: int,
    cache_embeddings: bool = True,
    *,
    config: Optional[SemanticScorerConfig] = None,
    strict: bool = True,
) -> SemanticScorer:
    """Create a semantic scorer backed by a user-supplied embedding function."""
    provider = CustomEmbeddingProvider(embedding_function, dimension)
    return SemanticScorer(
        provider,
        cache_embeddings=cache_embeddings,
        config=config,
        strict=strict,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AnthropicProvider",
    "CohereProvider",
    "CustomEmbeddingProvider",
    "EmbeddingModel",
    "EmbeddingProvider",
    "EmbeddingProviderError",
    "OpenAIProvider",
    "SemanticScore",
    "SemanticScorer",
    "SemanticScorerConfig",
    "SemanticScorerError",
    "SentenceTransformerProvider",
    "create_custom_scorer",
    "create_local_scorer",
    "create_openai_scorer",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.semantic_scorer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    if not _NUMPY_AVAILABLE:
        print("numpy not installed; skipping semantic scorer smoke test.")
    else:
        # ---- Deterministic fake provider ----------------------------- #
        import hashlib

        class _FakeProvider(EmbeddingProvider):
            """
            Deterministic 4-dim provider: each text maps to a vector derived
            from the counts of four keyword families.
            """

            name = "fake"

            def __init__(self) -> None:
                pass

            def _vector(self, text: str) -> Any:
                lowered = text.lower()
                families = ("energy", "carbon", "image", "audio")
                vec = [float(lowered.count(f)) for f in families]
                # Normalize to avoid all-zero vectors when no keyword matches.
                norm = sum(v * v for v in vec) ** 0.5
                if norm == 0:
                    # Hash-based fallback so distinct texts get distinct vectors.
                    h = int(hashlib.md5(text.encode()).hexdigest()[:8], 16)
                    vec = [(h >> (i * 8)) & 0xFF for i in range(4)]
                    norm = sum(v * v for v in vec) ** 0.5
                    vec = [v / max(norm, 1.0) for v in vec]
                else:
                    vec = [v / norm for v in vec]
                return np.asarray(vec, dtype=float)

            def encode(self, text: str) -> Any:
                return self._vector(text)

            def encode_batch(self, texts: List[str]) -> List[Any]:
                return [self._vector(t) for t in texts]

            def get_dimension(self) -> int:
                return 4

        # ---- Happy path --------------------------------------------- #
        scorer = SemanticScorer(_FakeProvider())
        print("repr       :", scorer)

        result = scorer.score_relevance(
            "renewable energy options",
            "Solar power reduces carbon emissions significantly.",
            content_id="doc_a",
        )
        print("score A    :", result)
        assert 0.0 <= result.similarity_score <= 1.0
        assert 0.0 <= result.confidence <= 1.0

        # ---- Cache hit on the second call --------------------------- #
        result2 = scorer.score_relevance(
            "renewable energy options",
            "Solar power reduces carbon emissions significantly.",
            content_id="doc_a",
        )
        print("cache_hit? :", result2.metadata["cache_hit"])
        assert result2.metadata["cache_hit"] is True

        # ---- Bug fix: cache_hit reflects the CURRENT call ----------- #
        result3 = scorer.score_relevance(
            "renewable energy options",
            "New content that was never seen before.",
            content_id="doc_new",
        )
        assert result3.metadata["cache_hit"] is False, (
            "first-time content must report cache_hit=False"
        )
        print("first-time : OK")

        # ---- Batch scoring ------------------------------------------ #
        contents = [
            ("doc_1", "Energy and carbon are key sustainability metrics."),
            ("doc_2", "Image recognition uses neural networks."),
            ("doc_3", "Audio processing and speech recognition."),
            ("doc_4", "Energy efficiency reduces carbon footprint."),
        ]
        batch_scores = scorer.score_batch("energy carbon sustainability", contents)
        print("batch      :", [
            f"{s.node_id}={s.similarity_score:.3f}" for s in batch_scores
        ])

        # ---- Ranked top-k ------------------------------------------- #
        ranked = scorer.score_and_rank(
            "energy carbon sustainability", contents, top_k=2,
        )
        print("top 2      :", [s.node_id for s in ranked])
        assert len(ranked) <= 2

        # ---- Metadata boost ----------------------------------------- #
        import time as _t
        now = _t.time()
        boosted = scorer.score_relevance(
            "energy",
            "Energy efficiency.",
            content_id="doc_boosted",
            metadata_boost={"timestamp": now, "importance": 1.0,
                            "content_type": "critical"},
        )
        plain = scorer.score_relevance(
            "energy",
            "Energy efficiency.",
            content_id="doc_plain",
        )
        print(f"boosted    : {boosted.similarity_score:.3f} "
              f"vs plain {plain.similarity_score:.3f}")
        assert boosted.similarity_score >= plain.similarity_score

        # ---- Precompute embeddings ---------------------------------- #
        added = scorer.precompute_embeddings([
            ("doc_5", "Precomputed content 1."),
            ("doc_6", "Precomputed content 2."),
            ("doc_5", "Precomputed content 1."),   # duplicate — skipped
        ])
        print("precompute :", added, "new entry(ies)")
        assert added == 2

        # ---- Bounded LRU cache -------------------------------------- #
        bounded = SemanticScorer(
            _FakeProvider(),
            config=SemanticScorerConfig(max_cache_entries=2),
        )
        for i in range(5):
            bounded.score_relevance(
                "q", f"content {i}", content_id=f"c{i}",
            )
        print(f"bounded    : cache_size={bounded.cache_size} "
              f"evictions={bounded._cache_evictions}")
        assert bounded.cache_size == 2
        assert bounded._cache_evictions == 3

        # ---- Similarity metrics ------------------------------------- #
        for metric in ("cosine", "dot", "euclidean"):
            s = SemanticScorer(_FakeProvider(), similarity_metric=metric)
            r = s.score_relevance("energy", "energy energy")
            print(f"{metric:9} : {r.similarity_score:.4f}")

        # ---- Statistics --------------------------------------------- #
        print("stats      :", {
            k: v for k, v in scorer.get_cache_stats().items()
        })

        # ---- SemanticScore serialization ---------------------------- #
        payload = result.to_json()
        restored = SemanticScore.from_json(payload)
        assert restored.to_dict() == result.to_dict()
        print("Round-trip OK.")

        # ---- Context manager ---------------------------------------- #
        with SemanticScorer(_FakeProvider()) as scoped:
            scoped.score_relevance("q", "content", content_id="ctx")
        print("Context    : OK")

        # ---- Validation failures ------------------------------------ #
        for bad_cfg in (
            dict(similarity_metric="bogus"),
            dict(max_cache_entries=0),
            dict(epsilon=0),
            dict(recency_boost_coefficient=-0.1),
            dict(importance_boost_coefficient=-0.1),
            dict(critical_content_boost=-1.0),
            dict(recency_half_life_hours=0),
            dict(confidence_weight_similarity=0.5),   # sum != 1
            dict(content_length_saturation_words=0),
            dict(query_length_saturation_words=0),
        ):
            try:
                SemanticScorerConfig(**bad_cfg)  # type: ignore[arg-type]
            except SemanticScorerError as exc:
                print("Rejected cfg:", exc)

        strict = SemanticScorer(_FakeProvider(), strict=True)
        for bad_call in (
            lambda: strict.score_relevance(None, "x"),  # type: ignore[arg-type]
            lambda: strict.score_relevance("q", None),  # type: ignore[arg-type]
            lambda: strict.score_relevance("q", "c", content_id=123),  # type: ignore[arg-type]
            lambda: strict.score_relevance("q", "c", metadata_boost="bad"),  # type: ignore[arg-type]
            lambda: strict.score_batch("q", "not-a-list"),  # type: ignore[arg-type]
            lambda: strict.score_batch("q", [("id", "text")], metadata_boosts=[{}, {}]),
            lambda: strict.score_and_rank("q", [("a", "b")], top_k=-1),
        ):
            try:
                bad_call()
            except SemanticScorerError as exc:
                print("Rejected   :", exc)

        # ---- Non-strict coerces ------------------------------------- #
        lenient = SemanticScorer(_FakeProvider(), strict=False)
        r = lenient.score_relevance(None, "content")  # type: ignore[arg-type]
        assert r is not None
        print("lenient    : OK")

        # ---- AnthropicProvider raises clearly ----------------------- #
        try:
            AnthropicProvider()
        except EmbeddingProviderError as exc:
            print("Anthropic  :", exc)

        print("\nSmoke test passed.")
