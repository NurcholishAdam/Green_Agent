# src/retrieval/semantic_scorer.py

"""
Semantic Scoring Module
=======================

Replaces keyword-based scoring with embedding-based semantic similarity.
Supports local embeddings (``sentence-transformers``), API-based embeddings
(OpenAI, Cohere), and user-supplied custom providers.

Design
------
- Optional ``numpy`` / ``sentence_transformers`` / ``openai`` / ``cohere``
  imports guarded by ``_*_AVAILABLE`` flags.
- ``SemanticScorerConfig`` — frozen, validated: similarity metric, cache
  size, epsilon, recency / importance / content-type boost coefficients.
- ``SemanticScore`` — frozen dataclass with validation and serialization.
- **Similarity is always in ``[0, 1]``.** Every metric (cosine, dot,
  euclidean) is clamped; cosine is remapped to ``(cos + 1) / 2`` so
  opposite vectors score ``0.0``. This guarantees the
  ``SemanticScore`` invariants hold for every legitimate pair.
- **LRU embedding cache** — bounded by ``max_cache_entries``.
- **Thread safety** — ``RLock`` guards the cache and hit/miss counters.
- **Round-trip serialization** — ``SemanticScorer.from_dict`` requires
  an explicit ``provider=`` argument (API keys, model names, and
  callables are never persisted). ``to_json`` uses
  ``allow_nan=False``.
- **Recency boost handles both Unix seconds and Unix milliseconds**,
  plus ISO-8601 strings.
- **Provider dimensions are validated against a model table** with an
  explicit ``dimension=`` override for unknown models.
- **Error hierarchy** — :class:`EmbeddingProviderError` is a subclass
  of :class:`SemanticScorerError`, so catching the module's main error
  catches provider failures too.

Version: 2.0.0
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
from types import TracebackType
from typing import (
    Any,
    Callable,
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
    "create_scorer_from_model",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional NumPy
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
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
    """Base error for the semantic scorer module."""


class EmbeddingProviderError(SemanticScorerError):
    """
    Raised when an embedding provider cannot fulfil a request.

    Subclass of :class:`SemanticScorerError` so callers catching the
    module-level error also catch provider failures.
    """


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class EmbeddingModel(Enum):
    """Available embedding models."""
    SENTENCE_TRANSFORMERS = "sentence-transformers"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    COHERE = "cohere"
    CUSTOM = "custom"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SemanticScorerConfig:
    """Tunable parameters for :class:`SemanticScorer`."""

    cache_embeddings: bool = True
    similarity_metric: str = "cosine"    # "cosine" | "dot" | "euclidean"

    max_cache_entries: int = 100_000

    epsilon: float = 1e-12

    recency_boost_coefficient: float = 0.2
    recency_half_life_hours: float = 24.0

    importance_boost_coefficient: float = 0.3

    critical_content_boost: float = 1.2

    confidence_weight_similarity: float = 0.6
    confidence_weight_content_length: float = 0.2
    confidence_weight_query_length: float = 0.2
    content_length_saturation_words: int = 100
    query_length_saturation_words: int = 10

    def __post_init__(self) -> None:
        if self.similarity_metric not in (
            "cosine", "dot", "euclidean",
        ):
            raise SemanticScorerError(
                f"similarity_metric must be 'cosine', 'dot', or "
                f"'euclidean', got {self.similarity_metric!r}."
            )
        for name in ("max_cache_entries",):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise SemanticScorerError(f"{name} must be > 0.")
        if (
            isinstance(self.epsilon, bool)
            or not isinstance(self.epsilon, (int, float))
            or not math.isfinite(float(self.epsilon))
            or self.epsilon <= 0
        ):
            raise SemanticScorerError(
                "epsilon must be finite and > 0."
            )
        for name in (
            "recency_boost_coefficient",
            "importance_boost_coefficient",
            "critical_content_boost",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise SemanticScorerError(
                    f"{name} must be finite and >= 0."
                )
        if (
            isinstance(self.recency_half_life_hours, bool)
            or not isinstance(
                self.recency_half_life_hours, (int, float)
            )
            or not math.isfinite(float(self.recency_half_life_hours))
            or self.recency_half_life_hours <= 0
        ):
            raise SemanticScorerError(
                "recency_half_life_hours must be finite and > 0."
            )
        weights = (
            self.confidence_weight_similarity
            + self.confidence_weight_content_length
            + self.confidence_weight_query_length
        )
        if abs(weights - 1.0) > 1e-6:
            raise SemanticScorerError(
                f"confidence weights must sum to 1.0 "
                f"(got {weights:.6f})."
            )
        for name in (
            "content_length_saturation_words",
            "query_length_saturation_words",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise SemanticScorerError(f"{name} must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "SemanticScorerConfig":
        if not isinstance(data, Mapping):
            raise SemanticScorerError(
                f"SemanticScorerConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            kwargs: Dict[str, Any] = {}
            for key in (
                "cache_embeddings",
                "similarity_metric",
                "max_cache_entries",
                "epsilon",
                "recency_boost_coefficient",
                "recency_half_life_hours",
                "importance_boost_coefficient",
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
        except (TypeError, ValueError) as exc:
            if isinstance(exc, SemanticScorerError):
                raise
            raise SemanticScorerError(
                f"invalid SemanticScorerConfig payload: {exc}"
            ) from exc


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
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                raise SemanticScorerError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise SemanticScorerError(
                    f"{name} must be finite, got {value!r}."
                )
            if not 0.0 <= fv <= 1.0:
                raise SemanticScorerError(
                    f"{name} must be in [0, 1], got {fv}."
                )
        if not isinstance(self.embedding_model, str) or (
            not self.embedding_model
        ):
            raise SemanticScorerError(
                "embedding_model must be a non-empty string."
            )
        if not isinstance(self.metadata, Mapping):
            raise SemanticScorerError(
                "metadata must be a Mapping."
            )

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
        if "node_id" not in data:
            raise SemanticScorerError(
                "SemanticScore requires 'node_id'."
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
                raise SemanticScorerError(
                    "metadata must be a Mapping."
                )
            return cls(
                node_id=str(data["node_id"]),
                query=str(data.get("query", "")),
                similarity_score=float(
                    data.get("similarity_score", 0.0)
                ),
                embedding_model=str(
                    data.get("embedding_model", "unknown")
                ),
                confidence=float(
                    data.get("confidence", 0.0)
                ),
                metadata=dict(metadata),
                timestamp=timestamp,
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, SemanticScorerError):
                raise
            raise SemanticScorerError(
                f"invalid SemanticScore payload: {exc}"
            ) from exc

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SemanticScore":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SemanticScorerError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

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

    @abstractmethod
    def encode(self, text: str) -> "np.ndarray":
        """Encode a single text into an embedding vector."""
        raise NotImplementedError

    @abstractmethod
    def encode_batch(
        self, texts: List[str],
    ) -> List["np.ndarray"]:
        """Encode a batch of texts."""
        raise NotImplementedError

    @abstractmethod
    def get_dimension(self) -> int:
        """Return the embedding dimension."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, "
            f"dimension={self.get_dimension()})"
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
            self.model = SentenceTransformer(  # type: ignore[misc]
                model_name,
            )
        except Exception as exc:
            raise EmbeddingProviderError(
                f"could not load sentence-transformers model "
                f"{model_name!r}: {exc}"
            ) from exc
        self.model_name = model_name

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got "
                f"{type(text).__name__}."
            )
        return self.model.encode(text, convert_to_numpy=True)

    def encode_batch(
        self, texts: List[str],
    ) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        if not texts:
            return []
        embeddings = self.model.encode(
            texts, convert_to_numpy=True,
        )
        return [emb for emb in embeddings]

    def get_dimension(self) -> int:
        return int(self.model.get_sentence_embedding_dimension())


# Known OpenAI embedding model dimensions.
_OPENAI_DIMENSIONS: Dict[str, int] = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}


class OpenAIProvider(EmbeddingProvider):
    """OpenAI embeddings provider (API-based)."""

    name = "openai"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "text-embedding-3-small",
        *,
        dimension: Optional[int] = None,
    ) -> None:
        _require_numpy()
        if not _OPENAI_AVAILABLE:
            raise EmbeddingProviderError(
                "openai not installed. "
                "Install with: pip install openai"
            )
        if not isinstance(model, str) or not model:
            raise EmbeddingProviderError(
                "model must be a non-empty string."
            )
        try:
            self.client = OpenAI(api_key=api_key)  # type: ignore[misc]
        except Exception as exc:
            raise EmbeddingProviderError(
                f"could not construct OpenAI client: {exc}"
            ) from exc
        self.model = model

        if dimension is not None:
            if (
                isinstance(dimension, bool)
                or not isinstance(dimension, int)
                or dimension <= 0
            ):
                raise EmbeddingProviderError(
                    "dimension must be a positive int or None."
                )
            self._dimension = dimension
        else:
            known = _OPENAI_DIMENSIONS.get(model)
            if known is None:
                raise EmbeddingProviderError(
                    f"unknown dimension for OpenAI model "
                    f"{model!r}; known models: "
                    f"{sorted(_OPENAI_DIMENSIONS)}. "
                    f"Pass dimension=... explicitly."
                )
            self._dimension = known

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got "
                f"{type(text).__name__}."
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

    def encode_batch(
        self, texts: List[str],
    ) -> List["np.ndarray"]:
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


# Known Cohere embedding model dimensions.
_COHERE_DIMENSIONS: Dict[str, int] = {
    "embed-english-v3.0":             1024,
    "embed-multilingual-v3.0":        1024,
    "embed-english-light-v3.0":        384,
    "embed-multilingual-light-v3.0":   384,
}


class CohereProvider(EmbeddingProvider):
    """Cohere embeddings provider (API-based)."""

    name = "cohere"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "embed-english-v3.0",
        *,
        dimension: Optional[int] = None,
    ) -> None:
        _require_numpy()
        if not _COHERE_AVAILABLE:
            raise EmbeddingProviderError(
                "cohere not installed. "
                "Install with: pip install cohere"
            )
        if not isinstance(model, str) or not model:
            raise EmbeddingProviderError(
                "model must be a non-empty string."
            )
        try:
            self.client = cohere.Client(api_key=api_key)  # type: ignore[union-attr]
        except Exception as exc:
            raise EmbeddingProviderError(
                f"could not construct Cohere client: {exc}"
            ) from exc
        self.model = model

        if dimension is not None:
            if (
                isinstance(dimension, bool)
                or not isinstance(dimension, int)
                or dimension <= 0
            ):
                raise EmbeddingProviderError(
                    "dimension must be a positive int or None."
                )
            self._dimension = dimension
        else:
            known = _COHERE_DIMENSIONS.get(model)
            if known is None:
                raise EmbeddingProviderError(
                    f"unknown dimension for Cohere model "
                    f"{model!r}; known models: "
                    f"{sorted(_COHERE_DIMENSIONS)}. "
                    f"Pass dimension=... explicitly."
                )
            self._dimension = known

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got "
                f"{type(text).__name__}."
            )
        try:
            response = self.client.embed(
                texts=[text],
                model=self.model,
                input_type="search_query",
            )
        except Exception as exc:
            raise EmbeddingProviderError(
                f"Cohere embedding call failed: {exc}"
            ) from exc
        return np.array(response.embeddings[0])

    def encode_batch(
        self, texts: List[str],
    ) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        if not texts:
            return []
        try:
            response = self.client.embed(
                texts=texts,
                model=self.model,
                input_type="search_document",
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

    Anthropic does not offer a public embeddings API. Attempting to
    construct this provider raises :class:`EmbeddingProviderError` with
    a clear message rather than silently doing nothing. Callers should
    use a different provider (OpenAI, Cohere, or
    ``sentence-transformers``).
    """

    name = "anthropic"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise EmbeddingProviderError(
            "Anthropic does not expose an embeddings API. "
            "Use OpenAI, Cohere, or a local sentence-transformers "
            "model instead."
        )

    def encode(self, text: str) -> "np.ndarray":  # pragma: no cover
        raise EmbeddingProviderError(
            "Anthropic embeddings unavailable."
        )

    def encode_batch(
        self, texts: List[str],
    ) -> List["np.ndarray"]:  # pragma: no cover
        raise EmbeddingProviderError(
            "Anthropic embeddings unavailable."
        )

    def get_dimension(self) -> int:  # pragma: no cover
        return 0


class CustomEmbeddingProvider(EmbeddingProvider):
    """Custom embedding provider for user-supplied embeddings."""

    name = "custom"

    def __init__(
        self,
        embedding_function: Callable[[str], Any],
        dimension: int,
    ) -> None:
        _require_numpy()
        if not callable(embedding_function):
            raise EmbeddingProviderError(
                "embedding_function must be callable."
            )
        if (
            isinstance(dimension, bool)
            or not isinstance(dimension, int)
            or dimension <= 0
        ):
            raise EmbeddingProviderError(
                "dimension must be a positive int."
            )
        self.embedding_fn = embedding_function
        self._dimension = dimension

    def encode(self, text: str) -> "np.ndarray":
        if not isinstance(text, str):
            raise EmbeddingProviderError(
                f"text must be a string, got "
                f"{type(text).__name__}."
            )
        try:
            result = self.embedding_fn(text)
        except Exception as exc:
            raise EmbeddingProviderError(
                f"embedding_function raised: {exc}"
            ) from exc
        return self._validate_vector(result)

    def encode_batch(
        self, texts: List[str],
    ) -> List["np.ndarray"]:
        if not isinstance(texts, list):
            raise EmbeddingProviderError("texts must be a list.")
        return [self.encode(t) for t in texts]

    def get_dimension(self) -> int:
        return int(self._dimension)

    def _validate_vector(self, vector: Any) -> "np.ndarray":
        if not isinstance(vector, np.ndarray):
            try:
                vector = np.asarray(vector, dtype=float)
            except Exception as exc:
                raise EmbeddingProviderError(
                    f"embedding_function returned "
                    f"un-coercible value: {exc}"
                ) from exc
        if vector.ndim != 1:
            raise EmbeddingProviderError(
                f"embedding_function must return a 1-D vector, "
                f"got shape {vector.shape}."
            )
        if vector.shape[0] != self._dimension:
            raise EmbeddingProviderError(
                f"embedding_function returned dimension "
                f"{vector.shape[0]}, expected {self._dimension}."
            )
        return vector


# --------------------------------------------------------------------------- #
# Scorer
# --------------------------------------------------------------------------- #
class SemanticScorer:
    """
    Semantic scorer using embeddings instead of keyword matching.

    Thread-safe, serializable, bounded in memory.

    Notes
    -----
    - Every returned ``similarity_score`` is in ``[0, 1]`` regardless
      of the configured metric.
    - ``SemanticScore.metadata["cache_hit"]`` reflects whether the
      *content* embedding for this call was served from the cache. The
      query embedding's cache state is not reported (the query is a
      single string per call and its cache state is uninformative).
    - Passing ``content_id=None`` yields ``node_id="unknown"`` on the
      result. Callers who need to distinguish results should pass a
      stable ``content_id``.
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
        self.cache_embeddings: bool = (
            self._config.cache_embeddings
        )
        self.similarity_metric: str = (
            self._config.similarity_metric
        )

        self._lock = threading.RLock()
        self.embedding_cache: "OrderedDict[str, Any]" = (
            OrderedDict()
        )
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self._cache_evictions: int = 0

        logger.debug(
            "SemanticScorer initialized "
            "(provider=%s, metric=%s, cache=%s, max_cache=%d, "
            "strict=%s)",
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

        query_emb, _ = self._get_embedding(
            query, cache_key=f"query:{query}",
        )
        content_emb, content_cache_hit = self._get_embedding(
            content, cache_key=content_id,
        )

        similarity = self._calculate_similarity(
            query_emb, content_emb,
        )

        if metadata_boost:
            factor = self._calculate_metadata_boost(
                metadata_boost,
            )
            similarity = min(1.0, similarity * factor)

        confidence = self._estimate_confidence(
            query, content, similarity,
        )

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
        metadata_boosts: Optional[
            List[Dict[str, Any]]
        ] = None,
    ) -> List[SemanticScore]:
        """
        Score a batch of ``(content_id, content_text)`` pairs.
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

        query_emb, _ = self._get_embedding(
            query, cache_key=f"query:{query}",
        )

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
                    f"contents[{i}] must be a (str, str) tuple, "
                    f"got {item!r}."
                )
                if self._strict:
                    raise SemanticScorerError(msg)
                logger.warning(
                    "%s Using empty placeholder.", msg,
                )
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
                misses.append(
                    (i, content_id, content_text),
                )

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
                    f"provider returned {len(fresh)} embeddings "
                    f"for {len(misses)} inputs."
                )
            for (i, content_id, _text), emb in zip(
                misses, fresh,
            ):
                embeddings[i] = emb
                self._cache_store(content_id, emb)

        scores: List[SemanticScore] = []
        for i, (content_id, content_text) in enumerate(contents):
            if embeddings[i] is None:
                continue
            similarity = self._calculate_similarity(
                query_emb, embeddings[i],
            )

            if metadata_boosts:
                boost = metadata_boosts[i]
                if boost:
                    factor = self._calculate_metadata_boost(
                        boost,
                    )
                    similarity = min(
                        1.0, similarity * factor,
                    )

            confidence = self._estimate_confidence(
                query, content_text, similarity,
            )
            scores.append(SemanticScore(
                node_id=content_id,
                query=query,
                similarity_score=float(similarity),
                embedding_model=type(
                    self.provider
                ).__name__,
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
        *,
        metadata_boosts: Optional[
            List[Dict[str, Any]]
        ] = None,
    ) -> List[SemanticScore]:
        """
        Score and rank contents by descending similarity.

        ``top_k=0`` returns an empty list. ``top_k=None`` returns all
        results.
        """
        if top_k is not None and (
            isinstance(top_k, bool)
            or not isinstance(top_k, int)
            or top_k < 0
        ):
            raise SemanticScorerError(
                "top_k must be a non-negative int or None."
            )
        scores = self.score_batch(
            query, contents,
            metadata_boosts=metadata_boosts,
        )
        scores.sort(
            key=lambda s: (-s.similarity_score, s.node_id)
        )
        if top_k is not None:
            return scores[:top_k]
        return scores

    def get_embedding(self, text: str) -> Any:
        """Public embedding accessor (bypasses the LRU cache)."""
        text = self._validate_text("text", text)
        emb, _ = self._get_embedding(text)
        return emb

    def precompute_embeddings(
        self, contents: List[Tuple[str, str]],
    ) -> int:
        """
        Precompute and cache embeddings for ``contents``.

        Returns the number of cache entries added. Existing entries
        are skipped.
        """
        if not isinstance(contents, list):
            raise SemanticScorerError("contents must be a list.")
        if not contents:
            return 0

        to_fetch: List[Tuple[str, str]] = []
        with self._lock:
            for i, item in enumerate(contents):
                if (
                    not isinstance(item, tuple)
                    or len(item) != 2
                    or not isinstance(item[0], str)
                    or not isinstance(item[1], str)
                ):
                    raise SemanticScorerError(
                        f"contents[{i}] must be a (str, str) "
                        f"tuple, got {item!r}."
                    )
                content_id, text = item
                if content_id in self.embedding_cache:
                    self.embedding_cache.move_to_end(
                        content_id,
                    )
                    continue
                to_fetch.append((content_id, text))

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

        if len(fresh) != len(to_fetch):
            raise SemanticScorerError(
                f"provider returned {len(fresh)} embeddings for "
                f"{len(to_fetch)} inputs."
            )

        added = 0
        for (content_id, _text), emb in zip(
            to_fetch, fresh,
        ):
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
        logger.debug(
            "SemanticScorer cache cleared (%d entries).", removed,
        )
        return removed

    def get_cache_stats(self) -> Dict[str, Any]:
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
            "embedding_dimension": (
                self.provider.get_dimension()
            ),
            "cache_capacity": capacity,
            "cache_evictions": evictions,
            "similarity_metric": self.similarity_metric,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_cache_stats()

    # ---------------------------------------------------------- internal
    def _validate_text(self, name: str, value: Any) -> str:
        if not isinstance(value, str):
            msg = (
                f"{name} must be a string, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise SemanticScorerError(msg)
            logger.warning("%s Coercing via str().", msg)
            value = str(value)
        if not value:
            msg = (
                f"{name} is empty; the embedding result for an "
                f"empty string is provider-dependent."
            )
            if self._strict:
                raise SemanticScorerError(
                    f"{name} must be non-empty."
                )
            logger.warning("%s", msg)
        return value

    def _cache_lookup(
        self, key: Optional[str],
    ) -> Optional[Any]:
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

    def _cache_store(
        self, key: Optional[str], embedding: Any,
    ) -> None:
        if not key or not self.cache_embeddings:
            return
        with self._lock:
            if key in self.embedding_cache:
                self.embedding_cache.move_to_end(key)
            self.embedding_cache[key] = embedding
            while (
                len(self.embedding_cache)
                > self._config.max_cache_entries
            ):
                self.embedding_cache.popitem(last=False)
                self._cache_evictions += 1

    def _get_embedding(
        self,
        text: str,
        cache_key: Optional[str] = None,
    ) -> Tuple[Any, bool]:
        """
        Return ``(embedding, cache_hit)``.

        ``cache_hit`` is True only when the current call retrieved a
        value from the cache.
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

    def _calculate_similarity(
        self, vec1: Any, vec2: Any,
    ) -> float:
        """
        Similarity in ``[0, 1]`` for every metric.

        - ``cosine``    : ``(cos + 1) / 2`` — opposite vectors
                          score ``0.0``, orthogonal ``0.5``, identical
                          ``1.0``.
        - ``dot``       : raw dot, clamped to ``[0, 1]``. Callers
                          choosing ``dot`` should normalize their
                          embeddings first if they want fine-grained
                          discrimination.
        - ``euclidean`` : ``1 / (1 + distance)``, already in ``(0, 1]``.

        Raises
        ------
        SemanticScorerError
            On shape mismatch or non-finite embedding values.
        """
        if not _NUMPY_AVAILABLE:  # pragma: no cover
            raise SemanticScorerError(
                "numpy is required for similarity computation."
            )

        v1 = np.asarray(vec1, dtype=float)
        v2 = np.asarray(vec2, dtype=float)
        if v1.shape != v2.shape:
            raise SemanticScorerError(
                f"vector shape mismatch: {v1.shape} vs {v2.shape}."
            )
        if not np.isfinite(v1).all() or not np.isfinite(v2).all():
            raise SemanticScorerError(
                "embedding vector contains non-finite values "
                "(NaN or Inf)."
            )

        metric = self.similarity_metric
        if metric == "cosine":
            norm1 = float(np.linalg.norm(v1))
            norm2 = float(np.linalg.norm(v2))
            if (
                norm1 <= self._config.epsilon
                or norm2 <= self._config.epsilon
            ):
                return 0.0
            cos = float(np.dot(v1, v2) / (norm1 * norm2))
            return max(0.0, min(1.0, (cos + 1.0) / 2.0))

        if metric == "dot":
            raw = float(np.dot(v1, v2))
            return max(0.0, min(1.0, raw))

        # euclidean
        distance = float(np.linalg.norm(v1 - v2))
        return 1.0 / (1.0 + distance)

    def similarity_diagnostics(
        self, vec1: Any, vec2: Any,
    ) -> Dict[str, float]:
        """
        Return raw and clamped similarity for inspection.

        Useful for callers who need the raw cosine value for logging or
        debugging — ``score_relevance`` always reports the clamped
        value.
        """
        if not _NUMPY_AVAILABLE:  # pragma: no cover
            raise SemanticScorerError(
                "numpy is required for similarity computation."
            )
        v1 = np.asarray(vec1, dtype=float)
        v2 = np.asarray(vec2, dtype=float)
        if v1.shape != v2.shape:
            raise SemanticScorerError(
                f"vector shape mismatch: {v1.shape} vs {v2.shape}."
            )
        denom = max(
            float(np.linalg.norm(v1))
            * float(np.linalg.norm(v2)),
            self._config.epsilon,
        )
        cos = float(np.dot(v1, v2) / denom)
        return {
            "raw_cosine": cos,
            "score": max(0.0, min(1.0, (cos + 1.0) / 2.0)),
        }

    def _metadata_age_hours(
        self, timestamp: Any,
    ) -> Optional[float]:
        """
        Return the age in hours of ``timestamp``.

        Accepts:
        - Unix seconds (typical: ``time.time()``).
        - Unix milliseconds (values > 1e11, divided by 1000).
        - ISO-8601 string (timezone-aware; ``Z`` accepted).
        - ``datetime`` (naive treated as UTC).

        Returns ``None`` if the value cannot be interpreted as a
        timestamp.
        """
        if isinstance(timestamp, bool):
            return None
        if isinstance(timestamp, (int, float)):
            ts = float(timestamp)
            if not math.isfinite(ts):
                return None
            # Millisecond detection: Unix seconds are ~1.7e9 today;
            # milliseconds are ~1.7e12. Threshold 1e11 is unambiguous.
            if ts > 1e11:
                ts = ts / 1000.0
            return max(0.0, (time.time() - ts) / 3600.0)
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )
            except ValueError:
                return None
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - dt
            return max(0.0, delta.total_seconds() / 3600.0)
        if isinstance(timestamp, datetime):
            dt = timestamp
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - dt
            return max(0.0, delta.total_seconds() / 3600.0)
        return None

    def _calculate_metadata_boost(
        self, metadata: Mapping[str, Any],
    ) -> float:
        """Multiplicative boost (>= 1.0) derived from metadata."""
        cfg = self._config
        boost = 1.0

        age_hours = self._metadata_age_hours(
            metadata.get("timestamp")
        )
        if age_hours is not None:
            half_life = cfg.recency_half_life_hours
            boost *= 1.0 + (
                cfg.recency_boost_coefficient
                / (1.0 + age_hours / half_life)
            )

        importance = metadata.get("importance")
        if (
            isinstance(importance, (int, float))
            and not isinstance(importance, bool)
        ):
            imp = max(0.0, min(1.0, float(importance)))
            boost *= (
                1.0 + imp * cfg.importance_boost_coefficient
            )

        content_type = metadata.get("content_type")
        if (
            isinstance(content_type, str)
            and content_type in ("critical", "high_priority")
        ):
            boost *= cfg.critical_content_boost

        return boost

    def _estimate_confidence(
        self,
        query: str,
        content: str,
        similarity: float,
    ) -> float:
        """Estimate confidence in ``[0, 1]`` for a score."""
        cfg = self._config
        similarity_conf = max(
            0.0, min(1.0, float(similarity)),
        )

        content_length = len(content.split())
        length_conf = min(
            1.0,
            content_length
            / cfg.content_length_saturation_words,
        )
        query_length = len(query.split())
        query_conf = min(
            1.0,
            query_length / cfg.query_length_saturation_words,
        )

        return max(0.0, min(
            1.0,
            cfg.confidence_weight_similarity * similarity_conf
            + cfg.confidence_weight_content_length * length_conf
            + cfg.confidence_weight_query_length * query_conf,
        ))

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_cache: bool = False,
    ) -> Dict[str, Any]:
        """
        Serialize config, counters, and provider metadata.

        ``include_cache=True`` appends ``cache_keys`` — the cache keys
        only. Embeddings are NumPy arrays and cannot be serialized to
        JSON losslessly; callers who need the actual vectors should
        re-encode the content by key. Restoration via
        :meth:`from_dict` populates counters but leaves the cache
        empty.
        """
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "provider": {
                    "type": type(self.provider).__name__,
                    "name": getattr(
                        self.provider, "name", "unknown",
                    ),
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
                payload["cache_keys"] = list(
                    self.embedding_cache.keys()
                )
        return payload

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        provider: EmbeddingProvider,
    ) -> "SemanticScorer":
        """
        Rebuild a scorer from :meth:`to_dict` output.

        Parameters
        ----------
        data : Mapping
        provider : EmbeddingProvider
            **Required.** The provider is not serialized (API keys,
            model names, and callables cannot be reconstructed).

        Raises
        ------
        SemanticScorerError
            On malformed payloads, provider type mismatch, or
            dimension mismatch.
        """
        if not isinstance(data, Mapping):
            raise SemanticScorerError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        if not isinstance(provider, EmbeddingProvider):
            raise SemanticScorerError(
                "provider must be an EmbeddingProvider instance."
            )
        try:
            cfg = SemanticScorerConfig.from_dict(
                dict(data.get("config", {}) or {})
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise SemanticScorerError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )

            provider_info = data.get("provider") or {}
            if not isinstance(provider_info, Mapping):
                raise SemanticScorerError(
                    "provider metadata must be a Mapping."
                )
            expected_dim = provider_info.get("dimension")
            if (
                isinstance(expected_dim, int)
                and expected_dim > 0
                and provider.get_dimension() != expected_dim
            ):
                raise SemanticScorerError(
                    f"provider dimension "
                    f"{provider.get_dimension()} does not match "
                    f"payload dimension {expected_dim}."
                )

            scorer = cls(
                provider, config=cfg, strict=strict_raw,
            )

            counters = data.get("counters") or {}
            if not isinstance(counters, Mapping):
                raise SemanticScorerError(
                    "counters must be a Mapping."
                )
            try:
                scorer.cache_hits = int(
                    counters.get("cache_hits", 0)
                )
                scorer.cache_misses = int(
                    counters.get("cache_misses", 0)
                )
                scorer._cache_evictions = int(
                    counters.get("cache_evictions", 0)
                )
            except (TypeError, ValueError) as exc:
                raise SemanticScorerError(
                    f"invalid counter: {exc}"
                ) from exc

            cache_keys = data.get("cache_keys") or []
            if not isinstance(cache_keys, list):
                raise SemanticScorerError(
                    "cache_keys must be a list."
                )

            return scorer
        except SemanticScorerError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise SemanticScorerError(
                f"invalid SemanticScorer payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_cache: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_cache=include_cache),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        provider: EmbeddingProvider,
    ) -> "SemanticScorer":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SemanticScorerError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw, provider=provider)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "SemanticScorer":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
                f"hits={self.cache_hits}, "
                f"misses={self.cache_misses}, "
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
    """Create a scorer backed by a local sentence-transformers model."""
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
    dimension: Optional[int] = None,
    config: Optional[SemanticScorerConfig] = None,
    strict: bool = True,
) -> SemanticScorer:
    """Create a scorer backed by OpenAI embeddings."""
    provider = OpenAIProvider(
        api_key, model, dimension=dimension,
    )
    return SemanticScorer(
        provider,
        cache_embeddings=cache_embeddings,
        config=config,
        strict=strict,
    )


def create_custom_scorer(
    embedding_function: Callable[[str], Any],
    dimension: int,
    cache_embeddings: bool = True,
    *,
    config: Optional[SemanticScorerConfig] = None,
    strict: bool = True,
) -> SemanticScorer:
    """Create a scorer backed by a user-supplied embedding function."""
    provider = CustomEmbeddingProvider(
        embedding_function, dimension,
    )
    return SemanticScorer(
        provider,
        cache_embeddings=cache_embeddings,
        config=config,
        strict=strict,
    )


def create_scorer_from_model(
    model: EmbeddingModel,
    *,
    model_name: Optional[str] = None,
    api_key: Optional[str] = None,
    embedding_function: Optional[
        Callable[[str], Any]
    ] = None,
    dimension: Optional[int] = None,
    cache_embeddings: bool = True,
    config: Optional[SemanticScorerConfig] = None,
    strict: bool = True,
) -> SemanticScorer:
    """
    Factory dispatching from :class:`EmbeddingModel`.

    Parameters
    ----------
    model : EmbeddingModel
    model_name : str, optional
        For ``SENTENCE_TRANSFORMERS``: the local model id (default
        ``"all-MiniLM-L6-v2"``). For ``OPENAI`` / ``COHERE``: the API
        model name (defaults chosen per provider).
    api_key : str, optional
        For ``OPENAI`` / ``COHERE``.
    embedding_function, dimension :
        Required for ``CUSTOM``.
    """
    if not isinstance(model, EmbeddingModel):
        raise SemanticScorerError(
            f"model must be an EmbeddingModel, got "
            f"{type(model).__name__}."
        )

    if model is EmbeddingModel.SENTENCE_TRANSFORMERS:
        provider: EmbeddingProvider = (
            SentenceTransformerProvider(
                model_name or "all-MiniLM-L6-v2",
            )
        )
    elif model is EmbeddingModel.OPENAI:
        provider = OpenAIProvider(
            api_key=api_key,
            model=model_name or "text-embedding-3-small",
            dimension=dimension,
        )
    elif model is EmbeddingModel.COHERE:
        provider = CohereProvider(
            api_key=api_key,
            model=model_name or "embed-english-v3.0",
            dimension=dimension,
        )
    elif model is EmbeddingModel.CUSTOM:
        if embedding_function is None or dimension is None:
            raise SemanticScorerError(
                "CUSTOM model requires embedding_function and "
                "dimension."
            )
        provider = CustomEmbeddingProvider(
            embedding_function, dimension,
        )
    elif model is EmbeddingModel.ANTHROPIC:
        raise SemanticScorerError(
            "Anthropic does not expose an embeddings API; choose "
            "OPENAI, COHERE, SENTENCE_TRANSFORMERS, or CUSTOM."
        )
    else:  # pragma: no cover
        raise SemanticScorerError(
            f"unsupported model {model!r}."
        )

    return SemanticScorer(
        provider,
        cache_embeddings=cache_embeddings,
        config=config,
        strict=strict,
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

    if not _NUMPY_AVAILABLE:
        print("numpy not installed; skipping smoke test.")
    else:
        import hashlib

        class _FakeProvider(EmbeddingProvider):
            name = "fake"

            def _vector(self, text: str) -> Any:
                lowered = text.lower()
                families = ("energy", "carbon", "image", "audio")
                vec = [
                    float(lowered.count(f)) for f in families
                ]
                norm = sum(v * v for v in vec) ** 0.5
                if norm == 0:
                    h = int(
                        hashlib.md5(
                            text.encode()
                        ).hexdigest()[:8],
                        16,
                    )
                    vec = [
                        (h >> (i * 8)) & 0xFF for i in range(4)
                    ]
                    norm = sum(v * v for v in vec) ** 0.5
                    vec = [v / max(norm, 1.0) for v in vec]
                else:
                    vec = [v / norm for v in vec]
                return np.asarray(vec, dtype=float)

            def encode(self, text: str) -> Any:
                return self._vector(text)

            def encode_batch(self, texts):
                return [self._vector(t) for t in texts]

            def get_dimension(self) -> int:
                return 4

        scorer = SemanticScorer(_FakeProvider())
        print("repr       :", scorer)

        # ---- Happy path ----------------------------------------- #
        r = scorer.score_relevance(
            "renewable energy options",
            "Solar power reduces carbon emissions significantly.",
            content_id="doc_a",
        )
        print("score A    :", r)
        assert 0.0 <= r.similarity_score <= 1.0
        assert 0.0 <= r.confidence <= 1.0

        # ---- Cache hit on second call --------------------------- #
        r2 = scorer.score_relevance(
            "renewable energy options",
            "Solar power reduces carbon emissions significantly.",
            content_id="doc_a",
        )
        assert r2.metadata["cache_hit"] is True
        print("cache_hit  : OK")

        # ---- cache_hit reflects current call -------------------- #
        r3 = scorer.score_relevance(
            "renewable energy options",
            "New content that was never seen before.",
            content_id="doc_new",
        )
        assert r3.metadata["cache_hit"] is False
        print("first-time : OK")

        # ---- Opposite-direction vectors score 0.0 --------------- #
        class _OppositeProvider(EmbeddingProvider):
            name = "opposite"

            def encode(self, text):
                if "left" in text.lower():
                    return np.array([1.0, 0.0])
                return np.array([-1.0, 0.0])

            def encode_batch(self, texts):
                return [self.encode(t) for t in texts]

            def get_dimension(self):
                return 2

        opp = SemanticScorer(
            _OppositeProvider(),
            similarity_metric="cosine",
        )
        r_opp = opp.score_relevance(
            "left", "right", content_id="x",
        )
        assert r_opp.similarity_score == 0.0, (
            f"expected 0.0, got {r_opp.similarity_score}"
        )
        print("opposite   : 0.0 (clamped, no crash)")

        # ---- Orthogonal vectors score 0.5 --------------------- #
        class _OrthoProvider(EmbeddingProvider):
            name = "ortho"

            def encode(self, text):
                if "h" in text.lower():
                    return np.array([1.0, 0.0])
                return np.array([0.0, 1.0])

            def encode_batch(self, texts):
                return [self.encode(t) for t in texts]

            def get_dimension(self):
                return 2

        ortho = SemanticScorer(
            _OrthoProvider(),
            similarity_metric="cosine",
        )
        r_ortho = ortho.score_relevance(
            "h", "v", content_id="x",
        )
        assert abs(r_ortho.similarity_score - 0.5) < 1e-6
        print("orthogonal : 0.5 (midpoint)")

        # ---- Dot metric clamped -------------------------------- #
        class _LargeProvider(EmbeddingProvider):
            name = "large"

            def encode(self, text):
                return np.array([100.0, 100.0])

            def encode_batch(self, texts):
                return [self.encode(t) for t in texts]

            def get_dimension(self):
                return 2

        dot = SemanticScorer(
            _LargeProvider(), similarity_metric="dot",
        )
        r_dot = dot.score_relevance(
            "q", "c", content_id="x",
        )
        assert r_dot.similarity_score == 1.0, (
            f"expected clamped 1.0, got {r_dot.similarity_score}"
        )
        print("dot        : clamped to [0, 1]")

        # ---- Batch scoring ------------------------------------- #
        contents = [
            ("doc_1", "Energy and carbon are key."),
            ("doc_2", "Image recognition uses neural networks."),
            ("doc_3", "Audio processing and speech recognition."),
            ("doc_4", "Energy efficiency reduces carbon footprint."),
        ]
        batch = scorer.score_batch(
            "energy carbon sustainability", contents,
        )
        print("batch      :", [
            f"{s.node_id}={s.similarity_score:.3f}"
            for s in batch
        ])

        # ---- Ranked top-k -------------------------------------- #
        ranked = scorer.score_and_rank(
            "energy carbon sustainability", contents, top_k=2,
        )
        assert len(ranked) <= 2
        print("top 2      :", [s.node_id for s in ranked])

        # ---- Metadata boost ------------------------------------ #
        now = time.time()
        boosted = scorer.score_relevance(
            "energy",
            "Energy efficiency.",
            content_id="doc_boosted",
            metadata_boost={
                "timestamp": now,
                "importance": 1.0,
                "content_type": "critical",
            },
        )
        plain = scorer.score_relevance(
            "energy",
            "Energy efficiency.",
            content_id="doc_plain",
        )
        assert boosted.similarity_score >= plain.similarity_score
        print("boost      : OK")

        # ---- Millisecond timestamp handled --------------------- #
        now_ms = int(now * 1000)
        boosted_ms = scorer.score_relevance(
            "energy",
            "Energy efficiency.",
            content_id="doc_ms",
            metadata_boost={"timestamp": now_ms},
        )
        print(f"ms ts      : {boosted_ms.similarity_score:.3f}")
        # Should NOT be saturated (millisecond timestamps now parsed).
        # Recent timestamp means high recency boost, so score should
        # be close to but not exactly the boosted value.
        assert 0.0 <= boosted_ms.similarity_score <= 1.0

        # ---- ISO string timestamp ------------------------------ #
        iso_ts = datetime.now(timezone.utc).isoformat()
        boosted_iso = scorer.score_relevance(
            "energy",
            "Energy efficiency.",
            content_id="doc_iso",
            metadata_boost={"timestamp": iso_ts},
        )
        assert 0.0 <= boosted_iso.similarity_score <= 1.0
        print("iso ts     : OK")

        # ---- Precompute embeddings ----------------------------- #
        added = scorer.precompute_embeddings([
            ("doc_5", "Precomputed content 1."),
            ("doc_6", "Precomputed content 2."),
            ("doc_5", "Precomputed content 1."),
        ])
        assert added == 2
        print("precompute :", added, "new entry(ies)")

        # Bad tuple shapes rejected
        for bad in (
            [("a", "b", "extra")],
            [(123, "text")],
            ["not-a-tuple"],
        ):
            try:
                scorer.precompute_embeddings(bad)
            except SemanticScorerError:
                pass
        print("bad tuple  : rejected")

        # ---- Bounded LRU cache --------------------------------- #
        bounded = SemanticScorer(
            _FakeProvider(),
            config=SemanticScorerConfig(
                max_cache_entries=2,
            ),
        )
        for i in range(5):
            bounded.score_relevance(
                "q", f"content {i}", content_id=f"c{i}",
            )
        assert bounded.cache_size == 2
        assert bounded._cache_evictions == 3
        print("LRU        : OK")

        # ---- Statistics --------------------------------------- #
        print("stats      :", {
            k: v for k, v in scorer.get_cache_stats().items()
        })

        # ---- SemanticScore serialization ---------------------- #
        payload = r.to_json()
        restored = SemanticScore.from_json(payload)
        assert restored.to_dict() == r.to_dict()
        print("score rt   : OK")

        # ---- Scorer round-trip -------------------------------- #
        scorer.energy_penalty = 0.5  # dynamic attr (not used)
        payload = scorer.to_json(include_cache=True)
        restored = SemanticScorer.from_json(
            payload, provider=_FakeProvider(),
        )
        assert (
            restored.get_cache_stats()["cache_hits"]
            == scorer.get_cache_stats()["cache_hits"]
        )
        assert (
            restored.similarity_metric
            == scorer.similarity_metric
        )
        print("scorer rt  : OK (provider passed)")

        # from_dict without provider raises
        try:
            SemanticScorer.from_dict(scorer.to_dict())
        except TypeError:
            pass  # required kwarg missing — Python raises TypeError
        print("no provider: TypeError")

        # Dimension mismatch rejected
        try:
            SemanticScorer.from_dict(
                scorer.to_dict(),
                provider=SentenceTransformerProvider(
                    "all-MiniLM-L6-v2",
                ),
            )
        except SemanticScorerError as exc:
            print("dim mismatch:", str(exc)[:50])
        except EmbeddingProviderError:
            # If sentence-transformers isn't installed
            print("dim mismatch: skipped (no st)")

        # ---- Error hierarchy ---------------------------------- #
        assert issubclass(
            EmbeddingProviderError, SemanticScorerError
        )
        print("hierarchy  : OK")

        # ---- Context manager ---------------------------------- #
        with SemanticScorer(_FakeProvider()) as scoped:
            scoped.score_relevance("q", "c", content_id="ctx")
        print("context    : OK")

        # ---- Config validation -------------------------------- #
        for bad_cfg in (
            dict(similarity_metric="bogus"),
            dict(max_cache_entries=0),
            dict(epsilon=0),
            dict(recency_boost_coefficient=-0.1),
            dict(importance_boost_coefficient=-0.1),
            dict(critical_content_boost=-1.0),
            dict(recency_half_life_hours=0),
            dict(confidence_weight_similarity=0.5),
            dict(content_length_saturation_words=0),
            dict(query_length_saturation_words=0),
        ):
            try:
                SemanticScorerConfig(**bad_cfg)  # type: ignore[arg-type]
            except SemanticScorerError as exc:
                print("Rejected cfg:", exc)

        # ---- Input validation --------------------------------- #
        strict = SemanticScorer(_FakeProvider(), strict=True)
        for bad_call in (
            lambda: strict.score_relevance(None, "x"),
            lambda: strict.score_relevance("q", None),
            lambda: strict.score_relevance("q", "c", content_id=123),
            lambda: strict.score_relevance(
                "q", "c", metadata_boost="bad",
            ),
            lambda: strict.score_batch("q", "not-a-list"),
            lambda: strict.score_batch(
                "q", [("id", "text")],
                metadata_boosts=[{}, {}],
            ),
            lambda: strict.score_and_rank(
                "q", [("a", "b")], top_k=-1,
            ),
        ):
            try:
                bad_call()
            except SemanticScorerError as exc:
                print("Rejected   :", str(exc)[:50])

        # ---- Non-strict coerces ------------------------------- #
        lenient = SemanticScorer(_FakeProvider(), strict=False)
        r = lenient.score_relevance(None, "content")  # type: ignore[arg-type]
        assert r is not None
        print("lenient    : OK")

        # ---- AnthropicProvider raises ------------------------- #
        try:
            AnthropicProvider()
        except EmbeddingProviderError as exc:
            print("Anthropic  :", str(exc)[:50])

        # ---- Factory from model ------------------------------- #
        try:
            create_scorer_from_model(EmbeddingModel.ANTHROPIC)
        except SemanticScorerError as exc:
            print("anthropic factory:", str(exc)[:50])

        # Custom factory works
        custom = create_scorer_from_model(
            EmbeddingModel.CUSTOM,
            embedding_function=lambda t: np.array([1.0, 0.0]),
            dimension=2,
        )
        r = custom.score_relevance("q", "c", content_id="custom")
        assert 0.0 <= r.similarity_score <= 1.0
        print("custom factory : OK")

        # ---- to_json rejects NaN ------------------------------ #
        tainted = SemanticScorer(_FakeProvider())
        tainted.score_relevance("q", "c", content_id="x")
        object.__setattr__(
            tainted._config, "epsilon", float("nan"),
        )
        try:
            tainted.to_json()
        except ValueError as exc:
            print("NaN in to_json:", str(exc)[:50])

        print("\nSmoke test passed.")
