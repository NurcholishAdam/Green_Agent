# -*- coding: utf-8 -*-
"""
Semantic Scoring Module

Replaces keyword-based scoring with embedding-based semantic similarity.
Supports both local embeddings and API-based embeddings for flexibility.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import numpy as np
from abc import ABC, abstractmethod


class EmbeddingModel(Enum):
    """Available embedding models."""
    SENTENCE_TRANSFORMERS = "sentence-transformers"  # Local, offline
    OPENAI = "openai"  # API-based
    ANTHROPIC = "anthropic"  # API-based
    COHERE = "cohere"  # API-based
    CUSTOM = "custom"  # User-provided


@dataclass
class SemanticScore:
    """Semantic similarity score."""
    node_id: str
    query: str
    similarity_score: float
    embedding_model: str
    confidence: float
    metadata: Dict[str, Any]


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""
    
    @abstractmethod
    def encode(self, text: str) -> np.ndarray:
        """Encode text to embedding vector."""
        pass
    
    @abstractmethod
    def encode_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Encode batch of texts."""
        pass
    
    @abstractmethod
    def get_dimension(self) -> int:
        """Get embedding dimension."""
        pass


class SentenceTransformerProvider(EmbeddingProvider):
    """Local sentence-transformers provider (offline capable)."""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Initialize sentence-transformers provider.
        
        Args:
            model_name: HuggingFace model name
                - all-MiniLM-L6-v2: Fast, 384-dim (recommended)
                - all-mpnet-base-v2: Better quality, 768-dim
                - paraphrase-multilingual: Multilingual support
        """
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(model_name)
            self.model_name = model_name
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
    
    def encode(self, text: str) -> np.ndarray:
        """Encode single text."""
        return self.model.encode(text, convert_to_numpy=True)
    
    def encode_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Encode batch of texts."""
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        return [emb for emb in embeddings]
    
    def get_dimension(self) -> int:
        """Get embedding dimension."""
        return self.model.get_sentence_embedding_dimension()


class OpenAIProvider(EmbeddingProvider):
    """OpenAI embeddings provider (API-based)."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "text-embedding-3-small"):
        """
        Initialize OpenAI provider.
        
        Args:
            api_key: OpenAI API key (or set OPENAI_API_KEY env var)
            model: Embedding model
                - text-embedding-3-small: 1536-dim, cost-effective
                - text-embedding-3-large: 3072-dim, higher quality
        """
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=api_key)
            self.model = model
            self._dimension = 1536 if "small" in model else 3072
        except ImportError:
            raise ImportError(
                "openai not installed. "
                "Install with: pip install openai"
            )
    
    def encode(self, text: str) -> np.ndarray:
        """Encode single text."""
        response = self.client.embeddings.create(
            input=text,
            model=self.model
        )
        return np.array(response.data[0].embedding)
    
    def encode_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Encode batch of texts."""
        response = self.client.embeddings.create(
            input=texts,
            model=self.model
        )
        return [np.array(item.embedding) for item in response.data]
    
    def get_dimension(self) -> int:
        """Get embedding dimension."""
        return self._dimension


class CustomEmbeddingProvider(EmbeddingProvider):
    """Custom embedding provider for user-supplied embeddings."""
    
    def __init__(self, embedding_function, dimension: int):
        """
        Initialize custom provider.
        
        Args:
            embedding_function: Function that takes text and returns np.ndarray
            dimension: Embedding dimension
        """
        self.embedding_fn = embedding_function
        self.dimension = dimension
    
    def encode(self, text: str) -> np.ndarray:
        """Encode single text."""
        return self.embedding_fn(text)
    
    def encode_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Encode batch of texts."""
        return [self.embedding_fn(text) for text in texts]
    
    def get_dimension(self) -> int:
        """Get embedding dimension."""
        return self.dimension


class SemanticScorer:
    """
    Semantic scorer using embeddings instead of keyword matching.
    
    Features:
    - Multiple embedding provider support
    - Batch processing for efficiency
    - Caching for repeated queries
    - Hybrid scoring (semantic + metadata)
    """
    
    def __init__(
        self,
        provider: EmbeddingProvider,
        cache_embeddings: bool = True,
        similarity_metric: str = "cosine"
    ):
        """
        Initialize semantic scorer.
        
        Args:
            provider: Embedding provider
            cache_embeddings: Cache embeddings to avoid recomputation
            similarity_metric: Similarity metric ("cosine", "dot", "euclidean")
        """
        self.provider = provider
        self.cache_embeddings = cache_embeddings
        self.similarity_metric = similarity_metric
        
        # Embedding cache
        self.embedding_cache: Dict[str, np.ndarray] = {}
        
        # Statistics
        self.cache_hits = 0
        self.cache_misses = 0
        
    def score_relevance(
        self,
        query: str,
        content: str,
        content_id: Optional[str] = None,
        metadata_boost: Optional[Dict[str, Any]] = None
    ) -> SemanticScore:
        """
        Score relevance of content to query using embeddings.
        
        Args:
            query: Query text
            content: Content to score
            content_id: Optional content identifier for caching
            metadata_boost: Optional metadata for boosting score
            
        Returns:
            Semantic score
        """
        # Get embeddings
        query_emb = self._get_embedding(query, cache_key=f"query:{query}")
        content_emb = self._get_embedding(content, cache_key=content_id)
        
        # Calculate similarity
        similarity = self._calculate_similarity(query_emb, content_emb)
        
        # Apply metadata boost if provided
        if metadata_boost:
            boost_factor = self._calculate_metadata_boost(metadata_boost)
            similarity = min(1.0, similarity * boost_factor)
        
        # Calculate confidence based on embedding quality
        confidence = self._estimate_confidence(query, content, similarity)
        
        return SemanticScore(
            node_id=content_id or "unknown",
            query=query,
            similarity_score=float(similarity),
            embedding_model=self.provider.__class__.__name__,
            confidence=confidence,
            metadata={
                "similarity_metric": self.similarity_metric,
                "cache_hit": content_id in self.embedding_cache if content_id else False
            }
        )
    
    def score_batch(
        self,
        query: str,
        contents: List[Tuple[str, str]],  # [(content_id, content_text), ...]
        metadata_boosts: Optional[List[Dict[str, Any]]] = None
    ) -> List[SemanticScore]:
        """
        Score batch of contents efficiently.
        
        Args:
            query: Query text
            contents: List of (content_id, content_text) tuples
            metadata_boosts: Optional metadata boosts per content
            
        Returns:
            List of semantic scores
        """
        # Get query embedding
        query_emb = self._get_embedding(query, cache_key=f"query:{query}")
        
        # Get content embeddings (use cache when possible)
        content_embeddings = []
        for content_id, content_text in contents:
            emb = self._get_embedding(content_text, cache_key=content_id)
            content_embeddings.append(emb)
        
        # Calculate similarities
        similarities = [
            self._calculate_similarity(query_emb, content_emb)
            for content_emb in content_embeddings
        ]
        
        # Apply metadata boosts
        if metadata_boosts:
            for i, boost in enumerate(metadata_boosts):
                if boost:
                    boost_factor = self._calculate_metadata_boost(boost)
                    similarities[i] = min(1.0, similarities[i] * boost_factor)
        
        # Create scores
        scores = []
        for i, (content_id, content_text) in enumerate(contents):
            confidence = self._estimate_confidence(query, content_text, similarities[i])
            
            scores.append(SemanticScore(
                node_id=content_id,
                query=query,
                similarity_score=float(similarities[i]),
                embedding_model=self.provider.__class__.__name__,
                confidence=confidence,
                metadata={
                    "similarity_metric": self.similarity_metric,
                    "cache_hit": content_id in self.embedding_cache
                }
            ))
        
        return scores
    
    def score_and_rank(
        self,
        query: str,
        contents: List[Tuple[str, str]],
        top_k: Optional[int] = None
    ) -> List[SemanticScore]:
        """
        Score and rank contents by relevance.
        
        Args:
            query: Query text
            contents: List of (content_id, content_text)
            top_k: Return top k results (None = all)
            
        Returns:
            Ranked list of semantic scores
        """
        scores = self.score_batch(query, contents)
        scores.sort(key=lambda x: x.similarity_score, reverse=True)
        
        if top_k:
            return scores[:top_k]
        return scores
    
    def get_embedding(self, text: str) -> np.ndarray:
        """Get embedding for text (public interface)."""
        return self._get_embedding(text)
    
    def precompute_embeddings(
        self,
        contents: List[Tuple[str, str]]  # [(content_id, content_text), ...]
    ):
        """
        Precompute and cache embeddings for contents.
        
        Useful for indexing large document collections.
        """
        for content_id, content_text in contents:
            self._get_embedding(content_text, cache_key=content_id)
    
    def clear_cache(self):
        """Clear embedding cache."""
        self.embedding_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total_requests if total_requests > 0 else 0
        
        return {
            "cache_size": len(self.embedding_cache),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": hit_rate,
            "embedding_dimension": self.provider.get_dimension()
        }
    
    def _get_embedding(
        self,
        text: str,
        cache_key: Optional[str] = None
    ) -> np.ndarray:
        """Get embedding with caching."""
        if cache_key and self.cache_embeddings:
            if cache_key in self.embedding_cache:
                self.cache_hits += 1
                return self.embedding_cache[cache_key]
            
            self.cache_misses += 1
        
        # Compute embedding
        embedding = self.provider.encode(text)
        
        # Cache if enabled
        if cache_key and self.cache_embeddings:
            self.embedding_cache[cache_key] = embedding
        
        return embedding
    
    def _calculate_similarity(
        self,
        vec1: np.ndarray,
        vec2: np.ndarray
    ) -> float:
        """Calculate similarity between vectors."""
        if self.similarity_metric == "cosine":
            # Cosine similarity
            dot_product = np.dot(vec1, vec2)
            norm1 = np.linalg.norm(vec1)
            norm2 = np.linalg.norm(vec2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            return dot_product / (norm1 * norm2)
        
        elif self.similarity_metric == "dot":
            # Dot product (assumes normalized vectors)
            return float(np.dot(vec1, vec2))
        
        elif self.similarity_metric == "euclidean":
            # Negative Euclidean distance (convert to similarity)
            distance = np.linalg.norm(vec1 - vec2)
            return 1.0 / (1.0 + distance)
        
        else:
            raise ValueError(f"Unknown similarity metric: {self.similarity_metric}")
    
    def _calculate_metadata_boost(
        self,
        metadata: Dict[str, Any]
    ) -> float:
        """Calculate boost factor from metadata."""
        boost = 1.0
        
        # Recency boost
        if "timestamp" in metadata:
            import time
            age_hours = (time.time() - metadata["timestamp"]) / 3600
            recency_boost = 1.0 + (0.2 / (1.0 + age_hours / 24.0))
            boost *= recency_boost
        
        # Importance boost
        if "importance" in metadata:
            importance = metadata["importance"]  # 0-1 scale
            boost *= (1.0 + importance * 0.3)
        
        # Type boost
        if "content_type" in metadata:
            if metadata["content_type"] in ["critical", "high_priority"]:
                boost *= 1.2
        
        return boost
    
    def _estimate_confidence(
        self,
        query: str,
        content: str,
        similarity: float
    ) -> float:
        """Estimate confidence in similarity score."""
        # Higher confidence for:
        # - High similarity
        # - Longer content (more context)
        # - Longer query (more specific)
        
        similarity_conf = similarity  # Already 0-1
        
        content_length = len(content.split())
        length_conf = min(1.0, content_length / 100.0)  # Saturates at 100 words
        
        query_length = len(query.split())
        query_conf = min(1.0, query_length / 10.0)  # Saturates at 10 words
        
        # Weighted combination
        confidence = (
            0.6 * similarity_conf +
            0.2 * length_conf +
            0.2 * query_conf
        )
        
        return float(confidence)


# Convenience functions for common use cases

def create_local_scorer(
    model_name: str = "all-MiniLM-L6-v2",
    cache_embeddings: bool = True
) -> SemanticScorer:
    """
    Create semantic scorer with local embeddings (offline capable).
    
    Args:
        model_name: Sentence-transformers model name
        cache_embeddings: Enable caching
        
    Returns:
        Configured SemanticScorer
    """
    provider = SentenceTransformerProvider(model_name)
    return SemanticScorer(provider, cache_embeddings=cache_embeddings)


def create_openai_scorer(
    api_key: Optional[str] = None,
    model: str = "text-embedding-3-small",
    cache_embeddings: bool = True
) -> SemanticScorer:
    """
    Create semantic scorer with OpenAI embeddings.
    
    Args:
        api_key: OpenAI API key
        model: Embedding model
        cache_embeddings: Enable caching
        
    Returns:
        Configured SemanticScorer
    """
    provider = OpenAIProvider(api_key, model)
    return SemanticScorer(provider, cache_embeddings=cache_embeddings)


def create_custom_scorer(
    embedding_function,
    dimension: int,
    cache_embeddings: bool = True
) -> SemanticScorer:
    """
    Create semantic scorer with custom embedding function.
    
    Args:
        embedding_function: Function(str) -> np.ndarray
        dimension: Embedding dimension
        cache_embeddings: Enable caching
        
    Returns:
        Configured SemanticScorer
    """
    provider = CustomEmbeddingProvider(embedding_function, dimension)
    return SemanticScorer(provider, cache_embeddings=cache_embeddings)
