# -*- coding: utf-8 -*-
"""
Context Compression Module

Implements intelligent context compression for retrieved content
to minimize token usage while preserving semantic information.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import re


@dataclass
class CompressionResult:
    """Result of context compression."""
    original_text: str
    compressed_text: str
    original_tokens: int
    compressed_tokens: int
    compression_ratio: float
    preserved_entities: List[str]
    metadata: Dict[str, Any]


class ContextCompressor:
    """
    Compresses retrieved context to reduce token usage.
    
    Strategies:
    - Entity-preserving summarization
    - Redundancy removal
    - Sentence ranking and selection
    - Semantic chunking
    """
    
    def __init__(
        self,
        target_compression_ratio: float = 0.5,
        preserve_entities: bool = True,
        preserve_numbers: bool = True
    ):
        """
        Initialize context compressor.
        
        Args:
            target_compression_ratio: Target ratio (0.5 = 50% compression)
            preserve_entities: Whether to preserve named entities
            preserve_numbers: Whether to preserve numerical data
        """
        self.target_ratio = target_compression_ratio
        self.preserve_entities = preserve_entities
        self.preserve_numbers = preserve_numbers
        
        # Compression statistics
        self.compression_history = []
        
    def compress(
        self,
        text: str,
        query: Optional[str] = None,
        max_tokens: Optional[int] = None
    ) -> CompressionResult:
        """
        Compress text while preserving key information.
        
        Args:
            text: Text to compress
            query: Optional query for relevance-guided compression
            max_tokens: Maximum tokens in compressed output
            
        Returns:
            Compression result
        """
        original_tokens = self._estimate_tokens(text)
        
        # Extract entities to preserve
        entities = self._extract_entities(text) if self.preserve_entities else []
        
        # Split into sentences
        sentences = self._split_sentences(text)
        
        # Score sentences by importance
        scored_sentences = self._score_sentences(sentences, query, entities)
        
        # Select sentences to keep
        target_tokens = max_tokens or int(original_tokens * self.target_ratio)
        selected = self._select_sentences(scored_sentences, target_tokens)
        
        # Reconstruct compressed text
        compressed = self._reconstruct_text(selected, entities)
        compressed_tokens = self._estimate_tokens(compressed)
        
        # Create result
        result = CompressionResult(
            original_text=text,
            compressed_text=compressed,
            original_tokens=original_tokens,
            compressed_tokens=compressed_tokens,
            compression_ratio=compressed_tokens / original_tokens if original_tokens > 0 else 0,
            preserved_entities=entities,
            metadata={
                "sentences_original": len(sentences),
                "sentences_kept": len(selected),
                "query_guided": query is not None
            }
        )
        
        self.compression_history.append(result)
        return result
    
    def compress_batch(
        self,
        texts: List[str],
        query: Optional[str] = None,
        total_token_budget: int = 4096
    ) -> List[CompressionResult]:
        """
        Compress multiple texts within total token budget.
        
        Args:
            texts: List of texts to compress
            query: Optional query for relevance guidance
            total_token_budget: Total token budget for all texts
            
        Returns:
            List of compression results
        """
        # Estimate original tokens
        original_tokens = [self._estimate_tokens(t) for t in texts]
        total_original = sum(original_tokens)
        
        # Allocate budget proportionally
        results = []
        for text, orig_tokens in zip(texts, original_tokens):
            allocated_budget = int(
                (orig_tokens / total_original) * total_token_budget
            )
            result = self.compress(text, query, allocated_budget)
            results.append(result)
        
        return results
    
    def adaptive_compress(
        self,
        text: str,
        min_ratio: float = 0.3,
        max_ratio: float = 0.8,
        quality_threshold: float = 0.7
    ) -> CompressionResult:
        """
        Adaptively compress text to meet quality threshold.
        
        Args:
            text: Text to compress
            min_ratio: Minimum compression ratio
            max_ratio: Maximum compression ratio
            quality_threshold: Minimum quality score
            
        Returns:
            Compression result meeting quality threshold
        """
        # Binary search for optimal compression ratio
        low, high = min_ratio, max_ratio
        best_result = None
        
        for _ in range(5):  # Max iterations
            mid = (low + high) / 2
            self.target_ratio = mid
            result = self.compress(text)
            quality = self._estimate_quality(result)
            
            if quality >= quality_threshold:
                best_result = result
                high = mid  # Try more compression
            else:
                low = mid  # Less compression needed
        
        return best_result or self.compress(text)
    
    def get_compression_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        if not self.compression_history:
            return {"status": "no_compressions"}
        
        avg_ratio = sum(
            r.compression_ratio for r in self.compression_history
        ) / len(self.compression_history)
        
        total_saved = sum(
            r.original_tokens - r.compressed_tokens
            for r in self.compression_history
        )
        
        return {
            "total_compressions": len(self.compression_history),
            "avg_compression_ratio": avg_ratio,
            "total_tokens_saved": total_saved,
            "avg_entities_preserved": sum(
                len(r.preserved_entities) for r in self.compression_history
            ) / len(self.compression_history)
        }
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count (rough approximation)."""
        return len(text.split()) * 1.3  # Rough estimate
    
    def _extract_entities(self, text: str) -> List[str]:
        """Extract named entities (simplified)."""
        # Simplified: extract capitalized words and numbers
        entities = []
        
        # Capitalized words (potential names)
        entities.extend(re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text))
        
        # Numbers and dates
        if self.preserve_numbers:
            entities.extend(re.findall(r'\b\d+(?:\.\d+)?(?:%|kg|m|km)?\b', text))
        
        return list(set(entities))
    
    def _split_sentences(self, text: str) -> List[str]:
        """Split text into sentences."""
        # Simple sentence splitting
        sentences = re.split(r'[.!?]+\s+', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _score_sentences(
        self,
        sentences: List[str],
        query: Optional[str],
        entities: List[str]
    ) -> List[Tuple[str, float]]:
        """Score sentences by importance."""
        scored = []
        
        for sentence in sentences:
            score = 0.0
            
            # Length penalty (prefer medium-length sentences)
            length = len(sentence.split())
            if 5 <= length <= 30:
                score += 0.3
            
            # Entity presence
            entity_count = sum(1 for e in entities if e in sentence)
            score += entity_count * 0.2
            
            # Query relevance
            if query:
                query_words = set(query.lower().split())
                sentence_words = set(sentence.lower().split())
                overlap = len(query_words & sentence_words)
                score += overlap * 0.5
            
            # Position bonus (first and last sentences often important)
            if sentences.index(sentence) in [0, len(sentences) - 1]:
                score += 0.2
            
            scored.append((sentence, score))
        
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored
    
    def _select_sentences(
        self,
        scored_sentences: List[Tuple[str, float]],
        target_tokens: int
    ) -> List[str]:
        """Select sentences to meet token budget."""
        selected = []
        current_tokens = 0
        
        for sentence, score in scored_sentences:
            sentence_tokens = self._estimate_tokens(sentence)
            if current_tokens + sentence_tokens <= target_tokens:
                selected.append(sentence)
                current_tokens += sentence_tokens
            
            if current_tokens >= target_tokens * 0.9:
                break
        
        return selected
    
    def _reconstruct_text(
        self,
        sentences: List[str],
        entities: List[str]
    ) -> str:
        """Reconstruct compressed text."""
        # Join sentences
        text = ". ".join(sentences)
        
        # Ensure entities are present
        if self.preserve_entities:
            for entity in entities:
                if entity not in text:
                    # Add entity context if missing
                    text += f" [{entity}]"
        
        return text
    
    def _estimate_quality(self, result: CompressionResult) -> float:
        """Estimate quality of compression."""
        # Quality based on:
        # - Compression ratio (not too aggressive)
        # - Entity preservation
        # - Sentence coherence
        
        ratio_score = 1.0 - abs(result.compression_ratio - self.target_ratio)
        entity_score = min(len(result.preserved_entities) / 10, 1.0)
        
        quality = (ratio_score * 0.6 + entity_score * 0.4)
        return quality


class SemanticChunker:
    """
    Chunks text into semantically coherent segments.
    
    Used for efficient retrieval and compression of long documents.
    """
    
    def __init__(
        self,
        chunk_size: int = 512,
        overlap: int = 50
    ):
        """
        Initialize semantic chunker.
        
        Args:
            chunk_size: Target chunk size in tokens
            overlap: Overlap between chunks in tokens
        """
        self.chunk_size = chunk_size
        self.overlap = overlap
    
    def chunk(self, text: str) -> List[Dict[str, Any]]:
        """
        Chunk text into semantic segments.
        
        Args:
            text: Text to chunk
            
        Returns:
            List of chunks with metadata
        """
        # Split into paragraphs
        paragraphs = text.split('\n\n')
        
        chunks = []
        current_chunk = []
        current_tokens = 0
        
        for para in paragraphs:
            para_tokens = len(para.split()) * 1.3
            
            if current_tokens + para_tokens > self.chunk_size:
                # Save current chunk
                if current_chunk:
                    chunk_text = '\n\n'.join(current_chunk)
                    chunks.append({
                        "text": chunk_text,
                        "tokens": current_tokens,
                        "paragraphs": len(current_chunk)
                    })
                
                # Start new chunk with overlap
                if self.overlap > 0 and current_chunk:
                    current_chunk = [current_chunk[-1], para]
                    current_tokens = len(current_chunk[-1].split()) * 1.3 + para_tokens
                else:
                    current_chunk = [para]
                    current_tokens = para_tokens
            else:
                current_chunk.append(para)
                current_tokens += para_tokens
        
        # Add final chunk
        if current_chunk:
            chunk_text = '\n\n'.join(current_chunk)
            chunks.append({
                "text": chunk_text,
                "tokens": current_tokens,
                "paragraphs": len(current_chunk)
            })
        
        return chunks
