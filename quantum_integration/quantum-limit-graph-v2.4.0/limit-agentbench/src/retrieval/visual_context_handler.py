# -*- coding: utf-8 -*-
"""
Visual Context Handler

Handles visual context in multimodal retrieval inspired by VimRAG's
approach to navigating massive visual context efficiently.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib
from datetime import datetime


class VisualContextType(Enum):
    """Types of visual context."""
    IMAGE = "image"
    DIAGRAM = "diagram"
    CHART = "chart"
    SCREENSHOT = "screenshot"
    VIDEO_FRAME = "video_frame"


@dataclass
class VisualContext:
    """Visual context item."""
    context_id: str
    context_type: VisualContextType
    source_path: str
    caption: Optional[str]
    embedding: Optional[List[float]]
    token_cost: int
    energy_cost: float
    metadata: Dict[str, Any]
    timestamp: float


@dataclass
class VisualRetrievalResult:
    """Result of visual context retrieval."""
    contexts: List[VisualContext]
    total_token_cost: int
    total_energy_cost: float
    relevance_scores: List[float]
    metadata: Dict[str, Any]


class VisualContextHandler:
    """
    Handles visual context in multimodal retrieval.
    
    Features:
    - Efficient visual context indexing
    - Token-aware visual retrieval
    - Energy-efficient visual processing
    - Caption-based filtering
    - Visual-text alignment
    """
    
    def __init__(
        self,
        visual_token_cost: int = 256,
        visual_energy_multiplier: float = 2.5,
        max_visual_contexts: int = 5
    ):
        """
        Initialize visual context handler.
        
        Args:
            visual_token_cost: Token cost per visual context
            visual_energy_multiplier: Energy multiplier for visual processing
            max_visual_contexts: Maximum visual contexts per retrieval
        """
        self.visual_token_cost = visual_token_cost
        self.visual_energy_multiplier = visual_energy_multiplier
        self.max_visual_contexts = max_visual_contexts
        
        # Visual context storage
        self.visual_contexts: Dict[str, VisualContext] = {}
        self.caption_index: Dict[str, List[str]] = {}  # word -> context_ids
        
        # Statistics
        self.retrieval_stats = {
            "total_retrievals": 0,
            "total_contexts_retrieved": 0,
            "avg_relevance": 0.0
        }
    
    def add_visual_context(
        self,
        source_path: str,
        context_type: VisualContextType,
        caption: Optional[str] = None,
        embedding: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Add visual context to handler.
        
        Args:
            source_path: Path to visual content
            context_type: Type of visual context
            caption: Optional text caption
            embedding: Optional visual embedding
            metadata: Additional metadata
            
        Returns:
            Context ID
        """
        context_id = self._generate_context_id(source_path)
        
        # Calculate costs
        token_cost = self.visual_token_cost
        energy_cost = token_cost * 0.000001 * self.visual_energy_multiplier
        
        context = VisualContext(
            context_id=context_id,
            context_type=context_type,
            source_path=source_path,
            caption=caption,
            embedding=embedding,
            token_cost=token_cost,
            energy_cost=energy_cost,
            metadata=metadata or {},
            timestamp=datetime.now().timestamp()
        )
        
        self.visual_contexts[context_id] = context
        
        # Index caption
        if caption:
            self._index_caption(context_id, caption)
        
        return context_id
    
    def retrieve_visual_contexts(
        self,
        query: str,
        max_contexts: Optional[int] = None,
        context_types: Optional[List[VisualContextType]] = None,
        token_budget: Optional[int] = None,
        energy_budget: Optional[float] = None
    ) -> VisualRetrievalResult:
        """
        Retrieve relevant visual contexts.
        
        Args:
            query: Text query
            max_contexts: Maximum contexts to retrieve
            context_types: Filter by context types
            token_budget: Token budget for retrieval
            energy_budget: Energy budget for retrieval
            
        Returns:
            Visual retrieval result
        """
        max_ctx = max_contexts or self.max_visual_contexts
        
        # Filter by type
        candidates = [
            ctx for ctx in self.visual_contexts.values()
            if not context_types or ctx.context_type in context_types
        ]
        
        # Score by relevance
        scored = [
            (ctx, self._score_relevance(query, ctx))
            for ctx in candidates
        ]
        scored.sort(key=lambda x: x[1], reverse=True)
        
        # Select within budget
        selected = []
        total_tokens = 0
        total_energy = 0.0
        relevance_scores = []
        
        for ctx, score in scored:
            if len(selected) >= max_ctx:
                break
            
            # Check budgets
            if token_budget and total_tokens + ctx.token_cost > token_budget:
                continue
            if energy_budget and total_energy + ctx.energy_cost > energy_budget:
                continue
            
            selected.append(ctx)
            total_tokens += ctx.token_cost
            total_energy += ctx.energy_cost
            relevance_scores.append(score)
        
        # Update statistics
        self.retrieval_stats["total_retrievals"] += 1
        self.retrieval_stats["total_contexts_retrieved"] += len(selected)
        if relevance_scores:
            avg_rel = sum(relevance_scores) / len(relevance_scores)
            self.retrieval_stats["avg_relevance"] = (
                (self.retrieval_stats["avg_relevance"] * 
                 (self.retrieval_stats["total_retrievals"] - 1) + avg_rel) /
                self.retrieval_stats["total_retrievals"]
            )
        
        return VisualRetrievalResult(
            contexts=selected,
            total_token_cost=total_tokens,
            total_energy_cost=total_energy,
            relevance_scores=relevance_scores,
            metadata={
                "query": query,
                "candidates_evaluated": len(candidates),
                "contexts_selected": len(selected)
            }
        )
    
    def align_visual_text(
        self,
        text_content: str,
        visual_contexts: List[VisualContext]
    ) -> List[Tuple[str, VisualContext, float]]:
        """
        Align text content with visual contexts.
        
        Args:
            text_content: Text content
            visual_contexts: Visual contexts to align
            
        Returns:
            List of (text_segment, visual_context, alignment_score)
        """
        alignments = []
        
        # Split text into segments
        segments = self._segment_text(text_content)
        
        for segment in segments:
            # Find best matching visual context
            best_match = None
            best_score = 0.0
            
            for ctx in visual_contexts:
                score = self._score_alignment(segment, ctx)
                if score > best_score:
                    best_score = score
                    best_match = ctx
            
            if best_match and best_score > 0.3:
                alignments.append((segment, best_match, best_score))
        
        return alignments
    
    def optimize_visual_budget(
        self,
        query: str,
        total_token_budget: int,
        text_priority: float = 0.7
    ) -> Dict[str, int]:
        """
        Optimize token budget allocation between text and visual.
        
        Args:
            query: Query for context
            total_token_budget: Total token budget
            text_priority: Priority for text (0-1)
            
        Returns:
            Budget allocation {text: tokens, visual: tokens}
        """
        # Allocate based on priority
        text_budget = int(total_token_budget * text_priority)
        visual_budget = total_token_budget - text_budget
        
        # Adjust based on query characteristics
        if self._is_visual_heavy_query(query):
            # Shift more budget to visual
            shift = int(text_budget * 0.2)
            text_budget -= shift
            visual_budget += shift
        
        return {
            "text_tokens": text_budget,
            "visual_tokens": visual_budget,
            "max_visual_contexts": visual_budget // self.visual_token_cost
        }
    
    def get_visual_stats(self) -> Dict[str, Any]:
        """Get visual context statistics."""
        type_counts = {}
        for ctx in self.visual_contexts.values():
            type_name = ctx.context_type.value
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
        
        total_tokens = sum(ctx.token_cost for ctx in self.visual_contexts.values())
        total_energy = sum(ctx.energy_cost for ctx in self.visual_contexts.values())
        
        return {
            "total_contexts": len(self.visual_contexts),
            "contexts_by_type": type_counts,
            "total_token_cost": total_tokens,
            "total_energy_cost": total_energy,
            "retrieval_stats": self.retrieval_stats,
            "avg_token_cost_per_context": (
                total_tokens / len(self.visual_contexts)
                if self.visual_contexts else 0
            )
        }
    
    def _generate_context_id(self, source_path: str) -> str:
        """Generate unique context ID."""
        hash_input = f"{source_path}:{datetime.now().timestamp()}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:16]
    
    def _index_caption(self, context_id: str, caption: str):
        """Index caption for retrieval."""
        words = caption.lower().split()
        for word in words:
            if word not in self.caption_index:
                self.caption_index[word] = []
            self.caption_index[word].append(context_id)
    
    def _score_relevance(self, query: str, context: VisualContext) -> float:
        """Score relevance of visual context to query."""
        score = 0.0
        
        # Caption matching
        if context.caption:
            query_words = set(query.lower().split())
            caption_words = set(context.caption.lower().split())
            overlap = len(query_words & caption_words)
            score += overlap * 0.5
        
        # Recency bonus
        age_hours = (datetime.now().timestamp() - context.timestamp) / 3600
        recency = 1.0 / (1.0 + age_hours / 24.0)
        score += recency * 0.3
        
        # Type bonus (some types more relevant for certain queries)
        if "chart" in query.lower() and context.context_type == VisualContextType.CHART:
            score += 0.2
        
        return score
    
    def _segment_text(self, text: str) -> List[str]:
        """Segment text for alignment."""
        # Simple paragraph-based segmentation
        return [p.strip() for p in text.split('\n\n') if p.strip()]
    
    def _score_alignment(self, text_segment: str, visual_context: VisualContext) -> float:
        """Score alignment between text and visual context."""
        if not visual_context.caption:
            return 0.0
        
        # Word overlap
        text_words = set(text_segment.lower().split())
        caption_words = set(visual_context.caption.lower().split())
        overlap = len(text_words & caption_words)
        
        return overlap / max(len(text_words), 1)
    
    def _is_visual_heavy_query(self, query: str) -> bool:
        """Check if query is visual-heavy."""
        visual_keywords = [
            "image", "picture", "diagram", "chart", "graph",
            "screenshot", "visual", "show", "display"
        ]
        query_lower = query.lower()
        return any(kw in query_lower for kw in visual_keywords)
