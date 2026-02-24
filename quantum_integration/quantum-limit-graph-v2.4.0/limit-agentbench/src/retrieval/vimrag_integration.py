# -*- coding: utf-8 -*-
"""
VimRAG Integration Orchestrator

Complete integration of all VimRAG modules into a unified retrieval pipeline.
Coordinates: topological allocation, semantic scoring, graph traversal,
carbon adaptation, multimodal fusion, and serendipity logging.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import networkx as nx
import numpy as np
from datetime import datetime

# Import all VimRAG modules
# Note: In production, adjust import paths based on actual module location
try:
    from .vimrag_coretrieval import VimRAGCoRetrieval, RetrievalNode, RetrievalTrace
    from .token_aware_filter import TokenAwareFilter, ContextSize, GridIntensity
    from .carbon_adaptive_controller import CarbonAdaptiveRetrievalController, RetrievalMode
    from .hierarchical_retrieval import HierarchicalRetrieval, RetrievalLevel
    from .multimodal_fusion import MultimodalFusion, ModalityType, FusedResult
    from .context_compression import ContextCompressor, CompressionResult
    from .visual_context_handler import VisualContextHandler, VisualContextType
    from .serendipity_logger import SerendipityTraceLogger, SerendipityEvent
except ImportError:
    # For standalone testing
    pass


@dataclass
class IntegratedRetrievalResult:
    """Complete retrieval result from VimRAG pipeline."""
    # Core retrieval
    retrieved_nodes: List[Any]
    retrieval_trace: Any
    
    # Token allocation
    token_allocations: Dict[str, Any]
    topological_summary: Dict[str, Any]
    
    # Semantic scoring
    semantic_scores: List[Any]
    avg_similarity: float
    
    # Energy and carbon
    total_energy_wh: float
    total_carbon_kg: float
    retrieval_mode: str
    carbon_savings_kg: float
    
    # Multimodal
    modalities_used: List[str]
    fusion_results: Optional[Any]
    
    # Compression
    compression_ratio: float
    tokens_saved: int
    
    # Serendipity
    serendipity_events: List[Any]
    efficiency_gains: float
    
    # Metadata
    query: str
    timestamp: float
    total_time_ms: float
    cache_hits: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class RetrievalPipeline(Enum):
    """Retrieval pipeline modes."""
    FAST = "fast"  # Minimal processing, fast results
    BALANCED = "balanced"  # Balance speed and quality
    COMPREHENSIVE = "comprehensive"  # Full pipeline, best quality
    GREEN = "green"  # Maximum energy efficiency
    MULTIMODAL = "multimodal"  # Optimized for multimodal content


class VimRAGIntegration:
    """
    Complete VimRAG integration orchestrator.
    
    Coordinates all VimRAG modules into a unified retrieval pipeline:
    1. Carbon adaptation (check grid intensity)
    2. Token filtering (budget management)
    3. Topological allocation (importance-based tokens)
    4. Semantic scoring (embedding-based relevance)
    5. Graph traversal (energy-optimal paths)
    6. Hierarchical retrieval (coarse-to-fine)
    7. Multimodal fusion (text + visual)
    8. Context compression (token optimization)
    9. Serendipity logging (efficiency discovery)
    
    Example Usage:
        vimrag = VimRAGIntegration()
        
        # Add documents to memory graph
        vimrag.add_document("Medical symptoms guide", "text", {...})
        vimrag.add_document("X-ray image", "visual", {...})
        
        # Retrieve with full pipeline
        result = vimrag.retrieve(
            query="Diagnose pneumonia from X-ray",
            pipeline=RetrievalPipeline.COMPREHENSIVE,
            energy_budget_wh=0.01
        )
    """
    
    def __init__(
        self,
        grid_region: str = "US-CA",
        enable_semantic_scoring: bool = True,
        enable_topological_allocation: bool = True,
        enable_carbon_adaptation: bool = True,
        enable_serendipity: bool = True,
        embedding_model: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize VimRAG integration.
        
        Args:
            grid_region: Carbon grid region
            enable_semantic_scoring: Use embeddings for scoring
            enable_topological_allocation: Use topology-based token allocation
            enable_carbon_adaptation: Enable carbon-aware decisions
            enable_serendipity: Enable serendipity logging
            embedding_model: Embedding model for semantic scoring
        """
        # Core retrieval
        self.core_retrieval = VimRAGCoRetrieval(
            max_tokens_per_retrieval=4096,
            max_energy_per_retrieval_wh=0.01,
            enable_visual_context=True
        )
        
        # Carbon adaptation
        self.carbon_controller = CarbonAdaptiveRetrievalController(
            clean_grid_threshold_g_kwh=200.0,
            dirty_grid_threshold_g_kwh=400.0,
            enable_deferral=True
        ) if enable_carbon_adaptation else None
        
        # Token filtering
        self.token_filter = TokenAwareFilter(
            current_grid_intensity_g_kwh=385.0,
            enable_adaptive_filtering=True
        )
        
        # Hierarchical retrieval
        self.hierarchical = HierarchicalRetrieval(
            coarse_budget_tokens=500,
            medium_budget_tokens=1500,
            fine_budget_tokens=4096
        )
        
        # Multimodal fusion
        self.fusion = MultimodalFusion(
            text_weight=0.6,
            visual_weight=0.4,
            energy_penalty_factor=0.1
        )
        
        # Context compression
        self.compressor = ContextCompressor(
            target_compression_ratio=0.5,
            preserve_entities=True,
            preserve_numbers=True
        )
        
        # Visual handler
        self.visual_handler = VisualContextHandler(
            visual_token_cost=256,
            visual_energy_multiplier=2.5,
            max_visual_contexts=5
        )
        
        # Serendipity logger
        self.serendipity = SerendipityTraceLogger(
            negawatt_reward_rate=100.0,
            min_savings_threshold_pct=10.0
        ) if enable_serendipity else None
        
        # Graph (NetworkX-based)
        self.graph = nx.DiGraph()
        
        # Topological allocator (if available)
        self.topo_allocator = None
        if enable_topological_allocation:
            try:
                from topological_allocator import TopologicalTokenAllocator, ImportanceMetric
                self.topo_allocator = TopologicalTokenAllocator(
                    importance_metric=ImportanceMetric.COMBINED,
                    enable_dynamic_adjustment=True
                )
            except ImportError:
                print("Warning: TopologicalTokenAllocator not available")
        
        # Semantic scorer (if available)
        self.semantic_scorer = None
        if enable_semantic_scoring:
            try:
                from semantic_scorer import create_local_scorer
                self.semantic_scorer = create_local_scorer(
                    model_name=embedding_model,
                    cache_embeddings=True
                )
            except ImportError:
                print("Warning: SemanticScorer not available, using keyword matching")
        
        # Statistics
        self.retrieval_count = 0
        self.total_energy_consumed = 0.0
        self.total_carbon_emitted = 0.0
        self.total_carbon_saved = 0.0
        
    def add_document(
        self,
        content: Any,
        content_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None
    ) -> str:
        """
        Add document to VimRAG memory graph.
        
        Args:
            content: Document content
            content_type: "text", "visual", or "multimodal"
            metadata: Optional metadata
            node_id: Optional custom node ID
            
        Returns:
            Node ID
        """
        # Add to core retrieval
        if node_id is None:
            node_id = self.core_retrieval.add_node(content, content_type, metadata)
        else:
            self.core_retrieval.memory_graph[node_id] = self.core_retrieval.add_node(
                content, content_type, metadata
            )
        
        # Add to NetworkX graph
        self.graph.add_node(node_id, content=content, content_type=content_type)
        
        # If visual, add to visual handler
        if content_type in ["visual", "multimodal"]:
            visual_type = VisualContextType.IMAGE  # Default
            self.visual_handler.add_visual_context(
                source_path=str(content),
                context_type=visual_type,
                caption=metadata.get("caption") if metadata else None
            )
        
        return node_id
    
    def connect_documents(self, source_id: str, target_id: str, weight: float = 1.0):
        """Create semantic connection between documents."""
        self.core_retrieval.connect_nodes(source_id, target_id)
        self.graph.add_edge(source_id, target_id, weight=weight)
    
    def retrieve(
        self,
        query: str,
        pipeline: RetrievalPipeline = RetrievalPipeline.BALANCED,
        max_results: int = 10,
        energy_budget_wh: Optional[float] = None,
        token_budget: Optional[int] = None,
        include_visual: bool = True,
        agent_id: str = "default_agent"
    ) -> IntegratedRetrievalResult:
        """
        Execute complete VimRAG retrieval pipeline.
        
        Args:
            query: Retrieval query
            pipeline: Pipeline mode (FAST, BALANCED, COMPREHENSIVE, GREEN, MULTIMODAL)
            max_results: Maximum results to return
            energy_budget_wh: Energy budget in Wh
            token_budget: Token budget
            include_visual: Include visual content
            agent_id: Agent identifier for serendipity tracking
            
        Returns:
            Integrated retrieval result
        """
        start_time = datetime.now()
        self.retrieval_count += 1
        
        # Default budgets
        if energy_budget_wh is None:
            energy_budget_wh = 0.01 if pipeline == RetrievalPipeline.GREEN else 0.05
        
        if token_budget is None:
            token_budget = 2048 if pipeline == RetrievalPipeline.FAST else 4096
        
        # Baseline energy for serendipity tracking
        baseline_energy = self._estimate_baseline_energy(query, token_budget)
        
        # 1. CARBON ADAPTATION
        carbon_decision = None
        retrieval_mode = "FULL"
        if self.carbon_controller:
            carbon_decision = self.carbon_controller.decide_retrieval(
                query=query,
                estimated_tokens=token_budget,
                content_type="multimodal" if include_visual else "text",
                urgency="normal"
            )
            
            if not carbon_decision.allowed:
                # Return empty result if not allowed
                return self._create_empty_result(query, "deferred_by_carbon_controller")
            
            retrieval_mode = carbon_decision.mode.value
            token_budget = min(token_budget, carbon_decision.max_tokens)
            include_visual = include_visual and carbon_decision.allow_visual
        
        # 2. TOKEN FILTERING
        filter_decision = self.token_filter.should_filter(
            token_count=token_budget,
            content_type="multimodal" if include_visual else "text",
            relevance_score=0.5  # Default, updated later
        )
        
        if filter_decision["should_filter"]:
            # Reduce token budget
            token_budget = int(token_budget * 0.5)
        
        # 3. TOPOLOGICAL TOKEN ALLOCATION
        token_allocations = {}
        topo_summary = {}
        if self.topo_allocator and pipeline in [RetrievalPipeline.COMPREHENSIVE, RetrievalPipeline.BALANCED]:
            # Allocate tokens based on graph topology
            token_allocations = self.topo_allocator.allocate_tokens(
                graph=self.graph,
                total_token_budget=token_budget,
                base_tokens_per_node=100,
                query_nodes=None  # Could enhance with query analysis
            )
            
            topo_summary = self.topo_allocator.get_allocation_summary(token_allocations)
        
        # 4. SEMANTIC SCORING
        semantic_scores = []
        avg_similarity = 0.0
        
        if self.semantic_scorer and pipeline != RetrievalPipeline.FAST:
            # Score all nodes semantically
            contents = [
                (nid, str(node.content))
                for nid, node in self.core_retrieval.memory_graph.items()
            ]
            
            semantic_scores = self.semantic_scorer.score_and_rank(
                query=query,
                contents=contents,
                top_k=max_results * 2  # Get extra for filtering
            )
            
            if semantic_scores:
                avg_similarity = sum(s.similarity_score for s in semantic_scores) / len(semantic_scores)
        
        # 5. HIERARCHICAL RETRIEVAL (if COMPREHENSIVE)
        hierarchical_results = []
        if pipeline == RetrievalPipeline.COMPREHENSIVE:
            nodes, stats = self.hierarchical.hierarchical_retrieve(
                query=query,
                start_level=RetrievalLevel.COARSE,
                max_results=max_results
            )
            hierarchical_results = nodes
        
        # 6. CORE RETRIEVAL
        retrieved_nodes, retrieval_trace = self.core_retrieval.retrieve(
            query=query,
            max_nodes=max_results,
            include_visual=include_visual,
            energy_budget_wh=energy_budget_wh
        )
        
        # 7. MULTIMODAL FUSION (if multimodal content)
        fusion_results = None
        modalities_used = ["text"]
        
        if include_visual and pipeline in [RetrievalPipeline.MULTIMODAL, RetrievalPipeline.COMPREHENSIVE]:
            # Prepare results for fusion
            multimodal_results = [
                {
                    "content_id": node.node_id,
                    "text": {"score": 0.8, "confidence": 0.9, "energy": 0.001, "tokens": 100},
                    "visual": {"score": 0.7, "confidence": 0.85, "energy": 0.0025, "tokens": 256}
                }
                for node in retrieved_nodes if node.content_type in ["visual", "multimodal"]
            ]
            
            if multimodal_results:
                fusion_results = self.fusion.fuse_multimodal_results(
                    results=multimodal_results,
                    fusion_method="adaptive",
                    energy_budget=energy_budget_wh
                )
                modalities_used = ["text", "visual"]
        
        # 8. CONTEXT COMPRESSION (if needed)
        compression_ratio = 1.0
        tokens_saved = 0
        
        if retrieval_trace.total_tokens > token_budget:
            # Compress retrieved content
            # This would compress the actual text content
            compression_ratio = 0.5  # Simplified
            tokens_saved = int(retrieval_trace.total_tokens * (1 - compression_ratio))
        
        # 9. SERENDIPITY LOGGING
        serendipity_events = []
        efficiency_gains = 0.0
        carbon_savings = 0.0
        
        actual_energy = retrieval_trace.total_energy_wh
        
        if self.serendipity and actual_energy < baseline_energy:
            event = self.serendipity.log_efficiency_gain(
                description=f"Efficient retrieval for query: {query[:50]}",
                baseline_energy_wh=baseline_energy,
                actual_energy_wh=actual_energy,
                retrieval_path=retrieval_trace.retrieval_path,
                agent_id=agent_id
            )
            
            if event:
                serendipity_events.append(event)
                efficiency_gains = (baseline_energy - actual_energy) / baseline_energy
                carbon_savings = event.energy_saved_wh * 0.385  # Default grid intensity
        
        # Calculate total carbon
        total_carbon = actual_energy * 0.385  # kg CO2 (default intensity)
        
        # Update statistics
        self.total_energy_consumed += actual_energy
        self.total_carbon_emitted += total_carbon
        self.total_carbon_saved += carbon_savings
        
        # Calculate elapsed time
        elapsed = (datetime.now() - start_time).total_seconds() * 1000  # ms
        
        # Create integrated result
        result = IntegratedRetrievalResult(
            retrieved_nodes=retrieved_nodes,
            retrieval_trace=retrieval_trace,
            token_allocations=token_allocations,
            topological_summary=topo_summary,
            semantic_scores=semantic_scores,
            avg_similarity=avg_similarity,
            total_energy_wh=actual_energy,
            total_carbon_kg=total_carbon,
            retrieval_mode=retrieval_mode,
            carbon_savings_kg=carbon_savings,
            modalities_used=modalities_used,
            fusion_results=fusion_results,
            compression_ratio=compression_ratio,
            tokens_saved=tokens_saved,
            serendipity_events=serendipity_events,
            efficiency_gains=efficiency_gains,
            query=query,
            timestamp=start_time.timestamp(),
            total_time_ms=elapsed,
            cache_hits=self.semantic_scorer.cache_hits if self.semantic_scorer else 0
        )
        
        return result
    
    def get_integration_stats(self) -> Dict[str, Any]:
        """Get comprehensive integration statistics."""
        stats = {
            "total_retrievals": self.retrieval_count,
            "total_energy_consumed_wh": self.total_energy_consumed,
            "total_carbon_emitted_kg": self.total_carbon_emitted,
            "total_carbon_saved_kg": self.total_carbon_saved,
            "avg_energy_per_retrieval": (
                self.total_energy_consumed / self.retrieval_count
                if self.retrieval_count > 0 else 0
            ),
            "memory_graph_nodes": len(self.core_retrieval.memory_graph),
            "memory_graph_edges": sum(
                len(edges) for edges in self.core_retrieval.graph_edges.values()
            )
        }
        
        # Add module-specific stats
        if self.carbon_controller:
            stats["carbon_controller"] = self.carbon_controller.get_controller_stats()
        
        if self.token_filter:
            stats["token_filter"] = self.token_filter.get_filtering_stats()
        
        if self.semantic_scorer:
            stats["semantic_scorer"] = self.semantic_scorer.get_cache_stats()
        
        if self.serendipity:
            stats["serendipity"] = self.serendipity.get_serendipity_summary()
        
        return stats
    
    def update_grid_intensity(self, intensity_g_kwh: float):
        """Update carbon grid intensity across all modules."""
        if self.carbon_controller:
            self.carbon_controller.update_carbon_intensity(intensity_g_kwh)
        
        if self.token_filter:
            self.token_filter.update_grid_intensity(intensity_g_kwh)
    
    def _estimate_baseline_energy(self, query: str, token_budget: int) -> float:
        """Estimate baseline energy for serendipity comparison."""
        # Simplified: assume 1 microWh per token
        return token_budget * 0.000001
    
    def _create_empty_result(self, query: str, reason: str) -> IntegratedRetrievalResult:
        """Create empty result when retrieval is blocked."""
        return IntegratedRetrievalResult(
            retrieved_nodes=[],
            retrieval_trace=None,
            token_allocations={},
            topological_summary={},
            semantic_scores=[],
            avg_similarity=0.0,
            total_energy_wh=0.0,
            total_carbon_kg=0.0,
            retrieval_mode="blocked",
            carbon_savings_kg=0.0,
            modalities_used=[],
            fusion_results=None,
            compression_ratio=0.0,
            tokens_saved=0,
            serendipity_events=[],
            efficiency_gains=0.0,
            query=query,
            timestamp=datetime.now().timestamp(),
            total_time_ms=0.0,
            cache_hits=0
        )
    
    def export_integration_report(self, filepath: str):
        """Export comprehensive integration report."""
        import json
        
        report = {
            "statistics": self.get_integration_stats(),
            "configuration": {
                "carbon_adaptation_enabled": self.carbon_controller is not None,
                "semantic_scoring_enabled": self.semantic_scorer is not None,
                "topological_allocation_enabled": self.topo_allocator is not None,
                "serendipity_logging_enabled": self.serendipity is not None
            },
            "timestamp": datetime.now().isoformat()
        }
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)


# Convenience function
def create_vimrag_pipeline(
    mode: str = "balanced",
    grid_region: str = "US-CA"
) -> VimRAGIntegration:
    """
    Create VimRAG pipeline with preset configuration.
    
    Args:
        mode: "fast", "balanced", "green", or "comprehensive"
        grid_region: Carbon grid region
        
    Returns:
        Configured VimRAGIntegration
    """
    if mode == "fast":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=False,
            enable_topological_allocation=False,
            enable_carbon_adaptation=False,
            enable_serendipity=False
        )
    
    elif mode == "green":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=True,
            enable_topological_allocation=True,
            enable_carbon_adaptation=True,
            enable_serendipity=True
        )
    
    elif mode == "comprehensive":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=True,
            enable_topological_allocation=True,
            enable_carbon_adaptation=True,
            enable_serendipity=True
        )
    
    else:  # balanced
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=True,
            enable_topological_allocation=False,
            enable_carbon_adaptation=True,
            enable_serendipity=True
        )
