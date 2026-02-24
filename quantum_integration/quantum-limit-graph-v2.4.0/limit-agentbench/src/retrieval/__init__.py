# -*- coding: utf-8 -*-
"""
VimRAG-Inspired Retrieval Module

Implements energy-efficient multimodal retrieval with carbon-adaptive controls.
"""

from .vimrag_coretrieval import VimRAGCoRetrieval, RetrievalNode, RetrievalTrace
from .token_aware_filter import TokenAwareFilter
from .serendipity_logger import SerendipityTraceLogger
from .carbon_adaptive_controller import CarbonAdaptiveRetrievalController
from .vimrag_integration import VimRAGIntegration, IntegratedRetrievalResult
from .hierarchical_retrieval import HierarchicalRetrieval, RetrievalLevel, HierarchicalNode
from .multimodal_fusion import MultimodalFusion, ModalityType, FusedResult
from .graph_traversal_optimizer import (
    GraphTraversalOptimizer,
    TraversalStrategy,
    TraversalPath,
    GraphEdge
)
from .context_compression import (
    ContextCompressor,
    CompressionResult,
    SemanticChunker
)
from .visual_context_handler import (
    VisualContextHandler,
    VisualContext,
    VisualContextType,
    VisualRetrievalResult
)

__all__ = [
    # Core retrieval
    "VimRAGCoRetrieval",
    "RetrievalNode",
    "RetrievalTrace",
    
    # Filtering and control
    "TokenAwareFilter",
    "SerendipityTraceLogger",
    "CarbonAdaptiveRetrievalController",
    
    # Integration
    "VimRAGIntegration",
    "IntegratedRetrievalResult",
    
    # Hierarchical retrieval
    "HierarchicalRetrieval",
    "RetrievalLevel",
    "HierarchicalNode",
    
    # Multimodal fusion
    "MultimodalFusion",
    "ModalityType",
    "FusedResult",
    
    # Graph traversal
    "GraphTraversalOptimizer",
    "TraversalStrategy",
    "TraversalPath",
    "GraphEdge",
    
    # Context compression
    "ContextCompressor",
    "CompressionResult",
    "SemanticChunker",
    
    # Visual context
    "VisualContextHandler",
    "VisualContext",
    "VisualContextType",
    "VisualRetrievalResult"
]
