# -*- coding: utf-8 -*-
"""
Topological Token Allocator

Implements VimRAG's core innovation: allocating token budgets based on 
graph topology and node importance rather than uniform distribution.

Key Concept:
- High-importance nodes (central, high betweenness) get high-resolution tokens
- Low-importance nodes (peripheral, redundant) get compressed/low-resolution tokens
- Total token budget is conserved but redistributed optimally
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import numpy as np
import networkx as nx
from collections import defaultdict


class ImportanceMetric(Enum):
    """Metrics for measuring node importance."""
    PAGERANK = "pagerank"
    BETWEENNESS = "betweenness"
    DEGREE_CENTRALITY = "degree_centrality"
    EIGENVECTOR = "eigenvector"
    CLOSENESS = "closeness"
    COMBINED = "combined"


class ResolutionLevel(Enum):
    """Token resolution levels."""
    CRITICAL = "critical"    # 100% of base tokens
    HIGH = "high"           # 75% of base tokens
    MEDIUM = "medium"       # 50% of base tokens
    LOW = "low"             # 25% of base tokens
    MINIMAL = "minimal"     # 10% of base tokens


@dataclass
class TokenAllocation:
    """Token allocation for a node."""
    node_id: str
    base_tokens: int
    allocated_tokens: int
    resolution_level: ResolutionLevel
    importance_score: float
    allocation_ratio: float
    metadata: Dict[str, Any]


class TopologicalTokenAllocator:
    """
    Allocates tokens based on graph topology and node importance.
    
    Core VimRAG Innovation:
    - Nodes with high graph centrality get more tokens (high-resolution)
    - Peripheral nodes get fewer tokens (low-resolution/compressed)
    - Total token budget remains constant but redistributed
    
    Example:
        Medical diagnosis scenario:
        - Critical symptom nodes: 1000 tokens (high-res)
        - Related condition nodes: 500 tokens (medium-res)
        - General health nodes: 100 tokens (low-res)
        - Total budget: same as uniform, but better allocated
    """
    
    def __init__(
        self,
        importance_metric: ImportanceMetric = ImportanceMetric.COMBINED,
        critical_threshold: float = 0.8,
        high_threshold: float = 0.6,
        medium_threshold: float = 0.4,
        low_threshold: float = 0.2,
        enable_dynamic_adjustment: bool = True
    ):
        """
        Initialize topological token allocator.
        
        Args:
            importance_metric: Metric for measuring node importance
            critical_threshold: Threshold for critical resolution (0-1)
            high_threshold: Threshold for high resolution
            medium_threshold: Threshold for medium resolution
            low_threshold: Threshold for low resolution
            enable_dynamic_adjustment: Dynamically adjust based on query
        """
        self.importance_metric = importance_metric
        self.thresholds = {
            ResolutionLevel.CRITICAL: critical_threshold,
            ResolutionLevel.HIGH: high_threshold,
            ResolutionLevel.MEDIUM: medium_threshold,
            ResolutionLevel.LOW: low_threshold,
            ResolutionLevel.MINIMAL: 0.0
        }
        self.enable_dynamic = enable_dynamic_adjustment
        
        # Resolution multipliers
        self.resolution_multipliers = {
            ResolutionLevel.CRITICAL: 1.0,
            ResolutionLevel.HIGH: 0.75,
            ResolutionLevel.MEDIUM: 0.5,
            ResolutionLevel.LOW: 0.25,
            ResolutionLevel.MINIMAL: 0.1
        }
        
        # Statistics
        self.allocation_history: List[Dict[str, Any]] = []
        
    def allocate_tokens(
        self,
        graph: nx.Graph,
        total_token_budget: int,
        base_tokens_per_node: int = 100,
        query_nodes: Optional[List[str]] = None
    ) -> Dict[str, TokenAllocation]:
        """
        Allocate tokens across graph nodes based on topology.
        
        Args:
            graph: NetworkX graph of memory nodes
            total_token_budget: Total token budget to allocate
            base_tokens_per_node: Base token count per node
            query_nodes: Nodes relevant to current query (get boosted importance)
            
        Returns:
            Dictionary mapping node_id to TokenAllocation
        """
        if len(graph.nodes) == 0:
            return {}
        
        # 1. Calculate importance scores for all nodes
        importance_scores = self._calculate_importance_scores(graph)
        
        # 2. Boost query-relevant nodes
        if query_nodes and self.enable_dynamic:
            importance_scores = self._boost_query_nodes(
                importance_scores,
                query_nodes,
                boost_factor=1.5
            )
        
        # 3. Normalize importance scores (0-1 range)
        importance_scores = self._normalize_scores(importance_scores)
        
        # 4. Assign resolution levels based on importance
        resolution_assignments = self._assign_resolution_levels(importance_scores)
        
        # 5. Calculate token allocations
        allocations = self._calculate_allocations(
            graph,
            importance_scores,
            resolution_assignments,
            total_token_budget,
            base_tokens_per_node
        )
        
        # 6. Record allocation for analysis
        self._record_allocation(allocations, total_token_budget)
        
        return allocations
    
    def allocate_for_retrieval(
        self,
        graph: nx.Graph,
        query: str,
        query_embedding: Optional[np.ndarray],
        total_token_budget: int,
        node_embeddings: Optional[Dict[str, np.ndarray]] = None
    ) -> Dict[str, TokenAllocation]:
        """
        Allocate tokens for specific retrieval query.
        
        Args:
            graph: Memory graph
            query: Query string
            query_embedding: Query embedding vector
            total_token_budget: Total token budget
            node_embeddings: Optional embeddings for semantic boosting
            
        Returns:
            Token allocations optimized for query
        """
        # Calculate base importance
        importance_scores = self._calculate_importance_scores(graph)
        
        # Boost based on semantic similarity if embeddings available
        if query_embedding is not None and node_embeddings:
            semantic_scores = self._calculate_semantic_scores(
                query_embedding,
                node_embeddings
            )
            # Combine topological and semantic importance
            importance_scores = self._combine_scores(
                importance_scores,
                semantic_scores,
                topology_weight=0.6,
                semantic_weight=0.4
            )
        
        # Normalize and allocate
        importance_scores = self._normalize_scores(importance_scores)
        resolution_assignments = self._assign_resolution_levels(importance_scores)
        
        allocations = self._calculate_allocations(
            graph,
            importance_scores,
            resolution_assignments,
            total_token_budget,
            base_tokens_per_node=100
        )
        
        return allocations
    
    def get_allocation_summary(
        self,
        allocations: Dict[str, TokenAllocation]
    ) -> Dict[str, Any]:
        """Get summary of token allocation."""
        if not allocations:
            return {"status": "no_allocations"}
        
        total_allocated = sum(a.allocated_tokens for a in allocations.values())
        
        # Count by resolution level
        level_counts = defaultdict(int)
        level_tokens = defaultdict(int)
        for alloc in allocations.values():
            level_counts[alloc.resolution_level.value] += 1
            level_tokens[alloc.resolution_level.value] += alloc.allocated_tokens
        
        # Find top nodes by allocation
        top_nodes = sorted(
            allocations.items(),
            key=lambda x: x[1].allocated_tokens,
            reverse=True
        )[:10]
        
        return {
            "total_nodes": len(allocations),
            "total_tokens_allocated": total_allocated,
            "avg_tokens_per_node": total_allocated / len(allocations),
            "nodes_by_resolution": dict(level_counts),
            "tokens_by_resolution": dict(level_tokens),
            "top_allocated_nodes": [
                {
                    "node_id": nid,
                    "tokens": alloc.allocated_tokens,
                    "resolution": alloc.resolution_level.value,
                    "importance": alloc.importance_score
                }
                for nid, alloc in top_nodes
            ]
        }
    
    def _calculate_importance_scores(
        self,
        graph: nx.Graph
    ) -> Dict[str, float]:
        """Calculate importance scores based on graph topology."""
        if self.importance_metric == ImportanceMetric.PAGERANK:
            return nx.pagerank(graph)
        
        elif self.importance_metric == ImportanceMetric.BETWEENNESS:
            return nx.betweenness_centrality(graph)
        
        elif self.importance_metric == ImportanceMetric.DEGREE_CENTRALITY:
            return nx.degree_centrality(graph)
        
        elif self.importance_metric == ImportanceMetric.EIGENVECTOR:
            try:
                return nx.eigenvector_centrality(graph, max_iter=1000)
            except:
                # Fallback to degree centrality if eigenvector fails
                return nx.degree_centrality(graph)
        
        elif self.importance_metric == ImportanceMetric.CLOSENESS:
            return nx.closeness_centrality(graph)
        
        else:  # COMBINED
            # Combine multiple metrics for robust importance
            pagerank = nx.pagerank(graph)
            betweenness = nx.betweenness_centrality(graph)
            degree = nx.degree_centrality(graph)
            
            combined = {}
            for node in graph.nodes:
                combined[node] = (
                    0.4 * pagerank.get(node, 0) +
                    0.3 * betweenness.get(node, 0) +
                    0.3 * degree.get(node, 0)
                )
            
            return combined
    
    def _boost_query_nodes(
        self,
        importance_scores: Dict[str, float],
        query_nodes: List[str],
        boost_factor: float = 1.5
    ) -> Dict[str, float]:
        """Boost importance of query-relevant nodes."""
        boosted = importance_scores.copy()
        
        for node in query_nodes:
            if node in boosted:
                boosted[node] *= boost_factor
        
        return boosted
    
    def _normalize_scores(
        self,
        scores: Dict[str, float]
    ) -> Dict[str, float]:
        """Normalize scores to 0-1 range."""
        if not scores:
            return {}
        
        min_score = min(scores.values())
        max_score = max(scores.values())
        
        if max_score == min_score:
            return {k: 0.5 for k in scores.keys()}
        
        return {
            k: (v - min_score) / (max_score - min_score)
            for k, v in scores.items()
        }
    
    def _assign_resolution_levels(
        self,
        importance_scores: Dict[str, float]
    ) -> Dict[str, ResolutionLevel]:
        """Assign resolution levels based on importance thresholds."""
        assignments = {}
        
        for node, score in importance_scores.items():
            if score >= self.thresholds[ResolutionLevel.CRITICAL]:
                assignments[node] = ResolutionLevel.CRITICAL
            elif score >= self.thresholds[ResolutionLevel.HIGH]:
                assignments[node] = ResolutionLevel.HIGH
            elif score >= self.thresholds[ResolutionLevel.MEDIUM]:
                assignments[node] = ResolutionLevel.MEDIUM
            elif score >= self.thresholds[ResolutionLevel.LOW]:
                assignments[node] = ResolutionLevel.LOW
            else:
                assignments[node] = ResolutionLevel.MINIMAL
        
        return assignments
    
    def _calculate_allocations(
        self,
        graph: nx.Graph,
        importance_scores: Dict[str, float],
        resolution_assignments: Dict[str, ResolutionLevel],
        total_budget: int,
        base_tokens_per_node: int
    ) -> Dict[str, TokenAllocation]:
        """Calculate actual token allocations."""
        allocations = {}
        
        # Calculate total weighted tokens needed
        total_weighted = sum(
            base_tokens_per_node * self.resolution_multipliers[level]
            for level in resolution_assignments.values()
        )
        
        # Scale factor to fit budget
        scale_factor = total_budget / total_weighted if total_weighted > 0 else 1.0
        
        # Allocate tokens
        for node in graph.nodes:
            level = resolution_assignments.get(node, ResolutionLevel.MINIMAL)
            multiplier = self.resolution_multipliers[level]
            
            allocated = int(base_tokens_per_node * multiplier * scale_factor)
            
            allocations[node] = TokenAllocation(
                node_id=node,
                base_tokens=base_tokens_per_node,
                allocated_tokens=allocated,
                resolution_level=level,
                importance_score=importance_scores.get(node, 0.0),
                allocation_ratio=allocated / base_tokens_per_node if base_tokens_per_node > 0 else 0,
                metadata={
                    "multiplier": multiplier,
                    "scale_factor": scale_factor
                }
            )
        
        return allocations
    
    def _calculate_semantic_scores(
        self,
        query_embedding: np.ndarray,
        node_embeddings: Dict[str, np.ndarray]
    ) -> Dict[str, float]:
        """Calculate semantic similarity scores."""
        scores = {}
        
        for node_id, node_embedding in node_embeddings.items():
            # Cosine similarity
            similarity = np.dot(query_embedding, node_embedding) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(node_embedding)
            )
            scores[node_id] = float(similarity)
        
        return scores
    
    def _combine_scores(
        self,
        topological_scores: Dict[str, float],
        semantic_scores: Dict[str, float],
        topology_weight: float,
        semantic_weight: float
    ) -> Dict[str, float]:
        """Combine topological and semantic scores."""
        combined = {}
        
        all_nodes = set(topological_scores.keys()) | set(semantic_scores.keys())
        
        for node in all_nodes:
            topo_score = topological_scores.get(node, 0.0)
            sem_score = semantic_scores.get(node, 0.0)
            
            combined[node] = (
                topology_weight * topo_score +
                semantic_weight * sem_score
            )
        
        return combined
    
    def _record_allocation(
        self,
        allocations: Dict[str, TokenAllocation],
        total_budget: int
    ):
        """Record allocation for analysis."""
        summary = self.get_allocation_summary(allocations)
        summary["total_budget"] = total_budget
        summary["timestamp"] = __import__("time").time()
        
        self.allocation_history.append(summary)
    
    def analyze_allocation_efficiency(self) -> Dict[str, Any]:
        """Analyze allocation efficiency over time."""
        if not self.allocation_history:
            return {"status": "no_history"}
        
        # Calculate average distribution
        avg_by_resolution = defaultdict(list)
        for record in self.allocation_history:
            for level, count in record["nodes_by_resolution"].items():
                avg_by_resolution[level].append(count)
        
        avg_distribution = {
            level: np.mean(counts)
            for level, counts in avg_by_resolution.items()
        }
        
        return {
            "total_allocations": len(self.allocation_history),
            "avg_nodes_by_resolution": avg_distribution,
            "latest_allocation": self.allocation_history[-1] if self.allocation_history else None
        }
