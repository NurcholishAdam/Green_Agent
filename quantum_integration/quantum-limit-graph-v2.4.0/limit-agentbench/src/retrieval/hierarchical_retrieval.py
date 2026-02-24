# -*- coding: utf-8 -*-
"""
Hierarchical Retrieval Module

Implements multi-level retrieval with coarse-to-fine filtering
inspired by VimRAG's hierarchical memory navigation.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import numpy as np


class RetrievalLevel(Enum):
    """Hierarchical retrieval levels."""
    COARSE = "coarse"  # High-level semantic clusters
    MEDIUM = "medium"  # Topic-level groups
    FINE = "fine"      # Individual nodes


@dataclass
class HierarchicalNode:
    """Node in hierarchical retrieval structure."""
    node_id: str
    level: RetrievalLevel
    content: Any
    children: List[str]
    parent: Optional[str]
    token_budget: int
    energy_budget: float
    metadata: Dict[str, Any]


class HierarchicalRetrieval:
    """
    Hierarchical retrieval system for efficient multi-level navigation.
    
    Implements coarse-to-fine retrieval strategy:
    1. Coarse: Identify relevant semantic clusters
    2. Medium: Navigate to topic groups
    3. Fine: Retrieve specific nodes
    
    Benefits:
    - Reduced token usage through early filtering
    - Lower energy consumption via hierarchical pruning
    - Better context relevance through structured navigation
    """
    
    def __init__(
        self,
        coarse_budget_tokens: int = 500,
        medium_budget_tokens: int = 1500,
        fine_budget_tokens: int = 4096
    ):
        """
        Initialize hierarchical retrieval.
        
        Args:
            coarse_budget_tokens: Token budget for coarse level
            medium_budget_tokens: Token budget for medium level
            fine_budget_tokens: Token budget for fine level
        """
        self.budgets = {
            RetrievalLevel.COARSE: coarse_budget_tokens,
            RetrievalLevel.MEDIUM: medium_budget_tokens,
            RetrievalLevel.FINE: fine_budget_tokens
        }
        
        # Hierarchical structure
        self.nodes: Dict[str, HierarchicalNode] = {}
        self.level_index: Dict[RetrievalLevel, List[str]] = {
            level: [] for level in RetrievalLevel
        }
        
        # Retrieval statistics
        self.level_access_counts = {level: 0 for level in RetrievalLevel}
        self.pruning_efficiency = []
        
    def add_hierarchical_node(
        self,
        node_id: str,
        level: RetrievalLevel,
        content: Any,
        parent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Add node to hierarchical structure."""
        node = HierarchicalNode(
            node_id=node_id,
            level=level,
            content=content,
            children=[],
            parent=parent,
            token_budget=self.budgets[level],
            energy_budget=self._estimate_energy_budget(level),
            metadata=metadata or {}
        )
        
        self.nodes[node_id] = node
        self.level_index[level].append(node_id)
        
        # Link to parent
        if parent and parent in self.nodes:
            self.nodes[parent].children.append(node_id)
    
    def hierarchical_retrieve(
        self,
        query: str,
        start_level: RetrievalLevel = RetrievalLevel.COARSE,
        max_results: int = 10
    ) -> Tuple[List[HierarchicalNode], Dict[str, Any]]:
        """
        Perform hierarchical retrieval from coarse to fine.
        
        Args:
            query: Retrieval query
            start_level: Starting retrieval level
            max_results: Maximum results to return
            
        Returns:
            Tuple of (retrieved nodes, retrieval stats)
        """
        stats = {
            "levels_traversed": [],
            "nodes_evaluated": 0,
            "nodes_pruned": 0,
            "total_tokens": 0,
            "total_energy": 0.0
        }
        
        # Start at coarse level
        current_level = start_level
        candidate_nodes = self.level_index[current_level]
        
        # Traverse hierarchy
        while current_level != RetrievalLevel.FINE:
            # Score candidates at current level
            scored = self._score_nodes(query, candidate_nodes)
            
            # Select top candidates
            top_k = min(5, len(scored))
            selected = scored[:top_k]
            
            # Update stats
            stats["levels_traversed"].append(current_level.value)
            stats["nodes_evaluated"] += len(candidate_nodes)
            stats["nodes_pruned"] += len(candidate_nodes) - len(selected)
            
            self.level_access_counts[current_level] += 1
            
            # Move to next level
            current_level = self._next_level(current_level)
            candidate_nodes = self._get_children(selected)
        
        # Final retrieval at fine level
        final_scored = self._score_nodes(query, candidate_nodes)
        final_results = final_scored[:max_results]
        
        stats["levels_traversed"].append(RetrievalLevel.FINE.value)
        stats["nodes_evaluated"] += len(candidate_nodes)
        
        # Calculate resource usage
        result_nodes = [self.nodes[nid] for nid, _ in final_results]
        stats["total_tokens"] = sum(n.token_budget for n in result_nodes)
        stats["total_energy"] = sum(n.energy_budget for n in result_nodes)
        
        # Calculate pruning efficiency
        pruning_eff = (
            stats["nodes_pruned"] / stats["nodes_evaluated"]
            if stats["nodes_evaluated"] > 0 else 0
        )
        self.pruning_efficiency.append(pruning_eff)
        stats["pruning_efficiency"] = pruning_eff
        
        return result_nodes, stats
    
    def get_hierarchical_stats(self) -> Dict[str, Any]:
        """Get statistics about hierarchical retrieval."""
        return {
            "total_nodes": len(self.nodes),
            "nodes_per_level": {
                level.value: len(nodes)
                for level, nodes in self.level_index.items()
            },
            "level_access_counts": {
                level.value: count
                for level, count in self.level_access_counts.items()
            },
            "avg_pruning_efficiency": (
                np.mean(self.pruning_efficiency)
                if self.pruning_efficiency else 0
            ),
            "token_budgets": {
                level.value: budget
                for level, budget in self.budgets.items()
            }
        }
    
    def optimize_hierarchy(self) -> Dict[str, Any]:
        """
        Optimize hierarchical structure based on access patterns.
        
        Returns:
            Optimization recommendations
        """
        stats = self.get_hierarchical_stats()
        recommendations = []
        
        # Check level balance
        level_counts = stats["nodes_per_level"]
        if level_counts["coarse"] > level_counts["medium"] * 0.5:
            recommendations.append({
                "type": "rebalance",
                "action": "increase_medium_level_nodes",
                "reason": "Coarse level too dense"
            })
        
        # Check pruning efficiency
        if stats["avg_pruning_efficiency"] < 0.5:
            recommendations.append({
                "type": "pruning",
                "action": "improve_coarse_level_discrimination",
                "reason": "Low pruning efficiency"
            })
        
        return {
            "recommendations": recommendations,
            "current_stats": stats
        }
    
    def _score_nodes(
        self,
        query: str,
        node_ids: List[str]
    ) -> List[Tuple[str, float]]:
        """Score and rank nodes by relevance."""
        scored = []
        query_words = set(query.lower().split())
        
        for nid in node_ids:
            if nid not in self.nodes:
                continue
            
            node = self.nodes[nid]
            content_words = set(str(node.content).lower().split())
            
            # Simple overlap scoring
            overlap = len(query_words & content_words)
            score = overlap / max(len(query_words), 1)
            
            scored.append((nid, score))
        
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored
    
    def _next_level(self, current: RetrievalLevel) -> RetrievalLevel:
        """Get next level in hierarchy."""
        if current == RetrievalLevel.COARSE:
            return RetrievalLevel.MEDIUM
        elif current == RetrievalLevel.MEDIUM:
            return RetrievalLevel.FINE
        return RetrievalLevel.FINE
    
    def _get_children(
        self,
        parent_nodes: List[Tuple[str, float]]
    ) -> List[str]:
        """Get all children of parent nodes."""
        children = []
        for nid, _ in parent_nodes:
            if nid in self.nodes:
                children.extend(self.nodes[nid].children)
        return children
    
    def _estimate_energy_budget(self, level: RetrievalLevel) -> float:
        """Estimate energy budget for level."""
        token_budget = self.budgets[level]
        # Simplified: 1 microWh per token
        return token_budget * 0.000001
