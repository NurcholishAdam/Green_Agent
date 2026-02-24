# -*- coding: utf-8 -*-
"""
Graph Traversal Optimizer

Optimizes retrieval paths through memory graphs using energy-aware
graph traversal algorithms inspired by VimRAG's navigation strategies.
"""

from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass
from enum import Enum
import heapq
from collections import defaultdict


class TraversalStrategy(Enum):
    """Graph traversal strategies."""
    BREADTH_FIRST = "bfs"
    DEPTH_FIRST = "dfs"
    ENERGY_OPTIMAL = "energy_optimal"
    RELEVANCE_GUIDED = "relevance_guided"


@dataclass
class TraversalPath:
    """Represents a traversal path through the graph."""
    path_id: str
    nodes: List[str]
    total_cost: float
    energy_cost: float
    relevance_score: float
    metadata: Dict[str, Any]


@dataclass
class GraphEdge:
    """Edge in the memory graph."""
    source: str
    target: str
    weight: float
    edge_type: str
    metadata: Dict[str, Any]


class GraphTraversalOptimizer:
    """
    Optimizes graph traversal for energy-efficient retrieval.
    
    Features:
    - Multiple traversal strategies (BFS, DFS, energy-optimal)
    - Path caching for repeated queries
    - Energy-aware path selection
    - Relevance-guided navigation
    """
    
    def __init__(
        self,
        energy_weight: float = 0.5,
        relevance_weight: float = 0.5,
        cache_size: int = 100
    ):
        """
        Initialize graph traversal optimizer.
        
        Args:
            energy_weight: Weight for energy cost in path selection
            relevance_weight: Weight for relevance in path selection
            cache_size: Maximum cached paths
        """
        self.energy_weight = energy_weight
        self.relevance_weight = relevance_weight
        self.cache_size = cache_size
        
        # Graph structure
        self.adjacency: Dict[str, List[GraphEdge]] = defaultdict(list)
        self.node_costs: Dict[str, float] = {}
        self.node_relevance: Dict[str, float] = {}
        
        # Path cache
        self.path_cache: Dict[str, TraversalPath] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        edge_type: str = "semantic",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Add edge to graph."""
        edge = GraphEdge(
            source=source,
            target=target,
            weight=weight,
            edge_type=edge_type,
            metadata=metadata or {}
        )
        self.adjacency[source].append(edge)
    
    def set_node_cost(self, node_id: str, cost: float):
        """Set energy cost for node."""
        self.node_costs[node_id] = cost
    
    def set_node_relevance(self, node_id: str, relevance: float):
        """Set relevance score for node."""
        self.node_relevance[node_id] = relevance
    
    def find_optimal_path(
        self,
        start: str,
        goal: str,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL,
        max_depth: int = 10
    ) -> Optional[TraversalPath]:
        """
        Find optimal path from start to goal node.
        
        Args:
            start: Starting node ID
            goal: Goal node ID
            strategy: Traversal strategy to use
            max_depth: Maximum path depth
            
        Returns:
            Optimal traversal path or None
        """
        # Check cache
        cache_key = f"{start}:{goal}:{strategy.value}"
        if cache_key in self.path_cache:
            self.cache_hits += 1
            return self.path_cache[cache_key]
        
        self.cache_misses += 1
        
        # Find path based on strategy
        if strategy == TraversalStrategy.BREADTH_FIRST:
            path = self._bfs_path(start, goal, max_depth)
        elif strategy == TraversalStrategy.DEPTH_FIRST:
            path = self._dfs_path(start, goal, max_depth)
        elif strategy == TraversalStrategy.ENERGY_OPTIMAL:
            path = self._energy_optimal_path(start, goal, max_depth)
        elif strategy == TraversalStrategy.RELEVANCE_GUIDED:
            path = self._relevance_guided_path(start, goal, max_depth)
        else:
            path = None
        
        # Cache result
        if path and len(self.path_cache) < self.cache_size:
            self.path_cache[cache_key] = path
        
        return path
    
    def find_k_best_paths(
        self,
        start: str,
        goal: str,
        k: int = 3,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL
    ) -> List[TraversalPath]:
        """
        Find k best paths from start to goal.
        
        Args:
            start: Starting node ID
            goal: Goal node ID
            k: Number of paths to find
            strategy: Traversal strategy
            
        Returns:
            List of k best paths
        """
        if strategy == TraversalStrategy.ENERGY_OPTIMAL:
            return self._k_shortest_paths_energy(start, goal, k)
        else:
            # Fallback: find single path and return as list
            path = self.find_optimal_path(start, goal, strategy)
            return [path] if path else []
    
    def explore_neighborhood(
        self,
        center: str,
        radius: int = 2,
        max_nodes: int = 20,
        energy_budget: float = 1.0
    ) -> List[str]:
        """
        Explore neighborhood around center node within energy budget.
        
        Args:
            center: Center node ID
            radius: Maximum distance from center
            max_nodes: Maximum nodes to explore
            energy_budget: Energy budget for exploration
            
        Returns:
            List of explored node IDs
        """
        explored = []
        visited = {center}
        queue = [(center, 0, 0.0)]  # (node, depth, cumulative_cost)
        
        while queue and len(explored) < max_nodes:
            node, depth, cost = queue.pop(0)
            
            if depth > radius or cost > energy_budget:
                continue
            
            explored.append(node)
            
            # Add neighbors
            for edge in self.adjacency.get(node, []):
                if edge.target not in visited:
                    visited.add(edge.target)
                    node_cost = self.node_costs.get(edge.target, 0.1)
                    new_cost = cost + node_cost
                    queue.append((edge.target, depth + 1, new_cost))
        
        return explored
    
    def get_traversal_stats(self) -> Dict[str, Any]:
        """Get traversal statistics."""
        total_requests = self.cache_hits + self.cache_misses
        cache_hit_rate = (
            self.cache_hits / total_requests
            if total_requests > 0 else 0
        )
        
        return {
            "total_nodes": len(self.node_costs),
            "total_edges": sum(len(edges) for edges in self.adjacency.values()),
            "cached_paths": len(self.path_cache),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": cache_hit_rate,
            "avg_node_cost": (
                sum(self.node_costs.values()) / len(self.node_costs)
                if self.node_costs else 0
            )
        }
    
    def _bfs_path(
        self,
        start: str,
        goal: str,
        max_depth: int
    ) -> Optional[TraversalPath]:
        """Breadth-first search path."""
        queue = [(start, [start], 0)]
        visited = {start}
        
        while queue:
            node, path, depth = queue.pop(0)
            
            if node == goal:
                return self._create_path(path)
            
            if depth >= max_depth:
                continue
            
            for edge in self.adjacency.get(node, []):
                if edge.target not in visited:
                    visited.add(edge.target)
                    queue.append((edge.target, path + [edge.target], depth + 1))
        
        return None
    
    def _dfs_path(
        self,
        start: str,
        goal: str,
        max_depth: int
    ) -> Optional[TraversalPath]:
        """Depth-first search path."""
        stack = [(start, [start], 0)]
        visited = {start}
        
        while stack:
            node, path, depth = stack.pop()
            
            if node == goal:
                return self._create_path(path)
            
            if depth >= max_depth:
                continue
            
            for edge in self.adjacency.get(node, []):
                if edge.target not in visited:
                    visited.add(edge.target)
                    stack.append((edge.target, path + [edge.target], depth + 1))
        
        return None
    
    def _energy_optimal_path(
        self,
        start: str,
        goal: str,
        max_depth: int
    ) -> Optional[TraversalPath]:
        """Find energy-optimal path using A* with energy cost."""
        # Priority queue: (cost, node, path)
        pq = [(0, start, [start])]
        visited = set()
        
        while pq:
            cost, node, path = heapq.heappop(pq)
            
            if node == goal:
                return self._create_path(path)
            
            if node in visited or len(path) > max_depth:
                continue
            
            visited.add(node)
            
            for edge in self.adjacency.get(node, []):
                if edge.target not in visited:
                    node_cost = self.node_costs.get(edge.target, 0.1)
                    new_cost = cost + node_cost + edge.weight
                    new_path = path + [edge.target]
                    heapq.heappush(pq, (new_cost, edge.target, new_path))
        
        return None
    
    def _relevance_guided_path(
        self,
        start: str,
        goal: str,
        max_depth: int
    ) -> Optional[TraversalPath]:
        """Find path guided by relevance scores."""
        # Priority queue: (-relevance, node, path)
        start_relevance = self.node_relevance.get(start, 0.5)
        pq = [(-start_relevance, start, [start])]
        visited = set()
        
        while pq:
            neg_rel, node, path = heapq.heappop(pq)
            
            if node == goal:
                return self._create_path(path)
            
            if node in visited or len(path) > max_depth:
                continue
            
            visited.add(node)
            
            for edge in self.adjacency.get(node, []):
                if edge.target not in visited:
                    relevance = self.node_relevance.get(edge.target, 0.5)
                    new_path = path + [edge.target]
                    heapq.heappush(pq, (-relevance, edge.target, new_path))
        
        return None
    
    def _k_shortest_paths_energy(
        self,
        start: str,
        goal: str,
        k: int
    ) -> List[TraversalPath]:
        """Find k shortest paths by energy cost."""
        # Use Yen's algorithm variant
        paths = []
        
        # Find first shortest path
        first_path = self._energy_optimal_path(start, goal, max_depth=10)
        if not first_path:
            return []
        
        paths.append(first_path)
        
        # Find k-1 additional paths (simplified)
        # In production, implement full Yen's algorithm
        for _ in range(k - 1):
            # Try alternative paths by temporarily removing edges
            # This is a simplified version
            break
        
        return paths
    
    def _create_path(self, nodes: List[str]) -> TraversalPath:
        """Create TraversalPath from node list."""
        total_cost = sum(self.node_costs.get(n, 0.1) for n in nodes)
        energy_cost = total_cost  # Simplified
        relevance_score = sum(self.node_relevance.get(n, 0.5) for n in nodes) / len(nodes)
        
        return TraversalPath(
            path_id=f"path_{hash(tuple(nodes))}",
            nodes=nodes,
            total_cost=total_cost,
            energy_cost=energy_cost,
            relevance_score=relevance_score,
            metadata={"length": len(nodes)}
        )
