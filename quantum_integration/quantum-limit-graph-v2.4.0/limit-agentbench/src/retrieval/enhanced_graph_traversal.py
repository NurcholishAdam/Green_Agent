# -*- coding: utf-8 -*-
"""
Enhanced Graph Traversal Optimizer with NetworkX

Replaces simple adjacency lists with full NetworkX graph capabilities.
Includes hooks for Quantum-LIMIT-Graph integration.
"""

from typing import Dict, List, Any, Optional, Tuple, Set, Callable
from dataclasses import dataclass
from enum import Enum
import networkx as nx
import numpy as np
from collections import defaultdict
import heapq


class TraversalStrategy(Enum):
    """Graph traversal strategies."""
    BREADTH_FIRST = "bfs"
    DEPTH_FIRST = "dfs"
    ENERGY_OPTIMAL = "energy_optimal"
    RELEVANCE_GUIDED = "relevance_guided"
    COMMUNITY_AWARE = "community_aware"
    QUANTUM_ENHANCED = "quantum_enhanced"  # For Quantum-LIMIT-Graph


@dataclass
class TraversalPath:
    """Represents a traversal path through the graph."""
    path_id: str
    nodes: List[str]
    total_cost: float
    energy_cost: float
    relevance_score: float
    community_ids: List[int]  # Communities traversed
    metadata: Dict[str, Any]


@dataclass
class GraphStatistics:
    """Graph structure statistics."""
    num_nodes: int
    num_edges: int
    avg_degree: float
    density: float
    num_communities: int
    clustering_coefficient: float
    diameter: Optional[int]
    is_connected: bool


class EnhancedGraphTraversalOptimizer:
    """
    Enhanced graph traversal with full NetworkX capabilities.
    
    New Features:
    - Real graph algorithms (shortest path, communities, centrality)
    - Community detection for cluster-aware retrieval
    - Graph metrics for optimization
    - Quantum-LIMIT-Graph integration hooks
    """
    
    def __init__(
        self,
        graph_type: str = "directed",
        energy_weight: float = 0.5,
        relevance_weight: float = 0.5,
        enable_communities: bool = True,
        cache_size: int = 100
    ):
        """
        Initialize enhanced graph traversal optimizer.
        
        Args:
            graph_type: "directed" or "undirected"
            energy_weight: Weight for energy cost
            relevance_weight: Weight for relevance
            enable_communities: Enable community detection
            cache_size: Path cache size
        """
        # Create NetworkX graph
        if graph_type == "directed":
            self.graph = nx.DiGraph()
        else:
            self.graph = nx.Graph()
        
        self.energy_weight = energy_weight
        self.relevance_weight = relevance_weight
        self.enable_communities = enable_communities
        self.cache_size = cache_size
        
        # Node attributes
        self.node_costs: Dict[str, float] = {}
        self.node_relevance: Dict[str, float] = {}
        self.node_embeddings: Dict[str, np.ndarray] = {}
        
        # Communities
        self.communities: List[Set[str]] = []
        self.node_to_community: Dict[str, int] = {}
        
        # Path cache
        self.path_cache: Dict[str, TraversalPath] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Quantum integration
        self.quantum_backend: Optional[Any] = None
        
    def add_node(
        self,
        node_id: str,
        cost: float = 0.1,
        relevance: float = 0.5,
        embedding: Optional[np.ndarray] = None,
        **attributes
    ):
        """Add node to graph with attributes."""
        self.graph.add_node(node_id, **attributes)
        self.node_costs[node_id] = cost
        self.node_relevance[node_id] = relevance
        if embedding is not None:
            self.node_embeddings[node_id] = embedding
    
    def add_edge(
        self,
        source: str,
        target: str,
        weight: float = 1.0,
        edge_type: str = "semantic",
        **attributes
    ):
        """Add edge to graph."""
        self.graph.add_edge(source, target, weight=weight, edge_type=edge_type, **attributes)
    
    def detect_communities(self, algorithm: str = "louvain") -> List[Set[str]]:
        """
        Detect communities in graph.
        
        Args:
            algorithm: Community detection algorithm
                - louvain: Fast, hierarchical
                - label_propagation: Very fast, semi-supervised
                - greedy_modularity: Greedy optimization
                
        Returns:
            List of communities (sets of node IDs)
        """
        if algorithm == "louvain":
            # Convert to undirected for Louvain
            G_undirected = self.graph.to_undirected()
            import networkx.algorithms.community as nx_comm
            communities = nx_comm.louvain_communities(G_undirected)
        
        elif algorithm == "label_propagation":
            G_undirected = self.graph.to_undirected()
            communities = list(nx.algorithms.community.label_propagation_communities(G_undirected))
        
        elif algorithm == "greedy_modularity":
            G_undirected = self.graph.to_undirected()
            communities = list(nx.algorithms.community.greedy_modularity_communities(G_undirected))
        
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")
        
        # Store communities
        self.communities = [set(comm) for comm in communities]
        
        # Build node-to-community mapping
        self.node_to_community = {}
        for i, comm in enumerate(self.communities):
            for node in comm:
                self.node_to_community[node] = i
        
        return self.communities
    
    def find_optimal_path(
        self,
        start: str,
        goal: str,
        strategy: TraversalStrategy = TraversalStrategy.ENERGY_OPTIMAL,
        max_depth: int = 10,
        energy_budget: Optional[float] = None
    ) -> Optional[TraversalPath]:
        """
        Find optimal path using NetworkX algorithms.
        
        Args:
            start: Starting node
            goal: Goal node
            strategy: Traversal strategy
            max_depth: Maximum path depth
            energy_budget: Optional energy budget constraint
            
        Returns:
            Optimal path or None
        """
        # Check cache
        cache_key = f"{start}:{goal}:{strategy.value}"
        if cache_key in self.path_cache:
            self.cache_hits += 1
            return self.path_cache[cache_key]
        
        self.cache_misses += 1
        
        # Find path based on strategy
        if strategy == TraversalStrategy.ENERGY_OPTIMAL:
            path = self._energy_optimal_path_nx(start, goal, energy_budget)
        
        elif strategy == TraversalStrategy.RELEVANCE_GUIDED:
            path = self._relevance_guided_path_nx(start, goal)
        
        elif strategy == TraversalStrategy.COMMUNITY_AWARE:
            path = self._community_aware_path(start, goal)
        
        elif strategy == TraversalStrategy.QUANTUM_ENHANCED:
            path = self._quantum_enhanced_path(start, goal)
        
        elif strategy == TraversalStrategy.BREADTH_FIRST:
            path = self._bfs_path_nx(start, goal)
        
        elif strategy == TraversalStrategy.DEPTH_FIRST:
            path = self._dfs_path_nx(start, goal)
        
        else:
            path = None
        
        # Cache result
        if path and len(self.path_cache) < self.cache_size:
            self.path_cache[cache_key] = path
        
        return path
    
    def find_k_shortest_paths(
        self,
        start: str,
        goal: str,
        k: int = 3,
        weight: str = "weight"
    ) -> List[TraversalPath]:
        """
        Find k shortest paths using NetworkX.
        
        Uses Yen's algorithm for k shortest paths.
        """
        try:
            # NetworkX has k_shortest_paths
            paths_generator = nx.shortest_simple_paths(
                self.graph,
                start,
                goal,
                weight=weight
            )
            
            paths = []
            for i, path_nodes in enumerate(paths_generator):
                if i >= k:
                    break
                
                path = self._create_path_from_nodes(path_nodes)
                paths.append(path)
            
            return paths
        
        except nx.NetworkXNoPath:
            return []
    
    def get_subgraph_around_nodes(
        self,
        center_nodes: List[str],
        radius: int = 2
    ) -> nx.Graph:
        """
        Extract subgraph around center nodes.
        
        Args:
            center_nodes: Center nodes
            radius: Radius in hops
            
        Returns:
            Subgraph containing nodes within radius
        """
        # Get all nodes within radius
        subgraph_nodes = set(center_nodes)
        
        for center in center_nodes:
            # Use ego_graph to get neighborhood
            ego = nx.ego_graph(self.graph, center, radius=radius)
            subgraph_nodes.update(ego.nodes())
        
        return self.graph.subgraph(subgraph_nodes)
    
    def get_graph_statistics(self) -> GraphStatistics:
        """Get comprehensive graph statistics."""
        num_nodes = self.graph.number_of_nodes()
        num_edges = self.graph.number_of_edges()
        
        if num_nodes == 0:
            return GraphStatistics(
                num_nodes=0,
                num_edges=0,
                avg_degree=0,
                density=0,
                num_communities=0,
                clustering_coefficient=0,
                diameter=None,
                is_connected=False
            )
        
        # Calculate statistics
        avg_degree = sum(dict(self.graph.degree()).values()) / num_nodes
        density = nx.density(self.graph)
        
        # Communities
        if not self.communities:
            self.detect_communities()
        num_communities = len(self.communities)
        
        # Clustering coefficient
        if isinstance(self.graph, nx.DiGraph):
            clustering = 0.0  # Not defined for directed graphs
        else:
            clustering = nx.average_clustering(self.graph)
        
        # Diameter (only for connected graphs)
        is_connected = nx.is_strongly_connected(self.graph) if isinstance(self.graph, nx.DiGraph) else nx.is_connected(self.graph)
        
        diameter = None
        if is_connected:
            try:
                diameter = nx.diameter(self.graph)
            except:
                diameter = None
        
        return GraphStatistics(
            num_nodes=num_nodes,
            num_edges=num_edges,
            avg_degree=avg_degree,
            density=density,
            num_communities=num_communities,
            clustering_coefficient=clustering,
            diameter=diameter,
            is_connected=is_connected
        )
    
    def optimize_graph_structure(self) -> Dict[str, Any]:
        """
        Analyze and optimize graph structure.
        
        Returns recommendations for graph improvements.
        """
        stats = self.get_graph_statistics()
        recommendations = []
        
        # Check connectivity
        if not stats.is_connected:
            recommendations.append({
                "issue": "disconnected_graph",
                "recommendation": "Add edges to connect isolated components",
                "severity": "high"
            })
        
        # Check density
        if stats.density < 0.01:
            recommendations.append({
                "issue": "sparse_graph",
                "recommendation": "Consider adding more semantic edges",
                "severity": "medium"
            })
        
        # Check communities
        if stats.num_communities == 0:
            recommendations.append({
                "issue": "no_communities",
                "recommendation": "Run community detection for cluster-aware retrieval",
                "severity": "low"
            })
        
        return {
            "statistics": stats.__dict__,
            "recommendations": recommendations
        }
    
    def integrate_quantum_backend(self, quantum_backend: Any):
        """
        Integrate Quantum-LIMIT-Graph backend.
        
        Args:
            quantum_backend: Quantum graph backend instance
        """
        self.quantum_backend = quantum_backend
    
    def _energy_optimal_path_nx(
        self,
        start: str,
        goal: str,
        energy_budget: Optional[float]
    ) -> Optional[TraversalPath]:
        """Energy-optimal path using Dijkstra."""
        # Create weight function based on energy cost
        def energy_weight(u, v, attrs):
            edge_weight = attrs.get("weight", 1.0)
            node_cost = self.node_costs.get(v, 0.1)
            return edge_weight + node_cost
        
        try:
            path_nodes = nx.shortest_path(
                self.graph,
                start,
                goal,
                weight=energy_weight
            )
            
            path = self._create_path_from_nodes(path_nodes)
            
            # Check energy budget
            if energy_budget and path.energy_cost > energy_budget:
                return None
            
            return path
        
        except nx.NetworkXNoPath:
            return None
    
    def _relevance_guided_path_nx(
        self,
        start: str,
        goal: str
    ) -> Optional[TraversalPath]:
        """Relevance-guided path using A* with relevance heuristic."""
        def relevance_heuristic(node):
            # Higher relevance = lower cost
            relevance = self.node_relevance.get(node, 0.5)
            return 1.0 - relevance
        
        try:
            path_nodes = nx.astar_path(
                self.graph,
                start,
                goal,
                heuristic=relevance_heuristic,
                weight="weight"
            )
            
            return self._create_path_from_nodes(path_nodes)
        
        except nx.NetworkXNoPath:
            return None
    
    def _community_aware_path(
        self,
        start: str,
        goal: str
    ) -> Optional[TraversalPath]:
        """Find path that minimizes community crossings."""
        if not self.communities:
            self.detect_communities()
        
        start_comm = self.node_to_community.get(start, -1)
        goal_comm = self.node_to_community.get(goal, -1)
        
        # Weight function that penalizes community crossings
        def community_weight(u, v, attrs):
            base_weight = attrs.get("weight", 1.0)
            
            u_comm = self.node_to_community.get(u, -1)
            v_comm = self.node_to_community.get(v, -1)
            
            # Penalty for crossing communities
            if u_comm != v_comm and u_comm != -1 and v_comm != -1:
                penalty = 2.0
            else:
                penalty = 1.0
            
            return base_weight * penalty
        
        try:
            path_nodes = nx.shortest_path(
                self.graph,
                start,
                goal,
                weight=community_weight
            )
            
            return self._create_path_from_nodes(path_nodes)
        
        except nx.NetworkXNoPath:
            return None
    
    def _quantum_enhanced_path(
        self,
        start: str,
        goal: str
    ) -> Optional[TraversalPath]:
        """
        Quantum-enhanced pathfinding.
        
        If quantum backend available, use quantum walk for exploration.
        Otherwise, fall back to classical algorithm.
        """
        if self.quantum_backend is None:
            # Fall back to energy optimal
            return self._energy_optimal_path_nx(start, goal, None)
        
        # TODO: Integrate with Quantum-LIMIT-Graph
        # This would use quantum walks for graph exploration
        # For now, placeholder
        
        try:
            # Simulate quantum-enhanced exploration
            # In real implementation, this would call quantum backend
            path_nodes = nx.shortest_path(self.graph, start, goal)
            return self._create_path_from_nodes(path_nodes)
        
        except nx.NetworkXNoPath:
            return None
    
    def _bfs_path_nx(self, start: str, goal: str) -> Optional[TraversalPath]:
        """BFS path using NetworkX."""
        try:
            path_nodes = nx.shortest_path(self.graph, start, goal)
            return self._create_path_from_nodes(path_nodes)
        except nx.NetworkXNoPath:
            return None
    
    def _dfs_path_nx(self, start: str, goal: str) -> Optional[TraversalPath]:
        """DFS path using NetworkX."""
        try:
            # DFS doesn't guarantee shortest path, but we'll use simple path
            path_nodes = next(nx.all_simple_paths(self.graph, start, goal))
            return self._create_path_from_nodes(path_nodes)
        except (nx.NetworkXNoPath, StopIteration):
            return None
    
    def _create_path_from_nodes(self, nodes: List[str]) -> TraversalPath:
        """Create TraversalPath from node list."""
        # Calculate costs
        energy_cost = sum(self.node_costs.get(n, 0.1) for n in nodes)
        relevance_score = sum(self.node_relevance.get(n, 0.5) for n in nodes) / len(nodes)
        
        # Get edge costs
        edge_costs = 0.0
        for i in range(len(nodes) - 1):
            if self.graph.has_edge(nodes[i], nodes[i+1]):
                edge_costs += self.graph[nodes[i]][nodes[i+1]].get("weight", 1.0)
        
        total_cost = energy_cost + edge_costs
        
        # Get communities traversed
        community_ids = [
            self.node_to_community.get(n, -1)
            for n in nodes
        ]
        
        return TraversalPath(
            path_id=f"path_{hash(tuple(nodes))}",
            nodes=nodes,
            total_cost=total_cost,
            energy_cost=energy_cost,
            relevance_score=relevance_score,
            community_ids=community_ids,
            metadata={
                "length": len(nodes),
                "unique_communities": len(set(c for c in community_ids if c != -1))
            }
        )
    
    def export_to_graphml(self, filepath: str):
        """Export graph to GraphML format."""
        nx.write_graphml(self.graph, filepath)
    
    def import_from_graphml(self, filepath: str):
        """Import graph from GraphML format."""
        self.graph = nx.read_graphml(filepath)
