"""
Extended Pareto Analysis with Memory, Quantum Circuit Depth, and Inference Variance

Adds three critical dimensions to Green_Agent's multi-objective optimization:

1. Memory Footprint (MB) - Hard constraint for edge deployment
2. Quantum Circuit Depth - Structural cost/fragility measure
3. Inference Variance (σ) - Stability/predictability measure

Each dimension captures different failure modes not visible in basic 4D analysis.
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
import numpy as np
import logging

logger = logging.getLogger(__name__)


@dataclass
class ExtendedParetoPoint:
    """
    Extended Pareto point with 7 dimensions
    
    Core 4 dimensions:
    - accuracy: Task success rate [0, 1]
    - energy_kwh: Energy consumption (kWh)
    - carbon_co2e_kg: Carbon emissions (kg CO₂e)
    - latency_ms: Task latency (milliseconds)
    
    Extended 3 dimensions:
    - memory_mb: Peak memory footprint (megabytes)
    - circuit_depth: Quantum circuit depth (for hybrid/quantum agents)
    - variance_score: Inference variance metric [0, 1] (lower = more stable)
    
    Why these dimensions matter:
    - Memory: Hard constraint on edge devices, power-hungry
    - Circuit Depth: Predicts quantum noise, decoherence, scalability
    - Variance: Stability under repeated execution (prevents SLA violations)
    """
    agent_id: str
    
    # Core dimensions (from original ParetoPoint)
    accuracy: float
    energy_kwh: float
    carbon_co2e_kg: float
    latency_ms: float
    
    # Extended dimensions
    memory_mb: float           # Peak memory usage
    circuit_depth: int = 0     # Quantum circuit depth (0 for classical)
    variance_score: float = 0.0  # Stability metric (lower = better)
    
    # Metadata
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate all dimensions"""
        if not 0 <= self.accuracy <= 1:
            logger.warning(f"Accuracy {self.accuracy} outside [0, 1] for {self.agent_id}")
        
        # Validate non-negative metrics
        for metric, value in [
            ('energy_kwh', self.energy_kwh),
            ('carbon_co2e_kg', self.carbon_co2e_kg),
            ('latency_ms', self.latency_ms),
            ('memory_mb', self.memory_mb),
            ('circuit_depth', self.circuit_depth),
            ('variance_score', self.variance_score)
        ]:
            if value < 0:
                raise ValueError(f"{metric} cannot be negative: {value}")
    
    def dominates(self, 
                  other: 'ExtendedParetoPoint',
                  dimensions: Optional[List[str]] = None) -> bool:
        """
        Check if this point Pareto-dominates another in extended space
        
        Domination rules:
        - Maximize: accuracy
        - Minimize: energy, carbon, latency, memory, circuit_depth, variance
        
        Args:
            other: Another ExtendedParetoPoint
            dimensions: List of dimensions to consider (default: all 7)
        
        Returns:
            True if this point dominates other
        
        Example:
            Agent A: 95% acc, 3 kWh, 512 MB, depth=10, var=0.1
            Agent B: 90% acc, 5 kWh, 1024 MB, depth=20, var=0.3
            => A dominates B (better on all dimensions)
        """
        if dimensions is None:
            dimensions = [
                'accuracy', 'energy_kwh', 'carbon_co2e_kg', 'latency_ms',
                'memory_mb', 'circuit_depth', 'variance_score'
            ]
        
        better_on_at_least_one = False
        
        for dim in dimensions:
            self_val = getattr(self, dim)
            other_val = getattr(other, dim)
            
            # Maximization objective (accuracy only)
            if dim == 'accuracy':
                if self_val < other_val:  # Worse
                    return False
                if self_val > other_val:  # Better
                    better_on_at_least_one = True
            
            # Minimization objectives (all others)
            else:
                if self_val > other_val:  # Worse
                    return False
                if self_val < other_val:  # Better
                    better_on_at_least_one = True
        
        return better_on_at_least_one
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary"""
        return {
            'agent_id': self.agent_id,
            'accuracy': self.accuracy,
            'energy_kwh': self.energy_kwh,
            'carbon_co2e_kg': self.carbon_co2e_kg,
            'latency_ms': self.latency_ms,
            'memory_mb': self.memory_mb,
            'circuit_depth': self.circuit_depth,
            'variance_score': self.variance_score,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ExtendedParetoPoint':
        """Deserialize from dictionary"""
        return cls(
            agent_id=data['agent_id'],
            accuracy=data['accuracy'],
            energy_kwh=data['energy_kwh'],
            carbon_co2e_kg=data['carbon_co2e_kg'],
            latency_ms=data['latency_ms'],
            memory_mb=data['memory_mb'],
            circuit_depth=data.get('circuit_depth', 0),
            variance_score=data.get('variance_score', 0.0),
            metadata=data.get('metadata', {})
        )


class ExtendedParetoAnalyzer:
    """
    Extended Pareto frontier analysis with 7 dimensions
    
    Handles multi-objective optimization across:
    1. Accuracy (↑)
    2. Energy (↓)
    3. Carbon (↓)
    4. Latency (↓)
    5. Memory (↓)
    6. Circuit Depth (↓)
    7. Variance (↓)
    
    Why 7D matters:
    - Humans can't reason in 7D, but policies are often 2D
    - Each dimension captures different failure modes
    - Multiple 2D projections reveal different trade-offs
    """
    
    def __init__(self, dimensions: Optional[List[str]] = None):
        """
        Initialize extended analyzer
        
        Args:
            dimensions: List of dimensions to consider (default: all 7)
        """
        self.dimensions = dimensions or [
            'accuracy', 'energy_kwh', 'carbon_co2e_kg', 'latency_ms',
            'memory_mb', 'circuit_depth', 'variance_score'
        ]
        logger.info(f"Initialized ExtendedParetoAnalyzer with {len(self.dimensions)}D")
    
    def compute_frontier(self, agents: List[ExtendedParetoPoint]) -> List[ExtendedParetoPoint]:
        """
        Compute 7D Pareto frontier
        
        Returns agents that are non-dominated in extended space.
        
        Args:
            agents: List of ExtendedParetoPoint objects
        
        Returns:
            List of agents on the Pareto frontier
        """
        if not agents:
            logger.warning("Empty agent list")
            return []
        
        frontier = []
        
        for agent in agents:
            is_dominated = False
            
            for other in agents:
                if other.agent_id != agent.agent_id:
                    if other.dominates(agent, self.dimensions):
                        is_dominated = True
                        logger.debug(f"{agent.agent_id} dominated by {other.agent_id}")
                        break
            
            if not is_dominated:
                frontier.append(agent)
        
        logger.info(f"7D frontier: {len(frontier)} / {len(agents)} agents")
        return frontier
    
    def project_2d(self,
                   agents: List[ExtendedParetoPoint],
                   x_dim: str,
                   y_dim: str) -> Tuple[List[ExtendedParetoPoint], List[ExtendedParetoPoint]]:
        """
        Project frontier onto 2D plane
        
        Returns agents on the 2D frontier for this projection.
        Important: An agent can be on 7D frontier but NOT on a 2D projection,
        and vice versa.
        
        Args:
            agents: List of agents (can be 7D frontier or all agents)
            x_dim: X-axis dimension
            y_dim: Y-axis dimension
        
        Returns:
            Tuple of (2d_frontier, dominated_agents)
        
        Example:
            # Agent might be excellent in Accuracy vs Carbon
            # but poor in Latency vs Energy
            frontier_2d, dominated = analyzer.project_2d(
                agents, 'carbon_co2e_kg', 'accuracy'
            )
        """
        # Create temporary 2D analyzer
        temp_analyzer = ExtendedParetoAnalyzer(dimensions=[x_dim, y_dim])
        
        # Compute 2D frontier
        frontier_2d = temp_analyzer.compute_frontier(agents)
        
        # Find dominated agents in this projection
        frontier_ids = {a.agent_id for a in frontier_2d}
        dominated = [a for a in agents if a.agent_id not in frontier_ids]
        
        logger.info(f"2D projection ({x_dim} vs {y_dim}): "
                   f"{len(frontier_2d)} frontier, {len(dominated)} dominated")
        
        return frontier_2d, dominated
    
    def analyze_memory_constraint(self,
                                  agents: List[ExtendedParetoPoint],
                                  max_memory_mb: float) -> Dict:
        """
        Analyze agents under memory constraint
        
        Critical for edge deployment where memory is hard limit.
        
        Args:
            agents: List of agents
            max_memory_mb: Maximum allowed memory (e.g., 512 MB for edge)
        
        Returns:
            Dict with:
            {
                'feasible': List[agents that fit in memory],
                'infeasible': List[agents that don't fit],
                'frontier_feasible': Frontier of feasible agents,
                'memory_efficiency': Dict[agent_id -> accuracy/MB]
            }
        """
        feasible = [a for a in agents if a.memory_mb <= max_memory_mb]
        infeasible = [a for a in agents if a.memory_mb > max_memory_mb]
        
        # Compute frontier of feasible agents
        frontier_feasible = self.compute_frontier(feasible) if feasible else []
        
        # Memory efficiency: accuracy per MB
        memory_efficiency = {
            a.agent_id: a.accuracy / a.memory_mb if a.memory_mb > 0 else 0.0
            for a in agents
        }
        
        logger.info(f"Memory constraint {max_memory_mb} MB: "
                   f"{len(feasible)} feasible, {len(infeasible)} infeasible")
        
        return {
            'max_memory_mb': max_memory_mb,
            'feasible_count': len(feasible),
            'infeasible_count': len(infeasible),
            'feasible': feasible,
            'infeasible': infeasible,
            'frontier_feasible': frontier_feasible,
            'memory_efficiency': memory_efficiency,
            'best_memory_efficient': max(memory_efficiency.items(), 
                                        key=lambda x: x[1])[0] if memory_efficiency else None
        }
    
    def analyze_circuit_depth_scalability(self,
                                         agents: List[ExtendedParetoPoint]) -> Dict:
        """
        Analyze quantum circuit depth implications
        
        Predicts scalability and stability on real quantum hardware.
        
        Args:
            agents: List of agents
        
        Returns:
            Dict with depth analysis:
            {
                'depth_distribution': histogram,
                'accuracy_vs_depth': correlation,
                'energy_vs_depth': correlation,
                'shallow_circuit_agents': agents with depth < threshold,
                'fragility_score': depth normalized by accuracy
            }
        """
        # Filter quantum/hybrid agents (circuit_depth > 0)
        quantum_agents = [a for a in agents if a.circuit_depth > 0]
        
        if not quantum_agents:
            logger.warning("No quantum agents found (circuit_depth=0)")
            return {'quantum_agents': 0}
        
        depths = [a.circuit_depth for a in quantum_agents]
        accuracies = [a.accuracy for a in quantum_agents]
        energies = [a.energy_kwh for a in quantum_agents]
        
        # Correlations
        acc_depth_corr = np.corrcoef(depths, accuracies)[0, 1] if len(depths) > 1 else 0
        energy_depth_corr = np.corrcoef(depths, energies)[0, 1] if len(depths) > 1 else 0
        
        # Fragility score: depth / accuracy (higher = more fragile)
        fragility_scores = {
            a.agent_id: a.circuit_depth / a.accuracy if a.accuracy > 0 else float('inf')
            for a in quantum_agents
        }
        
        # Find shallow circuits (depth < median)
        median_depth = np.median(depths)
        shallow_agents = [a for a in quantum_agents if a.circuit_depth < median_depth]
        
        logger.info(f"Circuit depth analysis: {len(quantum_agents)} quantum agents, "
                   f"median depth={median_depth:.0f}")
        
        return {
            'quantum_agents_count': len(quantum_agents),
            'depth_stats': {
                'mean': np.mean(depths),
                'median': median_depth,
                'min': np.min(depths),
                'max': np.max(depths),
                'std': np.std(depths)
            },
            'correlations': {
                'accuracy_vs_depth': acc_depth_corr,
                'energy_vs_depth': energy_depth_corr
            },
            'shallow_circuit_agents': [a.agent_id for a in shallow_agents],
            'fragility_scores': fragility_scores,
            'most_fragile': max(fragility_scores.items(), key=lambda x: x[1])[0],
            'most_robust': min(fragility_scores.items(), key=lambda x: x[1])[0]
        }
    
    def analyze_variance_stability(self,
                                   agents: List[ExtendedParetoPoint],
                                   stability_threshold: float = 0.2) -> Dict:
        """
        Analyze inference variance and stability
        
        Critical for production: unpredictable systems are less green in practice.
        High variance leads to:
        - SLA violations
        - Intermittent carbon cap breaches
        - Poor schedulability
        
        Args:
            agents: List of agents
            stability_threshold: Max acceptable variance score
        
        Returns:
            Dict with variance analysis:
            {
                'stable': agents with variance < threshold,
                'unstable': agents with variance >= threshold,
                'variance_cost': variance impact on mean metrics,
                'stability_ranking': agents sorted by stability
            }
        """
        stable = [a for a in agents if a.variance_score < stability_threshold]
        unstable = [a for a in agents if a.variance_score >= stability_threshold]
        
        # Variance cost: how much worse are actual P95 metrics vs mean?
        # Approximation: mean + 2*std = ~P95
        variance_cost = {}
        for agent in agents:
            # Estimate P95 energy (assuming variance_score ~ coefficient of variation)
            p95_energy_estimate = agent.energy_kwh * (1 + 2 * agent.variance_score)
            variance_cost[agent.agent_id] = p95_energy_estimate - agent.energy_kwh
        
        # Stability ranking (lower variance = higher rank)
        stability_ranking = sorted(agents, key=lambda a: a.variance_score)
        
        logger.info(f"Variance analysis: {len(stable)} stable, {len(unstable)} unstable "
                   f"(threshold={stability_threshold})")
        
        return {
            'stability_threshold': stability_threshold,
            'stable_count': len(stable),
            'unstable_count': len(unstable),
            'stable': stable,
            'unstable': unstable,
            'variance_cost': variance_cost,
            'stability_ranking': [a.agent_id for a in stability_ranking],
            'most_stable': stability_ranking[0].agent_id if stability_ranking else None,
            'least_stable': stability_ranking[-1].agent_id if stability_ranking else None,
            'mean_variance': np.mean([a.variance_score for a in agents])
        }
    
    def comprehensive_analysis(self,
                              agents: List[ExtendedParetoPoint],
                              constraints: Optional[Dict] = None) -> Dict:
        """
        Comprehensive 7D analysis with all extended dimensions
        
        Args:
            agents: List of agents
            constraints: Optional constraints:
                {
                    'max_memory_mb': 512,
                    'max_circuit_depth': 100,
                    'max_variance': 0.2
                }
        
        Returns:
            Complete analysis report
        """
        if constraints is None:
            constraints = {}
        
        # 7D Pareto frontier
        frontier_7d = self.compute_frontier(agents)
        
        # Memory analysis
        memory_analysis = self.analyze_memory_constraint(
            agents,
            constraints.get('max_memory_mb', 1024)
        )
        
        # Circuit depth analysis
        circuit_analysis = self.analyze_circuit_depth_scalability(agents)
        
        # Variance analysis
        variance_analysis = self.analyze_variance_stability(
            agents,
            constraints.get('max_variance', 0.2)
        )
        
        # Find agents that satisfy ALL constraints
        fully_compliant = []
        for agent in agents:
            compliant = True
            
            if agent.memory_mb > constraints.get('max_memory_mb', float('inf')):
                compliant = False
            if agent.circuit_depth > constraints.get('max_circuit_depth', float('inf')):
                compliant = False
            if agent.variance_score > constraints.get('max_variance', float('inf')):
                compliant = False
            
            if compliant:
                fully_compliant.append(agent)
        
        # Frontier of compliant agents
        frontier_compliant = self.compute_frontier(fully_compliant) if fully_compliant else []
        
        return {
            'total_agents': len(agents),
            'frontier_7d': frontier_7d,
            'frontier_7d_count': len(frontier_7d),
            'memory_analysis': memory_analysis,
            'circuit_analysis': circuit_analysis,
            'variance_analysis': variance_analysis,
            'constraints': constraints,
            'fully_compliant_count': len(fully_compliant),
            'frontier_compliant': frontier_compliant,
            'frontier_compliant_count': len(frontier_compliant),
            'recommendation': frontier_compliant[0].agent_id if frontier_compliant else None
        }
