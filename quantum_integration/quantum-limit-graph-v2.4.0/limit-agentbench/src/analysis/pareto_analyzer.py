"""
Pareto Frontier Analysis for Multi-Objective Agent Evaluation

This module implements Pareto optimality analysis to compare agents across
multiple objectives (accuracy, energy, carbon, latency) simultaneously.
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
import numpy as np
import logging

logger = logging.getLogger(__name__)


@dataclass
class ParetoPoint:
    """
    Represents a point in multi-objective space
    
    A Pareto point captures an agent's performance across multiple
    conflicting objectives (maximize accuracy, minimize energy/carbon/latency)
    """
    agent_id: str
    accuracy: float
    energy_kwh: float
    carbon_co2e_kg: float
    latency_ms: float
    
    # Optional metadata
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate point data"""
        if not 0 <= self.accuracy <= 1:
            logger.warning(f"Accuracy {self.accuracy} outside [0, 1] range for {self.agent_id}")
        if self.energy_kwh < 0:
            raise ValueError(f"Energy cannot be negative: {self.energy_kwh}")
        if self.carbon_co2e_kg < 0:
            raise ValueError(f"Carbon cannot be negative: {self.carbon_co2e_kg}")
        if self.latency_ms < 0:
            raise ValueError(f"Latency cannot be negative: {self.latency_ms}")
    
    def dominates(self, other: 'ParetoPoint', objectives: List[str] = None) -> bool:
        """
        Check if this point Pareto-dominates another
        
        Point A dominates Point B if:
        - A is no worse than B on all objectives
        - A is strictly better than B on at least one objective
        
        Args:
            other: Another ParetoPoint to compare against
            objectives: List of objectives to consider. If None, uses all.
        
        Returns:
            True if this point dominates other, False otherwise
        
        Example:
            Agent A: 95% accuracy, 0.003 kWh
            Agent B: 90% accuracy, 0.005 kWh
            => A dominates B (better on both accuracy and energy)
            
            Agent C: 95% accuracy, 0.005 kWh
            Agent D: 90% accuracy, 0.003 kWh
            => Neither dominates (trade-off exists)
        """
        if objectives is None:
            objectives = ['accuracy', 'energy_kwh', 'carbon_co2e_kg', 'latency_ms']
        
        better_on_at_least_one = False
        
        for obj in objectives:
            self_val = getattr(self, obj)
            other_val = getattr(other, obj)
            
            # For minimization objectives (energy, carbon, latency)
            if obj in ['energy_kwh', 'carbon_co2e_kg', 'latency_ms']:
                if self_val > other_val:  # Worse on this objective
                    return False
                if self_val < other_val:  # Better on this objective
                    better_on_at_least_one = True
            # For maximization objectives (accuracy)
            else:
                if self_val < other_val:  # Worse on this objective
                    return False
                if self_val > other_val:  # Better on this objective
                    better_on_at_least_one = True
        
        return better_on_at_least_one
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'agent_id': self.agent_id,
            'accuracy': self.accuracy,
            'energy_kwh': self.energy_kwh,
            'carbon_co2e_kg': self.carbon_co2e_kg,
            'latency_ms': self.latency_ms,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ParetoPoint':
        """Create ParetoPoint from dictionary"""
        return cls(
            agent_id=data['agent_id'],
            accuracy=data['accuracy'],
            energy_kwh=data['energy_kwh'],
            carbon_co2e_kg=data['carbon_co2e_kg'],
            latency_ms=data['latency_ms'],
            metadata=data.get('metadata', {})
        )


class ParetoFrontierAnalyzer:
    """
    Analyzes agents using Pareto optimality instead of single score
    
    Pareto frontier = set of non-dominated solutions
    These represent the best possible trade-offs between objectives
    """
    
    def __init__(self, objectives: List[str] = None):
        """
        Initialize Pareto analyzer
        
        Args:
            objectives: List of objectives to consider
                       Default: ['accuracy', 'energy_kwh', 'carbon_co2e_kg', 'latency_ms']
        """
        self.objectives = objectives or ['accuracy', 'energy_kwh', 'carbon_co2e_kg', 'latency_ms']
        logger.info(f"Initialized ParetoFrontierAnalyzer with objectives: {self.objectives}")
    
    def compute_frontier(self, agents: List[ParetoPoint]) -> List[ParetoPoint]:
        """
        Compute Pareto frontier (non-dominated points)
        
        Args:
            agents: List of ParetoPoint objects
        
        Returns:
            List of agents on the Pareto frontier
        
        Example:
            agents = [
                ParetoPoint('A', 0.95, 0.003, 0.0006, 150),
                ParetoPoint('B', 0.90, 0.005, 0.0010, 200),
                ParetoPoint('C', 0.93, 0.002, 0.0004, 180)
            ]
            frontier = analyzer.compute_frontier(agents)
            # Returns: [A, C] (B is dominated by A)
        """
        if not agents:
            logger.warning("Empty agent list provided to compute_frontier")
            return []
        
        frontier = []
        
        for agent in agents:
            is_dominated = False
            
            # Check if this agent is dominated by any other
            for other in agents:
                if other.agent_id != agent.agent_id:
                    if other.dominates(agent, self.objectives):
                        is_dominated = True
                        logger.debug(f"{agent.agent_id} dominated by {other.agent_id}")
                        break
            
            if not is_dominated:
                frontier.append(agent)
                logger.debug(f"{agent.agent_id} added to frontier")
        
        logger.info(f"Computed frontier with {len(frontier)} / {len(agents)} agents")
        return frontier
    
    def rank_by_dominance(self, agents: List[ParetoPoint]) -> Dict[int, List[ParetoPoint]]:
        """
        Rank agents by Pareto dominance layers
        
        Layer 0: Pareto frontier (non-dominated)
        Layer 1: Dominated only by layer 0
        Layer 2: Dominated only by layers 0-1
        ... and so on
        
        Args:
            agents: List of ParetoPoint objects
        
        Returns:
            Dictionary mapping rank -> list of agents
            {
                0: [agents on frontier],
                1: [agents dominated only by frontier],
                2: [agents dominated by layer 1],
                ...
            }
        
        Example:
            ranks = analyzer.rank_by_dominance(agents)
            print(f"Frontier agents: {[a.agent_id for a in ranks[0]]}")
            print(f"Second tier: {[a.agent_id for a in ranks[1]]}")
        """
        if not agents:
            return {}
        
        remaining = agents.copy()
        ranks = {}
        rank = 0
        
        while remaining:
            # Find non-dominated agents in remaining set
            frontier = self.compute_frontier(remaining)
            
            if not frontier:
                # Should not happen, but safety check
                logger.error("No frontier found in remaining agents")
                break
            
            ranks[rank] = frontier
            logger.info(f"Rank {rank}: {len(frontier)} agents")
            
            # Remove frontier from remaining
            frontier_ids = {f.agent_id for f in frontier}
            remaining = [a for a in remaining if a.agent_id not in frontier_ids]
            
            rank += 1
        
        return ranks
    
    def compare_agents(self, agent_a: ParetoPoint, agent_b: ParetoPoint) -> Dict:
        """
        Compare two agents and explain their relationship
        
        Args:
            agent_a: First agent
            agent_b: Second agent
        
        Returns:
            Dictionary with comparison results:
            {
                'relationship': 'dominates' | 'dominated_by' | 'non_comparable',
                'explanation': str,
                'trade_offs': Dict (if non-comparable)
            }
        """
        if agent_a.dominates(agent_b, self.objectives):
            return {
                'relationship': 'dominates',
                'explanation': f"{agent_a.agent_id} dominates {agent_b.agent_id}",
                'better_on': self._get_better_objectives(agent_a, agent_b)
            }
        elif agent_b.dominates(agent_a, self.objectives):
            return {
                'relationship': 'dominated_by',
                'explanation': f"{agent_a.agent_id} is dominated by {agent_b.agent_id}",
                'worse_on': self._get_better_objectives(agent_b, agent_a)
            }
        else:
            return {
                'relationship': 'non_comparable',
                'explanation': f"{agent_a.agent_id} and {agent_b.agent_id} are non-comparable (trade-offs exist)",
                'trade_offs': self._analyze_trade_offs(agent_a, agent_b)
            }
    
    def get_knee_point(self, frontier: List[ParetoPoint]) -> Optional[ParetoPoint]:
        """
        Find "knee point" on frontier - best overall balance
        
        The knee point is the point on the Pareto frontier that offers
        the best compromise between all objectives. It's found by
        computing the distance from an ideal point.
        
        Args:
            frontier: List of ParetoPoint objects on the frontier
        
        Returns:
            ParetoPoint representing the knee point, or None if frontier is empty
        
        Algorithm:
            1. Define ideal point: max accuracy, min energy/carbon/latency
            2. Normalize all objectives to [0, 1]
            3. Find point closest to ideal in normalized space
        """
        if not frontier:
            logger.warning("Empty frontier provided to get_knee_point")
            return None
        
        if len(frontier) == 1:
            return frontier[0]
        
        # Extract objective values
        accuracies = [p.accuracy for p in frontier]
        energies = [p.energy_kwh for p in frontier]
        carbons = [p.carbon_co2e_kg for p in frontier]
        latencies = [p.latency_ms for p in frontier]
        
        # Compute ranges for normalization
        acc_range = max(accuracies) - min(accuracies) if max(accuracies) > min(accuracies) else 1.0
        energy_range = max(energies) - min(energies) if max(energies) > min(energies) else 1.0
        carbon_range = max(carbons) - min(carbons) if max(carbons) > min(carbons) else 1.0
        latency_range = max(latencies) - min(latencies) if max(latencies) > min(latencies) else 1.0
        
        # Ideal point in normalized space: (1, 0, 0, 0)
        # (max accuracy, min energy, min carbon, min latency)
        ideal = np.array([1.0, 0.0, 0.0, 0.0])
        
        # Find point closest to ideal
        min_dist = float('inf')
        knee = None
        
        for point in frontier:
            # Normalize to [0, 1]
            norm_acc = (point.accuracy - min(accuracies)) / acc_range
            norm_energy = (point.energy_kwh - min(energies)) / energy_range
            norm_carbon = (point.carbon_co2e_kg - min(carbons)) / carbon_range
            norm_latency = (point.latency_ms - min(latencies)) / latency_range
            
            vec_norm = np.array([norm_acc, norm_energy, norm_carbon, norm_latency])
            
            # Euclidean distance to ideal
            dist = np.linalg.norm(vec_norm - ideal)
            
            if dist < min_dist:
                min_dist = dist
                knee = point
        
        logger.info(f"Knee point: {knee.agent_id} (distance to ideal: {min_dist:.4f})")
        return knee
    
    def _get_better_objectives(self, winner: ParetoPoint, loser: ParetoPoint) -> Dict[str, float]:
        """Helper to identify which objectives winner is better on"""
        better_on = {}
        
        for obj in self.objectives:
            winner_val = getattr(winner, obj)
            loser_val = getattr(loser, obj)
            
            if obj in ['energy_kwh', 'carbon_co2e_kg', 'latency_ms']:
                if winner_val < loser_val:
                    better_on[obj] = loser_val - winner_val
            else:  # accuracy
                if winner_val > loser_val:
                    better_on[obj] = winner_val - loser_val
        
        return better_on
    
    def _analyze_trade_offs(self, agent_a: ParetoPoint, agent_b: ParetoPoint) -> Dict:
        """Analyze trade-offs between two non-comparable agents"""
        trade_offs = {
            'a_better_on': [],
            'b_better_on': [],
            'differences': {}
        }
        
        for obj in self.objectives:
            a_val = getattr(agent_a, obj)
            b_val = getattr(agent_b, obj)
            diff = a_val - b_val
            
            trade_offs['differences'][obj] = diff
            
            if obj in ['energy_kwh', 'carbon_co2e_kg', 'latency_ms']:
                if a_val < b_val:
                    trade_offs['a_better_on'].append(obj)
                elif b_val < a_val:
                    trade_offs['b_better_on'].append(obj)
            else:  # accuracy
                if a_val > b_val:
                    trade_offs['a_better_on'].append(obj)
                elif b_val > a_val:
                    trade_offs['b_better_on'].append(obj)
        
        return trade_offs
    
    def export_results(self, agents: List[ParetoPoint], filepath: str):
        """Export Pareto analysis results to JSON"""
        import json
        
        frontier = self.compute_frontier(agents)
        ranks = self.rank_by_dominance(agents)
        knee = self.get_knee_point(frontier)
        
        results = {
            'total_agents': len(agents),
            'frontier_size': len(frontier),
            'frontier': [p.to_dict() for p in frontier],
            'knee_point': knee.to_dict() if knee else None,
            'ranks': {
                str(rank): [p.to_dict() for p in agents_list]
                for rank, agents_list in ranks.items()
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Exported Pareto results to {filepath}")
