# src/governance/benchmark_engine.py (EXTENDED)

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import numpy as np

@dataclass
class BenchmarkReport:
    """Enhanced benchmark report with helium metrics"""
    task_id: str
    energy_efficiency: float  # tasks per kWh
    carbon_efficiency: float   # tasks per kg CO2
    helium_efficiency: float    # tasks per helium unit
    pareto_frontier: List[Dict]
    recommendations: List[str]
    helium_resilience_score: float  # 0.0 to 1.0

class HeliumAwareBenchmarkEngine:
    """
    Enhanced benchmark engine with helium efficiency metrics and 3D Pareto analysis
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.metrics_history: List[Dict] = []
        self.pareto_frontier_cache = {}
    
    def calculate_helium_efficiency(self, execution_result) -> float:
        """Calculate tasks per unit helium dependency"""
        
        helium_usage = getattr(execution_result, 'helium_usage', 0.0)
        task_complexity = getattr(execution_result, 'complexity_score', 1.0)
        
        if helium_usage == 0:
            return float('inf')
        
        return task_complexity / helium_usage
    
    def calculate_helium_resilience_score(self, execution_result, 
                                          helium_supply_status) -> float:
        """
        Calculate how well the task performed under helium constraints
        Higher score = better resilience to helium scarcity
        """
        
        helium_zone = getattr(execution_result, 'helium_zone', None)
        fallback_used = getattr(execution_result, 'fallback_used', False)
        accuracy = getattr(execution_result, 'accuracy', 0.0)
        
        # Base resilience
        if helium_zone in ['helium_critical', 'helium_red']:
            if not fallback_used and accuracy > 0.8:
                # Excellent: Ran well despite scarcity
                resilience = 0.9
            elif fallback_used and accuracy > 0.7:
                # Good: Fallback worked
                resilience = 0.7
            else:
                # Poor: Failed or badly degraded
                resilience = 0.3
        elif helium_zone == 'helium_yellow':
            resilience = 0.8
        else:
            resilience = 1.0  # No stress
        
        # Adjust for supply status at execution
        if helium_supply_status and hasattr(helium_supply_status, 'scarcity_score'):
            # Higher scrutiny under severe scarcity
            resilience *= (1.0 - helium_supply_status.scarcity_score * 0.3)
        
        return max(0.0, min(1.0, resilience))
    
    def update_pareto_frontier(self, execution_result, helium_supply_status) -> BenchmarkReport:
        """
        Update 3D Pareto frontier: Energy × Time × Helium usage
        """
        
        # Extract metrics
        metrics = {
            'energy_kwh': getattr(execution_result, 'energy_consumed_kwh', 0),
            'execution_time_ms': getattr(execution_result, 'execution_time_ms', 0),
            'helium_usage': getattr(execution_result, 'helium_usage', 0),
            'accuracy': getattr(execution_result, 'accuracy', 0),
            'carbon_kg': getattr(execution_result, 'carbon_emitted_kg', 0)
        }
        
        # Add to history
        self.metrics_history.append(metrics)
        if len(self.metrics_history) > 1000:
            self.metrics_history = self.metrics_history[-1000:]
        
        # Calculate Pareto frontier (minimize all)
        frontier = self._compute_pareto_frontier(self.metrics_history)
        
        # Calculate efficiencies
        helium_efficiency = self.calculate_helium_efficiency(execution_result)
        helium_resilience = self.calculate_helium_resilience_score(execution_result, helium_supply_status)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, frontier)
        
        return BenchmarkReport(
            task_id=getattr(execution_result, 'task_id', 'unknown'),
            energy_efficiency=1.0 / metrics['energy_kwh'] if metrics['energy_kwh'] > 0 else float('inf'),
            carbon_efficiency=1.0 / metrics['carbon_kg'] if metrics['carbon_kg'] > 0 else float('inf'),
            helium_efficiency=helium_efficiency,
            pareto_frontier=frontier,
            recommendations=recommendations,
            helium_resilience_score=helium_resilience
        )
    
    def _compute_pareto_frontier(self, metrics_list: List[Dict]) -> List[Dict]:
        """
        Compute 3D Pareto frontier minimizing energy, time, and helium
        """
        points = [(m['energy_kwh'], m['execution_time_ms'], m['helium_usage']) 
                  for m in metrics_list]
        
        # Find Pareto optimal points (minimization)
        pareto_points = []
        for i, (e, t, h) in enumerate(points):
            dominated = False
            for j, (e2, t2, h2) in enumerate(points):
                if i != j and e2 <= e and t2 <= t and h2 <= h:
                    if e2 < e or t2 < t or h2 < h:
                        dominated = True
                        break
            if not dominated:
                pareto_points.append(metrics_list[i])
        
        return pareto_points
    
    def _generate_recommendations(self, metrics: Dict, frontier: List[Dict]) -> List[str]:
        """
        Generate actionable recommendations based on metrics
        """
        recommendations = []
        
        # Helium efficiency recommendation
        if metrics['helium_usage'] > 0.5:
            recommendations.append("Consider quantization to reduce helium footprint")
        
        if metrics['energy_kwh'] > 1.0:
            recommendations.append("High energy usage detected - consider pruning or distillation")
        
        # Compare to Pareto frontier
        if frontier:
            avg_helium = np.mean([f['helium_usage'] for f in frontier])
            if metrics['helium_usage'] > avg_helium * 1.5:
                recommendations.append(f"Helium usage {metrics['helium_usage']:.2f} is 50% above Pareto optimal - consider alternative hardware")
        
        if not recommendations:
            recommendations.append("Helium efficiency is optimal - maintaining current strategy")
        
        return recommendations
    
    def get_helium_ranking(self, top_n: int = 10) -> List[Dict]:
        """Get top N tasks by helium efficiency"""
        
        if not self.metrics_history:
            return []
        
        # Calculate efficiency for each point
        efficiencies = []
        for m in self.metrics_history:
            helium_usage = m.get('helium_usage', 0)
            if helium_usage > 0:
                efficiency = 1.0 / helium_usage
                efficiencies.append({
                    'efficiency': efficiency,
                    'metrics': m
                })
        
        # Sort by efficiency descending
        efficiencies.sort(key=lambda x: x['efficiency'], reverse=True)
        
        return efficiencies[:top_n]
