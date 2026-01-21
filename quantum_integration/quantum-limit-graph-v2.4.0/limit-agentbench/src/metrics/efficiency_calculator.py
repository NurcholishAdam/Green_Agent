"""
Normalized Efficiency Calculator

Calculates efficiency metrics normalized by task complexity to enable
fair comparison across tasks of different difficulties.
"""

from typing import Dict, List, Optional, Tuple
import numpy as np
import logging

# Import from analysis module
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.complexity_analyzer import ComplexityAnalyzer, TaskComplexity

logger = logging.getLogger(__name__)


class NormalizedEfficiencyCalculator:
    """
    Calculate efficiency metrics normalized by task complexity
    
    This enables fair comparison between agents across tasks of different
    complexity levels. Without normalization, simple tasks appear "efficient"
    and complex tasks appear "wasteful".
    
    Key Metrics:
    - Energy Efficiency = Energy / Task Complexity
    - Carbon Efficiency = Carbon / Task Complexity
    - Accuracy per Watt = Accuracy / Energy
    - Throughput per Watt = Tasks Completed / Energy
    """
    
    def __init__(self):
        """Initialize efficiency calculator"""
        self.complexity_analyzer = ComplexityAnalyzer()
        logger.info("Initialized NormalizedEfficiencyCalculator")
    
    def calculate_energy_efficiency(self, 
                                    energy_kwh: float,
                                    trace: Dict,
                                    complexity: Optional[TaskComplexity] = None) -> float:
        """
        Calculate energy efficiency normalized by task complexity
        
        Formula:
            energy_efficiency = energy_kwh / task_complexity_score
        
        Lower values are better (less energy per unit complexity)
        
        Args:
            energy_kwh: Energy consumed by agent
            trace: Execution trace for complexity analysis
            complexity: Pre-computed TaskComplexity (optional)
        
        Returns:
            Energy efficiency score (lower = better)
        
        Example:
            Task A: 0.003 kWh, complexity 1.0 => efficiency 0.003
            Task B: 0.006 kWh, complexity 2.0 => efficiency 0.003 (same!)
            
            Without normalization, Task A looks 2x better
            With normalization, they're equal (fair comparison)
        """
        # Get or compute complexity
        if complexity is None:
            complexity = self.complexity_analyzer.analyze_from_trace(trace)
        
        complexity_score = complexity.compute_composite_score()
        
        # Avoid division by zero
        if complexity_score < 0.01:
            logger.warning(f"Very low complexity score: {complexity_score}, using 0.01")
            complexity_score = 0.01
        
        efficiency = energy_kwh / complexity_score
        
        logger.debug(f"Energy efficiency: {efficiency:.6f} (energy={energy_kwh}, complexity={complexity_score})")
        return efficiency
    
    def calculate_carbon_efficiency(self,
                                    carbon_kg: float,
                                    trace: Dict,
                                    complexity: Optional[TaskComplexity] = None) -> float:
        """
        Calculate carbon efficiency normalized by task complexity
        
        Formula:
            carbon_efficiency = carbon_kg / task_complexity_score
        
        Lower values are better (less carbon per unit complexity)
        
        Args:
            carbon_kg: Carbon emissions in kg CO₂e
            trace: Execution trace
            complexity: Pre-computed TaskComplexity (optional)
        
        Returns:
            Carbon efficiency score (lower = better)
        """
        if complexity is None:
            complexity = self.complexity_analyzer.analyze_from_trace(trace)
        
        complexity_score = complexity.compute_composite_score()
        
        if complexity_score < 0.01:
            complexity_score = 0.01
        
        efficiency = carbon_kg / complexity_score
        
        logger.debug(f"Carbon efficiency: {efficiency:.6f}")
        return efficiency
    
    def calculate_accuracy_per_watt(self,
                                    accuracy: float,
                                    energy_kwh: float) -> float:
        """
        Calculate accuracy per watt-hour of energy
        
        Formula:
            accuracy_per_watt = accuracy / energy_kwh
        
        Higher values are better (more accuracy per unit energy)
        
        This metric is NOT normalized by complexity - it's a raw
        efficiency metric useful for comparing agents on identical tasks.
        
        Args:
            accuracy: Task accuracy [0.0, 1.0]
            energy_kwh: Energy consumed
        
        Returns:
            Accuracy per watt (higher = better)
        
        Example:
            Agent A: 95% accuracy, 0.003 kWh => 316.7 acc/Wh
            Agent B: 90% accuracy, 0.005 kWh => 180.0 acc/Wh
            => Agent A is more efficient
        """
        if energy_kwh < 1e-6:
            logger.warning(f"Very low energy: {energy_kwh}, using 1e-6")
            energy_kwh = 1e-6
        
        efficiency = accuracy / energy_kwh
        
        logger.debug(f"Accuracy per watt: {efficiency:.2f}")
        return efficiency
    
    def calculate_throughput_per_watt(self,
                                     tasks_completed: int,
                                     energy_kwh: float) -> float:
        """
        Calculate throughput per watt
        
        Formula:
            throughput_per_watt = tasks_completed / energy_kwh
        
        Higher values are better (more tasks per unit energy)
        
        Useful for batch processing scenarios where you care about
        how many tasks can be completed per unit energy.
        
        Args:
            tasks_completed: Number of tasks successfully completed
            energy_kwh: Total energy consumed
        
        Returns:
            Tasks per watt-hour (higher = better)
        """
        if energy_kwh < 1e-6:
            energy_kwh = 1e-6
        
        throughput = tasks_completed / energy_kwh
        
        logger.debug(f"Throughput: {throughput:.2f} tasks/Wh")
        return throughput
    
    def calculate_latency_efficiency(self,
                                    accuracy: float,
                                    latency_ms: float) -> float:
        """
        Calculate latency efficiency (accuracy per second)
        
        Formula:
            latency_efficiency = accuracy / (latency_ms / 1000)
        
        Higher values are better (more accuracy per second)
        
        Args:
            accuracy: Task accuracy
            latency_ms: Task latency in milliseconds
        
        Returns:
            Accuracy per second (higher = better)
        """
        if latency_ms < 1.0:
            latency_ms = 1.0
        
        latency_seconds = latency_ms / 1000.0
        efficiency = accuracy / latency_seconds
        
        logger.debug(f"Latency efficiency: {efficiency:.2f} acc/s")
        return efficiency
    
    def calculate_composite_efficiency(self,
                                      accuracy: float,
                                      energy_kwh: float,
                                      carbon_kg: float,
                                      latency_ms: float,
                                      trace: Dict,
                                      weights: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate composite efficiency score
        
        Combines multiple efficiency metrics into a single score,
        all normalized by task complexity.
        
        Args:
            accuracy: Task accuracy
            energy_kwh: Energy consumed
            carbon_kg: Carbon emitted
            latency_ms: Task latency
            trace: Execution trace
            weights: Custom weights for each component
        
        Returns:
            Composite efficiency score (higher = better)
        
        Default weights:
            - accuracy_per_watt: 0.4
            - energy_efficiency: 0.3 (inverted, so lower is better)
            - carbon_efficiency: 0.2 (inverted)
            - latency_efficiency: 0.1
        """
        if weights is None:
            weights = {
                'accuracy_per_watt': 0.4,
                'energy_efficiency': 0.3,
                'carbon_efficiency': 0.2,
                'latency_efficiency': 0.1
            }
        
        # Compute complexity
        complexity = self.complexity_analyzer.analyze_from_trace(trace)
        
        # Calculate individual efficiencies
        acc_per_watt = self.calculate_accuracy_per_watt(accuracy, energy_kwh)
        energy_eff = self.calculate_energy_efficiency(energy_kwh, trace, complexity)
        carbon_eff = self.calculate_carbon_efficiency(carbon_kg, trace, complexity)
        latency_eff = self.calculate_latency_efficiency(accuracy, latency_ms)
        
        # Normalize to [0, 1] range (rough normalization)
        # These values are domain-specific and may need tuning
        norm_acc_per_watt = min(acc_per_watt / 500.0, 1.0)  # 500 is excellent
        norm_energy_eff = max(1.0 - energy_eff / 0.01, 0.0)  # Lower is better, so invert
        norm_carbon_eff = max(1.0 - carbon_eff / 0.002, 0.0)  # Lower is better, so invert
        norm_latency_eff = min(latency_eff / 10.0, 1.0)  # 10 acc/s is excellent
        
        # Weighted sum
        composite = (
            weights['accuracy_per_watt'] * norm_acc_per_watt +
            weights['energy_efficiency'] * norm_energy_eff +
            weights['carbon_efficiency'] * norm_carbon_eff +
            weights['latency_efficiency'] * norm_latency_eff
        )
        
        logger.info(f"Composite efficiency: {composite:.3f}")
        return composite
    
    def compare_across_complexities(self,
                                    results: List[Dict]) -> Dict:
        """
        Fair comparison across tasks of different complexity
        
        This is the main function for comparing agents fairly when they
        were evaluated on tasks of different complexities.
        
        Args:
            results: List of result dictionaries:
                [{
                    'agent_id': str,
                    'task_id': str,
                    'accuracy': float,
                    'energy_kwh': float,
                    'carbon_kg': float,
                    'latency_ms': float,
                    'trace': Dict
                }, ...]
        
        Returns:
            Dictionary with normalized rankings and statistics:
            {
                'rankings': List[Dict],
                'summary': Dict,
                'complexity_distribution': Dict
            }
        
        Example:
            results = [
                {'agent_id': 'A', 'accuracy': 0.95, 'energy_kwh': 0.01, ...},
                {'agent_id': 'B', 'accuracy': 0.90, 'energy_kwh': 0.003, ...}
            ]
            comparison = calculator.compare_across_complexities(results)
            print(f"Best agent: {comparison['rankings'][0]['agent_id']}")
        """
        rankings = []
        
        for result in results:
            # Extract task complexity
            complexity = self.complexity_analyzer.analyze_from_trace(result['trace'])
            complexity_score = complexity.compute_composite_score()
            complexity_tier = self.complexity_analyzer.categorize_complexity(complexity)
            
            # Calculate normalized efficiencies
            energy_eff = self.calculate_energy_efficiency(
                result['energy_kwh'],
                result['trace'],
                complexity
            )
            
            carbon_eff = self.calculate_carbon_efficiency(
                result['carbon_kg'],
                result['trace'],
                complexity
            )
            
            accuracy_per_watt = self.calculate_accuracy_per_watt(
                result['accuracy'],
                result['energy_kwh']
            )
            
            latency_eff = self.calculate_latency_efficiency(
                result['accuracy'],
                result['latency_ms']
            )
            
            composite_eff = self.calculate_composite_efficiency(
                result['accuracy'],
                result['energy_kwh'],
                result['carbon_kg'],
                result['latency_ms'],
                result['trace']
            )
            
            rankings.append({
                'agent_id': result['agent_id'],
                'task_id': result['task_id'],
                
                # Raw metrics
                'accuracy': result['accuracy'],
                'energy_kwh': result['energy_kwh'],
                'carbon_kg': result['carbon_kg'],
                'latency_ms': result['latency_ms'],
                
                # Complexity info
                'task_complexity': complexity_score,
                'complexity_tier': complexity_tier,
                
                # Normalized efficiencies
                'energy_efficiency': energy_eff,
                'carbon_efficiency': carbon_eff,
                'accuracy_per_watt': accuracy_per_watt,
                'latency_efficiency': latency_eff,
                'composite_efficiency': composite_eff
            })
        
        # Sort by composite efficiency (higher is better)
        rankings.sort(key=lambda x: x['composite_efficiency'], reverse=True)
        
        # Add ranks
        for i, ranking in enumerate(rankings):
            ranking['rank'] = i + 1
        
        # Compute summary statistics
        summary = {
            'total_agents': len(rankings),
            'avg_energy_efficiency': np.mean([r['energy_efficiency'] for r in rankings]),
            'avg_carbon_efficiency': np.mean([r['carbon_efficiency'] for r in rankings]),
            'avg_accuracy_per_watt': np.mean([r['accuracy_per_watt'] for r in rankings]),
            'avg_composite_efficiency': np.mean([r['composite_efficiency'] for r in rankings]),
            'best_agent': rankings[0]['agent_id'] if rankings else None
        }
        
        # Complexity distribution
        tiers = [r['complexity_tier'] for r in rankings]
        complexity_distribution = {
            tier: tiers.count(tier) for tier in set(tiers)
        }
        
        return {
            'rankings': rankings,
            'summary': summary,
            'complexity_distribution': complexity_distribution
        }
    
    def benchmark_efficiency_baseline(self,
                                     results: List[Dict]) -> Dict:
        """
        Establish efficiency baselines from a set of results
        
        Useful for determining what "good" efficiency looks like
        in your specific domain.
        
        Args:
            results: List of result dictionaries
        
        Returns:
            Dictionary with baseline metrics:
            {
                'energy_efficiency': {'p50': ..., 'p90': ..., 'p95': ...},
                'accuracy_per_watt': {...},
                ...
            }
        """
        comparison = self.compare_across_complexities(results)
        rankings = comparison['rankings']
        
        if not rankings:
            return {}
        
        # Calculate percentiles for each metric
        metrics = [
            'energy_efficiency',
            'carbon_efficiency',
            'accuracy_per_watt',
            'latency_efficiency',
            'composite_efficiency'
        ]
        
        baselines = {}
        for metric in metrics:
            values = [r[metric] for r in rankings]
            baselines[metric] = {
                'min': np.min(values),
                'p25': np.percentile(values, 25),
                'p50': np.percentile(values, 50),
                'p75': np.percentile(values, 75),
                'p90': np.percentile(values, 90),
                'p95': np.percentile(values, 95),
                'max': np.max(values),
                'mean': np.mean(values),
                'std': np.std(values)
            }
        
        logger.info(f"Established baselines from {len(results)} results")
        return baselines
    
    def detect_efficiency_outliers(self,
                                   results: List[Dict],
                                   threshold_std: float = 2.0) -> List[Dict]:
        """
        Detect agents with unusually poor efficiency
        
        Uses statistical outlier detection to flag agents that are
        significantly less efficient than the population.
        
        Args:
            results: List of result dictionaries
            threshold_std: Standard deviations from mean to flag (default: 2.0)
        
        Returns:
            List of outlier agents with details
        """
        comparison = self.compare_across_complexities(results)
        rankings = comparison['rankings']
        
        if len(rankings) < 3:
            logger.warning("Too few results for outlier detection")
            return []
        
        # Calculate mean and std for composite efficiency
        efficiencies = [r['composite_efficiency'] for r in rankings]
        mean_eff = np.mean(efficiencies)
        std_eff = np.std(efficiencies)
        
        # Flag outliers
        outliers = []
        for ranking in rankings:
            z_score = (ranking['composite_efficiency'] - mean_eff) / std_eff
            
            if z_score < -threshold_std:  # Negative z-score = below average
                outliers.append({
                    'agent_id': ranking['agent_id'],
                    'task_id': ranking['task_id'],
                    'composite_efficiency': ranking['composite_efficiency'],
                    'z_score': z_score,
                    'deviation': f"{abs(z_score):.1f} std below mean",
                    'recommendation': self._generate_efficiency_recommendation(ranking)
                })
        
        logger.info(f"Detected {len(outliers)} efficiency outliers")
        return outliers
    
    def _generate_efficiency_recommendation(self, ranking: Dict) -> str:
        """Generate efficiency improvement recommendation"""
        issues = []
        
        # Check specific efficiency metrics
        if ranking['energy_efficiency'] > 0.01:
            issues.append("High energy consumption relative to task complexity")
        
        if ranking['accuracy_per_watt'] < 100:
            issues.append("Low accuracy per watt - consider more efficient models")
        
        if ranking['latency_efficiency'] < 1.0:
            issues.append("Slow execution - optimize inference speed")
        
        if not issues:
            return "Overall efficiency is poor - review entire pipeline"
        
        return "; ".join(issues)
