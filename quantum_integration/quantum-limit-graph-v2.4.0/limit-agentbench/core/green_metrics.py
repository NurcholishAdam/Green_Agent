# Update src/core/green_metrics.py

from analysis.complexity_analyzer import ComplexityAnalyzer
from metrics.efficiency_calculator import NormalizedEfficiencyCalculator

class GreenMetricsTracker:
    def __init__(self):
        self.complexity_analyzer = ComplexityAnalyzer()
        self.efficiency_calc = NormalizedEfficiencyCalculator()
    
    def get_normalized_metrics(self, trace: Dict) -> Dict:
        """Get complexity-normalized metrics"""
        complexity = self.complexity_analyzer.analyze_from_trace(trace)
        
        return {
            'task_complexity': complexity.compute_composite_score(),
            'complexity_tier': self.complexity_analyzer.categorize_complexity(complexity),
            'energy_efficiency': self.efficiency_calc.calculate_energy_efficiency(
                self.energy_kwh, trace
            ),
            'accuracy_per_watt': self.efficiency_calc.calculate_accuracy_per_watt(
                self.accuracy, self.energy_kwh
            )
        }
