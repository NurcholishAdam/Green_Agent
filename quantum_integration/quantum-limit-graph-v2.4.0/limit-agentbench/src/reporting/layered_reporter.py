"""
Multi-Layer Reporting System for Green_Agent

Three transparent reporting layers to prevent misleading conclusions:
- Layer 1: Raw metrics (ground truth)
- Layer 2: Normalized by task complexity
- Layer 3: Scenario-specific weighted scores
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
import numpy as np
import logging

# Import from analysis module
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.complexity_analyzer import ComplexityAnalyzer, TaskComplexity

logger = logging.getLogger(__name__)


@dataclass
class Layer1RawMetrics:
    """
    Layer 1: Raw, unprocessed metrics (ground truth)
    
    This is the source of truth - never altered or normalized.
    All other layers derive from this.
    """
    accuracy: float
    energy_wh: float
    carbon_co2_g: float
    latency_ms: float
    timestamp: str
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary"""
        return {
            'accuracy': self.accuracy,
            'energy_wh': self.energy_wh,
            'carbon_co2_g': self.carbon_co2_g,
            'latency_ms': self.latency_ms,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_result(cls, result: Dict, timestamp: str = None) -> 'Layer1RawMetrics':
        """Create from raw result dictionary"""
        from datetime import datetime
        
        return cls(
            accuracy=result.get('accuracy', 0.0),
            energy_wh=result.get('energy_kwh', 0.0) * 1000,  # Convert to Wh
            carbon_co2_g=result.get('carbon_kg', 0.0) * 1000,  # Convert to g
            latency_ms=result.get('latency_ms', 0.0),
            timestamp=timestamp or datetime.now().isoformat()
        )


@dataclass
class Layer2NormalizedMetrics:
    """
    Layer 2: Metrics normalized by task complexity
    
    Enables fair comparison across tasks of different difficulties.
    """
    energy_per_task: float
    carbon_per_correct_answer: float
    latency_per_reasoning_step: float
    efficiency_score: float
    task_complexity: float
    complexity_tier: str
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary"""
        return {
            'energy_per_task': self.energy_per_task,
            'carbon_per_correct_answer': self.carbon_per_correct_answer,
            'latency_per_reasoning_step': self.latency_per_reasoning_step,
            'efficiency_score': self.efficiency_score,
            'task_complexity': self.task_complexity,
            'complexity_tier': self.complexity_tier
        }


@dataclass
class Layer3ScenarioScore:
    """
    Layer 3: Scenario-specific weighted score
    
    Customized for specific use cases (production, research, cost-sensitive, eco-sensitive).
    """
    weighted_score: float
    scenario_name: str
    weights_used: Dict[str, float]
    rank: Optional[int] = None
    percentile: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary"""
        return {
            'weighted_score': self.weighted_score,
            'scenario_name': self.scenario_name,
            'weights_used': self.weights_used,
            'rank': self.rank,
            'percentile': self.percentile
        }


class LayeredReporter:
    """
    Three-layer reporting system to avoid misleading conclusions
    
    Usage:
        reporter = LayeredReporter()
        
        # Generate all layers
        layer1 = reporter.generate_layer1(result)
        layer2 = reporter.generate_layer2(result, complexity)
        layer3 = reporter.generate_layer3(result, 'production')
        
        # Or generate full report
        full_report = reporter.generate_full_report(results, 'production')
    """
    
    # Predefined scenario weights
    SCENARIO_WEIGHTS = {
        'production': {
            'accuracy': 0.4,
            'latency': 0.4,
            'energy': 0.1,
            'carbon': 0.1
        },
        'research': {
            'accuracy': 0.7,
            'latency': 0.1,
            'energy': 0.1,
            'carbon': 0.1
        },
        'cost_sensitive': {
            'accuracy': 0.3,
            'latency': 0.2,
            'energy': 0.3,
            'carbon': 0.2
        },
        'eco_sensitive': {
            'accuracy': 0.3,
            'latency': 0.1,
            'energy': 0.3,
            'carbon': 0.3
        },
        'real_time': {
            'accuracy': 0.3,
            'latency': 0.6,
            'energy': 0.05,
            'carbon': 0.05
        }
    }
    
    def __init__(self):
        """Initialize layered reporter"""
        self.complexity_analyzer = ComplexityAnalyzer()
        logger.info("Initialized LayeredReporter")
    
    def generate_layer1(self, result: Dict) -> Layer1RawMetrics:
        """
        Generate Layer 1: Raw metrics (no processing)
        
        This is the ground truth - never altered.
        
        Args:
            result: Raw result dictionary with keys:
                'accuracy', 'energy_kwh', 'carbon_kg', 'latency_ms'
        
        Returns:
            Layer1RawMetrics with unprocessed data
        """
        return Layer1RawMetrics.from_result(result)
    
    def generate_layer2(self,
                       result: Dict,
                       task_complexity: Optional[TaskComplexity] = None) -> Layer2NormalizedMetrics:
        """
        Generate Layer 2: Normalized metrics
        
        Fair comparison across different task complexities.
        
        Args:
            result: Raw result dictionary
            task_complexity: Pre-computed TaskComplexity (or computed from trace)
        
        Returns:
            Layer2NormalizedMetrics with complexity-normalized data
        """
        # Get or compute complexity
        if task_complexity is None:
            trace = result.get('trace', {})
            task_complexity = self.complexity_analyzer.analyze_from_trace(trace)
        
        complexity_score = task_complexity.compute_composite_score()
        complexity_tier = self.complexity_analyzer.categorize_complexity(task_complexity)
        
        # Normalize metrics
        energy_wh = result.get('energy_kwh', 0.0) * 1000
        energy_per_task = energy_wh / max(complexity_score, 0.01)
        
        # Carbon per correct answer
        accuracy = result.get('accuracy', 0.0)
        carbon_g = result.get('carbon_kg', 0.0) * 1000
        
        if accuracy > 0:
            carbon_per_correct = carbon_g / accuracy
        else:
            carbon_per_correct = float('inf')
        
        # Latency per reasoning step
        latency_ms = result.get('latency_ms', 0.0)
        if task_complexity.reasoning_steps > 0:
            latency_per_step = latency_ms / task_complexity.reasoning_steps
        else:
            latency_per_step = latency_ms
        
        # Overall efficiency score
        resource_usage = energy_wh / 1000 + carbon_g / 1000 + latency_ms / 1000
        efficiency = accuracy / max(resource_usage, 0.001)
        
        return Layer2NormalizedMetrics(
            energy_per_task=energy_per_task,
            carbon_per_correct_answer=carbon_per_correct,
            latency_per_reasoning_step=latency_per_step,
            efficiency_score=efficiency,
            task_complexity=complexity_score,
            complexity_tier=complexity_tier
        )
    
    def generate_layer3(self,
                       result: Dict,
                       scenario: str,
                       custom_weights: Optional[Dict[str, float]] = None) -> Layer3ScenarioScore:
        """
        Generate Layer 3: Scenario-specific score
        
        Custom weighted score for specific use case.
        
        Args:
            result: Raw result dictionary
            scenario: Scenario name ('production', 'research', 'cost_sensitive', etc.)
            custom_weights: Override scenario weights
        
        Returns:
            Layer3ScenarioScore with scenario-weighted score
        """
        # Get weights
        if custom_weights:
            weights = custom_weights
        elif scenario in self.SCENARIO_WEIGHTS:
            weights = self.SCENARIO_WEIGHTS[scenario]
        else:
            logger.warning(f"Unknown scenario '{scenario}', using 'production'")
            weights = self.SCENARIO_WEIGHTS['production']
        
        # Extract metrics
        accuracy = result.get('accuracy', 0.0)
        energy_kwh = result.get('energy_kwh', 0.0)
        carbon_kg = result.get('carbon_kg', 0.0)
        latency_ms = result.get('latency_ms', 0.0)
        
        # Normalize metrics to [0, 1] scale
        # Accuracy: already 0-1
        # Latency: inverse and cap at 5 seconds
        # Energy: inverse and cap at 0.01 kWh
        # Carbon: inverse and cap at 0.001 kg
        
        norm_accuracy = accuracy
        norm_latency = 1.0 - min(latency_ms / 5000, 1.0)
        norm_energy = 1.0 - min(energy_kwh / 0.01, 1.0)
        norm_carbon = 1.0 - min(carbon_kg / 0.001, 1.0)
        
        # Weighted sum
        weighted_score = (
            weights['accuracy'] * norm_accuracy +
            weights['latency'] * norm_latency +
            weights['energy'] * norm_energy +
            weights['carbon'] * norm_carbon
        )
        
        return Layer3ScenarioScore(
            weighted_score=weighted_score,
            scenario_name=scenario,
            weights_used=weights
        )
    
    def generate_full_report(self,
                           results: List[Dict],
                           scenario: str = 'production',
                           custom_weights: Optional[Dict[str, float]] = None) -> Dict:
        """
        Generate complete three-layer report for all results
        
        Args:
            results: List of result dictionaries
            scenario: Scenario for Layer 3 scoring
            custom_weights: Optional custom weights for Layer 3
        
        Returns:
            Comprehensive report with all layers:
            {
                'scenario': str,
                'total_agents': int,
                'reports': List[Dict],  # One per agent
                'summary': Dict
            }
        
        Example:
            results = [
                {'agent_id': 'A', 'accuracy': 0.95, 'energy_kwh': 0.003, ...},
                {'agent_id': 'B', 'accuracy': 0.90, 'energy_kwh': 0.002, ...}
            ]
            
            report = reporter.generate_full_report(results, 'production')
            
            # Access different layers
            for agent_report in report['reports']:
                print(f"Agent: {agent_report['agent_id']}")
                print(f"  Raw accuracy: {agent_report['layer1_raw']['accuracy']}")
                print(f"  Normalized energy: {agent_report['layer2_normalized']['energy_per_task']}")
                print(f"  Scenario score: {agent_report['layer3_scenario']['weighted_score']}")
        """
        reports = []
        
        for result in results:
            # Extract task complexity
            trace = result.get('trace', {})
            complexity = self.complexity_analyzer.analyze_from_trace(trace)
            
            # Generate all layers
            layer1 = self.generate_layer1(result)
            layer2 = self.generate_layer2(result, complexity)
            layer3 = self.generate_layer3(result, scenario, custom_weights)
            
            reports.append({
                'agent_id': result.get('agent_id', 'unknown'),
                'task_id': result.get('task_id', 'unknown'),
                'layer1_raw': layer1.to_dict(),
                'layer2_normalized': layer2.to_dict(),
                'layer3_scenario': layer3.to_dict(),
                'task_complexity': complexity.compute_composite_score(),
                'complexity_tier': self.complexity_analyzer.categorize_complexity(complexity)
            })
        
        # Sort by Layer 3 scores
        reports.sort(key=lambda x: x['layer3_scenario']['weighted_score'], reverse=True)
        
        # Add ranks and percentiles
        for i, report in enumerate(reports):
            report['layer3_scenario']['rank'] = i + 1
            report['layer3_scenario']['percentile'] = (len(reports) - i) / len(reports) * 100
        
        # Compute summary statistics
        summary = self._compute_summary(reports)
        
        return {
            'scenario': scenario,
            'total_agents': len(reports),
            'reports': reports,
            'summary': summary,
            'weights_used': custom_weights or self.SCENARIO_WEIGHTS.get(scenario)
        }
    
    def _compute_summary(self, reports: List[Dict]) -> Dict:
        """Compute summary statistics across all layers"""
        # Layer 1 averages
        layer1_accuracies = [r['layer1_raw']['accuracy'] for r in reports]
        layer1_energies = [r['layer1_raw']['energy_wh'] for r in reports]
        layer1_carbons = [r['layer1_raw']['carbon_co2_g'] for r in reports]
        layer1_latencies = [r['layer1_raw']['latency_ms'] for r in reports]
        
        # Layer 2 averages
        layer2_energy_per_task = [r['layer2_normalized']['energy_per_task'] for r in reports]
        layer2_efficiencies = [r['layer2_normalized']['efficiency_score'] for r in reports]
        
        # Layer 3 scores
        layer3_scores = [r['layer3_scenario']['weighted_score'] for r in reports]
        
        return {
            'layer1_avg': {
                'accuracy': np.mean(layer1_accuracies),
                'energy_wh': np.mean(layer1_energies),
                'carbon_g': np.mean(layer1_carbons),
                'latency_ms': np.mean(layer1_latencies)
            },
            'layer2_avg': {
                'energy_per_task': np.mean(layer2_energy_per_task),
                'efficiency_score': np.mean(layer2_efficiencies)
            },
            'layer3_avg': {
                'weighted_score': np.mean(layer3_scores),
                'std': np.std(layer3_scores)
            },
            'top_agent': reports[0]['agent_id'] if reports else None,
            'complexity_distribution': self._get_complexity_distribution(reports)
        }
    
    def _get_complexity_distribution(self, reports: List[Dict]) -> Dict:
        """Get distribution of task complexities"""
        tiers = [r['complexity_tier'] for r in reports]
        return {
            tier: tiers.count(tier) for tier in set(tiers)
        }
    
    def export_report(self, report: Dict, filepath: str, format: str = 'json'):
        """
        Export report to file
        
        Args:
            report: Full report dictionary
            filepath: Output file path
            format: 'json' or 'csv'
        """
        import json
        
        if format == 'json':
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Exported report to {filepath}")
        
        elif format == 'csv':
            import csv
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'agent_id', 'rank', 'weighted_score',
                    'accuracy', 'energy_wh', 'carbon_g', 'latency_ms',
                    'energy_per_task', 'efficiency_score'
                ])
                writer.writeheader()
                
                for r in report['reports']:
                    writer.writerow({
                        'agent_id': r['agent_id'],
                        'rank': r['layer3_scenario']['rank'],
                        'weighted_score': r['layer3_scenario']['weighted_score'],
                        'accuracy': r['layer1_raw']['accuracy'],
                        'energy_wh': r['layer1_raw']['energy_wh'],
                        'carbon_g': r['layer1_raw']['carbon_co2_g'],
                        'latency_ms': r['layer1_raw']['latency_ms'],
                        'energy_per_task': r['layer2_normalized']['energy_per_task'],
                        'efficiency_score': r['layer2_normalized']['efficiency_score']
                    })
            
            logger.info(f"Exported CSV report to {filepath}")
