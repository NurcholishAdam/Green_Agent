"""
Report Generator for Green_Agent

Generates formatted reports for different audiences:
- Executive summaries (business focus)
- Technical reports (detailed metrics)
- Research reports (methodology focus)
"""

from typing import Dict, Optional
import logging

from .layered_reporter import LayeredReporter

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Generate formatted reports for different audiences
    
    Transforms raw three-layer reports into human-readable formats
    tailored to specific stakeholders.
    """
    
    def __init__(self):
        """Initialize report generator"""
        self.reporter = LayeredReporter()
        logger.info("Initialized ReportGenerator")
    
    def generate_executive_summary(self, full_report: Dict) -> str:
        """
        Executive summary focusing on Layer 3 (business metrics)
        
        Targets: CTOs, Product Managers, Business Leaders
        Focus: High-level outcomes, ROI, deployment recommendations
        
        Args:
            full_report: Full three-layer report
        
        Returns:
            Formatted executive summary string
        """
        scenario = full_report['scenario']
        total = full_report['total_agents']
        top_agent = full_report['summary']['top_agent']
        
        summary = f"""
{'='*70}
EXECUTIVE SUMMARY - {scenario.upper()} SCENARIO
{'='*70}

OVERVIEW
--------
Evaluation Date: {full_report['reports'][0]['layer1_raw']['timestamp'][:10]}
Scenario: {scenario}
Agents Evaluated: {total}
Top Performer: {top_agent}

KEY FINDINGS
------------
"""
        
        # Layer 1 (Raw Performance)
        l1 = full_report['summary']['layer1_avg']
        summary += f"""
Average Performance Metrics:
  • Accuracy: {l1['accuracy']:.1%}
  • Energy Consumption: {l1['energy_wh']:.2f} Wh per task
  • Carbon Footprint: {l1['carbon_g']:.2f} g CO₂ per task
  • Response Time: {l1['latency_ms']:.0f} ms
"""
        
        # Layer 3 (Business Value)
        l3_score = full_report['summary']['layer3_avg']['weighted_score']
        summary += f"""
Composite Score (weighted for {scenario}): {l3_score:.2f} / 1.00

"""
        
        # Top 3 Agents
        summary += "TOP 3 RECOMMENDED AGENTS\n"
        summary += "-" * 70 + "\n"
        
        for i in range(min(3, len(full_report['reports']))):
            agent = full_report['reports'][i]
            summary += f"""
#{i+1}. {agent['agent_id']}
   Scenario Score: {agent['layer3_scenario']['weighted_score']:.3f}
   Accuracy: {agent['layer1_raw']['accuracy']:.1%}
   Energy: {agent['layer1_raw']['energy_wh']:.2f} Wh
   Latency: {agent['layer1_raw']['latency_ms']:.0f} ms
   
"""
        
        # Deployment Recommendation
        summary += "\nDEPLOYMENT RECOMMENDATION\n"
        summary += "-" * 70 + "\n"
        
        top_report = full_report['reports'][0]
        if scenario == 'production':
            summary += f"""Deploy {top_report['agent_id']} for production workloads.
This agent offers the best balance of accuracy and operational efficiency.

Estimated Operational Costs (per 1M tasks):
  • Energy: ~{l1['energy_wh'] * 1000:.0f} kWh
  • Carbon: ~{l1['carbon_g'] * 1000:.0f} kg CO₂
  • Latency: ~{l1['latency_ms'] * 1000:.0f} seconds total
"""
        elif scenario == 'eco_sensitive':
            summary += f"""Deploy {top_report['agent_id']} for environmentally-conscious deployment.
This agent minimizes environmental impact while maintaining acceptable performance.

Environmental Benefits (vs. average):
  • {((1 - top_report['layer1_raw']['energy_wh'] / l1['energy_wh']) * 100):.0f}% less energy
  • {((1 - top_report['layer1_raw']['carbon_co2_g'] / l1['carbon_g']) * 100):.0f}% less carbon
"""
        elif scenario == 'real_time':
            summary += f"""Deploy {top_report['agent_id']} for real-time applications.
This agent delivers the fastest response times.

Latency Performance:
  • Average: {top_report['layer1_raw']['latency_ms']:.0f} ms
  • 95th percentile: <{top_report['layer1_raw']['latency_ms'] * 1.5:.0f} ms (estimated)
"""
        
        summary += "\n" + "="*70 + "\n"
        
        return summary
    
    def generate_technical_report(self, full_report: Dict) -> str:
        """
        Technical report with all three layers
        
        Targets: ML Engineers, DevOps, Technical Leads
        Focus: Detailed metrics, normalization, complexity analysis
        
        Args:
            full_report: Full three-layer report
        
        Returns:
            Formatted technical report string
        """
        report = f"""
{'='*70}
TECHNICAL EVALUATION REPORT
{'='*70}

METHODOLOGY
-----------
This report uses three-layer transparent reporting:

Layer 1 (Raw Metrics): Unprocessed ground truth
Layer 2 (Normalized): Adjusted for task complexity
Layer 3 (Scenario): Weighted for {full_report['scenario']} use case

Scenario Weights:
"""
        
        weights = full_report['weights_used']
        for metric, weight in weights.items():
            report += f"  • {metric}: {weight:.1%}\n"
        
        report += f"""
Total Agents Evaluated: {full_report['total_agents']}

DETAILED RESULTS
----------------
"""
        
        # Show top 5 agents with all layers
        for i, agent_report in enumerate(full_report['reports'][:5]):
            report += f"\n{'─'*70}\n"
            report += f"RANK #{i+1}: {agent_report['agent_id']}\n"
            report += f"{'─'*70}\n"
            
            # Layer 1
            l1 = agent_report['layer1_raw']
            report += f"""
Layer 1 (Raw Metrics):
  Accuracy: {l1['accuracy']:.2%}
  Energy: {l1['energy_wh']:.4f} Wh
  Carbon: {l1['carbon_co2_g']:.2f} g CO₂
  Latency: {l1['latency_ms']:.0f} ms
"""
            
            # Layer 2
            l2 = agent_report['layer2_normalized']
            report += f"""
Layer 2 (Normalized by Complexity):
  Task Complexity: {agent_report['task_complexity']:.2f} ({agent_report['complexity_tier']})
  Energy/Task: {l2['energy_per_task']:.6f}
  Carbon/Correct Answer: {l2['carbon_per_correct_answer']:.4f} g
  Latency/Reasoning Step: {l2['latency_per_reasoning_step']:.2f} ms
  Efficiency Score: {l2['efficiency_score']:.4f}
"""
            
            # Layer 3
            l3 = agent_report['layer3_scenario']
            report += f"""
Layer 3 (Scenario Score):
  Weighted Score: {l3['weighted_score']:.4f}
  Percentile: {l3['percentile']:.1f}th
  Rank: #{l3['rank']}
"""
        
        # Summary Statistics
        report += f"\n{'='*70}\n"
        report += "SUMMARY STATISTICS\n"
        report += f"{'='*70}\n"
        
        summary = full_report['summary']
        
        report += f"""
Layer 1 Averages (Raw):
  Accuracy: {summary['layer1_avg']['accuracy']:.2%}
  Energy: {summary['layer1_avg']['energy_wh']:.4f} Wh
  Carbon: {summary['layer1_avg']['carbon_g']:.2f} g
  Latency: {summary['layer1_avg']['latency_ms']:.0f} ms

Layer 2 Averages (Normalized):
  Energy/Task: {summary['layer2_avg']['energy_per_task']:.6f}
  Efficiency: {summary['layer2_avg']['efficiency_score']:.4f}

Layer 3 Statistics:
  Mean Score: {summary['layer3_avg']['weighted_score']:.4f}
  Std Dev: {summary['layer3_avg']['std']:.4f}

Task Complexity Distribution:
"""
        
        for tier, count in summary['complexity_distribution'].items():
            report += f"  {tier}: {count} agents\n"
        
        report += "\n" + "="*70 + "\n"
        
        return report
    
    def generate_research_report(self, full_report: Dict) -> str:
        """
        Research report focusing on methodology and reproducibility
        
        Targets: Researchers, Academics, Peer Reviewers
        Focus: Methodology, statistical rigor, reproducibility
        
        Args:
            full_report: Full three-layer report
        
        Returns:
            Formatted research report string
        """
        report = f"""
{'='*70}
RESEARCH EVALUATION REPORT
{'='*70}

ABSTRACT
--------
This report presents a three-layer evaluation methodology for AI agent
benchmarking that addresses common pitfalls in performance reporting:

1. Layer 1 maintains raw metrics as ground truth
2. Layer 2 normalizes for task complexity to enable fair comparison
3. Layer 3 provides scenario-specific scoring for deployment contexts

Total agents evaluated: {full_report['total_agents']}
Evaluation scenario: {full_report['scenario']}

METHODOLOGY
-----------

Layer 1: Raw Metrics Collection
  - Accuracy (task success rate)
  - Energy consumption (Wh)
  - Carbon emissions (g CO₂e)
  - Latency (ms)
  
  No transformations applied. This layer serves as ground truth.

Layer 2: Complexity Normalization
  Task complexity computed from:
    - Prompt length (tokens)
    - Reasoning steps (count)
    - Tool calls (count)
    - Wall-clock time (ms)
    - Context size (tokens)
  
  Composite complexity score: weighted logarithmic combination
  
  Normalized metrics:
    - Energy/Task = Energy / Complexity
    - Carbon/Correct = Carbon / Accuracy
    - Latency/Step = Latency / Reasoning Steps

Layer 3: Scenario-Specific Scoring
  Weights for '{full_report['scenario']}' scenario:
"""
        
        weights = full_report['weights_used']
        for metric, weight in weights.items():
            report += f"    {metric}: {weight:.3f}\n"
        
        report += """
  Score = Σ(normalized_metric_i × weight_i)

RESULTS
-------

Statistical Summary (Layer 1 Raw Metrics):
"""
        
        l1_avg = full_report['summary']['layer1_avg']
        report += f"""
  Accuracy: μ={l1_avg['accuracy']:.4f}
  Energy: μ={l1_avg['energy_wh']:.4f} Wh
  Carbon: μ={l1_avg['carbon_g']:.4f} g CO₂
  Latency: μ={l1_avg['latency_ms']:.2f} ms

Normalized Performance (Layer 2):
  Energy Efficiency: μ={full_report['summary']['layer2_avg']['energy_per_task']:.6f}
  Overall Efficiency: μ={full_report['summary']['layer2_avg']['efficiency_score']:.6f}

Scenario Scores (Layer 3):
  Mean: {full_report['summary']['layer3_avg']['weighted_score']:.4f}
  Std Dev: {full_report['summary']['layer3_avg']['std']:.4f}
  Range: {full_report['summary']['layer3_avg']['weighted_score'] - full_report['summary']['layer3_avg']['std']:.4f} - {full_report['summary']['layer3_avg']['weighted_score'] + full_report['summary']['layer3_avg']['std']:.4f}

Top Performer: {full_report['summary']['top_agent']}
  L1 Accuracy: {full_report['reports'][0]['layer1_raw']['accuracy']:.4f}
  L2 Efficiency: {full_report['reports'][0]['layer2_normalized']['efficiency_score']:.4f}
  L3 Score: {full_report['reports'][0]['layer3_scenario']['weighted_score']:.4f}

DISCUSSION
----------

Task Complexity Distribution:
"""
        
        for tier, count in full_report['summary']['complexity_distribution'].items():
            pct = count / full_report['total_agents'] * 100
            report += f"  {tier}: {count} ({pct:.1f}%)\n"
        
        report += f"""
The three-layer approach reveals insights not visible in single-metric
evaluations:

1. Raw metrics (Layer 1) show absolute performance
2. Normalized metrics (Layer 2) enable fair cross-complexity comparison
3. Scenario scores (Layer 3) contextualize for deployment

This methodology prevents common reporting pitfalls:
  - Cherry-picking favorable metrics
  - Hiding complexity differences
  - Over-generalizing scenario-specific results

REPRODUCIBILITY
---------------

All metrics traceable from Layer 1 (raw) → Layer 2 (normalized) → Layer 3 (scenario).

Complexity normalization weights:
  prompt_length: 0.2, reasoning_steps: 0.3, tool_calls: 0.2,
  wall_clock: 0.2, context_size: 0.1

Scenario weights: See Layer 3 methodology above.

Data available in structured format for verification.

{'='*70}
"""
        
        return report
    
    def generate_comparison_report(self,
                                   report1: Dict,
                                   report2: Dict,
                                   comparison_label: str = "Comparison") -> str:
        """
        Generate comparative report between two evaluations
        
        Useful for:
        - Before/after optimization
        - Different scenarios on same agents
        - Same scenario on different agent versions
        
        Args:
            report1: First full report
            report2: Second full report
            comparison_label: Label for comparison
        
        Returns:
            Formatted comparison report
        """
        report = f"""
{'='*70}
{comparison_label.upper()}
{'='*70}

Report 1: {report1['scenario']} ({report1['total_agents']} agents)
Report 2: {report2['scenario']} ({report2['total_agents']} agents)

METRIC COMPARISON
-----------------

Layer 1 (Raw Metrics):
"""
        
        l1_a = report1['summary']['layer1_avg']
        l1_b = report2['summary']['layer1_avg']
        
        for metric in ['accuracy', 'energy_wh', 'carbon_g', 'latency_ms']:
            val_a = l1_a[metric]
            val_b = l1_b[metric]
            diff = val_b - val_a
            pct = (diff / val_a * 100) if val_a != 0 else 0
            
            direction = "↑" if diff > 0 else "↓" if diff < 0 else "→"
            report += f"  {metric}: {val_a:.4f} vs {val_b:.4f} ({direction} {abs(pct):.1f}%)\n"
        
        report += "\nLayer 3 (Scenario Scores):\n"
        score_a = report1['summary']['layer3_avg']['weighted_score']
        score_b = report2['summary']['layer3_avg']['weighted_score']
        diff = score_b - score_a
        
        report += f"  Report 1: {score_a:.4f}\n"
        report += f"  Report 2: {score_b:.4f}\n"
        report += f"  Difference: {diff:+.4f} ({diff/score_a*100:+.1f}%)\n"
        
        report += "\n" + "="*70 + "\n"
        
        return report
    
    def save_report(self, report_text: str, filepath: str):
        """Save report to file"""
        with open(filepath, 'w') as f:
            f.write(report_text)
        logger.info(f"Saved report to {filepath}")
