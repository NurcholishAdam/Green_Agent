# -*- coding: utf-8 -*-
"""
Green Dashboard Module

Visualization for reflective insights, Pareto positions, and interpretability.
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime


class GreenDashboard:
    """
    Dashboard for visualizing meta-cognitive insights and sustainability metrics.
    
    Responsibilities:
    - Visualize Pareto positions with reasoning
    - Display reflective insights
    - Compare agents on interpretability and efficiency
    - Generate leaderboard with meta-cognitive scores
    """
    
    def __init__(self):
        self.dashboard_data = {
            "agents": [],
            "reflections": [],
            "pareto_analysis": {},
            "interpretability_scores": {}
        }
    
    def add_agent_data(
        self,
        agent_id: str,
        metrics: Dict[str, Any],
        reflections: List[Dict[str, Any]],
        pareto_position: Dict[str, Any]
    ):
        """
        Add agent data to dashboard.
        
        Args:
            agent_id: Agent identifier
            metrics: Agent metrics
            reflections: Agent reflections
            pareto_position: Pareto analysis results
        """
        interpretability_score = self._calculate_interpretability_score(reflections)
        
        agent_entry = {
            "agent_id": agent_id,
            "metrics": metrics,
            "reflection_count": len(reflections),
            "pareto_position": pareto_position.get("position", "unknown"),
            "interpretability_score": interpretability_score,
            "reasoning_path": self._extract_reasoning_path(reflections),
            "timestamp": datetime.now().isoformat()
        }
        
        self.dashboard_data["agents"].append(agent_entry)
        self.dashboard_data["reflections"].extend(reflections)
    
    def _calculate_interpretability_score(self, reflections: List[Dict[str, Any]]) -> float:
        """
        Calculate interpretability score based on reflection quality.
        
        Returns:
            Score between 0.0 and 1.0
        """
        if not reflections:
            return 0.0
        
        score = 0.0
        
        # Factor 1: Reflection frequency (more reflections = better interpretability)
        reflection_score = min(len(reflections) / 10.0, 1.0) * 0.3
        score += reflection_score
        
        # Factor 2: Average confidence (higher confidence = clearer reasoning)
        avg_confidence = sum(r.get("confidence", 0) for r in reflections) / len(reflections)
        score += avg_confidence * 0.3
        
        # Factor 3: Decision consistency (consistent decisions = clearer strategy)
        decisions = [r.get("decision", "") for r in reflections]
        if decisions:
            most_common_count = max(decisions.count(d) for d in set(decisions))
            consistency = most_common_count / len(decisions)
            score += consistency * 0.2
        
        # Factor 4: Explanation quality (longer explanations = more detail)
        avg_explanation_length = sum(
            len(r.get("self_explanation", "")) for r in reflections
        ) / len(reflections)
        explanation_score = min(avg_explanation_length / 200.0, 1.0) * 0.2
        score += explanation_score
        
        return min(score, 1.0)
    
    def _extract_reasoning_path(self, reflections: List[Dict[str, Any]]) -> List[str]:
        """Extract key reasoning steps from reflections."""
        path = []
        for r in reflections:
            step = f"Step {r.get('step', 0)}: {r.get('decision', 'unknown')} - {r.get('self_explanation', '')[:100]}"
            path.append(step)
        return path
    
    def generate_leaderboard(self) -> Dict[str, Any]:
        """
        Generate leaderboard comparing agents on multiple dimensions.
        
        Returns:
            Leaderboard data with rankings
        """
        if not self.dashboard_data["agents"]:
            return {"agents": [], "rankings": {}}
        
        agents = self.dashboard_data["agents"]
        
        # Rank by efficiency (Pareto position)
        efficiency_ranking = sorted(
            agents,
            key=lambda a: (
                1 if a["pareto_position"] == "frontier" else 0,
                -a["metrics"].get("cumulative", {}).get("total_energy_wh", float('inf'))
            ),
            reverse=True
        )
        
        # Rank by interpretability
        interpretability_ranking = sorted(
            agents,
            key=lambda a: a["interpretability_score"],
            reverse=True
        )
        
        # Rank by sustainability (energy + carbon)
        sustainability_ranking = sorted(
            agents,
            key=lambda a: (
                a["metrics"].get("cumulative", {}).get("total_energy_wh", float('inf')) +
                a["metrics"].get("cumulative", {}).get("total_carbon_kg", float('inf')) * 1000
            )
        )
        
        return {
            "agents": agents,
            "rankings": {
                "efficiency": [a["agent_id"] for a in efficiency_ranking],
                "interpretability": [a["agent_id"] for a in interpretability_ranking],
                "sustainability": [a["agent_id"] for a in sustainability_ranking]
            },
            "top_performers": {
                "most_efficient": efficiency_ranking[0]["agent_id"] if efficiency_ranking else None,
                "most_interpretable": interpretability_ranking[0]["agent_id"] if interpretability_ranking else None,
                "most_sustainable": sustainability_ranking[0]["agent_id"] if sustainability_ranking else None
            }
        }
    
    def generate_comparison_view(self) -> Dict[str, Any]:
        """
        Generate comparison view showing why agents chose their paths.
        
        Returns:
            Comparison data with reasoning
        """
        comparisons = []
        
        for agent in self.dashboard_data["agents"]:
            comparison = {
                "agent_id": agent["agent_id"],
                "pareto_position": agent["pareto_position"],
                "reasoning_summary": self._summarize_reasoning(agent["reasoning_path"]),
                "key_decisions": self._extract_key_decisions(agent["reasoning_path"]),
                "interpretability": agent["interpretability_score"],
                "metrics_summary": {
                    "energy": agent["metrics"].get("cumulative", {}).get("total_energy_wh", 0),
                    "carbon": agent["metrics"].get("cumulative", {}).get("total_carbon_kg", 0),
                    "latency": agent["metrics"].get("cumulative", {}).get("total_latency_ms", 0)
                }
            }
            comparisons.append(comparison)
        
        return {
            "comparisons": comparisons,
            "insights": self._generate_comparative_insights(comparisons)
        }
    
    def _summarize_reasoning(self, reasoning_path: List[str]) -> str:
        """Summarize reasoning path."""
        if not reasoning_path:
            return "No reasoning available"
        
        key_steps = reasoning_path[:3] if len(reasoning_path) > 3 else reasoning_path
        return " → ".join([step.split(":")[1].split("-")[0].strip() for step in key_steps])
    
    def _extract_key_decisions(self, reasoning_path: List[str]) -> List[str]:
        """Extract key decisions from reasoning path."""
        decisions = []
        for step in reasoning_path:
            if "reduce" in step.lower() or "optimize" in step.lower() or "continue" in step.lower():
                decisions.append(step.split(":")[1].split("-")[0].strip())
        return decisions[:5]  # Top 5 decisions
    
    def _generate_comparative_insights(self, comparisons: List[Dict[str, Any]]) -> List[str]:
        """Generate insights from agent comparisons."""
        insights = []
        
        # Insight 1: Pareto frontier agents
        frontier_agents = [c for c in comparisons if c["pareto_position"] == "frontier"]
        if frontier_agents:
            insights.append(
                f"{len(frontier_agents)} agent(s) achieved Pareto-optimal trade-offs"
            )
        
        # Insight 2: Interpretability leaders
        high_interpretability = [c for c in comparisons if c["interpretability"] > 0.7]
        if high_interpretability:
            insights.append(
                f"{len(high_interpretability)} agent(s) demonstrated high interpretability (>0.7)"
            )
        
        # Insight 3: Energy efficiency
        energy_values = [c["metrics_summary"]["energy"] for c in comparisons]
        if energy_values:
            avg_energy = sum(energy_values) / len(energy_values)
            efficient_agents = [c for c in comparisons if c["metrics_summary"]["energy"] < avg_energy * 0.8]
            if efficient_agents:
                insights.append(
                    f"{len(efficient_agents)} agent(s) achieved 20% better energy efficiency than average"
                )
        
        return insights
    
    def export_dashboard(self, filepath: str):
        """Export dashboard data to JSON file."""
        leaderboard = self.generate_leaderboard()
        comparison = self.generate_comparison_view()
        
        export_data = {
            "generated_at": datetime.now().isoformat(),
            "leaderboard": leaderboard,
            "comparison_view": comparison,
            "raw_data": self.dashboard_data
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
    
    def generate_html_report(self, filepath: str):
        """Generate HTML dashboard report."""
        leaderboard = self.generate_leaderboard()
        comparison = self.generate_comparison_view()
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Green Agent Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #2ecc71; color: white; padding: 20px; }}
        .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; }}
        .agent-card {{ background: #f8f9fa; padding: 10px; margin: 10px 0; }}
        .metric {{ display: inline-block; margin: 5px 10px; }}
        .frontier {{ color: #27ae60; font-weight: bold; }}
        .dominated {{ color: #e74c3c; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🌱 Green Agent Meta-Cognitive Dashboard</h1>
        <p>Sustainability + Interpretability Analysis</p>
    </div>
    
    <div class="section">
        <h2>🏆 Leaderboard</h2>
        <h3>Top Performers</h3>
        <p><strong>Most Efficient:</strong> {leaderboard['top_performers']['most_efficient']}</p>
        <p><strong>Most Interpretable:</strong> {leaderboard['top_performers']['most_interpretable']}</p>
        <p><strong>Most Sustainable:</strong> {leaderboard['top_performers']['most_sustainable']}</p>
    </div>
    
    <div class="section">
        <h2>📊 Agent Comparisons</h2>
        {''.join([f'''
        <div class="agent-card">
            <h3>{c['agent_id']} <span class="{c['pareto_position']}">[{c['pareto_position']}]</span></h3>
            <p><strong>Reasoning:</strong> {c['reasoning_summary']}</p>
            <p><strong>Interpretability:</strong> {c['interpretability']:.2f}</p>
            <div class="metric">Energy: {c['metrics_summary']['energy']:.3f} Wh</div>
            <div class="metric">Carbon: {c['metrics_summary']['carbon']:.6f} kg</div>
            <div class="metric">Latency: {c['metrics_summary']['latency']:.1f} ms</div>
        </div>
        ''' for c in comparison['comparisons']])}
    </div>
    
    <div class="section">
        <h2>💡 Insights</h2>
        <ul>
            {''.join([f'<li>{insight}</li>' for insight in comparison['insights']])}
        </ul>
    </div>
</body>
</html>
        """
        
        with open(filepath, 'w') as f:
            f.write(html)
