# -*- coding: utf-8 -*-
"""
Policy Feedback Module

Merges external evaluator feedback with internal agent self-reflection
narratives to provide dual-layer feedback.
"""

from typing import Dict, Any, List, Optional


class PolicyFeedback:
    """
    Dual-layer feedback system combining objective metrics and subjective reasoning.
    
    Responsibilities:
    - Merge external Pareto analysis with internal reflections
    - Generate human-readable feedback
    - Provide interpretability for agent decisions
    - Explain trade-offs and budget violations
    """
    
    def __init__(self):
        self.feedback_history: List[Dict[str, Any]] = []
    
    def generate_dual_layer_feedback(
        self,
        pareto_analysis: Dict[str, Any],
        reflections: List[Dict[str, Any]],
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate dual-layer feedback combining external and internal perspectives.
        
        Args:
            pareto_analysis: External Pareto frontier analysis
            reflections: Internal agent reflections
            metrics: Execution metrics
            
        Returns:
            Comprehensive dual-layer feedback
        """
        feedback = {
            "timestamp": metrics.get("timestamp", 0),
            "objective_layer": self._generate_objective_feedback(pareto_analysis, metrics),
            "subjective_layer": self._generate_subjective_feedback(reflections),
            "synthesis": self._synthesize_feedback(pareto_analysis, reflections, metrics)
        }
        
        self.feedback_history.append(feedback)
        return feedback
    
    def _generate_objective_feedback(
        self,
        pareto_analysis: Dict[str, Any],
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate objective feedback from Pareto analysis."""
        return {
            "pareto_position": pareto_analysis.get("position", "unknown"),
            "dominated_by": pareto_analysis.get("dominated_by", []),
            "dominates": pareto_analysis.get("dominates", []),
            "efficiency_score": pareto_analysis.get("efficiency_score", 0.0),
            "metrics_summary": {
                "energy": metrics.get("cumulative", {}).get("total_energy_wh", 0),
                "carbon": metrics.get("cumulative", {}).get("total_carbon_kg", 0),
                "latency": metrics.get("cumulative", {}).get("total_latency_ms", 0),
                "memory": metrics.get("cumulative", {}).get("max_memory_mb", 0)
            },
            "interpretation": self._interpret_pareto_position(pareto_analysis)
        }
    
    def _interpret_pareto_position(self, pareto_analysis: Dict[str, Any]) -> str:
        """Interpret Pareto position in human-readable form."""
        position = pareto_analysis.get("position", "unknown")
        
        if position == "frontier":
            return "✅ This agent is on the Pareto frontier - optimal trade-off achieved"
        elif position == "dominated":
            dominated_by = len(pareto_analysis.get("dominated_by", []))
            return f"⚠️ This agent is dominated by {dominated_by} other agent(s) - suboptimal trade-offs"
        else:
            return "ℹ️ Pareto position unclear - insufficient comparison data"
    
    def _generate_subjective_feedback(
        self,
        reflections: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate subjective feedback from agent reflections."""
        if not reflections:
            return {
                "reflection_count": 0,
                "narrative": "No reflections available",
                "patterns": [],
                "confidence": 0.0
            }
        
        # Extract key insights from reflections
        narratives = [r.get("self_explanation", "") for r in reflections]
        decisions = [r.get("decision", "") for r in reflections]
        confidences = [r.get("confidence", 0.0) for r in reflections]
        
        return {
            "reflection_count": len(reflections),
            "narrative": self._synthesize_narrative(narratives),
            "decision_pattern": self._analyze_decision_pattern(decisions),
            "avg_confidence": sum(confidences) / len(confidences) if confidences else 0.0,
            "confidence_trend": self._analyze_confidence_trend(confidences)
        }
    
    def _synthesize_narrative(self, narratives: List[str]) -> str:
        """Synthesize multiple reflection narratives."""
        if not narratives:
            return "No narrative available"
        
        # Use the most recent narrative as primary
        primary = narratives[-1]
        
        # Add context from earlier reflections if patterns exist
        if len(narratives) > 1:
            return f"{primary} (Consistent with {len(narratives)-1} earlier reflection(s))"
        
        return primary
    
    def _analyze_decision_pattern(self, decisions: List[str]) -> Dict[str, Any]:
        """Analyze pattern in agent decisions."""
        if not decisions:
            return {"pattern": "none", "consistency": 0.0}
        
        decision_counts = {}
        for d in decisions:
            decision_counts[d] = decision_counts.get(d, 0) + 1
        
        most_common = max(decision_counts, key=decision_counts.get)
        consistency = decision_counts[most_common] / len(decisions)
        
        return {
            "pattern": most_common,
            "consistency": consistency,
            "distribution": decision_counts
        }
    
    def _analyze_confidence_trend(self, confidences: List[float]) -> str:
        """Analyze trend in confidence scores."""
        if len(confidences) < 2:
            return "insufficient_data"
        
        recent = confidences[-3:]
        if len(recent) >= 2:
            if all(recent[i] < recent[i+1] for i in range(len(recent)-1)):
                return "increasing"
            elif all(recent[i] > recent[i+1] for i in range(len(recent)-1)):
                return "decreasing"
        
        return "stable"
    
    def _synthesize_feedback(
        self,
        pareto_analysis: Dict[str, Any],
        reflections: List[Dict[str, Any]],
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Synthesize objective and subjective feedback."""
        pareto_position = pareto_analysis.get("position", "unknown")
        avg_confidence = sum(r.get("confidence", 0) for r in reflections) / len(reflections) if reflections else 0
        
        # Determine alignment
        alignment = "aligned"
        if pareto_position == "frontier" and avg_confidence > 0.8:
            alignment = "strongly_aligned"
            synthesis_text = "Agent's self-assessment aligns with objective performance - high confidence and optimal trade-offs"
        elif pareto_position == "dominated" and avg_confidence < 0.5:
            alignment = "aligned"
            synthesis_text = "Agent correctly recognizes suboptimal performance - self-awareness is accurate"
        elif pareto_position == "frontier" and avg_confidence < 0.5:
            alignment = "misaligned"
            synthesis_text = "Agent underestimates its performance - achieving optimal trade-offs despite low confidence"
        elif pareto_position == "dominated" and avg_confidence > 0.8:
            alignment = "misaligned"
            synthesis_text = "Agent overestimates its performance - high confidence despite suboptimal trade-offs"
        else:
            synthesis_text = "Mixed signals between objective performance and subjective assessment"
        
        return {
            "alignment": alignment,
            "synthesis_text": synthesis_text,
            "recommendations": self._generate_recommendations(pareto_position, avg_confidence, reflections)
        }
    
    def _generate_recommendations(
        self,
        pareto_position: str,
        avg_confidence: float,
        reflections: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        if pareto_position == "dominated":
            recommendations.append("Investigate dominated metrics and adjust strategy")
            recommendations.append("Consider trade-off adjustments to reach Pareto frontier")
        
        if avg_confidence < 0.5:
            recommendations.append("Low confidence detected - increase reflection frequency")
            recommendations.append("Review decision patterns for consistency")
        
        if reflections:
            recent_decisions = [r.get("decision", "") for r in reflections[-3:]]
            if "reduce_energy_usage" in recent_decisions:
                recommendations.append("Energy optimization in progress - monitor effectiveness")
            if "reduce_tool_calls" in recent_decisions:
                recommendations.append("Tool call reduction active - verify impact on accuracy")
        
        if not recommendations:
            recommendations.append("Continue current strategy - performance is satisfactory")
        
        return recommendations
    
    def explain(
        self,
        decision: str,
        before: Dict[str, Any],
        after: Dict[str, Any]
    ) -> str:
        """
        Explain a specific decision and its impact.
        
        Args:
            decision: Decision made by agent
            before: Metrics before decision
            after: Metrics after decision
            
        Returns:
            Human-readable explanation
        """
        explanations = {
            "reduce_tool_calls": f"Reduced tool calls from {before.get('tool_calls', 0)} to {after.get('tool_calls', 0)} to conserve energy",
            "reduce_energy_usage": f"Energy usage decreased from {before.get('energy', 0):.3f} to {after.get('energy', 0):.3f} Wh",
            "optimize_speed": f"Latency improved from {before.get('latency', 0):.1f} to {after.get('latency', 0):.1f} ms",
            "continue": "Maintaining current strategy as metrics are within acceptable ranges"
        }
        
        return explanations.get(decision, f"Applied decision: {decision}")
    
    def get_feedback_summary(self) -> Dict[str, Any]:
        """Get summary of all feedback provided."""
        if not self.feedback_history:
            return {"total_feedback": 0}
        
        alignments = [f.get("synthesis", {}).get("alignment", "unknown") for f in self.feedback_history]
        alignment_counts = {}
        for a in alignments:
            alignment_counts[a] = alignment_counts.get(a, 0) + 1
        
        return {
            "total_feedback": len(self.feedback_history),
            "alignment_distribution": alignment_counts,
            "latest_synthesis": self.feedback_history[-1].get("synthesis", {}).get("synthesis_text", "")
        }
