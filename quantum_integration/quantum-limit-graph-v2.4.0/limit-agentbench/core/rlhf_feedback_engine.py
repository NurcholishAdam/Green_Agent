# -*- coding: utf-8 -*-
"""
RLHF Feedback Engine - Reasoning Trace Analysis
Generates detailed feedback for agent improvement based on RLHF research
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json
import re


class ReasoningQuality(Enum):
    """Reasoning quality assessment levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"


class FeedbackCategory(Enum):
    """Categories of improvement feedback"""
    REASONING = "reasoning_quality"
    EFFICIENCY = "efficiency"
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    SUSTAINABILITY = "sustainability"


@dataclass
class ReasoningStep:
    """Individual reasoning step in agent trace"""
    step_id: int
    action: str
    thought: str
    observation: Optional[str] = None
    tool_used: Optional[str] = None
    duration: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_id": self.step_id,
            "action": self.action,
            "thought": self.thought,
            "observation": self.observation,
            "tool_used": self.tool_used,
            "duration": self.duration
        }


@dataclass
class FeedbackItem:
    """Individual feedback item"""
    category: FeedbackCategory
    severity: str  # "critical", "major", "minor"
    message: str
    suggestion: str
    affected_steps: Optional[List[int]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category.value,
            "severity": self.severity,
            "message": self.message,
            "suggestion": self.suggestion,
            "affected_steps": self.affected_steps
        }


class RLHFFeedbackEngine:
    """
    RLHF-based Feedback Engine for Agent Improvement
    
    Analyzes agent reasoning traces and generates actionable feedback:
    - Evaluates reasoning quality
    - Identifies inefficiencies
    - Suggests improvements
    - Provides comparative analysis
    """
    
    def __init__(self):
        self.feedback_history: List[Dict[str, Any]] = []
        self.baseline_metrics: Optional[Dict[str, float]] = None
        
    def analyze_reasoning_trace(
        self,
        reasoning_trace: List[Dict[str, Any]],
        task_type: str,
        execution_time: float,
        success: bool
    ) -> Dict[str, Any]:
        """
        Analyze agent reasoning trace and generate feedback
        
        Args:
            reasoning_trace: List of reasoning steps
            task_type: Type of task executed
            execution_time: Total execution time
            success: Whether task succeeded
            
        Returns:
            Comprehensive feedback analysis
        """
        # Parse reasoning steps
        steps = self._parse_reasoning_steps(reasoning_trace)
        
        # Analyze different aspects
        reasoning_quality = self._assess_reasoning_quality(steps, success)
        efficiency_analysis = self._analyze_efficiency(steps, execution_time)
        completeness_check = self._check_completeness(steps, task_type)
        
        # Generate feedback items
        feedback_items = []
        feedback_items.extend(self._generate_reasoning_feedback(reasoning_quality, steps))
        feedback_items.extend(self._generate_efficiency_feedback(efficiency_analysis))
        feedback_items.extend(self._generate_completeness_feedback(completeness_check))
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(
            reasoning_quality,
            efficiency_analysis,
            completeness_check,
            success
        )
        
        # Generate improvement suggestions
        suggestions = self._generate_improvement_suggestions(
            feedback_items,
            reasoning_quality,
            efficiency_analysis
        )
        
        feedback = {
            "overall_score": overall_score,
            "reasoning_quality": reasoning_quality["level"].value,
            "reasoning_score": reasoning_quality["score"],
            "efficiency_score": efficiency_analysis["score"],
            "completeness_score": completeness_check["score"],
            "feedback_items": [item.to_dict() for item in feedback_items],
            "improvement_suggestions": suggestions,
            "step_analysis": [step.to_dict() for step in steps],
            "metrics": {
                "total_steps": len(steps),
                "avg_step_duration": efficiency_analysis["avg_step_duration"],
                "redundant_steps": efficiency_analysis["redundant_steps"],
                "tool_usage_efficiency": efficiency_analysis["tool_efficiency"]
            }
        }
        
        # Store for historical comparison
        self.feedback_history.append({
            "timestamp": execution_time,
            "task_type": task_type,
            "score": overall_score,
            "success": success
        })
        
        return feedback
    
    def _parse_reasoning_steps(
        self,
        trace: List[Dict[str, Any]]
    ) -> List[ReasoningStep]:
        """Parse raw trace into structured reasoning steps"""
        steps = []
        for i, step_data in enumerate(trace):
            step = ReasoningStep(
                step_id=i,
                action=step_data.get('action', 'unknown'),
                thought=step_data.get('thought', ''),
                observation=step_data.get('observation'),
                tool_used=step_data.get('tool'),
                duration=step_data.get('duration')
            )
            steps.append(step)
        return steps
    
    def _assess_reasoning_quality(
        self,
        steps: List[ReasoningStep],
        success: bool
    ) -> Dict[str, Any]:
        """Assess quality of reasoning process"""
        score = 0.0
        issues = []
        
        # Check for logical progression
        has_clear_plan = any('plan' in step.thought.lower() for step in steps[:3])
        if has_clear_plan:
            score += 0.2
        else:
            issues.append("No clear planning phase detected")
        
        # Check for iterative refinement
        has_refinement = any(
            'refine' in step.thought.lower() or 'improve' in step.thought.lower()
            for step in steps
        )
        if has_refinement:
            score += 0.15
        
        # Check for error handling
        has_error_handling = any(
            'error' in step.thought.lower() or 'retry' in step.thought.lower()
            for step in steps
        )
        if has_error_handling:
            score += 0.15
        
        # Check thought quality (non-empty, meaningful)
        meaningful_thoughts = sum(
            1 for step in steps 
            if step.thought and len(step.thought.split()) > 3
        )
        thought_quality = meaningful_thoughts / max(len(steps), 1)
        score += thought_quality * 0.3
        
        # Success bonus
        if success:
            score += 0.2
        
        # Determine quality level
        if score >= 0.8:
            level = ReasoningQuality.EXCELLENT
        elif score >= 0.6:
            level = ReasoningQuality.GOOD
        elif score >= 0.4:
            level = ReasoningQuality.FAIR
        else:
            level = ReasoningQuality.POOR
        
        return {
            "score": min(score, 1.0),
            "level": level,
            "issues": issues,
            "has_planning": has_clear_plan,
            "has_refinement": has_refinement,
            "has_error_handling": has_error_handling
        }
    
    def _analyze_efficiency(
        self,
        steps: List[ReasoningStep],
        total_time: float
    ) -> Dict[str, Any]:
        """Analyze execution efficiency"""
        # Calculate average step duration
        step_durations = [s.duration for s in steps if s.duration is not None]
        avg_duration = sum(step_durations) / len(step_durations) if step_durations else 0
        
        # Detect redundant steps (similar actions)
        actions = [s.action for s in steps]
        redundant = len(actions) - len(set(actions))
        
        # Tool usage efficiency
        tool_steps = [s for s in steps if s.tool_used]
        tool_efficiency = len(tool_steps) / max(len(steps), 1)
        
        # Calculate efficiency score
        redundancy_penalty = redundant / max(len(steps), 1)
        efficiency_score = max(0, 1.0 - redundancy_penalty) * tool_efficiency
        
        return {
            "score": efficiency_score,
            "avg_step_duration": avg_duration,
            "redundant_steps": redundant,
            "tool_efficiency": tool_efficiency,
            "total_steps": len(steps)
        }
    
    def _check_completeness(
        self,
        steps: List[ReasoningStep],
        task_type: str
    ) -> Dict[str, Any]:
        """Check if reasoning process is complete"""
        score = 0.0
        missing_elements = []
        
        # Check for information gathering
        has_search = any(
            'search' in step.action.lower() or 'retrieve' in step.action.lower()
            for step in steps
        )
        if has_search:
            score += 0.3
        else:
            missing_elements.append("Information gathering phase")
        
        # Check for analysis
        has_analysis = any(
            'analyze' in step.thought.lower() or 'evaluate' in step.thought.lower()
            for step in steps
        )
        if has_analysis:
            score += 0.3
        else:
            missing_elements.append("Analysis phase")
        
        # Check for synthesis/conclusion
        has_conclusion = any(
            'conclude' in step.thought.lower() or 'synthesize' in step.thought.lower()
            for step in steps[-3:]  # Check last 3 steps
        )
        if has_conclusion:
            score += 0.4
        else:
            missing_elements.append("Synthesis/conclusion phase")
        
        return {
            "score": score,
            "missing_elements": missing_elements,
            "has_search": has_search,
            "has_analysis": has_analysis,
            "has_conclusion": has_conclusion
        }
    
    def _generate_reasoning_feedback(
        self,
        quality: Dict[str, Any],
        steps: List[ReasoningStep]
    ) -> List[FeedbackItem]:
        """Generate feedback items for reasoning quality"""
        feedback = []
        
        if not quality["has_planning"]:
            feedback.append(FeedbackItem(
                category=FeedbackCategory.REASONING,
                severity="major",
                message="No clear planning phase detected in reasoning trace",
                suggestion="Start with explicit planning: break down the task, "
                          "identify required information, and outline approach"
            ))
        
        if not quality["has_error_handling"]:
            feedback.append(FeedbackItem(
                category=FeedbackCategory.REASONING,
                severity="minor",
                message="Limited error handling observed",
                suggestion="Add explicit error checking and recovery strategies"
            ))
        
        if quality["level"] == ReasoningQuality.POOR:
            feedback.append(FeedbackItem(
                category=FeedbackCategory.REASONING,
                severity="critical",
                message="Overall reasoning quality is poor",
                suggestion="Focus on: 1) Clear problem decomposition, "
                          "2) Explicit intermediate goals, 3) Verification steps"
            ))
        
        return feedback
    
    def _generate_efficiency_feedback(
        self,
        efficiency: Dict[str, Any]
    ) -> List[FeedbackItem]:
        """Generate feedback items for efficiency"""
        feedback = []
        
        if efficiency["redundant_steps"] > 2:
            feedback.append(FeedbackItem(
                category=FeedbackCategory.EFFICIENCY,
                severity="major",
                message=f"Detected {efficiency['redundant_steps']} redundant steps",
                suggestion="Cache intermediate results and avoid repeating similar actions"
            ))
        
        if efficiency["tool_efficiency"] < 0.3:
            feedback.append(FeedbackItem(
                category=FeedbackCategory.EFFICIENCY,
                severity="minor",
                message="Low tool utilization detected",
                suggestion="Leverage available tools more effectively for information gathering"
            ))
        
        return feedback
    
    def _generate_completeness_feedback(
        self,
        completeness: Dict[str, Any]
    ) -> List[FeedbackItem]:
        """Generate feedback items for completeness"""
        feedback = []
        
        for missing in completeness["missing_elements"]:
            feedback.append(FeedbackItem(
                category=FeedbackCategory.COMPLETENESS,
                severity="major",
                message=f"Missing: {missing}",
                suggestion=f"Add explicit {missing.lower()} to reasoning process"
            ))
        
        return feedback
    
    def _calculate_overall_score(
        self,
        reasoning: Dict[str, Any],
        efficiency: Dict[str, Any],
        completeness: Dict[str, Any],
        success: bool
    ) -> float:
        """Calculate weighted overall score"""
        weights = {
            "reasoning": 0.4,
            "efficiency": 0.2,
            "completeness": 0.3,
            "success": 0.1
        }
        
        score = (
            reasoning["score"] * weights["reasoning"] +
            efficiency["score"] * weights["efficiency"] +
            completeness["score"] * weights["completeness"] +
            (1.0 if success else 0.0) * weights["success"]
        )
        
        return round(score, 3)
    
    def _generate_improvement_suggestions(
        self,
        feedback_items: List[FeedbackItem],
        reasoning: Dict[str, Any],
        efficiency: Dict[str, Any]
    ) -> List[str]:
        """Generate prioritized improvement suggestions"""
        suggestions = []
        
        # Critical issues first
        critical = [f for f in feedback_items if f.severity == "critical"]
        if critical:
            suggestions.append(f"CRITICAL: {critical[0].suggestion}")
        
        # Top 3 actionable suggestions
        major = [f for f in feedback_items if f.severity == "major"]
        for item in major[:3]:
            suggestions.append(f"{item.category.value.upper()}: {item.suggestion}")
        
        # Add comparative suggestion if we have history
        if len(self.feedback_history) > 5:
            avg_score = sum(h["score"] for h in self.feedback_history[-5:]) / 5
            current_score = reasoning["score"]
            if current_score < avg_score:
                suggestions.append(
                    f"Performance below recent average ({avg_score:.2f}). "
                    "Review recent successful executions for patterns."
                )
        
        return suggestions
    
    def get_comparative_analysis(self, task_type: Optional[str] = None) -> Dict[str, Any]:
        """Get comparative analysis across historical executions"""
        if not self.feedback_history:
            return {"message": "No historical data available"}
        
        # Filter by task type if specified
        history = self.feedback_history
        if task_type:
            history = [h for h in history if h.get("task_type") == task_type]
        
        if not history:
            return {"message": f"No historical data for task type: {task_type}"}
        
        scores = [h["score"] for h in history]
        success_rate = sum(1 for h in history if h["success"]) / len(history)
        
        return {
            "total_executions": len(history),
            "average_score": sum(scores) / len(scores),
            "best_score": max(scores),
            "worst_score": min(scores),
            "success_rate": success_rate,
            "trend": "improving" if len(scores) > 1 and scores[-1] > scores[0] else "declining"
        }
