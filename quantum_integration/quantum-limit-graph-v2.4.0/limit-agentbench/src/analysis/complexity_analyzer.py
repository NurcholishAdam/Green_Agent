"""
Task Complexity Analysis for Fair Agent Comparison

This module analyzes task complexity across multiple dimensions to enable
fair normalization of energy consumption and performance metrics.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
import numpy as np
import logging

logger = logging.getLogger(__name__)


@dataclass
class TaskComplexity:
    """
    Multi-dimensional task complexity measurement
    
    Tracks complexity across different dimensions:
    - Input size (prompt length, context)
    - Computational requirements (reasoning steps, tool calls)
    - Temporal requirements (wall-clock time)
    """
    prompt_length: int           # Number of tokens in input
    reasoning_steps: int          # Number of thinking/planning steps
    tool_calls: int              # Number of external tools invoked
    wall_clock_ms: float         # Actual execution time
    context_size: int            # Total context window used
    
    def compute_composite_score(self, weights: Optional[Dict[str, float]] = None) -> float:
        """
        Compute weighted complexity score
        
        Args:
            weights: Custom weights for each dimension
                    Default: Balanced weights across all dimensions
        
        Returns:
            Composite complexity score (higher = more complex)
        
        Formula:
            score = Σ(normalized_dimension_i * weight_i)
            
        Normalization uses log scale for large values to prevent
        single dimensions from dominating the score
        """
        if weights is None:
            # Default: balanced weights
            weights = {
                'prompt_length': 0.2,
                'reasoning_steps': 0.3,
                'tool_calls': 0.2,
                'wall_clock_ms': 0.2,
                'context_size': 0.1
            }
        
        # Validate weights sum to 1.0
        weight_sum = sum(weights.values())
        if not np.isclose(weight_sum, 1.0):
            logger.warning(f"Weights sum to {weight_sum}, normalizing to 1.0")
            weights = {k: v / weight_sum for k, v in weights.items()}
        
        # Normalize each component using log scale
        normalized = {
            'prompt_length': np.log1p(self.prompt_length) / 10.0,
            'reasoning_steps': np.log1p(self.reasoning_steps) / 5.0,
            'tool_calls': np.log1p(self.tool_calls) / 3.0,
            'wall_clock_ms': np.log1p(self.wall_clock_ms) / 1000.0,
            'context_size': np.log1p(self.context_size) / 15.0
        }
        
        # Weighted sum
        score = sum(normalized[k] * weights.get(k, 0.0) for k in normalized)
        
        logger.debug(f"Computed complexity score: {score:.4f} from {normalized}")
        return score
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'prompt_length': self.prompt_length,
            'reasoning_steps': self.reasoning_steps,
            'tool_calls': self.tool_calls,
            'wall_clock_ms': self.wall_clock_ms,
            'context_size': self.context_size,
            'composite_score': self.compute_composite_score()
        }


class ComplexityAnalyzer:
    """
    Analyzes task complexity from execution traces
    
    Extracts complexity metrics from agent execution data to enable
    fair comparison across tasks of different difficulties
    """
    
    # Complexity tier thresholds
    TIER_THRESHOLDS = {
        'trivial': 0.5,
        'simple': 1.5,
        'moderate': 3.0,
        'complex': 5.0,
        'extreme': float('inf')
    }
    
    def __init__(self):
        """Initialize complexity analyzer"""
        logger.info("Initialized ComplexityAnalyzer")
    
    def analyze_from_trace(self, trace: Dict) -> TaskComplexity:
        """
        Extract complexity metrics from execution trace
        
        Args:
            trace: Execution trace dictionary with keys:
                - 'prompt': str (input prompt)
                - 'reasoning': List[str] (reasoning steps)
                - 'tool_calls': List[Dict] (tool invocations)
                - 'execution_time_ms': float
                - 'context_tokens': int
        
        Returns:
            TaskComplexity object with extracted metrics
        
        Example:
            trace = {
                'prompt': "Classify this image...",
                'reasoning': ["Step 1: Load model", "Step 2: Preprocess"],
                'tool_calls': [{'tool': 'vision_api', 'params': {...}}],
                'execution_time_ms': 1500,
                'context_tokens': 512
            }
            complexity = analyzer.analyze_from_trace(trace)
            print(f"Complexity tier: {analyzer.categorize_complexity(complexity)}")
        """
        # Extract prompt length (tokens or words)
        prompt = trace.get('prompt', '')
        if isinstance(prompt, str):
            # Rough token estimate: ~0.75 words per token
            prompt_length = int(len(prompt.split()) * 1.33)
        else:
            prompt_length = 0
        
        # Extract reasoning steps
        reasoning = trace.get('reasoning', [])
        if isinstance(reasoning, list):
            reasoning_steps = len(reasoning)
        elif isinstance(reasoning, str):
            # If reasoning is a single string, count sentences
            reasoning_steps = reasoning.count('.') + reasoning.count('!') + reasoning.count('?')
        else:
            reasoning_steps = 0
        
        # Extract tool calls
        tool_calls = trace.get('tool_calls', [])
        if isinstance(tool_calls, list):
            num_tool_calls = len(tool_calls)
        else:
            num_tool_calls = 0
        
        # Extract execution time
        execution_time = trace.get('execution_time_ms', 0.0)
        if execution_time == 0.0:
            # Fallback to other time fields
            execution_time = trace.get('latency_ms', 0.0)
        
        # Extract context size
        context_size = trace.get('context_tokens', 0)
        if context_size == 0:
            # Estimate from prompt and reasoning
            context_size = prompt_length + (reasoning_steps * 50)  # Rough estimate
        
        complexity = TaskComplexity(
            prompt_length=prompt_length,
            reasoning_steps=reasoning_steps,
            tool_calls=num_tool_calls,
            wall_clock_ms=float(execution_time),
            context_size=context_size
        )
        
        logger.debug(f"Analyzed trace complexity: {complexity.to_dict()}")
        return complexity
    
    def categorize_complexity(self, complexity: TaskComplexity) -> str:
        """
        Categorize task into complexity tiers
        
        Args:
            complexity: TaskComplexity object
        
        Returns:
            Tier name: 'trivial', 'simple', 'moderate', 'complex', 'extreme'
        
        Tiers:
            - Trivial: Simple queries, minimal computation
            - Simple: Basic tasks, few reasoning steps
            - Moderate: Standard tasks, some complexity
            - Complex: Multi-step reasoning, tool usage
            - Extreme: Highly complex tasks, extensive computation
        """
        score = complexity.compute_composite_score()
        
        for tier, threshold in self.TIER_THRESHOLDS.items():
            if score < threshold:
                logger.info(f"Categorized complexity score {score:.2f} as '{tier}'")
                return tier
        
        return 'extreme'
    
    def compare_complexities(self, complexity_a: TaskComplexity, 
                            complexity_b: TaskComplexity) -> Dict:
        """
        Compare two task complexities
        
        Args:
            complexity_a: First task complexity
            complexity_b: Second task complexity
        
        Returns:
            Dictionary with comparison results:
            {
                'score_diff': float,
                'more_complex': str,
                'dimension_comparison': Dict
            }
        """
        score_a = complexity_a.compute_composite_score()
        score_b = complexity_b.compute_composite_score()
        
        dimension_comparison = {
            'prompt_length': complexity_a.prompt_length - complexity_b.prompt_length,
            'reasoning_steps': complexity_a.reasoning_steps - complexity_b.reasoning_steps,
            'tool_calls': complexity_a.tool_calls - complexity_b.tool_calls,
            'wall_clock_ms': complexity_a.wall_clock_ms - complexity_b.wall_clock_ms,
            'context_size': complexity_a.context_size - complexity_b.context_size
        }
        
        return {
            'score_diff': score_a - score_b,
            'more_complex': 'A' if score_a > score_b else 'B' if score_b > score_a else 'Equal',
            'tier_a': self.categorize_complexity(complexity_a),
            'tier_b': self.categorize_complexity(complexity_b),
            'dimension_comparison': dimension_comparison
        }
    
    def detect_over_reasoning(self, complexity: TaskComplexity, 
                             threshold_ratio: float = 3.0) -> Dict:
        """
        Detect if agent is using excessive reasoning steps
        
        Args:
            complexity: TaskComplexity to analyze
            threshold_ratio: Max ratio of reasoning_steps to prompt_length
                           Default: 3.0 (3 reasoning steps per token is excessive)
        
        Returns:
            Dictionary with detection results:
            {
                'over_reasoning': bool,
                'ratio': float,
                'recommendation': str
            }
        
        Example:
            For a 100-token prompt:
            - 50 reasoning steps = 0.5 ratio => OK
            - 300 reasoning steps = 3.0 ratio => Threshold
            - 500 reasoning steps = 5.0 ratio => Over-reasoning detected
        """
        if complexity.prompt_length == 0:
            return {
                'over_reasoning': False,
                'ratio': 0.0,
                'recommendation': 'Cannot analyze: prompt length is zero'
            }
        
        ratio = complexity.reasoning_steps / complexity.prompt_length
        over_reasoning = ratio > threshold_ratio
        
        if over_reasoning:
            recommendation = (
                f"⚠️ Agent used {complexity.reasoning_steps} reasoning steps for "
                f"{complexity.prompt_length} tokens (ratio: {ratio:.2f}). "
                f"Consider: reducing chain-of-thought depth, caching intermediate results, "
                f"or using a more efficient reasoning strategy."
            )
        else:
            recommendation = "✅ Reasoning depth appears appropriate for task complexity."
        
        return {
            'over_reasoning': over_reasoning,
            'ratio': ratio,
            'threshold': threshold_ratio,
            'recommendation': recommendation
        }
    
    def suggest_optimization(self, complexity: TaskComplexity) -> List[str]:
        """
        Suggest optimizations based on complexity analysis
        
        Args:
            complexity: TaskComplexity to analyze
        
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        
        # High prompt length
        if complexity.prompt_length > 2000:
            suggestions.append(
                "📝 Large prompt detected. Consider: prompt compression, "
                "summarization, or chunking input."
            )
        
        # Many reasoning steps
        if complexity.reasoning_steps > 50:
            suggestions.append(
                "🧠 High reasoning step count. Consider: early stopping, "
                "beam search pruning, or reduced chain-of-thought depth."
            )
        
        # Many tool calls
        if complexity.tool_calls > 10:
            suggestions.append(
                "🔧 Frequent tool usage detected. Consider: batching tool calls, "
                "caching results, or using lighter-weight tools."
            )
        
        # Long execution time
        if complexity.wall_clock_ms > 10000:  # 10 seconds
            suggestions.append(
                "⏱️ Long execution time. Consider: model quantization, "
                "parallel processing, or async execution."
            )
        
        # Large context
        if complexity.context_size > 8000:
            suggestions.append(
                "📊 Large context window. Consider: context pruning, "
                "sliding window attention, or retrieval-augmented generation."
            )
        
        if not suggestions:
            suggestions.append("✅ Task complexity is well-optimized.")
        
        return suggestions
    
    def batch_analyze(self, traces: List[Dict]) -> Dict:
        """
        Analyze complexity across multiple traces
        
        Args:
            traces: List of execution trace dictionaries
        
        Returns:
            Batch analysis summary with statistics
        """
        complexities = [self.analyze_from_trace(trace) for trace in traces]
        scores = [c.compute_composite_score() for c in complexities]
        tiers = [self.categorize_complexity(c) for c in complexities]
        
        # Compute statistics
        tier_distribution = {tier: tiers.count(tier) for tier in set(tiers)}
        
        return {
            'total_tasks': len(traces),
            'complexity_scores': {
                'mean': np.mean(scores),
                'std': np.std(scores),
                'min': np.min(scores),
                'max': np.max(scores),
                'median': np.median(scores)
            },
            'tier_distribution': tier_distribution,
            'complexities': [c.to_dict() for c in complexities]
        }
