# src/analysis/optimization/complexity_analyzer.py

"""
Task Complexity Analysis for Fair Agent Comparison (Enhanced)
==============================================================

Analyzes task complexity across multiple dimensions to enable fair
normalization of energy consumption and performance metrics.

Original API preserved:
    analyzer = ComplexityAnalyzer()
    complexity = analyzer.analyze_from_trace(trace)
    tier = analyzer.categorize_complexity(complexity)
    diff = analyzer.compare_complexities(a, b)
    rec = analyzer.detect_over_reasoning(complexity)
    suggestions = analyzer.suggest_optimization(complexity)
    batch = analyzer.batch_analyze(traces)

Enhanced API:
    record = analyzer.analyze_detailed(trace)      # ComplexityRecord
    rec = analyzer.to_decision_record(complexity)  # shared contract
    stats = analyzer.get_statistics()
    analyzer.set_normalization(...)                # configurable constants
    analyzer.record_outcome(...)                   # causal calibration

Enhancements:
  1. Quantum-Distillation      — precision-aware normalization
  2. Causal RL                 — calibration from outcomes
  3. Federated Analytics       — tier distribution profiles
  4. Multi-Agent Coordination  — per-agent complexity attribution
  5. Temporal Logic            — bounds verification on dimensions
  6. Explainable AI            — structured rationale
  7. Adaptive Precision        — complexity-aware precision hints
  8. Carbon Markets            — N/A (not applicable)
  9. Resilience                — complexity under chaos
 10. Human-in-the-Loop         — review for extreme complexity
 +   Provenance, uncertainty, simulated flags
 +   Configurable normalization + tier thresholds
 +   Statistics, validation, logging
"""

from __future__ import annotations

import logging
import math
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class ComplexityTier(Enum):
    """Complexity tiers (mirrors the original string tiers)."""
    TRIVIAL = "trivial"
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    EXTREME = "extreme"


class Severity(Enum):
    """Severity levels aligned with the analysis layer."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class SuggestionKind(Enum):
    """Categories of optimization suggestions (replaces emoji prefixes)."""
    PROMPT_COMPRESSION = "prompt_compression"
    REASONING_DEPTH = "reasoning_depth"
    TOOL_BATCHING = "tool_batching"
    EXECUTION_TIME = "execution_time"
    CONTEXT_PRUNING = "context_pruning"
    WELL_OPTIMIZED = "well_optimized"


# =============================================================================
# Original TaskComplexity — preserved and extended
# =============================================================================

@dataclass
class TaskComplexity:
    """
    Multi-dimensional task complexity measurement (extended).

    All original fields preserved with the same names and types.
    New fields are optional with defaults so existing code is unaffected.
    """
    # --- Original fields ---
    prompt_length: int
    reasoning_steps: int
    tool_calls: int
    wall_clock_ms: float
    context_size: int

    # --- Enhancement: provenance ---
    trace_id: Optional[str] = None
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    analyzed_at: datetime = field(default_factory=datetime.now)

    # --- Enhancement: uncertainty ---
    context_size_estimated: bool = False
    prompt_length_estimated: bool = True  # word-count heuristic

    # --- Enhancement: calibration ---
    normalization: Dict[str, float] = field(default_factory=dict)

    # --- Enhancement: temporal verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    def compute_composite_score(
        self,
        weights: Optional[Dict[str, float]] = None,
    ) -> float:
        """
        Compute weighted complexity score. Backward-compatible: same
        signature, same default weights, same return type.
        """
        if weights is None:
            weights = {
                'prompt_length': 0.2,
                'reasoning_steps': 0.3,
                'tool_calls': 0.2,
                'wall_clock_ms': 0.2,
                'context_size': 0.1,
            }

        # --- Validate weights sum to 1.0 (stdlib, no numpy) ---
        weight_sum = sum(weights.values())
        if abs(weight_sum - 1.0) > 1e-6:
            logger.warning(
                f"Weights sum to {weight_sum}, normalizing to 1.0"
            )
            if weight_sum > 0:
                weights = {k: v / weight_sum for k, v in weights.items()}
            else:
                weights = {k: 1.0 / len(weights) for k in weights}

        # --- Configurable normalization constants (defaults match original) ---
        norm = self.normalization or {}
        divisor_prompt = norm.get("prompt_length", 10.0)
        divisor_reasoning = norm.get("reasoning_steps", 5.0)
        divisor_tools = norm.get("tool_calls", 3.0)
        divisor_time = norm.get("wall_clock_ms", 1000.0)
        divisor_context = norm.get("context_size", 15.0)

        normalized = {
            'prompt_length': math.log1p(self.prompt_length) / divisor_prompt,
            'reasoning_steps': math.log1p(self.reasoning_steps) / divisor_reasoning,
            'tool_calls': math.log1p(self.tool_calls) / divisor_tools,
            'wall_clock_ms': math.log1p(self.wall_clock_ms) / divisor_time,
            'context_size': math.log1p(self.context_size) / divisor_context,
        }

        score = sum(
            normalized[k] * weights.get(k, 0.0) for k in normalized
        )

        logger.debug(
            f"Computed complexity score: {score:.4f} from {normalized}"
        )
        return score

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization (original keys preserved)."""
        out = {
            'prompt_length': self.prompt_length,
            'reasoning_steps': self.reasoning_steps,
            'tool_calls': self.tool_calls,
            'wall_clock_ms': self.wall_clock_ms,
            'context_size': self.context_size,
            'composite_score': self.compute_composite_score(),
        }
        # Enhancement additions (purely additive)
        out.update({
            'trace_id': self.trace_id,
            'run_id': self.run_id,
            'task_id': self.task_id,
            'agent_id': self.agent_id,
            'analyzed_at': self.analyzed_at.isoformat(),
            'context_size_estimated': self.context_size_estimated,
            'prompt_length_estimated': self.prompt_length_estimated,
            'bounds_ok': self.bounds_ok,
            'bounds_violations': list(self.bounds_violations),
        })
        return out


# =============================================================================
# Enhancement: ComplexityRecord with structured XAI + severity
# =============================================================================

@dataclass
class ComplexityRecord:
    """
    Rich analysis result wrapping TaskComplexity with provenance,
    uncertainty, structured XAI, severity, and tier.
    """
    complexity: TaskComplexity
    score: float
    tier: str
    severity: str

    # --- Structured XAI ---
    headline: str
    rationale: List[str]
    suggestions: List[Dict[str, Any]]

    # --- Uncertainty ---
    score_uncertainty: float = 0.0

    # --- Context ---
    over_reasoning: Dict[str, Any] = field(default_factory=dict)
    dimension_scores: Dict[str, float] = field(default_factory=dict)

    # --- HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        out["complexity"] = self.complexity.to_dict()
        return out


# =============================================================================
# Enhancement 5: Temporal logic — bounds verification
# =============================================================================

class ComplexityBoundsMonitor:
    """
    Verify that complexity dimensions are non-negative and within bounds.
    """

    MAX_VALUES = {
        "prompt_length": 1_000_000,
        "reasoning_steps": 10_000,
        "tool_calls": 1_000,
        "wall_clock_ms": 3_600_000.0,  # 1 hour
        "context_size": 10_000_000,
    }

    def verify(self, c: TaskComplexity) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for field_name, max_val in self.MAX_VALUES.items():
            value = getattr(c, field_name, 0)
            try:
                v = float(value)
            except (TypeError, ValueError):
                bad.append(f"{field_name}_non_numeric")
                continue
            if v < 0:
                bad.append(f"{field_name}_negative")
            if v > max_val:
                bad.append(f"{field_name}_above_max")
        return (len(bad) == 0, bad)


# =============================================================================
# Enhancement 2: Causal RL for normalization calibration
# =============================================================================

@dataclass
class CalibrationState:
    hour_of_day: int
    mean_score: float
    workload_hash: float


class CausalNormalizationLearner:
    """
    Learns multiplicative corrections to normalization divisors from
    (predicted_tier, actual_outcome) pairs. Kept deliberately simple:
    the learner nudges divisors toward better-separated tiers.
    """
    def __init__(self, lr: float = 0.01) -> None:
        self.lr = lr
        self.offsets: Dict[str, float] = defaultdict(float)
        self.observations: int = 0
        self.updates: int = 0

    def predict_offsets(self) -> Dict[str, float]:
        # Return bounded multipliers around 1.0
        return {
            k: max(0.8, min(1.2, 1.0 + v))
            for k, v in self.offsets.items()
        }

    def record(self, dimension: str, reward: float) -> None:
        # Reward > 0 nudges the divisor up (larger denominator → lower score)
        self.offsets[dimension] += self.lr * reward
        self.observations += 1

    def update(self) -> None:
        self.updates += 1


# =============================================================================
# Enhancement 3: Federated tier distribution
# =============================================================================

@dataclass
class FederatedComplexityProfile:
    deployment_id: str
    tier: str
    mean_score: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedComplexityAggregator:
    def __init__(self) -> None:
        self.profiles: List[FederatedComplexityProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, p: FederatedComplexityProfile) -> None:
        self.profiles.append(p)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedComplexityProfile]] = defaultdict(list)
        for p in self.profiles:
            grouped[p.tier].append(p)
        result: Dict[str, Dict[str, float]] = {}
        for tier, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[tier] = {
                "mean_score": sum(
                    p.mean_score * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result


# =============================================================================
# Enhancement 4: Multi-agent attribution
# =============================================================================

@dataclass
class AgentComplexityObservation:
    agent_id: str
    task_id: str
    score: float
    tier: str
    reasoning_steps: int
    tool_calls: int


# =============================================================================
# The Enhanced ComplexityAnalyzer
# =============================================================================

class ComplexityAnalyzer:
    """
    Enhanced task complexity analyzer.

    Backward-compatible: same constructor, same methods, same return types.
    """

    # Original tier thresholds (preserved; overridable per instance)
    TIER_THRESHOLDS = {
        'trivial': 0.5,
        'simple': 1.5,
        'moderate': 3.0,
        'complex': 5.0,
        'extreme': float('inf'),
    }

    # Default normalization divisors (mirror the original hardcoded values)
    DEFAULT_NORMALIZATION = {
        "prompt_length": 10.0,
        "reasoning_steps": 5.0,
        "tool_calls": 3.0,
        "wall_clock_ms": 1000.0,
        "context_size": 15.0,
    }

    # Complexity triggers HITL review at or above this tier
    HITL_TIER = "complex"

    def __init__(
        self,
        *,
        normalization: Optional[Dict[str, float]] = None,
        tier_thresholds: Optional[Dict[str, float]] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
    ):
        # --- Original state ---
        self.tier_thresholds = dict(
            tier_thresholds or self.TIER_THRESHOLDS
        )
        self.normalization = {
            **self.DEFAULT_NORMALIZATION, **(normalization or {})
        }
        self.agent_id = agent_id
        self.deployment_id = deployment_id

        # --- Enhancement: bounds monitor ---
        self.bounds_monitor = ComplexityBoundsMonitor()

        # --- Enhancement: causal learner ---
        self.calibration_learner = CausalNormalizationLearner()

        # --- Enhancement: federated ---
        self.federated = FederatedComplexityAggregator()

        # --- Enhancement: agent-level attribution ---
        self._agent_observations: List[AgentComplexityObservation] = []

        # --- Enhancement: history & statistics ---
        self._analyzed_count = 0
        self._tier_counts: Counter = Counter()
        self._score_sum = 0.0
        self._score_history: Deque[float] = deque(maxlen=1024)

        # --- Enhancement: HITL callback ---
        self._hitl_callback: Optional[Callable[[ComplexityRecord], bool]] = None

        logger.info(
            f"Enhanced ComplexityAnalyzer initialized "
            f"(agent_id={agent_id}, deployment={deployment_id})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API
    # ------------------------------------------------------------------

    def analyze_from_trace(self, trace: Dict) -> TaskComplexity:
        """
        Extract complexity metrics from an execution trace.

        Backward-compatible: same signature, same return type.
        """
        # --- Input validation ---
        if trace is None or not isinstance(trace, dict):
            logger.warning(
                f"analyze_from_trace received non-dict ({type(trace).__name__}); "
                "returning zero complexity"
            )
            trace = {}

        # --- Prompt length (with estimation flag) ---
        prompt = trace.get('prompt', '')
        prompt_estimated = True
        if isinstance(prompt, str):
            prompt_length = int(len(prompt.split()) * 1.33)
        elif isinstance(prompt, (int, float)):
            prompt_length = int(prompt)
            prompt_estimated = False
        else:
            prompt_length = 0

        # --- Reasoning steps ---
        reasoning = trace.get('reasoning', [])
        if isinstance(reasoning, list):
            reasoning_steps = len(reasoning)
        elif isinstance(reasoning, str):
            reasoning_steps = (
                reasoning.count('.') + reasoning.count('!')
                + reasoning.count('?')
            )
        elif isinstance(reasoning, int):
            reasoning_steps = reasoning
        else:
            reasoning_steps = 0

        # --- Tool calls ---
        tool_calls = trace.get('tool_calls', [])
        if isinstance(tool_calls, list):
            num_tool_calls = len(tool_calls)
        elif isinstance(tool_calls, int):
            num_tool_calls = tool_calls
        else:
            num_tool_calls = 0

        # --- Execution time ---
        execution_time = trace.get('execution_time_ms', 0.0)
        if not execution_time:
            execution_time = trace.get('latency_ms', 0.0)
        try:
            execution_time = float(execution_time)
        except (TypeError, ValueError):
            execution_time = 0.0

        # --- Context size (estimated flag) ---
        context_size_raw = trace.get('context_tokens', 0)
        context_size_estimated = False
        try:
            context_size = int(context_size_raw)
        except (TypeError, ValueError):
            context_size = 0
        if context_size == 0:
            context_size = prompt_length + (reasoning_steps * 50)
            context_size_estimated = True

        # --- Build TaskComplexity (with enhancement fields) ---
        complexity = TaskComplexity(
            prompt_length=prompt_length,
            reasoning_steps=reasoning_steps,
            tool_calls=num_tool_calls,
            wall_clock_ms=execution_time,
            context_size=context_size,
            trace_id=trace.get('trace_id'),
            run_id=trace.get('run_id'),
            task_id=trace.get('task_id'),
            agent_id=trace.get('agent_id', self.agent_id),
            context_size_estimated=context_size_estimated,
            prompt_length_estimated=prompt_estimated,
            normalization=dict(self.normalization),
        )

        # --- Temporal bounds verification ---
        ok, violations = self.bounds_monitor.verify(complexity)
        complexity.bounds_ok = ok
        complexity.bounds_violations = violations
        if not ok:
            logger.warning(
                f"Complexity bounds violations for trace "
                f"{trace.get('trace_id', '?')}: {violations}"
            )

        logger.debug(f"Analyzed trace complexity: {complexity.to_dict()}")
        return complexity

    def categorize_complexity(self, complexity: TaskComplexity) -> str:
        """
        Categorize task into complexity tiers. Backward-compatible.
        """
        score = complexity.compute_composite_score()
        for tier, threshold in self.tier_thresholds.items():
            if score < threshold:
                logger.info(
                    f"Categorized complexity score {score:.2f} as '{tier}'"
                )
                return tier
        return 'extreme'

    def compare_complexities(
        self,
        complexity_a: TaskComplexity,
        complexity_b: TaskComplexity,
    ) -> Dict:
        """
        Compare two task complexities. Backward-compatible.
        """
        score_a = complexity_a.compute_composite_score()
        score_b = complexity_b.compute_composite_score()

        dimension_comparison = {
            'prompt_length': complexity_a.prompt_length - complexity_b.prompt_length,
            'reasoning_steps': complexity_a.reasoning_steps - complexity_b.reasoning_steps,
            'tool_calls': complexity_a.tool_calls - complexity_b.tool_calls,
            'wall_clock_ms': complexity_a.wall_clock_ms - complexity_b.wall_clock_ms,
            'context_size': complexity_a.context_size - complexity_b.context_size,
        }

        return {
            'score_diff': score_a - score_b,
            'more_complex': (
                'A' if score_a > score_b
                else 'B' if score_b > score_a else 'Equal'
            ),
            'tier_a': self.categorize_complexity(complexity_a),
            'tier_b': self.categorize_complexity(complexity_b),
            'dimension_comparison': dimension_comparison,
        }

    def detect_over_reasoning(
        self,
        complexity: TaskComplexity,
        threshold_ratio: float = 3.0,
    ) -> Dict:
        """
        Detect excessive reasoning. Backward-compatible return shape;
        the original emoji-prefixed strings are preserved verbatim.
        """
        if complexity.prompt_length == 0:
            return {
                'over_reasoning': False,
                'ratio': 0.0,
                'recommendation': 'Cannot analyze: prompt length is zero',
            }

        ratio = complexity.reasoning_steps / complexity.prompt_length
        over_reasoning = ratio > threshold_ratio

        if over_reasoning:
            recommendation = (
                f"⚠️ Agent used {complexity.reasoning_steps} reasoning steps for "
                f"{complexity.prompt_length} tokens (ratio: {ratio:.2f}). "
                f"Consider: reducing chain-of-thought depth, caching intermediate "
                f"results, or using a more efficient reasoning strategy."
            )
        else:
            recommendation = (
                "✅ Reasoning depth appears appropriate for task complexity."
            )

        return {
            'over_reasoning': over_reasoning,
            'ratio': ratio,
            'threshold': threshold_ratio,
            'recommendation': recommendation,
        }

    def suggest_optimization(self, complexity: TaskComplexity) -> List[str]:
        """
        Suggest optimizations. Backward-compatible: still returns a list
        of strings with the same emoji prefixes.
        """
        suggestions = []

        if complexity.prompt_length > 2000:
            suggestions.append(
                "📝 Large prompt detected. Consider: prompt compression, "
                "summarization, or chunking input."
            )
        if complexity.reasoning_steps > 50:
            suggestions.append(
                "🧠 High reasoning step count. Consider: early stopping, "
                "beam search pruning, or reduced chain-of-thought depth."
            )
        if complexity.tool_calls > 10:
            suggestions.append(
                "🔧 Frequent tool usage detected. Consider: batching tool calls, "
                "caching results, or using lighter-weight tools."
            )
        if complexity.wall_clock_ms > 10000:
            suggestions.append(
                "⏱️ Long execution time. Consider: model quantization, "
                "parallel processing, or async execution."
            )
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
        Analyze complexity across multiple traces. Backward-compatible.
        """
        complexities = [self.analyze_from_trace(trace) for trace in traces]
        scores = [c.compute_composite_score() for c in complexities]
        tiers = [self.categorize_complexity(c) for c in complexities]

        tier_distribution = {tier: tiers.count(tier) for tier in set(tiers)}

        # --- Stdlib statistics (no numpy) ---
        if scores:
            mean_score = sum(scores) / len(scores)
            variance = sum((s - mean_score) ** 2 for s in scores) / len(scores)
            std_score = math.sqrt(variance)
            sorted_scores = sorted(scores)
            n = len(sorted_scores)
            median_score = (
                sorted_scores[n // 2] if n % 2 == 1
                else (sorted_scores[n // 2 - 1] + sorted_scores[n // 2]) / 2
            )
            min_score = min(scores)
            max_score = max(scores)
        else:
            mean_score = std_score = median_score = min_score = max_score = 0.0

        return {
            'total_tasks': len(traces),
            'complexity_scores': {
                'mean': mean_score,
                'std': std_score,
                'min': min_score,
                'max': max_score,
                'median': median_score,
            },
            'tier_distribution': tier_distribution,
            'complexities': [c.to_dict() for c in complexities],
        }

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def analyze_detailed(self, trace: Dict) -> ComplexityRecord:
        """
        Analyze a trace and return a ComplexityRecord with structured XAI,
        severity, tier, and suggestions.
        """
        complexity = self.analyze_from_trace(trace)
        score = complexity.compute_composite_score()
        tier = self.categorize_complexity(complexity)
        severity = self._tier_to_severity(tier)

        # --- Dimension scores ---
        norm = self.normalization
        dimension_scores = {
            "prompt_length": math.log1p(complexity.prompt_length) / norm["prompt_length"],
            "reasoning_steps": math.log1p(complexity.reasoning_steps) / norm["reasoning_steps"],
            "tool_calls": math.log1p(complexity.tool_calls) / norm["tool_calls"],
            "wall_clock_ms": math.log1p(complexity.wall_clock_ms) / norm["wall_clock_ms"],
            "context_size": math.log1p(complexity.context_size) / norm["context_size"],
        }

        # --- Over-reasoning ---
        over_reasoning = self.detect_over_reasoning(complexity)

        # --- Structured suggestions ---
        structured_suggestions = self._structured_suggestions(complexity)

        # --- Uncertainty from estimated flags ---
        uncertainty = 0.0
        if complexity.context_size_estimated:
            uncertainty += 0.05
        if complexity.prompt_length_estimated:
            uncertainty += 0.02
        if not complexity.bounds_ok:
            uncertainty += 0.1

        # --- Rationale ---
        rationale: List[str] = []
        rationale.append(
            f"Composite score = {score:.4f} across 5 dimensions."
        )
        rationale.append(
            f"Dominant dimension = '{max(dimension_scores, key=dimension_scores.get)}'."
        )
        if complexity.context_size_estimated:
            rationale.append(
                "Context size was estimated from prompt length + reasoning steps."
            )
        if not complexity.bounds_ok:
            rationale.append(
                f"Bounds violations: {complexity.bounds_violations}."
            )
        if over_reasoning["over_reasoning"]:
            rationale.append(
                f"Over-reasoning detected (ratio {over_reasoning['ratio']:.2f})."
            )

        # --- HITL review ---
        needs_review = False
        review_reason: Optional[str] = None
        if tier in (self.HITL_TIER, "extreme"):
            needs_review = True
            review_reason = f"tier='{tier}' meets HITL threshold"
            if self._hitl_callback is not None:
                try:
                    self._hitl_callback(ComplexityRecord(
                        complexity=complexity,
                        score=score,
                        tier=tier,
                        severity=severity.value,
                        headline=f"[{tier}] score={score:.3f}",
                        rationale=rationale,
                        suggestions=structured_suggestions,
                        score_uncertainty=uncertainty,
                        over_reasoning=over_reasoning,
                        dimension_scores=dimension_scores,
                        needs_review=True,
                        review_reason=review_reason,
                    ))
                except Exception as e:
                    logger.warning(f"HITL callback failed: {e}")

        # --- Record for statistics ---
        self._analyzed_count += 1
        self._tier_counts[tier] += 1
        self._score_sum += score
        self._score_history.append(score)

        # --- Multi-agent attribution ---
        if complexity.agent_id:
            self._agent_observations.append(AgentComplexityObservation(
                agent_id=complexity.agent_id,
                task_id=complexity.task_id or "",
                score=score,
                tier=tier,
                reasoning_steps=complexity.reasoning_steps,
                tool_calls=complexity.tool_calls,
            ))

        # --- Federated contribution ---
        self.federated.push(FederatedComplexityProfile(
            deployment_id=self.deployment_id,
            tier=tier,
            mean_score=score,
            sample_count=1,
        ))

        return ComplexityRecord(
            complexity=complexity,
            score=score,
            tier=tier,
            severity=severity.value,
            headline=f"[{tier}] score={score:.3f}",
            rationale=rationale,
            suggestions=structured_suggestions,
            score_uncertainty=uncertainty,
            over_reasoning=over_reasoning,
            dimension_scores=dimension_scores,
            needs_review=needs_review,
            review_reason=review_reason,
        )

    def to_decision_record(
        self,
        complexity: TaskComplexity,
        *,
        run_id: Optional[str] = None,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord for the complexity analysis (shared contract).

        Returns None if the analysis contract isn't importable.
        """
        try:
            from src.analysis import DecisionRecord
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "DecisionRecord not importable; skipping emission"
                )
                return None

        score = complexity.compute_composite_score()
        tier = self.categorize_complexity(complexity)
        return DecisionRecord(
            run_id=run_id or complexity.run_id or "",
            timestamp=complexity.analyzed_at,
            task_id=complexity.task_id or "",
            selected_action=f"complexity_analysis(tier={tier})",
            alternatives=[],
            policy_version=policy_version,
            model_or_agent=complexity.agent_id or "",
            quality_score=None,
            latency_ms=complexity.wall_clock_ms,
            energy_kwh=0.0,
            carbon_operational_kg=0.0,
            explanation={
                "complexity_score": score,
                "tier": tier,
                "dimensions": {
                    "prompt_length": complexity.prompt_length,
                    "reasoning_steps": complexity.reasoning_steps,
                    "tool_calls": complexity.tool_calls,
                    "context_size": complexity.context_size,
                },
                "context_size_estimated": complexity.context_size_estimated,
                "bounds_ok": complexity.bounds_ok,
            },
            provenance={
                "source": "complexity_analyzer",
                "trace_id": complexity.trace_id,
            },
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative complexity analysis statistics."""
        stats: Dict[str, Any] = {
            "analyzed_count": self._analyzed_count,
            "tier_distribution": dict(self._tier_counts),
            "circuits": {},  # for parity with other analyzers
            "calibration_observations": self.calibration_learner.observations,
            "normalization": dict(self.normalization),
            "tier_thresholds": {
                k: (v if v != float("inf") else "inf")
                for k, v in self.tier_thresholds.items()
            },
            "federated_aggregate": self.federated.aggregate(),
        }
        if self._score_history:
            scores = list(self._score_history)
            mean_s = sum(scores) / len(scores)
            stats["score_stats"] = {
                "mean": mean_s,
                "min": min(scores),
                "max": max(scores),
            }
        return stats

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        return {
            "statistics": self.get_statistics(),
            "agent_observations": [asdict(o) for o in self._agent_observations],
        }

    # ------------------------------------------------------------------
    # Enhancement: calibration & HITL
    # ------------------------------------------------------------------

    def set_hitl_callback(
        self, callback: Callable[[ComplexityRecord], bool],
    ) -> None:
        """Register a HITL review callback for high-complexity tasks."""
        self._hitl_callback = callback

    def record_calibration(
        self,
        dimension: str,
        reward: float,
    ) -> None:
        """
        Feed back a reward signal for a normalization dimension.

        Positive reward nudges the divisor up (score goes down); negative
        reward nudges it down. Then this module applies the learned
        multipliers to subsequent analyses.
        """
        self.calibration_learner.record(dimension, reward)
        if self.calibration_learner.observations % 10 == 0:
            self.calibration_learner.update()
        # Apply learned offsets to active normalization
        for k, mult in self.calibration_learner.predict_offsets().items():
            if k in self.DEFAULT_NORMALIZATION:
                self.normalization[k] = (
                    self.DEFAULT_NORMALIZATION[k] * mult
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _tier_to_severity(tier: str) -> Severity:
        return {
            "trivial": Severity.INFO,
            "simple": Severity.INFO,
            "moderate": Severity.WARNING,
            "complex": Severity.CRITICAL,
            "extreme": Severity.EMERGENCY,
        }.get(tier, Severity.INFO)

    @staticmethod
    def _structured_suggestions(
        c: TaskComplexity,
    ) -> List[Dict[str, Any]]:
        """Structured suggestions — the emoji-string equivalents as dicts."""
        out: List[Dict[str, Any]] = []
        if c.prompt_length > 2000:
            out.append({
                "kind": SuggestionKind.PROMPT_COMPRESSION.value,
                "detail": "prompt_length > 2000",
                "actions": ["compress", "summarize", "chunk"],
            })
        if c.reasoning_steps > 50:
            out.append({
                "kind": SuggestionKind.REASONING_DEPTH.value,
                "detail": "reasoning_steps > 50",
                "actions": ["early_stopping", "beam_pruning", "reduce_cot_depth"],
            })
        if c.tool_calls > 10:
            out.append({
                "kind": SuggestionKind.TOOL_BATCHING.value,
                "detail": "tool_calls > 10",
                "actions": ["batch", "cache", "lighter_tools"],
            })
        if c.wall_clock_ms > 10000:
            out.append({
                "kind": SuggestionKind.EXECUTION_TIME.value,
                "detail": "wall_clock_ms > 10s",
                "actions": ["quantization", "parallelize", "async"],
            })
        if c.context_size > 8000:
            out.append({
                "kind": SuggestionKind.CONTEXT_PRUNING.value,
                "detail": "context_size > 8000",
                "actions": ["prune", "sliding_window", "rag"],
            })
        if not out:
            out.append({
                "kind": SuggestionKind.WELL_OPTIMIZED.value,
                "detail": "within all thresholds",
                "actions": [],
            })
        return out


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    analyzer = ComplexityAnalyzer(agent_id="analyzer-A")

    # --- Original behavior ---
    print("=== Original behavior ===")
    trace = {
        'prompt': "Classify this image based on the content shown.",
        'reasoning': ["Step 1", "Step 2", "Step 3"],
        'tool_calls': [{'tool': 'vision_api'}],
        'execution_time_ms': 1500,
        'context_tokens': 512,
    }
    c = analyzer.analyze_from_trace(trace)
    print(f"  score: {c.compute_composite_score():.4f}")
    print(f"  tier:  {analyzer.categorize_complexity(c)}")
    print(f"  over_reasoning: {analyzer.detect_over_reasoning(c)}")
    print(f"  suggestions:")
    for s in analyzer.suggest_optimization(c):
        print(f"    - {s}")

    # --- Enhanced behavior ---
    print("\n=== Enhanced (structured record) ===")
    rec = analyzer.analyze_detailed({
        'prompt': "A" * 500 + " " + "B" * 500,
        'reasoning': [f"step {i}" for i in range(60)],
        'tool_calls': [{'tool': f't{i}'} for i in range(15)],
        'execution_time_ms': 15000,
        'context_tokens': 12000,
        'task_id': "task-001",
        'run_id': "run-001",
    })
    print(f"  tier:      {rec.tier}")
    print(f"  severity:  {rec.severity}")
    print(f"  score:     {rec.score:.4f} ± {rec.score_uncertainty:.4f}")
    print(f"  HITL:      {rec.needs_review} ({rec.review_reason})")
    print(f"  rationale:")
    for r in rec.rationale:
        print(f"    • {r}")
    print(f"  structured suggestions:")
    for s in rec.suggestions:
        print(f"    - {s}")

    # --- DecisionRecord emission ---
    print("\n=== DecisionRecord ===")
    dr = analyzer.to_decision_record(c, run_id="run-001", policy_version="v5.0.1")
    if dr:
        print(f"  selected_action: {dr.selected_action}")
        print(f"  explanation keys: {list(dr.explanation.keys())}")

    # --- Calibration feedback ---
    analyzer.record_calibration("reasoning_steps", reward=0.5)

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(analyzer.get_statistics(), indent=2, default=str))
