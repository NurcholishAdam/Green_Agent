# src/analysis/optimization/complexity_analyzer.py
"""
Task Complexity Analysis for Fair Agent Comparison (Enhanced, v2.0.0)
=======================================================================

Analyzes task complexity across multiple dimensions to enable fair
normalization of energy consumption and performance metrics.

Original API preserved (all signatures and return shapes intact):
    analyzer = ComplexityAnalyzer()
    complexity = analyzer.analyze_from_trace(trace)
    tier       = analyzer.categorize_complexity(complexity)
    diff       = analyzer.compare_complexities(a, b)
    rec        = analyzer.detect_over_reasoning(complexity)
    suggestions= analyzer.suggest_optimization(complexity)
    batch      = analyzer.batch_analyze(traces)

Enhanced API:
    record = analyzer.analyze_detailed(trace)        # ComplexityRecord
    dr     = analyzer.to_decision_record(complexity) # shared contract
    stats  = analyzer.get_statistics()
    analyzer.set_normalization({...})                # replace baseline
    analyzer.record_calibration(dim, reward)         # reward feedback
    analyzer.set_hitl_callback(cb)                   # in-place reviewer

Enhancements implemented (v2.0.0):
  1.  Input sanitisation     — NaN/Inf/negative values zeroed and flagged
  2.  Configurable norm      — immutable baseline + calibration multipliers
  3.  Adaptive precision     — estimation flags downweight their dimensions
  4.  Weight profiles        — default / latency_critical / quality_critical / cheap
  5.  Data-driven uncertainty— estimation flags + tier-boundary proximity
  6.  Counterfactual XAI     — "what if dimension X were halved?" deltas
  7.  Anomaly detection      — rolling z-score outlier flagging (> 3 sigma)
  8.  Causal calibration     — reward-driven divisor adjustment with decay
  9.  Federated analytics    — bounded profiles + running tier aggregates
 10.  Multi-agent attribution— running per-agent aggregates + recent window
 11.  Temporal bounds        — verify() detects non-numeric/NaN/Inf/neg/oversize
 12.  Structured XAI         — headline, rationale, suggestions, counterfactuals
 13.  HITL                   — in-place callback on high-complexity tasks
 14.  Bounded memory         — agent/federated deques + running aggregates
 15.  Thread safety          — RLock around all mutable state
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict, replace
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

__version__ = "2.0.0"

__all__ = [
    "ComplexityAnalyzer",
    "ComplexityRecord",
    "TaskComplexity",
    "ComplexityTier",
    "Severity",
    "SuggestionKind",
    "ComplexityBoundsMonitor",
    "CausalNormalizationLearner",
    "FederatedComplexityAggregator",
    "FederatedComplexityProfile",
    "AgentComplexityObservation",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

_DEFAULT_WEIGHTS: Dict[str, float] = {
    "prompt_length": 0.2,
    "reasoning_steps": 0.3,
    "tool_calls": 0.2,
    "wall_clock_ms": 0.2,
    "context_size": 0.1,
}

# Smallest allowed divisor to prevent ZeroDivision / negative-log growth
_MIN_DIVISOR = 1e-9


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
    """Categories of optimisation suggestions (replaces emoji prefixes)."""
    PROMPT_COMPRESSION = "prompt_compression"
    REASONING_DEPTH = "reasoning_depth"
    TOOL_BATCHING = "tool_batching"
    EXECUTION_TIME = "execution_time"
    CONTEXT_PRUNING = "context_pruning"
    WELL_OPTIMIZED = "well_optimized"


# =============================================================================
# Numeric safety helpers
# =============================================================================

def _sanitize_nonneg(value: Any, name: str) -> Tuple[float, Optional[str]]:
    """
    Coerce `value` to a non-negative finite float.

    Returns (safe_value, violation). Violation is `None` when clean,
    otherwise one of "<name>_non_numeric", "<name>_nan",
    "<name>_inf", "<name>_negative".
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0, f"{name}_non_numeric"
    if math.isnan(v):
        return 0.0, f"{name}_nan"
    if math.isinf(v):
        return 0.0, f"{name}_inf"
    if v < 0:
        return 0.0, f"{name}_negative"
    return v, None


def _safe_log1p(value: Any) -> float:
    """log1p that never raises and never returns NaN/Inf."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(v) or math.isinf(v) or v < 0:
        return 0.0
    return math.log1p(v)


def _safe_divisor(value: Any, default: float) -> float:
    """Ensure the divisor is strictly positive and finite."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(v) or math.isinf(v) or v < _MIN_DIVISOR:
        return default
    return v


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

    # --- Enhancement: calibration / reproducibility ---
    normalization: Dict[str, float] = field(default_factory=dict)
    weights: Dict[str, float] = field(default_factory=dict)

    # --- Enhancement: temporal verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    def compute_composite_score(
        self,
        weights: Optional[Dict[str, float]] = None,
    ) -> float:
        """
        Compute weighted complexity score. Backward-compatible.

        Resolution order for weights:
          1. explicit `weights` argument
          2. `self.weights` (stored at extraction time, incl. profile + adaptive precision)
          3. module-level `_DEFAULT_WEIGHTS`
        """
        if weights is None:
            weights = dict(self.weights) if self.weights else dict(_DEFAULT_WEIGHTS)
        else:
            weights = dict(weights)

        if not weights:
            logger.warning("compute_composite_score called with empty weights")
            return 0.0

        weight_sum = sum(weights.values())
        if weight_sum <= 0:
            logger.warning(
                "Weights sum to non-positive (%s); returning 0.0", weight_sum
            )
            return 0.0
        if abs(weight_sum - 1.0) > 1e-6:
            logger.warning(
                "Weights sum to %s, normalising to 1.0", weight_sum
            )
            weights = {k: v / weight_sum for k, v in weights.items()}

        norm = self.normalization or {}
        divisor_prompt = _safe_divisor(norm.get("prompt_length"), 10.0)
        divisor_reasoning = _safe_divisor(norm.get("reasoning_steps"), 5.0)
        divisor_tools = _safe_divisor(norm.get("tool_calls"), 3.0)
        divisor_time = _safe_divisor(norm.get("wall_clock_ms"), 1000.0)
        divisor_context = _safe_divisor(norm.get("context_size"), 15.0)

        normalized = {
            "prompt_length": _safe_log1p(self.prompt_length) / divisor_prompt,
            "reasoning_steps": _safe_log1p(self.reasoning_steps) / divisor_reasoning,
            "tool_calls": _safe_log1p(self.tool_calls) / divisor_tools,
            "wall_clock_ms": _safe_log1p(self.wall_clock_ms) / divisor_time,
            "context_size": _safe_log1p(self.context_size) / divisor_context,
        }

        score = sum(normalized[k] * weights.get(k, 0.0) for k in normalized)

        logger.debug(
            "Computed complexity score: %.4f from %s", score, normalized
        )
        return score

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialisation (original keys preserved)."""
        out: Dict[str, Any] = {
            "prompt_length": self.prompt_length,
            "reasoning_steps": self.reasoning_steps,
            "tool_calls": self.tool_calls,
            "wall_clock_ms": self.wall_clock_ms,
            "context_size": self.context_size,
            "composite_score": self.compute_composite_score(),
        }
        out.update({
            "trace_id": self.trace_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "analyzed_at": self.analyzed_at.isoformat(),
            "context_size_estimated": self.context_size_estimated,
            "prompt_length_estimated": self.prompt_length_estimated,
            # --- Enhancement: reproducibility ---
            "normalization": dict(self.normalization),
            "weights": dict(self.weights),
            "bounds_ok": self.bounds_ok,
            "bounds_violations": list(self.bounds_violations),
        })
        return out


# =============================================================================
# ComplexityRecord with structured XAI + severity
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

    # --- Counterfactual XAI ---
    counterfactuals: List[Dict[str, Any]] = field(default_factory=list)

    # --- Anomaly detection ---
    is_outlier: bool = False
    outlier_z_score: float = 0.0

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
# Temporal logic — bounds verification (extended)
# =============================================================================

class ComplexityBoundsMonitor:
    """
    Verify that complexity dimensions are non-negative and within bounds.
    Detects non-numeric, NaN, Inf, negative, and over-max values.
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
            if math.isnan(v):
                bad.append(f"{field_name}_nan")
                continue
            if math.isinf(v):
                bad.append(f"{field_name}_inf")
                continue
            if v < 0:
                bad.append(f"{field_name}_negative")
            if v > max_val:
                bad.append(f"{field_name}_above_max")
        return (len(bad) == 0, bad)


# =============================================================================
# Causal RL for normalization calibration (with decay)
# =============================================================================

class CausalNormalizationLearner:
    """
    Learns multiplicative corrections to normalization divisors from
    reward signals. Offsets are exponentially decayed on each `update()`
    so stale rewards do not bias forever.
    """

    def __init__(self, lr: float = 0.01, decay: float = 0.99) -> None:
        self.lr = lr
        self.decay = decay
        self.offsets: Dict[str, float] = defaultdict(float)
        self.observations: int = 0
        self.updates: int = 0

    def predict_offsets(self) -> Dict[str, float]:
        """Return bounded multiplicative offsets around 1.0."""
        return {
            k: max(0.8, min(1.2, 1.0 + v))
            for k, v in self.offsets.items()
        }

    def record(self, dimension: str, reward: float) -> None:
        # Reward > 0 nudges divisor up (larger denominator -> lower score).
        self.offsets[dimension] += self.lr * reward
        self.observations += 1

    def update(self) -> None:
        """Decay every offset toward zero."""
        self.updates += 1
        for k in list(self.offsets.keys()):
            self.offsets[k] *= self.decay


# =============================================================================
# Federated tier distribution (bounded + incremental)
# =============================================================================

@dataclass
class FederatedComplexityProfile:
    deployment_id: str
    tier: str
    mean_score: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedComplexityAggregator:
    """
    Bounded profiles (recent window) plus incremental running aggregates
    per tier — push is O(1), aggregate is O(#tiers).
    """

    def __init__(self, max_profiles: int = 10_000) -> None:
        self._profiles: Deque[FederatedComplexityProfile] = deque(
            maxlen=max_profiles
        )
        self._tier_agg: Dict[str, Dict[str, float]] = {}
        self._lock = threading.Lock()

    def push(self, p: FederatedComplexityProfile) -> None:
        with self._lock:
            self._profiles.append(p)
            agg = self._tier_agg.setdefault(
                p.tier, {"sum_score": 0.0, "count": 0.0}
            )
            agg["sum_score"] += p.mean_score * p.sample_count
            agg["count"] += p.sample_count

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {
                tier: {
                    "mean_score": agg["sum_score"] / agg["count"],
                    "sample_count": agg["count"],
                }
                for tier, agg in self._tier_agg.items()
                if agg["count"] > 0
            }

    def recent_profiles(self) -> List[FederatedComplexityProfile]:
        with self._lock:
            return list(self._profiles)


# =============================================================================
# Multi-agent attribution
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

    Backward-compatible: same constructor keys, same methods, same returns.
    """

    TIER_THRESHOLDS: Dict[str, float] = {
        "trivial": 0.5,
        "simple": 1.5,
        "moderate": 3.0,
        "complex": 5.0,
        "extreme": float("inf"),
    }

    DEFAULT_NORMALIZATION: Dict[str, float] = {
        "prompt_length": 10.0,
        "reasoning_steps": 5.0,
        "tool_calls": 3.0,
        "wall_clock_ms": 1000.0,
        "context_size": 15.0,
    }

    WEIGHT_PROFILES: Dict[str, Dict[str, float]] = {
        "default": dict(_DEFAULT_WEIGHTS),
        "latency_critical": {
            "prompt_length": 0.10,
            "reasoning_steps": 0.20,
            "tool_calls": 0.20,
            "wall_clock_ms": 0.40,
            "context_size": 0.10,
        },
        "quality_critical": {
            "prompt_length": 0.15,
            "reasoning_steps": 0.40,
            "tool_calls": 0.20,
            "wall_clock_ms": 0.10,
            "context_size": 0.15,
        },
        "cheap": {
            "prompt_length": 0.25,
            "reasoning_steps": 0.15,
            "tool_calls": 0.30,
            "wall_clock_ms": 0.10,
            "context_size": 0.20,
        },
    }

    HITL_TIER = "complex"

    def __init__(
        self,
        *,
        normalization: Optional[Dict[str, float]] = None,
        tier_thresholds: Optional[Dict[str, float]] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        weight_profile: str = "default",
        weights: Optional[Dict[str, float]] = None,
        adaptive_precision: bool = True,
        max_agent_observations: int = 10_000,
        max_federated_profiles: int = 10_000,
        calibration_lr: float = 0.01,
        calibration_decay: float = 0.99,
        score_history_size: int = 1024,
    ) -> None:
        # --- Tier thresholds ---
        self.tier_thresholds = dict(tier_thresholds or self.TIER_THRESHOLDS)

        # --- Normalisation: baseline (immutable) + multipliers (learned) ---
        self._baseline_normalization: Dict[str, float] = {
            **self.DEFAULT_NORMALIZATION, **(normalization or {})
        }
        self._calibration_multipliers: Dict[str, float] = {
            k: 1.0 for k in self._baseline_normalization
        }

        # --- Weights ---
        if weights is not None:
            self.weights: Dict[str, float] = dict(weights)
            self._user_weights_override = True
        else:
            self.weights = dict(
                self.WEIGHT_PROFILES.get(
                    weight_profile, self.WEIGHT_PROFILES["default"]
                )
            )
            self._user_weights_override = False
        self._adaptive_precision = adaptive_precision

        # --- Identity ---
        self.agent_id = agent_id
        self.deployment_id = deployment_id

        # --- Enhancement subsystems ---
        self.bounds_monitor = ComplexityBoundsMonitor()
        self.calibration_learner = CausalNormalizationLearner(
            lr=calibration_lr, decay=calibration_decay
        )
        self.federated = FederatedComplexityAggregator(
            max_profiles=max_federated_profiles
        )

        # --- Bounded per-agent state ---
        self._agent_observations: Deque[AgentComplexityObservation] = deque(
            maxlen=max_agent_observations
        )
        self._agent_agg: Dict[str, Dict[str, float]] = {}

        # --- Statistics ---
        self._analyzed_count = 0
        self._tier_counts: Counter = Counter()
        self._score_sum_all = 0.0
        self._score_count_all = 0
        self._score_history: Deque[float] = deque(maxlen=score_history_size)
        self._outlier_count = 0

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[ComplexityRecord], Optional[bool]]
        ] = None

        # --- Thread safety ---
        self._lock = threading.RLock()

        logger.info(
            "Enhanced ComplexityAnalyzer initialised "
            "(agent_id=%s, deployment=%s, profile=%s, adaptive_precision=%s)",
            agent_id, deployment_id,
            "custom" if weights else weight_profile, adaptive_precision,
        )

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    @property
    def normalization(self) -> Dict[str, float]:
        """Current effective normalization (baseline * calibration)."""
        return {
            k: self._baseline_normalization[k]
            * self._calibration_multipliers.get(k, 1.0)
            for k in self._baseline_normalization
        }

    def set_normalization(self, normalization: Dict[str, float]) -> None:
        """
        Replace part of the baseline normalisation.

        Calibration multipliers are preserved. Unknown keys and
        non-positive / non-finite values are ignored with a warning.
        """
        if not isinstance(normalization, dict):
            logger.warning("set_normalization expects dict, got %s",
                           type(normalization).__name__)
            return
        with self._lock:
            for k, v in normalization.items():
                if k not in self.DEFAULT_NORMALIZATION:
                    logger.warning("Unknown normalisation dimension: %s", k)
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    logger.warning("Invalid value for %s: %r", k, v)
                    continue
                if math.isnan(fv) or math.isinf(fv) or fv < _MIN_DIVISOR:
                    logger.warning("Rejected non-positive/non-finite value "
                                   "for %s: %r", k, v)
                    continue
                self._baseline_normalization[k] = fv

    def set_hitl_callback(
        self,
        callback: Callable[[ComplexityRecord], Optional[bool]],
    ) -> None:
        """
        Register a HITL reviewer invoked for tiers >= HITL_TIER.

        The callback receives the *returned* ComplexityRecord and may
        mutate it (e.g. append rationale, set needs_review=False). Its
        optional return value — if False — annotates the record as
        escalated. Exceptions are logged and swallowed.
        """
        self._hitl_callback = callback

    # ------------------------------------------------------------------
    # ORIGINAL public API (backward-compatible)
    # ------------------------------------------------------------------

    def analyze_from_trace(self, trace: Dict) -> TaskComplexity:
        """
        Extract complexity metrics from an execution trace.

        Pure extraction: does **not** update analyser statistics.
        Use `analyze_detailed` or `batch_analyze(track=True)` for that.
        """
        # --- Input validation ---
        if trace is None or not isinstance(trace, dict):
            logger.warning(
                "analyze_from_trace received %s; returning zero complexity",
                type(trace).__name__,
            )
            trace = {}

        violations: List[str] = []

        # --- Prompt length ---
        prompt = trace.get("prompt", "")
        prompt_estimated = True
        if isinstance(prompt, str):
            raw_prompt = int(len(prompt.split()) * 1.33)
        elif isinstance(prompt, (int, float)) and not isinstance(prompt, bool):
            raw_prompt = int(prompt)
            prompt_estimated = False
        else:
            raw_prompt = 0
        prompt_length, v = _sanitize_nonneg(raw_prompt, "prompt_length")
        if v:
            violations.append(v)
        prompt_length = int(prompt_length)

        # --- Reasoning steps ---
        reasoning = trace.get("reasoning", [])
        if isinstance(reasoning, list):
            raw_steps = len(reasoning)
        elif isinstance(reasoning, str):
            raw_steps = (
                reasoning.count(".")
                + reasoning.count("!")
                + reasoning.count("?")
            )
        elif isinstance(reasoning, int) and not isinstance(reasoning, bool):
            raw_steps = reasoning
        else:
            raw_steps = 0
        reasoning_steps_f, v = _sanitize_nonneg(raw_steps, "reasoning_steps")
        if v:
            violations.append(v)
        reasoning_steps = int(reasoning_steps_f)

        # --- Tool calls ---
        tool_calls = trace.get("tool_calls", [])
        if isinstance(tool_calls, list):
            raw_tools = len(tool_calls)
        elif isinstance(tool_calls, int) and not isinstance(tool_calls, bool):
            raw_tools = tool_calls
        else:
            raw_tools = 0
        num_tool_calls_f, v = _sanitize_nonneg(raw_tools, "tool_calls")
        if v:
            violations.append(v)
        num_tool_calls = int(num_tool_calls_f)

        # --- Execution time ---
        execution_time = trace.get("execution_time_ms", 0.0)
        if not execution_time:
            execution_time = trace.get("latency_ms", 0.0)
        execution_time_f, v = _sanitize_nonneg(execution_time, "wall_clock_ms")
        if v:
            violations.append(v)

        # --- Context size ---
        context_size_raw = trace.get("context_tokens", 0)
        context_size_f, v = _sanitize_nonneg(context_size_raw, "context_size")
        if v:
            violations.append(v)
        context_size = int(context_size_f)
        context_size_estimated = False
        if context_size == 0:
            context_size = prompt_length + (reasoning_steps * 50)
            context_size_estimated = True

        # --- Adaptive precision weights ---
        weights = dict(self.weights)
        if self._adaptive_precision and not self._user_weights_override:
            if prompt_estimated:
                weights = self._rebalance(weights, "prompt_length", 0.5)
            if context_size_estimated:
                weights = self._rebalance(weights, "context_size", 0.5)

        # --- Build TaskComplexity ---
        complexity = TaskComplexity(
            prompt_length=prompt_length,
            reasoning_steps=reasoning_steps,
            tool_calls=num_tool_calls,
            wall_clock_ms=execution_time_f,
            context_size=context_size,
            trace_id=trace.get("trace_id"),
            run_id=trace.get("run_id"),
            task_id=trace.get("task_id"),
            agent_id=trace.get("agent_id", self.agent_id),
            context_size_estimated=context_size_estimated,
            prompt_length_estimated=prompt_estimated,
            normalization=self.normalization,     # snapshot (baseline*cal)
            weights=weights,
        )

        # --- Temporal bounds verification (catches above_max) ---
        ok, verify_violations = self.bounds_monitor.verify(complexity)
        all_violations = list(dict.fromkeys(violations + verify_violations))
        complexity.bounds_ok = len(all_violations) == 0
        complexity.bounds_violations = all_violations
        if not ok:
            logger.warning(
                "Complexity bounds violations for trace %s: %s",
                trace.get("trace_id", "?"), all_violations,
            )

        logger.debug("Analyzed trace complexity: %s", complexity.to_dict())
        return complexity

    def categorize_complexity(self, complexity: TaskComplexity) -> str:
        """Categorize task into complexity tiers. Backward-compatible."""
        score = complexity.compute_composite_score()
        return self._categorize_from_score(score)

    def compare_complexities(
        self,
        complexity_a: TaskComplexity,
        complexity_b: TaskComplexity,
    ) -> Dict[str, Any]:
        """
        Compare two task complexities. Backward-compatible.

        Warns (but proceeds) if the operands carry different normalization
        snapshots — diffs computed across baselines are not directly
        comparable.
        """
        if complexity_a.normalization != complexity_b.normalization:
            logger.warning(
                "compare_complexities: operands carry different "
                "normalization snapshots; score_diff may not be meaningful"
            )

        score_a = complexity_a.compute_composite_score()
        score_b = complexity_b.compute_composite_score()

        dimension_comparison = {
            "prompt_length": complexity_a.prompt_length - complexity_b.prompt_length,
            "reasoning_steps": complexity_a.reasoning_steps - complexity_b.reasoning_steps,
            "tool_calls": complexity_a.tool_calls - complexity_b.tool_calls,
            "wall_clock_ms": complexity_a.wall_clock_ms - complexity_b.wall_clock_ms,
            "context_size": complexity_a.context_size - complexity_b.context_size,
        }

        return {
            "score_diff": score_a - score_b,
            "more_complex": (
                "A" if score_a > score_b
                else "B" if score_b > score_a else "Equal"
            ),
            "tier_a": self._categorize_from_score(score_a),
            "tier_b": self._categorize_from_score(score_b),
            "dimension_comparison": dimension_comparison,
        }

    def detect_over_reasoning(
        self,
        complexity: TaskComplexity,
        threshold_ratio: float = 3.0,
    ) -> Dict[str, Any]:
        """
        Detect excessive reasoning. Backward-compatible return shape;
        the original emoji-prefixed strings are preserved verbatim.
        """
        if complexity.prompt_length == 0:
            return {
                "over_reasoning": False,
                "ratio": 0.0,
                "recommendation": "Cannot analyze: prompt length is zero",
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
            "over_reasoning": over_reasoning,
            "ratio": ratio,
            "threshold": threshold_ratio,
            "recommendation": recommendation,
        }

    def suggest_optimization(self, complexity: TaskComplexity) -> List[str]:
        """
        Suggest optimizations. Backward-compatible: still returns a list
        of strings with the same emoji prefixes.
        """
        suggestions: List[str] = []

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

    def batch_analyze(
        self, traces: List[Dict], *, track: bool = True,
    ) -> Dict[str, Any]:
        """
        Analyze complexity across multiple traces. Backward-compatible.

        When `track=True` (default), each analysis feeds analyser statistics
        (tier counts, score history, per-agent aggregates, federated profiles).
        Set `track=False` for pure side-effect-free batch summarisation.
        """
        complexities: List[TaskComplexity] = []
        scores: List[float] = []
        tiers: List[str] = []

        for trace in traces:
            c = self.analyze_from_trace(trace)
            s = c.compute_composite_score()
            complexities.append(c)
            scores.append(s)
            tiers.append(self._categorize_from_score(s))

        if track:
            for c, s, t in zip(complexities, scores, tiers):
                self._record_observation(c, s, t)

        tier_distribution = dict(Counter(tiers))

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
            min_score, max_score = min(scores), max(scores)
        else:
            mean_score = std_score = median_score = min_score = max_score = 0.0

        return {
            "total_tasks": len(traces),
            "complexity_scores": {
                "mean": mean_score,
                "std": std_score,
                "min": min_score,
                "max": max_score,
                "median": median_score,
            },
            "tier_distribution": tier_distribution,
            "complexities": [c.to_dict() for c in complexities],
        }

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def analyze_detailed(self, trace: Dict) -> ComplexityRecord:
        """
        Analyze a trace and return a ComplexityRecord with structured XAI,
        severity, tier, uncertainty, counterfactuals, and anomaly flag.
        """
        complexity = self.analyze_from_trace(trace)
        score = complexity.compute_composite_score()
        tier = self._categorize_from_score(score)
        severity = self._tier_to_severity(tier)

        # --- Anomaly detection (compare against *prior* history) ---
        is_outlier, z = self._check_anomaly(score)

        # --- Dimension scores (read from complexity snapshot for consistency) ---
        norm = complexity.normalization
        dimension_scores = {
            "prompt_length":
                _safe_log1p(complexity.prompt_length)
                / _safe_divisor(norm.get("prompt_length"), 10.0),
            "reasoning_steps":
                _safe_log1p(complexity.reasoning_steps)
                / _safe_divisor(norm.get("reasoning_steps"), 5.0),
            "tool_calls":
                _safe_log1p(complexity.tool_calls)
                / _safe_divisor(norm.get("tool_calls"), 3.0),
            "wall_clock_ms":
                _safe_log1p(complexity.wall_clock_ms)
                / _safe_divisor(norm.get("wall_clock_ms"), 1000.0),
            "context_size":
                _safe_log1p(complexity.context_size)
                / _safe_divisor(norm.get("context_size"), 15.0),
        }

        over_reasoning = self.detect_over_reasoning(complexity)
        structured_suggestions = self._structured_suggestions(complexity)
        counterfactuals = self._compute_counterfactuals(complexity)
        uncertainty = self._compute_uncertainty(complexity, score)

        # --- Rationale ---
        rationale: List[str] = [
            f"Composite score = {score:.4f} across 5 dimensions.",
            f"Dominant dimension = "
            f"'{max(dimension_scores, key=dimension_scores.get)}'.",
        ]
        if complexity.context_size_estimated:
            rationale.append(
                "Context size was estimated from prompt length + reasoning steps."
            )
        if complexity.prompt_length_estimated:
            rationale.append(
                "Prompt length was estimated from word count (×1.33 heuristic)."
            )
        if not complexity.bounds_ok:
            rationale.append(
                f"Bounds violations: {complexity.bounds_violations}."
            )
        if over_reasoning["over_reasoning"]:
            rationale.append(
                f"Over-reasoning detected (ratio {over_reasoning['ratio']:.2f})."
            )
        if is_outlier:
            rationale.append(
                f"Score is an outlier vs. recent history (z = {z:+.2f})."
            )

        # --- HITL ---
        needs_review = tier in (self.HITL_TIER, "extreme")
        review_reason = (
            f"tier='{tier}' meets HITL threshold" if needs_review else None
        )

        # --- Build the record ONCE ---
        record = ComplexityRecord(
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
            counterfactuals=counterfactuals,
            is_outlier=is_outlier,
            outlier_z_score=z,
            needs_review=needs_review,
            review_reason=review_reason,
        )

        # --- HITL callback: can mutate / veto the returned record ---
        if record.needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback(record)
                if approved is False:
                    record.rationale.append(
                        "HITL reviewer returned False — marked for escalation."
                    )
            except Exception as e:  # never let a reviewer crash analysis
                logger.warning("HITL callback failed: %s", e)

        # --- Record statistics ---
        self._record_observation(complexity, score, tier, is_outlier)

        # --- Structured log event ---
        logger.info(
            "complexity.analyzed",
            extra={
                "trace_id": complexity.trace_id,
                "run_id": complexity.run_id,
                "task_id": complexity.task_id,
                "agent_id": complexity.agent_id,
                "score": score,
                "tier": tier,
                "severity": severity.value,
                "uncertainty": uncertainty,
                "is_outlier": is_outlier,
                "needs_review": needs_review,
            },
        )

        return record

    def to_decision_record(
        self,
        complexity: TaskComplexity,
        *,
        run_id: Optional[str] = None,
        policy_version: str = "",
        score: Optional[float] = None,
        tier: Optional[str] = None,
        score_uncertainty: Optional[float] = None,
        needs_review: Optional[bool] = None,
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord for the complexity analysis (shared contract).

        Accepts pre-computed `score`/`tier`/`score_uncertainty`/`needs_review`
        so callers already inside `analyze_detailed` avoid drift.
        Returns None if the analysis contract isn't importable.
        """
        try:
            from src.analysis import DecisionRecord
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug("DecisionRecord not importable; skipping emission")
                return None

        if score is None:
            score = complexity.compute_composite_score()
        if tier is None:
            tier = self._categorize_from_score(score)
        if score_uncertainty is None:
            score_uncertainty = self._compute_uncertainty(complexity, score)
        if needs_review is None:
            needs_review = tier in (self.HITL_TIER, "extreme")

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
                "severity": self._tier_to_severity(tier).value,
                "dimensions": {
                    "prompt_length": complexity.prompt_length,
                    "reasoning_steps": complexity.reasoning_steps,
                    "tool_calls": complexity.tool_calls,
                    "context_size": complexity.context_size,
                },
                "context_size_estimated": complexity.context_size_estimated,
                "prompt_length_estimated": complexity.prompt_length_estimated,
                "bounds_ok": complexity.bounds_ok,
                "bounds_violations": list(complexity.bounds_violations),
                "score_uncertainty": score_uncertainty,
                "needs_review": needs_review,
            },
            provenance={
                "source": "complexity_analyzer",
                "trace_id": complexity.trace_id,
                "analyzer_version": __version__,
            },
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative complexity analysis statistics."""
        with self._lock:
            all_time_mean = (
                self._score_sum_all / self._score_count_all
                if self._score_count_all else 0.0
            )
            stats: Dict[str, Any] = {
                "analyzer_version": __version__,
                "analyzed_count": self._analyzed_count,
                "outlier_count": self._outlier_count,
                "tier_distribution": dict(self._tier_counts),
                "score_stats": {
                    "all_time_mean": all_time_mean,
                    "recent_window_size": len(self._score_history),
                },
                "calibration_observations":
                    self.calibration_learner.observations,
                "calibration_updates":
                    self.calibration_learner.updates,
                "normalization": dict(self.normalization),
                "calibration_multipliers":
                    dict(self._calibration_multipliers),
                "weights": dict(self.weights),
                "tier_thresholds": {
                    k: (v if v != float("inf") else "inf")
                    for k, v in self.tier_thresholds.items()
                },
                "federated_aggregate": self.federated.aggregate(),
                "agent_stats": {
                    aid: {
                        "count": int(agg["count"]),
                        "mean_score": (
                            agg["sum_score"] / agg["count"]
                            if agg["count"] else 0.0
                        ),
                    }
                    for aid, agg in self._agent_agg.items()
                },
            }
            if self._score_history:
                scores = list(self._score_history)
                stats["score_stats"].update({
                    "recent_mean": sum(scores) / len(scores),
                    "recent_min": min(scores),
                    "recent_max": max(scores),
                })
        return stats

    def export(self) -> Dict[str, Any]:
        """Full serialisable export (bounded by configured memory limits)."""
        with self._lock:
            return {
                "analyzer_version": __version__,
                "statistics": self.get_statistics(),
                "recent_agent_observations": [
                    asdict(o) for o in self._agent_observations
                ],
                "recent_federated_profiles": [
                    asdict(p) for p in self.federated.recent_profiles()
                ],
            }

    def record_calibration(self, dimension: str, reward: float) -> None:
        """
        Feed back a reward signal for a normalization dimension.

        Positive reward nudges the divisor up (score decreases); negative
        reward nudges it down. Multipliers are bounded to [0.8, 1.2] and
        periodically decayed. The user-supplied baseline is never mutated.
        """
        if dimension not in self.DEFAULT_NORMALIZATION:
            logger.warning("Unknown calibration dimension: %s", dimension)
            return
        try:
            reward = float(reward)
        except (TypeError, ValueError):
            logger.warning("Invalid reward %r; ignored", reward)
            return
        if math.isnan(reward) or math.isinf(reward):
            logger.warning("Non-finite reward %r; ignored", reward)
            return

        with self._lock:
            self.calibration_learner.record(dimension, reward)
            if self.calibration_learner.observations % 10 == 0:
                self.calibration_learner.update()
            # Recompute calibration multipliers from the learner.
            for k, mult in self.calibration_learner.predict_offsets().items():
                if k in self._baseline_normalization:
                    self._calibration_multipliers[k] = max(
                        0.8, min(1.2, mult)
                    )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _categorize_from_score(self, score: float) -> str:
        for tier, threshold in self.tier_thresholds.items():
            if score < threshold:
                logger.debug(
                    "Categorized complexity score %.2f as '%s'", score, tier
                )
                return tier
        return "extreme"

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
    def _rebalance(
        weights: Dict[str, float], dim: str, fraction: float,
    ) -> Dict[str, float]:
        """
        Reduce `weights[dim]` by `fraction` and redistribute evenly to the
        other dimensions. Returns a new dict (does not mutate input).
        """
        out = dict(weights)
        if dim not in out or not (0 < fraction < 1):
            return out
        freed = out[dim] * fraction
        out[dim] -= freed
        others = [k for k in out if k != dim]
        if others:
            per = freed / len(others)
            for k in others:
                out[k] += per
        return out

    def _compute_uncertainty(
        self, complexity: TaskComplexity, score: float,
    ) -> float:
        """
        Data-driven uncertainty:
          - estimation flags contribute
          - bounds violations contribute more
          - scores near a tier boundary contribute (ambiguous tier)
        """
        u = 0.0
        if complexity.prompt_length_estimated:
            u += 0.02
        if complexity.context_size_estimated:
            u += 0.05
        if not complexity.bounds_ok:
            u += 0.10
        # Boundary proximity: within 10% of any finite threshold.
        for threshold in self.tier_thresholds.values():
            if threshold == float("inf") or threshold <= 0:
                continue
            if abs(score - threshold) / threshold < 0.10:
                u += 0.03
                break
        return min(u, 0.5)  # clamp so it stays informative

    def _check_anomaly(self, score: float) -> Tuple[bool, float]:
        """Rolling z-score check against the *prior* score history."""
        with self._lock:
            if len(self._score_history) < 10:
                return False, 0.0
            scores = list(self._score_history)
        mean = sum(scores) / len(scores)
        variance = sum((s - mean) ** 2 for s in scores) / len(scores)
        std = math.sqrt(variance)
        if std < 1e-9:
            return False, 0.0
        z = (score - mean) / std
        return abs(z) > 3.0, z

    def _record_observation(
        self,
        complexity: TaskComplexity,
        score: float,
        tier: str,
        is_outlier: bool = False,
    ) -> None:
        """Thread-safe statistics update. Called by analyze_detailed and batch."""
        with self._lock:
            self._analyzed_count += 1
            self._tier_counts[tier] += 1
            self._score_history.append(score)
            self._score_sum_all += score
            self._score_count_all += 1
            if is_outlier:
                self._outlier_count += 1

            if complexity.agent_id:
                self._agent_observations.append(AgentComplexityObservation(
                    agent_id=complexity.agent_id,
                    task_id=complexity.task_id or "",
                    score=score,
                    tier=tier,
                    reasoning_steps=complexity.reasoning_steps,
                    tool_calls=complexity.tool_calls,
                ))
                agg = self._agent_agg.setdefault(
                    complexity.agent_id, {"count": 0.0, "sum_score": 0.0}
                )
                agg["count"] += 1
                agg["sum_score"] += score

            self.federated.push(FederatedComplexityProfile(
                deployment_id=self.deployment_id,
                tier=tier,
                mean_score=score,
                sample_count=1,
            ))

    def _compute_counterfactuals(
        self, complexity: TaskComplexity,
    ) -> List[Dict[str, Any]]:
        """
        For each dimension, compute the score if that dimension were halved.
        Only non-trivial deltas are reported.
        """
        current = complexity.compute_composite_score()
        out: List[Dict[str, Any]] = []
        for dim in (
            "prompt_length", "reasoning_steps", "tool_calls",
            "wall_clock_ms", "context_size",
        ):
            orig = getattr(complexity, dim, 0)
            if not orig or orig <= 0:
                continue
            try:
                halved = replace(complexity, **{dim: orig / 2})
            except Exception:  # dataclass.replace edge case
                continue
            new_score = halved.compute_composite_score()
            delta = new_score - current
            if abs(delta) > 1e-4:
                out.append({
                    "dimension": dim,
                    "current_value": orig,
                    "halved_value": orig / 2,
                    "current_score": current,
                    "projected_score": new_score,
                    "delta": delta,
                })
        return out

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
    import json

    logging.basicConfig(level=logging.INFO)

    analyzer = ComplexityAnalyzer(agent_id="analyzer-A")

    # --- Original behavior ---
    print("=== Original behavior ===")
    trace = {
        "prompt": "Classify this image based on the content shown.",
        "reasoning": ["Step 1", "Step 2", "Step 3"],
        "tool_calls": [{"tool": "vision_api"}],
        "execution_time_ms": 1500,
        "context_tokens": 512,
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
        "prompt": "A" * 500 + " " + "B" * 500,
        "reasoning": [f"step {i}" for i in range(60)],
        "tool_calls": [{"tool": f"t{i}"} for i in range(15)],
        "execution_time_ms": 15000,
        "context_tokens": 12000,
        "task_id": "task-001",
        "run_id": "run-001",
    })
    print(f"  tier:      {rec.tier}")
    print(f"  severity:  {rec.severity}")
    print(f"  score:     {rec.score:.4f} ± {rec.score_uncertainty:.4f}")
    print(f"  outlier:   {rec.is_outlier} (z={rec.outlier_z_score:+.2f})")
    print(f"  HITL:      {rec.needs_review} ({rec.review_reason})")
    print("  rationale:")
    for r in rec.rationale:
        print(f"    • {r}")
    print("  structured suggestions:")
    for s in rec.suggestions:
        print(f"    - {s}")
    print("  counterfactuals (top 3):")
    for cf in rec.counterfactuals[:3]:
        print(f"    - {cf['dimension']}: {cf['current_value']:.0f} → "
              f"{cf['halved_value']:.0f} ⇒ Δscore {cf['delta']:+.4f}")

    # --- Sanitization test: negative + NaN inputs ---
    print("\n=== Sanitisation (negative / NaN inputs) ===")
    bad = analyzer.analyze_from_trace({
        "prompt": -100,
        "reasoning": -5,
        "tool_calls": float("nan"),
        "execution_time_ms": float("inf"),
        "context_tokens": -50,
    })
    print(f"  bounds_ok:  {bad.bounds_ok}")
    print(f"  violations: {bad.bounds_violations}")
    print(f"  score:      {bad.compute_composite_score():.4f}  (safe, no crash)")

    # --- DecisionRecord emission ---
    print("\n=== DecisionRecord ===")
    dr = analyzer.to_decision_record(
        c, run_id="run-001", policy_version="v5.0.1",
    )
    if dr:
        print(f"  selected_action: {dr.selected_action}")
        print(f"  explanation keys: {list(dr.explanation.keys())}")

    # --- Calibration feedback (baseline preserved) ---
    print("\n=== Calibration ===")
    print(f"  before: {analyzer.get_statistics()['normalization']}")
    analyzer.record_calibration("reasoning_steps", reward=0.5)
    print(f"  after:  {analyzer.get_statistics()['normalization']}")

    # --- User baseline is not clobbered by calibration ---
    custom = ComplexityAnalyzer(normalization={"prompt_length": 20.0})
    custom.record_calibration("prompt_length", reward=1.0)
    print(f"  custom baseline respected: "
          f"prompt_length={custom.normalization['prompt_length']:.4f} "
          f"(base 20.0 × multiplier)")

    # --- Statistics ---
    print("\n=== Statistics ===")
    print(json.dumps(analyzer.get_statistics(), indent=2, default=str))
