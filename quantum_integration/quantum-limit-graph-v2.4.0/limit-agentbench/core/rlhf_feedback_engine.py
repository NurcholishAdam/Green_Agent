# -*- coding: utf-8 -*-
"""
RLHF Feedback Engine - Reasoning Trace Analysis (Enhanced, v2)
==============================================================

Generates detailed feedback for agent improvement based on RLHF research.

Fixes applied vs. v1
--------------------
  P0  update() was never called (optimizer inert)       -> record_outcome()
  P0  Gate update was `pass` w/ crash-on-execute code   -> real regression gate
  P0  MODP weight 3 mapped to success, not sustain.     -> sustainability feature
  P0  FeedbackCategory.SUSTAINABILITY/ACCURACY unused   -> emitted
  P0  Efficiency collapsed to 0 without tools           -> additive tool factor
  P0  Exploration vs non-exploration 3.3x scale diff    -> same scale + noise
  P0  Config mutation + empty-list crash                -> frozen + validated
  P0  Unseeded RNG                                      -> default_rng(seed)
  P0  No persistence                                    -> save()/load()
  P0  Dead use_evolutionary flag                        -> EvolutionaryFeedback
  P1  feature_dim hard-coded                            -> FEATURE_DIM=8
  P1  epsilon never decays                              -> epsilon_min/decay
  P1  batch_size/distill_w/rl_w hard-coded              -> config
  P1  Feature vector not clipped                        -> _clamp
  P1  feedback_history: no bound, wrong timestamp       -> deque + datetime
  P1  graph_metrics/human by reference in report        -> copied
  P2  No thread safety                                  -> RLock
  P2  No metrics sink                                   -> MetricsSink
  P2  Dict-only return                                  -> FeedbackReport
  P2  FeedbackItem.severity was bare string             -> FeedbackSeverity enum
  P2  No record_human_feedback()                        -> added
  P2  Dead baseline_metrics                             -> implemented

Preserved public surface
------------------------
  ReasoningQuality, FeedbackCategory, ReasoningStep, FeedbackItem,
  RLHFConfig, FeedbackState, FeedbackDistillationOptimizer, RLHFFeedbackEngine,
  .analyze_reasoning_trace(...), .get_comparative_analysis(...),
  .feedback_history, .baseline_metrics, .use_enhancements, .distillation_optimizer

Breaking changes (documented)
-----------------------------
  - `FeedbackDistillationOptimizer.update()` is now called via
    `RLHFFeedbackEngine.record_outcome(reward)`. Callers that never call
    `record_outcome` will still get a functional (if untrained) engine —
    which matches v1 behaviour, since v1 never trained either.
  - `RLHFFeedbackEngine.analyze_reasoning_trace()` returns a dict with the
    same keys as v1 plus optional `sustainability_score`, `optimizer_stats`.
"""

from __future__ import annotations

import json
import logging
import math
import threading
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import numpy as np
    _NUMPY_OK = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_OK = False

logger = logging.getLogger(__name__)


# ============================================================================
# SECTION 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _is_finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _save_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(path)


def _load_json_guarded(path: Path) -> Optional[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring corrupt checkpoint %s: %s", path, exc)
        return None


# ============================================================================
# SECTION 2 — Metrics sink
# ============================================================================

class MetricsSink(Protocol):
    def observe(self, name: str, value: float, **labels: str) -> None: ...


class InMemoryMetrics:
    def __init__(self) -> None:
        self._data: Dict[str, float] = {}
        self._lock = threading.Lock()

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = name + (
            ":" + ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            if labels else ""
        )
        with self._lock:
            self._data[key] = value

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._data)


class LoggingMetrics:
    def __init__(self) -> None:
        self._log = logging.getLogger("rlhf_engine.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Enums
# ============================================================================

class ReasoningQuality(str, Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"


class FeedbackCategory(str, Enum):
    REASONING = "reasoning_quality"
    EFFICIENCY = "efficiency"
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    SUSTAINABILITY = "sustainability"


class FeedbackSeverity(str, Enum):
    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"


# ============================================================================
# SECTION 4 — Data records
# ============================================================================

@dataclass
class ReasoningStep:
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
            "duration": self.duration,
        }


@dataclass
class FeedbackItem:
    category: FeedbackCategory
    severity: FeedbackSeverity
    message: str
    suggestion: str
    affected_steps: Optional[List[int]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category.value,
            "severity": self.severity.value,
            "message": self.message,
            "suggestion": self.suggestion,
            "affected_steps": self.affected_steps,
        }


# ============================================================================
# SECTION 5 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class RLHFConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5, "connectivity": 0.5, "density": 0.4,
        }
    )

    # MODP weights: (reasoning, efficiency, completeness, sustainability)
    modp_weights: Tuple[float, float, float, float] = (0.35, 0.25, 0.25, 0.15)
    human_feedback_score: float = 0.5

    use_distillation: bool = True
    use_evolutionary: bool = False

    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995
    distill_w: float = 0.7
    rl_w: float = 0.3

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Sustainability normalisation scale
    sustainability_time_scale: float = 30.0      # seconds to "neutral" score
    sustainability_step_scale: int = 20          # steps to full step penalty

    # Exploration noise (student std)
    exploration_noise: float = 0.05

    history_maxlen: int = 1000
    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components (reasoning, efficiency, "
                f"completeness, sustainability), got {w!r}"
            )
        if any(float(x) < 0 for x in w):
            raise ValueError(f"modp_weights must be non-negative, got {w!r}")
        if sum(float(x) for x in w) <= 0:
            raise ValueError(f"modp_weights sum must be > 0, got {w!r}")
        if not 0.0 <= self.human_feedback_score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        if not 0.0 <= self.epsilon <= 1.0:
            raise ValueError("epsilon must be in [0,1]")
        if not 0.0 <= self.epsilon_min <= self.epsilon:
            raise ValueError("epsilon_min must be in [0, epsilon]")
        if not 0.0 < self.epsilon_decay <= 1.0:
            raise ValueError("epsilon_decay must be in (0,1]")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.population_size < 2:
            raise ValueError("population_size must be >= 2")
        if not 0 <= self.elitism < self.population_size:
            raise ValueError("elitism must be in [0, population_size)")
        if self.distill_w < 0 or self.rl_w < 0 or (self.distill_w + self.rl_w) <= 0:
            raise ValueError("distill_w and rl_w must be non-negative, sum > 0")
        if self.sustainability_time_scale <= 0:
            raise ValueError("sustainability_time_scale must be > 0")
        if self.sustainability_step_scale < 1:
            raise ValueError("sustainability_step_scale must be >= 1")
        if not 0.0 <= self.exploration_noise <= 0.5:
            raise ValueError("exploration_noise must be in [0, 0.5]")
        if self.history_maxlen < 1:
            raise ValueError("history_maxlen must be >= 1")
        for k, v in self.graph_metrics.items():
            if not _is_finite(v) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "RLHFConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "RLHFConfig ignoring unknown keys: %s", sorted(unknown)
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                if v is None:
                    continue
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 6 — FeedbackState (8 features, all clipped, cached)
# ============================================================================

class FeedbackState:
    """
    Feature vector: 8 components, all clipped to [0, 1].

      0. reasoning       (from _assess_reasoning_quality)
      1. efficiency      (from _analyze_efficiency)
      2. completeness    (from _check_completeness)
      3. sustainability  (computed from execution_time + step count)
      4. success         (1.0 if successful, else 0.0)
      5. centrality      (graph metric)
      6. connectivity    (graph metric)
      7. human_feedback  (RLHF score)
    """
    FEATURE_DIM = 8

    __slots__ = (
        "reasoning", "efficiency", "completeness", "sustainability",
        "success", "centrality", "connectivity", "human",
        "_vector",
    )

    def __init__(
        self,
        reasoning_score: float,
        efficiency_score: float,
        completeness_score: float,
        sustainability_score: float,
        success: bool,
        graph_metrics: Mapping[str, float],
        human_feedback: float,
    ) -> None:
        self.reasoning = _clamp(reasoning_score)
        self.efficiency = _clamp(efficiency_score)
        self.completeness = _clamp(completeness_score)
        self.sustainability = _clamp(sustainability_score)
        self.success = 1.0 if success else 0.0
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human = _clamp(human_feedback)

        self._vector = np.array(
            [
                self.reasoning, self.efficiency, self.completeness,
                self.sustainability, self.success,
                self.centrality, self.connectivity, self.human,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 7 — Typed results
# ============================================================================

@dataclass
class PendingFeedback:
    """State captured by analyze_reasoning_trace(), consumed by record_outcome()."""
    state_vec: "np.ndarray"
    teacher_scores: "np.ndarray"
    gate_probs: "np.ndarray"
    overall_score: float
    task_type: str


@dataclass
class FeedbackReport:
    overall_score: float
    reasoning_quality: str
    reasoning_score: float
    efficiency_score: float
    completeness_score: float
    sustainability_score: float
    feedback_items: List[Dict[str, Any]]
    improvement_suggestions: List[str]
    step_analysis: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    task_type: str = ""
    success: bool = False
    analyzed_at: str = ""
    enhanced: bool = False
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None
    optimizer_stats: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


# ============================================================================
# SECTION 8 — Teachers (regression-form: scalar scores)
# ============================================================================

class FeedbackTeacher(Protocol):
    def predict(self, state: FeedbackState) -> float: ...


class RuleTeacher:
    """
    MODP-weighted average. weights[3] now correctly applies to sustainability
    (was applied to success in v1 — fixes §2.3).
    """
    def __init__(self, weights: Sequence[float]) -> None:
        w = [float(x) for x in weights]
        if len(w) != 4 or any(x < 0 for x in w) or sum(w) <= 0:
            raise ValueError(f"weights must be 4 non-negative values, got {weights!r}")
        total = sum(w)
        self.weights = [x / total for x in w]

    def predict(self, state: FeedbackState) -> float:
        return _clamp(
            self.weights[0] * state.reasoning
            + self.weights[1] * state.efficiency
            + self.weights[2] * state.completeness
            + self.weights[3] * state.sustainability
        )


class RLHFTeacher:
    """Blends base quality with the human feedback signal."""
    def __init__(self, human_feedback: float = 0.5) -> None:
        self.h = _clamp(human_feedback)

    def predict(self, state: FeedbackState) -> float:
        base = (state.reasoning + state.efficiency + state.completeness) / 3.0
        if state.success > 0.5:
            base += 0.1
        adjustment = 0.2 * (state.human - 0.5)
        return _clamp(base + adjustment)


class HistoricalTeacher:
    """Simulated trained model: success + centrality boost."""
    def predict(self, state: FeedbackState) -> float:
        base = (
            0.4 * state.reasoning
            + 0.2 * state.efficiency
            + 0.2 * state.completeness
            + 0.2 * state.sustainability
        )
        if state.success > 0.5:
            base += 0.1
        if state.centrality > 0.7:
            base += 0.05
        return _clamp(base)


# ============================================================================
# SECTION 9 — Distillation optimizer (real MoE, correct regression gradient)
# ============================================================================

class FeedbackDistillationOptimizer:
    """
    Multi-teacher distillation + MoE gating for the feedback score.

    Forward returns full context so backward can train the gate on real
    teacher scores (fixes the `pass` block and the `FeedbackState.__new__`
    crash in v1).

    The gate gradient uses the **regression** form (this is a scalar
    prediction, not a classification):
        combined = gate @ teacher_scores      # scalar
        dL/dg    = (combined - target) * teacher_scores
        grad_logits = gate * (dL/dg - gate @ dL/dg)
    """

    def __init__(
        self,
        config: RLHFConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not _NUMPY_OK:
            raise RuntimeError("numpy is required for FeedbackDistillationOptimizer")

        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = FeedbackState.FEATURE_DIM
        self.n_teachers = 3

        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size
        self.exploration_noise = config.exploration_noise

        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.5                      # sensible prior
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.n_teachers)) * 0.01
        )
        self.gate_bias = np.zeros(self.n_teachers)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)

        self.teachers: List[FeedbackTeacher] = [
            RuleTeacher(config.modp_weights),
            RLHFTeacher(config.human_feedback_score),
            HistoricalTeacher(),
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".distill.json"))

    # ---- forward ---------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    def predict_score(
        self,
        state: FeedbackState,
        exploration: bool = False,
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        score           : float in [0,1]
        state_vec       : (FEATURE_DIM,)
        teacher_scores  : (T,) individual teacher predictions
        gate_probs      : (T,) MoE gate weights
        """
        x = state.to_vector()
        teacher_scores = np.array(
            [t.predict(state) for t in self.teachers], dtype=np.float64
        )
        gate = self._gate_forward(x)
        teacher_mix = float(gate @ teacher_scores)
        student_pred = float(x @ self.student_weights + self.student_bias)

        # Exploration adds *noise* to the student — it does NOT collapse the
        # blend to a different scale (fixes the 3.3x-mismatch in v1).
        if exploration and self.rng.random() < self.epsilon:
            student_pred += float(
                self.rng.normal(0.0, self.exploration_noise)
            )

        score = 0.7 * student_pred + 0.3 * teacher_mix
        return (
            float(np.clip(score, 0.0, 1.0)),
            x,
            teacher_scores,
            gate,
        )

    # ---- backward --------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        teacher_scores: "np.ndarray",
        gate_probs: "np.ndarray",
        reward: float,
    ) -> None:
        """
        Train on the TRUE outcome reward. `reward` should represent
        downstream quality / ground-truth feedback (in [0,1]).
        """
        r = float(np.clip(reward, 0.0, 1.0))
        self.replay.append(
            (
                state_vec.astype(np.float64),
                teacher_scores.astype(np.float64),
                gate_probs.astype(np.float64),
                r,
            )
        )
        self.counter += 1
        if (
            self.counter % self.train_every == 0
            and len(self.replay) >= self.batch_size
        ):
            self._train_step()

    def _train_step(self) -> None:
        n = min(self.batch_size, len(self.replay))
        idxs = self.rng.choice(len(self.replay), size=n, replace=False)
        batch = [self.replay[i] for i in idxs]

        for x, t_scores, gate, reward in batch:
            # ---- student (MSE) ----
            pred = float(x @ self.student_weights + self.student_bias)
            err = pred - reward
            self.student_weights -= self.lr * err * x
            self.student_bias -= self.lr * err

            # ---- gate (regression-form chain rule) ----
            teacher_mix = float(gate @ t_scores)          # scalar
            err_g = teacher_mix - reward                  # scalar
            dL_dg = err_g * t_scores                      # (T,)
            grad_logits = gate * (dL_dg - float(gate @ dL_dg))  # (T,)
            self.gate_weights -= self.gate_lr * np.outer(x, grad_logits)
            self.gate_bias -= self.gate_lr * grad_logits

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence -----------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json_atomic(
            path,
            {
                "kind": "feedback_distillation",
                "feature_dim": self.feature_dim,
                "n_teachers": self.n_teachers,
                "student_weights": self.student_weights.tolist(),
                "student_bias": [float(self.student_bias)],
                "gate_weights": self.gate_weights.tolist(),
                "gate_bias": self.gate_bias.tolist(),
                "epsilon": float(self.epsilon),
                "counter": int(self.counter),
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json_guarded(path)
        if not payload:
            return
        if payload.get("kind") != "feedback_distillation":
            logger.warning("Checkpoint kind mismatch: %s", payload.get("kind"))
            return
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = float(payload["student_bias"][0])
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", self.counter))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 10 — Evolutionary optimizer (fully implemented)
# ============================================================================

@dataclass
class _FeedbackInd:
    weights: "np.ndarray"     # (FEATURE_DIM,)
    bias: float
    fitness: float = 0.0


class EvolutionaryFeedbackOptimizer:
    """
    Population of linear regression models: features → scalar score.

    Fitness = negative MSE against the true reward (per observation),
    accumulated across calls. Evolves via tournament + crossover + mutation
    + elitism. Fully implements the v1 `use_evolutionary` flag.
    """

    def __init__(
        self,
        config: RLHFConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not _NUMPY_OK:
            raise RuntimeError("numpy is required for EvolutionaryFeedbackOptimizer")

        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = FeedbackState.FEATURE_DIM

        self.population: List[_FeedbackInd] = [
            _FeedbackInd(
                weights=self.rng.standard_normal(self.feature_dim) * 0.1,
                bias=0.5 + float(self.rng.standard_normal() * 0.05),
            )
            for _ in range(config.population_size)
        ]
        self.counter = 0
        self.generation = 0

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolution.json"))

    def _predict_with(self, ind: _FeedbackInd, x: "np.ndarray") -> float:
        return _clamp(float(x @ ind.weights + ind.bias))

    def predict_score(
        self,
        state: FeedbackState,
        exploration: bool = False,
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray"]:
        x = state.to_vector()
        if exploration and self.rng.random() < self.config.epsilon:
            idx = int(self.rng.integers(0, len(self.population)))
            best = self.population[idx]
        else:
            best = max(self.population, key=lambda ind: ind.fitness)
        score = self._predict_with(best, x)
        # Provide a uniform "teacher context" to keep the interface consistent.
        teacher_scores = np.array(
            [0.5, 0.5, 0.5], dtype=np.float64
        )
        gate = np.full(3, 1.0 / 3.0)
        return score, x, teacher_scores, gate

    def update(
        self,
        state_vec: "np.ndarray",
        teacher_scores: "np.ndarray",
        gate_probs: "np.ndarray",
        reward: float,
    ) -> None:
        r = float(np.clip(reward, 0.0, 1.0))
        for ind in self.population:
            pred = self._predict_with(ind, state_vec)
            ind.fitness -= (pred - r) ** 2
        self.counter += 1
        if self.counter % self.config.train_every == 0:
            self._evolve()

    def _evolve(self) -> None:
        self.population.sort(key=lambda ind: ind.fitness, reverse=True)
        n_elite = max(1, self.config.elitism)
        new_pop: List[_FeedbackInd] = [
            _FeedbackInd(weights=ind.weights.copy(), bias=ind.bias, fitness=0.0)
            for ind in self.population[:n_elite]
        ]
        while len(new_pop) < len(self.population):
            a = self._tournament()
            b = self._tournament()
            child = self._crossover(a, b)
            self._mutate(child)
            child.fitness = 0.0
            new_pop.append(child)
        self.population = new_pop
        self.generation += 1

    def _tournament(self, k: int = 3) -> _FeedbackInd:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        return max((self.population[int(i)] for i in idxs),
                   key=lambda ind: ind.fitness)

    def _crossover(self, a: _FeedbackInd, b: _FeedbackInd) -> _FeedbackInd:
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(self.feature_dim) < 0.5
            return _FeedbackInd(
                weights=np.where(mask, a.weights, b.weights).copy(),
                bias=float(a.bias if self.rng.random() < 0.5 else b.bias),
            )
        return _FeedbackInd(weights=a.weights.copy(), bias=a.bias)

    def _mutate(self, ind: _FeedbackInd) -> None:
        mask = self.rng.random(self.feature_dim) < self.config.mutation_rate
        ind.weights = ind.weights + mask * self.rng.standard_normal(self.feature_dim) * 0.05
        if self.rng.random() < self.config.mutation_rate:
            ind.bias += float(self.rng.standard_normal() * 0.05)

    # ---- persistence -----------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json_atomic(
            path,
            {
                "kind": "feedback_evolution",
                "feature_dim": self.feature_dim,
                "generation": self.generation,
                "counter": self.counter,
                "population": [
                    {
                        "weights": ind.weights.tolist(),
                        "bias": float(ind.bias),
                        "fitness": float(ind.fitness),
                    }
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json_guarded(path)
        if not payload or payload.get("kind") != "feedback_evolution":
            return
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.population = [
            _FeedbackInd(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                bias=float(ind["bias"]),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 11 — RLHFFeedbackEngine (main)
# ============================================================================

class RLHFFeedbackEngine:
    """
    RLHF-based Feedback Engine for Agent Improvement with optional enhancements.

    The enhanced path is now a *closed learning loop*:
        1. analyze_reasoning_trace()  → produces a report AND stores pending state
        2. record_outcome(reward)     → consumes the pending state, trains the optimizer
        3. save() / load()            → persists across restarts

    Callers that never invoke record_outcome get a stable, untrained optimizer
    — identical to v1's broken behaviour, but at least it's now explicit.
    """

    def __init__(
        self,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        # -- Config ------------------------------------------------
        if config is None:
            self.config = RLHFConfig()
        elif isinstance(config, RLHFConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = RLHFConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be RLHFConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.use_enhancements = bool(
            self.config.use_enhancements and _NUMPY_OK
        )
        if self.config.use_enhancements and not _NUMPY_OK:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "running in non-enhanced mode"
            )

        # -- Mutable per-engine state (config stays frozen) --------
        self._human_feedback_score = float(self.config.human_feedback_score)
        self._graph_metrics = dict(self.config.graph_metrics)
        self._last_known_metrics = dict(self.config.graph_metrics)

        self._lock = threading.RLock()
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if _NUMPY_OK else None
        )

        # -- History and pending state -----------------------------
        self.feedback_history: deque = deque(maxlen=self.config.history_maxlen)
        self.baseline_metrics: Optional[Dict[str, float]] = None
        self._pending: Optional[PendingFeedback] = None

        # -- Optimizer selection -----------------------------------
        self.distillation_optimizer: Optional[Any] = None
        if self.use_enhancements:
            if self.config.use_evolutionary:
                self.distillation_optimizer = EvolutionaryFeedbackOptimizer(
                    self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.distillation_optimizer = FeedbackDistillationOptimizer(
                    self.config, rng=self.rng
                )

        # Backwards-compat alias
        self.cost_optimizer = self.distillation_optimizer

    # ------------------------------------------------------------------
    # Public: analyze a trace (pure w.r.t. the optimizer; records pending)
    # ------------------------------------------------------------------
    def analyze_reasoning_trace(
        self,
        reasoning_trace: List[Dict[str, Any]],
        task_type: str,
        execution_time: float,
        success: bool,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        # -- Parse + analyse ---------------------------------------
        steps = self._parse_reasoning_steps(reasoning_trace)
        reasoning_quality = self._assess_reasoning_quality(steps, success)
        efficiency_analysis = self._analyze_efficiency(
            steps, execution_time, task_type=task_type
        )
        completeness_check = self._check_completeness(steps, task_type)
        sustainability_score = self._compute_sustainability_score(
            execution_time, len(steps)
        )

        # -- Feedback items ----------------------------------------
        feedback_items: List[FeedbackItem] = []
        feedback_items.extend(
            self._generate_reasoning_feedback(reasoning_quality, steps)
        )
        feedback_items.extend(
            self._generate_efficiency_feedback(efficiency_analysis)
        )
        feedback_items.extend(
            self._generate_completeness_feedback(completeness_check)
        )
        feedback_items.extend(
            self._generate_accuracy_feedback(
                reasoning_quality, success
            )
        )
        feedback_items.extend(
            self._generate_sustainability_feedback(
                sustainability_score, execution_time
            )
        )

        # -- Effective graph / human feedback ----------------------
        gm = dict(graph_metrics) if graph_metrics is not None else dict(
            self._graph_metrics
        )
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else self._human_feedback_score
        )

        # -- Overall score -----------------------------------------
        pending: Optional[PendingFeedback] = None
        if self.distillation_optimizer is not None:
            state = FeedbackState(
                reasoning_score=reasoning_quality["score"],
                efficiency_score=efficiency_analysis["score"],
                completeness_score=completeness_check["score"],
                sustainability_score=sustainability_score,
                success=success,
                graph_metrics=gm,
                human_feedback=hf,
            )
            overall_score, state_vec, t_scores, gate = (
                self.distillation_optimizer.predict_score(
                    state, exploration=False
                )
            )
            pending = PendingFeedback(
                state_vec=state_vec,
                teacher_scores=t_scores,
                gate_probs=gate,
                overall_score=float(overall_score),
                task_type=task_type,
            )
        else:
            overall_score = self._calculate_overall_score(
                reasoning_quality,
                efficiency_analysis,
                completeness_check,
                sustainability_score,
                success,
            )

        # -- Suggestions -------------------------------------------
        suggestions = self._generate_improvement_suggestions(
            feedback_items,
            reasoning_quality,
            efficiency_analysis,
            current_overall_score=overall_score,
        )

        analyzed_at = datetime.now(timezone.utc).isoformat()
        report = FeedbackReport(
            overall_score=round(float(overall_score), 3),
            reasoning_quality=reasoning_quality["level"].value,
            reasoning_score=round(float(reasoning_quality["score"]), 3),
            efficiency_score=round(float(efficiency_analysis["score"]), 3),
            completeness_score=round(float(completeness_check["score"]), 3),
            sustainability_score=round(float(sustainability_score), 3),
            feedback_items=[fi.to_dict() for fi in feedback_items],
            improvement_suggestions=suggestions,
            step_analysis=[s.to_dict() for s in steps],
            metrics={
                "total_steps": len(steps),
                "avg_step_duration": efficiency_analysis["avg_step_duration"],
                "redundant_steps": efficiency_analysis["redundant_steps"],
                "tool_usage_efficiency": efficiency_analysis["tool_efficiency"],
                "execution_time": float(execution_time),
            },
            task_type=task_type,
            success=bool(success),
            analyzed_at=analyzed_at,
            enhanced=self.use_enhancements,
        )

        if self.use_enhancements:
            report.graph_metrics = dict(gm)
            report.human_feedback_score = float(hf)
            if self.distillation_optimizer is not None:
                report.optimizer_stats = self._optimizer_stats()

        # -- Bookkeeping -------------------------------------------
        with self._lock:
            self._pending = pending
            self.feedback_history.append(report)

        # -- Observability -----------------------------------------
        self.metrics.observe(
            "rlhf_analyses_total", 1.0, task_type=task_type,
            enhanced=str(self.use_enhancements),
        )
        self.metrics.observe("rlhf_overall_score", report.overall_score)
        self.metrics.observe("rlhf_reasoning_score", report.reasoning_score)
        self.metrics.observe("rlhf_efficiency_score", report.efficiency_score)
        self.metrics.observe("rlhf_completeness_score", report.completeness_score)
        self.metrics.observe("rlhf_sustainability_score", report.sustainability_score)
        for fi in feedback_items:
            self.metrics.observe(
                "rlhf_feedback_items_total", 1.0,
                severity=fi.severity.value, category=fi.category.value,
            )

        return report.to_dict()

    # ------------------------------------------------------------------
    # Public: close the learning loop
    # ------------------------------------------------------------------
    def record_outcome(self, reward: float) -> bool:
        """
        Provide the true outcome reward in [0,1] for the most recent
        analyze_reasoning_trace() call. Trains the optimizer.

        Returns True if a training sample was accepted.
        """
        if not self.use_enhancements or self.distillation_optimizer is None:
            return False
        with self._lock:
            if self._pending is None:
                logger.debug(
                    "record_outcome(): no pending feedback state; "
                    "call analyze_reasoning_trace() first"
                )
                return False
            pending = self._pending
            try:
                self.distillation_optimizer.update(
                    pending.state_vec,
                    pending.teacher_scores,
                    pending.gate_probs,
                    float(reward),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer update failed: %s", exc)
                return False
            finally:
                self._pending = None
        self.metrics.observe("rlhf_outcome_reward", float(reward))
        return True

    # ------------------------------------------------------------------
    # Public: update the live human feedback score
    # ------------------------------------------------------------------
    def record_human_feedback(self, score: float) -> None:
        score = float(score)
        if not 0.0 <= score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        with self._lock:
            self._human_feedback_score = score
        self.metrics.observe("rlhf_human_feedback_score", score)
        logger.info("Human feedback score updated to %.3f", score)

    # ------------------------------------------------------------------
    # Public: persistence + summary
    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.distillation_optimizer is None:
            return
        try:
            self.distillation_optimizer.save(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.distillation_optimizer is None:
            return
        try:
            self.distillation_optimizer.load(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Load failed: %s", exc)

    def set_baseline(self, metrics: Mapping[str, float]) -> None:
        self.baseline_metrics = {k: float(v) for k, v in metrics.items()}

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            total = len(self.feedback_history)
            if not total:
                return {"total_analyses": 0}
            scores = [r.overall_score for r in self.feedback_history]
            successes = sum(1 for r in self.feedback_history if r.success)
            return {
                "total_analyses": total,
                "average_score": sum(scores) / total,
                "best_score": max(scores),
                "worst_score": min(scores),
                "success_rate": successes / total,
                "optimizer_stats": self._optimizer_stats(),
            }

    # ------------------------------------------------------------------
    # Public: comparative analysis (typed history, real timestamps)
    # ------------------------------------------------------------------
    def get_comparative_analysis(self, task_type: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            history = list(self.feedback_history)
        if not history:
            return {"message": "No historical data available"}
        if task_type:
            history = [h for h in history if h.task_type == task_type]
        if not history:
            return {"message": f"No historical data for task type: {task_type}"}

        scores = [h.overall_score for h in history]
        success_rate = sum(1 for h in history if h.success) / len(history)

        trend = "stable"
        n = min(5, len(scores) // 2)
        if n >= 1:
            first = sum(scores[:n]) / n
            last = sum(scores[-n:]) / n
            if last - first > 0.02:
                trend = "improving"
            elif first - last > 0.02:
                trend = "declining"

        quality_counts: Dict[str, int] = {}
        for h in history:
            q = h.reasoning_quality
            quality_counts[q] = quality_counts.get(q, 0) + 1

        return {
            "total_executions": len(history),
            "average_score": sum(scores) / len(scores),
            "best_score": max(scores),
            "worst_score": min(scores),
            "success_rate": success_rate,
            "trend": trend,
            "quality_distribution": quality_counts,
        }

    # ==================================================================
    # Internal: base analysis (from v1, largely preserved)
    # ==================================================================
    def _parse_reasoning_steps(
        self, trace: List[Dict[str, Any]]
    ) -> List[ReasoningStep]:
        steps: List[ReasoningStep] = []
        for i, step_data in enumerate(trace):
            if not isinstance(step_data, Mapping):
                logger.warning("Reasoning step %d is not a dict; skipping", i)
                continue
            steps.append(
                ReasoningStep(
                    step_id=i,
                    action=str(step_data.get("action", "unknown")),
                    thought=str(step_data.get("thought", "")),
                    observation=step_data.get("observation"),
                    tool_used=step_data.get("tool"),
                    duration=step_data.get("duration"),
                )
            )
        return steps

    def _assess_reasoning_quality(
        self, steps: List[ReasoningStep], success: bool
    ) -> Dict[str, Any]:
        score = 0.0
        issues: List[str] = []

        has_clear_plan = any("plan" in step.thought.lower() for step in steps[:3])
        if has_clear_plan:
            score += 0.2
        else:
            issues.append("No clear planning phase detected")

        has_refinement = any(
            "refine" in step.thought.lower() or "improve" in step.thought.lower()
            for step in steps
        )
        if has_refinement:
            score += 0.15

        has_error_handling = any(
            "error" in step.thought.lower() or "retry" in step.thought.lower()
            for step in steps
        )
        if has_error_handling:
            score += 0.15

        meaningful_thoughts = sum(
            1 for step in steps if step.thought and len(step.thought.split()) > 3
        )
        thought_quality = meaningful_thoughts / max(len(steps), 1)
        score += thought_quality * 0.3

        if success:
            score += 0.2

        if score >= 0.8:
            level = ReasoningQuality.EXCELLENT
        elif score >= 0.6:
            level = ReasoningQuality.GOOD
        elif score >= 0.4:
            level = ReasoningQuality.FAIR
        else:
            level = ReasoningQuality.POOR

        return {
            "score": _clamp(score),
            "level": level,
            "issues": issues,
            "has_planning": has_clear_plan,
            "has_refinement": has_refinement,
            "has_error_handling": has_error_handling,
        }

    # Task types that benefit from tool use — the multiplicative penalty
    # is only applied to these. Other tasks are scored on cleanliness alone.
    _TOOL_DEPENDENT_TASKS = frozenset(
        {"research", "information_retrieval", "data_analysis", "web_research"}
    )

    def _analyze_efficiency(
        self,
        steps: List[ReasoningStep],
        total_time: float,
        task_type: str = "",
    ) -> Dict[str, Any]:
        step_durations = [s.duration for s in steps if s.duration is not None]
        avg_duration = (
            sum(step_durations) / len(step_durations) if step_durations else 0.0
        )
        actions = [s.action for s in steps]
        redundant = len(actions) - len(set(actions))
        tool_steps = [s for s in steps if s.tool_used]
        tool_efficiency = len(tool_steps) / max(len(steps), 1)
        redundancy_penalty = redundant / max(len(steps), 1)
        base_cleanliness = max(0.0, 1.0 - redundancy_penalty)

        # Tool usage is weighted multiplicatively ONLY for tool-dependent
        # tasks. For other tasks, cleanliness alone determines the score
        # (fixes the v1 behaviour where no-tool reasoning scored 0).
        if task_type in self._TOOL_DEPENDENT_TASKS:
            efficiency_score = base_cleanliness * (0.5 + 0.5 * tool_efficiency)
        else:
            efficiency_score = base_cleanliness

        return {
            "score": _clamp(efficiency_score),
            "avg_step_duration": avg_duration,
            "redundant_steps": redundant,
            "tool_efficiency": tool_efficiency,
            "total_steps": len(steps),
        }

    def _check_completeness(
        self, steps: List[ReasoningStep], task_type: str
    ) -> Dict[str, Any]:
        score = 0.0
        missing_elements: List[str] = []

        has_search = any(
            "search" in step.action.lower() or "retrieve" in step.action.lower()
            for step in steps
        )
        if has_search:
            score += 0.3
        else:
            missing_elements.append("Information gathering phase")

        has_analysis = any(
            "analyze" in step.thought.lower() or "evaluate" in step.thought.lower()
            for step in steps
        )
        if has_analysis:
            score += 0.3
        else:
            missing_elements.append("Analysis phase")

        # Search the full trace for a conclusion — not just the last 3 steps.
        has_conclusion = any(
            "conclude" in step.thought.lower() or "synthesize" in step.thought.lower()
            for step in steps
        )
        if has_conclusion:
            score += 0.4
        else:
            missing_elements.append("Synthesis/conclusion phase")

        return {
            "score": _clamp(score),
            "missing_elements": missing_elements,
            "has_search": has_search,
            "has_analysis": has_analysis,
            "has_conclusion": has_conclusion,
        }

    def _compute_sustainability_score(
        self, execution_time: float, step_count: int
    ) -> float:
        """
        1/(1 + t/scale) with a step-count penalty. Bounded, scale-invariant.
        1.0 = instant + minimal steps; approaches 0 for slow/long traces.
        """
        t = max(0.0, float(execution_time))
        scale = max(self.config.sustainability_time_scale, 1e-9)
        base = 1.0 / (1.0 + t / scale)
        step_penalty = min(step_count / self.config.sustainability_step_scale, 1.0)
        return _clamp(base * (1.0 - 0.3 * step_penalty))

    def _calculate_overall_score(
        self,
        reasoning: Dict[str, Any],
        efficiency: Dict[str, Any],
        completeness: Dict[str, Any],
        sustainability_score: float,
        success: bool,
    ) -> float:
        """Non-enhanced path — uses the config's MODP weights directly."""
        w = self.config.modp_weights
        total = sum(w)
        wn = [x / total for x in w]
        score = (
            wn[0] * reasoning["score"]
            + wn[1] * efficiency["score"]
            + wn[2] * completeness["score"]
            + wn[3] * sustainability_score
        )
        if success:
            score += 0.05
        return _clamp(score)

    # ==================================================================
    # Internal: feedback generation
    # ==================================================================
    def _generate_reasoning_feedback(
        self, quality: Dict[str, Any], steps: List[ReasoningStep]
    ) -> List[FeedbackItem]:
        feedback: List[FeedbackItem] = []
        if not quality["has_planning"]:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.REASONING,
                    severity=FeedbackSeverity.MAJOR,
                    message="No clear planning phase detected in reasoning trace",
                    suggestion=(
                        "Start with explicit planning: break down the task, "
                        "identify required information, and outline approach"
                    ),
                )
            )
        if not quality["has_error_handling"]:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.REASONING,
                    severity=FeedbackSeverity.MINOR,
                    message="Limited error handling observed",
                    suggestion="Add explicit error checking and recovery strategies",
                )
            )
        if quality["level"] == ReasoningQuality.POOR:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.REASONING,
                    severity=FeedbackSeverity.CRITICAL,
                    message="Overall reasoning quality is poor",
                    suggestion=(
                        "Focus on: 1) Clear problem decomposition, "
                        "2) Explicit intermediate goals, 3) Verification steps"
                    ),
                )
            )
        return feedback

    def _generate_efficiency_feedback(
        self, efficiency: Dict[str, Any]
    ) -> List[FeedbackItem]:
        feedback: List[FeedbackItem] = []
        if efficiency["redundant_steps"] > 2:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.EFFICIENCY,
                    severity=FeedbackSeverity.MAJOR,
                    message=(
                        f"Detected {efficiency['redundant_steps']} redundant steps"
                    ),
                    suggestion=(
                        "Cache intermediate results and avoid repeating similar actions"
                    ),
                )
            )
        if efficiency["tool_efficiency"] < 0.3 and efficiency["total_steps"] > 2:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.EFFICIENCY,
                    severity=FeedbackSeverity.MINOR,
                    message="Low tool utilization detected",
                    suggestion=(
                        "Leverage available tools more effectively for "
                        "information gathering"
                    ),
                )
            )
        return feedback

    def _generate_completeness_feedback(
        self, completeness: Dict[str, Any]
    ) -> List[FeedbackItem]:
        feedback: List[FeedbackItem] = []
        for missing in completeness["missing_elements"]:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.COMPLETENESS,
                    severity=FeedbackSeverity.MAJOR,
                    message=f"Missing: {missing}",
                    suggestion=f"Add explicit {missing.lower()} to reasoning process",
                )
            )
        return feedback

    def _generate_accuracy_feedback(
        self, reasoning: Dict[str, Any], success: bool
    ) -> List[FeedbackItem]:
        """
        Emit ACCURACY feedback when the agent reasoned well but failed, or
        reasoned poorly but happened to succeed. Distinguishes accuracy
        problems from reasoning-quality problems.
        """
        feedback: List[FeedbackItem] = []
        if not success and reasoning["level"] in (
            ReasoningQuality.EXCELLENT, ReasoningQuality.GOOD,
        ):
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.ACCURACY,
                    severity=FeedbackSeverity.CRITICAL,
                    message=(
                        "Reasoning quality is high but the task failed — "
                        "accuracy issue in tool use or final answer"
                    ),
                    suggestion=(
                        "Verify tool inputs/outputs and add a final "
                        "self-consistency check before answering"
                    ),
                )
            )
        if success and reasoning["level"] == ReasoningQuality.POOR:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.ACCURACY,
                    severity=FeedbackSeverity.MINOR,
                    message=(
                        "Task succeeded despite poor reasoning quality — "
                        "outcome may be brittle"
                    ),
                    suggestion=(
                        "Strengthen reasoning even when results look correct; "
                        "add explicit verification"
                    ),
                )
            )
        return feedback

    def _generate_sustainability_feedback(
        self, sustainability: float, execution_time: float
    ) -> List[FeedbackItem]:
        feedback: List[FeedbackItem] = []
        if sustainability < 0.4:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.SUSTAINABILITY,
                    severity=FeedbackSeverity.MAJOR,
                    message=(
                        f"Low sustainability (score={sustainability:.2f}, "
                        f"execution_time={execution_time:.1f}s)"
                    ),
                    suggestion=(
                        "Reduce step count, batch tool calls, or cache "
                        "intermediate results to lower execution cost"
                    ),
                )
            )
        elif sustainability < 0.6:
            feedback.append(
                FeedbackItem(
                    category=FeedbackCategory.SUSTAINABILITY,
                    severity=FeedbackSeverity.MINOR,
                    message=f"Moderate sustainability (score={sustainability:.2f})",
                    suggestion="Consider further optimising trace length",
                )
            )
        return feedback

    def _generate_improvement_suggestions(
        self,
        feedback_items: List[FeedbackItem],
        reasoning: Dict[str, Any],
        efficiency: Dict[str, Any],
        current_overall_score: float,
    ) -> List[str]:
        suggestions: List[str] = []
        critical = [f for f in feedback_items if f.severity == FeedbackSeverity.CRITICAL]
        if critical:
            suggestions.append(f"CRITICAL: {critical[0].suggestion}")

        major = [f for f in feedback_items if f.severity == FeedbackSeverity.MAJOR]
        for item in major[:3]:
            suggestions.append(f"{item.category.value.upper()}: {item.suggestion}")

        # Compare overall_score (not reasoning_score) to the rolling average —
        # v1 compared reasoning["score"] to the rolling overall score, which
        # was comparing apples to oranges.
        if len(self.feedback_history) > 5:
            recent = [r.overall_score for r in list(self.feedback_history)[-5:]]
            avg_score = sum(recent) / len(recent)
            if current_overall_score < avg_score - 0.05:
                suggestions.append(
                    f"Overall score below recent average "
                    f"({avg_score:.2f} vs {current_overall_score:.2f}). "
                    f"Review recent successful executions for patterns."
                )
        return suggestions

    # ==================================================================
    # Internal: optimizer stats
    # ==================================================================
    def _optimizer_stats(self) -> Dict[str, Any]:
        if self.distillation_optimizer is None:
            return {}
        stats: Dict[str, Any] = {
            "kind": type(self.distillation_optimizer).__name__,
            "counter": getattr(self.distillation_optimizer, "counter", None),
            "epsilon": getattr(self.distillation_optimizer, "epsilon", None),
        }
        if hasattr(self.distillation_optimizer, "replay"):
            stats["replay_size"] = len(self.distillation_optimizer.replay)
        return stats


# ============================================================================
# SECTION 12 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    engine = RLHFFeedbackEngine(
        {
            "use_enhancements": True,
            "use_distillation": True,
            "use_evolutionary": False,
            "modp_weights": [0.35, 0.25, 0.25, 0.15],
            "human_feedback_score": 0.6,
            "graph_metrics": {"centrality": 0.7, "connectivity": 0.6},
            "seed": 42,
            "model_path": Path("./models/rlhf_engine.json"),
        }
    )

    trace = [
        {"action": "plan", "thought": "I will plan the steps", "duration": 1.2},
        {"action": "search", "thought": "Search for sources", "tool": "web", "duration": 2.0},
        {"action": "analyze", "thought": "Analyze the results carefully", "duration": 1.5},
        {"action": "synthesize", "thought": "Synthesize final conclusion", "duration": 1.0},
    ]

    report = engine.analyze_reasoning_trace(
        reasoning_trace=trace,
        task_type="research",
        execution_time=6.5,
        success=True,
    )
    print(json.dumps(report, indent=2, default=str))

    # Train on the true outcome
    trained = engine.record_outcome(0.85)
    print(f"trained: {trained}")

    engine.record_human_feedback(0.8)

    print("summary:", json.dumps(engine.summary(), indent=2, default=str))
    print("comparative:", json.dumps(
        engine.get_comparative_analysis(task_type="research"), indent=2
    ))

    engine.save()
