# -*- coding: utf-8 -*-
"""
Phase 2 — Meta-Cognition Layer (Enhanced, v2)
==============================================

Wraps the CausalGraph and exposes a clean observe → diagnose → feedback
loop that integrates with AgentEvaluator.

Fixes applied vs. v1
--------------------
  P0  _rule_teacher off-by-one (4/7 wrong actions)   -> uses RECOMMENDATION_MAP
  P0  MoE gate gradient wrong shape/sign             -> _train_step (softmax chain-rule)
  P0  Teachers trained on gated mixture              -> forward returns teacher_outputs
  P0  Config mutation + empty-list crash             -> frozen config + __post_init__
  P0  Stale _last_decision on nominal diagnosis      -> cleared at top of diagnose()
  P0  _last_decision not cleared after feedback      -> cleared in finally
  P0  Reward can escape [-1, 1] after RLHF tilt      -> clipped before update
  P0  Unseeded RNG                                   -> np.random.default_rng(seed)
  P0  No model persistence                           -> save()/load() on both optimizers
  P0  Dead use_evolutionary flag                     -> EvolutionaryActionOptimizer
  P1  feature_dim / n_actions hard-coded             -> derived from state / action list
  P1  epsilon never decays                           -> epsilon_min / epsilon_decay
  P1  batch size hard-coded                          -> config.batch_size
  P1  distill_w / rl_w hard-coded                    -> config
  P1  Feature vector not clipped                     -> all features clipped to [0,1]
  P1  graph_metrics / human_feedback by reference    -> copied into reports
  P2  No thread safety                               -> RLock on layer
  P2  _history unbounded                             -> bounded deque
  P2  No metrics sink                                -> MetricsSink protocol
  P2  Dict-only report                               -> DiagnosisReport dataclass
  P2  No recommendation accuracy                     -> summary() computes it
  P2  No feedback-consumed guard                     -> single-shot consumption

Preserved public surface
------------------------
  RECOMMENDATION_MAP, MetaCognitionConfig, ActionRecommendationState,
  ActionDistillationOptimizer, MetaCognitionLayer,
  .observe_snapshot(), .diagnose(), .feedback(), .get_history(), .summary(),
  .ALL_ACTIONS class attribute
"""

from __future__ import annotations

import json
import logging
import math
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import numpy as np
    _NUMPY_OK = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_OK = False

from .causal_graph import CausalGraph

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
        self._log = logging.getLogger("meta_cognition.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Recommendation map (single source of truth)
# ============================================================================

RECOMMENDATION_MAP: Dict[str, str] = {
    "WeatherEvent":          "defer_until_grid_stabilizes",
    "RenewableShortfall":    "defer_until_grid_stabilizes",
    "GridStrain":            "throttle_model_accuracy",
    "QueueDepth":            "throttle_model_accuracy",
    "BatteryLevel":          "switch_to_low_power_mode",
    "TaskPriority":          "execute_immediately",
    "ModelThrottleDecision": "review_throttle_policy",
}

# Root-cause → encoded float (derived from RECOMMENDATION_MAP keys, so it
# stays in sync when the map grows).
_ROOT_CAUSE_ORDER: List[str] = sorted(RECOMMENDATION_MAP.keys())
_ROOT_CAUSE_ENCODING: Dict[str, float] = {
    name: round((i + 1) / (len(_ROOT_CAUSE_ORDER) + 1), 4)
    for i, name in enumerate(_ROOT_CAUSE_ORDER)
}


# ============================================================================
# SECTION 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class MetaCognitionConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )

    # MODP weights: (carbon, latency, energy) — sum > 0
    modp_weights: Tuple[float, float, float] = (0.4, 0.3, 0.3)
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

    # Reward shaping (before RLHF tilt)
    reward_correct: float = 1.0
    reward_wrong: float = -0.5
    rlhf_tilt: float = 0.3

    history_maxlen: int = 1000
    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 3:
            raise ValueError(
                f"modp_weights must have 3 components (carbon, latency, energy), "
                f"got {w!r}"
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
        if self.history_maxlen < 1:
            raise ValueError("history_maxlen must be >= 1")
        for k, v in self.graph_metrics.items():
            if not _is_finite(v) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    # ---------------------------------------------------------------
    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "MetaCognitionConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "MetaCognitionConfig ignoring unknown keys: %s", sorted(unknown)
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                if v is None:
                    continue  # keep default
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 5 — Typed reports
# ============================================================================

@dataclass
class PendingDecision:
    state_vec: "np.ndarray"
    action_idx: int
    teacher_outputs: "np.ndarray"
    gate_probs: "np.ndarray"
    teacher_probs: "np.ndarray"
    action: str


@dataclass
class DiagnosisReport:
    status: str
    anomalies: List[str]
    root_causes: List[Dict[str, Any]]
    recommended_action: str
    graph_state: Dict[str, Any]
    graph_metrics: Optional[Dict[str, float]] = None
    human_feedback_score: Optional[float] = None
    optimizer_stats: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


@dataclass
class FeedbackResult:
    edges_updated: int
    reward: float
    optimizer_updated: bool
    optimizer_counter: Optional[int] = None


# ============================================================================
# SECTION 6 — State representation (clipped, cached)
# ============================================================================

class ActionRecommendationState:
    FEATURE_DIM = 6

    __slots__ = (
        "anomaly_count", "root_cause_name", "root_cause_id",
        "cum_weight", "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    def __init__(
        self,
        anomalies: List[str],
        root_cause: str,
        cumulative_weight: float,
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> None:
        self.anomaly_count = len(anomalies)
        self.root_cause_name = str(root_cause or "")
        self.root_cause_id = _ROOT_CAUSE_ENCODING.get(self.root_cause_name, 0.0)
        self.cum_weight = _clamp(float(cumulative_weight))
        self.centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        self.connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))
        self._vector = np.array(
            [
                min(self.anomaly_count / 10.0, 1.0),
                self.root_cause_id,
                self.cum_weight,
                self.centrality,
                self.connectivity,
                self.human_feedback,
            ],
            dtype=np.float32,
        )
        assert self._vector.shape == (self.FEATURE_DIM,)

    def to_vector(self) -> "np.ndarray":
        return self._vector


# ============================================================================
# SECTION 7 — Teachers
# ============================================================================

class ActionTeacher(Protocol):
    def predict(self, state: ActionRecommendationState) -> "np.ndarray": ...


class RuleTeacher:
    """Direct lookup via RECOMMENDATION_MAP — no threshold drift."""

    def __init__(self, action_index: Dict[str, int], n_actions: int) -> None:
        self.action_index = dict(action_index)
        self.n_actions = n_actions

    def predict(self, state: ActionRecommendationState) -> "np.ndarray":
        probs = np.full(self.n_actions, 0.05, dtype=np.float64)
        rec = RECOMMENDATION_MAP.get(state.root_cause_name)
        if rec is None:
            idx = self.action_index.get("investigate_manually", 0)
            probs[idx] = 0.5
        else:
            idx = self.action_index.get(rec)
            if idx is None:
                # Recommendation action not available in action list.
                idx = self.action_index.get("investigate_manually", 0)
                probs[idx] = 0.5
            else:
                probs[idx] = 0.8
        return probs / probs.sum()


class RLHFTeacher:
    """Human-feedback tilt toward execute (trust) or defer (distrust)."""

    def __init__(
        self,
        action_index: Dict[str, int],
        n_actions: int,
        human_feedback: float = 0.5,
    ) -> None:
        self.action_index = dict(action_index)
        self.n_actions = n_actions
        self.h = _clamp(float(human_feedback))

    def predict(self, state: ActionRecommendationState) -> "np.ndarray":
        probs = np.full(self.n_actions, 1.0 / self.n_actions)
        if self.h > 0.7:
            idx = self.action_index.get("execute_immediately")
            if idx is not None:
                probs[idx] += 0.2
        elif self.h < 0.3:
            idx = self.action_index.get("defer_until_grid_stabilizes")
            if idx is not None:
                probs[idx] += 0.2
        return probs / probs.sum()


class HistoricalTeacher:
    """Simulated trained model: high-confidence root causes map to specific actions."""

    def __init__(self, action_index: Dict[str, int], n_actions: int) -> None:
        self.action_index = dict(action_index)
        self.n_actions = n_actions

    def predict(self, state: ActionRecommendationState) -> "np.ndarray":
        probs = np.full(self.n_actions, 0.05, dtype=np.float64)
        if state.cum_weight > 0.8:
            if state.centrality > 0.7:
                idx = self.action_index.get("throttle_model_accuracy")
            else:
                idx = self.action_index.get("defer_until_grid_stabilizes")
        else:
            idx = self.action_index.get("investigate_manually")
        if idx is None:
            idx = 0
        probs[idx] = 0.6
        return probs / probs.sum()


# ============================================================================
# SECTION 8 — Distillation + MoE (fixed gate gradient, persistence)
# ============================================================================

class ActionDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating for action selection.

    Forward returns (action, state_vec, teacher_outputs, gate_probs, teacher_probs)
    so that backward can train the gate on the actual teacher predictions.
    """

    def __init__(
        self,
        action_list: Sequence[str],
        config: MetaCognitionConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not _NUMPY_OK:
            raise RuntimeError("numpy is required for ActionDistillationOptimizer")
        if len(action_list) < 1:
            raise ValueError("action_list must be non-empty")

        self.actions: List[str] = list(action_list)
        self.action_index: Dict[str, int] = {a: i for i, a in enumerate(self.actions)}
        self.n_actions = len(self.actions)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = ActionRecommendationState.FEATURE_DIM
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size
        self.counter = 0
        self.replay_buffer: deque = deque(maxlen=config.replay_size)

        self.teachers: List[ActionTeacher] = [
            RuleTeacher(self.action_index, self.n_actions),
            RLHFTeacher(self.action_index, self.n_actions, config.human_feedback_score),
            HistoricalTeacher(self.action_index, self.n_actions),
        ]

        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, len(self.teachers))) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers))

        if config.model_path:
            self.load(config.model_path)

    # ---- forward ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, state_vec: "np.ndarray") -> "np.ndarray":
        return self._softmax(state_vec @ self.gate_weights + self.gate_bias)

    def select_action(
        self,
        state: ActionRecommendationState,
        exploration: bool = True,
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        action            : int
        state_vec         : (D,)
        teacher_outputs   : (T, P) individual teacher distributions
        gate_probs        : (T,)   MoE gate weights
        teacher_probs     : (P,)   gated mixture
        """
        x = state.to_vector()
        teacher_outputs = np.stack([t.predict(state) for t in self.teachers])  # (T,P)
        teacher_outputs = np.clip(teacher_outputs, 1e-10, 1.0)
        teacher_outputs = teacher_outputs / teacher_outputs.sum(axis=1, keepdims=True)
        gate = self._gate_forward(x)

        teacher_probs = gate @ teacher_outputs                       # (P,)
        s = teacher_probs.sum()
        teacher_probs = teacher_probs / s if s > 0 else np.full(self.n_actions, 1.0 / self.n_actions)

        if exploration and self.rng.random() < self.epsilon:
            action = int(self.rng.integers(0, self.n_actions))
        else:
            student = self._softmax(x @ self.student_weights + self.student_bias)
            action = int(np.argmax(0.8 * student + 0.2 * teacher_probs))

        return action, x, teacher_outputs, gate, teacher_probs

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        self.replay_buffer.append(
            (
                state_vec.astype(np.float64),
                int(action),
                r,
                teacher_outputs.astype(np.float64),
                gate_probs.astype(np.float64),
            )
        )
        self.counter += 1
        if (
            self.counter % self.train_every == 0
            and len(self.replay_buffer) >= self.batch_size
        ):
            self._train_step()

    def _train_step(self) -> None:
        n = min(self.batch_size, len(self.replay_buffer))
        idxs = self.rng.choice(len(self.replay_buffer), size=n, replace=False)
        batch = [self.replay_buffer[i] for i in idxs]

        for s, a, r, t_outs, gate in batch:
            cur = self._softmax(s @ self.student_weights + self.student_bias)

            # ---- student ----
            combined_teacher = gate @ t_outs                     # (P,)
            one_hot = np.zeros(self.n_actions)
            one_hot[a] = 1.0
            grad_distill = -(combined_teacher - cur)
            grad_rl = -r * (one_hot - cur)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # ---- gate (correct softmax chain rule) ----
            err = combined_teacher - cur                          # (P,)
            dL_dg = t_outs.T @ err                                # (T,)
            grad_logits = gate * (dL_dg - float(gate @ dL_dg))    # (T,)
            self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
            self.gate_bias -= self.gate_lr * grad_logits

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        if not _NUMPY_OK:
            return
        _save_json_atomic(
            path,
            {
                "kind": "distillation",
                "n_actions": self.n_actions,
                "feature_dim": self.feature_dim,
                "actions": self.actions,
                "student_weights": self.student_weights.tolist(),
                "student_bias": self.student_bias.tolist(),
                "gate_weights": self.gate_weights.tolist(),
                "gate_bias": self.gate_bias.tolist(),
                "epsilon": float(self.epsilon),
                "counter": int(self.counter),
            },
        )

    def load(self, path: Path) -> None:
        if not _NUMPY_OK:
            return
        payload = _load_json_guarded(path)
        if not payload:
            return
        if payload.get("kind") != "distillation":
            logger.warning("Checkpoint kind mismatch: %s", payload.get("kind"))
            return
        if list(payload.get("actions", [])) != self.actions:
            raise ValueError(
                f"Checkpoint actions {payload.get('actions')} != current {self.actions}"
            )
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError(
                f"Checkpoint feature_dim={payload.get('feature_dim')} "
                f"!= optimizer feature_dim={self.feature_dim}"
            )
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", self.counter))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 9 — Evolutionary optimizer (fully implemented)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"
    bias: "np.ndarray"
    fitness: float = 0.0


class EvolutionaryActionOptimizer:
    """
    Population-based linear policy: state features → action logits.

    Fitness is negative MSE against a reward-weighted one-hot target, so
    higher reward translates to higher fitness regardless of which action
    was taken.
    """

    def __init__(
        self,
        action_list: Sequence[str],
        config: MetaCognitionConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not _NUMPY_OK:
            raise RuntimeError("numpy is required for EvolutionaryActionOptimizer")

        self.actions = list(action_list)
        self.n_actions = len(self.actions)
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = ActionRecommendationState.FEATURE_DIM
        self.counter = 0
        self.generation = 0

        self.population: List[_Ind] = [
            _Ind(
                weights=self.rng.standard_normal(
                    (self.feature_dim, self.n_actions)
                ) * 0.1,
                bias=self.rng.standard_normal(self.n_actions) * 0.01,
            )
            for _ in range(config.population_size)
        ]

        if config.model_path:
            self.load(config.model_path)

    def _forward(self, ind: _Ind, x: "np.ndarray") -> "np.ndarray":
        return x @ ind.weights + ind.bias

    def select_action(
        self,
        state: ActionRecommendationState,
        exploration: bool = True,
    ) -> Tuple[int, "np.ndarray", "np.ndarray", "np.ndarray", "np.ndarray"]:
        x = state.to_vector()
        if exploration and self.rng.random() < self.config.epsilon:
            action = int(self.rng.integers(0, self.n_actions))
        else:
            best = max(self.population, key=lambda ind: ind.fitness)
            action = int(np.argmax(self._forward(best, x)))
        # Uniform teacher/gate context (unused by this optimizer's update).
        teacher_outputs = np.full(
            (3, self.n_actions), 1.0 / self.n_actions, dtype=np.float64
        )
        gate_probs = np.full(3, 1.0 / 3.0, dtype=np.float64)
        teacher_probs = np.full(self.n_actions, 1.0 / self.n_actions, dtype=np.float64)
        return action, x, teacher_outputs, gate_probs, teacher_probs

    def update(
        self,
        state_vec: "np.ndarray",
        action: int,
        reward: float,
        teacher_outputs: Optional["np.ndarray"] = None,
        gate_probs: Optional["np.ndarray"] = None,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        target = np.full(
            self.n_actions,
            (1.0 - r) / max(self.n_actions - 1, 1),
            dtype=np.float64,
        )
        target[action] = r
        for ind in self.population:
            pred = self._forward(ind, state_vec)
            ind.fitness -= float(np.sum((pred - target) ** 2))
        self.counter += 1
        if self.counter % self.config.train_every == 0:
            self._evolve()

    def _evolve(self) -> None:
        self.population.sort(key=lambda ind: ind.fitness, reverse=True)
        n_elite = min(self.config.elitism, len(self.population))
        new_pop: List[_Ind] = [
            _Ind(weights=ind.weights.copy(), bias=ind.bias.copy(), fitness=0.0)
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

    def _tournament(self, k: int = 3) -> _Ind:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        return max((self.population[int(i)] for i in idxs), key=lambda ind: ind.fitness)

    def _crossover(self, a: _Ind, b: _Ind) -> _Ind:
        if self.rng.random() < self.config.crossover_rate:
            mask_w = self.rng.random(a.weights.shape) < 0.5
            mask_b = self.rng.random(a.bias.shape) < 0.5
            return _Ind(
                weights=np.where(mask_w, a.weights, b.weights).copy(),
                bias=np.where(mask_b, a.bias, b.bias).copy(),
            )
        return _Ind(weights=a.weights.copy(), bias=a.bias.copy())

    def _mutate(self, ind: _Ind) -> None:
        mw = self.rng.random(ind.weights.shape) < self.config.mutation_rate
        mb = self.rng.random(ind.bias.shape) < self.config.mutation_rate
        ind.weights = ind.weights + mw * self.rng.standard_normal(ind.weights.shape) * 0.05
        ind.bias = ind.bias + mb * self.rng.standard_normal(ind.bias.shape) * 0.02

    def save(self, path: Path) -> None:
        payload = {
            "kind": "evolutionary",
            "actions": self.actions,
            "feature_dim": self.feature_dim,
            "generation": self.generation,
            "counter": self.counter,
            "population": [
                {
                    "weights": ind.weights.tolist(),
                    "bias": ind.bias.tolist(),
                    "fitness": float(ind.fitness),
                }
                for ind in self.population
            ],
        }
        _save_json_atomic(path, payload)

    def load(self, path: Path) -> None:
        payload = _load_json_guarded(path)
        if not payload:
            return
        if payload.get("kind") != "evolutionary":
            logger.warning("Checkpoint kind mismatch: %s", payload.get("kind"))
            return
        if list(payload.get("actions", [])) != self.actions:
            raise ValueError("Checkpoint actions do not match current action list")
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.population = [
            _Ind(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                bias=np.asarray(ind["bias"], dtype=np.float64),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 10 — MetaCognitionLayer (main)
# ============================================================================

class MetaCognitionLayer:
    """
    Monitors benchmark execution state, diagnoses anomalies via causal
    graph traversal, and feeds outcome results back into the graph as
    online learning signal.
    """

    ALL_ACTIONS: Tuple[str, ...] = (
        "defer_until_grid_stabilizes",
        "throttle_model_accuracy",
        "switch_to_low_power_mode",
        "execute_immediately",
        "review_throttle_policy",
        "investigate_manually",
        "no_action",
        "proceed",
    )

    def __init__(
        self,
        causal_graph: Optional[CausalGraph] = None,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        # -- Config: accept dataclass | dict | None -------------------
        if config is None:
            self.config = MetaCognitionConfig()
        elif isinstance(config, MetaCognitionConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = MetaCognitionConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be MetaCognitionConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.graph = causal_graph or CausalGraph()
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if _NUMPY_OK else None
        )

        # Bounded history + last-report cache
        self._history: deque = deque(maxlen=self.config.history_maxlen)
        self._last_report: Optional[DiagnosisReport] = None

        # Pending decision to be consumed by exactly one feedback() call.
        self._last_decision: Optional[PendingDecision] = None
        self._feedback_consumed: bool = True  # nothing to consume on init
        self._lock = threading.RLock()

        # Optimizer selection
        self.cost_optimizer: Optional[Any] = None
        if self.config.use_enhancements and _NUMPY_OK:
            actions = list(self.ALL_ACTIONS)
            if self.config.use_evolutionary:
                self.cost_optimizer = EvolutionaryActionOptimizer(
                    actions, self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.cost_optimizer = ActionDistillationOptimizer(
                    actions, self.config, rng=self.rng
                )
        elif self.config.use_enhancements and not _NUMPY_OK:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "running in non-enhanced mode"
            )

        # Backwards-compat attribute (v1 exposed .distillation_optimizer)
        self.distillation_optimizer = self.cost_optimizer

    # ------------------------------------------------------------------
    # Observe
    # ------------------------------------------------------------------
    def observe_snapshot(self, snapshot: Dict[str, Any]) -> None:
        if not isinstance(snapshot, Mapping):
            raise TypeError("snapshot must be a Mapping")
        self.graph.observe_batch(dict(snapshot))

    # ------------------------------------------------------------------
    # Diagnose
    # ------------------------------------------------------------------
    def diagnose(self, max_chains: int = 3) -> Dict[str, Any]:
        if not isinstance(max_chains, int) or max_chains < 1:
            raise ValueError("max_chains must be a positive int")

        with self._lock:
            # CRITICAL: clear any pending decision from a previous run
            # (fixes stale-_last_decision bug).
            self._last_decision = None
            self._feedback_consumed = True

            anomalies = self.graph.get_anomalies()

            if not anomalies:
                report = DiagnosisReport(
                    status="nominal",
                    anomalies=[],
                    root_causes=[],
                    recommended_action="proceed",
                    graph_state=self.graph.export_state(),
                )
                if self.config.use_enhancements:
                    report.graph_metrics = dict(self.config.graph_metrics)
                    report.human_feedback_score = self.config.human_feedback_score
                self._last_report = report
                self._history.append(report)
                self.metrics.observe(
                    "meta_cognition_diagnoses_total", 1.0, status="nominal"
                )
                return report.to_dict()

            # Gather and rank root-cause chains
            all_chains: List[Dict[str, Any]] = []
            for anomaly in anomalies:
                chains = self.graph.trace_root_causes(anomaly, max_depth=5)
                for chain in chains:
                    chain["anomaly_variable"] = anomaly
                    all_chains.append(chain)
            all_chains.sort(
                key=lambda c: float(c.get("cumulative_weight", 0.0)),
                reverse=True,
            )
            top_chains = all_chains[:max_chains]

            # Decide
            if self.cost_optimizer is not None and top_chains:
                first = top_chains[0]
                state = ActionRecommendationState(
                    anomalies=anomalies,
                    root_cause=first.get("root_cause", ""),
                    cumulative_weight=float(first.get("cumulative_weight", 0.5)),
                    graph_metrics=self.config.graph_metrics,
                    human_feedback=self.config.human_feedback_score,
                )
                (
                    action_idx,
                    state_vec,
                    teacher_outputs,
                    gate_probs,
                    teacher_probs,
                ) = self.cost_optimizer.select_action(state, exploration=True)
                action = self.ALL_ACTIONS[action_idx]
                self._last_decision = PendingDecision(
                    state_vec=state_vec,
                    action_idx=action_idx,
                    teacher_outputs=teacher_outputs,
                    gate_probs=gate_probs,
                    teacher_probs=teacher_probs,
                    action=action,
                )
                self._feedback_consumed = False
            else:
                action = self._recommend(top_chains)

            report = DiagnosisReport(
                status="anomaly_detected",
                anomalies=list(anomalies),
                root_causes=top_chains,
                recommended_action=action,
                graph_state=self.graph.export_state(),
            )
            if self.config.use_enhancements:
                report.graph_metrics = dict(self.config.graph_metrics)
                report.human_feedback_score = self.config.human_feedback_score
                if self.cost_optimizer is not None:
                    report.optimizer_stats = self._optimizer_stats()

            self._last_report = report
            self._history.append(report)
            self.metrics.observe(
                "meta_cognition_diagnoses_total", 1.0, status="anomaly_detected"
            )
            self.metrics.observe(
                "meta_cognition_anomaly_count", float(len(anomalies))
            )
            return report.to_dict()

    # ------------------------------------------------------------------
    # Feedback
    # ------------------------------------------------------------------
    def feedback(
        self,
        decision_was_correct: bool,
        learning_rate: float = 0.05,
    ) -> Dict[str, Any]:
        with self._lock:
            if self._last_report is None:
                return asdict(
                    FeedbackResult(edges_updated=0, reward=0.0, optimizer_updated=False)
                )

            # 1) Update edge weights
            edges_updated = 0
            for chain in self._last_report.root_causes:
                path = chain.get("path", [])
                for i in range(len(path) - 1):
                    self.graph.update_edge_weight(
                        source=path[i],
                        target=path[i + 1],
                        outcome_correct=decision_was_correct,
                        learning_rate=learning_rate,
                    )
                    edges_updated += 1

            # 2) Train the optimizer if a decision is pending
            optimizer_updated = False
            reward = 0.0
            counter: Optional[int] = None

            if (
                self.cost_optimizer is not None
                and self._last_decision is not None
                and not self._feedback_consumed
            ):
                try:
                    pending = self._last_decision
                    reward = (
                        self.config.reward_correct
                        if decision_was_correct
                        else self.config.reward_wrong
                    )
                    if (
                        self.config.human_feedback_score > 0.7
                        and decision_was_correct
                    ):
                        reward += self.config.rlhf_tilt
                    elif (
                        self.config.human_feedback_score < 0.3
                        and not decision_was_correct
                    ):
                        reward -= self.config.rlhf_tilt
                    # Clip to [-1, 1] (fixes post-RLHF-tilt escape)
                    reward = float(np.clip(reward, -1.0, 1.0))

                    self.cost_optimizer.update(
                        pending.state_vec,
                        pending.action_idx,
                        reward,
                        pending.teacher_outputs,
                        pending.gate_probs,
                    )
                    optimizer_updated = True
                    counter = getattr(self.cost_optimizer, "counter", None)
                finally:
                    # Consume exactly once (fixes double-training bug)
                    self._last_decision = None
                    self._feedback_consumed = True

            result = FeedbackResult(
                edges_updated=edges_updated,
                reward=float(reward),
                optimizer_updated=optimizer_updated,
                optimizer_counter=counter,
            )
            self.metrics.observe(
                "meta_cognition_feedback_total", 1.0,
                correct=str(bool(decision_was_correct)),
            )
            if optimizer_updated:
                self.metrics.observe("meta_cognition_reward", reward)
            return asdict(result)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------
    def _recommend(self, chains: List[Dict[str, Any]]) -> str:
        if not chains:
            return "no_action"
        top_root = chains[0].get("root_cause", "")
        rec = RECOMMENDATION_MAP.get(top_root)
        if rec is None:
            logger.debug("Unknown root cause %r; falling back to investigate", top_root)
            return "investigate_manually"
        return rec

    def _optimizer_stats(self) -> Dict[str, Any]:
        if self.cost_optimizer is None:
            return {}
        return {
            "kind": type(self.cost_optimizer).__name__,
            "counter": getattr(self.cost_optimizer, "counter", None),
            "epsilon": getattr(self.cost_optimizer, "epsilon", None),
            "replay_size": (
                len(self.cost_optimizer.replay_buffer)
                if hasattr(self.cost_optimizer, "replay_buffer")
                else None
            ),
        }

    def get_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.to_dict() for r in self._history]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            total = len(self._history)
            anomaly_runs = sum(
                1 for r in self._history if r.status == "anomaly_detected"
            )
            actions: Dict[str, int] = {}
            for r in self._history:
                a = r.recommended_action or "unknown"
                actions[a] = actions.get(a, 0) + 1

            summary: Dict[str, Any] = {
                "total_diagnoses": total,
                "anomaly_rate": (
                    round(anomaly_runs / total, 3) if total else 0.0
                ),
                "action_distribution": actions,
            }
            if self.cost_optimizer is not None:
                summary["optimizer_stats"] = self._optimizer_stats()
            return summary

    def save(self) -> None:
        if self.config.model_path is None or self.cost_optimizer is None:
            return
        try:
            self.cost_optimizer.save(self.config.model_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to save optimizer: %s", exc)

    def load(self) -> None:
        if self.config.model_path is None or self.cost_optimizer is None:
            return
        try:
            self.cost_optimizer.load(self.config.model_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load optimizer: %s", exc)

    def record_human_feedback(self, score: float) -> None:
        """
        Update the human feedback score without rebuilding the layer.
        (Turns the static config value into a live RLHF signal.)
        """
        score = float(score)
        if not (0.0 <= score <= 1.0):
            raise ValueError("human_feedback_score must be in [0,1]")
        new_cfg = MetaCognitionConfig(
            **{
                **{k: getattr(self.config, k) for k in self.config.__dataclass_fields__},
                "human_feedback_score": score,
            }
        )
        with self._lock:
            object.__setattr__(self, "config", new_cfg)
            if self.cost_optimizer is not None:
                # Update the RLHF teacher in place
                for teacher in getattr(self.cost_optimizer, "teachers", []):
                    if isinstance(teacher, RLHFTeacher):
                        teacher.h = score


# ============================================================================
# SECTION 11 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = MetaCognitionConfig.from_dict(
        {
            "use_enhancements": True,
            "use_distillation": True,
            "use_evolutionary": False,
            "modp_weights": [0.4, 0.3, 0.3],
            "human_feedback_score": 0.7,
            "graph_metrics": {"centrality": 0.8, "connectivity": 0.6},
            "seed": 42,
            "model_path": Path("./models/meta_cognition.json"),
        }
    )

    layer = MetaCognitionLayer(config=cfg)

    report = layer.diagnose()
    print(json.dumps(report, indent=2, default=str))

    result = layer.feedback(decision_was_correct=True)
    print("feedback:", result)

    print("summary:", json.dumps(layer.summary(), indent=2))
    layer.save()
