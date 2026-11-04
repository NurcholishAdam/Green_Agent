# -*- coding: utf-8 -*-
"""
Phase 5 — Dual-Graph AI Evaluator (Enhanced, v2)
================================================

Compares Graph A (actual agent decision path) against Graph B (ideal /
counterfactual path) using an approximate Graph Edit Distance (GED) algorithm.
Produces an Explainable AI (XAI) report explaining WHY a decision path was
suboptimal — including counterfactual deltas for each relabel gap.

Fixes applied vs. v1
--------------------
  P0  MoE gate gradient wrong shape/sign             -> _train_step (softmax chain-rule)
  P0  Teachers trained on fake inputs                -> forward now returns teacher_preds
  P0  Config mutation (modp_weights, graph_metrics)  -> frozen config + local normalization
  P0  Empty-list modp_weights ZeroDivisionError     -> __post_init__ validation
  P0  Distillation target from reward sign only      -> reward-weighted target
  P0  No seeded RNG                                  -> np.random.default_rng(seed)
  P1  use_evolutionary was a dead flag               -> full EvolutionaryEditCostOptimizer
  P1  No model persistence                           -> save()/load() on both optimizers
  P1  Graph metrics not used in severity             -> _classify_gaps now uses them
  P1  MODP reward dominated by constants             -> pluggable, real-metric-ready
  P1  Non-deterministic DP tie-breaking              -> OP_PRIORITY secondary key
  P1  Thread-unsafe _ideal_store                     -> IdealPathRegistry with RLock
  P1  register_ideal_path silently overwrites        -> overwrite=False by default
  P2  CostOptimizer protocol                         -> added
  P2  MetricsSink hooks                              -> added
  P2  EvaluationReport dataclass                     -> added
  P2  Counterfactual trace in XAI                    -> added
  P2  evaluate_batch                                 -> added
  P2  epsilon decay                                  -> added
  P2  Smooth RLHF severity shift                     -> added
  P2  step_index in corrective actions               -> added
  P2  Unknown-gap-type handling                      -> added

Preserved public surface
------------------------
  DecisionNode, DecisionEdge, DecisionGraph,
  DualGraphConfig, DualGraphEvaluator,
  EditCostTeacher, RuleBasedTeacher, RLHFTeacher, HistoricalTeacher,
  EditCostDistillationOptimizer (alias kept for compat)
"""

from __future__ import annotations

import copy
import json
import logging
import threading
from collections import deque
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

import numpy as np

logger = logging.getLogger(__name__)

FEATURE_DIM = 3  # seq_a_len_norm, seq_b_len_norm, centrality


# ============================================================================
# SECTION 1 — Graph primitives (validated)
# ============================================================================

@dataclass(frozen=True)
class DecisionNode:
    node_id: str
    action: str
    context_snapshot: Dict[str, Any] = field(default_factory=dict)
    outcome_delta_si: float = 0.0


@dataclass(frozen=True)
class DecisionEdge:
    source_id: str
    target_id: str
    transition_type: str = "followed_by"
    weight: float = 1.0


class DecisionGraph:
    """Single agent execution as a labelled sequence of decision nodes."""

    def __init__(self, graph_id: str, graph_type: str = "executor") -> None:
        if not graph_id:
            raise ValueError("graph_id must be non-empty")
        self.graph_id = graph_id
        self.graph_type = graph_type
        self.nodes: Dict[str, DecisionNode] = {}
        self.edges: List[DecisionEdge] = []
        self._sequence: List[str] = []

    def add_step(self, node: DecisionNode, after_id: Optional[str] = None) -> None:
        if node.node_id in self.nodes:
            raise ValueError(f"duplicate node_id {node.node_id!r}")
        if after_id is not None and after_id not in self.nodes:
            raise ValueError(
                f"after_id {after_id!r} not present; add it before linking"
            )
        self.nodes[node.node_id] = node
        self._sequence.append(node.node_id)
        if after_id is not None:
            self.edges.append(
                DecisionEdge(source_id=after_id, target_id=node.node_id)
            )

    def get_action_sequence(self) -> List[str]:
        return [self.nodes[nid].action for nid in self._sequence if nid in self.nodes]

    def total_outcome_delta(self) -> float:
        return sum(n.outcome_delta_si for n in self.nodes.values())

    def __len__(self) -> int:
        return len(self.nodes)

    def __bool__(self) -> bool:
        return bool(self.nodes)


# ============================================================================
# SECTION 2 — Edit operation (with relabel-cost provenance for auditability)
# ============================================================================

@dataclass
class EditOperation:
    op_type: str
    step_index: int
    actual_action: Optional[str] = None
    ideal_action: Optional[str] = None
    cost: float = 0.0
    gap_type: str = ""
    explanation: str = ""


# ============================================================================
# SECTION 3 — Configuration (frozen, validated)
# ============================================================================

_ALLOWED_GAP_TYPES = frozenset(
    {
        "unnecessary_deferral", "failed_to_defer",
        "wrong_optimization_strategy", "over_throttled", "under_throttled",
        "suboptimal_substitution", "spurious_action", "missing_action",
        "unknown_gap",
    }
)


@dataclass(frozen=True)
class DualGraphConfig:
    use_enhancements: bool = False
    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    modp_weights: Optional[Tuple[float, ...]] = None  # (energy, latency, carbon, quality)
    human_feedback_score: float = 0.5

    use_distillation: bool = True
    use_evolutionary: bool = False

    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1                # teacher-override probability
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    # Verdict thresholds (was hard-coded)
    verdict_near_optimal_max: float = 0.20
    verdict_suboptimal_max: float = 0.50

    # Relabel-cost clipping bounds
    relabel_cost_min: float = 0.5
    relabel_cost_max: float = 1.0

    def __post_init__(self) -> None:
        # Normalize modp_weights (empty/None/length => default 4-tuple).
        w = self.modp_weights
        if w is None or len(w) == 0:
            object.__setattr__(self, "modp_weights", (0.3, 0.3, 0.2, 0.2))
        else:
            if len(w) != 4:
                raise ValueError(
                    f"modp_weights must have 4 components (energy, latency, "
                    f"carbon, quality); got {len(w)}"
                )
            if any(float(x) < 0 for x in w) or sum(float(x) for x in w) <= 0:
                raise ValueError(f"modp_weights must be non-negative, got {w!r}")
            object.__setattr__(self, "modp_weights", tuple(float(x) for x in w))

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
        if self.relabel_cost_min >= self.relabel_cost_max:
            raise ValueError("relabel_cost_min must be < relabel_cost_max")
        if self.verdict_near_optimal_max >= self.verdict_suboptimal_max:
            raise ValueError(
                "verdict_near_optimal_max must be < verdict_suboptimal_max"
            )
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(f"graph_metric {k!r} must be in [0,1], got {v!r}")


# ============================================================================
# SECTION 4 — Pluggable reward (MODP)
# ============================================================================

class RewardFn(Protocol):
    def __call__(
        self,
        *,
        normalized_ged: float,
        critical_count: int,
        energy_kwh: Optional[float],
        latency_s: Optional[float],
        carbon_kg: Optional[float],
        human_feedback: float,
    ) -> float: ...


class MODPReward:
    """
    MODP reward over (energy, latency, carbon, quality).

    Quality = 1 - normalized_ged (higher is better).
    Energy / latency / carbon are supplied by the caller (from green metrics).
    If a term is None, it is treated as neutral (its contribution is dropped
    and remaining weights renormalize).

    Reward is bounded to [-1, 1] so the optimizer always sees a stable signal.
    """

    def __init__(self, weights: Tuple[float, ...] = (0.3, 0.3, 0.2, 0.2)) -> None:
        w = np.asarray(weights, dtype=np.float64)
        if len(w) != 4 or (w < 0).any() or w.sum() <= 0:
            raise ValueError(f"Invalid modp_weights: {weights!r}")
        self.weights = w / w.sum()

    def __call__(
        self,
        *,
        normalized_ged: float,
        critical_count: int,
        energy_kwh: Optional[float],
        latency_s: Optional[float],
        carbon_kg: Optional[float],
        human_feedback: float,
    ) -> float:
        quality = float(np.clip(1.0 - normalized_ged, 0.0, 1.0))
        # Optional terms: None -> neutral (excluded, weight renormalized).
        parts: List[Tuple[float, float]] = [(self.weights[3], quality)]
        if energy_kwh is not None:
            # 1 - min(1, E/E_scale); scale = 1e-3 kWh (a full second at ~3.6W)
            parts.append((self.weights[0], 1.0 - min(energy_kwh / 1e-3, 1.0)))
        if latency_s is not None:
            # 1 - min(1, L/L_scale); scale = timeout-equivalent 30 s
            parts.append((self.weights[1], 1.0 - min(latency_s / 30.0, 1.0)))
        if carbon_kg is not None:
            parts.append((self.weights[2], 1.0 - min(carbon_kg / 4e-4, 1.0)))

        total_w = sum(w for w, _ in parts)
        if total_w <= 0:
            return 0.0
        reward = sum(w * v for w, v in parts) / total_w

        # Critical gaps penalize proportionally.
        reward -= 0.05 * min(critical_count, 5)
        # Human feedback shifts baseline slightly (±0.05).
        reward += (human_feedback - 0.5) * 0.1
        return float(np.clip(reward, -1.0, 1.0))


# ============================================================================
# SECTION 5 — Metrics
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
        self._log = logging.getLogger("dual_graph.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 6 — Teachers (protocol + implementations)
# ============================================================================

class EditCostTeacher(Protocol):
    """Predicts the relabel edit cost from graph-level features."""
    def predict(
        self,
        seq_a_len: int,
        seq_b_len: int,
        graph_metrics: Dict[str, float],
    ) -> float: ...


class RuleBasedTeacher:
    def predict(self, seq_a_len, seq_b_len, graph_metrics):
        centrality = float(graph_metrics.get("centrality", 0.5))
        if centrality > 0.7:
            return 0.6
        if seq_a_len + seq_b_len > 10:
            return 0.8
        return 0.7


class RLHFTeacher:
    def __init__(self, human_feedback: float = 0.5) -> None:
        self.human_feedback = float(np.clip(human_feedback, 0.0, 1.0))

    def predict(self, seq_a_len, seq_b_len, graph_metrics):
        # Trust agent more -> lower relabel cost (more lenient on the ideal).
        # Trust agent less -> higher relabel cost (stricter on deviations).
        return float(np.clip(1.0 - self.human_feedback, 0.5, 1.0))


class HistoricalTeacher:
    """Linear model on (seq_a_len, seq_b_len, centrality) — bounded inputs."""

    def __init__(self, weights: Optional[np.ndarray] = None) -> None:
        w = weights if weights is not None else np.array([0.05, 0.1, 0.02])
        w = np.asarray(w, dtype=np.float64)
        if w.shape != (FEATURE_DIM,):
            raise ValueError(f"weights must have shape ({FEATURE_DIM},)")
        self.weights = w

    def predict(self, seq_a_len, seq_b_len, graph_metrics):
        feat = np.array([
            min(seq_a_len / 10.0, 1.0),
            min(seq_b_len / 10.0, 1.0),
            min(float(graph_metrics.get("centrality", 0.5)), 1.0),
        ])
        return float(np.dot(feat, self.weights)) + 0.7


# ============================================================================
# SECTION 7 — Optimizers (protocol + distillation + evolutionary)
# ============================================================================

class EditCostOptimizer(Protocol):
    """Interface for anything that predicts/trains the relabel cost."""
    def predict_relabel_cost(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> float: ...
    def predict_relabel_cost_with_context(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> Tuple[float, np.ndarray, np.ndarray, np.ndarray]:
        """Return (cost, state_vec, teacher_preds, gate_probs)."""
    def update(
        self,
        state_vec: np.ndarray,
        reward: float,
        *,
        teacher_preds: Optional[np.ndarray] = None,
        gate_probs: Optional[np.ndarray] = None,
    ) -> None: ...
    def save(self, path: Path) -> None: ...
    def load(self, path: Path) -> None: ...


# ---- persistence helpers ---------------------------------------------------

def _save_arrays(path: Path, **arrays: np.ndarray) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {k: np.asarray(v).tolist() for k, v in arrays.items()}
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    tmp.replace(path)


def _load_arrays(path: Path) -> Dict[str, np.ndarray]:
    path = Path(path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return {k: np.asarray(v) for k, v in payload.items()}


# ---- distillation ---------------------------------------------------------

class EditCostDistillationOptimizer:
    """
    Multi-teacher distillation + MoE gating for the relabel edit cost.

    Action space: continuous relabel cost in [relabel_cost_min, relabel_cost_max].

    Forward returns full context so backward can train the gate on real
    teacher predictions (fixes §2.1 / §2.2).
    """

    def __init__(
        self,
        config: DualGraphConfig,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = FEATURE_DIM
        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.7  # default relabel cost

        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay

        self.teachers: List[EditCostTeacher] = [
            RuleBasedTeacher(),
            RLHFTeacher(config.human_feedback_score),
            HistoricalTeacher(),
        ]

        # Gate: (feature_dim, n_teachers)
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, len(self.teachers))) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers))

        self.replay: deque = deque(maxlen=config.replay_size)
        self.counter = 0
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        if config.model_path:
            self.load(config.model_path)

    # ---- internals --------------------------------------------------------
    @staticmethod
    def _softmax(logits: np.ndarray) -> np.ndarray:
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        return self._softmax(state_vec @ self.gate_weights + self.gate_bias)

    def _state_features(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> np.ndarray:
        return np.array(
            [
                min(seq_a_len / 10.0, 1.0),
                min(seq_b_len / 10.0, 1.0),
                min(float(graph_metrics.get("centrality", 0.5)), 1.0),
            ],
            dtype=np.float32,
        )

    # ---- forward ----------------------------------------------------------
    def predict_relabel_cost_with_context(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> Tuple[float, np.ndarray, np.ndarray, np.ndarray]:
        state = self._state_features(seq_a_len, seq_b_len, graph_metrics)
        teacher_preds = np.array(
            [t.predict(seq_a_len, seq_b_len, graph_metrics) for t in self.teachers]
        )
        gate_probs = self._gate_forward(state)
        combined = float(gate_probs @ teacher_preds)

        if self.rng.random() < self.epsilon:
            predicted = combined
        else:
            student_pred = float(state @ self.student_weights + self.student_bias)
            predicted = 0.8 * student_pred + 0.2 * combined

        predicted = float(
            np.clip(predicted, self.config.relabel_cost_min, self.config.relabel_cost_max)
        )
        return predicted, state, teacher_preds, gate_probs

    def predict_relabel_cost(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> float:
        cost, *_ = self.predict_relabel_cost_with_context(
            seq_a_len, seq_b_len, graph_metrics
        )
        return cost

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: np.ndarray,
        reward: float,
        *,
        teacher_preds: Optional[np.ndarray] = None,
        gate_probs: Optional[np.ndarray] = None,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))

        if teacher_preds is None or gate_probs is None:
            # Legacy path: synthesise context (matches old behaviour but warning).
            logger.warning(
                "update() called without teacher_preds/gate_probs; "
                "gate gradient will be approximate. "
                "Prefer predict_relabel_cost_with_context() then pass its outputs."
            )
            teacher_preds = np.array(
                [t.predict(0, 0, {}) for t in self.teachers]
            )
            gate_probs = self._gate_forward(state_vec)

        self.replay.append(
            (
                state_vec.astype(np.float64),
                r,
                teacher_preds.astype(np.float64),
                gate_probs.astype(np.float64),
            )
        )
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay) >= self.batch_size:
            self._train_step()

    def _train_step(self) -> None:
        n = min(self.batch_size, len(self.replay))
        idxs = self.rng.choice(len(self.replay), size=n, replace=False)
        batch = [self.replay[i] for i in idxs]

        for state_vec, reward, teacher_preds, gate_probs in batch:
            # Reward-weighted target in [relabel_cost_min, relabel_cost_max].
            # reward = +1 -> relabel cost = max (more lenient on ideal)
            # reward = -1 -> relabel cost = min (stricter on ideal)
            target = (
                self.config.relabel_cost_min
                + (reward + 1.0) * 0.5
                * (self.config.relabel_cost_max - self.config.relabel_cost_min)
            )

            # ---- student update: MSE regression ----
            pred = float(state_vec @ self.student_weights + self.student_bias)
            err_student = pred - target
            self.student_weights -= self.lr * err_student * state_vec
            self.student_bias -= self.lr * err_student

            # ---- gate update (FIXED) ----
            # Forward: combined = gate_probs @ teacher_preds
            # L = 0.5 * (combined - target)^2
            # dL/d(logits_t) = (combined - target) * gate_probs[t]
            #                  * (teacher_preds[t] - combined)
            combined = float(gate_probs @ teacher_preds)
            err_gate = combined - target
            grad_logits = err_gate * gate_probs * (teacher_preds - combined)  # (T,)

            self.gate_weights -= self.gate_lr * np.outer(state_vec, grad_logits)
            self.gate_bias -= self.gate_lr * grad_logits

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_arrays(
            path,
            student_weights=self.student_weights,
            student_bias=self.student_bias,
            gate_weights=self.gate_weights,
            gate_bias=self.gate_bias,
            epsilon=np.asarray(self.epsilon),
            counter=np.asarray(self.counter),
            feature_dim=np.asarray(self.feature_dim),
        )
        logger.info("Saved distillation checkpoint to %s", path)

    def load(self, path: Path) -> None:
        data = _load_arrays(path)
        if not data:
            return
        if "feature_dim" in data and int(data["feature_dim"]) != self.feature_dim:
            raise ValueError(
                f"Checkpoint feature_dim={int(data['feature_dim'])} "
                f"does not match optimizer feature_dim={self.feature_dim}"
            )
        self.student_weights = data["student_weights"]
        self.student_bias = data["student_bias"]
        self.gate_weights = data["gate_weights"]
        self.gate_bias = data["gate_bias"]
        self.epsilon = float(data.get("epsilon", self.epsilon))
        self.counter = int(data.get("counter", self.counter))
        logger.info("Loaded distillation checkpoint from %s", path)


# Keep the original class name as an alias (backwards compat).
EditCostDistillationOptimizer_v1 = EditCostDistillationOptimizer


# ---- evolutionary ---------------------------------------------------------

@dataclass
class _Ind:
    weights: np.ndarray
    bias: float
    fitness: float = 0.0


class EvolutionaryEditCostOptimizer:
    """
    Population-based edit-cost predictor. Each individual is a linear
    model over (seq_a_len, seq_b_len, centrality). Fitness accumulates
    reward-weighted MSE-to-target across observed states; selection is
    tournament + uniform crossover + Gaussian mutation + elitism.
    """

    def __init__(
        self,
        config: DualGraphConfig,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = FEATURE_DIM
        self.population: List[_Ind] = [
            _Ind(
                weights=self.rng.standard_normal(self.feature_dim) * 0.1,
                bias=0.7 + self.rng.standard_normal() * 0.02,
            )
            for _ in range(config.population_size)
        ]
        self.generation = 0
        self.counter = 0
        if config.model_path:
            self.load(config.model_path)

    def _predict_with(self, ind: _Ind, state: np.ndarray) -> float:
        return float(np.clip(
            state @ ind.weights + ind.bias,
            self.config.relabel_cost_min,
            self.config.relabel_cost_max,
        ))

    def predict_relabel_cost_with_context(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> Tuple[float, np.ndarray, np.ndarray, np.ndarray]:
        state = np.array(
            [
                min(seq_a_len / 10.0, 1.0),
                min(seq_b_len / 10.0, 1.0),
                min(float(graph_metrics.get("centrality", 0.5)), 1.0),
            ],
            dtype=np.float32,
        )
        best = max(self.population, key=lambda ind: ind.fitness)
        cost = self._predict_with(best, state)
        # Uniform teacher context (this optimizer doesn't use teachers).
        teacher_preds = np.array([cost] * 3, dtype=np.float64)
        gate_probs = np.ones(3) / 3
        return cost, state, teacher_preds, gate_probs

    def predict_relabel_cost(
        self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]
    ) -> float:
        cost, *_ = self.predict_relabel_cost_with_context(
            seq_a_len, seq_b_len, graph_metrics
        )
        return cost

    def update(
        self,
        state_vec: np.ndarray,
        reward: float,
        *,
        teacher_preds: Optional[np.ndarray] = None,
        gate_probs: Optional[np.ndarray] = None,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        # Target: high reward -> high relabel cost (lenient on ideal).
        target = (
            self.config.relabel_cost_min
            + (r + 1.0) * 0.5
            * (self.config.relabel_cost_max - self.config.relabel_cost_min)
        )
        for ind in self.population:
            pred = self._predict_with(ind, state_vec)
            ind.fitness -= (pred - target) ** 2  # lower MSE => higher fitness
        self.counter += 1
        if self.counter % self.config.train_every == 0:
            self._evolve()

    # ---- genetic operators ------------------------------------------------
    def _evolve(self) -> None:
        self.population.sort(key=lambda ind: ind.fitness, reverse=True)
        n_elite = min(self.config.elitism, len(self.population))
        new_pop = [copy.deepcopy(ind) for ind in self.population[:n_elite]]
        while len(new_pop) < len(self.population):
            a = self._tournament()
            b = self._tournament()
            child = self._crossover(a, b)
            self._mutate(child)
            new_pop.append(child)
        self.population = new_pop
        self.generation += 1

    def _tournament(self, k: int = 3) -> _Ind:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        return max((self.population[i] for i in idxs), key=lambda ind: ind.fitness)

    def _crossover(self, a: _Ind, b: _Ind) -> _Ind:
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(self.feature_dim) < 0.5
            return _Ind(
                weights=np.where(mask, a.weights, b.weights).copy(),
                bias=float(a.bias if self.rng.random() < 0.5 else b.bias),
            )
        return _Ind(weights=a.weights.copy(), bias=a.bias)

    def _mutate(self, ind: _Ind) -> None:
        mask = self.rng.random(self.feature_dim) < self.config.mutation_rate
        ind.weights = ind.weights + mask * self.rng.standard_normal(self.feature_dim) * 0.05
        if self.rng.random() < self.config.mutation_rate:
            ind.bias += float(self.rng.standard_normal() * 0.02)

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
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
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        tmp.replace(path)
        logger.info("Saved evolutionary checkpoint to %s", path)

    def load(self, path: Path) -> None:
        path = Path(path)
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        self.generation = int(payload["generation"])
        self.counter = int(payload.get("counter", 0))
        self.population = [
            _Ind(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                bias=float(ind["bias"]),
                fitness=float(ind["fitness"]),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 8 — Thread-safe ideal-path registry
# ============================================================================

class IdealPathRegistry:
    def __init__(self) -> None:
        self._store: Dict[str, DecisionGraph] = {}
        self._lock = threading.RLock()

    def register(
        self, task_type: str, graph_b: DecisionGraph, *, overwrite: bool = False
    ) -> None:
        if not task_type:
            raise ValueError("task_type must be non-empty")
        if not isinstance(graph_b, DecisionGraph) or not graph_b:
            raise ValueError("graph_b must be a non-empty DecisionGraph")
        with self._lock:
            if task_type in self._store and not overwrite:
                raise KeyError(
                    f"ideal already registered for {task_type!r}; "
                    f"pass overwrite=True to replace"
                )
            self._store[task_type] = graph_b

    def get(self, task_type: str) -> Optional[DecisionGraph]:
        with self._lock:
            return self._store.get(task_type)

    def __contains__(self, task_type: str) -> bool:
        with self._lock:
            return task_type in self._store

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._store.keys())


# ============================================================================
# SECTION 9 — Evaluation report (typed result)
# ============================================================================

@dataclass
class EvaluationReport:
    status: str
    task_type: str
    graph_a_sequence: List[str]
    graph_b_sequence: List[str]
    ged_score: float
    normalized_ged: float
    reasoning_gaps: List[Dict[str, Any]]
    edit_operations: List[Dict[str, Any]]
    xai_explanation: str
    verdict: str
    corrective_actions: List[str]
    counterfactual_trace: List[Dict[str, Any]] = field(default_factory=list)
    relabel_cost: float = 0.0
    relabel_cost_source: str = "static"  # "static" | "distilled" | "evolutionary"
    modp_reward: Optional[float] = None
    optimizer_stats: Optional[Dict[str, Any]] = None
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "task_type": self.task_type,
            "graph_a_sequence": self.graph_a_sequence,
            "graph_b_sequence": self.graph_b_sequence,
            "ged_score": self.ged_score,
            "normalized_ged": self.normalized_ged,
            "reasoning_gaps": self.reasoning_gaps,
            "edit_operations": self.edit_operations,
            "xai_explanation": self.xai_explanation,
            "verdict": self.verdict,
            "corrective_actions": self.corrective_actions,
            "counterfactual_trace": self.counterfactual_trace,
            "relabel_cost": self.relabel_cost,
            "relabel_cost_source": self.relabel_cost_source,
            "modp_reward": self.modp_reward,
            "optimizer_stats": self.optimizer_stats,
            "message": self.message,
        }


# ============================================================================
# SECTION 10 — The evaluator
# ============================================================================

class DualGraphEvaluator:
    """
    GED-based evaluator with XAI reasoning-gap classification.

    Preserved public API:
        DualGraphEvaluator(config=None)
        .register_ideal_path(task_type, graph_b, *, overwrite=False)
        .build_ideal_from_sequence(task_type, action_sequence)
        .compute_ged(graph_a, graph_b) -> dict
        .evaluate(graph_a, task_type, graph_b=None) -> dict

    New API:
        .evaluate_batch(graphs_a, task_type, graph_b=None) -> List[dict]
        .ideal_registry  (IdealPathRegistry)
    """

    # Static defaults (fallback for non-enhanced mode).
    EDIT_COSTS: Dict[str, float] = {
        "insert": 1.0,
        "delete": 1.0,
        "relabel": 0.7,
        "no_op": 0.0,
    }
    # Tie-break priority (lower => preferred on equal cost).
    OP_PRIORITY: Dict[str, int] = {
        "no_op": 0, "relabel": 1, "delete": 2, "insert": 3,
    }
    CRITICAL_SUBSTITUTIONS: Dict[Tuple[str, str], str] = {
        ("defer",    "execute"):  "unnecessary_deferral",
        ("execute",  "defer"):    "failed_to_defer",
        ("quantize", "prune"):    "wrong_optimization_strategy",
        ("throttle", "execute"):  "over_throttled",
        ("execute",  "throttle"): "under_throttled",
        ("prune",    "quantize"): "wrong_optimization_strategy",
    }

    def __init__(
        self,
        config: Optional[DualGraphConfig] = None,
        *,
        reward_fn: Optional[RewardFn] = None,
        metrics: Optional[MetricsSink] = None,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        self.config = config or DualGraphConfig()
        self.reward_fn: RewardFn = reward_fn or MODPReward(self.config.modp_weights)
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else np.random.default_rng(self.config.seed)

        self._ideal_store = IdealPathRegistry()
        self._critical_gap_types = frozenset(self.CRITICAL_SUBSTITUTIONS.values())

        # Keep the original public attribute for backwards compat.
        self.distillation_optimizer: Optional[EditCostOptimizer] = None
        self.cost_optimizer: Optional[EditCostOptimizer] = None

        if self.config.use_enhancements:
            if self.config.use_evolutionary:
                self.cost_optimizer = EvolutionaryEditCostOptimizer(
                    self.config, rng=self.rng
                )
                self._relabel_cost_source = "evolutionary"
            elif self.config.use_distillation:
                self.cost_optimizer = EditCostDistillationOptimizer(
                    self.config, rng=self.rng
                )
                self._relabel_cost_source = "distilled"
            # Keep the legacy attribute name.
            self.distillation_optimizer = self.cost_optimizer
        else:
            self._relabel_cost_source = "static"

    # ------------------------------------------------------------------
    # Ideal-path registry
    # ------------------------------------------------------------------
    @property
    def ideal_registry(self) -> IdealPathRegistry:
        return self._ideal_store

    def register_ideal_path(
        self, task_type: str, graph_b: DecisionGraph, *, overwrite: bool = False
    ) -> None:
        self._ideal_store.register(task_type, graph_b, overwrite=overwrite)

    def build_ideal_from_sequence(
        self, task_type: str, action_sequence: Sequence[str]
    ) -> DecisionGraph:
        if not action_sequence:
            raise ValueError("action_sequence must be non-empty")
        graph_b = DecisionGraph(graph_id=f"ideal_{task_type}", graph_type="evaluator")
        prev_id: Optional[str] = None
        for i, action in enumerate(action_sequence):
            node = DecisionNode(node_id=f"ideal_step_{i}", action=str(action))
            graph_b.add_step(node, after_id=prev_id)
            prev_id = node.node_id
        self.register_ideal_path(task_type, graph_b, overwrite=True)
        return graph_b

    # ------------------------------------------------------------------
    # GED computation
    # ------------------------------------------------------------------
    def compute_ged(
        self, graph_a: DecisionGraph, graph_b: DecisionGraph
    ) -> Dict[str, Any]:
        seq_a = graph_a.get_action_sequence()
        seq_b = graph_b.get_action_sequence()

        relabel_cost, ctx = self._resolve_relabel_cost(len(seq_a), len(seq_b))
        edit_costs = {
            "insert": self.EDIT_COSTS["insert"],
            "delete": self.EDIT_COSTS["delete"],
            "relabel": relabel_cost,
            "no_op": self.EDIT_COSTS["no_op"],
        }

        ops = self._dp_align(seq_a, seq_b, edit_costs)
        ged_score = sum(op.cost for op in ops)
        max_len = max(len(seq_a), len(seq_b), 1)
        gaps = self._classify_gaps(ops)

        return {
            "ged_score": round(ged_score, 4),
            "normalized_ged": round(ged_score / max_len, 4),
            "edit_operations": [vars(op) for op in ops],
            "reasoning_gaps": gaps,
            "graph_a_sequence": seq_a,
            "graph_b_sequence": seq_b,
            "relabel_cost": relabel_cost,
            "relabel_cost_source": self._relabel_cost_source,
            "_context": ctx,  # internal, stripped by caller
            "_ops": ops,      # internal, for counterfactual trace
            "_seq_a": seq_a,
            "_seq_b": seq_b,
            "_edit_costs": edit_costs,
        }

    def _resolve_relabel_cost(
        self, seq_a_len: int, seq_b_len: int
    ) -> Tuple[float, Optional[Tuple[np.ndarray, np.ndarray, np.ndarray]]]:
        """
        Returns (cost, (state_vec, teacher_preds, gate_probs) | None).
        The context is only populated in enhanced mode (needed for update).
        """
        if self.cost_optimizer is None:
            return self.EDIT_COSTS["relabel"], None
        cost, state, t_preds, g_probs = self.cost_optimizer.predict_relabel_cost_with_context(
            seq_a_len, seq_b_len, self.config.graph_metrics
        )
        return float(cost), (state, t_preds, g_probs)

    # ------------------------------------------------------------------
    # DP alignment (deterministic tie-breaking)
    # ------------------------------------------------------------------
    def _dp_align(
        self, seq_a: List[str], seq_b: List[str], edit_costs: Dict[str, float]
    ) -> List[EditOperation]:
        m, n = len(seq_a), len(seq_b)
        # dp[i][j] = (cost, op_type)
        dp: List[List[Tuple[float, str]]] = [
            [(0.0, "no_op")] * (n + 1) for _ in range(m + 1)
        ]
        for i in range(1, m + 1):
            dp[i][0] = (dp[i - 1][0][0] + edit_costs["delete"], "delete")
        for j in range(1, n + 1):
            dp[0][j] = (dp[0][j - 1][0] + edit_costs["insert"], "insert")

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if seq_a[i - 1] == seq_b[j - 1]:
                    dp[i][j] = (dp[i - 1][j - 1][0], "no_op")
                    continue
                candidates = [
                    (dp[i - 1][j][0] + edit_costs["delete"], "delete"),
                    (dp[i][j - 1][0] + edit_costs["insert"], "insert"),
                    (dp[i - 1][j - 1][0] + edit_costs["relabel"], "relabel"),
                ]
                # Deterministic tie-break by OP_PRIORITY.
                dp[i][j] = min(
                    candidates,
                    key=lambda x: (x[0], self.OP_PRIORITY[x[1]]),
                )

        # Reconstruct
        ops: List[EditOperation] = []
        i, j = m, n
        while i > 0 or j > 0:
            _, op_type = dp[i][j]
            if op_type == "no_op" and i > 0 and j > 0:
                i -= 1
                j -= 1
            elif op_type == "relabel" and i > 0 and j > 0:
                actual = seq_a[i - 1]
                ideal = seq_b[j - 1]
                gap_type = self.CRITICAL_SUBSTITUTIONS.get(
                    (actual, ideal), "suboptimal_substitution"
                )
                ops.append(
                    EditOperation(
                        op_type="relabel",
                        step_index=i,
                        actual_action=actual,
                        ideal_action=ideal,
                        cost=edit_costs["relabel"],
                        gap_type=gap_type,
                        explanation=(
                            f"Step {i}: agent chose '{actual}', ideal path "
                            f"requires '{ideal}' — {gap_type}"
                        ),
                    )
                )
                i -= 1
                j -= 1
            elif op_type == "delete" and i > 0:
                ops.append(
                    EditOperation(
                        op_type="delete",
                        step_index=i,
                        actual_action=seq_a[i - 1],
                        cost=edit_costs["delete"],
                        gap_type="spurious_action",
                        explanation=(
                            f"Step {i}: agent performed '{seq_a[i-1]}' "
                            f"which is absent from the ideal path"
                        ),
                    )
                )
                i -= 1
            elif op_type == "insert" and j > 0:
                ops.append(
                    EditOperation(
                        op_type="insert",
                        step_index=j,
                        ideal_action=seq_b[j - 1],
                        cost=edit_costs["insert"],
                        gap_type="missing_action",
                        explanation=(
                            f"Ideal step {j}: '{seq_b[j-1]}' was required "
                            f"but agent omitted it"
                        ),
                    )
                )
                j -= 1
            else:  # pragma: no cover — impossible if DP is correct
                raise RuntimeError(
                    f"DP reconstruction reached illegal state (i={i}, j={j}, "
                    f"op_type={op_type!r})"
                )

        ops.reverse()
        return [op for op in ops if op.op_type != "no_op"]

    # ------------------------------------------------------------------
    # Gap classification (uses graph metrics — matches docstring)
    # ------------------------------------------------------------------
    def _classify_gaps(self, ops: List[EditOperation]) -> List[Dict[str, Any]]:
        centrality = float(self.config.graph_metrics.get("centrality", 0.5))
        connectivity = float(self.config.graph_metrics.get("connectivity", 0.5))

        # RLHF shifts severity score smoothly around the 0.5 baseline.
        rlhf_shift = (self.config.human_feedback_score - 0.5) * 0.6  # [-0.3, +0.3]

        gaps: List[Dict[str, Any]] = []
        for op in ops:
            base = 1.0 if op.gap_type in self._critical_gap_types else 0.5
            # LIMIT Graph: high centrality/connectivity nodes are root causes.
            score = base * (1.0 + 0.5 * centrality + 0.5 * connectivity) + rlhf_shift
            severity = "critical" if score >= 1.0 else "warning"

            gaps.append(
                {
                    "gap_type": op.gap_type,
                    "severity": severity,
                    "actual_action": op.actual_action,
                    "ideal_action": op.ideal_action,
                    "step_index": op.step_index,
                    "explanation": op.explanation,
                    "severity_score": round(score, 3),
                }
            )
        return gaps

    # ------------------------------------------------------------------
    # Full evaluation entry point
    # ------------------------------------------------------------------
    def evaluate(
        self,
        graph_a: DecisionGraph,
        task_type: str,
        graph_b: Optional[DecisionGraph] = None,
        *,
        energy_kwh: Optional[float] = None,
        latency_s: Optional[float] = None,
        carbon_kg: Optional[float] = None,
    ) -> Dict[str, Any]:
        if graph_b is None:
            graph_b = self._ideal_store.get(task_type)
        if graph_b is None:
            return EvaluationReport(
                status="no_ideal_path",
                task_type=task_type,
                graph_a_sequence=graph_a.get_action_sequence(),
                graph_b_sequence=[],
                ged_score=0.0,
                normalized_ged=0.0,
                reasoning_gaps=[],
                edit_operations=[],
                xai_explanation="",
                verdict="unknown",
                corrective_actions=[],
                message=f"No ideal path registered for task_type='{task_type}'.",
            ).to_dict()

        ged = self.compute_ged(graph_a, graph_b)
        xai = self._build_xai(ged)
        verdict = self._verdict(ged["normalized_ged"], ged["reasoning_gaps"])
        corrections = self._corrective_actions(ged["reasoning_gaps"])
        counterfactual = self._counterfactual_trace(ged)

        report = EvaluationReport(
            status="evaluated",
            task_type=task_type,
            graph_a_sequence=ged["graph_a_sequence"],
            graph_b_sequence=ged["graph_b_sequence"],
            ged_score=ged["ged_score"],
            normalized_ged=ged["normalized_ged"],
            reasoning_gaps=ged["reasoning_gaps"],
            edit_operations=ged["edit_operations"],
            xai_explanation=xai,
            verdict=verdict,
            corrective_actions=corrections,
            counterfactual_trace=counterfactual,
            relabel_cost=ged["relabel_cost"],
            relabel_cost_source=ged["relabel_cost_source"],
        )

        # Enhanced: MODP reward + optimizer update.
        if self.cost_optimizer is not None and ged["_context"] is not None:
            state_vec, teacher_preds, gate_probs = ged["_context"]
            critical_count = sum(
                1 for g in ged["reasoning_gaps"] if g["severity"] == "critical"
            )
            reward = self.reward_fn(
                normalized_ged=ged["normalized_ged"],
                critical_count=critical_count,
                energy_kwh=energy_kwh,
                latency_s=latency_s,
                carbon_kg=carbon_kg,
                human_feedback=self.config.human_feedback_score,
            )
            self.cost_optimizer.update(
                state_vec,
                reward,
                teacher_preds=teacher_preds,
                gate_probs=gate_probs,
            )
            report.modp_reward = reward
            report.optimizer_stats = {
                "kind": type(self.cost_optimizer).__name__,
                "counter": getattr(self.cost_optimizer, "counter", None),
                "relabel_cost_source": self._relabel_cost_source,
            }

        # Observability
        self.metrics.observe(
            "evaluator_ged_score", ged["ged_score"], task_type=task_type,
        )
        self.metrics.observe(
            "evaluator_normalized_ged", ged["normalized_ged"], task_type=task_type,
        )
        self.metrics.observe(
            "evaluator_verdict", 1.0, task_type=task_type, verdict=verdict,
        )
        for g in ged["reasoning_gaps"]:
            self.metrics.observe(
                "evaluator_gap_count", 1.0,
                task_type=task_type, severity=g["severity"],
                gap_type=g["gap_type"],
            )

        return report.to_dict()

    # ------------------------------------------------------------------
    def evaluate_batch(
        self,
        graphs_a: Sequence[DecisionGraph],
        task_type: str,
        graph_b: Optional[DecisionGraph] = None,
        *,
        metrics_list: Optional[Sequence[Dict[str, Optional[float]]]] = None,
    ) -> List[Dict[str, Any]]:
        if metrics_list is not None and len(metrics_list) != len(graphs_a):
            raise ValueError("metrics_list length must match graphs_a length")
        out: List[Dict[str, Any]] = []
        for i, ga in enumerate(graphs_a):
            kw = (metrics_list[i] if metrics_list is not None else {})
            out.append(
                self.evaluate(
                    ga, task_type, graph_b,
                    energy_kwh=kw.get("energy_kwh") if kw else None,
                    latency_s=kw.get("latency_s") if kw else None,
                    carbon_kg=kw.get("carbon_kg") if kw else None,
                )
            )
        return out

    # ------------------------------------------------------------------
    # XAI output builder (with counterfactual deltas)
    # ------------------------------------------------------------------
    def _counterfactual_trace(self, ged: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        For each relabel gap, compute what the GED would be if the agent had
        taken the ideal action at that step. Tells the operator: "if only the
        agent had done X instead of Y, GED would have dropped by Δ."
        """
        seq_a = ged["_seq_a"]
        seq_b = ged["_seq_b"]
        edit_costs = ged["_edit_costs"]
        base_ged = ged["ged_score"]

        trace: List[Dict[str, Any]] = []
        for op in ged["_ops"]:
            if op.op_type != "relabel":
                continue
            idx = op.step_index - 1
            if not (0 <= idx < len(seq_a)):
                continue
            modified = list(seq_a)
            modified[idx] = op.ideal_action
            new_ops = self._dp_align(modified, seq_b, edit_costs)
            new_ged = sum(o.cost for o in new_ops)
            trace.append(
                {
                    "step_index": op.step_index,
                    "from_action": op.actual_action,
                    "to_action": op.ideal_action,
                    "ged_before": round(base_ged, 4),
                    "ged_if_fixed": round(new_ged, 4),
                    "delta": round(base_ged - new_ged, 4),
                }
            )
        return trace

    def _build_xai(self, ged: Dict[str, Any]) -> str:
        gaps = ged["reasoning_gaps"]
        seq_a = ged["graph_a_sequence"]
        seq_b = ged["graph_b_sequence"]
        matched = max(len(seq_a), len(seq_b)) - len(gaps)

        if not gaps:
            return (
                f"Agent path {seq_a} matches ideal path exactly. "
                f"No reasoning gap. GED = 0."
            )

        critical = [g for g in gaps if g["severity"] == "critical"]
        warnings = [g for g in gaps if g["severity"] == "warning"]

        lines = [
            f"GED={ged['ged_score']} (normalised={ged['normalized_ged']}) "
            f"| {len(gaps)} gap(s): {len(critical)} critical, "
            f"{len(warnings)} warning(s) | matched steps={matched}/{max(len(seq_a), len(seq_b))}",
            f"Actual path:  {' → '.join(seq_a)}",
            f"Ideal path:   {' → '.join(seq_b)}",
            f"graph_metrics: centrality="
            f"{self.config.graph_metrics.get('centrality', 0.5):.2f}, "
            f"connectivity="
            f"{self.config.graph_metrics.get('connectivity', 0.5):.2f}",
            f"relabel_cost={ged['relabel_cost']:.3f} ({ged['relabel_cost_source']})",
        ]
        for g in critical:
            lines.append(f"  [CRITICAL] {g['explanation']}")
        for w in warnings:
            lines.append(f"  [WARNING]  {w['explanation']}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Verdict
    # ------------------------------------------------------------------
    def _verdict(self, norm_ged: float, gaps: List[Dict[str, Any]]) -> str:
        if norm_ged == 0.0:
            return "optimal"
        critical_count = sum(1 for g in gaps if g["severity"] == "critical")
        if (
            norm_ged <= self.config.verdict_near_optimal_max
            and critical_count == 0
        ):
            return "near_optimal"
        if (
            norm_ged <= self.config.verdict_suboptimal_max
            or critical_count == 1
        ):
            return "suboptimal"
        return "reasoning_failure"

    # ------------------------------------------------------------------
    # Corrective actions (step-prefixed, covers all known gap types)
    # ------------------------------------------------------------------
    def _corrective_actions(self, gaps: List[Dict[str, Any]]) -> List[str]:
        corrections: List[str] = []
        for g in gaps:
            gap = g.get("gap_type", "")
            step = g.get("step_index", "?")
            prefix = f"Step {step}: "
            if gap == "unnecessary_deferral":
                corrections.append(
                    prefix
                    + "Re-evaluate defer threshold — grid strain signal may be "
                    "false positive. Check CausalGraph confidence on "
                    "GridStrain → DeferralSignal edge."
                )
            elif gap == "failed_to_defer":
                corrections.append(
                    prefix
                    + "Task should have deferred. Increase CarbonLevel:high → "
                    "Decision:defer weight in PolicyGraph."
                )
            elif gap == "wrong_optimization_strategy":
                corrections.append(
                    prefix
                    + "Wrong compression applied. Verify ModelOptimizer selects "
                    "strategy based on task type, not global default."
                )
            elif gap == "over_throttled":
                corrections.append(
                    prefix
                    + "Model over-throttled for this priority level. Decrease "
                    "Zone:yellow → Decision:throttle weight."
                )
            elif gap == "under_throttled":
                corrections.append(
                    prefix
                    + "Model under-throttled. Increase Zone:yellow → "
                    "Decision:throttle weight."
                )
            elif gap == "missing_action":
                corrections.append(
                    prefix
                    + f"Agent skipped required action '{g.get('ideal_action')}'. "
                    "Verify PolicyGraph traversal depth is sufficient."
                )
            elif gap == "spurious_action":
                corrections.append(
                    prefix
                    + f"Agent performed extraneous action '{g.get('actual_action')}' "
                    "absent from ideal path. Tighten PolicyGraph gating."
                )
            elif gap == "suboptimal_substitution":
                corrections.append(
                    prefix
                    + f"Agent chose '{g.get('actual_action')}' but ideal was "
                    f"'{g.get('ideal_action')}'. Review policy at this branch."
                )
            else:
                corrections.append(
                    prefix
                    + f"Unclassified reasoning gap ({gap!r}); manual review required."
                )
        # Deduplicate while preserving order.
        return list(dict.fromkeys(corrections))


# ============================================================================
# SECTION 11 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = DualGraphConfig(
        use_enhancements=True,
        use_distillation=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.75, "connectivity": 0.6},
        seed=42,
    )
    evaluator = DualGraphEvaluator(cfg)
    evaluator.build_ideal_from_sequence(
        "web_research", ["plan", "search", "read", "synthesize", "answer"]
    )

    actual = DecisionGraph("run-42", "executor")
    prev = None
    for i, action in enumerate(["plan", "search", "search", "synthesize", "answer"]):
        n = DecisionNode(node_id=f"a{i}", action=action)
        actual.add_step(n, after_id=prev)
        prev = n.node_id

    report = evaluator.evaluate(
        actual, "web_research",
        energy_kwh=2e-4, latency_s=4.0, carbon_kg=8e-5,
    )
    print(json.dumps(report, indent=2))

    if evaluator.cost_optimizer is not None and cfg.model_path:
        evaluator.cost_optimizer.save(cfg.model_path)
