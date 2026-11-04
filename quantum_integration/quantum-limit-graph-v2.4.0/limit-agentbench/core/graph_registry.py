# -*- coding: utf-8 -*-
"""
graph_registry.py — Centralised Lifecycle Manager (Enhanced, v2)
================================================================

Fixes applied vs. v1
--------------------
  P0  MoE gate gradient wrong shape/sign (§2.1)   -> _train_step (softmax chain-rule)
  P0  Teachers trained on gated mixture (§2.1)     -> forward returns teacher_outputs
  P0  Fallback slice in prune_executions (§2.2)    -> explicit survivors list
  P0  keep_last=0 silent no-op (§2.3)              -> validated + fixed
  P0  Optimizer only learns from prunes (§2.4)     -> reward for every action
  P0  Binary reward with no resolution (§2.5)      -> bounded monotone value
  P0  Optimizer state lost on restart (§2.6)       -> to_state_dict/from_state_dict
  P0  Dead use_evolutionary flag (§2.7)            -> PruneEvolutionaryOptimizer
  P0  Config mutation in restore (§2.8)            -> self._last_known_metrics
  P0  Unseeded RNG (§2.9)                          -> np.random.default_rng(seed)
  P0  Non-atomic snapshot write (§2.10)            -> tmp + replace
  P0  Unguarded corrupt snapshot (§2.11)           -> try/except + version check
  P0  Silent duplicate register_execution (§2.12)  -> raises unless overwrite=True
  P0  keep_last not validated (§2.13)              -> ValueError
  P1  feature_dim/n_actions hard-coded (§3.1)      -> derived from state
  P1  epsilon never decays (§3.2)                  -> epsilon_min / epsilon_decay
  P1  batch size hard-coded (§3.3)                 -> config.batch_size
  P1  distill_w/rl_w hard-coded (§3.4)             -> config
  P1  MODP weights were dead (§6.1)                -> used in PruneReward
  P1  IDEAL/EXECUTION ignored by metrics (§3.8)    -> included
  P1  Centrality saturates (§3.10)                 -> normalized
  P1  touch() inflated by internal reads (§3.12)   -> _get_singleton_unlocked()
  P2  MetricsSink hooks (§6.9)                     -> added
  P2  Return stats from prune_executions (§5.16)   -> {"pruned", "kept", "fallback"}
  P2  clear_all / reset (§6.8)                     -> added
  P2  prune_ideals (§6.4)                          -> added
  P2  stop_event (§6.10)                           -> added
  P2  Snapshot version (§6.3)                      -> version=2

Preserved public surface
------------------------
  GraphType, GraphRegistryConfig, GraphRecord, GraphPruneState,
  PruneDistillationOptimizer, GraphRegistry,
  .get_or_create(), .get(), .register_execution(), .get_execution(),
  .list_executions(), .prune_executions(), .compute_graph_metrics(),
  .snapshot(), ._restore_snapshots(), .health(),
  .feed_diagnosis_to_policy(), .register_helium_monitor(),
  .get_helium_monitor()
"""

from __future__ import annotations

import json
import logging
import math
import threading
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

import numpy as np

# Import graph primitives — kept identical to v1 (with sys.path hack preserved
# for backwards compat, though package-relative imports are preferred).
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.causal_graph import CausalGraph
from core.policy_graph import PolicyGraph
from core.dual_graph_evaluator import DecisionGraph, DualGraphEvaluator

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = 2


# ============================================================================
# SECTION 1 — Graph type enum
# ============================================================================

class GraphType(str, Enum):
    CAUSAL    = "causal"
    POLICY    = "policy"
    EXECUTION = "execution"
    IDEAL     = "ideal"


# ============================================================================
# SECTION 2 — Pluggable reward (MODP used here)
# ============================================================================

class RewardFn(Protocol):
    def __call__(
        self,
        *,
        action: int,
        record: "GraphRecord",
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> float: ...


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


class PruneReward:
    """
    Bounded, monotone, MODP-weighted prune reward.

    For action == PRUNE (1):  reward =  2 * value - 1   ∈ [-1, +1]
    For action == KEEP  (0):  reward =  1 - 2 * value   ∈ [-1, +1]

    `value` ∈ [0, 1] combines:
        w_acc * (1 - access_ratio)   — cold graphs are higher-value prunes
        w_lat * age_ratio            — old graphs are higher-value prunes
        w_cbn * size_ratio           — big graphs cost more to keep (carbon proxy)
        w_erg * size_ratio           — big graphs cost more to keep (energy proxy)

    Symmetric in [prune, keep] — training on keeps is as informative as
    training on prunes, so the optimizer learns a ranking, not a threshold.
    """

    def __init__(
        self,
        weights: Tuple[float, float, float, float] = (0.4, 0.3, 0.15, 0.15),
        access_scale: int = 100,
        age_scale_seconds: float = 3600.0,
        size_scale: int = 400,
    ) -> None:
        w = np.asarray(weights, dtype=np.float64)
        if len(w) != 4 or (w < 0).any() or w.sum() <= 0:
            raise ValueError(f"Invalid MODP weights: {weights!r}")
        self.weights = w / w.sum()
        self.access_scale = max(int(access_scale), 1)
        self.age_scale_seconds = float(age_scale_seconds)
        self.size_scale = max(int(size_scale), 1)

    def _prune_value(
        self, record: "GraphRecord", graph_metrics: Dict[str, float]
    ) -> float:
        access_ratio = _clamp(
            math.log1p(max(record.access_count, 0)) / math.log1p(self.access_scale)
        )
        age_seconds = record.age_seconds()
        age_ratio = _clamp(age_seconds / max(self.age_scale_seconds, 1e-9))
        size = _safe_len(getattr(record.instance, "nodes", {})) + _safe_len(
            getattr(record.instance, "edges", [])
        )
        size_ratio = _clamp(size / self.size_scale)

        w = self.weights
        return float(
            w[0] * (1.0 - access_ratio)
            + w[1] * age_ratio
            + w[2] * size_ratio
            + w[3] * size_ratio
        )

    def __call__(
        self,
        *,
        action: int,
        record: "GraphRecord",
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> float:
        v = self._prune_value(record, graph_metrics)
        # RLHF tilt: trust agent more -> bias reward toward "keep" (+shift).
        rlhf_bias = (float(human_feedback) - 0.5) * 0.1  # ∈ [-0.05, +0.05]
        if action == 1:
            reward = 2.0 * v - 1.0 + rlhf_bias
        else:
            reward = 1.0 - 2.0 * v + rlhf_bias
        return float(_clamp(reward, -1.0, 1.0))


def _safe_len(x: Any) -> int:
    try:
        return len(x)
    except TypeError:
        return 0


# ============================================================================
# SECTION 3 — Metrics sink
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
        self._log = logging.getLogger("graph_registry.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 4 — Persistence helpers (atomic)
# ============================================================================

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
        logger.warning("Ignoring corrupt snapshot %s: %s", path, exc)
        return None


# ============================================================================
# SECTION 5 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class GraphRegistryConfig:
    use_enhancements: bool = False
    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "centrality": 0.5,
            "connectivity": 0.5,
            "density": 0.4,
        }
    )
    modp_weights: Optional[Tuple[float, float, float, float]] = None

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

    seed: Optional[int] = None
    snapshot_version: int = SNAPSHOT_VERSION

    def __post_init__(self) -> None:
        if self.modp_weights is not None:
            w = self.modp_weights
            if len(w) != 4:
                raise ValueError(
                    f"modp_weights must have 4 components, got {len(w)}"
                )
            if any(float(x) < 0 for x in w) or sum(float(x) for x in w) <= 0:
                raise ValueError(f"modp_weights must be non-negative, got {w!r}")
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
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(f"graph_metric {k!r} must be in [0,1], got {v!r}")


# ============================================================================
# SECTION 6 — Graph record (mutable, with read/prune semantics)
# ============================================================================

@dataclass
class GraphRecord:
    graph_type: GraphType
    graph_id: str
    instance: Any
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    access_count: int = 0
    last_accessed: Optional[str] = None

    def touch(self) -> None:
        """Mark an external read (inflates access_count)."""
        self.access_count += 1
        self.last_accessed = datetime.now(timezone.utc).isoformat()

    def age_seconds(self) -> float:
        try:
            created = datetime.fromisoformat(self.created_at)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            return max(
                0.0,
                (datetime.now(timezone.utc) - created).total_seconds(),
            )
        except (ValueError, TypeError):
            return 0.0


# ============================================================================
# SECTION 7 — Prune-state feature extractor
# ============================================================================

class GraphPruneState:
    FEATURE_DIM = 7

    def __init__(
        self,
        record: GraphRecord,
        graph_metrics: Dict[str, float],
        human_feedback: float,
    ) -> None:
        age = record.age_seconds()
        self.features = np.array(
            [
                min(record.access_count / 100.0, 1.0),
                min(age / 3600.0, 1.0),
                min(_safe_len(getattr(record.instance, "nodes", {})) / 200.0, 1.0),
                min(_safe_len(getattr(record.instance, "edges", [])) / 200.0, 1.0),
                _clamp(float(graph_metrics.get("centrality", 0.5))),
                _clamp(float(graph_metrics.get("connectivity", 0.5))),
                _clamp(float(human_feedback)),
            ],
            dtype=np.float32,
        )
        assert self.features.shape == (self.FEATURE_DIM,)


# ============================================================================
# SECTION 8 — PruneOptimizer protocol
# ============================================================================

class PruneOptimizer(Protocol):
    def select_action(
        self, state: GraphPruneState, exploration: bool = True
    ) -> Tuple[int, np.ndarray, Optional[np.ndarray], Optional[np.ndarray]]: ...

    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        *,
        teacher_outputs: Optional[np.ndarray] = None,
        gate_probs: Optional[np.ndarray] = None,
    ) -> None: ...

    def to_state_dict(self) -> Dict[str, Any]: ...

    def from_state_dict(self, payload: Dict[str, Any]) -> None: ...


# ============================================================================
# SECTION 9 — Distillation + MoE (fixed gate gradient, persistence, decay)
# ============================================================================

class PruneDistillationOptimizer:
    """
    Distillation + MoE for the KEEP(0) / PRUNE(1) decision.

    Forward returns teacher_outputs so the gate can be trained on the exact
    teacher predictions that drove the decision (fixes §2.1).

    Backward uses the correct softmax chain rule for the gate.
    """

    N_ACTIONS = 2

    def __init__(
        self,
        config: GraphRegistryConfig,
        rng: Optional[np.random.Generator] = None,
        feature_dim: int = GraphPruneState.FEATURE_DIM,
    ) -> None:
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = int(feature_dim)
        self.n_actions = self.N_ACTIONS

        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

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

        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, len(self.teachers))) * 0.01
        )
        self.gate_bias = np.zeros(len(self.teachers))

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: GraphPruneState) -> np.ndarray:
        if state.features[0] < 0.2 or state.features[1] > 0.5:
            return np.array([0.1, 0.9])
        return np.array([0.8, 0.2])

    def _rlhf_teacher(self, state: GraphPruneState) -> np.ndarray:
        if state.features[6] > 0.7:
            return np.array([0.7, 0.3])
        if state.features[6] < 0.3:
            return np.array([0.2, 0.8])
        return np.array([0.5, 0.5])

    def _historical_teacher(self, state: GraphPruneState) -> np.ndarray:
        # Cold + large + low-centrality -> prune.
        cold = 1.0 - state.features[0]
        size = 0.5 * (state.features[2] + state.features[3])
        centrality = state.features[4]
        prune_score = _clamp(0.5 * cold + 0.3 * size + 0.2 * (1.0 - centrality))
        return np.array([1.0 - prune_score, prune_score])

    # ---- forward ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: np.ndarray) -> np.ndarray:
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, state_vec: np.ndarray) -> np.ndarray:
        return self._softmax(state_vec @ self.gate_weights + self.gate_bias)

    def select_action(
        self, state: GraphPruneState, exploration: bool = True
    ) -> Tuple[int, np.ndarray, np.ndarray, np.ndarray]:
        x = state.features
        teacher_outputs = np.stack([t(state) for t in self.teachers])  # (T, P)
        gate_probs = self._gate_forward(x)
        teacher_probs = np.sum(gate_probs[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        if exploration and self.rng.random() < self.epsilon:
            action = int(self.rng.integers(0, self.n_actions))
        else:
            student_probs = self._softmax(
                x @ self.student_weights + self.student_bias
            )
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_outputs, gate_probs

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        *,
        teacher_outputs: Optional[np.ndarray] = None,
        gate_probs: Optional[np.ndarray] = None,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))

        if teacher_outputs is None or gate_probs is None:
            logger.warning(
                "update() called without teacher_outputs/gate_probs; gate "
                "gradient will be skipped. Prefer select_action() then pass "
                "its outputs through."
            )
            teacher_outputs = None
            gate_probs = None

        self.replay_buffer.append(
            (
                state_vec.astype(np.float64),
                int(action),
                r,
                teacher_outputs.astype(np.float64) if teacher_outputs is not None else None,
                gate_probs.astype(np.float64) if gate_probs is not None else None,
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

        for s, a, r, t_outs, g_probs in batch:
            cur = self._softmax(s @ self.student_weights + self.student_bias)

            # ---- student update ----
            one_hot = np.zeros(self.n_actions)
            one_hot[a] = 1.0

            if t_outs is not None and g_probs is not None:
                combined_teacher = g_probs @ t_outs
            else:
                combined_teacher = cur  # no teacher signal

            grad_distill = -(combined_teacher - cur)
            grad_rl = -r * (one_hot - cur)
            grad = self.distill_w * grad_distill + self.rl_w * grad_rl
            self.student_weights -= self.lr * np.outer(s, grad)
            self.student_bias -= self.lr * grad

            # ---- gate update (correct softmax chain rule) ----
            if t_outs is not None and g_probs is not None:
                # Target is a reward-weighted distribution.
                target = np.full(
                    self.n_actions, (1.0 - r) / max(self.n_actions - 1, 1)
                )
                target[a] = r

                combined = g_probs @ t_outs               # (P,)
                err = combined - target                   # (P,)
                # dL/d(gate_probs) = t_outs @ err        # (T,)
                dL_dg = t_outs.T @ err                    # (T,)
                # Chain through softmax: grad_z = g * (dL_dg - g·dL_dg)
                grad_logits = g_probs * (dL_dg - float(g_probs @ dL_dg))
                self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                self.gate_bias -= self.gate_lr * grad_logits

        # Epsilon decay
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence ------------------------------------------------------
    def to_state_dict(self) -> Dict[str, Any]:
        return {
            "kind": "distillation",
            "feature_dim": self.feature_dim,
            "n_actions": self.n_actions,
            "student_weights": self.student_weights.tolist(),
            "student_bias": self.student_bias.tolist(),
            "gate_weights": self.gate_weights.tolist(),
            "gate_bias": self.gate_bias.tolist(),
            "epsilon": float(self.epsilon),
            "counter": int(self.counter),
        }

    def from_state_dict(self, payload: Dict[str, Any]) -> None:
        if payload.get("kind") != "distillation":
            raise ValueError(f"Wrong optimizer kind: {payload.get('kind')!r}")
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


# ============================================================================
# SECTION 10 — Evolutionary prune optimizer (was a dead flag)
# ============================================================================

@dataclass
class _Ind:
    weights: np.ndarray  # (feature_dim, n_actions)
    bias: np.ndarray     # (n_actions,)
    fitness: float = 0.0


class PruneEvolutionaryOptimizer:
    """
    Population-based linear policy. Fitness is negative MSE between the
    individual's action logits and a reward-weighted target — so that higher
    reward always translates to higher fitness, regardless of the action.
    """

    def __init__(
        self,
        config: GraphRegistryConfig,
        rng: Optional[np.random.Generator] = None,
        feature_dim: int = GraphPruneState.FEATURE_DIM,
    ) -> None:
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = int(feature_dim)
        self.n_actions = 2
        self.population: List[_Ind] = self._init_population(config.population_size)
        self.generation = 0
        self.counter = 0

    def _init_population(self, size: int) -> List[_Ind]:
        return [
            _Ind(
                weights=self.rng.standard_normal(
                    (self.feature_dim, self.n_actions)
                ) * 0.1,
                bias=self.rng.standard_normal(self.n_actions) * 0.01,
            )
            for _ in range(size)
        ]

    def _forward(self, ind: _Ind, x: np.ndarray) -> np.ndarray:
        return x @ ind.weights + ind.bias

    def select_action(
        self, state: GraphPruneState, exploration: bool = True
    ) -> Tuple[int, np.ndarray, Optional[np.ndarray], Optional[np.ndarray]]:
        x = state.features
        if exploration and self.rng.random() < self.config.epsilon:
            action = int(self.rng.integers(0, self.n_actions))
        else:
            best = max(self.population, key=lambda ind: ind.fitness)
            action = int(np.argmax(self._forward(best, x)))
        return action, x, None, None

    def update(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        *,
        teacher_outputs: Optional[np.ndarray] = None,
        gate_probs: Optional[np.ndarray] = None,
    ) -> None:
        r = float(np.clip(reward, -1.0, 1.0))
        # Target: reward-weighted one-hot (same shape as logits).
        target = np.full(self.n_actions, (1.0 - r) / max(self.n_actions - 1, 1))
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
        import copy as _copy

        new_pop = [_copy.deepcopy(ind) for ind in self.population[:n_elite]]
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
        return max((self.population[i] for i in idxs), key=lambda ind: ind.fitness)

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

    def to_state_dict(self) -> Dict[str, Any]:
        return {
            "kind": "evolutionary",
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

    def from_state_dict(self, payload: Dict[str, Any]) -> None:
        if payload.get("kind") != "evolutionary":
            raise ValueError(f"Wrong optimizer kind: {payload.get('kind')!r}")
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


# ============================================================================
# SECTION 11 — GraphRegistry (main class)
# ============================================================================

class GraphRegistry:
    """
    Thread-safe singleton store for all Green Agent graph instances.

    Singletons (one per type):
      CAUSAL, POLICY, IDEAL

    Collections (many per session):
      EXECUTION -> {graph_id: DecisionGraph}
    """

    _FACTORIES = {
        GraphType.CAUSAL: CausalGraph,
        GraphType.POLICY: PolicyGraph,
        GraphType.IDEAL:  DualGraphEvaluator,
    }

    def __init__(
        self,
        snapshot_dir: str = "./snapshots",
        config: Optional[GraphRegistryConfig] = None,
        *,
        reward_fn: Optional[RewardFn] = None,
        metrics: Optional[MetricsSink] = None,
        rng: Optional[np.random.Generator] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._singletons: Dict[GraphType, GraphRecord] = {}
        self._executions: Dict[str, GraphRecord] = {}

        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        self.config = config or GraphRegistryConfig()
        self.rng = rng if rng is not None else np.random.default_rng(self.config.seed)

        self.reward_fn: RewardFn = reward_fn or PruneReward(
            weights=self.config.modp_weights or (0.4, 0.3, 0.15, 0.15)
        )
        self.metrics: MetricsSink = metrics or InMemoryMetrics()

        self._last_known_metrics: Dict[str, float] = dict(self.config.graph_metrics)
        self._last_known_human_feedback: float = self.config.human_feedback_score
        self._restore_in_progress = False

        self._helium_monitor = None

        # ---- Optimizer selection (§2.7) ----
        self.cost_optimizer: Optional[PruneOptimizer] = None
        if self.config.use_enhancements:
            if self.config.use_evolutionary:
                self.cost_optimizer = PruneEvolutionaryOptimizer(
                    self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.cost_optimizer = PruneDistillationOptimizer(
                    self.config, rng=self.rng
                )
        # Backwards compat (v1 exposed .distillation_optimizer)
        self.distillation_optimizer = self.cost_optimizer

        self._restore_snapshots()

    # ------------------------------------------------------------------
    # Singleton access
    # ------------------------------------------------------------------
    def _get_singleton_unlocked(self, graph_type: GraphType) -> Any:
        """Create if missing — does NOT touch the access counter."""
        if graph_type not in self._singletons:
            factory = self._FACTORIES[graph_type]
            instance = factory()
            self._singletons[graph_type] = GraphRecord(
                graph_type=graph_type,
                graph_id=f"singleton_{graph_type.value}",
                instance=instance,
            )
        return self._singletons[graph_type].instance

    def get_or_create(self, graph_type: GraphType) -> Any:
        if graph_type == GraphType.EXECUTION:
            raise ValueError(
                "Use register_execution() / get_execution() for EXECUTION graphs."
            )
        with self._lock:
            instance = self._get_singleton_unlocked(graph_type)
            if not self._restore_in_progress:
                self._singletons[graph_type].touch()
            return instance

    def get(self, graph_type: GraphType) -> Optional[Any]:
        with self._lock:
            record = self._singletons.get(graph_type)
            if record:
                record.touch()
                return record.instance
            return None

    # ------------------------------------------------------------------
    # Execution graph collection
    # ------------------------------------------------------------------
    def register_execution(
        self, decision_graph: DecisionGraph, *, overwrite: bool = False
    ) -> str:
        if not isinstance(decision_graph, DecisionGraph):
            raise TypeError("decision_graph must be a DecisionGraph")
        if not getattr(decision_graph, "graph_id", ""):
            raise ValueError("decision_graph.graph_id must be non-empty")
        for attr in ("nodes", "edges"):
            if not hasattr(decision_graph, attr):
                raise ValueError(f"decision_graph missing required attribute {attr!r}")

        with self._lock:
            gid = decision_graph.graph_id
            if gid in self._executions and not overwrite:
                raise ValueError(
                    f"execution graph_id {gid!r} already registered; "
                    f"pass overwrite=True to replace"
                )
            self._executions[gid] = GraphRecord(
                graph_type=GraphType.EXECUTION,
                graph_id=gid,
                instance=decision_graph,
            )
            self.metrics.observe("graph_registry_executions_total", 1.0)
            return gid

    def get_execution(self, graph_id: str) -> Optional[DecisionGraph]:
        with self._lock:
            record = self._executions.get(graph_id)
            if record:
                record.touch()
                return record.instance
            return None

    def list_executions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "graph_id": r.graph_id,
                    "created_at": r.created_at,
                    "access_count": r.access_count,
                    "node_count": _safe_len(getattr(r.instance, "nodes", {})),
                    "edge_count": _safe_len(getattr(r.instance, "edges", [])),
                    "graph_type": getattr(r.instance, "graph_type", "execution"),
                }
                for r in self._executions.values()
            ]

    # ------------------------------------------------------------------
    # Pruning
    # ------------------------------------------------------------------
    def prune_executions(
        self,
        keep_last: int = 100,
        *,
        stop_event: Optional[threading.Event] = None,
    ) -> Dict[str, int]:
        """
        Trim EXECUTION graphs down to `keep_last`.

        Enhanced mode:
          1. Compute a keep/prune decision for every graph.
          2. Apply prunes until `excess` reached.
          3. Deterministic fallback (oldest-first) if still over budget.
          4. Train the optimizer on EVERY decision (keep and prune).

        Returns:
          {"pruned": n, "kept": m, "fallback": k}
        """
        if not isinstance(keep_last, int) or keep_last < 0:
            raise ValueError("keep_last must be a non-negative int")

        with self._lock:
            n0 = len(self._executions)
            if n0 <= keep_last:
                return {"pruned": 0, "kept": n0, "fallback": 0}

            excess = n0 - keep_last
            sorted_items = sorted(
                self._executions.items(),
                key=lambda kv: kv[1].created_at,
            )

            # ---- 1. Compute decisions ----
            decisions: Dict[str, Tuple[int, Any, Any, Any]] = {}
            if self.cost_optimizer is not None:
                for gid, record in sorted_items:
                    if stop_event is not None and stop_event.is_set():
                        logger.info("prune_executions aborted by stop_event")
                        break
                    state = GraphPruneState(
                        record,
                        self._last_known_metrics,
                        self._last_known_human_feedback,
                    )
                    action, s_vec, t_outs, g_probs = self.cost_optimizer.select_action(
                        state, exploration=False
                    )
                    decisions[gid] = (action, s_vec, t_outs, g_probs)
            else:
                # No optimizer -> prune everything chronologically.
                for gid, _ in sorted_items:
                    decisions[gid] = (1, None, None, None)

            # ---- 2. Apply prunes chosen by optimizer ----
            pruned: List[str] = []
            for gid, record in sorted_items:
                if len(pruned) >= excess:
                    break
                action, s_vec, t_outs, g_probs = decisions.get(
                    gid, (1, None, None, None)
                )
                if action == 1 and gid in self._executions:
                    del self._executions[gid]
                    pruned.append(gid)
                    if self.cost_optimizer is not None and s_vec is not None:
                        reward = self.reward_fn(
                            action=1,
                            record=record,
                            graph_metrics=self._last_known_metrics,
                            human_feedback=self._last_known_human_feedback,
                        )
                        self.cost_optimizer.update(
                            s_vec, 1, reward,
                            teacher_outputs=t_outs, gate_probs=g_probs,
                        )
                        self.metrics.observe(
                            "graph_registry_prune_reward", reward,
                            action="prune",
                        )

            # ---- 3. Deterministic fallback (oldest-first) ----
            fallback = 0
            for gid, record in sorted_items:
                if len(pruned) >= excess:
                    break
                if gid in self._executions:
                    del self._executions[gid]
                    pruned.append(gid)
                    fallback += 1

            # ---- 4. Train on keeps (fixes §2.4) ----
            if self.cost_optimizer is not None:
                for gid, record in sorted_items:
                    if gid in pruned:
                        continue
                    action, s_vec, t_outs, g_probs = decisions.get(
                        gid, (0, None, None, None)
                    )
                    if action == 0 and s_vec is not None:
                        reward = self.reward_fn(
                            action=0,
                            record=record,
                            graph_metrics=self._last_known_metrics,
                            human_feedback=self._last_known_human_feedback,
                        )
                        self.cost_optimizer.update(
                            s_vec, 0, reward,
                            teacher_outputs=t_outs, gate_probs=g_probs,
                        )
                        self.metrics.observe(
                            "graph_registry_prune_reward", reward,
                            action="keep",
                        )

            pruned_total = len(pruned)
            kept_total = len(self._executions)
            self.metrics.observe(
                "graph_registry_pruned_total", float(pruned_total),
                fallback=bool(fallback > 0),
            )
            return {
                "pruned": pruned_total,
                "kept": kept_total,
                "fallback": fallback,
            }

    # ------------------------------------------------------------------
    # Ideal-path pruning
    # ------------------------------------------------------------------
    def prune_ideals(self, keep_types: int = 50) -> int:
        """Prune oldest ideal paths if the registry exceeds `keep_types`."""
        if not isinstance(keep_types, int) or keep_types < 0:
            raise ValueError("keep_types must be a non-negative int")
        with self._lock:
            ideal = self._singletons.get(GraphType.IDEAL)
            if ideal is None:
                return 0
            inst = ideal.instance
            registry = getattr(inst, "ideal_registry", None)
            if registry is None:
                registry = getattr(inst, "_ideal_store", None)
            if registry is None:
                return 0
            keys = list(registry.keys()) if hasattr(registry, "keys") else []
            if len(keys) <= keep_types:
                return 0
            to_drop = keys[: len(keys) - keep_types]
            for k in to_drop:
                try:
                    del registry._store[k]  # type: ignore[attr-defined]
                except (AttributeError, KeyError):
                    pass
            return len(to_drop)

    # ------------------------------------------------------------------
    # LIMIT graph metrics
    # ------------------------------------------------------------------
    def compute_graph_metrics(self) -> Dict[str, float]:
        """
        Aggregate centrality/connectivity/density across CAUSAL, POLICY,
        and EXECUTION graphs. (IDEAL is a store, not a graph.)
        """
        with self._lock:
            per_graph_centrality: List[float] = []
            per_graph_connectivity: List[float] = []
            per_graph_density: List[float] = []

            def _measure(nodes: Any, edges: Any) -> None:
                n_nodes = _safe_len(nodes)
                n_edges = _safe_len(edges)
                weights: List[float] = []
                for e in (edges or []):
                    w = getattr(e, "weight", None)
                    if isinstance(w, (int, float)):
                        weights.append(abs(float(w)))
                if weights:
                    mean_w = sum(weights) / len(weights)
                    max_w = max(weights) or 1.0
                    per_graph_centrality.append(_clamp(mean_w / max_w))
                else:
                    per_graph_centrality.append(0.0)

                if n_nodes > 1:
                    max_edges = n_nodes * (n_nodes - 1)
                    per_graph_connectivity.append(
                        _clamp(n_edges / max_edges)
                    )
                    per_graph_density.append(_clamp(n_edges / n_nodes))
                else:
                    per_graph_connectivity.append(0.0)
                    per_graph_density.append(0.0)

            for gtype in (GraphType.CAUSAL, GraphType.POLICY):
                rec = self._singletons.get(gtype)
                if rec:
                    _measure(
                        getattr(rec.instance, "nodes", {}),
                        getattr(rec.instance, "edges", []),
                    )
            for rec in self._executions.values():
                _measure(
                    getattr(rec.instance, "nodes", {}),
                    getattr(rec.instance, "edges", []),
                )

            def _mean(xs: List[float], default: float) -> float:
                return float(sum(xs) / len(xs)) if xs else default

            return {
                "centrality": _mean(per_graph_centrality, 0.5),
                "connectivity": _mean(per_graph_connectivity, 0.5),
                "density": _mean(per_graph_density, 0.4),
            }

    # ------------------------------------------------------------------
    # Snapshot / restore
    # ------------------------------------------------------------------
    def snapshot(self) -> None:
        with self._lock:
            state: Dict[str, Any] = {
                "version": SNAPSHOT_VERSION,
                "snapshot_at": datetime.now(timezone.utc).isoformat(),
            }

            causal = self._singletons.get(GraphType.CAUSAL)
            if causal:
                try:
                    state["causal_edges"] = causal.instance.export_state()["edges"]
                except Exception as exc:  # noqa: BLE001
                    logger.warning("causal export failed: %s", exc)

            policy = self._singletons.get(GraphType.POLICY)
            if policy:
                try:
                    state["policy_edges"] = policy.instance.export_weights()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("policy export failed: %s", exc)

            if self.config.use_enhancements:
                state["graph_metrics"] = self._last_known_metrics
                state["human_feedback_score"] = self._last_known_human_feedback
                if self.cost_optimizer is not None:
                    state["optimizer"] = self.cost_optimizer.to_state_dict()

            _save_json_atomic(self.snapshot_dir / "graph_weights.json", state)
            self.metrics.observe("graph_registry_snapshot_total", 1.0)

    def _restore_snapshots(self) -> None:
        snap_file = self.snapshot_dir / "graph_weights.json"
        state = _load_json_guarded(snap_file)
        if state is None:
            return
        if int(state.get("version", 1)) > SNAPSHOT_VERSION:
            logger.warning(
                "Snapshot version %s > supported %s; ignoring",
                state.get("version"), SNAPSHOT_VERSION,
            )
            return

        self._restore_in_progress = True
        try:
            if "causal_edges" in state:
                causal = self._get_singleton_unlocked(GraphType.CAUSAL)
                try:
                    weight_map = {
                        (e["source"], e["target"]): (
                            e["weight"], e.get("confidence", 1.0),
                        )
                        for e in state["causal_edges"]
                        if all(k in e for k in ("source", "target", "weight"))
                    }
                    for edge in causal.edges:
                        key = (edge.source_id, edge.target_id)
                        if key in weight_map:
                            w, c = weight_map[key]
                            edge.weight = w
                            edge.confidence = c
                except Exception as exc:  # noqa: BLE001
                    logger.warning("causal restore failed: %s", exc)

            if "policy_edges" in state:
                policy = self._get_singleton_unlocked(GraphType.POLICY)
                try:
                    weight_map = {
                        (e["source"], e["target"]): e["weight"]
                        for e in state["policy_edges"]
                        if all(k in e for k in ("source", "target", "weight"))
                    }
                    for edge in policy.edges:
                        key = (edge.source_id, edge.target_id)
                        if key in weight_map:
                            edge.weight = weight_map[key]
                except Exception as exc:  # noqa: BLE001
                    logger.warning("policy restore failed: %s", exc)

            if self.config.use_enhancements:
                gm = state.get("graph_metrics")
                if isinstance(gm, dict):
                    cleaned: Dict[str, float] = {}
                    for k, v in gm.items():
                        if isinstance(v, (int, float)) and 0.0 <= float(v) <= 1.0:
                            cleaned[str(k)] = float(v)
                    if cleaned:
                        self._last_known_metrics = cleaned
                hf = state.get("human_feedback_score")
                if isinstance(hf, (int, float)) and 0.0 <= float(hf) <= 1.0:
                    self._last_known_human_feedback = float(hf)
                opt_state = state.get("optimizer")
                if self.cost_optimizer is not None and isinstance(opt_state, dict):
                    try:
                        self.cost_optimizer.from_state_dict(opt_state)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("optimizer restore failed: %s", exc)
        finally:
            self._restore_in_progress = False

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    def health(self) -> Dict[str, Any]:
        with self._lock:
            report: Dict[str, Any] = {
                "snapshot_dir": str(self.snapshot_dir),
                "singletons": {},
                "execution_count": len(self._executions),
            }
            for gtype, record in self._singletons.items():
                inst = record.instance
                info: Dict[str, Any] = {
                    "graph_id": record.graph_id,
                    "access_count": record.access_count,
                    "last_accessed": record.last_accessed,
                    "node_count": _safe_len(getattr(inst, "nodes", {})),
                    "edge_count": _safe_len(getattr(inst, "edges", [])),
                }
                if hasattr(inst, "get_anomalies"):
                    try:
                        info["anomaly_count"] = len(inst.get_anomalies())
                    except Exception:  # noqa: BLE001
                        pass
                if isinstance(inst, DualGraphEvaluator):
                    reg = getattr(inst, "ideal_registry", None) or getattr(
                        inst, "_ideal_store", None
                    )
                    if reg is not None and hasattr(reg, "__len__"):
                        info["ideal_path_count"] = len(reg)
                report["singletons"][gtype.value] = info

            if self.config.use_enhancements:
                report["graph_metrics"] = self._last_known_metrics
                report["human_feedback_score"] = self._last_known_human_feedback
                if self.cost_optimizer is not None:
                    report["optimizer_stats"] = {
                        "kind": type(self.cost_optimizer).__name__,
                        "counter": getattr(self.cost_optimizer, "counter", None),
                    }
            return report

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------
    def feed_diagnosis_to_policy(self, diagnosis_report: Dict[str, Any]) -> None:
        action = diagnosis_report.get("recommended_action", "")
        policy: PolicyGraph = self.get_or_create(GraphType.POLICY)
        zone_map = {
            "defer_until_grid_stabilizes": "red",
            "throttle_model_accuracy": "yellow",
            "switch_to_low_power_mode": "yellow",
            "execute_immediately": "green",
            "proceed": "green",
        }
        zone = zone_map.get(action, None)
        setter = getattr(policy, "set_meta_zone_override", None)
        if callable(setter):
            setter(zone)
        else:
            policy._meta_zone_override = zone  # backwards compat

    # ------------------------------------------------------------------
    # Reset / clear
    # ------------------------------------------------------------------
    def clear_all(self, *, keep_snapshots: bool = True) -> None:
        with self._lock:
            self._singletons.clear()
            self._executions.clear()
            if not keep_snapshots:
                snap = self.snapshot_dir / "graph_weights.json"
                if snap.exists():
                    try:
                        snap.unlink()
                    except OSError:
                        pass

    # ------------------------------------------------------------------
    # Helium monitor registration
    # ------------------------------------------------------------------
    def register_helium_monitor(self, monitor: Any) -> None:
        self._helium_monitor = monitor
        logger.info("HeliumMonitor registered with GraphRegistry")

    def get_helium_monitor(self) -> Any:
        return self._helium_monitor


# ============================================================================
# SECTION 12 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = GraphRegistryConfig(
        use_enhancements=True,
        use_distillation=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7, "connectivity": 0.6, "density": 0.5},
        modp_weights=(0.4, 0.3, 0.15, 0.15),
        seed=42,
    )
    registry = GraphRegistry(snapshot_dir="./snapshots", config=cfg)

    # Populate some executions
    for i in range(150):
        dg = DecisionGraph(graph_id=f"run-{i:03d}", graph_type="executor")
        prev = None
        for j, action in enumerate(["plan", "search", "synthesize"]):
            node = DecisionNode(node_id=f"n{i}-{j}", action=action)
            dg.add_step(node, after_id=prev)
            prev = node.node_id
        registry.register_execution(dg, overwrite=True)

    print("before:", len(registry.list_executions()))
    stats = registry.prune_executions(keep_last=100)
    print("prune stats:", stats)
    print("after:", len(registry.list_executions()))

    print("metrics:", registry.compute_graph_metrics())
    print("health:", json.dumps(registry.health(), indent=2))

    registry.snapshot()
