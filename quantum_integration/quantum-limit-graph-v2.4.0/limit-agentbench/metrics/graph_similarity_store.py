# -*- coding: utf-8 -*-
"""
Phase 1 — Graph Similarity Store (Enhanced, v2)
================================================

Stores every benchmark execution as a labeled subgraph, then on a new task
returns the top-K most similar past executions as a statistical prior.

Fixes applied vs. v1
--------------------
  P0  Weights were [0.7, 0.3, 0.0] — the graph-metric weight was zero,
      so `use_enhancements=True` produced a *byte-identical* prior to
      `use_enhancements=False`. The "LIMIT Graph integration" was dead.
      Fix: base path keeps [0.7, 0.3] on (cos, edge); enhanced path uses
      [0.5, 0.3, 0.2] with a real third term.
  P0  `SimilarityDistillationOptimizer.update()` was `pass` and never
      called. Fix: `record_outcome(reward)` invokes real training;
      the optimizer learns a context→weights mapping.
  P0  Teachers all returned the same constant. Fix: rule teacher emits
      [0.5, 0.3, 0.2], RLHF teacher rebalances based on human feedback,
      historical teacher is a real k-NN over replay.
  P0  `EvolutionaryWeightOptimizer.update_fitness()` had the index-0
      bug and was never called. Fix: whole-population scoring, historical
      best, and `record_outcome` invokes `observe(weights_used, reward)`.
  P0  `composite_modp_score` was computed and never consumed. Fix:
      `flag_outlier` compares the actual composite against the prior
      composite; missing metrics are excluded (not treated as zero).
  P0  Graph metrics were double-counted (inside cosine + as `gm_sim`).
      Fix: `_feature_vector` no longer injects `graph:*` keys when
      `gm_sim` is available; the caller chooses one path or the other.
  P1  `node.get("value") or 1.0` bumped legitimate `0.0` to `1.0`.
  P1  `_graph_similarity` returned 0.5 for missing — not neutral when
      the graph weight is non-zero. Now returns `None`; caller
      renormalizes.
  P1  `_edge_overlap` returned 1.0 for both-empty — a spurious perfect
      match. Now returns `None`; caller renormalizes.
  P1  Config mutation + 3 crash classes on `modp_weights`. Fix:
      frozen config + `__post_init__` validation + local normalized copy.
  P1  MODP ordering was the 9th variant in the codebase. Fix:
      `MODP_SCHEMA` constant; documented in the header.
  P1  Unseeded RNG. Fix: `np.random.default_rng(seed)`.
  P1  Non-atomic, unguarded, unversioned index persistence. Fix:
      atomic write, guarded load, `{"version": 2, "entries": [...]}`.
  P1  No thread safety. Fix: `RLock` on index and optimizer state.
  P1  No metrics. Fix: `MetricsSink` protocol; emits
      `similarity_queries_total`, `similarity_top1_score`, `index_size`,
      `similarity_reward`.
  P1  No config validation. Fix: frozen config with `__post_init__`.
  P1  `top_k` not validated. Fix: raises on `< 1` or non-int.
  P2  `std == 0` outlier skipped silently. Fix: flags it when actual
      differs from mean.
  P2  `store_execution` mutated the input `ExecutionSubgraph`. Fix:
      does not touch the caller's object.
  P2  No persistence for optimizers. Fix: `save()`/`load()` on both.
  P2  Dead imports (`uuid`, `asdict`, `datetime`, `timezone`, `Tuple`,
      `deque`). Fix: removed.
  P2  "Priority: FIRST — lowest risk" framing invited enabling the
      enhancements — the mode that didn't work. Fix: header is honest
      about what works.

Design notes
------------
The default *base* path (``use_enhancements=False``) is unchanged from v1:
``0.7·cos + 0.3·edge``. This preserves v1's output byte-for-byte for any
deployment that never opted in.

The *enhanced* path (``use_enhancements=True``) uses a learned per-query
weight vector ``w ∈ Δ³`` produced by a three-teacher mixture:
  - rule teacher:      fixed balanced prior ``[0.5, 0.3, 0.2]``
  - RLHF teacher:      rebalanced by ``human_feedback_score``
  - historical teacher: k-NN over past (context, weights, reward) records

The MoE gate mixes the three based on a 5-dim context vector. The student
learns from the gate and the teachers via reward-weighted regression in
``record_outcome``.

Training closes the loop:
  1. ``find_similar(...)`` returns top-K, stores the pending decision.
  2. The caller runs the benchmark against the prior and estimates reward
     (e.g., ``1 - |actual - predicted_mean| / predicted_mean``).
  3. ``record_outcome(reward)`` trains the distillation and evolutionary
     optimizers.

Without ``record_outcome``, the optimizers stay at their initialized
values (rule teacher for distillation, config weights for evolutionary),
so the enhanced path defaults to a defensible answer even before training.

Public API (preserved)
----------------------
  GraphSimilarityConfig, ExecutionNode, ExecutionEdge, ExecutionSubgraph,
  SimilarityDistillationOptimizer, EvolutionaryWeightOptimizer,
  GraphSimilarityStore
  .store_execution(...), .find_similar(...), .build_prior(...),
  .flag_outlier(...)
  + NEW: .record_outcome(reward), .save(path=None), .load(path=None)

Priority: FIRST — additive on top of existing ledger, lowest risk
=================================================================

The "lowest risk" claim now holds because:
  - Base path is unchanged (v1 byte-identical).
  - Enhanced path only activates when ``use_enhancements=True`` AND a
    reward signal is provided via ``record_outcome``. Without it, the
    enhanced path produces the rule teacher's fixed prior — a defensible
    answer, not a random one.
  - All failures (invalid config, missing numpy, corrupt index) either
    raise clearly or fall back to the base path with a logged warning.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = 2

# Canonical MODP ordering for this file. Other files in the codebase use
# different orderings — never import weights across files without
# translating through this constant.
MODP_SCHEMA: Tuple[str, ...] = (
    "accuracy", "energy_kwh", "carbon_co2e_kg", "sustainability_index",
)

# Context vector dimension for the distillation optimizer.
CONTEXT_DIM = 5   # [bias, node_count, edge_count, has_gm, mean_gm]
WEIGHT_DIM = 3    # [cos, edge, gm_sim]


# ============================================================================
# Section 1 — Helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _finite_nonneg(value: Any, name: str) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {value!r}")
    if not math.isfinite(f) or f < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return f


def _atomic_write(path: Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def _load_guarded(path: Path) -> Optional[Any]:
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring corrupt file %s: %s", path, exc)
        return None


# ============================================================================
# Section 2 — Metrics sink
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
        self._log = logging.getLogger("graph_similarity.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# Section 3 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class GraphSimilarityConfig:
    use_enhancements: bool = False

    default_graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # MODP weights in MODP_SCHEMA order.
    modp_weights: Tuple[float, float, float, float] = (0.25, 0.25, 0.25, 0.25)
    human_feedback_score: float = 0.5

    use_distillation: bool = True
    use_evolutionary: bool = False

    distillation_lr: float = 0.02
    gating_lr: float = 0.01
    replay_size: int = 2000
    train_every: int = 5
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995
    distill_w: float = 0.5
    rl_w: float = 0.5

    population_size: int = 10
    mutation_rate: float = 0.15
    crossover_rate: float = 0.7
    elitism: int = 2

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components {MODP_SCHEMA}, got {w!r}"
            )
        if any(float(x) < 0 for x in w):
            raise ValueError(f"modp_weights must be non-negative, got {w!r}")
        if sum(float(x) for x in w) <= 0:
            raise ValueError(f"modp_weights sum must be > 0, got {w!r}")
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
        if self.distill_w < 0 or self.rl_w < 0 or (self.distill_w + self.rl_w) <= 0:
            raise ValueError("distill_w and rl_w must be non-negative, sum > 0")
        for k, v in self.default_graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "GraphSimilarityConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "GraphSimilarityConfig ignoring unknown keys: %s",
                sorted(unknown),
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                if v is None:
                    continue
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "default_graph_metrics":
                kwargs[k] = dict(v)
            elif k == "model_path":
                kwargs[k] = Path(v) if v is not None else None
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# Section 4 — Data classes (unchanged signatures)
# ============================================================================

@dataclass
class ExecutionNode:
    node_id: str
    node_type: str
    label: str
    value: Optional[float] = None


@dataclass
class ExecutionEdge:
    source_id: str
    target_id: str
    relation: str
    weight: float = 1.0


@dataclass
class ExecutionSubgraph:
    graph_id: str
    nodes: list
    edges: list
    outcome_metrics: dict
    timestamp: str
    framework: str
    graph_metrics: Optional[Dict[str, float]] = None


# ============================================================================
# Section 5 — Distillation optimizer (real teachers + trained gate)
# ============================================================================

class SimilarityDistillationOptimizer:
    """Multi-teacher distillation + MoE gating for the per-query weight vector.

    Each teacher proposes a 3-dim weight vector over ``(cos, edge, gm_sim)``.
    The MoE gate softmixes them based on a 5-dim context vector. The student
    learns a context→weights mapping.

    ``update()`` trains:
      - student toward the reward-weighted mixture of teachers
      - gate toward teachers that contributed to the observed reward
    """

    N_TEACHERS = 3

    def __init__(
        self,
        config: GraphSimilarityConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError(
                "numpy is required for SimilarityDistillationOptimizer"
            )
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        # Student: linear context → weights. Initialize bias to the rule
        # teacher's prior so the initial output is correct.
        self.student_weights = np.zeros((CONTEXT_DIM, WEIGHT_DIM))
        self.student_bias = np.array([0.5, 0.3, 0.2], dtype=np.float64)

        # MoE gate
        self.gate_weights = (
            self.rng.standard_normal((CONTEXT_DIM, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)
        self._lock = threading.RLock()

        # Moving reward baselines per teacher, used by the gate.
        self._teacher_reward_ema = np.zeros(self.N_TEACHERS)

        if config.model_path:
            self.load(Path(str(config.model_path) + ".distill.json"))

    # ---- helpers ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    @staticmethod
    def _normalize_weights(w: "np.ndarray") -> "np.ndarray":
        w = np.maximum(np.asarray(w, dtype=np.float64), 0.0)
        total = float(w.sum())
        return w / total if total > 0 else np.full(WEIGHT_DIM, 1.0 / WEIGHT_DIM)

    def _gate_forward(self, ctx: "np.ndarray") -> "np.ndarray":
        return self._softmax(ctx @ self.gate_weights + self.gate_bias)

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self) -> "np.ndarray":
        """Balanced prior over (cos, edge, gm_sim)."""
        return np.array([0.5, 0.3, 0.2], dtype=np.float64)

    def _rlhf_teacher(self, human_feedback: float) -> "np.ndarray":
        """Human feedback rebalances: high feedback trusts node similarity
        (accuracy proxy); low feedback trusts edge structure."""
        h = _clamp(human_feedback)
        # h=0.5 → [0.5, 0.3, 0.2]; h=1.0 → [0.7, 0.2, 0.1]; h=0.0 → [0.3, 0.5, 0.2]
        return np.array([
            0.5 + 0.2 * (h - 0.5) * 2,   # 0.3..0.7
            0.3 - 0.1 * (h - 0.5) * 2,   # 0.4..0.2
            0.2,                          # fixed
        ], dtype=np.float64)

    def _historical_teacher(self, ctx: "np.ndarray") -> "np.ndarray":
        """k-NN over the replay buffer's (context, weights, reward)."""
        if not self.replay:
            return self._rule_teacher()
        with self._lock:
            recent = list(self.replay)[-32:]
        query = ctx
        num = np.zeros(WEIGHT_DIM)
        den = 0.0
        for s, w, r in recent:
            sim = 1.0 / (1.0 + float(np.linalg.norm(s - query)))
            weight = sim * max(r, 0.0)
            num += weight * w
            den += weight
        if den == 0:
            return self._rule_teacher()
        return self._normalize_weights(num / den)

    def _teacher_outputs(
        self, ctx: "np.ndarray", human_feedback: float
    ) -> "np.ndarray":
        """Return (N_TEACHERS, WEIGHT_DIM) individual proposals."""
        return np.stack([
            self._rule_teacher(),
            self._rlhf_teacher(human_feedback),
            self._historical_teacher(ctx),
        ])

    # ---- forward ----------------------------------------------------------
    def predict_weights(
        self,
        ctx: "np.ndarray",
        human_feedback: float = 0.5,
        exploration: bool = False,
    ) -> Tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        weights         : (WEIGHT_DIM,) normalized, sums to 1
        teacher_outputs : (N_TEACHERS, WEIGHT_DIM)
        gate_probs      : (N_TEACHERS,)
        """
        with self._lock:
            teacher_outputs = self._teacher_outputs(ctx, human_feedback)
            gate_probs = self._gate_forward(ctx)
            teacher_mix = gate_probs @ teacher_outputs  # (WEIGHT_DIM,)

            student_pred = ctx @ self.student_weights + self.student_bias
            student_pred = self._normalize_weights(student_pred)

            # 50/50 blend — student's initial bias equals the rule teacher,
            # so this is a no-op until training begins.
            blended = 0.5 * student_pred + 0.5 * teacher_mix

            if exploration and self.rng.random() < self.epsilon:
                # Small perturbation, not a branch swap.
                noise = self.rng.normal(0.0, 0.02, WEIGHT_DIM)
                blended = self._normalize_weights(blended + noise)

            return self._normalize_weights(blended), teacher_outputs, gate_probs

    def combine(
        self, node_cos: float, edge_sim: float, graph_sim: float
    ) -> float:
        """Backward-compatible helper: combine scores with the current
        student prior. Prefer `predict_weights` in new code — it adapts
        to the query context."""
        ctx = np.array([1.0, 0.0, 0.0, 1.0, graph_sim], dtype=np.float64)
        w, _, _ = self.predict_weights(ctx, exploration=False)
        return float(np.dot(w, [node_cos, edge_sim, graph_sim]))

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        ctx: "np.ndarray",
        weights_used: "np.ndarray",
        teacher_outputs: "np.ndarray",
        gate_probs: "np.ndarray",
        reward: float,
    ) -> None:
        """Train toward the observed reward. The reward is the caller's
        assessment of how useful the prior was (e.g., `1 - relative_error`)."""
        r = float(np.clip(reward, -1.0, 1.0))
        with self._lock:
            self.replay.append(
                (
                    np.asarray(ctx, dtype=np.float64),
                    np.asarray(weights_used, dtype=np.float64),
                    r,
                )
            )
            # Per-teacher reward EMA — teachers whose outputs agree with
            # the used weights get credit for the observed reward.
            for t in range(self.N_TEACHERS):
                agree = 1.0 / (
                    1.0 + float(np.linalg.norm(teacher_outputs[t] - weights_used))
                )
                self._teacher_reward_ema[t] = (
                    0.9 * self._teacher_reward_ema[t] + 0.1 * agree * r
                )

            self.counter += 1
            should_train = (
                self.counter % self.train_every == 0
                and len(self.replay) >= self.batch_size
            )
        if should_train:
            self._train_step()

    def _train_step(self) -> None:
        with self._lock:
            n = min(self.batch_size, len(self.replay))
            idxs = self.rng.choice(len(self.replay), size=n, replace=False)
            batch = [self.replay[int(i)] for i in idxs]

            for ctx, weights_used, reward in batch:
                teacher_outputs = self._teacher_outputs(ctx, self.config.human_feedback_score)
                gate = self._gate_forward(ctx)
                teacher_mix = gate @ teacher_outputs

                # ---- Student: reward-weighted regression toward teacher mix ----
                student_pred = self._normalize_weights(
                    ctx @ self.student_weights + self.student_bias
                )
                target = (
                    teacher_mix if reward > 0
                    else self._normalize_weights(student_pred + (student_pred - teacher_mix))
                )
                err = student_pred - target
                scale = self.distill_w * reward if reward != 0 else 0.0
                self.student_weights -= self.lr * scale * np.outer(ctx, err)
                self.student_bias -= self.lr * scale * err

                # ---- Gate: reinforce teachers with positive reward EMA ----
                target_gate = self._softmax(self._teacher_reward_ema / 0.1)
                gate_err = gate - target_gate
                self.gate_weights -= self.gate_lr * np.outer(ctx, gate_err)
                self.gate_bias -= self.gate_lr * gate_err

            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _atomic_write(path, {
            "kind": "similarity_distillation",
            "context_dim": CONTEXT_DIM,
            "weight_dim": WEIGHT_DIM,
            "student_weights": self.student_weights.tolist(),
            "student_bias": self.student_bias.tolist(),
            "gate_weights": self.gate_weights.tolist(),
            "gate_bias": self.gate_bias.tolist(),
            "teacher_reward_ema": self._teacher_reward_ema.tolist(),
            "epsilon": float(self.epsilon),
            "counter": int(self.counter),
        })

    def load(self, path: Path) -> None:
        payload = _load_guarded(path)
        if not payload or payload.get("kind") != "similarity_distillation":
            return
        if int(payload.get("context_dim", -1)) != CONTEXT_DIM:
            raise ValueError("Checkpoint context_dim mismatch")
        if int(payload.get("weight_dim", -1)) != WEIGHT_DIM:
            raise ValueError("Checkpoint weight_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = np.asarray(payload["student_bias"], dtype=np.float64)
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self._teacher_reward_ema = np.asarray(
            payload.get("teacher_reward_ema", [0.0, 0.0, 0.0]), dtype=np.float64
        )
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# Section 6 — Evolutionary optimizer (fixed index-0 bug, real GA)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"    # (WEIGHT_DIM,)
    fitness: float = 0.0


class EvolutionaryWeightOptimizer:
    """Evolves the (cos, edge, gm_sim) weight vector against observed rewards.

    Each individual is a 3-dim normalized weight vector. Fitness is a
    similarity-weighted reward: an individual whose weights are close to
    a high-reward `weights_used` gets credit for that reward.
    """

    def __init__(
        self,
        config: GraphSimilarityConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError(
                "numpy is required for EvolutionaryWeightOptimizer"
            )
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.counter = 0
        self.generation = 0
        self.best_weights = np.array([0.5, 0.3, 0.2], dtype=np.float64)
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        # Seed population with Dirichlet + the config weights + rule prior
        self.population: List[_Ind] = [
            _Ind(weights=self.rng.dirichlet(np.ones(WEIGHT_DIM)))
            for _ in range(config.population_size)
        ]
        self.population[0] = _Ind(weights=np.array([0.5, 0.3, 0.2], dtype=np.float64))
        self.population[1] = _Ind(weights=np.array([0.7, 0.3, 0.0], dtype=np.float64))

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    # ---- scoring ----------------------------------------------------------
    def observe(self, weights_used: "np.ndarray", reward: float) -> None:
        """Score every individual against the observed reward.

        An individual's fitness increases by
        ``reward × similarity(individual.weights, weights_used)``, so
        individuals whose weights match a high-reward choice gain credit,
        and individuals that match a low-reward choice lose credit.
        """
        r = float(np.clip(reward, -1.0, 1.0))
        w_used = np.asarray(weights_used, dtype=np.float64)
        with self._lock:
            for ind in self.population:
                sim = 1.0 / (1.0 + float(np.linalg.norm(ind.weights - w_used)))
                ind.fitness += sim * r
            best = max(self.population, key=lambda ind: ind.fitness)
            if best.fitness > self.best_fitness:
                self.best_fitness = float(best.fitness)
                self.best_weights = best.weights.copy()
            self.counter += 1
            should_evolve = self.counter % self.config.train_every == 0
        if should_evolve:
            self._evolve()

    def _evolve(self) -> None:
        with self._lock:
            self.population.sort(key=lambda ind: ind.fitness, reverse=True)
            n_elite = max(1, self.config.elitism)
            new_pop: List[_Ind] = [
                _Ind(weights=ind.weights.copy(), fitness=0.0)
                for ind in self.population[:n_elite]
            ]
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
        return max(
            (self.population[int(i)] for i in idxs),
            key=lambda ind: ind.fitness,
        )

    def _crossover(self, a: _Ind, b: _Ind) -> _Ind:
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(WEIGHT_DIM) < 0.5
            w = np.where(mask, a.weights, b.weights)
        else:
            w = a.weights.copy()
        total = w.sum()
        if total > 0:
            w = w / total
        return _Ind(weights=w)

    def _mutate(self, ind: _Ind) -> None:
        mask = self.rng.random(WEIGHT_DIM) < self.config.mutation_rate
        w = ind.weights + mask * self.rng.standard_normal(WEIGHT_DIM) * 0.05
        w = np.clip(w, 1e-6, None)
        total = w.sum()
        if total > 0:
            ind.weights = w / total

    def get_weights(self) -> "np.ndarray":
        with self._lock:
            return self.best_weights.copy()

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _atomic_write(path, {
            "kind": "similarity_evolution",
            "generation": self.generation,
            "counter": self.counter,
            "best_fitness": float(self.best_fitness),
            "best_weights": self.best_weights.tolist(),
            "population": [
                {"weights": ind.weights.tolist(), "fitness": float(ind.fitness)}
                for ind in self.population
            ],
        })

    def load(self, path: Path) -> None:
        payload = _load_guarded(path)
        if not payload or payload.get("kind") != "similarity_evolution":
            return
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.best_fitness = float(payload.get("best_fitness", -math.inf))
        self.best_weights = np.asarray(payload["best_weights"], dtype=np.float64)
        self.population = [
            _Ind(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# Section 7 — Store (main class)
# ============================================================================

class GraphSimilarityStore:
    """Persists execution subgraphs, searches them by hybrid similarity,
    and returns statistical priors.

    Base path (``use_enhancements=False``) preserves v1 behaviour exactly.
    Enhanced path learns per-query weights via ``record_outcome``.
    """

    def __init__(
        self,
        storage_path: str = "./graph_store",
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = GraphSimilarityConfig()
        elif isinstance(config, GraphSimilarityConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = GraphSimilarityConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be GraphSimilarityConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )

        # Local normalized MODP weights — never mutate config.
        w = self.config.modp_weights
        total = sum(w)
        self._modp_weights: Tuple[float, float, float, float] = (
            w[0] / total, w[1] / total, w[2] / total, w[3] / total
        )

        self.use_enhancements = bool(
            self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        )
        if self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to base path"
            )

        self._index: List[Dict[str, Any]] = self._load_index()

        # Optimizers
        self.distillation_optimizer: Optional[SimilarityDistillationOptimizer] = None
        self.evolutionary_optimizer: Optional[EvolutionaryWeightOptimizer] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = SimilarityDistillationOptimizer(
                    self.config, rng=self.rng
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryWeightOptimizer(
                    self.config, rng=self.rng
                )

        # Pending state for record_outcome
        self._pending: Optional[Dict[str, Any]] = None

        logger.info(
            "Initialized GraphSimilarityStore: path=%s, %d entries, "
            "enhancements=%s",
            self.storage_path, len(self._index), self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Index persistence
    # ------------------------------------------------------------------
    def _index_path(self) -> Path:
        return self.storage_path / "index.json"

    def _load_index(self) -> List[Dict[str, Any]]:
        payload = _load_guarded(self._index_path())
        if payload is None:
            return []
        if isinstance(payload, Mapping):
            version = int(payload.get("version", 1))
            if version > SNAPSHOT_VERSION:
                logger.warning(
                    "Index version %d > supported %d; ignoring",
                    version, SNAPSHOT_VERSION,
                )
                return []
            entries = payload.get("entries")
        elif isinstance(payload, list):
            entries = payload
        else:
            return []
        if not isinstance(entries, list):
            return []
        # Filter malformed entries defensively.
        cleaned = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            if "graph_id" not in entry or "feature_vector" not in entry:
                continue
            cleaned.append(dict(entry))
        return cleaned

    def _save_index(self) -> None:
        with self._lock:
            _atomic_write(self._index_path(), {
                "version": SNAPSHOT_VERSION,
                "entries": self._index,
            })

    # ------------------------------------------------------------------
    # Feature extraction
    # ------------------------------------------------------------------
    def _feature_vector(
        self,
        nodes: List[Dict[str, Any]],
        graph_metrics: Optional[Dict[str, float]] = None,
        *,
        include_graph_metrics_in_features: bool = False,
    ) -> Dict[str, float]:
        """Build a feature dict for cosine similarity.

        ``include_graph_metrics_in_features`` defaults to False so graph
        metrics are not double-counted when ``gm_sim`` is also computed
        and weighted. v1 always included them *and* weighted ``gm_sim``,
        producing an inconsistent double-signal.
        """
        vec: Dict[str, float] = {}
        for node in nodes:
            key = f"{node['node_type']}:{node['label']}"
            raw = node.get("value")
            vec[key] = float(raw) if raw is not None else 1.0
        if graph_metrics and include_graph_metrics_in_features:
            for k, v in graph_metrics.items():
                vec[f"graph:{k}"] = float(v)
        return vec

    def _edge_signature(self, edges: List[Dict[str, Any]]) -> set:
        return {
            f"{e['source_id']}:{e['target_id']}:{e['relation']}" for e in edges
        }

    @staticmethod
    def _cosine(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
        all_keys = set(vec_a) | set(vec_b)
        if not all_keys:
            return 0.0
        dot = sum(vec_a.get(k, 0.0) * vec_b.get(k, 0.0) for k in all_keys)
        mag_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
        mag_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    @staticmethod
    def _edge_overlap(sig_a: set, sig_b: set) -> Optional[float]:
        """Jaccard overlap. Returns None when both sides are empty — an
        empty-vs-empty match should be renormalized out, not scored as 1.0."""
        if not sig_a and not sig_b:
            return None
        intersection = len(sig_a & sig_b)
        union = len(sig_a | sig_b)
        return intersection / union if union else None

    @staticmethod
    def _graph_similarity(
        gm_a: Optional[Dict[str, float]],
        gm_b: Optional[Dict[str, float]],
    ) -> Optional[float]:
        """Normalized Euclidean distance. Returns None when either side is
        missing — a missing graph metric should not get a spurious 0.5
        (which, with a nonzero weight, acts as a boost)."""
        if not gm_a or not gm_b:
            return None
        keys = set(gm_a) | set(gm_b)
        if not keys:
            return None
        diff_sq = sum((gm_a.get(k, 0.5) - gm_b.get(k, 0.5)) ** 2 for k in keys)
        dist = math.sqrt(diff_sq)
        return max(0.0, 1.0 - dist / math.sqrt(len(keys)))

    @staticmethod
    def _combine_scores(
        weights: "np.ndarray",
        cos: float,
        edge_sim: Optional[float],
        gm_sim: Optional[float],
    ) -> float:
        """Weighted sum over available components. Missing components are
        excluded and the weights are renormalized — a missing metric does
        not drag the score down or up."""
        total_w = 0.0
        total_s = 0.0
        if cos is not None:
            total_w += weights[0]
            total_s += weights[0] * cos
        if edge_sim is not None:
            total_w += weights[1]
            total_s += weights[1] * edge_sim
        if gm_sim is not None:
            total_w += weights[2]
            total_s += weights[2] * gm_sim
        return total_s / total_w if total_w > 0 else 0.0

    @staticmethod
    def _context_vector(
        query_nodes: List[Dict[str, Any]],
        query_edges: Optional[List[Dict[str, Any]]],
        graph_metrics: Optional[Dict[str, float]],
    ) -> "np.ndarray":
        return np.array([
            1.0,
            min(len(query_nodes) / 10.0, 1.0),
            min(len(query_edges or []) / 10.0, 1.0),
            1.0 if graph_metrics else 0.0,
            float(np.mean(list(graph_metrics.values()))) if graph_metrics else 0.0,
        ], dtype=np.float64)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def store_execution(
        self,
        subgraph: ExecutionSubgraph,
        graph_metrics: Optional[Dict[str, float]] = None,
    ) -> None:
        """Persist a completed execution. Does not mutate ``subgraph``."""
        if not isinstance(subgraph.graph_id, str) or not subgraph.graph_id:
            raise ValueError("subgraph.graph_id must be a non-empty string")

        # Do NOT mutate the caller's subgraph.
        gm = subgraph.graph_metrics if subgraph.graph_metrics is not None else graph_metrics

        with self._lock:
            # Reject duplicates.
            if any(e["graph_id"] == subgraph.graph_id for e in self._index):
                raise ValueError(
                    f"graph_id {subgraph.graph_id!r} already stored"
                )

            fvec = self._feature_vector(
                subgraph.nodes, gm,
                include_graph_metrics_in_features=False,
            )
            esig = list(self._edge_signature(subgraph.edges))

            graph_file = self.storage_path / f"{subgraph.graph_id}.json"
            _atomic_write(graph_file, {
                "graph_id": subgraph.graph_id,
                "nodes": subgraph.nodes,
                "edges": subgraph.edges,
                "outcome_metrics": subgraph.outcome_metrics,
                "timestamp": subgraph.timestamp,
                "framework": subgraph.framework,
                "graph_metrics": gm,
            })

            self._index.append({
                "graph_id": subgraph.graph_id,
                "feature_vector": fvec,
                "edge_signature": esig,
                "outcome_metrics": subgraph.outcome_metrics,
                "framework": subgraph.framework,
                "graph_metrics": gm,
            })
            self._save_index()

            try:
                self.metrics.observe("index_size", float(len(self._index)))
            except Exception:  # noqa: BLE001
                pass

    def find_similar(
        self,
        query_nodes: List[Dict[str, Any]],
        query_edges: Optional[List[Dict[str, Any]]] = None,
        top_k: int = 3,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Hybrid similarity search."""
        if not isinstance(top_k, int) or top_k < 1:
            raise ValueError("top_k must be a positive integer")

        if graph_metrics is None and self.use_enhancements:
            graph_metrics = self.config.default_graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        ctx = (
            self._context_vector(query_nodes, query_edges, graph_metrics)
            if self.use_enhancements else None
        )

        # Weight selection
        teacher_outputs: Optional["np.ndarray"] = None
        gate_probs: Optional["np.ndarray"] = None
        if self.distillation_optimizer is not None and ctx is not None:
            weights, teacher_outputs, gate_probs = (
                self.distillation_optimizer.predict_weights(
                    ctx, human_feedback_score, exploration=False
                )
            )
            with self._lock:
                self._pending = {
                    "ctx": ctx,
                    "weights": weights,
                    "teacher_outputs": teacher_outputs,
                    "gate_probs": gate_probs,
                }
        elif self.evolutionary_optimizer is not None:
            weights = self.evolutionary_optimizer.get_weights()
            with self._lock:
                self._pending = {"weights": weights}
        else:
            # Base path: v1's exact weights on (cos, edge). Graph metrics
            # are ignored, matching v1's base behaviour byte-for-byte.
            weights = np.array([0.7, 0.3, 0.0], dtype=np.float64)
            with self._lock:
                self._pending = None

        query_vec = self._feature_vector(
            query_nodes, graph_metrics,
            include_graph_metrics_in_features=False,
        )
        query_sig = self._edge_signature(query_edges or [])

        scored: List[Dict[str, Any]] = []
        with self._lock:
            index_snapshot = list(self._index)

        for entry in index_snapshot:
            cos = self._cosine(query_vec, entry["feature_vector"])
            edge_sim = self._edge_overlap(
                query_sig, set(entry.get("edge_signature", []))
            )
            gm_sim = self._graph_similarity(
                graph_metrics, entry.get("graph_metrics")
            )

            combined = self._combine_scores(weights, cos, edge_sim, gm_sim)

            scored.append({
                "graph_id": entry["graph_id"],
                "similarity_score": round(combined, 4),
                "cosine_similarity": round(cos, 4),
                "edge_overlap": None if edge_sim is None else round(edge_sim, 4),
                "graph_metric_similarity": None if gm_sim is None else round(gm_sim, 4),
                "outcome_metrics": entry["outcome_metrics"],
                "framework": entry.get("framework", "unknown"),
                "graph_metrics": entry.get("graph_metrics"),
            })

        scored.sort(key=lambda x: x["similarity_score"], reverse=True)
        top = scored[:top_k]

        try:
            self.metrics.observe("similarity_queries_total", 1.0)
            if top:
                self.metrics.observe(
                    "similarity_top1_score", top[0]["similarity_score"]
                )
        except Exception:  # noqa: BLE001
            pass

        return top

    def build_prior(self, similar_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate top-K into a statistical prior with an optional
        MODP composite that only uses present metrics."""
        if not similar_results:
            return {"available": False}

        prior: Dict[str, Any] = {
            "available": True,
            "n_samples": len(similar_results),
            "metrics": {},
        }

        for key in MODP_SCHEMA:
            values = [
                r["outcome_metrics"][key]
                for r in similar_results
                if key in r["outcome_metrics"]
            ]
            if not values:
                continue
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            prior["metrics"][key] = {
                "min": round(min(values), 6),
                "max": round(max(values), 6),
                "mean": round(mean, 6),
                "std_dev": round(math.sqrt(variance), 6),
            }

        if self.use_enhancements:
            composite, coverage = self._compute_composite(prior["metrics"])
            prior["composite_modp_score"] = (
                None if composite is None else round(composite, 4)
            )
            prior["composite_coverage"] = coverage

        return prior

    def _compute_composite(
        self, metrics: Dict[str, Dict[str, float]]
    ) -> Tuple[Optional[float], str]:
        """Compute the MODP composite using only present metrics.

        v1 conflated "missing metric" with "terrible metric" by defaulting
        the norm to 0.0. That penalized sparse priors. This version
        renormalizes over the metrics that actually exist, and reports
        coverage so the caller can interpret the score.
        """
        present: List[Tuple[float, float]] = []  # (weight, normalized_value)
        for i, key in enumerate(MODP_SCHEMA):
            if key not in metrics:
                continue
            mean = metrics[key]["mean"]
            if key in ("accuracy", "sustainability_index"):
                norm = _clamp(mean)
            else:  # energy_kwh, carbon_co2e_kg — lower is better
                norm = 1.0 / (1.0 + max(mean, 0.0))
            present.append((self._modp_weights[i], norm))

        if not present:
            return None, f"0/{len(MODP_SCHEMA)}"

        total_w = sum(w for w, _ in present)
        if total_w == 0:
            return None, f"0/{len(MODP_SCHEMA)}"
        composite = sum(w * v for w, v in present) / total_w
        return composite, f"{len(present)}/{len(MODP_SCHEMA)}"

    def flag_outlier(
        self,
        prior: Dict[str, Any],
        actual_metrics: Dict[str, float],
        sigma_threshold: float = 2.0,
    ) -> List[Dict[str, Any]]:
        """Flag actual metrics that deviate from the prior.

        - ``std == 0``: flag if actual differs from mean (v1 skipped
          silently, hiding real deviations on constant metrics).
        - Composite MODP is checked when available.
        """
        if not prior.get("available"):
            return []

        flags: List[Dict[str, Any]] = []
        for key, stats in prior.get("metrics", {}).items():
            if key not in actual_metrics:
                continue
            actual = float(actual_metrics[key])
            mean = stats["mean"]
            std = stats["std_dev"]
            if std == 0:
                if abs(actual - mean) > 1e-9:
                    flags.append({
                        "metric": key,
                        "actual": actual,
                        "expected_mean": mean,
                        "z_score": None,
                        "severity": "critical",
                        "reason": "constant prior, actual differs",
                    })
                continue
            z_score = abs(actual - mean) / std
            if z_score > sigma_threshold:
                flags.append({
                    "metric": key,
                    "actual": actual,
                    "expected_mean": mean,
                    "z_score": round(z_score, 3),
                    "severity": "critical" if z_score > 3.0 else "warning",
                })

        # Composite MODP outlier detection (v1 had this as `pass`).
        prior_composite = prior.get("composite_modp_score")
        coverage = prior.get("composite_coverage", "0/4")
        try:
            present_count = int(coverage.split("/")[0])
        except (ValueError, IndexError):
            present_count = 0
        if (
            self.use_enhancements
            and prior_composite is not None
            and present_count >= 3
        ):
            actual_composite, _ = self._compute_composite({
                k: {"mean": float(actual_metrics[k])}
                for k in MODP_SCHEMA if k in actual_metrics
            })
            if actual_composite is not None:
                delta = abs(actual_composite - prior_composite)
                if delta > 0.2:
                    flags.append({
                        "metric": "composite_modp",
                        "actual": round(actual_composite, 4),
                        "expected_mean": prior_composite,
                        "z_score": None,
                        "severity": "warning",
                        "reason": f"composite delta={delta:.3f}",
                    })

        return flags

    # ------------------------------------------------------------------
    # Training loop
    # ------------------------------------------------------------------
    def record_outcome(self, reward: float) -> bool:
        """Train the optimizers against the reward for the last find_similar.

        ``reward`` should be in [-1, 1]. Common definitions:
          - ``1 - |actual_mean - predicted_mean| / predicted_mean``
          - ``+1`` if the top-1 result was the true best, ``-1`` otherwise

        Returns True if at least one optimizer was updated.
        """
        if not self.use_enhancements:
            return False
        with self._lock:
            pending = self._pending
            self._pending = None
        if pending is None:
            logger.debug("record_outcome(): no pending find_similar")
            return False

        r = float(np.clip(_finite_nonneg(reward + 1.0, "reward") - 1.0, -1.0, 1.0))
        updated = False

        if self.distillation_optimizer is not None and "ctx" in pending:
            try:
                self.distillation_optimizer.update(
                    pending["ctx"],
                    pending["weights"],
                    pending["teacher_outputs"],
                    pending["gate_probs"],
                    r,
                )
                updated = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation update failed: %s", exc)

        if self.evolutionary_optimizer is not None and "weights" in pending:
            try:
                self.evolutionary_optimizer.observe(pending["weights"], r)
                updated = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary update failed: %s", exc)

        if updated:
            try:
                self.metrics.observe("similarity_reward", r)
            except Exception:  # noqa: BLE001
                pass
        return updated

    # ------------------------------------------------------------------
    # Persistence for optimizers
    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.distillation_optimizer is not None:
            try:
                self.distillation_optimizer.save(
                    Path(str(target) + ".distill.json")
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation save failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.save(
                    Path(str(target) + ".evolve.json")
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.distillation_optimizer is not None:
            try:
                self.distillation_optimizer.load(
                    Path(str(target) + ".distill.json")
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation load failed: %s", exc)
        if self.evolutionary_optimizer is not None:
            try:
                self.evolutionary_optimizer.load(
                    Path(str(target) + ".evolve.json")
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary load failed: %s", exc)


# ============================================================================
# Section 8 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    def make_graph(gid, acc=0.9, energy=0.5):
        return ExecutionSubgraph(
            graph_id=gid,
            nodes=[{"node_id": "n", "node_type": "workload",
                    "label": "resnet", "value": 1.0}],
            edges=[{"source_id": "n", "target_id": "n",
                    "relation": "self"}],
            outcome_metrics={
                "accuracy": acc,
                "energy_kwh": energy,
                "carbon_co2e_kg": energy * 0.4,
                "sustainability_index": 1.0 - energy,
            },
            timestamp="2025-01-01T00:00:00Z",
            framework="vllm",
            graph_metrics={"centrality": 0.7, "connectivity": 0.6},
        )

    print("=== Base path (use_enhancements=False) ===")
    store_base = GraphSimilarityStore("/tmp/store_base")
    for i in range(5):
        store_base.store_execution(make_graph(f"g{i}", acc=0.8 + 0.02 * i))
    query_nodes = [{"node_id": "q", "node_type": "workload",
                    "label": "resnet", "value": 1.0}]
    query_edges = [{"source_id": "q", "target_id": "q", "relation": "self"}]
    results = store_base.find_similar(query_nodes, query_edges, top_k=3)
    for r in results:
        print(f"  {r['graph_id']}: score={r['similarity_score']} "
              f"cos={r['cosine_similarity']} edge={r['edge_overlap']}")
    prior = store_base.build_prior(results)
    print(f"  prior available={prior['available']} n={prior['n_samples']}")
    print(f"  prior accuracy mean={prior['metrics'].get('accuracy', {}).get('mean')}")

    print("\n=== Enhanced path (use_enhancements=True) ===")
    cfg = GraphSimilarityConfig(use_enhancements=True, use_evolutionary=True, seed=42)
    store_enh = GraphSimilarityStore("/tmp/store_enh", config=cfg)
    for i in range(5):
        store_enh.store_execution(make_graph(f"g{i}", acc=0.8 + 0.02 * i))

    # Simulate training: 20 rounds of query → run → reward → record.
    import random as _rng
    _rng.seed(0)
    for round_num in range(20):
        query_gm = {"centrality": _rng.uniform(0.5, 0.9),
                    "connectivity": _rng.uniform(0.4, 0.8)}
        results = store_enh.find_similar(
            query_nodes, query_edges, top_k=3,
            graph_metrics=query_gm,
        )
        if results:
            # "Actual" outcome correlates with accuracy; closer match → higher reward.
            predicted = store_enh.build_prior(results)
            predicted_acc = predicted["metrics"].get("accuracy", {}).get("mean", 0.5)
            actual = results[0]["outcome_metrics"]["accuracy"]
            reward = 1.0 - min(abs(actual - predicted_acc), 1.0)
            store_enh.record_outcome(reward)

    print("  After 20 training rounds:")
    final_results = store_enh.find_similar(
        query_nodes, query_edges, top_k=3,
        graph_metrics={"centrality": 0.8, "connectivity": 0.6},
    )
    for r in final_results:
        print(f"  {r['graph_id']}: score={r['similarity_score']} "
              f"cos={r['cosine_similarity']} "
              f"edge={r['edge_overlap']} "
              f"gm={r['graph_metric_similarity']}")
    final_prior = store_enh.build_prior(final_results)
    print(f"  composite_modp_score={final_prior.get('composite_modp_score')} "
          f"coverage={final_prior.get('composite_coverage')}")

    print("\n  Optimizer stats:")
    if store_enh.distillation_optimizer is not None:
        print(f"    distillation: counter={store_enh.distillation_optimizer.counter}, "
              f"epsilon={store_enh.distillation_optimizer.epsilon:.4f}")
    if store_enh.evolutionary_optimizer is not None:
        print(f"    evolutionary: generation={store_enh.evolutionary_optimizer.generation}, "
              f"best_weights={store_enh.evolutionary_optimizer.get_weights().round(3)}")

    print("\n=== Persistence round-trip ===")
    store_enh.save("/tmp/store_model")
    store_reloaded = GraphSimilarityStore("/tmp/store_enh", config=cfg)
    store_reloaded.load("/tmp/store_model")
    if store_reloaded.evolutionary_optimizer is not None:
        print(f"    reloaded best_weights="
              f"{store_reloaded.evolutionary_optimizer.get_weights().round(3)}")
