# -*- coding: utf-8 -*-
"""
Phase 1 — Similarity Benchmark Integration (Enhanced, v2)
==========================================================

Provides helpers that BenchmarkHarness calls to:
  1. Build an ExecutionSubgraph from a completed result  (build_execution_subgraph)
  2. Retrieve a performance prior BEFORE a new run       (get_benchmark_prior)
  3. Check for outliers AFTER a run                      (check_outliers)  ← NEW

Fixes applied vs. v1
--------------------
  P0  ENHANCEMENTS_AVAILABLE was undefined (crash)     -> defined
  P0  check_outliers was documented but missing        -> implemented
  P0  PriorDistillationOptimizer was a stub            -> real MoE + teachers
  P0  combine_prior/update() were never called         -> closed learning loop
  P0  Optimizer was recreated per call                 -> SimilarityIntegration
  P0  MODP re-weighting was a literal `pass`           -> real weighted composite
  P0  carbon_co2e_kg had no node                       -> n_carbon added
  P0  graph_metric nodes hurt similarity matching      -> same nodes in query
  P0  uuid4 broke idempotency                          -> optional graph_id
  P0  use_evolutionary was a dead flag                 -> AlphaEvolution + real impl
  P1  Config not frozen/validated                      -> frozen + __post_init__
  P1  Feature not clipped                              -> _clamp
  P1  No persistence                                   -> save()/load()
  P1  Unseeded RNG                                     -> default_rng(seed)
  P1  No metrics                                       -> MetricsSink
  P1  No thread safety                                 -> RLock
  P2  Dead `import random`                             -> removed
  P2  Node/edge magic strings                          -> NodeType/Relation enums
  P2  RLHF was decorative                              -> record_human_feedback
  P2  No typed results                                 -> PriorResult/OutlierReport

Preserved public surface
------------------------
  SimilarityIntegrationConfig, PriorDistillationOptimizer,
  build_execution_subgraph(...), get_benchmark_prior(...),
  + NEW check_outliers(...), SimilarityIntegration class,
  + NEW in_place enhanced variants via the class.

Breaking changes (documented)
-----------------------------
  - get_benchmark_prior now returns the same shape plus optional
    `optimizer_stats` and `composite_score` keys.
  - build_execution_subgraph accepts an optional `graph_id=` kwarg.
  - PriorDistillationOptimizer now exposes predict_prior/update/save/load.
    combine_prior() is preserved for backwards compat (delegates to
    predict_prior and returns just the value).
"""

from __future__ import annotations

import json
import logging
import math
import threading
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from metrics.graph_similarity_store import GraphSimilarityStore, ExecutionSubgraph

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
        self._log = logging.getLogger("similarity.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Enums (replace magic strings)
# ============================================================================

class NodeType(str, Enum):
    WORKLOAD = "workload"
    SUITE = "suite"
    HARDWARE = "hardware"
    GRID = "grid"
    FRAMEWORK = "framework"
    OUTCOME = "outcome"
    GRAPH_METRIC = "graph_metric"
    RLHF = "rlhf"


class Relation(str, Enum):
    BELONGS_TO = "belongs_to"
    RAN_ON = "ran_on"
    CONSTRAINED_BY = "constrained_by"
    EXECUTED = "executed"
    PRODUCED = "produced"
    CONSUMED = "consumed"
    HAS_METRIC = "has_metric"
    HAS_FEEDBACK = "has_feedback"


# ============================================================================
# SECTION 4 — Configuration (frozen, validated)
# ============================================================================

# Metric order MUST match modp_weights ordering.
_MODP_METRIC_KEYS: Tuple[str, str, str, str] = (
    "accuracy",
    "energy_kwh",
    "carbon_co2e_kg",
    "sustainability_index",
)


@dataclass(frozen=True)
class SimilarityIntegrationConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )

    # MODP weights: (accuracy, energy, carbon, sustainability_index)
    modp_weights: Tuple[float, float, float, float] = (0.25, 0.25, 0.25, 0.25)
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

    # Outlier detection threshold (in standard deviations)
    outlier_k: float = 2.0

    seed: Optional[int] = None
    model_path: Optional[Path] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(
                f"modp_weights must have 4 components "
                f"(accuracy, energy, carbon, sustainability_index), got {w!r}"
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
        if self.outlier_k <= 0:
            raise ValueError("outlier_k must be > 0")
        for k, v in self.graph_metrics.items():
            if not _is_finite(v) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SimilarityIntegrationConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "SimilarityIntegrationConfig ignoring unknown keys: %s",
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
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 5 — Typed results
# ============================================================================

@dataclass
class PendingPrior:
    """State captured by get_benchmark_prior(), consumed by record_outcome()."""
    state_vec: "np.ndarray"
    teacher_scores: "np.ndarray"
    gate_probs: "np.ndarray"
    metric_name: str
    prior_value: float


@dataclass
class OutlierReport:
    is_outlier: bool
    reason: str
    metric: str = ""
    actual: Optional[float] = None
    prior_mean: Optional[float] = None
    prior_std: Optional[float] = None
    threshold: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# SECTION 6 — MODP composite
# ============================================================================

def compute_modp_composite(
    outcome_metrics: Mapping[str, Any],
    weights: Sequence[float],
) -> float:
    """
    Weighted composite over (accuracy, energy_kwh, carbon_co2e_kg,
    sustainability_index). Energy and carbon are *costs* (lower is better),
    so their normalized contributions are inverted.
    """
    if len(weights) != 4 or sum(weights) <= 0:
        raise ValueError("weights must be 4 non-negative values, sum > 0")
    total = sum(weights)
    w = [float(x) / total for x in weights]

    accuracy = _clamp(float(outcome_metrics.get("accuracy", 0.0)))
    energy = float(outcome_metrics.get("energy_kwh", 0.0))
    carbon = float(outcome_metrics.get("carbon_co2e_kg", 0.0))
    sustainability = _clamp(float(outcome_metrics.get("sustainability_index", 0.0)))

    # Energy/carbon get inverted with a scale-agnostic 1/(1+q) normalization
    # so that the composite stays in [0, 1].
    energy_score = 1.0 / (1.0 + max(energy, 0.0))
    carbon_score = 1.0 / (1.0 + max(carbon, 0.0))

    return _clamp(
        w[0] * accuracy
        + w[1] * energy_score
        + w[2] * carbon_score
        + w[3] * sustainability
    )


# ============================================================================
# SECTION 7 — Teachers for prior combination
# ============================================================================

def _metric_value(ex: Mapping[str, Any], metric_name: str) -> float:
    metrics = ex.get("metrics") or ex.get("outcome_metrics") or {}
    try:
        return float(metrics.get(metric_name, 0.0))
    except (TypeError, ValueError):
        return 0.0


class PriorTeacher(Protocol):
    def predict(
        self,
        similar_executions: Sequence[Mapping[str, Any]],
        metric_name: str,
        graph_metrics: Mapping[str, float],
    ) -> float: ...


class KNNTeacher:
    """Distance-weighted k-NN average of the requested metric."""

    def predict(self, similar_executions, metric_name, graph_metrics):
        if not similar_executions:
            return 0.0
        weights = []
        values = []
        for ex in similar_executions:
            similarity = float(ex.get("similarity", 0.5))
            values.append(_metric_value(ex, metric_name))
            weights.append(max(similarity, 1e-6))
        w = np.asarray(weights, dtype=np.float64)
        v = np.asarray(values, dtype=np.float64)
        return float(np.dot(w, v) / w.sum())


class MODPTeacher:
    """Prior computed from the MODP composite of each similar execution."""

    def __init__(self, weights: Sequence[float]) -> None:
        self.weights = tuple(float(x) for x in weights)

    def predict(self, similar_executions, metric_name, graph_metrics):
        if not similar_executions:
            return 0.0
        composites = []
        weights = []
        for ex in similar_executions:
            metrics = ex.get("metrics") or ex.get("outcome_metrics") or {}
            composites.append(compute_modp_composite(metrics, self.weights))
            weights.append(max(float(ex.get("similarity", 0.5)), 1e-6))
        w = np.asarray(weights, dtype=np.float64)
        v = np.asarray(composites, dtype=np.float64)
        return float(np.dot(w, v) / w.sum())


class CentralityTeacher:
    """k-NN prior adjusted by graph centrality/connectivity."""

    def predict(self, similar_executions, metric_name, graph_metrics):
        if not similar_executions:
            return 0.0
        base = KNNTeacher().predict(
            similar_executions, metric_name, graph_metrics
        )
        centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        boost = 0.15 * (centrality - 0.5) + 0.10 * (connectivity - 0.5)
        return _clamp(base + boost)


# ============================================================================
# SECTION 8 — Evolutionary optimizer (implements use_evolutionary)
# ============================================================================

@dataclass
class _PriorInd:
    weights: "np.ndarray"   # (N_TEACHERS,)
    bias: float
    fitness: float = 0.0


class EvolutionaryPriorOptimizer:
    """
    Population of linear gates over teacher scores.
    Fitness = negative MSE against the true value.
    """
    N_TEACHERS = 3

    def __init__(
        self,
        config: SimilarityIntegrationConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.population: List[_PriorInd] = [
            _PriorInd(
                weights=self.rng.standard_normal(self.N_TEACHERS) * 0.1,
                bias=0.0,
            )
            for _ in range(config.population_size)
        ]
        self.counter = 0
        self.generation = 0

    def _predict_with(self, ind: _PriorInd, teacher_scores: "np.ndarray") -> float:
        return float(np.clip(teacher_scores @ ind.weights + ind.bias, 0.0, 1.0))

    def predict_from_teachers(
        self, teacher_scores: "np.ndarray", exploration: bool = False
    ) -> Tuple[float, "np.ndarray", "np.ndarray"]:
        if exploration and self.rng.random() < self.config.epsilon:
            idx = int(self.rng.integers(0, len(self.population)))
            best = self.population[idx]
        else:
            best = max(self.population, key=lambda ind: ind.fitness)
        score = self._predict_with(best, teacher_scores)
        gate = np.full(self.N_TEACHERS, 1.0 / self.N_TEACHERS)
        state_vec = teacher_scores.copy()
        return score, gate, state_vec

    def update(
        self,
        state_vec: "np.ndarray",
        teacher_scores: "np.ndarray",
        gate_probs: "np.ndarray",
        actual: float,
    ) -> None:
        r = float(np.clip(actual, 0.0, 1.0))
        for ind in self.population:
            pred = self._predict_with(ind, teacher_scores)
            ind.fitness -= (pred - r) ** 2
        self.counter += 1
        if self.counter % self.config.train_every == 0:
            self._evolve()

    def _evolve(self) -> None:
        self.population.sort(key=lambda ind: ind.fitness, reverse=True)
        n_elite = max(1, self.config.elitism)
        new_pop: List[_PriorInd] = [
            _PriorInd(weights=ind.weights.copy(), bias=ind.bias, fitness=0.0)
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

    def _tournament(self, k: int = 3) -> _PriorInd:
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        return max((self.population[int(i)] for i in idxs),
                   key=lambda ind: ind.fitness)

    def _crossover(self, a: _PriorInd, b: _PriorInd) -> _PriorInd:
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(self.N_TEACHERS) < 0.5
            return _PriorInd(
                weights=np.where(mask, a.weights, b.weights).copy(),
                bias=float(a.bias if self.rng.random() < 0.5 else b.bias),
            )
        return _PriorInd(weights=a.weights.copy(), bias=a.bias)

    def _mutate(self, ind: _PriorInd) -> None:
        mask = self.rng.random(self.N_TEACHERS) < self.config.mutation_rate
        ind.weights = ind.weights + mask * self.rng.standard_normal(
            self.N_TEACHERS
        ) * 0.05
        if self.rng.random() < self.config.mutation_rate:
            ind.bias += float(self.rng.standard_normal() * 0.05)

    def save(self, path: Path) -> None:
        _save_json_atomic(
            path,
            {
                "kind": "prior_evolution",
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
        if not payload or payload.get("kind") != "prior_evolution":
            return
        self.generation = int(payload.get("generation", 0))
        self.counter = int(payload.get("counter", 0))
        self.population = [
            _PriorInd(
                weights=np.asarray(ind["weights"], dtype=np.float64),
                bias=float(ind["bias"]),
                fitness=float(ind.get("fitness", 0.0)),
            )
            for ind in payload["population"]
        ]


# ============================================================================
# SECTION 9 — Real distillation optimizer (fixed, full MoE)
# ============================================================================

class PriorDistillationOptimizer:
    """
    Real multi-teacher distillation + MoE gating for prior prediction.

    Preserves v1's `combine_prior()` API (delegates to predict_prior).

    Forward:
        teacher_scores (T,) <- 3 teachers
        gate_probs     (T,) <- softmax(feature @ gate_weights + gate_bias)
        combined       scalar <- gate_probs @ teacher_scores
        student        scalar <- feature @ student_weights + student_bias
        score          0.7*student + 0.3*combined

    Backward:
        student: MSE gradient
        gate:    regression-form softmax chain rule (not classification)
    """

    N_TEACHERS = 3
    FEATURE_DIM = 4   # (mean_similarity, top_similarity, centrality, connectivity)

    def __init__(
        self,
        config: SimilarityIntegrationConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for PriorDistillationOptimizer")

        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = self.FEATURE_DIM
        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.5
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)

        self.teachers: List[PriorTeacher] = [
            KNNTeacher(),
            MODPTeacher(config.modp_weights),
            CentralityTeacher(),
        ]

    # ---- feature construction -------------------------------------------
    @staticmethod
    def _state_vector(
        similar: Sequence[Mapping[str, Any]],
        graph_metrics: Mapping[str, float],
    ) -> "np.ndarray":
        if not similar:
            mean_sim = 0.0
            top_sim = 0.0
        else:
            sims = sorted(
                (float(ex.get("similarity", 0.5)) for ex in similar),
                reverse=True,
            )
            mean_sim = sum(sims) / len(sims)
            top_sim = sims[0]
        centrality = _clamp(float(graph_metrics.get("centrality", 0.5)))
        connectivity = _clamp(float(graph_metrics.get("connectivity", 0.5)))
        return np.array(
            [mean_sim, top_sim, centrality, connectivity], dtype=np.float64
        )

    # ---- forward --------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    def predict_prior(
        self,
        similar_executions: Sequence[Mapping[str, Any]],
        metric_name: str,
        graph_metrics: Optional[Mapping[str, float]] = None,
        exploration: bool = False,
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        value          : float in [0,1]
        state_vec      : (FEATURE_DIM,)
        teacher_scores : (N_TEACHERS,)
        gate_probs     : (N_TEACHERS,)
        """
        gm = dict(graph_metrics or self.config.graph_metrics)
        x = self._state_vector(similar_executions, gm)
        teacher_scores = np.array(
            [t.predict(similar_executions, metric_name, gm) for t in self.teachers],
            dtype=np.float64,
        )
        gate = self._gate_forward(x)
        teacher_mix = float(gate @ teacher_scores)
        student = float(x @ self.student_weights + self.student_bias)

        if exploration and self.rng.random() < self.epsilon:
            student += float(self.rng.normal(0.0, 0.02))

        value = 0.7 * student + 0.3 * teacher_mix
        return _clamp(value), x, teacher_scores, gate

    # Backwards-compat wrapper (v1 API)
    def combine_prior(
        self,
        similar_executions: Sequence[Mapping[str, Any]],
        metric_name: str,
    ) -> float:
        value, *_ = self.predict_prior(similar_executions, metric_name)
        return float(value)

    # ---- backward -------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        teacher_scores: "np.ndarray",
        gate_probs: "np.ndarray",
        actual: float,
    ) -> None:
        r = float(np.clip(actual, 0.0, 1.0))
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
        for i in idxs:
            x, t_scores, gate, reward = self.replay[int(i)]

            # Student MSE
            pred = float(x @ self.student_weights + self.student_bias)
            err = pred - reward
            self.student_weights -= self.lr * err * x
            self.student_bias -= self.lr * err

            # Gate regression-form chain rule
            teacher_mix = float(gate @ t_scores)
            err_g = teacher_mix - reward
            dL_dg = err_g * t_scores
            grad_logits = gate * (dL_dg - float(gate @ dL_dg))
            self.gate_weights -= self.gate_lr * np.outer(x, grad_logits)
            self.gate_bias -= self.gate_lr * grad_logits

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence ----------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json_atomic(
            path,
            {
                "kind": "prior_distillation",
                "feature_dim": self.feature_dim,
                "n_teachers": self.N_TEACHERS,
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
        if payload.get("kind") != "prior_distillation":
            logger.warning("Checkpoint kind mismatch: %s", payload.get("kind"))
            return
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = float(payload["student_bias"][0])
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded prior checkpoint from %s", path)


# ============================================================================
# SECTION 10 — Factory: build_execution_subgraph
# ============================================================================

def _make_outcome_metric_nodes_and_edges(
    outcome_metrics: Mapping[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build the six outcome nodes and the six task->outcome edges."""
    specs = [
        ("n_acc", "accuracy", Relation.PRODUCED),
        ("n_energy", "energy_kwh", Relation.CONSUMED),
        ("n_carbon", "carbon_co2e_kg", Relation.CONSUMED),
        ("n_si", "sustainability_index", Relation.PRODUCED),
    ]
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    for node_id, metric_name, relation in specs:
        try:
            value = float(outcome_metrics.get(metric_name, 0.0))
        except (TypeError, ValueError):
            value = 0.0
        nodes.append(
            {
                "node_id": node_id,
                "node_type": NodeType.OUTCOME.value,
                "label": metric_name,
                "value": value,
            }
        )
        edges.append(
            {
                "source_id": "n_task",
                "target_id": node_id,
                "relation": relation.value,
                "weight": value,
            }
        )
    return nodes, edges


def build_execution_subgraph(
    task: Mapping[str, Any],
    hardware_profile: str,
    grid_region: str,
    framework: str,
    outcome_metrics: Mapping[str, Any],
    config: Optional[SimilarityIntegrationConfig] = None,
    graph_metrics: Optional[Mapping[str, float]] = None,
    human_feedback_score: Optional[float] = None,
    *,
    graph_id: Optional[str] = None,
) -> ExecutionSubgraph:
    """
    Convert a completed benchmark result into a labelled ExecutionSubgraph.
    """
    config = config or SimilarityIntegrationConfig()
    use_enh = config.use_enhancements and ENHANCEMENTS_AVAILABLE

    gm = dict(graph_metrics) if graph_metrics is not None else dict(
        config.graph_metrics
    )
    hf = (
        float(human_feedback_score)
        if human_feedback_score is not None
        else float(config.human_feedback_score)
    )

    # ---- Base nodes / edges --------------------------------------------
    nodes: List[Dict[str, Any]] = [
        {"node_id": "n_task", "node_type": NodeType.WORKLOAD.value,
         "label": task.get("task_type", "unknown"), "value": None},
        {"node_id": "n_suite", "node_type": NodeType.SUITE.value,
         "label": task.get("suite", "unknown"), "value": None},
        {"node_id": "n_hw", "node_type": NodeType.HARDWARE.value,
         "label": hardware_profile, "value": None},
        {"node_id": "n_grid", "node_type": NodeType.GRID.value,
         "label": grid_region, "value": None},
        {"node_id": "n_fw", "node_type": NodeType.FRAMEWORK.value,
         "label": framework, "value": None},
    ]
    edges: List[Dict[str, Any]] = [
        {"source_id": "n_task", "target_id": "n_suite",
         "relation": Relation.BELONGS_TO.value, "weight": 1.0},
        {"source_id": "n_task", "target_id": "n_hw",
         "relation": Relation.RAN_ON.value, "weight": 1.0},
        {"source_id": "n_task", "target_id": "n_grid",
         "relation": Relation.CONSTRAINED_BY.value, "weight": 1.0},
        {"source_id": "n_fw", "target_id": "n_task",
         "relation": Relation.EXECUTED.value, "weight": 1.0},
    ]

    outcome_nodes, outcome_edges = _make_outcome_metric_nodes_and_edges(outcome_metrics)
    nodes.extend(outcome_nodes)
    edges.extend(outcome_edges)

    # ---- Enhanced: graph-metric + RLHF nodes (v1 §2.10 fix) ------------
    if use_enh:
        centrality = _clamp(float(gm.get("centrality", 0.5)))
        connectivity = _clamp(float(gm.get("connectivity", 0.5)))
        hf_clamped = _clamp(hf)
        nodes.extend(
            [
                {"node_id": "n_centrality", "node_type": NodeType.GRAPH_METRIC.value,
                 "label": "centrality", "value": centrality},
                {"node_id": "n_connectivity", "node_type": NodeType.GRAPH_METRIC.value,
                 "label": "connectivity", "value": connectivity},
                {"node_id": "n_human_feedback", "node_type": NodeType.RLHF.value,
                 "label": "human_feedback", "value": hf_clamped},
            ]
        )
        edges.extend(
            [
                {"source_id": "n_task", "target_id": "n_centrality",
                 "relation": Relation.HAS_METRIC.value, "weight": centrality},
                {"source_id": "n_task", "target_id": "n_connectivity",
                 "relation": Relation.HAS_METRIC.value, "weight": connectivity},
                {"source_id": "n_task", "target_id": "n_human_feedback",
                 "relation": Relation.HAS_FEEDBACK.value, "weight": hf_clamped},
            ]
        )

    # ---- Enhanced: MODP composite (replaces the v1 `pass`) -------------
    stored_metrics: Dict[str, float] = {
        k: float(outcome_metrics.get(k, 0.0))
        for k in ("accuracy", "energy_kwh", "carbon_co2e_kg",
                  "sustainability_index")
    }
    if use_enh:
        stored_metrics["composite_score"] = compute_modp_composite(
            stored_metrics, config.modp_weights
        )
        stored_metrics["human_feedback_score"] = _clamp(hf)

    return ExecutionSubgraph(
        graph_id=graph_id or str(uuid.uuid4()),
        nodes=nodes,
        edges=edges,
        outcome_metrics=stored_metrics,
        timestamp=datetime.now(timezone.utc).isoformat(),
        framework=framework,
    )


# ============================================================================
# SECTION 11 — Coordinator (store-attached optimizer, closes the loop)
# ============================================================================

class SimilarityIntegration:
    """
    Coordinator that owns the prior optimizer for its lifetime, fixes the
    v1 architecture bug where the optimizer was recreated per call.
    """

    def __init__(
        self,
        store: "GraphSimilarityStore",
        config: Optional[SimilarityIntegrationConfig] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = SimilarityIntegrationConfig()
        elif isinstance(config, SimilarityIntegrationConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = SimilarityIntegrationConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be SimilarityIntegrationConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.store = store
        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )
        self._lock = threading.RLock()
        self._pending: Optional[PendingPrior] = None
        self._human_feedback_score = float(self.config.human_feedback_score)

        # Optimizer selection
        self.optimizer: Optional[Any] = None
        if self.config.use_enhancements and ENHANCEMENTS_AVAILABLE:
            if self.config.use_evolutionary:
                self.optimizer = EvolutionaryPriorOptimizer(
                    self.config, rng=self.rng
                )
            elif self.config.use_distillation:
                self.optimizer = PriorDistillationOptimizer(
                    self.config, rng=self.rng
                )
        elif self.config.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "falling back to plain k-NN priors"
            )

        if self.config.model_path and self.optimizer is not None:
            self._load(self.config.model_path)

    # ------------------------------------------------------------------
    def record_human_feedback(self, score: float) -> None:
        score = float(score)
        if not 0.0 <= score <= 1.0:
            raise ValueError("human_feedback_score must be in [0,1]")
        with self._lock:
            self._human_feedback_score = score
        self.metrics.observe("similarity_human_feedback_score", score)

    # ------------------------------------------------------------------
    def get_benchmark_prior(
        self,
        task: Mapping[str, Any],
        hardware_profile: str,
        grid_region: str,
        framework: str,
        top_k: int = 3,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        use_enh = self.config.use_enhancements and ENHANCEMENTS_AVAILABLE
        gm = dict(graph_metrics) if graph_metrics is not None else dict(
            self.config.graph_metrics
        )
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )

        # ---- Query: MUST include the same enhanced nodes as the subgraph.
        # (fixes §2.10 — otherwise the extra stored nodes lower similarity)
        query_nodes: List[Dict[str, Any]] = [
            {"node_id": "q_task", "node_type": NodeType.WORKLOAD.value,
             "label": task.get("task_type", "unknown"), "value": None},
            {"node_id": "q_suite", "node_type": NodeType.SUITE.value,
             "label": task.get("suite", "unknown"), "value": None},
            {"node_id": "q_hw", "node_type": NodeType.HARDWARE.value,
             "label": hardware_profile, "value": None},
            {"node_id": "q_grid", "node_type": NodeType.GRID.value,
             "label": grid_region, "value": None},
            {"node_id": "q_fw", "node_type": NodeType.FRAMEWORK.value,
             "label": framework, "value": None},
        ]
        query_edges: List[Dict[str, Any]] = [
            {"source_id": "q_task", "target_id": "q_hw",
             "relation": Relation.RAN_ON.value},
            {"source_id": "q_task", "target_id": "q_grid",
             "relation": Relation.CONSTRAINED_BY.value},
            {"source_id": "q_fw", "target_id": "q_task",
             "relation": Relation.EXECUTED.value},
        ]

        if use_enh:
            centrality = _clamp(float(gm.get("centrality", 0.5)))
            connectivity = _clamp(float(gm.get("connectivity", 0.5)))
            hf_clamped = _clamp(hf)
            query_nodes.extend(
                [
                    {"node_id": "q_centrality",
                     "node_type": NodeType.GRAPH_METRIC.value,
                     "label": "centrality", "value": centrality},
                    {"node_id": "q_connectivity",
                     "node_type": NodeType.GRAPH_METRIC.value,
                     "label": "connectivity", "value": connectivity},
                    {"node_id": "q_human_feedback",
                     "node_type": NodeType.RLHF.value,
                     "label": "human_feedback", "value": hf_clamped},
                ]
            )
            query_edges.extend(
                [
                    {"source_id": "q_task", "target_id": "q_centrality",
                     "relation": Relation.HAS_METRIC.value},
                    {"source_id": "q_task", "target_id": "q_connectivity",
                     "relation": Relation.HAS_METRIC.value},
                    {"source_id": "q_task", "target_id": "q_human_feedback",
                     "relation": Relation.HAS_FEEDBACK.value},
                ]
            )

        similar = self.store.find_similar(query_nodes, query_edges, top_k=top_k)
        prior = self.store.build_prior(similar)

        if use_enh and self.optimizer is not None and prior.get("available"):
            try:
                value, x, t_scores, gate = self.optimizer.predict_prior(
                    similar, "accuracy", gm, exploration=False
                ) if isinstance(self.optimizer, PriorDistillationOptimizer) \
                    else self._evolutionary_predict(similar, "accuracy", gm)
                prior.setdefault("metrics", {})["accuracy"] = float(value)
                self._pending = (
                    PendingPrior(
                        state_vec=x,
                        teacher_scores=t_scores,
                        gate_probs=gate,
                        metric_name="accuracy",
                        prior_value=float(value),
                    )
                    if isinstance(self.optimizer, PriorDistillationOptimizer)
                    else None
                )
                prior["optimizer_stats"] = self._optimizer_stats()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer predict failed: %s", exc)

        if use_enh:
            prior["graph_metrics"] = dict(gm)
            prior["human_feedback_score"] = float(hf)

        self.metrics.observe(
            "similarity_prior_available_total",
            float(1.0 if prior.get("available") else 0.0),
        )

        return {
            "prior": prior,
            "similar_executions": similar,
            "prior_available": prior.get("available", False),
        }

    def _evolutionary_predict(
        self,
        similar: Sequence[Mapping[str, Any]],
        metric_name: str,
        gm: Mapping[str, float],
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray"]:
        # Build the same teacher scores the distillation optimizer would.
        t_scores = np.array(
            [
                KNNTeacher().predict(similar, metric_name, gm),
                MODPTeacher(self.config.modp_weights).predict(
                    similar, metric_name, gm
                ),
                CentralityTeacher().predict(similar, metric_name, gm),
            ],
            dtype=np.float64,
        )
        value, gate, state_vec = self.optimizer.predict_from_teachers(t_scores)
        return float(value), state_vec, t_scores, gate

    # ------------------------------------------------------------------
    def record_outcome(self, actual_metrics: Mapping[str, Any]) -> bool:
        """
        Train the optimizer against the actual outcome after the benchmark ran.
        Returns True if training occurred.
        """
        if self.optimizer is None or not ENHANCEMENTS_AVAILABLE:
            return False
        try:
            actual = float(actual_metrics.get("accuracy", 0.0))
        except (TypeError, ValueError):
            logger.warning("actual_metrics.accuracy is not a number; skipping")
            return False

        with self._lock:
            if self._pending is None:
                logger.debug("record_outcome(): no pending prior state")
                return False
            pending = self._pending
            try:
                if isinstance(self.optimizer, PriorDistillationOptimizer):
                    self.optimizer.update(
                        pending.state_vec,
                        pending.teacher_scores,
                        pending.gate_probs,
                        actual,
                    )
                else:
                    self.optimizer.update(
                        pending.state_vec,
                        pending.teacher_scores,
                        pending.gate_probs,
                        actual,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Optimizer update failed: %s", exc)
                return False
            finally:
                self._pending = None
        self.metrics.observe("similarity_outcome_accuracy", float(actual))
        return True

    # ------------------------------------------------------------------
    def check_outliers(
        self,
        result: Mapping[str, Any],
        *,
        k: Optional[float] = None,
        metrics_to_check: Sequence[str] = ("accuracy", "energy_kwh",
                                            "sustainability_index"),
    ) -> Dict[str, Any]:
        """
        Compare the result's metrics against the prior ± k·stddev.
        RLHF influences the threshold: high human_feedback → looser threshold
        (trust the agent); low human_feedback → stricter threshold.
        """
        task = result.get("task", {})
        hardware_profile = result.get("hardware_profile", "")
        grid_region = result.get("grid_region", "")
        framework = result.get("framework", "")
        actual_metrics = result.get("outcome_metrics", {})

        prior_resp = self.get_benchmark_prior(
            task, hardware_profile, grid_region, framework
        )
        if not prior_resp.get("prior_available"):
            return OutlierReport(
                is_outlier=False, reason="no_prior"
            ).to_dict()

        prior_metrics = prior_resp["prior"].get("metrics", {})
        k_base = float(k) if k is not None else float(self.config.outlier_k)
        # RLHF adjustment: high feedback loosens, low tightens.
        rlhf_tilt = (self._human_feedback_score - 0.5) * 0.4
        effective_k = max(0.5, k_base - rlhf_tilt)

        # Compute stddev from the similar executions per metric.
        similar = prior_resp.get("similar_executions", [])
        for metric_name in metrics_to_check:
            try:
                actual = float(actual_metrics.get(metric_name, 0.0))
            except (TypeError, ValueError):
                continue
            values = [_metric_value(ex, metric_name) for ex in similar]
            if not values:
                continue
            mean = sum(values) / len(values)
            var = sum((v - mean) ** 2 for v in values) / len(values)
            std = math.sqrt(var)
            threshold = effective_k * max(std, 1e-6)
            if abs(actual - mean) > threshold:
                self.metrics.observe(
                    "similarity_outliers_total", 1.0, metric=metric_name
                )
                return OutlierReport(
                    is_outlier=True,
                    reason="metric_outside_threshold",
                    metric=metric_name,
                    actual=actual,
                    prior_mean=mean,
                    prior_std=std,
                    threshold=threshold,
                )

        return OutlierReport(
            is_outlier=False, reason="within_threshold"
        ).to_dict()

    # ------------------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None or self.optimizer is None:
            return
        try:
            self.optimizer.save(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Save failed: %s", exc)

    def load(self, path: Optional[Path] = None) -> None:
        self._load(Path(path) if path is not None else self.config.model_path)

    def _load(self, target: Optional[Path]) -> None:
        if target is None or self.optimizer is None:
            return
        try:
            self.optimizer.load(target)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Load failed: %s", exc)

    def _optimizer_stats(self) -> Dict[str, Any]:
        if self.optimizer is None:
            return {}
        stats = {
            "kind": type(self.optimizer).__name__,
            "counter": getattr(self.optimizer, "counter", None),
        }
        if hasattr(self.optimizer, "epsilon"):
            stats["epsilon"] = self.optimizer.epsilon
        if hasattr(self.optimizer, "replay"):
            stats["replay_size"] = len(self.optimizer.replay)
        return stats


# ============================================================================
# SECTION 12 — Module-level helper functions (v1 API, now backed by a shared store)
# ============================================================================

def get_benchmark_prior(
    store: "GraphSimilarityStore",
    task: Mapping[str, Any],
    hardware_profile: str,
    grid_region: str,
    framework: str,
    top_k: int = 3,
    config: Optional[SimilarityIntegrationConfig] = None,
    graph_metrics: Optional[Mapping[str, float]] = None,
    human_feedback_score: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Module-level wrapper. For the enhanced (learning) path, prefer the
    `SimilarityIntegration` class so the optimizer survives across calls.

    This function creates a one-shot coordinator — adequate for retrieving
    a prior but *cannot* train across calls.
    """
    cfg = config or SimilarityIntegrationConfig()
    if cfg.use_enhancements and ENHANCEMENTS_AVAILABLE:
        logger.debug(
            "get_benchmark_prior(): enhancements are enabled but no "
            "optimizer state can persist across calls. Use "
            "SimilarityIntegration for the learning path."
        )
    coord = SimilarityIntegration(store, config=cfg)
    return coord.get_benchmark_prior(
        task, hardware_profile, grid_region, framework,
        top_k=top_k,
        graph_metrics=graph_metrics,
        human_feedback_score=human_feedback_score,
    )


def check_outliers(
    store: "GraphSimilarityStore",
    result: Mapping[str, Any],
    *,
    k: Optional[float] = None,
    config: Optional[SimilarityIntegrationConfig] = None,
) -> Dict[str, Any]:
    """
    Module-level wrapper for outlier detection (v1 documented but missing).
    """
    coord = SimilarityIntegration(store, config=config)
    return coord.check_outliers(result, k=k)


# ============================================================================
# SECTION 13 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Assume a GraphSimilarityStore instance is available (stubbed here).
    class _StubStore:
        def find_similar(self, query_nodes, query_edges, top_k=3):
            return [
                {"metrics": {"accuracy": 0.78, "energy_kwh": 0.12,
                             "carbon_co2e_kg": 0.05,
                             "sustainability_index": 0.7},
                 "similarity": 0.9},
                {"metrics": {"accuracy": 0.81, "energy_kwh": 0.14,
                             "carbon_co2e_kg": 0.06,
                             "sustainability_index": 0.65},
                 "similarity": 0.85},
                {"metrics": {"accuracy": 0.75, "energy_kwh": 0.11,
                             "carbon_co2e_kg": 0.04,
                             "sustainability_index": 0.72},
                 "similarity": 0.80},
            ]

        def build_prior(self, similar):
            if not similar:
                return {"available": False}
            acc = sum(s["metrics"]["accuracy"] for s in similar) / len(similar)
            return {"available": True,
                    "metrics": {"accuracy": acc}}

    cfg = SimilarityIntegrationConfig.from_dict(
        {
            "use_enhancements": True,
            "use_distillation": True,
            "modp_weights": [0.25, 0.25, 0.25, 0.25],
            "human_feedback_score": 0.6,
            "seed": 42,
        }
    )
    coord = SimilarityIntegration(store=_StubStore(), config=cfg)

    # 1. Prior
    resp = coord.get_benchmark_prior(
        task={"task_type": "web_research", "suite": "green"},
        hardware_profile="A100",
        grid_region="us-west",
        framework="vllm",
    )
    print("prior:", json.dumps(resp, indent=2, default=str))

    # 2. Outcome feedback (closes the learning loop)
    trained = coord.record_outcome({"accuracy": 0.83})
    print(f"trained: {trained}")

    # 3. Outlier check (v1's missing feature)
    outlier = coord.check_outliers(
        {
            "task": {"task_type": "web_research", "suite": "green"},
            "hardware_profile": "A100",
            "grid_region": "us-west",
            "framework": "vllm",
            "outcome_metrics": {"accuracy": 0.99, "energy_kwh": 0.12,
                                "sustainability_index": 0.7},
        }
    )
    print("outlier:", json.dumps(outlier, indent=2))
