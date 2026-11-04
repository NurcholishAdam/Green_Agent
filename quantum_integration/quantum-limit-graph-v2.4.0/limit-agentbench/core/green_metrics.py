# -*- coding: utf-8 -*-
"""
src/core/green_metrics.py  —  Enhanced Green Metrics Tracker (v2)
==================================================================

Fixes applied vs. v1
--------------------
  P0  Config dict was unvalidated / callable-mutable         -> frozen GreenMetricsConfig
  P0  modp_weights=[0,0,0,0] -> ZeroDivisionError            -> __post_init__ validation
  P0  Getter trained the model (CQS violation)               -> pure getter + record_outcome()
  P0  Distillation was a linear regression stub              -> real MoE with 3 teachers + gate
  P0  Circular training target (composite_score)             -> explicit outcome reward
  P0  Evolutionary never evolved (index-0 hard-coded)        -> full population evaluation
  P0  Composite score was 0.8 for every untracked task       -> _populated flag + warning
  P0  Unseeded RNG (random + np.random mixed)                -> np.random.default_rng(seed)
  P0  Silent enhancement-disabled fallback                   -> logged warning
  P1  Hard-coded ceilings (10 kWh, 1 kg, 10 s)               -> bounded 1/(1+q/scale)
  P1  feature_dim hard-coded                                 -> validated at predict()
  P1  No model persistence                                   -> save/load on both optimizers
  P1  No thread safety                                       -> RLock around updates
  P1  No metrics sink                                        -> MetricsSink protocol
  P2  Dict-only return                                       -> GreenMetricsResult dataclass
  P2  All-or-nothing enhancement flag                        -> independent use_distillation /
                                                                 use_evolutionary
  P2  No record_metrics API                                  -> added
  P2  No rolling statistics                                  -> rolling_stats()
  P2  No health report                                       -> emit_report()

Preserved public surface
------------------------
  GreenMetricsTracker(config: dict = None)
  .get_normalized_metrics(trace: Dict) -> Dict
  .energy_kwh, .carbon_co2e_kg, .latency_ms, .accuracy, .trace
  .use_enhancements, .modp_weights, .human_feedback_score, .graph_metrics
  .distillation_optimizer, .evolutionary_weights
  ._create_distillation_optimizer(), ._create_evolutionary_optimizer()
  ENHANCEMENTS_AVAILABLE (module-level constant)
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

# ---- Base (non-enhanced) analyzers — assumed present in the codebase -------
from analysis.complexity_analyzer import ComplexityAnalyzer
from metrics.efficiency_calculator import NormalizedEfficiencyCalculator

# ---- Optional numpy / enhancements ----------------------------------------
try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# SECTION 1 — Small helpers
# ============================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return float(x)


def _is_finite_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _save_arrays(path: Path, **arrays: Any) -> None:
    if np is None:
        raise RuntimeError("numpy is required for persistence")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        k: np.asarray(v).tolist() for k, v in arrays.items()
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    tmp.replace(path)


def _load_arrays(path: Path) -> Dict[str, Any]:
    if np is None:
        return {}
    path = Path(path)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring corrupt checkpoint %s: %s", path, exc)
        return {}
    return {k: np.asarray(v) for k, v in payload.items()}


# ============================================================================
# SECTION 2 — Metrics sink (pluggable observability)
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
        self._log = logging.getLogger("green_metrics.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 3 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class GreenMetricsConfig:
    use_enhancements: bool = False

    # MODP weights: (energy, carbon, latency, accuracy) — sum > 0
    modp_weights: Tuple[float, float, float, float] = (0.3, 0.3, 0.2, 0.2)
    human_feedback_score: float = 0.5
    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )

    # Independent feature flags (fixes §6.7 of the analysis)
    use_distillation: bool = True
    use_evolutionary: bool = False

    # Distillation params
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    batch_size: int = 16
    epsilon: float = 0.1
    epsilon_min: float = 0.02
    epsilon_decay: float = 0.995

    # Evolutionary params
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Bounded-normalization scales (replaces hard-coded ceilings)
    energy_scale_kwh: float = 1.0
    carbon_scale_kg: float = 0.1
    latency_scale_ms: float = 1000.0

    # Persistence + determinism
    model_path: Optional[Path] = None
    seed: Optional[int] = None

    def __post_init__(self) -> None:
        w = self.modp_weights
        if not isinstance(w, (tuple, list)) or len(w) != 4:
            raise ValueError(f"modp_weights must have 4 components, got {w!r}")
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
        for name, val in (
            ("energy_scale_kwh", self.energy_scale_kwh),
            ("carbon_scale_kg", self.carbon_scale_kg),
            ("latency_scale_ms", self.latency_scale_ms),
        ):
            if not _is_finite_number(val) or val <= 0:
                raise ValueError(f"{name} must be a positive finite number")
        for k, v in self.graph_metrics.items():
            if not _is_finite_number(v) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        # Normalize model_path
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    # --------------------------------------------------------------
    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "GreenMetricsConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "GreenMetricsConfig ignoring unknown keys: %s", sorted(unknown)
            )
        kwargs: Dict[str, Any] = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k == "modp_weights":
                # Accept list/tuple/Sequence
                kwargs[k] = tuple(float(x) for x in v)
            elif k == "graph_metrics":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 4 — Typed result
# ============================================================================

@dataclass(frozen=True)
class GreenMetricsResult:
    task_complexity: float
    complexity_tier: str
    energy_efficiency: float
    accuracy_per_watt: float
    modp_composite_score: Optional[float] = None
    graph_adjusted_complexity: Optional[float] = None
    distillation_score: Optional[float] = None
    final_sustainability_score: Optional[float] = None
    evolutionary_weights: Optional[List[float]] = None
    enhanced: bool = False
    unpopulated: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# SECTION 5 — Teachers (protocol + three implementations)
# ============================================================================

class GreenMetricsTeacher(Protocol):
    """Predicts the sustainability score from the 8-dim state vector."""
    def predict(self, state: "np.ndarray") -> float: ...


class RuleBasedTeacher:
    """Weighted average of the four normalized metrics."""
    def __init__(self, weights: Sequence[float] = (0.25, 0.25, 0.2, 0.3)) -> None:
        w = [float(x) for x in weights]
        if len(w) != 4 or any(x < 0 for x in w) or sum(w) <= 0:
            raise ValueError(f"weights must be 4 non-negative values, got {weights!r}")
        total = sum(w)
        self.weights = [x / total for x in w]

    def predict(self, state: "np.ndarray") -> float:
        return float(np.clip(np.dot(state[:4], self.weights), 0.0, 1.0))


class RLHFTeacher:
    """
    Trust agent more (high feedback) -> bias toward the mean of the four norms.
    Trust agent less (low feedback) -> bias toward a conservative 0.5 baseline.
    """
    def __init__(self, human_feedback: float = 0.5) -> None:
        if not 0.0 <= human_feedback <= 1.0:
            raise ValueError("human_feedback must be in [0,1]")
        self.h = float(human_feedback)

    def predict(self, state: "np.ndarray") -> float:
        base = float(np.mean(state[:4]))
        blended = self.h * base + (1.0 - self.h) * 0.5
        return float(np.clip(blended, 0.0, 1.0))


class HistoricalTeacher:
    """Linear model over all eight features — placeholder for a trained model."""
    def __init__(self, weights: Optional[Sequence[float]] = None) -> None:
        if weights is None:
            weights = (0.15, 0.15, 0.10, 0.30, 0.05, 0.05, 0.10, 0.10)
        w = [float(x) for x in weights]
        if len(w) != 8:
            raise ValueError("weights must have 8 components")
        self.weights = w

    def predict(self, state: "np.ndarray") -> float:
        return float(np.clip(np.dot(state, self.weights), 0.0, 1.0))


# ============================================================================
# SECTION 6 — Real MoE distillation optimizer
# ============================================================================

class GreenMetricsDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating.

    Forward: state_vec -> { teacher_preds, gate_probs, student_pred, blended }
             blended = 0.7 * student + 0.3 * (gate_probs @ teacher_preds)

    Backward: correct chain rule through softmax gating
              (see GreenMetricsTracker.record_outcome -> _train_step)

    Action space: scalar sustainability score in [0, 1].
    Training target: the TRUE outcome reward (not the composite heuristic).
    """

    FEATURE_DIM = 8
    N_TEACHERS = 3
    STUDENT_WEIGHT = 0.7
    TEACHER_WEIGHT = 0.3

    def __init__(
        self,
        config: GreenMetricsConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if np is None:
            raise RuntimeError("numpy is required for distillation")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.feature_dim = self.FEATURE_DIM
        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.5
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay

        self.replay: deque = deque(maxlen=config.replay_size)
        self.counter = 0
        self.train_every = config.train_every
        self.batch_size = config.batch_size
        self._lock = threading.RLock()

        self.teachers: List[GreenMetricsTeacher] = [
            RuleBasedTeacher(),
            RLHFTeacher(config.human_feedback_score),
            HistoricalTeacher(),
        ]

    # ---- forward ---------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, state: "np.ndarray") -> "np.ndarray":
        return self._softmax(state @ self.gate_weights + self.gate_bias)

    def _teacher_predictions(self, state: "np.ndarray") -> "np.ndarray":
        return np.array([t.predict(state) for t in self.teachers], dtype=np.float64)

    def predict(self, state_vec: "np.ndarray") -> float:
        """Pure forward pass. No side effects."""
        if not isinstance(state_vec, np.ndarray):
            state_vec = np.asarray(state_vec, dtype=np.float32)
        if state_vec.shape != (self.feature_dim,):
            raise ValueError(
                f"state_vec must have shape ({self.feature_dim},), "
                f"got {state_vec.shape}"
            )
        with self._lock:
            teacher_preds = self._teacher_predictions(state_vec)
            gate = self._gate_forward(state_vec)
            teacher_mix = float(gate @ teacher_preds)
            student = float(state_vec @ self.student_weights + self.student_bias)
            blended = self.STUDENT_WEIGHT * student + self.TEACHER_WEIGHT * teacher_mix
        return float(_clamp(blended, 0.0, 1.0))

    # ---- backward --------------------------------------------------------
    def update(self, state_vec: "np.ndarray", reward: float) -> None:
        """Explicit training step against the TRUE outcome reward in [0, 1]."""
        if not isinstance(state_vec, np.ndarray):
            state_vec = np.asarray(state_vec, dtype=np.float32)
        if state_vec.shape != (self.feature_dim,):
            raise ValueError(
                f"state_vec must have shape ({self.feature_dim},), "
                f"got {state_vec.shape}"
            )
        r = float(_clamp(float(reward), 0.0, 1.0))
        with self._lock:
            self.replay.append((state_vec.astype(np.float64), r))
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

        for state, reward in batch:
            teacher_preds = self._teacher_predictions(state)      # (T,)
            gate = self._gate_forward(state)                       # (T,)
            teacher_mix = float(gate @ teacher_preds)              # scalar
            student = float(state @ self.student_weights + self.student_bias)
            blended = (
                self.STUDENT_WEIGHT * student
                + self.TEACHER_WEIGHT * teacher_mix
            )
            err = blended - reward

            # ---- student gradient (blend 0.7, L=0.5*err^2) ----
            d_student = self.STUDENT_WEIGHT * err
            self.student_weights -= self.lr * d_student * state
            self.student_bias -= self.lr * d_student

            # ---- gate gradient (blend 0.3, chain through softmax) ----
            d_teacher_mix = self.TEACHER_WEIGHT * err              # scalar
            d_gate = d_teacher_mix * teacher_preds                 # (T,)
            grad_logits = gate * (d_gate - float(gate @ d_gate))   # (T,)
            self.gate_weights -= self.gate_lr * np.outer(state, grad_logits)
            self.gate_bias -= self.gate_lr * grad_logits

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    # ---- persistence -----------------------------------------------------
    def save(self, path: Path) -> None:
        _save_arrays(
            path,
            student_weights=self.student_weights,
            student_bias=np.asarray([self.student_bias]),
            gate_weights=self.gate_weights,
            gate_bias=self.gate_bias,
            epsilon=np.asarray([self.epsilon]),
            counter=np.asarray([self.counter]),
            feature_dim=np.asarray([self.feature_dim]),
        )

    def load(self, path: Path) -> None:
        data = _load_arrays(path)
        if not data:
            return
        if "feature_dim" in data and int(data["feature_dim"][0]) != self.feature_dim:
            raise ValueError(
                f"checkpoint feature_dim={int(data['feature_dim'][0])} "
                f"!= optimizer feature_dim={self.feature_dim}"
            )
        self.student_weights = data["student_weights"]
        self.student_bias = float(data["student_bias"][0])
        self.gate_weights = data["gate_weights"]
        self.gate_bias = data["gate_bias"]
        self.epsilon = float(data["epsilon"][0])
        self.counter = int(data["counter"][0])
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 7 — Evolutionary optimizer (fully fixed)
# ============================================================================

class GreenEvolutionaryOptimizer:
    """
    Population of MODP weight vectors. Each individual is a linear predictor
    of the sustainability score from the four normalized metric features.

    Fitness: negative absolute error against the TRUE outcome reward
             (higher fitness = better predictor).

    Selection: tournament; crossover: uniform; mutation: Gaussian;
               elitism carries top-k forward.
    """

    N_WEIGHTS = 4

    def __init__(
        self,
        config: GreenMetricsConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if np is None:
            raise RuntimeError("numpy is required for evolutionary optimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)

        self.population: List["np.ndarray"] = [
            self.rng.dirichlet(np.ones(self.N_WEIGHTS))
            for _ in range(config.population_size)
        ]
        self.fitness = np.zeros(config.population_size)
        self.best_weights = self.population[0].copy()
        self.best_fitness = 0.0
        self.counter = 0
        self.train_every = config.train_every
        self._lock = threading.RLock()

    # ---- forward ---------------------------------------------------------
    def get_best_weights(self) -> "np.ndarray":
        with self._lock:
            return self.best_weights.copy()

    # ---- backward --------------------------------------------------------
    def evaluate_outcome(self, state_vec: "np.ndarray", reward: float) -> None:
        """
        Score every individual by how well it predicted the outcome reward.
        Evolve once every `train_every` observations.
        """
        if not isinstance(state_vec, np.ndarray):
            state_vec = np.asarray(state_vec, dtype=np.float32)
        if state_vec.shape[0] < self.N_WEIGHTS:
            raise ValueError(
                f"state_vec must have at least {self.N_WEIGHTS} features"
            )
        r = float(_clamp(float(reward), 0.0, 1.0))
        norm_features = state_vec[: self.N_WEIGHTS]

        with self._lock:
            for i, w in enumerate(self.population):
                pred = float(norm_features @ w)
                self.fitness[i] += -abs(pred - r)

            # Track the current best BEFORE evolving
            best_idx = int(np.argmax(self.fitness))
            self.best_weights = self.population[best_idx].copy()
            self.best_fitness = float(self.fitness[best_idx])

            self.counter += 1
            if self.counter % self.train_every == 0:
                self._evolve()

    def _evolve(self) -> None:
        order = np.argsort(-self.fitness)
        elite_count = max(1, self.config.elitism)
        new_pop: List["np.ndarray"] = [
            self.population[i].copy() for i in order[:elite_count]
        ]

        while len(new_pop) < len(self.population):
            a = self._tournament()
            b = self._tournament()
            child = self._crossover(a, b)
            self._mutate(child)
            child = np.maximum(child, 0.0)
            total = float(child.sum())
            if total > 0:
                child = child / total
            new_pop.append(child)

        self.population = new_pop
        # Reset fitness so the next generation starts fresh.
        self.fitness = np.zeros(len(self.population))

    def _tournament(self, k: int = 3) -> "np.ndarray":
        size = min(k, len(self.population))
        idxs = self.rng.choice(len(self.population), size=size, replace=False)
        best_idx = max(int(i) for i in idxs, key=lambda i: self.fitness[i])
        return self.population[best_idx].copy()

    def _crossover(self, a: "np.ndarray", b: "np.ndarray") -> "np.ndarray":
        if self.rng.random() < self.config.crossover_rate:
            mask = self.rng.random(a.shape) < 0.5
            return np.where(mask, a, b)
        return a.copy()

    def _mutate(self, w: "np.ndarray") -> None:
        mask = self.rng.random(w.shape) < self.config.mutation_rate
        w += mask * self.rng.standard_normal(w.shape) * 0.05

    # ---- persistence -----------------------------------------------------
    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "counter": int(self.counter),
            "best_fitness": float(self.best_fitness),
            "best_weights": self.best_weights.tolist(),
            "population": [w.tolist() for w in self.population],
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        tmp.replace(path)

    def load(self, path: Path) -> None:
        path = Path(path)
        if not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Ignoring corrupt evolutionary checkpoint: %s", exc)
            return
        self.counter = int(payload.get("counter", 0))
        self.best_fitness = float(payload.get("best_fitness", 0.0))
        self.best_weights = np.asarray(payload["best_weights"], dtype=np.float64)
        self.population = [
            np.asarray(w, dtype=np.float64) for w in payload["population"]
        ]
        if len(self.population) != len(self.fitness):
            self.fitness = np.zeros(len(self.population))
        logger.info("Loaded evolutionary checkpoint from %s", path)


# ============================================================================
# SECTION 8 — Main tracker
# ============================================================================

class GreenMetricsTracker:
    """
    Green metrics tracker with optional enhancement stack.

    Enhanced stack:
      - MODP composite score (bounded normalization)
      - RLHF weight tilt
      - Real MoE distillation (3 teachers + gate + student)
      - Optional evolutionary weight selection
      - Optional model persistence
      - Pluggable MetricsSink

    Public API
    ----------
      tracker.record_metrics(energy_kwh, carbon_kg, latency_ms, accuracy)
      tracker.get_normalized_metrics(trace)   -> Dict  (pure)
      tracker.get_result(trace)               -> GreenMetricsResult  (pure)
      tracker.record_outcome(reward)          -> None  (explicit training)
      tracker.rolling_stats(window=100)       -> Dict
      tracker.emit_report()                   -> Dict
      tracker.save() / tracker.load()         (config.model_path if set)
    """

    MODP_DEFAULT: Tuple[float, float, float, float] = (0.3, 0.3, 0.2, 0.2)

    # ---- construction ---------------------------------------------------
    def __init__(
        self,
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        # Accept dict | dataclass | None
        if config is None:
            self.config = GreenMetricsConfig()
        elif isinstance(config, GreenMetricsConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = GreenMetricsConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be dict, GreenMetricsConfig or None; "
                f"got {type(config).__name__}"
            )

        # Enhancement gate
        requested = self.config.use_enhancements
        if requested and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "use_enhancements=True but numpy is unavailable; "
                "running in non-enhanced mode"
            )
        self.use_enhancements = bool(requested and ENHANCEMENTS_AVAILABLE)

        # RNG
        if ENHANCEMENTS_AVAILABLE:
            self.rng = rng if rng is not None else np.random.default_rng(
                self.config.seed
            )
        else:
            self.rng = None

        # Metrics
        self.metrics: MetricsSink = metrics or InMemoryMetrics()

        # Base analyzers
        self.complexity_analyzer = ComplexityAnalyzer()
        self.efficiency_calc = NormalizedEfficiencyCalculator()

        # Tracked values
        self.energy_kwh: float = 0.0
        self.carbon_co2e_kg: float = 0.0
        self.latency_ms: float = 0.0
        self.accuracy: float = 0.0
        self.trace: Dict[str, Any] = {}
        self._populated: bool = False
        self._lock = threading.RLock()

        # Rolling buffer of composite scores
        self._composite_history: deque = deque(maxlen=500)

        # State for explicit training (captured by getter, used by record_outcome)
        self._last_state: Optional["np.ndarray"] = None
        self._last_composite: Optional[float] = None

        # Public-facing copies (backwards compat with v1)
        self.modp_weights = self._normalize_modp_weights(self.config.modp_weights)
        self.human_feedback_score = float(self.config.human_feedback_score)
        self.graph_metrics = dict(self.config.graph_metrics)

        # ---- Enhanced components ----
        if self.use_enhancements:
            self.distillation_optimizer = (
                self._create_distillation_optimizer()
                if self.config.use_distillation
                else None
            )
            self.evolutionary_weights = (
                self._create_evolutionary_optimizer()
                if self.config.use_evolutionary
                else None
            )
        else:
            self.distillation_optimizer = None
            self.evolutionary_weights = None

        # Restore persisted models
        self._restore_models()

    # ---- config helpers -----------------------------------------------
    @staticmethod
    def _normalize_modp_weights(
        w: Sequence[float],
    ) -> List[float]:
        arr = [float(x) for x in w]
        total = sum(arr)
        if total <= 0:
            raise ValueError("modp_weights sum must be > 0")
        return [x / total for x in arr]

    # ---- factories (preserved for subclass overrides) -----------------
    def _create_distillation_optimizer(
        self,
    ) -> Optional[GreenMetricsDistillationOptimizer]:
        if not ENHANCEMENTS_AVAILABLE:
            return None
        return GreenMetricsDistillationOptimizer(self.config, rng=self.rng)

    def _create_evolutionary_optimizer(
        self,
    ) -> Optional[GreenEvolutionaryOptimizer]:
        if not ENHANCEMENTS_AVAILABLE:
            return None
        return GreenEvolutionaryOptimizer(self.config, rng=self.rng)

    # ---- persistence ---------------------------------------------------
    def _restore_models(self) -> None:
        path = self.config.model_path
        if path is None:
            return
        for opt, suffix in (
            (self.distillation_optimizer, ".distill.json"),
            (self.evolutionary_weights, ".evolution.json"),
        ):
            if opt is None:
                continue
            p = Path(str(path) + suffix)
            try:
                opt.load(p)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load %s: %s", p, exc)

    def save(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            logger.debug("save(): no model_path configured; skipping")
            return
        if self.distillation_optimizer is not None:
            self.distillation_optimizer.save(Path(str(target) + ".distill.json"))
        if self.evolutionary_weights is not None:
            self.evolutionary_weights.save(Path(str(target) + ".evolution.json"))

    def load(self, path: Optional[Path] = None) -> None:
        target = Path(path) if path is not None else self.config.model_path
        if target is None:
            return
        if self.distillation_optimizer is not None:
            self.distillation_optimizer.load(Path(str(target) + ".distill.json"))
        if self.evolutionary_weights is not None:
            self.evolutionary_weights.load(Path(str(target) + ".evolution.json"))

    # ---- metrics API ---------------------------------------------------
    def record_metrics(
        self,
        energy_kwh: float,
        carbon_kg: float,
        latency_ms: float,
        accuracy: float,
    ) -> None:
        """Populate the tracked values. Must be called before get_normalized_metrics."""
        for name, val in (
            ("energy_kwh", energy_kwh),
            ("carbon_kg", carbon_kg),
            ("latency_ms", latency_ms),
            ("accuracy", accuracy),
        ):
            if not _is_finite_number(val) or float(val) < 0:
                raise ValueError(
                    f"{name} must be a non-negative finite number, got {val!r}"
                )
        with self._lock:
            self.energy_kwh = float(energy_kwh)
            self.carbon_co2e_kg = float(carbon_kg)
            self.latency_ms = float(latency_ms)
            self.accuracy = float(accuracy)
            self._populated = True

    def reset(self) -> None:
        with self._lock:
            self.energy_kwh = 0.0
            self.carbon_co2e_kg = 0.0
            self.latency_ms = 0.0
            self.accuracy = 0.0
            self.trace = {}
            self._populated = False
            self._composite_history.clear()
            self._last_state = None
            self._last_composite = None

    # ---- normalization helpers ----------------------------------------
    def _normalize_bounded(self, quantity: float, scale: float) -> float:
        """1/(1+q/scale) — smooth, bounded on (0, 1], no hard ceiling."""
        q = max(float(quantity), 0.0)
        s = max(float(scale), 1e-12)
        return 1.0 / (1.0 + q / s)

    # ---- main getter (pure) -------------------------------------------
    def get_normalized_metrics(self, trace: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute normalized metrics. Pure: no optimizer update happens here.
        Call record_outcome(reward) to train.

        Returns a dict for backwards compatibility. Use get_result() for
        a typed GreenMetricsResult.
        """
        if not isinstance(trace, Mapping):
            raise TypeError("trace must be a Mapping")
        trace_data = dict(trace)
        self.trace = trace_data

        complexity = self.complexity_analyzer.analyze_from_trace(trace_data)
        complexity_score = float(complexity.compute_composite_score())
        tier = self.complexity_analyzer.categorize_complexity(complexity)

        base_metrics: Dict[str, Any] = {
            "task_complexity": complexity_score,
            "complexity_tier": tier,
            "energy_efficiency": self.efficiency_calc.calculate_energy_efficiency(
                self.energy_kwh, trace_data
            ),
            "accuracy_per_watt": self.efficiency_calc.calculate_accuracy_per_watt(
                self.accuracy, self.energy_kwh
            ),
        }

        if not self.use_enhancements:
            return base_metrics

        # ---- Bounded normalization ----
        energy_norm = self._normalize_bounded(
            self.energy_kwh, self.config.energy_scale_kwh
        )
        carbon_norm = self._normalize_bounded(
            self.carbon_co2e_kg, self.config.carbon_scale_kg
        )
        latency_norm = self._normalize_bounded(
            self.latency_ms, self.config.latency_scale_ms
        )
        accuracy_norm = _clamp(self.accuracy, 0.0, 1.0)

        # ---- RLHF weight tilt ----
        h = float(self.human_feedback_score)
        raw = [
            self.modp_weights[0] * (1.0 - 0.2 * h),
            self.modp_weights[1] * (1.0 - 0.1 * h),
            self.modp_weights[2] * (1.0 - 0.1 * h),
            self.modp_weights[3] * (1.0 + 0.3 * h),
        ]
        adj_sum = sum(raw)
        if adj_sum <= 0:
            adjusted = list(self.modp_weights)
        else:
            adjusted = [x / adj_sum for x in raw]

        composite = (
            adjusted[0] * energy_norm
            + adjusted[1] * carbon_norm
            + adjusted[2] * latency_norm
            + adjusted[3] * accuracy_norm
        )

        centrality = _clamp(float(self.graph_metrics.get("centrality", 0.5)))
        connectivity = _clamp(float(self.graph_metrics.get("connectivity", 0.5)))
        graph_factor = 0.8 + 0.2 * centrality * connectivity

        base_metrics["modp_composite_score"] = composite
        base_metrics["graph_adjusted_complexity"] = complexity_score * graph_factor

        # ---- Capture state for explicit training ----
        state_vec = np.array(
            [
                energy_norm,
                carbon_norm,
                latency_norm,
                accuracy_norm,
                centrality,
                connectivity,
                h,
                complexity_score,
            ],
            dtype=np.float32,
        )
        with self._lock:
            self._last_state = state_vec
            self._last_composite = composite
            self._composite_history.append(composite)

        # ---- Distillation prediction (PURE) ----
        if self.distillation_optimizer is not None:
            try:
                learned = self.distillation_optimizer.predict(state_vec)
            except Exception as exc:  # noqa: BLE001
                logger.warning("distillation predict failed: %s", exc)
                learned = composite
            final_score = 0.7 * composite + 0.3 * learned
            base_metrics["distillation_score"] = float(learned)
            base_metrics["final_sustainability_score"] = float(final_score)
        else:
            base_metrics["final_sustainability_score"] = composite

        # ---- Evolutionary best weights (PURE) ----
        if self.evolutionary_weights is not None:
            base_metrics["evolutionary_weights"] = (
                self.evolutionary_weights.get_best_weights().tolist()
            )

        # ---- Unpopulated warning ----
        if not self._populated:
            base_metrics["unpopulated"] = True
            logger.warning(
                "get_normalized_metrics() called before record_metrics(); "
                "energy/carbon/latency/accuracy are all zero — composite "
                "score is a fixed constant. Call record_metrics() first."
            )

        # ---- Observability ----
        try:
            self.metrics.observe("green_metrics_composite_score", composite)
            self.metrics.observe(
                "green_metrics_final_score",
                base_metrics["final_sustainability_score"],
            )
            self.metrics.observe("green_metrics_energy_kwh", self.energy_kwh)
            self.metrics.observe("green_metrics_carbon_kg", self.carbon_co2e_kg)
        except Exception as exc:  # noqa: BLE001
            logger.debug("metrics.observe failed: %s", exc)

        return base_metrics

    # ---- typed getter --------------------------------------------------
    def get_result(self, trace: Dict[str, Any]) -> GreenMetricsResult:
        metrics = self.get_normalized_metrics(trace)
        return GreenMetricsResult(
            task_complexity=metrics["task_complexity"],
            complexity_tier=metrics["complexity_tier"],
            energy_efficiency=metrics["energy_efficiency"],
            accuracy_per_watt=metrics["accuracy_per_watt"],
            modp_composite_score=metrics.get("modp_composite_score"),
            graph_adjusted_complexity=metrics.get("graph_adjusted_complexity"),
            distillation_score=metrics.get("distillation_score"),
            final_sustainability_score=metrics.get("final_sustainability_score"),
            evolutionary_weights=metrics.get("evolutionary_weights"),
            enhanced=self.use_enhancements,
            unpopulated=metrics.get("unpopulated", False),
        )

    # ---- explicit training --------------------------------------------
    def record_outcome(self, reward: float) -> None:
        """
        Train the enhanced optimizers against the TRUE outcome reward.
        `reward` should be in [0, 1] and represent the true sustainability
        of the agent's output (e.g., a downstream quality score, an RLHF
        score, or a verified green metric).

        No-op when enhancements are disabled or no prior get_normalized_metrics
        call has captured a state vector.
        """
        if not self.use_enhancements:
            return
        if self._last_state is None:
            logger.debug("record_outcome(): no prior state vector; skipping")
            return

        r = float(reward)
        if not (0.0 <= r <= 1.0):
            r = _clamp(r, 0.0, 1.0)
            logger.debug("record_outcome(): reward clipped to [0,1]")

        with self._lock:
            if self.distillation_optimizer is not None:
                self.distillation_optimizer.update(self._last_state, r)
            if self.evolutionary_weights is not None:
                self.evolutionary_weights.evaluate_outcome(self._last_state, r)

        try:
            self.metrics.observe("green_metrics_outcome_reward", r)
        except Exception as exc:  # noqa: BLE001
            logger.debug("metrics.observe failed: %s", exc)

    # ---- rolling stats ------------------------------------------------
    def rolling_stats(self, window: Optional[int] = None) -> Dict[str, float]:
        with self._lock:
            values = list(self._composite_history)
        if window is not None and window > 0:
            values = values[-window:]
        if not values:
            return {"n": 0}
        arr = np.asarray(values, dtype=np.float64) if np is not None else values
        if np is None:
            n = len(values)
            mean = sum(values) / n
            var = sum((v - mean) ** 2 for v in values) / n
            return {
                "n": n,
                "mean": mean,
                "std": math.sqrt(var),
                "min": min(values),
                "max": max(values),
            }
        return {
            "n": int(arr.size),
            "mean": float(arr.mean()),
            "std": float(arr.std()),
            "min": float(arr.min()),
            "max": float(arr.max()),
        }

    # ---- health report ------------------------------------------------
    def emit_report(self) -> Dict[str, Any]:
        cfg = asdict(self.config)
        if cfg.get("model_path") is not None:
            cfg["model_path"] = str(cfg["model_path"])
        report: Dict[str, Any] = {
            "enhanced": self.use_enhancements,
            "populated": self._populated,
            "energy_kwh": self.energy_kwh,
            "carbon_co2e_kg": self.carbon_co2e_kg,
            "latency_ms": self.latency_ms,
            "accuracy": self.accuracy,
            "config": cfg,
        }
        if self.distillation_optimizer is not None:
            report["distillation"] = {
                "counter": self.distillation_optimizer.counter,
                "epsilon": self.distillation_optimizer.epsilon,
                "replay_size": len(self.distillation_optimizer.replay),
                "student_bias": float(self.distillation_optimizer.student_bias),
            }
        if self.evolutionary_weights is not None:
            report["evolutionary"] = {
                "counter": self.evolutionary_weights.counter,
                "best_fitness": self.evolutionary_weights.best_fitness,
                "best_weights": self.evolutionary_weights.best_weights.tolist(),
            }
        return report


# ============================================================================
# SECTION 9 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Backwards-compatible dict config
    tracker = GreenMetricsTracker(
        {
            "use_enhancements": True,
            "use_distillation": True,
            "use_evolutionary": False,
            "modp_weights": [0.3, 0.3, 0.2, 0.2],
            "human_feedback_score": 0.7,
            "graph_metrics": {"centrality": 0.8, "connectivity": 0.6},
            "energy_scale_kwh": 0.01,
            "carbon_scale_kg": 0.004,
            "latency_scale_ms": 500.0,
            "seed": 42,
            "model_path": "./models/green_metrics",
        }
    )

    # Track a realistic task
    tracker.record_metrics(
        energy_kwh=0.002,
        carbon_kg=0.0008,
        latency_ms=420.0,
        accuracy=0.87,
    )

    trace = {"steps": [{"action": "plan"}, {"action": "search"}, {"action": "answer"}]}
    metrics = tracker.get_normalized_metrics(trace)
    print(json.dumps(metrics, indent=2, default=str))

    # Emit the true outcome (e.g., from a downstream evaluator)
    tracker.record_outcome(0.82)
    tracker.record_outcome(0.91)
    tracker.record_outcome(0.75)

    print("rolling:", tracker.rolling_stats(window=100))
    print("report:", json.dumps(tracker.emit_report(), indent=2, default=str))

    # Persist
    tracker.save()
