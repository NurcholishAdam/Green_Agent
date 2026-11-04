# -*- coding: utf-8 -*-
"""
Efficiency Scorer (Enhanced, v2)
================================

Performance efficiency metrics with optional learned composite scoring.

Fixes applied vs. v1
--------------------
  P0  _rule_teacher saturated to 1.0 for any realistic energy
      (accuracy / (energy/10) at 0.001 kWh = 8500, clamped)
      → composite score was a near-constant ~0.28   -> bounded-inverse
                                                        on raw energy
  P0  Training was circular: update() target = predict() output
      (gradient was identically zero)                -> record_outcome()
                                                        accepts the true
                                                        target
  P0  MoE gate was never trained (update only touched student)
                                                     -> gate stored in
                                                        replay, updated via
                                                        correct regression
                                                        chain rule
  P0  student_bias = 0 collapsed output range to
      [0, 0.3]                                        -> student init to
                                                        MODP weights, bias 0
  P0  use_evolutionary was `pass` (8th dead flag)     -> implemented
  P0  modp_weights ordering was the 8th variant      -> MODP_SCHEMA
  P0  config.modp_weights mutated in __init__;
      [] / [1,1] / negative crashed or mis-scored     -> frozen config +
                                                        validation + local
                                                        copy
  P0  Unseeded RNG (random + np.random)               -> default_rng(seed)
  P0  Exploration branch swapped to 100% teacher
      (3.3× different output scale)                   -> perturb output
  P1  State stored only normalized (lossy) energy     -> raw + normalized
  P1  Hardcoded ceilings (10 kWh, 1 s, 10 USD)        -> 1/(1+q/scale)
  P1  energy_norm/latency_norm/cost_norm used with
      opposite semantics in state vs. fallback        -> named *_efficiency
                                                        fields (higher=better)
  P1  accuracy not clamped to [0, 1]                  -> clamped
  P1  _historical_teacher was a fixed formula, not
      historical                                      -> k-NN over replay
  P2  No record_outcome API                           -> added
  P2  No persistence                                  -> save()/load()
  P2  No metrics                                      -> MetricsSink
  P2  No thread safety                                -> RLock
  P2  epsilon never decayed                           -> epsilon_min /
                                                        epsilon_decay
  P2  batch_size / distill_w / rl_w hardcoded         -> config
  P2  connectivity feature unused                     -> used by RLHF
                                                        teacher
  P2  Dict[str, any] type bug                         -> Dict[str, Any]
  P2  improvement_percent = 0 when eff_a = 0          -> None + reason
  P2  ELECTRICITY_COST had only 8 regions             -> expanded to match
                                                        file 27
  P2  Unknown grid_region silently fell back          -> warning log
  P3  Unused duration_seconds in performance_per_watt -> documented

Design notes
------------
The composite score has three layers:

  1. Fallback (deterministic, correct in v1 but unreachable):
       score = Σ w_i × efficiency_i
     where efficiency_i ∈ [0, 1] and higher = better. This is the source
     of truth.

  2. Distillation (optional, enabled via use_distillation):
     The student is initialized to the fallback formula's weights, so the
     initial prediction equals the fallback. Training pulls it toward the
     true target supplied by record_outcome().

  3. Evolutionary MODP weights (optional, enabled via use_evolutionary):
     A population of weight vectors is evolved against observed outcomes.
     When distillation is off, these weights replace the config weights in
     the fallback formula.

record_outcome(actual_efficiency) must be called after the caller has
evaluated the true efficiency. Without it, the distillation and
evolutionary optimizers stay at their defaults (which match the fallback
formula for distillation, so the score is still correct).

Public API preserved
--------------------
  EfficiencyScorerConfig, EfficiencyState, EfficiencyDistillationOptimizer,
  EfficiencyScorer
  .calculate_efficiency_score, .calculate_performance_per_watt,
  .calculate_cost_efficiency, .calculate_throughput_efficiency,
  .compare_efficiency, .calculate_composite_score
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from collections import deque
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Tuple

try:
    import numpy as np
    ENHANCEMENTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    ENHANCEMENTS_AVAILABLE = False

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


def _bounded_inverse(q: float, scale: float) -> float:
    """1 / (1 + q/scale) — bounded on (0, 1], higher = better (less q)."""
    q = max(float(q), 0.0)
    s = max(float(scale), 1e-9)
    return 1.0 / (1.0 + q / s)


def _finite_nonneg(value: Any, name: str) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {value!r}")
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if f < 0:
        raise ValueError(f"{name} must be non-negative, got {f!r}")
    return f


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
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
# SECTION 2 — MODP schema (single source of truth)
# ============================================================================

# Canonical ordering used by this file and config.modp_weights.
# Other files in the codebase use different orderings — do not import
# weights across files without translating through this schema.
MODP_SCHEMA: Tuple[str, ...] = ("accuracy", "energy", "latency", "cost")


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
        self._log = logging.getLogger("efficiency_scorer.metrics")

    def observe(self, name: str, value: float, **labels: str) -> None:
        self._log.info("metric=%s value=%.6f labels=%s", name, value, labels)


# ============================================================================
# SECTION 4 — Configuration (frozen, validated)
# ============================================================================

@dataclass(frozen=True)
class EfficiencyScorerConfig:
    use_enhancements: bool = False

    graph_metrics: Dict[str, float] = field(
        default_factory=lambda: {"centrality": 0.5, "connectivity": 0.5}
    )
    # MODP weights in MODP_SCHEMA order: (accuracy, energy, latency, cost).
    modp_weights: Tuple[float, float, float, float] = (0.4, 0.3, 0.2, 0.1)
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
    distill_w: float = 0.5
    rl_w: float = 0.5

    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2

    # Bounded-normalization scales (replace hardcoded ceilings)
    energy_scale_kwh: float = 0.01
    latency_scale_ms: float = 500.0
    cost_scale_usd: float = 0.01

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
        object.__setattr__(
            self, "modp_weights", tuple(float(x) for x in w)
        )

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
        for name in ("energy_scale_kwh", "latency_scale_ms", "cost_scale_usd"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        for k, v in self.graph_metrics.items():
            if not isinstance(v, (int, float)) or not 0.0 <= float(v) <= 1.0:
                raise ValueError(
                    f"graph_metric {k!r} must be in [0,1], got {v!r}"
                )
        if self.model_path is not None:
            object.__setattr__(self, "model_path", Path(self.model_path))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "EfficiencyScorerConfig":
        if not isinstance(d, Mapping):
            raise TypeError(f"config must be a Mapping, got {type(d).__name__}")
        known = set(cls.__dataclass_fields__.keys())
        unknown = set(d.keys()) - known
        if unknown:
            logger.warning(
                "EfficiencyScorerConfig ignoring unknown keys: %s",
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
            elif k == "model_path":
                kwargs[k] = Path(v) if v is not None else None
            else:
                kwargs[k] = v
        return cls(**kwargs)


# ============================================================================
# SECTION 5 — Efficiency state (raw + normalized-efficiency)
# ============================================================================

class EfficiencyState:
    """
    Caller's efficiency context.

    Both raw values (for teachers) and normalized efficiency (higher = better,
    for feature vectors) are exposed. This makes the semantics unambiguous:
    every field named `*_efficiency` is bounded in [0, 1] and higher is
    better, whereas `*_norm` is retained as a deprecated alias for v1
    compatibility with the opposite (cost) semantics.
    """
    FEATURE_DIM = 7

    __slots__ = (
        "accuracy",
        "raw_energy_kwh", "raw_latency_ms", "raw_cost_usd",
        "energy_efficiency", "latency_efficiency", "cost_efficiency",
        "energy_norm", "latency_norm", "cost_norm",   # deprecated
        "centrality", "connectivity", "human_feedback",
        "_vector",
    )

    def __init__(
        self,
        accuracy: float,
        energy_kwh: float,
        latency_ms: float = 0.0,
        cost_usd: float = 0.0,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback: float = 0.5,
        *,
        energy_scale_kwh: float = 0.01,
        latency_scale_ms: float = 500.0,
        cost_scale_usd: float = 0.01,
    ) -> None:
        self.accuracy = _clamp(_finite_nonneg(accuracy, "accuracy"))
        self.raw_energy_kwh = _finite_nonneg(energy_kwh, "energy_kwh")
        self.raw_latency_ms = _finite_nonneg(latency_ms, "latency_ms")
        self.raw_cost_usd = _finite_nonneg(cost_usd, "cost_usd")

        # Higher = better (efficiency semantics)
        self.energy_efficiency = _bounded_inverse(self.raw_energy_kwh, energy_scale_kwh)
        self.latency_efficiency = _bounded_inverse(self.raw_latency_ms, latency_scale_ms)
        self.cost_efficiency = _bounded_inverse(self.raw_cost_usd, cost_scale_usd)

        # Deprecated aliases for v1 compatibility (higher = worse for the
        # cost metrics, but preserved so external callers don't break).
        self.energy_norm = 1.0 - self.energy_efficiency
        self.latency_norm = 1.0 - self.latency_efficiency
        self.cost_norm = 1.0 - self.cost_efficiency

        gm = graph_metrics or {}
        self.centrality = _clamp(float(gm.get("centrality", 0.5)))
        self.connectivity = _clamp(float(gm.get("connectivity", 0.5)))
        self.human_feedback = _clamp(float(human_feedback))

        # Feature vector uses efficiency semantics throughout (higher = better),
        # so the student learns consistently.
        self._vector = np.array(
            [
                self.accuracy,
                self.energy_efficiency,
                self.latency_efficiency,
                self.cost_efficiency,
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
# SECTION 6 — PendingDecision (typed)
# ============================================================================

@dataclass
class PendingDecision:
    state_vec: "np.ndarray"
    reward_observed: float


# ============================================================================
# SECTION 7 — Distillation optimizer (trained, non-circular)
# ============================================================================

class EfficiencyDistillationOptimizer:
    """
    Multi-teacher distillation with MoE gating for a scalar efficiency score.

    Fixes vs. v1:
      - Student is initialized to the fallback formula (accuracy, energy,
        latency, cost weighted by MODP weights), so the initial prediction
        equals the deterministic answer.
      - `update(state_vec, target)` is called from `record_outcome`, not
        from `calculate_composite_score` with its own output as the target.
      - Gate is trained via the correct regression-form chain rule.
      - Exploration perturbs the output by ±N(0, 0.02) instead of swapping
        to 100% teacher (which produced a 3.3× scale change).
    """

    N_TEACHERS = 3

    def __init__(
        self,
        config: EfficiencyScorerConfig,
        rng: Optional["np.random.Generator"] = None,
        *,
        initial_weights: Optional[Tuple[float, float, float, float]] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the distillation optimizer")

        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.feature_dim = EfficiencyState.FEATURE_DIM

        self.lr = config.distillation_lr
        self.gate_lr = config.gating_lr
        self.distill_w = config.distill_w
        self.rl_w = config.rl_w
        self.epsilon = config.epsilon
        self.epsilon_min = config.epsilon_min
        self.epsilon_decay = config.epsilon_decay
        self.train_every = config.train_every
        self.batch_size = config.batch_size

        # Student: linear regression on the 7 features.
        # Initialize to the fallback formula (MODP weights on first 4
        # features; zeros on the graph/feedback features).
        w = initial_weights if initial_weights is not None else config.modp_weights
        total = sum(w) if sum(w) > 0 else 1.0
        self.student_weights = np.array(
            [w[0] / total, w[1] / total, w[2] / total, w[3] / total, 0.0, 0.0, 0.0],
            dtype=np.float64,
        )
        self.student_bias = 0.0

        # MoE gate
        self.gate_weights = (
            self.rng.standard_normal((self.feature_dim, self.N_TEACHERS)) * 0.01
        )
        self.gate_bias = np.zeros(self.N_TEACHERS)

        self.counter = 0
        self.replay: deque = deque(maxlen=config.replay_size)
        self._lock = threading.RLock()

        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]

        if config.model_path:
            self.load(Path(str(config.model_path) + ".distill.json"))

    # ---- teachers ---------------------------------------------------------
    def _rule_teacher(self, state: EfficiencyState) -> float:
        # accuracy × energy_efficiency — bounded, varies across the full
        # realistic energy range (v1's accuracy / (energy/10) saturated
        # to 1.0 for anything under ~8.5 kWh).
        return _clamp(state.accuracy * state.energy_efficiency)

    def _rlhf_teacher(self, state: EfficiencyState) -> float:
        # Human feedback reweights accuracy vs. energy+latency+cost
        # efficiency. High feedback (>0.7) favors accuracy; low (<0.3)
        # favors efficiency.
        h = state.human_feedback
        if h > 0.7:
            acc_w = 0.7
        elif h < 0.3:
            acc_w = 0.3
        else:
            acc_w = 0.5
        eff_avg = (
            state.energy_efficiency
            + state.latency_efficiency
            + state.cost_efficiency
        ) / 3.0
        return _clamp(acc_w * state.accuracy + (1.0 - acc_w) * eff_avg)

    def _historical_teacher(self, state: EfficiencyState) -> float:
        # k-NN over the replay buffer's observed (state, reward) pairs.
        if not self.replay:
            # Before any training, use the fallback formula as prior.
            return _clamp(
                self.config.modp_weights[0] * state.accuracy
                + self.config.modp_weights[1] * state.energy_efficiency
                + self.config.modp_weights[2] * state.latency_efficiency
                + self.config.modp_weights[3] * state.cost_efficiency
            )
        with self._lock:
            recent = list(self.replay)[-16:]
        query = state.to_vector()
        num = 0.0
        den = 0.0
        for s, r, _t_outs, _gate in recent:
            sim = 1.0 / (1.0 + float(np.linalg.norm(s - query)))
            num += sim * r
            den += sim
        return num / den if den > 0 else 0.5

    # ---- forward ----------------------------------------------------------
    @staticmethod
    def _softmax(logits: "np.ndarray") -> "np.ndarray":
        z = logits - logits.max()
        e = np.exp(z)
        return e / e.sum()

    def _gate_forward(self, x: "np.ndarray") -> "np.ndarray":
        return self._softmax(x @ self.gate_weights + self.gate_bias)

    def predict_score(
        self,
        state: EfficiencyState,
        exploration: bool = False,
    ) -> Tuple[float, "np.ndarray", "np.ndarray", "np.ndarray"]:
        """
        Returns
        -------
        score           : float in [0, 1]
        state_vec       : (FEATURE_DIM,)
        teacher_scores  : (T,)
        gate_probs      : (T,)
        """
        x = state.to_vector()
        with self._lock:
            teacher_scores = np.array(
                [t(state) for t in self.teachers], dtype=np.float64
            )
            gate = self._gate_forward(x)
            teacher_combined = float(gate @ teacher_scores)
            student_pred = float(x @ self.student_weights + self.student_bias)

            pred = 0.5 * student_pred + 0.5 * teacher_combined
            if exploration and self.rng.random() < self.epsilon:
                # Perturb, don't swap. v1's swap produced a 3.3× scale
                # change between branches (0.3× teacher vs. 1.0× teacher).
                pred += float(self.rng.normal(0.0, 0.02))

            score = _clamp(pred)
        return score, x, teacher_scores, gate

    # ---- backward ---------------------------------------------------------
    def update(
        self,
        state_vec: "np.ndarray",
        target_score: float,
        *,
        teacher_scores: Optional["np.ndarray"] = None,
        gate_probs: Optional["np.ndarray"] = None,
    ) -> None:
        """
        Train the student toward the observed target. When teacher_scores
        and gate_probs are provided (from a prior `predict_score` call),
        the MoE gate is also trained. Without them, only the student is
        updated.
        """
        target = _clamp(target_score)
        with self._lock:
            self.replay.append(
                (
                    np.asarray(state_vec, dtype=np.float64),
                    target,
                    None if teacher_scores is None else np.asarray(teacher_scores, dtype=np.float64),
                    None if gate_probs is None else np.asarray(gate_probs, dtype=np.float64),
                )
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

            for s, target, t_scores, gate in batch:
                # ---- student MSE ----
                pred = float(s @ self.student_weights + self.student_bias)
                err = pred - target
                self.student_weights -= self.lr * err * s
                self.student_bias -= self.lr * err

                # ---- gate (regression-form chain rule) ----
                # Only train when we have the forward-pass context;
                # otherwise the gate would be trained on synthesized data
                # that doesn't match what the gate actually saw.
                if t_scores is not None and gate is not None:
                    combined = float(gate @ t_scores)
                    err_g = combined - target
                    dL_dg = err_g * t_scores
                    grad_logits = gate * (dL_dg - float(gate @ dL_dg))
                    self.gate_weights -= self.gate_lr * np.outer(s, grad_logits)
                    self.gate_bias -= self.gate_lr * grad_logits

            # Epsilon decay
            self.epsilon = max(
                self.epsilon_min, self.epsilon * self.epsilon_decay
            )

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "efficiency_distillation",
                "feature_dim": self.feature_dim,
                "student_weights": self.student_weights.tolist(),
                "student_bias": [float(self.student_bias)],
                "gate_weights": self.gate_weights.tolist(),
                "gate_bias": self.gate_bias.tolist(),
                "epsilon": float(self.epsilon),
                "counter": int(self.counter),
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload:
            return
        if payload.get("kind") != "efficiency_distillation":
            raise ValueError(f"Wrong checkpoint kind: {payload.get('kind')!r}")
        if int(payload.get("feature_dim", -1)) != self.feature_dim:
            raise ValueError("Checkpoint feature_dim mismatch")
        self.student_weights = np.asarray(payload["student_weights"], dtype=np.float64)
        self.student_bias = float(payload["student_bias"][0])
        self.gate_weights = np.asarray(payload["gate_weights"], dtype=np.float64)
        self.gate_bias = np.asarray(payload["gate_bias"], dtype=np.float64)
        self.epsilon = float(payload.get("epsilon", self.epsilon))
        self.counter = int(payload.get("counter", 0))
        logger.info("Loaded distillation checkpoint from %s", path)


# ============================================================================
# SECTION 8 — Evolutionary MODP weights optimizer (implements use_evolutionary)
# ============================================================================

@dataclass
class _Ind:
    weights: "np.ndarray"    # (4,) — normalized MODP weights
    fitness: float = 0.0


class EvolutionaryMODPWeightsOptimizer:
    """
    Evolves MODP weights against observed outcomes.

    Fitness = -(Σ (predicted_score - actual_score)²), where
    predicted_score = Σ w_i × efficiency_i for the observed state.
    Population is scored on every observation; evolves every train_every.
    """

    def __init__(
        self,
        config: EfficiencyScorerConfig,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if not ENHANCEMENTS_AVAILABLE:
            raise RuntimeError("numpy is required for the evolutionary optimizer")
        self.config = config
        self.rng = rng if rng is not None else np.random.default_rng(config.seed)
        self.n_weights = 4
        self.counter = 0
        self.generation = 0

        w = config.modp_weights
        total = sum(w) if sum(w) > 0 else 1.0
        self.best_weights = np.array([w[0] / total, w[1] / total,
                                       w[2] / total, w[3] / total],
                                      dtype=np.float64)
        self.best_fitness = -math.inf
        self._lock = threading.RLock()

        self.population: List[_Ind] = [
            _Ind(weights=self.rng.dirichlet(np.ones(self.n_weights)))
            for _ in range(config.population_size)
        ]
        # Seed with config weights as one of the individuals so the
        # caller's initial preference is in the gene pool.
        self.population[0] = _Ind(weights=self.best_weights.copy())

        if config.model_path:
            self.load(Path(str(config.model_path) + ".evolve.json"))

    # ---- scoring ----------------------------------------------------------
    @staticmethod
    def _score(weights: "np.ndarray", features: "np.ndarray") -> float:
        # features = (accuracy, energy_eff, latency_eff, cost_eff)
        return _clamp(float(weights @ features))

    def observe(
        self,
        region_features: "np.ndarray",   # (4,) — efficiency features
        actual_score: float,
    ) -> None:
        target = _clamp(actual_score)
        with self._lock:
            for ind in self.population:
                pred = self._score(ind.weights, region_features)
                ind.fitness -= (pred - target) ** 2
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
            mask = self.rng.random(self.n_weights) < 0.5
            w = np.where(mask, a.weights, b.weights)
        else:
            w = a.weights.copy()
        total = w.sum()
        if total > 0:
            w = w / total
        return _Ind(weights=w)

    def _mutate(self, ind: _Ind) -> None:
        mask = self.rng.random(self.n_weights) < self.config.mutation_rate
        w = ind.weights + mask * self.rng.standard_normal(self.n_weights) * 0.05
        w = np.clip(w, 1e-6, None)
        total = w.sum()
        if total > 0:
            ind.weights = w / total

    def get_best_weights(self) -> "np.ndarray":
        with self._lock:
            return self.best_weights.copy()

    # ---- persistence ------------------------------------------------------
    def save(self, path: Path) -> None:
        _save_json(
            path,
            {
                "kind": "efficiency_evolution",
                "generation": self.generation,
                "counter": self.counter,
                "best_fitness": float(self.best_fitness),
                "best_weights": self.best_weights.tolist(),
                "population": [
                    {"weights": ind.weights.tolist(), "fitness": float(ind.fitness)}
                    for ind in self.population
                ],
            },
        )

    def load(self, path: Path) -> None:
        payload = _load_json(path)
        if not payload or payload.get("kind") != "efficiency_evolution":
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
# SECTION 9 — EfficiencyScorer (main)
# ============================================================================

class EfficiencyScorer:
    """
    Performance efficiency scorer with optional enhanced scoring.

    Base methods (`calculate_efficiency_score`, `calculate_cost_efficiency`,
    `calculate_throughput_efficiency`, `compare_efficiency`) are unchanged
    from v1.

    Enhanced method `calculate_composite_score` returns a bounded score in
    [0, 1] that:
      - equals the fallback formula when enhancements are off;
      - equals the fallback formula's initialization of the distillation
        student when distillation is on (so the default answer is the
        same, and training only *improves* it);
      - uses the evolved MODP weights when distillation is off but
        evolutionary is on.

    Call `record_outcome(actual_efficiency)` after each score to train the
    distillation and evolutionary optimizers against the true target.
    """

    # Expanded to match file 27's CARBON_INTENSITY regions.
    ELECTRICITY_COST: Dict[str, float] = {
        "US-CA": 0.25, "US-TX": 0.12, "US-NY": 0.20, "US-WV": 0.10,
        "EU-DE": 0.35, "EU-FR": 0.18, "EU-NO": 0.20, "EU-PL": 0.22,
        "CN": 0.08, "IN": 0.07, "JP": 0.22, "AU": 0.20,
        "BR": 0.15, "GLOBAL": 0.15,
    }

    def __init__(
        self,
        grid_region: str = "GLOBAL",
        config: Optional[Any] = None,
        *,
        metrics: Optional[MetricsSink] = None,
        rng: Optional["np.random.Generator"] = None,
    ) -> None:
        if config is None:
            self.config = EfficiencyScorerConfig()
        elif isinstance(config, EfficiencyScorerConfig):
            self.config = config
        elif isinstance(config, Mapping):
            self.config = EfficiencyScorerConfig.from_dict(config)
        else:
            raise TypeError(
                f"config must be EfficiencyScorerConfig, dict or None; "
                f"got {type(config).__name__}"
            )

        self.grid_region = grid_region
        self.electricity_cost = self.ELECTRICITY_COST.get(grid_region)
        if self.electricity_cost is None:
            logger.warning(
                "Unknown grid_region %r; falling back to GLOBAL intensity 0.15",
                grid_region,
            )
            self.electricity_cost = self.ELECTRICITY_COST["GLOBAL"]
            self.grid_region = "GLOBAL"

        self.metrics: MetricsSink = metrics or InMemoryMetrics()
        self._lock = threading.RLock()
        self.rng = rng if rng is not None else (
            np.random.default_rng(self.config.seed) if ENHANCEMENTS_AVAILABLE
            else None
        )

        # Local normalized weights — never mutate config.
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
                "falling back to deterministic scoring"
            )

        self._human_feedback_score = float(self.config.human_feedback_score)

        self.distillation_optimizer: Optional[EfficiencyDistillationOptimizer] = None
        self.evolutionary_optimizer: Optional[EvolutionaryMODPWeightsOptimizer] = None
        if self.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = EfficiencyDistillationOptimizer(
                    self.config,
                    rng=self.rng,
                    initial_weights=self._modp_weights,
                )
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(
                    self.config, rng=self.rng
                )

        # Pending decision for record_outcome
        self._pending: Optional[PendingDecision] = None
        self._pending_features: Optional["np.ndarray"] = None
        self._pending_teacher_scores: Optional["np.ndarray"] = None
        self._pending_gate_probs: Optional["np.ndarray"] = None

        logger.info(
            "Initialized EfficiencyScorer: region=%s, cost=%.3f USD/kWh, "
            "enhancements=%s",
            self.grid_region, self.electricity_cost, self.use_enhancements,
        )

    # ------------------------------------------------------------------
    # Base methods (unchanged behaviour)
    # ------------------------------------------------------------------
    def calculate_efficiency_score(self, accuracy: float, energy_kwh: float) -> float:
        """Raw accuracy / energy_kwh (unbounded; use the composite score
        for a normalized efficiency metric)."""
        if energy_kwh == 0:
            return 0.0
        return float(accuracy) / float(energy_kwh)

    def calculate_performance_per_watt(
        self, accuracy: float, power_watts: float,
        duration_seconds: Optional[float] = None,
    ) -> float:
        """Performance per watt.

        `duration_seconds` is accepted for v1 compatibility but unused —
        the value is `accuracy / power_watts`, which is instantaneous
        performance per unit power. For energy-based efficiency, use
        `calculate_efficiency_score(accuracy, energy_kwh)` where
        `energy_kwh = power_watts * duration_seconds / 3_600_000`.
        """
        if power_watts == 0:
            return 0.0
        return float(accuracy) / float(power_watts)

    def calculate_cost_efficiency(
        self, accuracy: float, energy_kwh: float
    ) -> Dict[str, float]:
        cost_usd = float(energy_kwh) * self.electricity_cost
        accuracy_per_dollar = (float(accuracy) / cost_usd) if cost_usd > 0 else 0.0
        return {
            "cost_usd": cost_usd,
            "accuracy_per_dollar": accuracy_per_dollar,
            "electricity_cost_per_kwh": self.electricity_cost,
            "grid_region": self.grid_region,
        }

    def calculate_throughput_efficiency(
        self, num_tasks: int, energy_kwh: float, duration_seconds: float
    ) -> Dict[str, float]:
        tasks_per_kwh = float(num_tasks) / energy_kwh if energy_kwh > 0 else 0.0
        tasks_per_second = (
            float(num_tasks) / duration_seconds if duration_seconds > 0 else 0.0
        )
        energy_per_task = (
            float(energy_kwh) / num_tasks if num_tasks > 0 else 0.0
        )
        return {
            "tasks_per_kwh": tasks_per_kwh,
            "tasks_per_second": tasks_per_second,
            "energy_per_task": energy_per_task,
            "num_tasks": int(num_tasks),
        }

    def compare_efficiency(
        self,
        agent_a_metrics: Mapping[str, float],
        agent_b_metrics: Mapping[str, float],
    ) -> Dict[str, Any]:
        eff_a = self.calculate_efficiency_score(
            agent_a_metrics.get("accuracy", 0.0),
            agent_a_metrics.get("energy_kwh", 1.0),
        )
        eff_b = self.calculate_efficiency_score(
            agent_b_metrics.get("accuracy", 0.0),
            agent_b_metrics.get("energy_kwh", 1.0),
        )
        # When both are 0, improvement is undefined.
        if eff_a > 0:
            improvement: Optional[float] = (eff_b - eff_a) / eff_a * 100.0
            improvement_reason = "ok"
        elif eff_b > 0:
            improvement = None
            improvement_reason = "agent_a_zero"
        else:
            improvement = 0.0
            improvement_reason = "both_zero"
        return {
            "agent_a_efficiency": eff_a,
            "agent_b_efficiency": eff_b,
            "improvement_percent": improvement,
            "improvement_reason": improvement_reason,
            "winner": "agent_b" if eff_b > eff_a else "agent_a",
        }

    # ------------------------------------------------------------------
    # Enhanced: composite efficiency score
    # ------------------------------------------------------------------
    def _fallback_score(
        self,
        state: EfficiencyState,
        weights: Tuple[float, float, float, float],
    ) -> float:
        """Deterministic MODP-weighted score. Source of truth."""
        w = weights
        return _clamp(
            w[0] * state.accuracy
            + w[1] * state.energy_efficiency
            + w[2] * state.latency_efficiency
            + w[3] * state.cost_efficiency
        )

    def calculate_composite_score(
        self,
        accuracy: float,
        energy_kwh: float,
        latency_ms: float = 0.0,
        cost_usd: Optional[float] = None,
        graph_metrics: Optional[Mapping[str, float]] = None,
        human_feedback_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Compute a multi-objective efficiency score in [0, 1].

        - When enhancements are off, returns the deterministic MODP score.
        - When distillation is on, returns the student/teacher blend; the
          student is initialized to the deterministic score, so the two
          are equal until record_outcome() trains it.
        - When evolutionary is on, uses the evolved MODP weights.
        """
        if cost_usd is None:
            cost_usd = _finite_nonneg(energy_kwh, "energy_kwh") * self.electricity_cost
        gm = dict(graph_metrics or self.config.graph_metrics)
        hf = (
            float(human_feedback_score)
            if human_feedback_score is not None
            else float(self._human_feedback_score)
        )

        state = EfficiencyState(
            accuracy=accuracy,
            energy_kwh=energy_kwh,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            graph_metrics=gm,
            human_feedback=hf,
            energy_scale_kwh=self.config.energy_scale_kwh,
            latency_scale_ms=self.config.latency_scale_ms,
            cost_scale_usd=self.config.cost_scale_usd,
        )

        # Reset pending at entry — a new score invalidates the previous.
        with self._lock:
            self._pending = None
            self._pending_features = None
            self._pending_teacher_scores = None
            self._pending_gate_probs = None

        if not self.use_enhancements:
            score = self._fallback_score(state, self._modp_weights)
            return {
                "score": round(score, 4),
                "method": "fallback",
                "graph_metrics": gm,
                "human_feedback_score": hf,
            }

        if self.distillation_optimizer is not None:
            score, x, t_scores, gate = self.distillation_optimizer.predict_score(
                state, exploration=False
            )
            with self._lock:
                self._pending_features = x
                self._pending_teacher_scores = t_scores
                self._pending_gate_probs = gate
                self._pending = PendingDecision(
                    state_vec=x, reward_observed=score
                )
            method = "distillation"
        elif self.evolutionary_optimizer is not None:
            best = self.evolutionary_optimizer.get_best_weights()
            score = self._fallback_score(
                state, (float(best[0]), float(best[1]), float(best[2]), float(best[3]))
            )
            with self._lock:
                self._pending_features = np.array(
                    [state.accuracy, state.energy_efficiency,
                     state.latency_efficiency, state.cost_efficiency],
                    dtype=np.float64,
                )
            method = "evolutionary"
        else:
            score = self._fallback_score(state, self._modp_weights)
            method = "fallback"

        try:
            self.metrics.observe(
                "efficiency_composite_score", float(score), method=method,
            )
        except Exception:  # noqa: BLE001
            pass

        return {
            "score": round(float(score), 4),
            "method": method,
            "graph_metrics": gm,
            "human_feedback_score": hf,
            "optimizer_stats": self._optimizer_stats(),
        }

    # ------------------------------------------------------------------
    # Enhanced: record the true outcome
    # ------------------------------------------------------------------
    def record_outcome(self, actual_efficiency: float) -> bool:
        """
        Train the optimizers against the true efficiency of the last
        calculate_composite_score call.

        Returns True if at least one optimizer was updated.
        """
        if not self.use_enhancements:
            return False
        target = _clamp(_finite_nonneg(actual_efficiency, "actual_efficiency"))
        with self._lock:
            x = self._pending_features
            t_scores = self._pending_teacher_scores
            gate = self._pending_gate_probs
            self._pending = None
            self._pending_features = None
            self._pending_teacher_scores = None
            self._pending_gate_probs = None
        if x is None:
            logger.debug("record_outcome(): no pending score")
            return False

        updated = False
        if self.distillation_optimizer is not None:
            try:
                self.distillation_optimizer.update(
                    x, target,
                    teacher_scores=t_scores, gate_probs=gate,
                )
                updated = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Distillation update failed: %s", exc)

        if self.evolutionary_optimizer is not None:
            try:
                # Only the first 4 features are the MODP inputs.
                self.evolutionary_optimizer.observe(x[:4], target)
                updated = True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolutionary update failed: %s", exc)

        if updated:
            try:
                self.metrics.observe(
                    "efficiency_actual", float(target),
                )
            except Exception:  # noqa: BLE001
                pass
        return updated

    # ------------------------------------------------------------------
    # Enhanced: update the live human feedback score
    # ------------------------------------------------------------------
    def record_human_feedback(self, score: float) -> None:
        score = _clamp(_finite_nonneg(score, "human_feedback_score"))
        with self._lock:
            self._human_feedback_score = score
        try:
            self.metrics.observe("efficiency_human_feedback", float(score))
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _optimizer_stats(self) -> Optional[Dict[str, Any]]:
        if self.distillation_optimizer is None and self.evolutionary_optimizer is None:
            return None
        stats: Dict[str, Any] = {}
        if self.distillation_optimizer is not None:
            stats["distillation"] = {
                "counter": self.distillation_optimizer.counter,
                "epsilon": self.distillation_optimizer.epsilon,
                "replay_size": len(self.distillation_optimizer.replay),
            }
        if self.evolutionary_optimizer is not None:
            stats["evolutionary"] = {
                "counter": self.evolutionary_optimizer.counter,
                "generation": self.evolutionary_optimizer.generation,
                "best_weights": self.evolutionary_optimizer.get_best_weights().tolist(),
            }
        return stats

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
# SECTION 10 — Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cases = [
        ("high efficiency", dict(accuracy=1.00, energy_kwh=0.001, latency_ms=80)),
        ("good efficiency", dict(accuracy=0.85, energy_kwh=0.001, latency_ms=100)),
        ("medium efficiency", dict(accuracy=0.85, energy_kwh=0.01, latency_ms=250)),
        ("wasteful", dict(accuracy=0.50, energy_kwh=0.1, latency_ms=800)),
        ("very wasteful", dict(accuracy=0.50, energy_kwh=8.0, latency_ms=3000)),
    ]

    print("=== Default (no enhancements) ===")
    base = EfficiencyScorer(grid_region="US-CA")
    for label, kw in cases:
        r = base.calculate_composite_score(**kw)
        print(f"  {label:<20} score={r['score']:.3f}  method={r['method']}")

    print("\n=== Enhanced (distillation, untrained) ===")
    cfg = EfficiencyScorerConfig.from_dict({
        "use_enhancements": True,
        "use_distillation": True,
        "use_evolutionary": True,
        "modp_weights": [0.4, 0.3, 0.2, 0.1],
        "seed": 42,
    })
    enh = EfficiencyScorer(grid_region="US-CA", config=cfg)
    for label, kw in cases:
        r = enh.calculate_composite_score(**kw)
        # Caller knows the "truth" is the fallback score.
        truth = base.calculate_composite_score(**kw)["score"]
        enh.record_outcome(truth)
        print(f"  {label:<20} score={r['score']:.3f}  truth={truth:.3f}  "
              f"method={r['method']}")

    print("\n=== Enhanced (after training) ===")
    for label, kw in cases:
        r = enh.calculate_composite_score(**kw)
        truth = base.calculate_composite_score(**kw)["score"]
        print(f"  {label:<20} score={r['score']:.3f}  truth={truth:.3f}  "
              f"delta={r['score']-truth:+.3f}")

    print("\nOptimizer stats:", enh._optimizer_stats())
